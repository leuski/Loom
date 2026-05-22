//
//  LoomNativeQUICSessionTransport.swift
//  Loom
//
//  Created by Ethan Lipnik on 5/21/26.
//

import Dispatch
import Foundation
import Network

@available(macOS 26.0, iOS 26.0, visionOS 26.0, tvOS 26.0, watchOS 26.0, *)
package actor LoomNativeQUICSessionTransport: LoomSessionTransport {
    package let receiveSemantics: LoomSessionReceiveSemantics = .independentReliableAndUnreliable

    private let connection: NetworkConnection<QUIC>
    private let role: LoomSessionRole
    private var controlStream: QUIC.Stream<QUICStream>?
    private var datagrams: QUIC.Datagrams<QUICDatagram>?
    private var receiveBuffer = Data()
    private var queuedUnreliableSenders: [LoomQueuedUnreliableSendProfile: LoomOrderedUnreliableSendQueue] = [:]
    private var inboundStreamTask: Task<Void, Never>?
    private var datagramReceiveTask: Task<Void, Never>?
    private var isClosed = false

    private let unreliableDeliveryStream: AsyncStream<Data>
    private var unreliableDeliveryContinuation: AsyncStream<Data>.Continuation?
    private let priorityUnreliableDeliveryStream: AsyncStream<Data>
    private var priorityUnreliableDeliveryContinuation: AsyncStream<Data>.Continuation?

    package init(
        connection: NetworkConnection<QUIC>,
        role: LoomSessionRole
    ) {
        self.connection = connection
        self.role = role
        let (unreliableStream, unreliableContinuation) = AsyncStream.makeStream(of: Data.self)
        unreliableDeliveryStream = unreliableStream
        unreliableDeliveryContinuation = unreliableContinuation
        let (priorityStream, priorityContinuation) = AsyncStream.makeStream(of: Data.self)
        priorityUnreliableDeliveryStream = priorityStream
        priorityUnreliableDeliveryContinuation = priorityContinuation
    }

    deinit {
        inboundStreamTask?.cancel()
        datagramReceiveTask?.cancel()
        unreliableDeliveryContinuation?.finish()
        priorityUnreliableDeliveryContinuation?.finish()
        for sender in queuedUnreliableSenders.values {
            sender.close()
        }
    }

    package func startAndAwaitReady(queue: DispatchQueue) async throws {
        try await awaitConnectionReady()
        switch role {
        case .initiator:
            controlStream = try await connection.openStream(directionality: .bidirectional)
        case .receiver:
            controlStream = try await receiveInitialInboundStream()
        }
        datagrams = try await connection.datagrams
        startDatagramReceiveLoop()
    }

    package func sendMessage(_ data: Data) async throws {
        try await sendFrame(data)
    }

    package func receiveMessage(maxBytes: Int) async throws -> Data {
        try await readFrame(maxBytes: maxBytes)
    }

    package func sendHandshakeMessage(_ data: Data) async throws {
        try await sendMessage(data)
    }

    package func receiveHandshakeMessage(maxBytes: Int) async throws -> Data {
        try await receiveMessage(maxBytes: maxBytes)
    }

    package func sendUnreliable(_ data: Data) async throws {
        guard let datagrams else {
            throw LoomError.protocolError("Native QUIC datagram channel is not ready.")
        }
        try await datagrams.send(data)
    }

    package func sendUnreliableQueued(
        _ data: Data,
        profile: LoomQueuedUnreliableSendProfile,
        onComplete: @escaping @Sendable (Error?) -> Void
    ) async {
        guard datagrams != nil else {
            onComplete(LoomError.protocolError("Native QUIC datagram channel is not ready."))
            return
        }

        queuedUnreliableSender(for: profile).enqueue(data) { error in
            if let error {
                onComplete(LoomError.connectionFailed(LoomConnectionFailure.classify(error)))
            } else {
                onComplete(nil)
            }
        }
    }

    package func resetQueuedUnreliableSends(
        profile: LoomQueuedUnreliableSendProfile
    ) async {
        queuedUnreliableSenders.removeValue(forKey: profile)?.close()
    }

    package func receiveUnreliable(maxBytes: Int) async throws -> Data {
        for await message in unreliableDeliveryStream {
            if message.count > maxBytes {
                throw LoomError.protocolError("Received native QUIC datagram exceeds limit: \(message.count) > \(maxBytes)")
            }
            return message
        }
        throw LoomError.connectionFailed(
            LoomConnectionFailure(reason: .cancelled, detail: "Native QUIC datagram receive cancelled.")
        )
    }

    package func receivePriorityUnreliable(maxBytes: Int) async throws -> Data {
        for await message in priorityUnreliableDeliveryStream {
            if message.count > maxBytes {
                throw LoomError.protocolError("Received native QUIC priority datagram exceeds limit: \(message.count) > \(maxBytes)")
            }
            return message
        }
        throw LoomError.connectionFailed(
            LoomConnectionFailure(reason: .cancelled, detail: "Native QUIC priority datagram receive cancelled.")
        )
    }

    package func cancelPendingUnreliableSends() async {
        isClosed = true
        inboundStreamTask?.cancel()
        datagramReceiveTask?.cancel()
        for sender in queuedUnreliableSenders.values {
            sender.close()
        }
        unreliableDeliveryContinuation?.finish()
        priorityUnreliableDeliveryContinuation?.finish()
    }

    private func awaitConnectionReady() async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let box = LoomNativeQUICReadyContinuationBox(continuation: continuation)
            connection.onStateUpdate { _, state in
                switch state {
                case .ready:
                    box.complete(.success(()))
                case let .failed(error):
                    box.complete(.failure(LoomError.connectionFailed(LoomConnectionFailure.classify(error))))
                case .cancelled:
                    box.complete(
                        .failure(
                            LoomError.connectionFailed(
                                LoomConnectionFailure(reason: .cancelled, detail: "Native QUIC connection cancelled.")
                            )
                        )
                    )
                case let .waiting(error):
                    LoomLogger.transport("Native QUIC connection waiting: \(error)")
                    if LoomFramedConnection.shouldFailAfterWaiting(error) {
                        DispatchQueue.global().asyncAfter(deadline: .now() + 2) {
                            box.complete(.failure(LoomError.connectionFailed(LoomConnectionFailure.classify(error))))
                        }
                    }
                case .setup, .preparing:
                    break
                @unknown default:
                    break
                }
            }
            _ = connection.start()
        }
    }

    private func receiveInitialInboundStream() async throws -> QUIC.Stream<QUICStream> {
        let (stream, continuation) = AsyncThrowingStream.makeStream(of: QUIC.Stream<QUICStream>.self)
        inboundStreamTask = Task { [connection] in
            do {
                try await connection.inboundStreams { inboundStream in
                    continuation.yield(inboundStream)
                }
                continuation.finish()
            } catch {
                continuation.finish(throwing: LoomError.connectionFailed(LoomConnectionFailure.classify(error)))
            }
        }

        var iterator = stream.makeAsyncIterator()
        guard let inboundStream = try await iterator.next() else {
            throw LoomError.connectionFailed(
                LoomConnectionFailure(reason: .closed, detail: "Native QUIC connection closed before opening a control stream.")
            )
        }
        return inboundStream
    }

    private func startDatagramReceiveLoop() {
        guard datagramReceiveTask == nil, let datagrams else { return }
        datagramReceiveTask = Task { [weak self, datagrams] in
            while !Task.isCancelled {
                do {
                    let message = try await datagrams.receive()
                    await self?.handleIncomingDatagram(message.content)
                } catch {
                    if !Task.isCancelled {
                        await self?.finishDatagramStreams()
                    }
                    break
                }
            }
        }
    }

    private func handleIncomingDatagram(_ data: Data) {
        guard !isClosed else { return }
        if data.first == LoomSessionTrafficClass.priorityInput.rawValue {
            priorityUnreliableDeliveryContinuation?.yield(data)
        } else {
            unreliableDeliveryContinuation?.yield(data)
        }
    }

    private func finishDatagramStreams() {
        unreliableDeliveryContinuation?.finish()
        priorityUnreliableDeliveryContinuation?.finish()
    }

    private func queuedUnreliableSender(
        for profile: LoomQueuedUnreliableSendProfile
    ) -> LoomOrderedUnreliableSendQueue {
        if let existing = queuedUnreliableSenders[profile] {
            return existing
        }

        let limits = LoomOrderedUnreliableSendQueue.limits(for: profile)
        let datagrams = datagrams
        let sender = LoomOrderedUnreliableSendQueue(
            queue: DispatchQueue(
                label: "loom.native-quic.datagram.send.\(profile.rawValue)",
                qos: .userInteractive
            ),
            maxOutstandingPackets: limits.maxOutstandingPackets,
            maxOutstandingBytes: limits.maxOutstandingBytes,
            maxQueuedPackets: limits.maxQueuedPackets,
            replacesQueuedSends: limits.replacesQueuedSends,
            diagnosticsLabel: "native-quic.\(profile.rawValue)"
        ) { data, onComplete in
            guard let datagrams else {
                onComplete(.posix(.ENOTCONN))
                return
            }
            Task {
                do {
                    try await datagrams.send(data)
                    onComplete(nil)
                } catch {
                    onComplete((error as? NWError) ?? .posix(.EIO))
                }
            }
        }
        queuedUnreliableSenders[profile] = sender
        return sender
    }

    private func sendFrame(_ data: Data) async throws {
        guard let controlStream else {
            throw LoomError.protocolError("Native QUIC control stream is not ready.")
        }
        var frame = Data(capacity: 4 + data.count)
        let length = UInt32(data.count).bigEndian
        withUnsafeBytes(of: length) { frame.append(contentsOf: $0) }
        frame.append(data)
        try await controlStream.send(frame)
    }

    private func readFrame(maxBytes: Int) async throws -> Data {
        while receiveBuffer.count < 4 {
            try await appendControlChunk()
        }

        let length =
            (UInt32(receiveBuffer[0]) << 24) |
            (UInt32(receiveBuffer[1]) << 16) |
            (UInt32(receiveBuffer[2]) << 8) |
            UInt32(receiveBuffer[3])
        guard length <= UInt32(maxBytes) else {
            throw LoomError.protocolError("Received native QUIC frame larger than \(maxBytes) bytes.")
        }

        let requiredBytes = 4 + Int(length)
        while receiveBuffer.count < requiredBytes {
            try await appendControlChunk()
        }

        let payload = Data(receiveBuffer[4..<requiredBytes])
        receiveBuffer.removeSubrange(0..<requiredBytes)
        return payload
    }

    private func appendControlChunk() async throws {
        guard let controlStream else {
            throw LoomError.protocolError("Native QUIC control stream is not ready.")
        }
        let message = try await controlStream.receive(atLeast: 1, atMost: 65_536)
        if message.content.isEmpty, message.metadata.endOfStream {
            throw LoomError.connectionFailed(
                LoomConnectionFailure(reason: .closed, detail: "Native QUIC control stream closed by peer.")
            )
        }
        receiveBuffer.append(message.content)
    }
}

@available(macOS 26.0, iOS 26.0, visionOS 26.0, tvOS 26.0, watchOS 26.0, *)
private final class LoomNativeQUICReadyContinuationBox: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Error>?

    init(continuation: CheckedContinuation<Void, Error>) {
        self.continuation = continuation
    }

    func complete(_ result: Result<Void, Error>) {
        lock.lock()
        guard let continuation else {
            lock.unlock()
            return
        }
        self.continuation = nil
        lock.unlock()

        switch result {
        case .success:
            continuation.resume()
        case let .failure(error):
            continuation.resume(throwing: error)
        }
    }
}
