//
//  LoomNativeQUICDirectListener.swift
//  Loom
//
//  Created by Ethan Lipnik on 5/21/26.
//

import Foundation
import Network

@available(macOS 26.0, iOS 26.0, visionOS 26.0, tvOS 26.0, watchOS 26.0, *)
package final class LoomNativeQUICDirectListener: @unchecked Sendable {
    private let enablePeerToPeer: Bool
    private let quicALPN: [String]
    private let serviceClass: NWParameters.ServiceClass
    private var listener: NetworkListener<QUIC>?
    private var runTask: Task<Void, Never>?

    package init(
        enablePeerToPeer: Bool,
        quicALPN: [String],
        serviceClass: NWParameters.ServiceClass
    ) {
        self.enablePeerToPeer = enablePeerToPeer
        self.quicALPN = quicALPN
        self.serviceClass = serviceClass
    }

    package func start(
        port requestedPort: UInt16,
        onConnection: @escaping @Sendable (NetworkConnection<QUIC>) -> Void
    ) async throws -> UInt16 {
        let listener = try LoomNativeQUICTransportFactory.makeListener(
            port: requestedPort,
            enablePeerToPeer: enablePeerToPeer,
            quicALPN: quicALPN,
            serviceClass: serviceClass
        )
        self.listener = listener

        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let box = LoomNativeQUICListenerReadyContinuationBox(continuation: continuation)
            listener.onStateUpdate { _, state in
                switch state {
                case .ready:
                    box.complete(.success(()))
                case let .failed(error):
                    box.complete(.failure(LoomError.connectionFailed(LoomConnectionFailure.classify(error))))
                case .cancelled:
                    box.complete(
                        .failure(
                            LoomError.connectionFailed(
                                LoomConnectionFailure(reason: .cancelled, detail: "Native QUIC listener cancelled.")
                            )
                        )
                    )
                case let .waiting(error):
                    LoomLogger.transport("Native QUIC listener waiting: \(error)")
                case .setup:
                    break
                @unknown default:
                    break
                }
            }

            runTask = Task { [listener] in
                do {
                    try await listener.run { connection in
                        onConnection(connection)
                    }
                } catch {
                    box.complete(.failure(LoomError.connectionFailed(LoomConnectionFailure.classify(error))))
                    LoomLogger.transport("Native QUIC listener stopped: \(error)")
                }
            }
        }

        guard let port = listener.port?.rawValue else {
            throw LoomError.protocolError("Native QUIC listener started without a bound port.")
        }
        LoomLogger.transport("Native QUIC listener started on port \(port)")
        return port
    }

    package func stop() async {
        runTask?.cancel()
        runTask = nil
        listener = nil
    }
}

@available(macOS 26.0, iOS 26.0, visionOS 26.0, tvOS 26.0, watchOS 26.0, *)
private final class LoomNativeQUICListenerReadyContinuationBox: @unchecked Sendable {
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
