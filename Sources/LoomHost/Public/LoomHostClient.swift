//
//  LoomHostClient.swift
//  LoomHost
//
//  Created by Ethan Lipnik on 3/10/26.
//

import Foundation
import Loom

#if os(macOS)
import Darwin

package struct LoomHostClientConnection: Sendable {
    package let descriptor: LoomHostConnectionDescriptor
    package let session: LoomVirtualAppSession
}

/// Client for the App Group-scoped shared-host broker.
public actor LoomHostClient {
    public let configuration: LoomSharedHostConfiguration

    private let clientID = UUID()
    private let runtimeFactory: (@Sendable () async throws -> LoomHostRuntimeDependencies)?
    private let stateBroadcaster = LoomAsyncBroadcaster<LoomHostStateSnapshot>()
    private let incomingConnectionBroadcaster = LoomAsyncBroadcaster<LoomHostClientConnection>()

    private var broker: LoomHostBroker?
    private var socket: LoomHostSocketConnection?
    private var currentSnapshot = LoomHostStateSnapshot(
        peers: [],
        isRunning: false,
        isRemoteHosting: false,
        lastErrorMessage: nil
    )
    private var isStarted = false
    private var pendingReplies: [UUID: CheckedContinuation<LoomHostIPCMessage, Error>] = [:]
    private var sessionsByConnectionID: [UUID: LoomVirtualAppSession] = [:]

    public init(configuration: LoomSharedHostConfiguration) {
        self.configuration = configuration
        runtimeFactory = nil
    }

    package init(
        configuration: LoomSharedHostConfiguration,
        runtimeFactory: @escaping @Sendable () async throws -> LoomHostRuntimeDependencies
    ) {
        self.configuration = configuration
        self.runtimeFactory = runtimeFactory
    }

    deinit {
        stateBroadcaster.finish()
        incomingConnectionBroadcaster.finish()
    }

    package func makeStateStream() -> AsyncStream<LoomHostStateSnapshot> {
        stateBroadcaster.makeStream(initialValue: currentSnapshot)
    }

    package func makeIncomingConnectionStream() -> AsyncStream<LoomHostClientConnection> {
        incomingConnectionBroadcaster.makeStream()
    }

    package func start() async throws {
        isStarted = true
        try await connectAndRegister()
    }

    package func stop() async {
        isStarted = false
        if socket != nil {
            _ = try? await request(.unregister(clientID: clientID))
        }
        let liveSessions = Array(sessionsByConnectionID.values)
        sessionsByConnectionID.removeAll()
        for session in liveSessions {
            await session.handleStateChanged(.cancelled)
        }
        if let socket {
            await socket.close()
        }
        socket = nil
    }

    package func refreshPeers() async throws {
        _ = try await request(.refreshPeers(clientID: clientID))
    }

    package func startRemoteHosting(
        sessionID: String,
        publicHostForTCP: String?
    ) async throws {
        _ = try await request(
            .startRemoteHosting(
                clientID: clientID,
                sessionID: sessionID,
                publicHostForTCP: publicHostForTCP
            )
        )
    }

    package func stopRemoteHosting() async throws {
        _ = try await request(.stopRemoteHosting(clientID: clientID))
    }

    package func connect(to peerID: LoomPeerID) async throws -> LoomHostClientConnection {
        let reply = try await request(.connect(clientID: clientID, peerID: peerID))
        guard case let .connected(descriptor) = reply else {
            throw LoomHostError.protocolViolation("Broker returned an unexpected connect response.")
        }
        return makeConnection(from: descriptor)
    }

    package func connect(remoteSessionID: String) async throws -> LoomHostClientConnection {
        let reply = try await request(
            .connectRemote(
                clientID: clientID,
                sessionID: remoteSessionID
            )
        )
        guard case let .connected(descriptor) = reply else {
            throw LoomHostError.protocolViolation("Broker returned an unexpected remote connect response.")
        }
        return makeConnection(from: descriptor)
    }

    package func disconnect(connectionID: UUID) async throws {
        _ = try await request(
            .disconnect(
                clientID: clientID,
                connectionID: connectionID
            )
        )
    }

    private func connectAndRegister() async throws {
        if socket == nil {
            socket = try await connectTransport()
        }

        let reply = try await request(
            .register(
                clientID: clientID,
                app: configuration.app
            )
        )
        guard case let .registered(snapshot) = reply else {
            throw LoomHostError.protocolViolation("Broker returned an unexpected register response.")
        }
        currentSnapshot = snapshot
        stateBroadcaster.yield(snapshot)
    }

    private func connectTransport() async throws -> LoomHostSocketConnection {
        let layout = try Self.socketLayout(for: configuration)
        try FileManager.default.createDirectory(
            at: layout.directoryURL,
            withIntermediateDirectories: true
        )

        if let connection = try? await LoomHostSocketConnection.connect(
            to: layout.socketURL.path,
            onFrame: { [weak self] frame in
                guard let self else { return }
                await self.handle(frame: frame)
            },
            onClosed: { [weak self] in
                guard let self else { return }
                await self.handleSocketClosed()
            }
        ) {
            return connection
        }

        try await maybeLaunchBroker(layout: layout)

        for _ in 0..<20 {
            if let connection = try? await LoomHostSocketConnection.connect(
                to: layout.socketURL.path,
                onFrame: { [weak self] frame in
                    guard let self else { return }
                    await self.handle(frame: frame)
                },
                onClosed: { [weak self] in
                    guard let self else { return }
                    await self.handleSocketClosed()
                }
            ) {
                return connection
            }
            try await Task.sleep(for: .milliseconds(100))
        }

        throw LoomHostError.brokerUnavailable
    }

    private func maybeLaunchBroker(layout: LoomHostSocketLayout) async throws {
        guard broker == nil else {
            return
        }
        guard let runtimeFactory else {
            return
        }

        let lockFD = open(layout.lockURL.path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
        guard lockFD >= 0 else {
            throw POSIXError(.init(rawValue: errno) ?? .EIO)
        }
        guard flock(lockFD, LOCK_EX | LOCK_NB) == 0 else {
            Darwin.close(lockFD)
            return
        }

        let broker = LoomHostBroker(
            configuration: configuration,
            socketPath: layout.socketURL.path,
            lockFileDescriptor: lockFD,
            runtimeFactory: runtimeFactory
        )
        try await broker.start()
        self.broker = broker
    }

    private func request(_ message: LoomHostIPCMessage) async throws -> LoomHostIPCMessage {
        if socket == nil {
            try await connectAndRegister()
        }
        guard let socket else {
            throw LoomHostError.brokerUnavailable
        }

        let requestID = UUID()
        return try await withCheckedThrowingContinuation { continuation in
            pendingReplies[requestID] = continuation
            Task { [weak self] in
                do {
                    try await socket.send(
                        LoomHostIPCFrame(
                            requestID: requestID,
                            message: message
                        )
                    )
                } catch {
                    guard let self else {
                        continuation.resume(throwing: LoomHostError.brokerUnavailable)
                        return
                    }
                    await self.failPendingReply(
                        requestID: requestID,
                        error: error
                    )
                }
            }
        }
    }

    private func handle(frame: LoomHostIPCFrame) async {
        if let requestID = frame.requestID,
           let continuation = pendingReplies.removeValue(forKey: requestID) {
            switch frame.message {
            case let .reply(status):
                switch status {
                case .ok:
                    continuation.resume(returning: frame.message)
                case let .failed(message):
                    continuation.resume(throwing: LoomHostError.remoteFailure(message))
                }
            default:
                continuation.resume(returning: frame.message)
            }
            return
        }

        switch frame.message {
        case let .registered(snapshot),
             let .stateChanged(snapshot):
            currentSnapshot = snapshot
            stateBroadcaster.yield(snapshot)

        case let .incomingConnection(descriptor):
            let connection = makeConnection(from: descriptor)
            incomingConnectionBroadcaster.yield(connection)

        case let .connectionStateChanged(connectionID, state, _):
            if let session = sessionsByConnectionID[connectionID] {
                await session.handleStateChanged(state)
            }
            switch state {
            case .cancelled,
                 .failed:
                sessionsByConnectionID.removeValue(forKey: connectionID)
            case .idle,
                 .handshaking,
                 .ready:
                break
            }

        case let .streamOpened(connectionID, streamID, label):
            if let session = sessionsByConnectionID[connectionID] {
                await session.handleRemoteStreamOpened(streamID: streamID, label: label)
            }

        case let .streamDataReceived(connectionID, streamID, payloadBase64):
            guard let payload = Data(base64Encoded: payloadBase64),
                  let session = sessionsByConnectionID[connectionID] else {
                return
            }
            await session.handleRemoteStreamData(streamID: streamID, payload: payload)

        case let .streamClosed(connectionID, streamID):
            if let session = sessionsByConnectionID[connectionID] {
                await session.handleRemoteStreamClosed(streamID: streamID)
            }

        case .reply,
             .connect,
             .connectRemote,
             .refreshPeers,
             .register,
             .unregister,
             .disconnect,
             .openStream,
             .streamData,
             .closeStream,
             .startRemoteHosting,
             .stopRemoteHosting,
             .connected:
            break
        }
    }

    private func handleSocketClosed() async {
        socket = nil
        failAllPendingReplies(error: LoomHostError.brokerUnavailable)

        let liveSessions = Array(sessionsByConnectionID.values)
        sessionsByConnectionID.removeAll()
        for session in liveSessions {
            await session.handleStateChanged(.failed("shared-host-broker-disconnected"))
        }

        if !isStarted {
            return
        }

        for _ in 0..<10 {
            do {
                try await connectAndRegister()
                return
            } catch {
                try? await Task.sleep(for: .milliseconds(250))
            }
        }
        currentSnapshot = LoomHostStateSnapshot(
            peers: [],
            isRunning: false,
            isRemoteHosting: false,
            lastErrorMessage: LoomHostError.brokerUnavailable.localizedDescription
        )
        stateBroadcaster.yield(currentSnapshot)
    }

    private func failPendingReply(
        requestID: UUID,
        error: Error
    ) {
        guard let continuation = pendingReplies.removeValue(forKey: requestID) else {
            return
        }
        continuation.resume(throwing: error)
    }

    private func failAllPendingReplies(error: Error) {
        let continuations = Array(pendingReplies.values)
        pendingReplies.removeAll()
        for continuation in continuations {
            continuation.resume(throwing: error)
        }
    }

    private func makeConnection(from descriptor: LoomHostConnectionDescriptor) -> LoomHostClientConnection {
        if let existing = sessionsByConnectionID[descriptor.connectionID] {
            return LoomHostClientConnection(descriptor: descriptor, session: existing)
        }
        let session = LoomVirtualAppSession(
            connectionID: descriptor.connectionID,
            transportKind: descriptor.context.transportKind,
            context: descriptor.context,
            openHandler: { [weak self] connectionID, streamID, label in
                guard let self else { throw LoomHostError.brokerUnavailable }
                _ = try await self.request(
                    .openStream(
                        clientID: self.clientID,
                        connectionID: connectionID,
                        streamID: streamID,
                        label: label
                    )
                )
            },
            sendHandler: { [weak self] connectionID, streamID, payload in
                guard let self else { throw LoomHostError.brokerUnavailable }
                _ = try await self.request(
                    .streamData(
                        clientID: self.clientID,
                        connectionID: connectionID,
                        streamID: streamID,
                        payloadBase64: payload.base64EncodedString()
                    )
                )
            },
            closeHandler: { [weak self] connectionID, streamID in
                guard let self else { throw LoomHostError.brokerUnavailable }
                _ = try await self.request(
                    .closeStream(
                        clientID: self.clientID,
                        connectionID: connectionID,
                        streamID: streamID
                    )
                )
            },
            cancelHandler: { [weak self] connectionID in
                guard let self else { return }
                try? await self.disconnect(connectionID: connectionID)
            }
        )
        sessionsByConnectionID[descriptor.connectionID] = session
        return LoomHostClientConnection(descriptor: descriptor, session: session)
    }

    private static func socketLayout(for configuration: LoomSharedHostConfiguration) throws -> LoomHostSocketLayout {
        let directoryURL: URL
        if let directoryURLOverride = configuration.directoryURLOverride {
            directoryURL = directoryURLOverride
        } else if let appGroupURL = FileManager.default.containerURL(
            forSecurityApplicationGroupIdentifier: configuration.appGroupIdentifier
        ) {
            directoryURL = appGroupURL.appendingPathComponent("LoomHost", isDirectory: true)
        } else {
            throw LoomHostError.invalidSharedContainer(
                "The shared-host App Group \(configuration.appGroupIdentifier) is unavailable."
            )
        }

        return LoomHostSocketLayout(
            directoryURL: directoryURL,
            socketURL: directoryURL.appendingPathComponent("\(configuration.socketName).sock"),
            lockURL: directoryURL.appendingPathComponent("\(configuration.socketName).lock")
        )
    }
}

package struct LoomHostSocketLayout: Sendable {
    package let directoryURL: URL
    package let socketURL: URL
    package let lockURL: URL
}
#else
package struct LoomHostClientConnection: Sendable {
    package let descriptor: LoomHostConnectionDescriptor
    package let session: any LoomSessionProtocol
}

/// Client for the App Group-scoped shared-host broker.
public actor LoomHostClient {
    public let configuration: LoomSharedHostConfiguration

    public init(configuration: LoomSharedHostConfiguration) {
        self.configuration = configuration
    }

    package func makeStateStream() -> AsyncStream<LoomHostStateSnapshot> {
        AsyncStream { continuation in
            continuation.yield(
                LoomHostStateSnapshot(
                    peers: [],
                    isRunning: false,
                    isRemoteHosting: false,
                    lastErrorMessage: LoomHostError.unsupportedPlatform.localizedDescription
                )
            )
            continuation.finish()
        }
    }

    package func makeIncomingConnectionStream() -> AsyncStream<LoomHostClientConnection> {
        AsyncStream { continuation in
            continuation.finish()
        }
    }

    package func start() async throws {
        throw LoomHostError.unsupportedPlatform
    }

    package func stop() async {}

    package func refreshPeers() async throws {
        throw LoomHostError.unsupportedPlatform
    }

    package func startRemoteHosting(
        sessionID: String,
        publicHostForTCP: String?
    ) async throws {
        throw LoomHostError.unsupportedPlatform
    }

    package func stopRemoteHosting() async throws {
        throw LoomHostError.unsupportedPlatform
    }

    package func connect(to peerID: LoomPeerID) async throws -> LoomHostClientConnection {
        throw LoomHostError.unsupportedPlatform
    }

    package func connect(remoteSessionID: String) async throws -> LoomHostClientConnection {
        throw LoomHostError.unsupportedPlatform
    }

    package func disconnect(connectionID: UUID) async throws {
        throw LoomHostError.unsupportedPlatform
    }
}
#endif
