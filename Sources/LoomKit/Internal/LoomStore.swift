//
//  LoomStore.swift
//  LoomKit
//
//  Created by Ethan Lipnik on 3/10/26.
//

import Foundation
import Loom
import LoomCloudKit
import LoomSharedRuntime
#if canImport(UIKit)
import UIKit
#endif

struct LoomStoreSnapshot: Sendable {
    let peers: [LoomPeerSnapshot]
    let connections: [LoomConnectionSnapshot]
    let transfers: [LoomTransferSnapshot]
    let isRunning: Bool
    let isPublishingRemoteReachability: Bool
    let localPeerCapabilities: LoomPeerCapabilities
    let lastErrorMessage: String?
}

private struct ManagedConnection: Sendable {
    let handle: LoomConnectionHandle
    let signalingSessionID: String?
}

enum LoomStoreError: LocalizedError, Sendable {
    case invalidConfiguration(String)
    case peerNotFound(LoomPeerID)
    case remoteSignalingUnavailable
    case cloudKitUnavailable
    case bootstrapMetadataUnavailable
    case wakeOnLANUnavailable
    case controlEndpointUnavailable
    case sshEndpointUnavailable

    var errorDescription: String? {
        switch self {
        case let .invalidConfiguration(message):
            message
        case let .peerNotFound(peerID):
            "LoomKit could not resolve peer \(peerID.uuidString)."
        case .remoteSignalingUnavailable:
            "LoomKit remote signaling configuration is unavailable."
        case .cloudKitUnavailable:
            "LoomKit CloudKit integration is unavailable."
        case .bootstrapMetadataUnavailable:
            "LoomKit peer bootstrap metadata is unavailable."
        case .wakeOnLANUnavailable:
            "The selected peer does not publish Wake-on-LAN metadata."
        case .controlEndpointUnavailable:
            "The selected peer does not publish a bootstrap control endpoint."
        case .sshEndpointUnavailable:
            "The selected peer does not publish an SSH bootstrap endpoint."
        }
    }
}

actor LoomStore {
    let configuration: LoomContainerConfiguration
    let deviceID: UUID

    private let node: LoomNode
    private let trustStore: LoomTrustStore
    private let cloudKitManager: LoomCloudKitManager?
    private let peerProvider: LoomCloudKitPeerProvider?
    private let peerManager: LoomCloudKitPeerManager?
    private let signalingClient: LoomRemoteSignalingClient?
    private let connectionCoordinator: LoomConnectionCoordinator
    private let hostClient: LoomHostClient?
    private let wakeOnLANClient: any LoomWakeOnLANClient
    private let bootstrapControlClient: any LoomBootstrapControlClient
    private let sshBootstrapClient: any LoomSSHBootstrapClient
    private let snapshotBroadcaster = LoomAsyncBroadcaster<LoomStoreSnapshot>()
    private let incomingConnectionBroadcaster = LoomAsyncBroadcaster<LoomConnectionHandle>()

    private var isRunning = false
    private var isRemoteHosting = false
    private var lastErrorMessage: String?
    private var listeningPorts: [LoomTransportKind: UInt16] = [:]
    private var discoveryObserverToken: UUID?
    private var overlayDirectory: LoomOverlayDirectory?
    private var overlayDirectoryObserverToken: UUID?
    private var localPeersByID: [LoomPeerID: LoomPeer] = [:]
    private var localPeerLastSeen: [LoomPeerID: Date] = [:]
    private var overlayPeersByID: [LoomPeerID: LoomPeer] = [:]
    private var overlayPeerLastSeen: [LoomPeerID: Date] = [:]
    private var cloudPeersByID: [LoomPeerID: LoomCloudKitPeerInfo] = [:]
    private var connections: [UUID: ManagedConnection] = [:]
    private var connectionSnapshots: [UUID: LoomConnectionSnapshot] = [:]
    private var transferSnapshots: [UUID: LoomTransferSnapshot] = [:]
    private var signalingHeartbeatTask: Task<Void, Never>?
    private var currentRemoteSessionID: String?
    private var currentPublicHostForTCP: String?
    private var cachedBootstrapMetadata: LoomBootstrapMetadata?
    private var hostSnapshot: LoomHostStateSnapshot?
    private var hostStateTask: Task<Void, Never>?
    private var hostIncomingTask: Task<Void, Never>?

    init(
        configuration: LoomContainerConfiguration,
        deviceID: UUID,
        node: LoomNode,
        trustStore: LoomTrustStore,
        cloudKitManager: LoomCloudKitManager?,
        peerProvider: LoomCloudKitPeerProvider?,
        peerManager: LoomCloudKitPeerManager?,
        signalingClient: LoomRemoteSignalingClient?,
        connectionCoordinator: LoomConnectionCoordinator,
        hostClient: LoomHostClient? = nil,
        wakeOnLANClient: any LoomWakeOnLANClient = LoomDefaultWakeOnLANClient(),
        bootstrapControlClient: any LoomBootstrapControlClient = LoomDefaultBootstrapControlClient(),
        sshBootstrapClient: any LoomSSHBootstrapClient = LoomDefaultSSHBootstrapClient()
    ) {
        self.configuration = configuration
        self.deviceID = deviceID
        self.node = node
        self.trustStore = trustStore
        self.cloudKitManager = cloudKitManager
        self.peerProvider = peerProvider
        self.peerManager = peerManager
        self.signalingClient = signalingClient
        self.connectionCoordinator = connectionCoordinator
        self.hostClient = hostClient
        self.wakeOnLANClient = wakeOnLANClient
        self.bootstrapControlClient = bootstrapControlClient
        self.sshBootstrapClient = sshBootstrapClient

    }

    func makeSnapshotStream() -> AsyncStream<LoomStoreSnapshot> {
        snapshotBroadcaster.makeStream(initialValue: currentSnapshot())
    }

    func makeIncomingConnectionsStream() -> AsyncStream<LoomConnectionHandle> {
        incomingConnectionBroadcaster.makeStream()
    }

    func start() async throws {
        guard !isRunning else {
            return
        }

        if let hostClient {
            do {
                ensureHostObserversStarted()
                try await hostClient.start()
                await notifyStateChanged()
                return
            } catch {
                await record(error)
            }
        }

        do {
            try validateConfiguration()
            lastErrorMessage = nil

            if let cloudKitManager {
                await cloudKitManager.initialize()
            }
            if let peerManager {
                await peerManager.setup()
            }

            let discovery = await MainActor.run {
                node.makeDiscovery(localDeviceID: deviceID)
            }
            let observerToken = await MainActor.run {
                discovery.addPeersChangedObserver { [weak self] peers in
                    guard let self else { return }
                    Task {
                        await self.handleLocalPeersChanged(peers)
                    }
                }
            }
            discoveryObserverToken = observerToken

            if let overlayDirectoryConfiguration = configuration.overlayDirectory {
                if overlayDirectory == nil {
                    overlayDirectory = await MainActor.run {
                        LoomOverlayDirectory(
                            configuration: overlayDirectoryConfiguration,
                            localDeviceID: deviceID
                        )
                    }
                }
                if let overlayDirectory {
                    let token = await MainActor.run {
                        overlayDirectory.addPeersChangedObserver { [weak self] peers in
                            guard let self else { return }
                            Task {
                                await self.handleOverlayPeersChanged(peers)
                            }
                        }
                    }
                    overlayDirectoryObserverToken = token
                }
            }

            let ports = try await node.startAuthenticatedAdvertising(
                serviceName: configuration.serviceName,
                helloProvider: { [weak self] in
                    guard let self else {
                        throw LoomStoreError.invalidConfiguration("LoomKit store is unavailable.")
                    }
                    return try await self.makeHelloRequest()
                },
                onSession: { [weak self] session in
                    guard let self else { return }
                    Task {
                        await self.acceptIncomingSession(session)
                    }
                }
            )
            listeningPorts = ports
            isRunning = true

            await MainActor.run {
                discovery.startDiscovery()
            }
            await handleLocalPeersChanged(
                await MainActor.run {
                    discovery.discoveredPeers
                }
            )
            if let overlayDirectory {
                await MainActor.run {
                    overlayDirectory.start()
                }
                await overlayDirectory.refresh()
                await handleOverlayPeersChanged(
                    await MainActor.run {
                        overlayDirectory.discoveredPeers
                    }
                )
            }
            await refreshCloudPeers()
            try await publishCurrentPeer()

            if let remoteSessionID = configuration.remoteSessionID,
               signalingClient != nil {
                try await startRemoteHosting(
                    sessionID: remoteSessionID,
                    publicHostForTCP: nil,
                    shouldNotify: false
                )
            }

            await notifyStateChanged()
        } catch {
            await record(error)
            await stop()
            throw error
        }
    }

    func stop() async {
        signalingHeartbeatTask?.cancel()
        signalingHeartbeatTask = nil

        if let currentRemoteSessionID,
           let signalingClient {
            try? await signalingClient.closePeerSession(sessionID: currentRemoteSessionID)
        }
        currentRemoteSessionID = nil
        currentPublicHostForTCP = nil
        isRemoteHosting = false

        let activeConnections = Array(connections.values)

        for managedConnection in activeConnections {
            await managedConnection.handle.disconnect()
        }

        if let signalingClient {
            let joinedSignalingSessionIDs = Set(activeConnections.compactMap(\.signalingSessionID))
            for signalingSessionID in joinedSignalingSessionIDs {
                try? await signalingClient.leaveSession(sessionID: signalingSessionID)
            }
        }

        connections.removeAll()
        connectionSnapshots.removeAll()
        transferSnapshots.removeAll()

        if let discoveryObserverToken,
           let discovery = await MainActor.run(body: { node.discovery }) {
            await MainActor.run {
                discovery.removePeersChangedObserver(discoveryObserverToken)
                discovery.stopDiscovery()
            }
        }
        self.discoveryObserverToken = nil
        if let overlayDirectoryObserverToken,
           let overlayDirectory {
            await MainActor.run {
                overlayDirectory.removePeersChangedObserver(overlayDirectoryObserverToken)
                overlayDirectory.stop()
            }
        }
        self.overlayDirectoryObserverToken = nil

        localPeersByID.removeAll()
        localPeerLastSeen.removeAll()
        overlayPeersByID.removeAll()
        overlayPeerLastSeen.removeAll()
        cloudPeersByID.removeAll()
        listeningPorts.removeAll()

        if let hostClient {
            await hostClient.stop()
            hostSnapshot = nil
            isRunning = false
            isRemoteHosting = false
            await notifyStateChanged()
            return
        }

        await node.stopAdvertising()
        isRunning = false
        await notifyStateChanged()
    }

    func refreshPeers() async {
        if let hostClient {
            do {
                try await hostClient.refreshPeers()
            } catch {
                await record(error)
            }
            return
        }
        if let discovery = await MainActor.run(body: { node.discovery }),
           isRunning {
            await MainActor.run {
                discovery.refresh()
            }
        }
        if let overlayDirectory,
           isRunning {
            await overlayDirectory.refresh()
        }
        await refreshCloudPeers()
    }

    func connect(to peerSnapshot: LoomPeerSnapshot) async throws -> LoomConnectionHandle {
        if let hostClient {
            if !isRunning {
                try await start()
            }
            let connection = try await hostClient.connect(to: peerSnapshot.id)
            return await registerConnection(
                session: connection.session,
                peerSnapshot: snapshot(fromHostRecord: connection.descriptor.peer),
                signalingSessionID: connection.descriptor.peer.signalingSessionID
            )
        }
        if !isRunning {
            try await start()
        }

        let resolvedPeer = currentPeerSnapshot(for: peerSnapshot.id) ?? peerSnapshot
        let localPeer = localPeersByID[resolvedPeer.id]
        let overlayPeer = localPeer == nil ? overlayPeersByID[resolvedPeer.id] : nil
        let signalingSessionID = LoomConnectionCoordinator.signalingFallbackSessionID(
            advertisedSignalingSessionID: resolvedPeer.signalingSessionID,
            localPeer: localPeer
        )

        guard localPeer != nil || overlayPeer != nil || signalingSessionID != nil else {
            throw LoomStoreError.peerNotFound(resolvedPeer.id)
        }

        return try await connect(
            preferredPeer: resolvedPeer,
            localPeer: localPeer,
            overlayPeer: overlayPeer,
            signalingSessionID: signalingSessionID
        )
    }

    func connect(remoteSessionID: String) async throws -> LoomConnectionHandle {
        let sessionID = remoteSessionID.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !sessionID.isEmpty else {
            throw LoomStoreError.invalidConfiguration("LoomKit remote session ID must not be empty.")
        }

        if let hostClient {
            if !isRunning {
                try await start()
            }
            let connection = try await hostClient.connect(remoteSessionID: sessionID)
            return await registerConnection(
                session: connection.session,
                peerSnapshot: snapshot(fromHostRecord: connection.descriptor.peer),
                signalingSessionID: connection.descriptor.peer.signalingSessionID
            )
        }
        if !isRunning {
            try await start()
        }

        let knownPeer = currentSnapshot().peers.first { $0.signalingSessionID == sessionID }
        return try await connect(
            preferredPeer: knownPeer,
            localPeer: nil,
            overlayPeer: nil,
            signalingSessionID: sessionID
        )
    }

    func disconnect(connectionID: UUID) async {
        guard let managedConnection = connections[connectionID] else {
            return
        }
        await managedConnection.handle.disconnect()
    }

    func startRemoteHosting(
        sessionID: String,
        publicHostForTCP: String?
    ) async throws {
        if let hostClient {
            try await hostClient.startRemoteHosting(
                sessionID: sessionID,
                publicHostForTCP: publicHostForTCP
            )
            return
        }
        try await startRemoteHosting(
            sessionID: sessionID,
            publicHostForTCP: publicHostForTCP,
            shouldNotify: true
        )
    }

    func stopRemoteHosting() async {
        if let hostClient {
            do {
                try await hostClient.stopRemoteHosting()
            } catch {
                await record(error)
            }
            return
        }
        signalingHeartbeatTask?.cancel()
        signalingHeartbeatTask = nil

        if let currentRemoteSessionID,
           let signalingClient {
            try? await signalingClient.closePeerSession(sessionID: currentRemoteSessionID)
        }

        currentRemoteSessionID = nil
        currentPublicHostForTCP = nil
        isRemoteHosting = false

        do {
            try await publishCurrentPeer()
        } catch {
            await record(error)
        }
        await notifyStateChanged()
    }

    func wake(_ peerSnapshot: LoomPeerSnapshot) async throws {
        guard let wakeOnLAN = resolveBootstrapMetadata(for: peerSnapshot)?.wakeOnLAN else {
            throw LoomStoreError.wakeOnLANUnavailable
        }
        try await wakeOnLANClient.sendMagicPacket(wakeOnLAN, retries: 2, retryDelay: .milliseconds(400))
    }

    func requestUnlock(
        _ peerSnapshot: LoomPeerSnapshot,
        username: String,
        password: String,
        sshServerTrust: LoomSSHServerTrustConfiguration
    ) async throws -> LoomBootstrapControlResult {
        guard let bootstrapMetadata = resolveBootstrapMetadata(for: peerSnapshot) else {
            throw LoomStoreError.bootstrapMetadataUnavailable
        }

        let resolvedEndpoints = LoomBootstrapEndpointResolver.resolve(bootstrapMetadata.endpoints)
        guard let bootstrapEndpoint = resolvedEndpoints.first else {
            throw LoomStoreError.bootstrapMetadataUnavailable
        }

        guard let sshPort = bootstrapMetadata.sshPort else {
            throw LoomStoreError.sshEndpointUnavailable
        }

        let sshEndpoint = LoomBootstrapEndpoint(
            host: bootstrapEndpoint.host,
            port: sshPort,
            source: bootstrapEndpoint.source
        )
        let sshResult = try await sshBootstrapClient.unlockVolumeOverSSH(
            endpoint: sshEndpoint,
            username: username,
            password: password,
            serverTrust: sshServerTrust,
            timeout: .seconds(20)
        )
        return LoomBootstrapControlResult(
            state: sshResult.unlocked ? .ready : .unavailable,
            message: sshResult.unlocked ? "SSH bootstrap completed." : "SSH bootstrap did not report a ready session."
        )
    }

    func updateConnectionState(
        id: UUID,
        state: LoomConnectionSnapshot.State,
        lastError: String?
    ) async {
        guard let existingSnapshot = connectionSnapshots[id] else {
            return
        }
        connectionSnapshots[id] = LoomConnectionSnapshot(
            id: existingSnapshot.id,
            peerID: existingSnapshot.peerID,
            peerName: existingSnapshot.peerName,
            state: state,
            transportKind: existingSnapshot.transportKind,
            connectedAt: existingSnapshot.connectedAt,
            lastError: lastError
        )
        await notifyStateChanged()
    }

    func updateTransferSnapshot(_ snapshot: LoomTransferSnapshot) async {
        transferSnapshots[snapshot.id] = snapshot
        await notifyStateChanged()
    }

    func handleConnectionDisconnected(
        id: UUID,
        errorMessage: String?
    ) async {
        if let signalingSessionID = connections[id]?.signalingSessionID,
           let signalingClient {
            try? await signalingClient.leaveSession(sessionID: signalingSessionID)
        }

        connections.removeValue(forKey: id)
        if let existingSnapshot = connectionSnapshots[id] {
            connectionSnapshots[id] = LoomConnectionSnapshot(
                id: existingSnapshot.id,
                peerID: existingSnapshot.peerID,
                peerName: existingSnapshot.peerName,
                state: errorMessage == nil ? .disconnected : .failed,
                transportKind: existingSnapshot.transportKind,
                connectedAt: existingSnapshot.connectedAt,
                lastError: errorMessage
            )
            connectionSnapshots.removeValue(forKey: id)
        }
        transferSnapshots = transferSnapshots.filter { $0.value.connectionID != id }
        await notifyStateChanged()
    }

    private func startRemoteHosting(
        sessionID: String,
        publicHostForTCP: String?,
        shouldNotify: Bool
    ) async throws {
        guard let signalingClient else {
            throw LoomStoreError.remoteSignalingUnavailable
        }
        if !isRunning {
            try await start()
        }

        let trimmedSessionID = sessionID.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedSessionID.isEmpty else {
            throw LoomStoreError.invalidConfiguration("LoomKit remote session ID must not be empty.")
        }

        let peerCandidates = await LoomDirectCandidateCollector.collect(
            configuration: await MainActor.run { node.configuration },
            listeningPorts: listeningPorts,
            publicHostForTCP: publicHostForTCP
        )
        let advertisement = try await makeAdvertisement()
        try await signalingClient.advertisePeerSession(
            sessionID: trimmedSessionID,
            peerID: deviceID,
            acceptingConnections: true,
            peerCandidates: peerCandidates,
            advertisement: advertisement
        )

        currentRemoteSessionID = trimmedSessionID
        currentPublicHostForTCP = publicHostForTCP
        isRemoteHosting = true

        signalingHeartbeatTask?.cancel()
        signalingHeartbeatTask = Task { [weak self] in
            guard let self else { return }
            await self.runSignalingHeartbeat(sessionID: trimmedSessionID)
        }

        try await publishCurrentPeer()
        if shouldNotify {
            await notifyStateChanged()
        }
    }

    private func runSignalingHeartbeat(sessionID: String) async {
        while !Task.isCancelled {
            do {
                guard let signalingClient else {
                    return
                }
                let peerCandidates = await LoomDirectCandidateCollector.collect(
                    configuration: await MainActor.run { node.configuration },
                    listeningPorts: listeningPorts,
                    publicHostForTCP: currentPublicHostForTCP
                )
                let advertisement = try await self.makeAdvertisement()
                try await signalingClient.peerHeartbeat(
                    sessionID: sessionID,
                    acceptingConnections: true,
                    peerCandidates: peerCandidates,
                    advertisement: advertisement,
                    ttlSeconds: 360
                )
                if let peerManager {
                    await peerManager.updateLastSeen()
                }
            } catch let signalingError as LoomRemoteSignalingError {
                await record(signalingError)
                if signalingError.isPermanentConfigurationFailure {
                    await stopRemoteHosting()
                    return
                }
            } catch {
                await record(error)
            }

            try? await Task.sleep(for: .seconds(30))
        }
    }

    private func publishCurrentPeer() async throws {
        guard let peerManager,
              let cloudKitManager else {
            return
        }
        let isCloudKitAvailable = await MainActor.run {
            cloudKitManager.isAvailable
        }
        guard isCloudKitAvailable else {
            return
        }

        let advertisement = try await makeAdvertisement()
        let identity = try await MainActor.run {
            try (node.identityManager ?? LoomIdentityManager.shared).currentIdentity()
        }
        try await peerManager.registerPeer(
            deviceID: deviceID,
            name: configuration.serviceName,
            advertisement: advertisement,
            identityPublicKey: identity.publicKey,
            remoteAccessEnabled: isRemoteHosting,
            signalingSessionID: currentRemoteSessionID,
            bootstrapMetadata: try await loadBootstrapMetadata()
        )
        await refreshCloudPeers()
    }

    private func refreshCloudPeers() async {
        guard let peerProvider,
              let cloudKitManager else {
            cloudPeersByID.removeAll()
            await notifyStateChanged()
            return
        }
        let isCloudKitAvailable = await MainActor.run {
            cloudKitManager.isAvailable
        }
        guard isCloudKitAvailable else {
            cloudPeersByID.removeAll()
            await notifyStateChanged()
            return
        }

        await peerProvider.fetchPeers()
        let cloudPeers = await MainActor.run {
            peerProvider.ownPeers.filter { $0.deviceID != deviceID }
        }
        cloudPeersByID = Dictionary(
            uniqueKeysWithValues: cloudPeers.map { ($0.id, $0) }
        )
        await notifyStateChanged()
    }

    private func handleLocalPeersChanged(_ peers: [LoomPeer]) async {
        let now = Date()
        localPeersByID = Dictionary(
            uniqueKeysWithValues: peers
                .filter { $0.deviceID != deviceID }
                .map { ($0.id, $0) }
        )
        localPeerLastSeen = Dictionary(
            uniqueKeysWithValues: localPeersByID.keys.map { ($0, now) }
        )
        await notifyStateChanged()
    }

    private func handleOverlayPeersChanged(_ peers: [LoomPeer]) async {
        let now = Date()
        overlayPeersByID = Dictionary(
            uniqueKeysWithValues: peers
                .filter { $0.deviceID != deviceID }
                .map { ($0.id, $0) }
        )
        overlayPeerLastSeen = Dictionary(
            uniqueKeysWithValues: overlayPeersByID.keys.map { ($0, now) }
        )
        await notifyStateChanged()
    }

    private func acceptIncomingSession(_ session: LoomAuthenticatedSession) async {
        do {
            let peerSnapshot = try await resolveConnectedPeer(
                preferredPeer: nil,
                session: session,
                signalingSessionID: nil
            )
            let handle = await registerConnection(
                session: session,
                peerSnapshot: peerSnapshot,
                signalingSessionID: nil
            )
            incomingConnectionBroadcaster.yield(handle)
            await notifyStateChanged()
        } catch {
            await record(error)
            await session.cancel()
        }
    }

    private func handleHostIncomingConnection(
        _ connection: LoomHostClientConnection
    ) async {
        let peerSnapshot = snapshot(fromHostRecord: connection.descriptor.peer)
        let handle = await registerConnection(
            session: connection.session,
            peerSnapshot: peerSnapshot,
            signalingSessionID: nil
        )
        incomingConnectionBroadcaster.yield(handle)
        await notifyStateChanged()
    }

    private func connect(
        preferredPeer: LoomPeerSnapshot?,
        localPeer: LoomPeer?,
        overlayPeer: LoomPeer?,
        signalingSessionID: String?
    ) async throws -> LoomConnectionHandle {
        let hello = try await makeHelloRequest()
        var didJoinSignaling = false

        if let signalingSessionID,
           localPeer == nil,
           overlayPeer == nil {
            guard let signalingClient else {
                throw LoomStoreError.remoteSignalingUnavailable
            }
            try await signalingClient.joinSession(sessionID: signalingSessionID)
            didJoinSignaling = true
        }

        do {
            let session = try await connectionCoordinator.connect(
                hello: hello,
                localPeer: localPeer,
                signalingSessionID: signalingSessionID
            )
            let peerSnapshot = try await resolveConnectedPeer(
                preferredPeer: preferredPeer,
                session: session,
                signalingSessionID: didJoinSignaling ? signalingSessionID : nil
            )
            return await registerConnection(
                session: session,
                peerSnapshot: peerSnapshot,
                signalingSessionID: didJoinSignaling ? signalingSessionID : nil
            )
        } catch {
            if didJoinSignaling,
               let signalingSessionID,
               let signalingClient {
                try? await signalingClient.leaveSession(sessionID: signalingSessionID)
            }
            await record(error)
            throw error
        }
    }

    private func registerConnection(
        session: any LoomSessionProtocol,
        peerSnapshot: LoomPeerSnapshot,
        signalingSessionID: String?
    ) async -> LoomConnectionHandle {
        let connectionID = UUID()
        let handle = LoomConnectionHandle(
            id: connectionID,
            peer: peerSnapshot,
            session: session,
            transferConfiguration: configuration.transferConfiguration,
            onStateChanged: { [weak self] id, state, lastError in
                guard let self else { return }
                await self.updateConnectionState(id: id, state: state, lastError: lastError)
            },
            onTransferChanged: { [weak self] snapshot in
                guard let self else { return }
                await self.updateTransferSnapshot(snapshot)
            },
            onDisconnected: { [weak self] id, errorMessage in
                guard let self else { return }
                await self.handleConnectionDisconnected(id: id, errorMessage: errorMessage)
            }
        )
        connections[connectionID] = ManagedConnection(
            handle: handle,
            signalingSessionID: signalingSessionID
        )
        connectionSnapshots[connectionID] = LoomConnectionSnapshot(
            id: connectionID,
            peerID: peerSnapshot.id,
            peerName: peerSnapshot.name,
            state: .connected,
            transportKind: await session.transportKind,
            connectedAt: Date()
        )
        await handle.startObservers()
        return handle
    }

    private func resolveConnectedPeer(
        preferredPeer: LoomPeerSnapshot?,
        session: LoomAuthenticatedSession,
        signalingSessionID: String?
    ) async throws -> LoomPeerSnapshot {
        if let preferredPeer {
            return preferredPeer
        }
        guard let sessionContext = await session.context else {
            throw LoomStoreError.invalidConfiguration("LoomKit connected without authenticated session context.")
        }
        if let currentPeerSnapshot = currentPeerSnapshot(
            for: LoomPeerID(deviceID: sessionContext.peerIdentity.deviceID)
        ) {
            return currentPeerSnapshot
        }

        return Self.fallbackPeerSnapshot(
            from: sessionContext,
            signalingSessionID: signalingSessionID
        )
    }

    nonisolated static func fallbackPeerSnapshot(
        from sessionContext: LoomAuthenticatedSessionContext,
        signalingSessionID: String?
    ) -> LoomPeerSnapshot {
        let advertisement = sessionContext.peerAdvertisement
        let peerIdentity = sessionContext.peerIdentity

        return LoomPeerSnapshot(
            id: peerIdentity.deviceID,
            name: peerIdentity.name,
            deviceType: peerIdentity.deviceType,
            sources: signalingSessionID == nil ? [] : [.remoteSignaling],
            isNearby: false,
            remoteAccessEnabled: signalingSessionID != nil,
            signalingSessionID: signalingSessionID,
            advertisement: LoomPeerAdvertisement(
                protocolVersion: advertisement.protocolVersion,
                deviceID: advertisement.deviceID ?? peerIdentity.deviceID,
                identityKeyID: advertisement.identityKeyID ?? peerIdentity.identityKeyID,
                deviceType: advertisement.deviceType ?? peerIdentity.deviceType,
                modelIdentifier: advertisement.modelIdentifier,
                iconName: advertisement.iconName,
                machineFamily: advertisement.machineFamily,
                hostName: advertisement.hostName,
                directTransports: advertisement.directTransports,
                metadata: advertisement.metadata
            ),
            bootstrapMetadata: nil,
            lastSeen: Date()
        )
    }

    private func makeHelloRequest() async throws -> LoomSessionHelloRequest {
        let bootstrapMetadata = try await loadBootstrapMetadata()
        let profile = await makeDeviceProfile(bootstrapMetadata: bootstrapMetadata)
        let identityKeyID = try await MainActor.run {
            try (node.identityManager ?? LoomIdentityManager.shared).currentIdentity().keyID
        }

        return try profile.makeHelloRequest(
            identityKeyID: identityKeyID,
            directTransports: currentDirectTransports()
        ) { metadata in
            try LoomKitMetadataCodec.addingBootstrapMetadata(
                bootstrapMetadata,
                to: metadata
            )
        }
    }

    private func makeAdvertisement() async throws -> LoomPeerAdvertisement {
        let bootstrapMetadata = try await loadBootstrapMetadata()
        let profile = await makeDeviceProfile(bootstrapMetadata: bootstrapMetadata)
        let identityKeyID = try await MainActor.run {
            try (node.identityManager ?? LoomIdentityManager.shared).currentIdentity().keyID
        }

        return try profile.makeAdvertisement(
            identityKeyID: identityKeyID,
            directTransports: currentDirectTransports()
        ) { metadata in
            try LoomKitMetadataCodec.addingBootstrapMetadata(
                bootstrapMetadata,
                to: metadata
            )
        }
    }

    private func makeDeviceProfile(
        bootstrapMetadata: LoomBootstrapMetadata?
    ) async -> LoomDeviceProfile {
        LoomDeviceProfile(
            deviceID: deviceID,
            deviceName: configuration.serviceName,
            deviceType: await Self.currentDeviceType(),
            iCloudUserID: await MainActor.run {
                cloudKitManager?.currentUserRecordID
            },
            additionalAdvertisementMetadata: configuration.advertisementMetadata,
            additionalSupportedFeatures: configuration.supportedFeatures
                + bootstrapSupportedFeatures(for: bootstrapMetadata)
        )
    }

    private func loadBootstrapMetadata() async throws -> LoomBootstrapMetadata? {
        let bootstrapMetadata = try await configuration.bootstrapMetadataProvider?()
        cachedBootstrapMetadata = bootstrapMetadata
        return bootstrapMetadata
    }

    private func bootstrapSupportedFeatures(
        for bootstrapMetadata: LoomBootstrapMetadata?
    ) -> [String] {
        guard let bootstrapMetadata,
              bootstrapMetadata.enabled else {
            return []
        }

        var features: [String] = []
        if bootstrapMetadata.wakeOnLAN != nil {
            features.append("loom.bootstrap.wake-on-lan.v1")
        }
        if bootstrapMetadata.sshPort != nil {
            features.append("loom.bootstrap.ssh-unlock.v1")
        }
        if bootstrapMetadata.supportsPreloginDaemon,
           bootstrapMetadata.controlPort != nil {
            features.append("loom.bootstrap.prelogin-control.v1")
        }
        return features
    }

    private func currentDirectTransports() -> [LoomDirectTransportAdvertisement] {
        listeningPorts.keys.sorted { $0.rawValue < $1.rawValue }.compactMap { transportKind in
            guard let port = listeningPorts[transportKind],
                  port > 0 else {
                return nil
            }
            return LoomDirectTransportAdvertisement(
                transportKind: transportKind,
                port: port
            )
        }
    }

    private func currentSnapshot() -> LoomStoreSnapshot {
        let isPublishingRemoteReachability = hostSnapshot?.isRemoteHosting ?? isRemoteHosting
        return LoomStoreSnapshot(
            peers: hostSnapshot.map { $0.peers.map(snapshot(fromHostRecord:)) } ?? mergedPeers(),
            connections: connectionSnapshots.values.sorted { lhs, rhs in
                if lhs.connectedAt != rhs.connectedAt {
                    return lhs.connectedAt > rhs.connectedAt
                }
                return lhs.peerName < rhs.peerName
            },
            transfers: transferSnapshots.values.sorted { lhs, rhs in
                if lhs.logicalName != rhs.logicalName {
                    return lhs.logicalName < rhs.logicalName
                }
                return lhs.id.uuidString < rhs.id.uuidString
            },
            isRunning: hostSnapshot?.isRunning ?? isRunning,
            isPublishingRemoteReachability: isPublishingRemoteReachability,
            localPeerCapabilities: currentLocalPeerCapabilities(
                isPublishingRemoteReachability: isPublishingRemoteReachability
            ),
            lastErrorMessage: lastErrorMessage ?? hostSnapshot?.lastErrorMessage
        )
    }

    private func currentLocalPeerCapabilities(
        isPublishingRemoteReachability: Bool
    ) -> LoomPeerCapabilities {
        let supportsNearbyDirectConnections: Bool
        if hostSnapshot != nil {
            supportsNearbyDirectConnections = hostSnapshot?.isRunning ?? false
        } else {
            supportsNearbyDirectConnections = isRunning && listeningPorts.isEmpty == false
        }

        return LoomPeerCapabilities(
            connectivity: LoomPeerConnectivityCapabilities(
                supportsNearbyDirectConnections: supportsNearbyDirectConnections,
                supportsRemoteSignalingReachability: isPublishingRemoteReachability
            ),
            bootstrap: LoomPeerCapabilities(
                advertisement: LoomPeerAdvertisement(),
                remoteAccessEnabled: false,
                signalingSessionID: nil,
                bootstrapMetadata: cachedBootstrapMetadata
            ).bootstrap
        )
    }

    private func mergedPeers() -> [LoomPeerSnapshot] {
        let peerIDs = Set(localPeersByID.keys)
            .union(overlayPeersByID.keys)
            .union(cloudPeersByID.keys)
        return peerIDs.compactMap(currentPeerSnapshot(for:)).sorted { lhs, rhs in
            if lhs.name != rhs.name {
                return lhs.name < rhs.name
            }
            return lhs.id.uuidString < rhs.id.uuidString
        }
    }

    private func currentPeerSnapshot(for peerID: LoomPeerID) -> LoomPeerSnapshot? {
        let localPeer = localPeersByID[peerID]
        let overlayPeer = overlayPeersByID[peerID]
        let cloudPeer = cloudPeersByID[peerID]
        guard localPeer != nil || overlayPeer != nil || cloudPeer != nil else {
            return nil
        }

        let deviceType = localPeer?.deviceType ?? overlayPeer?.deviceType ?? cloudPeer?.deviceType ?? .unknown
        let advertisement = localPeer?.advertisement
            ?? overlayPeer?.advertisement
            ?? cloudPeer?.advertisement
            ?? LoomPeerAdvertisement(deviceID: peerID.deviceID, deviceType: deviceType)
        let bootstrapMetadata = LoomKitMetadataCodec.bootstrapMetadata(from: advertisement)
            ?? cloudPeer?.bootstrapMetadata
        var sources: [LoomPeerSource] = []
        if localPeer != nil {
            sources.append(.nearby)
        }
        if overlayPeer != nil {
            sources.append(.overlay)
        }
        if let cloudPeer {
            sources.append(.cloudKitOwn)
            if cloudPeer.signalingSessionID != nil {
                sources.append(.remoteSignaling)
            }
        }

        let localLastSeen = localPeerLastSeen[peerID] ?? .distantPast
        let overlayLastSeen = overlayPeerLastSeen[peerID] ?? .distantPast
        let cloudLastSeen = cloudPeer?.lastSeen ?? .distantPast
        let lastSeen = max(localLastSeen, max(overlayLastSeen, cloudLastSeen))
        let name = resolvedPeerName(
            localPeer: localPeer,
            overlayPeer: overlayPeer,
            cloudPeer: cloudPeer
        )

        return LoomPeerSnapshot(
            id: peerID,
            name: name,
            deviceType: deviceType,
            sources: sources,
            isNearby: localPeer != nil,
            remoteAccessEnabled: cloudPeer?.remoteAccessEnabled ?? false,
            signalingSessionID: cloudPeer?.signalingSessionID,
            advertisement: advertisement,
            bootstrapMetadata: bootstrapMetadata,
            lastSeen: lastSeen
        )
    }

    private func resolveBootstrapMetadata(for peerSnapshot: LoomPeerSnapshot) -> LoomBootstrapMetadata? {
        currentPeerSnapshot(for: peerSnapshot.id)?.bootstrapMetadata ?? peerSnapshot.bootstrapMetadata
    }

    private func resolvedPeerName(
        localPeer: LoomPeer?,
        overlayPeer: LoomPeer?,
        cloudPeer: LoomCloudKitPeerInfo?
    ) -> String {
        if let localPeer,
           localPeer.name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
            return localPeer.name
        }
        if let overlayPeer,
           overlayPeer.name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
            return overlayPeer.name
        }
        if let cloudPeer,
           cloudPeer.name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
            return cloudPeer.name
        }
        return "Unknown Peer"
    }

    private func notifyStateChanged() async {
        snapshotBroadcaster.yield(currentSnapshot())
    }

    private func handleHostStateChanged(_ snapshot: LoomHostStateSnapshot) async {
        hostSnapshot = snapshot
        isRunning = snapshot.isRunning
        isRemoteHosting = snapshot.isRemoteHosting
        if let message = snapshot.lastErrorMessage {
            lastErrorMessage = message
        }
        await notifyStateChanged()
    }

    private func ensureHostObserversStarted() {
        guard let hostClient else {
            return
        }
        if hostStateTask == nil {
            hostStateTask = Task { [weak self] in
                guard let self else { return }
                let snapshots = await hostClient.makeStateStream()
                for await snapshot in snapshots {
                    await self.handleHostStateChanged(snapshot)
                }
            }
        }
        if hostIncomingTask == nil {
            hostIncomingTask = Task { [weak self] in
                guard let self else { return }
                let incomingConnections = await hostClient.makeIncomingConnectionStream()
                for await connection in incomingConnections {
                    await self.handleHostIncomingConnection(connection)
                }
            }
        }
    }

    private func snapshot(fromHostRecord record: LoomHostPeerRecord) -> LoomPeerSnapshot {
        LoomPeerSnapshot(
            id: record.id,
            name: record.name,
            deviceType: record.deviceType,
            sources: record.sources.map {
                switch $0 {
                case .nearby: .nearby
                case .overlay: .overlay
                case .cloudKitOwn: .cloudKitOwn
                case .remoteSignaling: .remoteSignaling
                }
            },
            isNearby: record.isNearby,
            remoteAccessEnabled: record.remoteAccessEnabled,
            signalingSessionID: record.signalingSessionID,
            advertisement: record.advertisement,
            bootstrapMetadata: record.bootstrapMetadata,
            lastSeen: record.lastSeen
        )
    }

    private func record(_ error: Error) async {
        lastErrorMessage = error.localizedDescription
        await notifyStateChanged()
    }

    private func validateConfiguration() throws {
        if configuration.serviceType.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            throw LoomStoreError.invalidConfiguration("LoomKit service type must not be empty.")
        }
        if configuration.serviceName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            throw LoomStoreError.invalidConfiguration("LoomKit service name must not be empty.")
        }
        if let overlayDirectory = configuration.overlayDirectory,
           overlayDirectory.probePort == 0 {
            throw LoomStoreError.invalidConfiguration("LoomKit overlay probe port must not be zero.")
        }
    }

    private static func currentDeviceType() async -> DeviceType {
        #if os(macOS)
        .mac
        #elseif os(iOS)
        await MainActor.run {
            UIDevice.current.userInterfaceIdiom == .pad ? .iPad : .iPhone
        }
        #elseif os(visionOS)
        .vision
        #else
        .unknown
        #endif
    }
}
