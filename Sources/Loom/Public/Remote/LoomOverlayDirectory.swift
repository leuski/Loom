//
//  LoomOverlayDirectory.swift
//  Loom
//
//  Created by Ethan Lipnik on 3/11/26.
//

import Foundation
import Network
import Observation

/// Seed-driven off-LAN Loom peer directory for overlay and VPN-style networks.
@Observable
@MainActor
public final class LoomOverlayDirectory {
    /// Current peers resolved from overlay seeds.
    public private(set) var discoveredPeers: [LoomPeer] = []

    /// Whether the directory is actively refreshing seeds.
    public private(set) var isSearching = false

    /// Optional local device identifier used to filter self from directory output.
    public var localDeviceID: UUID?

    /// Callback invoked whenever the overlay peer set changes.
    public var onPeersChanged: (([LoomPeer]) -> Void)?

    private let configuration: LoomOverlayDirectoryConfiguration
    private var peersChangedObservers: [UUID: ([LoomPeer]) -> Void] = [:]
    private var refreshTask: Task<Void, Never>?
    private var refreshProcessorTask: Task<Void, Never>?
    private var requestedRefreshGeneration = 0
    private var completedRefreshGeneration = 0
    private var lastPublishedPeerSnapshots: [PublishedPeerSnapshot] = []

    public init(
        configuration: LoomOverlayDirectoryConfiguration,
        localDeviceID: UUID? = nil
    ) {
        self.configuration = configuration
        self.localDeviceID = localDeviceID
    }

    /// Start polling seeds and probing overlay hosts.
    public func start() {
        guard refreshTask == nil else {
            return
        }
        isSearching = true
        refreshTask = Task { [weak self] in
            guard let self else { return }
            await self.runRefreshLoop()
        }
    }

    /// Stop polling and clear the current peer set.
    public func stop() {
        refreshTask?.cancel()
        refreshTask = nil
        isSearching = false
        publishDiscoveredPeers([])
    }

    /// Force an immediate seed refresh.
    public func refresh() async {
        await requestRefresh()
    }

    /// Registers an observer that is invoked whenever discovered peers change.
    @discardableResult
    public func addPeersChangedObserver(_ observer: @escaping ([LoomPeer]) -> Void) -> UUID {
        let token = UUID()
        peersChangedObservers[token] = observer
        return token
    }

    /// Removes a previously-registered peer-change observer.
    public func removePeersChangedObserver(_ token: UUID) {
        peersChangedObservers.removeValue(forKey: token)
    }

    private func runRefreshLoop() async {
        await requestRefresh()
        while !Task.isCancelled {
            do {
                try await Task.sleep(for: configuration.refreshInterval)
            } catch {
                break
            }
            await requestRefresh()
        }
    }

    private func requestRefresh() async {
        requestedRefreshGeneration += 1
        let targetGeneration = requestedRefreshGeneration
        startRefreshProcessorIfNeeded()

        while completedRefreshGeneration < targetGeneration {
            guard let refreshProcessorTask else { return }
            await refreshProcessorTask.value
        }
    }

    private func startRefreshProcessorIfNeeded() {
        guard refreshProcessorTask == nil else { return }

        refreshProcessorTask = Task { @MainActor [weak self] in
            guard let self else { return }
            await self.runRefreshProcessor()
        }
    }

    private func runRefreshProcessor() async {
        defer {
            refreshProcessorTask = nil
            isSearching = refreshTask != nil
        }

        while completedRefreshGeneration < requestedRefreshGeneration {
            let generation = requestedRefreshGeneration
            await refreshNow(generation: generation)
            completedRefreshGeneration = max(completedRefreshGeneration, generation)
        }
    }

    private func refreshNow(generation: Int) async {
        isSearching = true
        do {
            let seeds = try await configuration.seedProvider()
            LoomLogger.debug(
                .transport,
                "Overlay directory refresh \(generation) started: seeds=\(seeds.count) attempts=\(configuration.probeAttempts)"
            )
            let candidates = await Self.probeCandidates(
                for: seeds,
                configuration: configuration
            )
            publishDiscoveredPeers(resolvePeers(from: candidates))
            isSearching = refreshTask != nil
            LoomLogger.debug(
                .transport,
                "Overlay directory refresh \(generation) completed: candidates=\(candidates.count) peers=\(discoveredPeers.count)"
            )
        } catch {
            publishDiscoveredPeers([])
            isSearching = refreshTask != nil
            LoomLogger.debug(
                .transport,
                "Overlay directory refresh \(generation) failed: \(error.localizedDescription)"
            )
        }
    }

    private func resolvePeers(
        from candidates: [LoomOverlayDirectoryCandidate]
    ) -> [LoomPeer] {
        let candidatesByDeviceID = Dictionary(grouping: candidates, by: \.deviceID)

        var resolvedPeers: [LoomPeer] = []
        for (deviceID, deviceCandidates) in candidatesByDeviceID {
            guard deviceID != localDeviceID,
                  let preferredCandidate = deviceCandidates.min(by: Self.isPreferredCandidate(_:_:))
            else {
                continue
            }

            let projections = LoomHostCatalogCodec.projections(
                peerName: preferredCandidate.name,
                advertisement: preferredCandidate.advertisement
            )
            for projection in projections {
                resolvedPeers.append(
                    LoomPeer(
                        id: projection.peerID,
                        name: projection.displayName,
                        deviceType: preferredCandidate.deviceType,
                        endpoint: Self.endpoint(
                            host: preferredCandidate.host,
                            advertisement: projection.advertisement
                        ),
                        advertisement: projection.advertisement
                    )
                )
            }
        }

        return resolvedPeers.sorted { lhs, rhs in
            if lhs.name != rhs.name {
                return lhs.name < rhs.name
            }
            return lhs.id.rawValue < rhs.id.rawValue
        }
    }

    private static func probeCandidates(
        for seeds: [LoomOverlaySeed],
        configuration: LoomOverlayDirectoryConfiguration
    ) async -> [LoomOverlayDirectoryCandidate] {
        await withTaskGroup(of: LoomOverlayDirectoryCandidate?.self) { group in
            for seed in seeds where seed.host.isEmpty == false {
                group.addTask {
                    await probeCandidate(seed: seed, configuration: configuration)
                }
            }

            var candidates: [LoomOverlayDirectoryCandidate] = []
            for await candidate in group {
                if let candidate {
                    candidates.append(candidate)
                }
            }
            return candidates
        }
    }

    private static func probeCandidate(
        seed: LoomOverlaySeed,
        configuration: LoomOverlayDirectoryConfiguration
    ) async -> LoomOverlayDirectoryCandidate? {
        for attempt in 1...configuration.probeAttempts {
            do {
                let response = try await LoomOverlayProbeClient.probe(
                    seed: seed,
                    defaultPort: configuration.probePort,
                    timeout: configuration.probeTimeout
                )
                guard let deviceID = response.advertisement.deviceID,
                      !response.advertisement.directTransports.isEmpty else {
                    LoomLogger.debug(
                        .transport,
                        "Overlay seed \(seed.host) ignored on attempt \(attempt): incomplete advertisement"
                    )
                    return nil
                }
                if attempt > 1 {
                    LoomLogger.debug(.transport, "Overlay seed \(seed.host) succeeded on attempt \(attempt)")
                }
                return LoomOverlayDirectoryCandidate(
                    deviceID: deviceID,
                    host: seed.host,
                    name: response.name,
                    deviceType: response.deviceType,
                    advertisement: response.advertisement
                )
            } catch {
                if attempt >= configuration.probeAttempts {
                    LoomLogger.debug(
                        .transport,
                        "Overlay seed \(seed.host) failed after \(attempt) attempt(s): \(error.localizedDescription)"
                    )
                    return nil
                }

                LoomLogger.debug(
                    .transport,
                    "Overlay seed \(seed.host) failed attempt \(attempt); retrying: \(error.localizedDescription)"
                )
                do {
                    try await Task.sleep(for: configuration.probeRetryDelay)
                } catch {
                    return nil
                }
            }
        }

        return nil
    }

    private static func endpoint(
        host: String,
        advertisement: LoomPeerAdvertisement
    ) -> NWEndpoint {
        let preferredTransport = advertisement.directTransports.min(by: isPreferredTransport(_:_:))
        let endpointPort = NWEndpoint.Port(rawValue: preferredTransport?.port ?? 0) ?? .any
        return .hostPort(
            host: .init(host),
            port: endpointPort
        )
    }

    private static func isPreferredCandidate(
        _ lhs: LoomOverlayDirectoryCandidate,
        _ rhs: LoomOverlayDirectoryCandidate
    ) -> Bool {
        let leftHasQUIC = lhs.advertisement.directTransports.contains { $0.transportKind == .quic }
        let rightHasQUIC = rhs.advertisement.directTransports.contains { $0.transportKind == .quic }
        if leftHasQUIC != rightHasQUIC {
            return leftHasQUIC
        }
        return lhs.host < rhs.host
    }

    private static func isPreferredTransport(
        _ lhs: LoomDirectTransportAdvertisement,
        _ rhs: LoomDirectTransportAdvertisement
    ) -> Bool {
        transportRank(lhs.transportKind) < transportRank(rhs.transportKind)
    }

    private static func transportRank(_ transportKind: LoomTransportKind) -> Int {
        switch transportKind {
        case .udp:
            0
        case .quic:
            1
        case .tcp:
            2
        }
    }

    private func publishDiscoveredPeers(_ peers: [LoomPeer]) {
        let snapshots = peers.map(PublishedPeerSnapshot.init)
        guard snapshots != lastPublishedPeerSnapshots else { return }

        discoveredPeers = peers
        lastPublishedPeerSnapshots = snapshots
        onPeersChanged?(peers)
        for observer in peersChangedObservers.values {
            observer(peers)
        }
    }
}

private struct PublishedPeerSnapshot: Equatable {
    let id: LoomPeerID
    let name: String
    let deviceType: DeviceType
    let endpoint: NWEndpoint
    let advertisement: LoomPeerAdvertisement
    let resolvedAddresses: [NWEndpoint.Host]

    init(_ peer: LoomPeer) {
        id = peer.id
        name = peer.name
        deviceType = peer.deviceType
        endpoint = peer.endpoint
        advertisement = peer.advertisement
        resolvedAddresses = peer.resolvedAddresses
    }
}

private struct LoomOverlayDirectoryCandidate: Sendable {
    let deviceID: UUID
    let host: String
    let name: String
    let deviceType: DeviceType
    let advertisement: LoomPeerAdvertisement
}
