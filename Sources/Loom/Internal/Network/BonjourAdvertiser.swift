//
//  BonjourAdvertiser.swift
//  Loom
//
//  Created by Ethan Lipnik on 1/2/26.
//

import Foundation
import Network

/// Advertises a Loom peer service via Bonjour.
///
/// This listener uses TCP parameters solely for Bonjour service registration
/// and macOS local network permission grants. No TCP connections are ever
/// accepted or established through it. Actual sessions are handled by a
/// separate ``LoomDirectListener`` configured for UDP.
///
/// The reason TCP is used here: `NWConnection` cannot resolve Bonjour service
/// endpoints whose type ends in `_udp` — the connection times out without
/// completing DNS-SD resolution. Until Apple's Network framework supports
/// `_udp` service endpoint resolution, the Bonjour advertisement must use a
/// `_tcp` service type so that clients can discover the host. Clients read
/// the UDP port from the TXT record and connect directly via
/// `NWEndpoint.hostPort`.
actor BonjourAdvertiser {
    private var listener: NWListener?
    private let serviceType: String
    private let serviceName: String
    private var advertisement: LoomPeerAdvertisement
    private let enablePeerToPeer: Bool

    private var isAdvertising = false

    init(
        serviceName: String,
        advertisement: LoomPeerAdvertisement = LoomPeerAdvertisement(),
        serviceType: String = Loom.serviceType,
        enablePeerToPeer: Bool = true
    ) {
        self.serviceName = serviceName
        self.advertisement = advertisement
        self.serviceType = serviceType
        self.enablePeerToPeer = enablePeerToPeer
    }

    /// Start advertising the service
    func start(port: UInt16 = 0, onConnection: @escaping @Sendable (NWConnection) -> Void) async throws -> UInt16 {
        guard !isAdvertising else { throw LoomError.alreadyAdvertising }

        validateBonjourInfoPlistKeys(serviceType: serviceType)

        // TCP listener for Bonjour service registration only — enables discovery
        // and local network permissions. Actual sessions use the separate UDP listener.
        // TODO: Investigate using a UDP Bonjour listener once NWConnection supports
        // resolving _udp service endpoints (rdar://FB...).
        let parameters = Self.makeAdvertiserParameters(enablePeerToPeer: enablePeerToPeer)

        let actualPort: NWEndpoint.Port = port == 0 ? .any : NWEndpoint.Port(rawValue: port)!
        parameters.allowLocalEndpointReuse = true

        listener = try NWListener(using: parameters, on: actualPort)

        // Configure Bonjour advertisement with TXT record
        let txtRecord = NWTXTRecord(advertisement.toTXTRecord())
        listener?.service = NWListener.Service(
            name: serviceName,
            type: serviceType,
            txtRecord: txtRecord
        )

        // Set connection handler BEFORE starting the listener
        listener?.newConnectionHandler = onConnection

        // Capture listener reference for the closure
        guard let listener else { throw LoomError.protocolError("Failed to create listener") }

        return try await withCheckedThrowingContinuation { continuation in
            let continuationBox = ContinuationBox<UInt16>(continuation)

            listener.stateUpdateHandler = { [weak self, continuationBox] state in
                LoomLogger.discovery("Advertiser state: \(state)")
                switch state {
                case .ready:
                    if let port = listener.port?.rawValue {
                        Task { await self?.setAdvertising(true) }
                        continuationBox.resume(returning: port)
                    }
                case let .failed(error):
                    Task { await self?.setAdvertising(false) }
                    continuationBox.resume(throwing: error)
                case let .waiting(error):
                    LoomLogger.discovery("Advertiser waiting: \(error)")
                case .cancelled:
                    Task { await self?.setAdvertising(false) }
                    continuationBox.resume(throwing: LoomError.protocolError("Listener cancelled"))
                default:
                    break
                }
            }

            listener.start(queue: .global(qos: .userInteractive))
        }
    }

    package static func makeAdvertiserParameters(enablePeerToPeer: Bool) -> NWParameters {
        let parameters = NWParameters.tcp
        parameters.serviceClass = .interactiveVideo
        parameters.includePeerToPeer = enablePeerToPeer

        if let tcpOptions = parameters.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
            tcpOptions.noDelay = true
            tcpOptions.enableKeepalive = true
            // Detect dead peers within ~25s of idle.
            // macOS defaults are far too lenient for LAN P2P
            // (keepaliveIdle defaults to 7200s = 2 hours), which
            // leaves half-open sockets undetected effectively
            // forever. Override all three to bound detection.
            tcpOptions.keepaliveIdle = 10        // seconds of idle before first probe
            tcpOptions.keepaliveInterval = 5     // seconds between probes
            tcpOptions.keepaliveCount = 3        // probes before declaring dead
        }

        return parameters
    }

    private func setAdvertising(_ value: Bool) {
        isAdvertising = value
    }

    /// Stop advertising
    func stop() {
        listener?.cancel()
        listener = nil
        isAdvertising = false
    }

    /// Update TXT record with a new advertisement payload.
    func updateAdvertisement(_ advertisement: LoomPeerAdvertisement) {
        self.advertisement = advertisement
        let txtRecord = NWTXTRecord(advertisement.toTXTRecord())
        listener?.service = NWListener.Service(
            name: serviceName,
            type: serviceType,
            txtRecord: txtRecord
        )
    }

    var port: UInt16? { listener?.port?.rawValue }

    func currentAdvertisement() -> LoomPeerAdvertisement {
        advertisement
    }
}
