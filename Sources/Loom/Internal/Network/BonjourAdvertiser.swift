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

    /// True once the initial bind has reached `.ready` at least once.
    /// Used to distinguish startup-time `.failed` (caller awaits the
    /// continuation; throw to them) from post-startup `.failed` (the
    /// caller has long since returned; trigger self-recovery).
    private var hasEverBecomeReady = false

    /// Captured connection handler from `start(...)`. Held so the
    /// recovery path can rebuild the listener with the same hook
    /// without forcing the caller to call `start` again.
    private var onConnection: (@Sendable (NWConnection) -> Void)?

    /// `true` while a caller-initiated `stop()` is shutting us down.
    /// Suppresses recovery so an explicit stop doesn't immediately
    /// spawn a new listener.
    private var isStoppingByCaller = false

    /// Pending recovery task. Cancelled by `stop()` and by start of
    /// each new attempt so we never have more than one in flight.
    private var recoveryTask: Task<Void, Never>?

    /// Count of consecutive failures driving the recovery backoff.
    /// Reset to 0 whenever the listener reaches `.ready`.
    private var recoveryAttempts: Int = 0

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

    /// Start advertising the service.
    ///
    /// After the initial bind reaches `.ready`, the advertiser
    /// self-heals from subsequent `.failed` state transitions by
    /// tearing the listener down and recreating it, with bounded
    /// exponential backoff. This protects against transient network
    /// changes (Wi-Fi association drops, AWDL cycles) that would
    /// otherwise leave the advertiser silently dead for the lifetime
    /// of the process.
    func start(port: UInt16 = 0, onConnection: @escaping @Sendable (NWConnection) -> Void) async throws -> UInt16 {
        guard !isAdvertising else { throw LoomError.alreadyAdvertising }

        validateBonjourInfoPlistKeys(serviceType: serviceType)

        // Reset recovery state for a fresh start.
        isStoppingByCaller = false
        hasEverBecomeReady = false
        recoveryAttempts = 0
        self.onConnection = onConnection

        return try await bindListener(port: port)
    }

    /// Internal bind step shared by `start()` and the recovery path.
    /// Constructs a fresh `NWListener` with the same parameters/TXT/
    /// connection hook and awaits the first state transition.
    /// Startup `.failed` throws to the caller; post-startup state
    /// transitions are handled by the listener's stateUpdateHandler
    /// without resuming the continuation again.
    private func bindListener(port: UInt16) async throws -> UInt16 {
        // TCP listener for Bonjour service registration only — enables discovery
        // and local network permissions. Actual sessions use the separate UDP listener.
        // TODO: Investigate using a UDP Bonjour listener once NWConnection supports
        // resolving _udp service endpoints (rdar://FB...).
        let parameters = Self.makeAdvertiserParameters(enablePeerToPeer: enablePeerToPeer)

        let actualPort: NWEndpoint.Port = port == 0 ? .any : NWEndpoint.Port(rawValue: port)!
        parameters.allowLocalEndpointReuse = true

        let newListener = try NWListener(using: parameters, on: actualPort)

        // Configure Bonjour advertisement with TXT record
        let txtRecord = NWTXTRecord(advertisement.toTXTRecord())
        newListener.service = NWListener.Service(
            name: serviceName,
            type: serviceType,
            txtRecord: txtRecord
        )

        // Set connection handler BEFORE starting the listener.
        // Captured at `start()` time so recovery doesn't need a
        // caller-supplied handler.
        if let onConnection {
            newListener.newConnectionHandler = onConnection
        }

        listener = newListener

        return try await withCheckedThrowingContinuation { continuation in
            let continuationBox = ContinuationBox<UInt16>(continuation)

            newListener.stateUpdateHandler = { [weak self, continuationBox] state in
                LoomLogger.discovery("Advertiser state: \(state)")
                switch state {
                case .ready:
                    if let port = newListener.port?.rawValue {
                        // `resume(returning:)` is a no-op after the
                        // first call, so subsequent recovery `.ready`
                        // events do not double-resume.
                        continuationBox.resume(returning: port)
                        Task { await self?.handleReady(port: port) }
                    }
                case let .failed(error):
                    // First-time `.failed` throws to the awaiting
                    // caller. Post-startup `.failed` schedules a
                    // recovery (handleFailed checks isReady to decide).
                    continuationBox.resume(throwing: error)
                    Task { await self?.handleFailed(error: error) }
                case let .waiting(error):
                    LoomLogger.discovery("Advertiser waiting: \(error)")
                case .cancelled:
                    continuationBox.resume(
                        throwing: LoomError.protocolError("Listener cancelled"))
                    Task { await self?.handleCancelled() }
                default:
                    break
                }
            }

            newListener.start(queue: .global(qos: .userInteractive))
        }
    }

    private func handleReady(port: UInt16) {
        isAdvertising = true
        recoveryAttempts = 0
        recoveryTask?.cancel()
        recoveryTask = nil
        if hasEverBecomeReady {
            LoomLogger.discovery(
                "Advertiser recovered after \(recoveryAttempts) attempt(s) on port \(port)")
        }
        hasEverBecomeReady = true
    }

    private func handleFailed(error: Error) {
        isAdvertising = false
        // Startup failure: the caller is being thrown to by the
        // continuation; do not schedule recovery — let the caller
        // decide whether to retry.
        guard hasEverBecomeReady else { return }
        guard !isStoppingByCaller else { return }
        LoomLogger.discovery(
            "Advertiser failed post-startup: \(error). Scheduling recovery.")
        scheduleRecovery()
    }

    private func handleCancelled() {
        isAdvertising = false
        // A cancellation reached after the caller has returned is
        // either our own stop() or a system-initiated drop; in the
        // latter case the same recovery policy applies.
        guard hasEverBecomeReady else { return }
        guard !isStoppingByCaller else { return }
        LoomLogger.discovery(
            "Advertiser cancelled post-startup. Scheduling recovery.")
        scheduleRecovery()
    }

    private func scheduleRecovery() {
        // Only one in-flight at a time. We increment attempts inside
        // performRecovery so a backoff burst doesn't lose count if
        // multiple state events arrive before the timer fires.
        guard recoveryTask == nil else { return }
        let attempt = recoveryAttempts + 1
        let delay = Self.recoveryDelaySeconds(forAttempt: attempt)
        LoomLogger.discovery(
            "Advertiser recovery scheduled (attempt \(attempt), delay \(delay)s)")
        recoveryTask = Task { [weak self] in
            try? await Task.sleep(
                nanoseconds: UInt64(delay * 1_000_000_000))
            if Task.isCancelled { return }
            await self?.performRecovery(attempt: attempt)
        }
    }

    private func performRecovery(attempt: Int) async {
        recoveryTask = nil
        guard !isStoppingByCaller else { return }
        recoveryAttempts = attempt
        // Drop any stale listener before recreating. `bindListener`
        // replaces `self.listener`, but cancelling first ensures the
        // previous one's resources are released.
        listener?.cancel()
        listener = nil
        do {
            _ = try await bindListener(port: 0)
        } catch {
            LoomLogger.discovery(
                "Advertiser recovery attempt \(attempt) failed during bind: \(error)")
            scheduleRecovery()
        }
    }

    /// Bounded exponential backoff: 1, 2, 4, 8, 16, 30, 30, …
    /// (seconds). Public so a unit test can pin the schedule without
    /// having to drive a real NWListener.
    package static func recoveryDelaySeconds(forAttempt attempt: Int) -> Double {
        guard attempt > 0 else { return 0 }
        let cap = 30.0
        let exp = Double(min(attempt - 1, 5))
        return min(pow(2.0, exp), cap)
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

    /// Stop advertising. Cancels any in-flight recovery so an
    /// explicit stop is final.
    func stop() {
        isStoppingByCaller = true
        recoveryTask?.cancel()
        recoveryTask = nil
        listener?.cancel()
        listener = nil
        isAdvertising = false
        hasEverBecomeReady = false
        onConnection = nil
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
