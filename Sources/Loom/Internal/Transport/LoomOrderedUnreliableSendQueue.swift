//
//  LoomOrderedUnreliableSendQueue.swift
//  Loom
//
//  Created by Ethan Lipnik on 3/30/26.
//

import Dispatch
import Foundation
import Network

package final class LoomOrderedUnreliableSendQueue: @unchecked Sendable {
    package struct Limits: Sendable, Equatable {
        let maxOutstandingPackets: Int
        let maxOutstandingBytes: Int
        let maxQueuedPackets: Int?
        let replacesQueuedSends: Bool
    }

    private struct PendingSend {
        let data: Data
        let enqueuedAt: TimeInterval
        let onComplete: @Sendable (NWError?) -> Void
    }

    package static let defaultMaxOutstandingPackets = 1024
    package static let defaultMaxOutstandingBytes = 2 * 1024 * 1024
    package static let throughputProbeMaxOutstandingPackets = 262_144
    package static let throughputProbeMaxOutstandingBytes = 512 * 1024 * 1024

    private let queue: DispatchQueue
    private let sendOperation: @Sendable (Data, @escaping @Sendable (NWError?) -> Void) -> Void
    private let maxOutstandingPackets: Int
    private let maxOutstandingBytes: Int
    private let maxQueuedPackets: Int?
    private let replacesQueuedSends: Bool
    private let diagnosticsLabel: String
    private var isClosed = false
    private var pendingSends: [PendingSend] = []
    private var outstandingPackets = 0
    private var outstandingBytes = 0
    private var diagnosticEnqueuedCount: UInt64 = 0
    private var diagnosticSentCount: UInt64 = 0
    private var diagnosticCompletedCount: UInt64 = 0
    private var diagnosticDroppedCount: UInt64 = 0
    private var diagnosticErrorCount: UInt64 = 0
    private var diagnosticPendingMax = 0
    private var diagnosticOutstandingMax = 0
    private var diagnosticQueuedBytesMax = 0
    private var diagnosticQueueDwellSamplesMs: [Double] = []
    private var diagnosticContentProcessedSamplesMs: [Double] = []
    private var diagnosticLastLogAt: TimeInterval = 0

    package static func limits(for profile: LoomQueuedUnreliableSendProfile) -> Limits {
        let recommendedLimits = profile.recommendedLimits
        return Limits(
            maxOutstandingPackets: recommendedLimits.maxOutstandingPackets,
            maxOutstandingBytes: recommendedLimits.maxOutstandingBytes,
            maxQueuedPackets: profile == .priorityInputRealtimeSequenced ? 8 : nil,
            replacesQueuedSends: profile == .priorityInputRealtime
        )
    }

    package init(
        connection: NWConnection,
        queue: DispatchQueue,
        maxOutstandingPackets: Int = defaultMaxOutstandingPackets,
        maxOutstandingBytes: Int = defaultMaxOutstandingBytes,
        maxQueuedPackets: Int? = nil,
        replacesQueuedSends: Bool = false,
        diagnosticsLabel: String = "unlabeled"
    ) {
        self.queue = queue
        self.maxOutstandingPackets = max(1, maxOutstandingPackets)
        self.maxOutstandingBytes = max(1, maxOutstandingBytes)
        self.maxQueuedPackets = maxQueuedPackets.map { max(0, $0) }
        self.replacesQueuedSends = replacesQueuedSends
        self.diagnosticsLabel = diagnosticsLabel
        sendOperation = { [connection] data, onComplete in
            connection.send(content: data, completion: .contentProcessed { error in
                onComplete(error)
            })
        }
    }

    package init(
        queue: DispatchQueue,
        maxOutstandingPackets: Int = defaultMaxOutstandingPackets,
        maxOutstandingBytes: Int = defaultMaxOutstandingBytes,
        maxQueuedPackets: Int? = nil,
        replacesQueuedSends: Bool = false,
        diagnosticsLabel: String = "unlabeled",
        sendOperation: @escaping @Sendable (Data, @escaping @Sendable (NWError?) -> Void) -> Void
    ) {
        self.queue = queue
        self.maxOutstandingPackets = max(1, maxOutstandingPackets)
        self.maxOutstandingBytes = max(1, maxOutstandingBytes)
        self.maxQueuedPackets = maxQueuedPackets.map { max(0, $0) }
        self.replacesQueuedSends = replacesQueuedSends
        self.diagnosticsLabel = diagnosticsLabel
        self.sendOperation = sendOperation
    }

    package func enqueue(_ data: Data, onComplete: @escaping @Sendable (NWError?) -> Void) {
        queue.async { [self] in
            let now = ProcessInfo.processInfo.systemUptime
            guard !isClosed else {
                onComplete(.posix(.ECANCELED))
                return
            }
            if replacesQueuedSends {
                let droppedSends = pendingSends
                pendingSends.removeAll(keepingCapacity: true)
                diagnosticDroppedCount &+= UInt64(droppedSends.count)
                droppedSends.forEach { $0.onComplete(.posix(.ECANCELED)) }
            }
            pendingSends.append(PendingSend(data: data, enqueuedAt: now, onComplete: onComplete))
            diagnosticEnqueuedCount &+= 1
            updateDiagnosticMaxima()
            trimQueuedSendsIfNeeded()
            drainIfPossible()
            logDiagnosticsIfNeeded(now: now)
        }
    }

    package func close() {
        queue.async { [self] in
            guard !isClosed else { return }
            isClosed = true
            let droppedSends = pendingSends
            pendingSends.removeAll(keepingCapacity: false)
            diagnosticDroppedCount &+= UInt64(droppedSends.count)
            droppedSends.forEach { $0.onComplete(.posix(.ECANCELED)) }
        }
    }

    private func trimQueuedSendsIfNeeded() {
        guard let maxQueuedPackets else { return }
        while pendingSends.count > maxQueuedPackets {
            let droppedSend = pendingSends.removeFirst()
            diagnosticDroppedCount &+= 1
            droppedSend.onComplete(.posix(.ECANCELED))
        }
        updateDiagnosticMaxima()
    }

    private func drainIfPossible() {
        while !pendingSends.isEmpty {
            let nextSend = pendingSends[0]
            let nextBytes = nextSend.data.count
            let packetBudgetExceeded = outstandingPackets >= maxOutstandingPackets
            let byteBudgetExceeded = outstandingPackets > 0 &&
                (outstandingBytes + nextBytes) > maxOutstandingBytes

            if packetBudgetExceeded || byteBudgetExceeded {
                return
            }

            pendingSends.removeFirst()
            outstandingPackets += 1
            outstandingBytes += nextBytes
            let sendStartedAt = ProcessInfo.processInfo.systemUptime
            diagnosticSentCount &+= 1
            recordDiagnosticSample(
                &diagnosticQueueDwellSamplesMs,
                max(0, sendStartedAt - nextSend.enqueuedAt) * 1000
            )
            updateDiagnosticMaxima()

            sendOperation(nextSend.data) { [weak self] error in
                guard let self else {
                    nextSend.onComplete(.posix(.ECANCELED))
                    return
                }
                self.queue.async {
                    let completedAt = ProcessInfo.processInfo.systemUptime
                    self.outstandingPackets = max(0, self.outstandingPackets - 1)
                    self.outstandingBytes = max(0, self.outstandingBytes - nextBytes)
                    self.diagnosticCompletedCount &+= 1
                    if error != nil {
                        self.diagnosticErrorCount &+= 1
                    }
                    self.recordDiagnosticSample(
                        &self.diagnosticContentProcessedSamplesMs,
                        max(0, completedAt - sendStartedAt) * 1000
                    )
                    self.updateDiagnosticMaxima()
                    nextSend.onComplete(error)
                    self.drainIfPossible()
                    self.logDiagnosticsIfNeeded(now: completedAt)
                }
            }
        }
    }

    package func queuedBytesSnapshot() -> Int {
        queue.sync {
            pendingSends.reduce(into: outstandingBytes) { total, pendingSend in
                total += pendingSend.data.count
            }
        }
    }

    private func updateDiagnosticMaxima() {
        diagnosticPendingMax = max(diagnosticPendingMax, pendingSends.count)
        diagnosticOutstandingMax = max(diagnosticOutstandingMax, outstandingPackets)
        diagnosticQueuedBytesMax = max(diagnosticQueuedBytesMax, queuedBytesUnsafe())
    }

    private func recordDiagnosticSample(_ samples: inout [Double], _ value: Double) {
        guard value.isFinite, value >= 0 else { return }
        samples.append(value)
        if samples.count > 256 {
            samples.removeFirst(samples.count - 256)
        }
    }

    private func logDiagnosticsIfNeeded(now: TimeInterval) {
        guard LoomLogger.isEnabled(.transport) else { return }
        if diagnosticLastLogAt == 0 {
            diagnosticLastLogAt = now
            return
        }
        guard now - diagnosticLastLogAt >= 1 else { return }
        guard diagnosticEnqueuedCount > 0 ||
            diagnosticSentCount > 0 ||
            diagnosticCompletedCount > 0 ||
            diagnosticDroppedCount > 0 ||
            diagnosticErrorCount > 0 else {
            diagnosticLastLogAt = now
            return
        }
        LoomLogger.transport(
            "Unreliable send queue diagnostics profile=\(diagnosticsLabel) " +
                "enqueued=\(diagnosticEnqueuedCount) sent=\(diagnosticSentCount) " +
                "completed=\(diagnosticCompletedCount) dropped=\(diagnosticDroppedCount) " +
                "errors=\(diagnosticErrorCount) pendingMax=\(diagnosticPendingMax) " +
                "outstandingMax=\(diagnosticOutstandingMax) queuedBytesMax=\(diagnosticQueuedBytesMax) " +
                "queueDwellP99=\(formatMs(percentile(diagnosticQueueDwellSamplesMs, 0.99)))ms " +
                "contentProcessedP99=\(formatMs(percentile(diagnosticContentProcessedSamplesMs, 0.99)))ms"
        )
        diagnosticEnqueuedCount = 0
        diagnosticSentCount = 0
        diagnosticCompletedCount = 0
        diagnosticDroppedCount = 0
        diagnosticErrorCount = 0
        diagnosticPendingMax = pendingSends.count
        diagnosticOutstandingMax = outstandingPackets
        diagnosticQueuedBytesMax = queuedBytesUnsafe()
        diagnosticQueueDwellSamplesMs.removeAll(keepingCapacity: true)
        diagnosticContentProcessedSamplesMs.removeAll(keepingCapacity: true)
        diagnosticLastLogAt = now
    }

    private func queuedBytesUnsafe() -> Int {
        pendingSends.reduce(into: outstandingBytes) { total, pendingSend in
            total += pendingSend.data.count
        }
    }

    private func percentile(_ values: [Double], _ percentile: Double) -> Double {
        guard !values.isEmpty else { return 0 }
        let sorted = values.sorted()
        let clamped = Swift.max(0, Swift.min(1, percentile))
        let index = Int((Double(sorted.count - 1) * clamped).rounded(.up))
        return sorted[Swift.min(Swift.max(0, index), sorted.count - 1)]
    }

    private func formatMs(_ value: Double) -> String {
        value.formatted(.number.precision(.fractionLength(1)))
    }
}
