//
//  LoomIncomingByteBatchDispatcher.swift
//  Loom
//
//  Created by Ethan Lipnik on 5/10/26.
//

import Foundation

final class LoomIncomingByteBatchDispatcher: @unchecked Sendable {
    private enum Delivery: @unchecked Sendable {
        case async(AsyncStream<[Data]>.Continuation)
        case immediate(@Sendable ([Data]) -> Void)
    }

    private let lock = NSLock()
    private let dispatchLock = NSRecursiveLock()
    private let maxBatchSize: Int
    private let maxDelay: Duration
    private let flushesPartialBatchesImmediately: Bool
    private let delivery: Delivery
    private let workerTask: Task<Void, Never>?
    private var bufferedPayloads: [Data] = []
    private var flushTask: Task<Void, Never>?
    private var isFinished = false

    init(
        maxBatchSize: Int,
        maxDelay: Duration,
        handler: @escaping @Sendable ([Data]) async -> Void
    ) {
        self.maxBatchSize = max(1, maxBatchSize)
        self.maxDelay = maxDelay
        flushesPartialBatchesImmediately = maxDelay == .zero
        let (stream, continuation) = AsyncStream.makeStream(of: [Data].self)
        delivery = .async(continuation)
        workerTask = Task(priority: .userInitiated) {
            for await batch in stream {
                await handler(batch)
            }
        }
        bufferedPayloads.reserveCapacity(max(1, maxBatchSize))
    }

    init(
        maxBatchSize: Int,
        immediateHandler: @escaping @Sendable ([Data]) -> Void
    ) {
        self.maxBatchSize = max(1, maxBatchSize)
        maxDelay = .zero
        flushesPartialBatchesImmediately = false
        delivery = .immediate(immediateHandler)
        workerTask = nil
        bufferedPayloads.reserveCapacity(max(1, maxBatchSize))
    }

    deinit {
        finish()
    }

    func yield(_ data: Data) {
        let batch: [Data]?
        lock.lock()
        guard !isFinished else {
            lock.unlock()
            return
        }

        bufferedPayloads.append(data)
        if bufferedPayloads.count >= maxBatchSize || flushesPartialBatchesImmediately {
            batch = takeBatchLocked()
        } else {
            batch = nil
            scheduleFlushLocked()
        }
        lock.unlock()

        if let batch {
            dispatch(batch, permitsFinishedDelivery: false)
        }
    }

    func finish() {
        let batch: [Data]
        lock.lock()
        guard !isFinished else {
            lock.unlock()
            return
        }
        isFinished = true
        flushTask?.cancel()
        flushTask = nil
        batch = takeBatchLocked()
        lock.unlock()

        if !batch.isEmpty {
            dispatch(batch, permitsFinishedDelivery: true)
        }
        if case .async(let continuation) = delivery {
            continuation.finish()
        }
    }

    private func scheduleFlushLocked() {
        guard flushTask == nil else { return }
        flushTask = Task.detached(priority: .userInitiated) { [weak self, maxDelay] in
            do {
                try await Task.sleep(for: maxDelay)
            } catch {
                return
            }
            self?.flushScheduledBatch()
        }
    }

    private func flushScheduledBatch() {
        let batch: [Data]
        lock.lock()
        guard !isFinished else {
            flushTask = nil
            lock.unlock()
            return
        }
        flushTask = nil
        batch = takeBatchLocked()
        lock.unlock()

        if !batch.isEmpty {
            dispatch(batch, permitsFinishedDelivery: false)
        }
    }

    private func takeBatchLocked() -> [Data] {
        guard !bufferedPayloads.isEmpty else { return [] }
        let batch = bufferedPayloads
        bufferedPayloads.removeAll(keepingCapacity: true)
        flushTask?.cancel()
        flushTask = nil
        return batch
    }

    private func dispatch(_ batch: [Data], permitsFinishedDelivery: Bool) {
        guard !batch.isEmpty else { return }
        dispatchLock.lock()
        lock.lock()
        let shouldDeliver = permitsFinishedDelivery || !isFinished
        lock.unlock()
        guard shouldDeliver else {
            dispatchLock.unlock()
            return
        }

        switch delivery {
        case .async(let continuation):
            continuation.yield(batch)
        case .immediate(let handler):
            handler(batch)
        }
        dispatchLock.unlock()
    }
}
