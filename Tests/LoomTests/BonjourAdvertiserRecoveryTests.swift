//
//  BonjourAdvertiserRecoveryTests.swift
//  Loom
//
//  Verifies the bounded exponential backoff used by
//  `BonjourAdvertiser` when recovering from post-startup `.failed`
//  state transitions. The recovery driver itself is harder to unit
//  test because `NWListener` can't be cleanly mocked — this suite
//  pins the deterministic part (the schedule) so behavior can't
//  silently drift.
//

@testable import Loom
import Testing

@Suite("BonjourAdvertiser recovery backoff schedule")
struct BonjourAdvertiserRecoveryTests {
    @Test("attempt 0 returns 0 seconds (no recovery scheduled)")
    func attemptZeroIsImmediate() {
        let delay = BonjourAdvertiser.recoveryDelaySeconds(forAttempt: 0)
        #expect(delay == 0)
    }

    @Test(
        "attempts 1..6 follow 1, 2, 4, 8, 16, 30 seconds",
        arguments: [
            (1, 1.0),
            (2, 2.0),
            (3, 4.0),
            (4, 8.0),
            (5, 16.0),
            (6, 30.0),
        ]
    )
    func exponentialSchedule(_ attempt: Int, _ expected: Double) {
        let delay = BonjourAdvertiser.recoveryDelaySeconds(forAttempt: attempt)
        #expect(delay == expected)
    }

    @Test("schedule caps at 30 seconds for high attempts")
    func capped() {
        for attempt in 7...100 {
            let delay = BonjourAdvertiser.recoveryDelaySeconds(forAttempt: attempt)
            #expect(delay == 30.0, "attempt \(attempt) should be capped")
        }
    }
}
