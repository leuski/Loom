//
//  BonjourEntitlementCheck.swift
//  Loom
//
//  Created by Ethan Lipnik on 3/17/26.
//

import Foundation

/// Validates that the host app's Info.plist contains the required keys for
/// Bonjour discovery and advertising to work. Fires `assertionFailure` in
/// debug builds so developers see a clear message instead of the opaque
/// NWBrowser `-65555 (NoAuth)` error.
func validateBonjourInfoPlistKeys(serviceType: String) {
    #if DEBUG
    guard !isRunningInSwiftPMTestBundle() else {
        return
    }

    let info = Bundle.main.infoDictionary

    if let services = info?["NSBonjourServices"] as? [String] {
        if !services.contains(serviceType) {
            assertionFailure(
                """
                Loom: Your app's Info.plist declares NSBonjourServices but does \
                not include "\(serviceType)". Add it to the array so the system \
                authorizes Bonjour operations for this service type.

                <key>NSBonjourServices</key>
                <array>
                    <string>\(serviceType)</string>
                </array>
                """
            )
        }
    } else {
        assertionFailure(
            """
            Loom: Your app's Info.plist is missing NSBonjourServices. Without \
            this key the system denies Bonjour discovery and advertising with \
            error -65555 (NoAuth).

            Add the following to your Info.plist:

            <key>NSBonjourServices</key>
            <array>
                <string>\(serviceType)</string>
            </array>
            """
        )
    }

    if info?["NSLocalNetworkUsageDescription"] == nil {
        assertionFailure(
            """
            Loom: Your app's Info.plist is missing NSLocalNetworkUsageDescription. \
            Without this key the system cannot present the local network permission \
            prompt and Bonjour operations will fail with error -65555 (NoAuth).

            Add a user-facing description, for example:

            <key>NSLocalNetworkUsageDescription</key>
            <string>This app uses the local network to discover and connect to nearby devices.</string>
            """
        )
    }
    #endif
}

#if DEBUG
private func isRunningInSwiftPMTestBundle() -> Bool {
    let processInfo = ProcessInfo.processInfo
    if processInfo.environment["XCTestConfigurationFilePath"] != nil {
        return true
    }

    var bundlePaths = [
        Bundle.main.bundlePath,
        Bundle.main.bundleURL.path,
    ]
    if let executablePath = Bundle.main.executableURL?.path {
        bundlePaths.append(executablePath)
    }
    if bundlePaths.contains(where: { $0.contains(".xctest") }) {
        return true
    }

    return processInfo.arguments.contains { argument in
        argument.contains(".xctest") ||
            argument == "--testing-library" ||
            argument == "swift-testing"
    }
}
#endif
