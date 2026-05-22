//
//  LoomDirectListener.swift
//  Loom
//
//  Created by Ethan Lipnik on 3/9/26.
//

import Foundation
import Network

package actor LoomDirectListener {
    private var listener: NWListener?
    private let transportKind: LoomTransportKind
    private let enablePeerToPeer: Bool
    private let quicALPN: [String]
    private let udpServiceClass: NWParameters.ServiceClass

    package init(
        transportKind: LoomTransportKind,
        enablePeerToPeer: Bool,
        quicALPN: [String] = [],
        udpServiceClass: NWParameters.ServiceClass = .interactiveVideo
    ) {
        self.transportKind = transportKind
        self.enablePeerToPeer = enablePeerToPeer
        self.quicALPN = quicALPN
        self.udpServiceClass = udpServiceClass
    }

    package func start(
        port: UInt16 = 0,
        onConnection: @escaping @Sendable (NWConnection) -> Void
    ) async throws -> UInt16 {
        let parameters = try LoomTransportParametersFactory.makeParameters(
            for: transportKind,
            enablePeerToPeer: enablePeerToPeer,
            quicALPN: quicALPN,
            udpServiceClass: udpServiceClass
        )
        let actualPort: NWEndpoint.Port = port == 0 ? .any : NWEndpoint.Port(rawValue: port) ?? .any
        parameters.allowLocalEndpointReuse = true
        listener = try NWListener(using: parameters, on: actualPort)
        listener?.newConnectionHandler = onConnection
        guard let listener else {
            throw LoomError.protocolError("Failed to create Loom direct listener.")
        }

        return try await withCheckedThrowingContinuation { continuation in
            let continuationBox = ContinuationBox<UInt16>(continuation)
            listener.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    if let port = listener.port?.rawValue {
                        continuationBox.resume(returning: port)
                    }
                case let .failed(error):
                    continuationBox.resume(throwing: error)
                case .cancelled:
                    continuationBox.resume(throwing: LoomError.protocolError("Direct listener cancelled."))
                default:
                    break
                }
            }
            listener.start(queue: .global(qos: .userInitiated))
        }
    }

    package func stop() {
        listener?.cancel()
        listener = nil
    }
}

package enum LoomTransportParametersFactory {
    package static func makeParameters(
        for transportKind: LoomTransportKind,
        enablePeerToPeer: Bool,
        requiredInterface: NWInterface? = nil,
        requiredInterfaceType: NWInterface.InterfaceType? = nil,
        quicALPN: [String] = [],
        udpServiceClass: NWParameters.ServiceClass = .interactiveVideo
    ) throws -> NWParameters {
        let parameters: NWParameters
        switch transportKind {
        case .tcp:
            parameters = NWParameters.tcp
            parameters.includePeerToPeer = enablePeerToPeer
            if let tcpOptions = parameters.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
                tcpOptions.noDelay = true
                tcpOptions.enableKeepalive = true
                tcpOptions.keepaliveInterval = 5
            }
        case .quic:
            let options = quicALPN.isEmpty
                ? NWProtocolQUIC.Options()
                : NWProtocolQUIC.Options(alpn: quicALPN)
            parameters = NWParameters(quic: options)
            parameters.includePeerToPeer = enablePeerToPeer
        case .udp:
            parameters = NWParameters.udp
            parameters.includePeerToPeer = enablePeerToPeer
            parameters.serviceClass = udpServiceClass
        }
        if let requiredInterface {
            parameters.requiredInterface = requiredInterface
        } else if let requiredInterfaceType {
            parameters.requiredInterfaceType = requiredInterfaceType
        }
        return parameters
    }
}
