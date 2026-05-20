//
//  LoomEndpointResolverTests.swift
//  Loom
//
//  Created by Ethan Lipnik on 4/5/26.
//

@testable import Loom
import Darwin
import Network
import Testing

@Suite("Loom Endpoint Resolver")
struct LoomEndpointResolverTests {
    @Test("Local host resolution falls back to the original hostname when pre-resolution fails")
    func localHostResolutionFallsBackToOriginalHostname() async throws {
        let port: UInt16 = 61_714

        let endpoint = try await LoomEndpointResolver.resolveHostPort(
            host: "ethansmacstudio.local",
            port: port,
            resolver: { _, _ in
                throw LoomError.protocolError(
                    "Failed to resolve ethansmacstudio.local: nodename nor servname provided, or not known"
                )
            }
        )

        let expectedEndpoint: NWEndpoint = .hostPort(
            host: NWEndpoint.Host("ethansmacstudio.local"),
            port: try #require(NWEndpoint.Port(rawValue: port))
        )
        #expect(endpoint.debugDescription == expectedEndpoint.debugDescription)
    }

    @Test("Non-local hosts bypass the pre-resolver")
    func nonLocalHostsBypassPreResolver() async throws {
        let port: UInt16 = 61_714
        let endpoint = try await LoomEndpointResolver.resolveHostPort(
            host: "100.64.10.2",
            port: port,
            resolver: { _, _ in
                Issue.record("Non-local hosts should not invoke the Bonjour pre-resolver.")
                return NWEndpoint.Host("203.0.113.44")
            }
        )

        let expectedEndpoint: NWEndpoint = .hostPort(
            host: NWEndpoint.Host("100.64.10.2"),
            port: try #require(NWEndpoint.Port(rawValue: port))
        )
        #expect(endpoint.debugDescription == expectedEndpoint.debugDescription)
    }

    @Test("Peer-to-peer local hosts bypass pre-resolution")
    func peerToPeerLocalHostsBypassPreResolution() {
        #expect(
            !LoomEndpointResolver.shouldPreResolveLocalHost(
                "ethansmacstudio.local",
                enablePeerToPeer: true
            )
        )
        #expect(
            LoomEndpointResolver.shouldPreResolveLocalHost(
                "ethansmacstudio.local",
                enablePeerToPeer: false
            )
        )
    }

    @Test("Socket address conversion preserves scoped link-local IPv6 addresses")
    func socketAddressConversionPreservesScopedLinkLocalIPv6Addresses() throws {
        let loopbackIndex = if_nametoindex("lo0")
        #expect(loopbackIndex != 0)

        let address = try makeIPv6SocketAddress("fe80::1", scopeID: loopbackIndex)
        let host = try #require(LoomSocketAddressConverter.host(fromIPv6: address))
        let ipv6 = try #require(ipv6Address(from: host))

        #expect(ipv6.interface?.name == "lo0")
        #expect("\(host)".contains("%lo0"))
    }

    @Test("Socket address conversion leaves zero-scope IPv6 addresses unscoped")
    func socketAddressConversionLeavesZeroScopeIPv6AddressesUnscoped() throws {
        let address = try makeIPv6SocketAddress("fe80::1", scopeID: 0)
        let host = try #require(LoomSocketAddressConverter.host(fromIPv6: address))
        let ipv6 = try #require(ipv6Address(from: host))

        #expect(ipv6.interface == nil)
        #expect(!"\(host)".contains("%"))
    }

    @Test("Socket address conversion rejects scoped link-local IPv6 addresses with unknown scopes")
    func socketAddressConversionRejectsScopedLinkLocalIPv6AddressesWithUnknownScopes() throws {
        let address = try makeIPv6SocketAddress("fe80::1", scopeID: UInt32.max)

        #expect(LoomSocketAddressConverter.host(fromIPv6: address) == nil)
    }

    @Test("Socket address conversion can drop unknown scopes for non-link-local IPv6 addresses")
    func socketAddressConversionCanDropUnknownScopesForNonLinkLocalIPv6Addresses() throws {
        let address = try makeIPv6SocketAddress("2001:db8::1", scopeID: UInt32.max)
        let host = try #require(LoomSocketAddressConverter.host(fromIPv6: address))
        let ipv6 = try #require(ipv6Address(from: host))

        #expect(ipv6.interface == nil)
        #expect("\(host)" == "2001:db8::1")
    }

    private func makeIPv6SocketAddress(
        _ literal: String,
        scopeID: UInt32
    ) throws -> sockaddr_in6 {
        var address = sockaddr_in6()
        address.sin6_len = UInt8(MemoryLayout<sockaddr_in6>.size)
        address.sin6_family = sa_family_t(AF_INET6)
        address.sin6_scope_id = scopeID
        let conversionStatus = withUnsafeMutablePointer(to: &address.sin6_addr) { pointer in
            inet_pton(AF_INET6, literal, pointer)
        }
        #expect(conversionStatus == 1)
        return address
    }

    private func ipv6Address(from host: NWEndpoint.Host) -> IPv6Address? {
        guard case let .ipv6(address) = host else { return nil }
        return address
    }
}
