/*
 * Copyright 2026 DDS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dds.nifi.routendjson.expression;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;

/** Utility methods for literal IP and CIDR matching used by JEL. */
public final class IpCidr {
    private IpCidr() { }

    public static boolean isIp(final String value) {
        return isIpv4(value) || isIpv6(value);
    }

    public static boolean isIpv4(final String value) {
        return parseIpv4(value) != null;
    }

    public static boolean isIpv6(final String value) {
        return parseIpv6(value) != null;
    }

    public static CidrRange parseCidrOrThrow(final String cidr) {
        return parseCidr(cidr);
    }

    public static CidrRange parseCidr(final String cidr) {
        if (cidr == null || cidr.isBlank()) {
            throw new IllegalArgumentException("CIDR value is required");
        }
        final String text = cidr.trim();
        final int slash = text.indexOf('/');
        if (slash <= 0 || slash == text.length() - 1 || text.indexOf('/', slash + 1) >= 0) {
            throw new IllegalArgumentException("CIDR must be in address/prefix form: " + cidr);
        }

        final String addressText = text.substring(0, slash).trim();
        final String prefixText = text.substring(slash + 1).trim();
        final int prefix;
        try {
            prefix = Integer.parseInt(prefixText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid CIDR prefix in " + cidr, e);
        }

        final byte[] ipv4 = parseIpv4(addressText);
        if (ipv4 != null) {
            if (prefix < 0 || prefix > 32) {
                throw new IllegalArgumentException("IPv4 CIDR prefix must be between 0 and 32: " + cidr);
            }
            return new CidrRange(mask(ipv4, prefix), prefix, 4, cidr);
        }

        final byte[] ipv6 = parseIpv6(addressText);
        if (ipv6 != null) {
            if (prefix < 0 || prefix > 128) {
                throw new IllegalArgumentException("IPv6 CIDR prefix must be between 0 and 128: " + cidr);
            }
            return new CidrRange(mask(ipv6, prefix), prefix, 16, cidr);
        }

        throw new IllegalArgumentException("Invalid IP address in CIDR: " + cidr);
    }

    private static byte[] parseIp(final String value) {
        final byte[] ipv4 = parseIpv4(value);
        if (ipv4 != null) {
            return ipv4;
        }
        return parseIpv6(value);
    }

    private static byte[] parseIpv4(final String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        final String text = value.trim();
        final String[] parts = text.split("\\.", -1);
        if (parts.length != 4) {
            return null;
        }
        final byte[] out = new byte[4];
        for (int i = 0; i < parts.length; i++) {
            final String part = parts[i];
            if (part.isEmpty() || part.length() > 3) {
                return null;
            }
            int n = 0;
            for (int j = 0; j < part.length(); j++) {
                final char c = part.charAt(j);
                if (c < '0' || c > '9') {
                    return null;
                }
                n = (n * 10) + (c - '0');
            }
            if (n > 255) {
                return null;
            }
            out[i] = (byte) n;
        }
        return out;
    }

    private static byte[] parseIpv6(final String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        final String text = value.trim();
        if (!text.contains(":")) {
            return null;
        }
        try {
            final InetAddress address = InetAddress.getByName(text);
            final byte[] bytes = address.getAddress();
            return bytes.length == 16 ? bytes : null;
        } catch (UnknownHostException e) {
            return null;
        }
    }

    private static byte[] mask(final byte[] bytes, final int prefix) {
        final byte[] masked = Arrays.copyOf(bytes, bytes.length);
        int remaining = prefix;
        for (int i = 0; i < masked.length; i++) {
            if (remaining >= 8) {
                remaining -= 8;
                continue;
            }
            if (remaining <= 0) {
                masked[i] = 0;
            } else {
                final int mask = (0xFF << (8 - remaining)) & 0xFF;
                masked[i] = (byte) (masked[i] & mask);
                remaining = 0;
            }
        }
        return masked;
    }

    public static final class CidrRange {
        private final byte[] network;
        private final int prefix;
        private final int addressLength;
        private final String source;

        private CidrRange(final byte[] network, final int prefix, final int addressLength, final String source) {
            this.network = Objects.requireNonNull(network);
            this.prefix = prefix;
            this.addressLength = addressLength;
            this.source = source;
        }

        public boolean contains(final String ip) {
            final byte[] address = parseIp(ip);
            if (address == null || address.length != addressLength) {
                return false;
            }
            return Arrays.equals(network, mask(address, prefix));
        }

        @Override
        public String toString() {
            return source;
        }
    }
}
