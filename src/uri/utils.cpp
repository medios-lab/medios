// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __WINDOWS__
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif // __WINDOWS__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <ostream>
#include <string>
#include <vector>

#include <stout/os/getenv.hpp>
#include <stout/os/realpath.hpp>
#include <stout/strings.hpp>

#include <medios/uri/uri.hpp>

#include "uri/utils.hpp"

using std::ostream;
using std::string;
using std::vector;

namespace medios {

ostream& operator<<(ostream& stream, const URI& uri)
{
  stream << uri.scheme() << ":";

  // The 'authority' part.
  if (uri.has_host()) {
    stream << "//";

    if (uri.has_user()) {
      stream << uri.user();

      if (uri.has_password()) {
        stream << ":" << uri.password();
      }

      stream << "@";
    }

    stream << uri.host();

    if (uri.has_port()) {
      stream << ":" << uri.port();
    }
  }

  // The 'path' part.
  stream << uri.path();

  // The 'query' part.
  if (uri.has_query()) {
    stream << "?" << uri.query();
  }

  // The 'fragment' part.
  if (uri.has_fragment()) {
    stream << "#" << uri.fragment();
  }

  return stream;
}

namespace uri {

URI construct(
    const string& scheme,
    const string& path,
    const Option<string>& host,
    const Option<int>& port,
    const Option<string>& query,
    const Option<string>& fragment,
    const Option<string>& user,
    const Option<string>& password)
{
  URI uri;

  uri.set_scheme(scheme);
  uri.set_path(path);

  if (host.isSome()) { uri.set_host(host.get()); }
  if (port.isSome()) { uri.set_port(port.get()); }
  if (query.isSome()) { uri.set_query(query.get()); }
  if (fragment.isSome()) { uri.set_fragment(fragment.get()); }
  if (user.isSome()) { uri.set_user(user.get()); }
  if (password.isSome()) { uri.set_password(password.get()); }

  return uri;
}


URI redact(const URI& uri)
{
  URI redacted = uri;
  if (redacted.has_password()) {
    redacted.set_password("***");
  }
  return redacted;
}


namespace {

bool disallowPrivateIps()
{
  const Option<string> env = os::getenv("MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS");
  if (env.isNone()) {
    return false;
  }

  const string normalized = strings::lower(strings::trim(*env));
  return normalized == "true" || normalized == "1";
}


// Parses IPv4 literals in standard dotted-quad as well as `inet_aton`
// 1/2/3-part decimal, octal (`0251`), and hexadecimal (`0xa9fea9fe`) forms
// into host byte order (#386).
Option<uint32_t> tryParseIPv4HostOrder(const string& host)
{
  if (host.empty()) {
    return None();
  }

  const vector<string> parts = strings::split(host, ".");
  if (parts.empty() || parts.size() > 4) {
    return None();
  }

  vector<uint64_t> values;
  values.reserve(parts.size());

  for (const string& part : parts) {
    if (part.empty() || part[0] == '+' || part[0] == '-' ||
        isspace(static_cast<unsigned char>(part[0]))) {
      return None();
    }

    char* end = nullptr;
    errno = 0;
    const unsigned long long val = ::strtoull(part.c_str(), &end, 0);
    if (errno != 0 || end == nullptr || *end != '\0' || val > 0xffffffffULL) {
      return None();
    }
    values.push_back(static_cast<uint64_t>(val));
  }

  switch (values.size()) {
    case 1:
      return static_cast<uint32_t>(values[0]);
    case 2:
      if (values[0] > 0xffULL || values[1] > 0xffffffULL) {
        return None();
      }
      return static_cast<uint32_t>((values[0] << 24) | values[1]);
    case 3:
      if (values[0] > 0xffULL || values[1] > 0xffULL ||
          values[2] > 0xffffULL) {
        return None();
      }
      return static_cast<uint32_t>(
          (values[0] << 24) | (values[1] << 16) | values[2]);
    case 4:
      if (values[0] > 0xffULL || values[1] > 0xffULL ||
          values[2] > 0xffULL || values[3] > 0xffULL) {
        return None();
      }
      return static_cast<uint32_t>(
          (values[0] << 24) | (values[1] << 16) |
          (values[2] << 8) | values[3]);
    default:
      return None();
  }
}


Option<Error> validateIPv4HostOrder(
    uint32_t ip,
    const string& displayHost,
    bool disallowPrivate)
{
  // Always block 0.0.0.0/8 ("this network", aliases to localhost on Linux/macOS).
  if ((ip & 0xff000000U) == 0x00000000U) {
    return Error(
        "Refusing network fetch targeting unspecified/local address '" +
        displayHost + "' (#386)");
  }

  // Always block IPv4 link-local and cloud instance metadata (169.254.0.0/16,
  // including 169.254.169.254).
  if ((ip & 0xffff0000U) == 0xa9fe0000U) {
    return Error(
        "Refusing network fetch targeting link-local / cloud metadata address '" +
        displayHost + "' (169.254.0.0/16, #386)");
  }

  // Always block Alibaba Cloud metadata endpoint (100.100.100.200/32).
  if (ip == 0x646464c8U) {
    return Error(
        "Refusing network fetch targeting cloud metadata address '" +
        displayHost + "' (100.100.100.200, #386)");
  }

  if (disallowPrivate) {
    // Loopback 127.0.0.0/8.
    if ((ip & 0xff000000U) == 0x7f000000U) {
      return Error(
          "Refusing network fetch targeting loopback address '" +
          displayHost + "' (MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS, #386)");
    }

    // RFC1918 private ranges: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16,
    // and RFC6598 shared CGNAT space 100.64.0.0/10.
    if ((ip & 0xff000000U) == 0x0a000000U ||
        (ip & 0xfff00000U) == 0xac100000U ||
        (ip & 0xffff0000U) == 0xc0a80000U ||
        (ip & 0xffc00000U) == 0x64400000U) {
      return Error(
          "Refusing network fetch targeting private network address '" +
          displayHost + "' (MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS, #386)");
    }
  }

  return None();
}


Option<Error> validateIPv6Bytes(
    const uint8_t b[16],
    const string& displayHost,
    bool disallowPrivate)
{
  // Check IPv4-mapped (`::ffff:a.b.c.d`) and IPv4-compatible (`::a.b.c.d`).
  const bool zeroPrefix80 =
    std::all_of(b, b + 10, [](uint8_t v) { return v == 0; });
  const bool isV4Mapped =
    zeroPrefix80 && b[10] == 0xff && b[11] == 0xff;
  const bool isV4Compat =
    zeroPrefix80 && b[10] == 0x00 && b[11] == 0x00 &&
    !(b[12] == 0 && b[13] == 0 && b[14] == 0 && (b[15] == 0 || b[15] == 1));

  if (isV4Mapped || isV4Compat) {
    const uint32_t mappedIpv4 =
      (static_cast<uint32_t>(b[12]) << 24) |
      (static_cast<uint32_t>(b[13]) << 16) |
      (static_cast<uint32_t>(b[14]) << 8) |
      static_cast<uint32_t>(b[15]);
    return validateIPv4HostOrder(mappedIpv4, displayHost, disallowPrivate);
  }

  // Always block unspecified IPv6 address `::`.
  if (std::all_of(b, b + 16, [](uint8_t v) { return v == 0; })) {
    return Error(
        "Refusing network fetch targeting unspecified IPv6 address '" +
        displayHost + "' (#386)");
  }

  // Always block IPv6 link-local `fe80::/10`.
  if (b[0] == 0xfe && (b[1] & 0xc0) == 0x80) {
    return Error(
        "Refusing network fetch targeting IPv6 link-local address '" +
        displayHost + "' (fe80::/10, #386)");
  }

  // Always block AWS EC2 IPv6 IMDS (`fd00:ec2::254`).
  const bool middleZero =
    std::all_of(b + 4, b + 14, [](uint8_t v) { return v == 0; });
  if (b[0] == 0xfd && b[1] == 0x00 && b[2] == 0x0e && b[3] == 0xc2 &&
      middleZero && b[14] == 0x02 && b[15] == 0x54) {
    return Error(
        "Refusing network fetch targeting cloud metadata IPv6 address '" +
        displayHost + "' (fd00:ec2::254, #386)");
  }

  if (disallowPrivate) {
    // Loopback `::1`.
    const bool isLoopback6 =
      std::all_of(b, b + 15, [](uint8_t v) { return v == 0; }) &&
      b[15] == 1;
    if (isLoopback6) {
      return Error(
          "Refusing network fetch targeting IPv6 loopback address '" +
          displayHost + "' (MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS, #386)");
    }

    // Unique Local Addresses `fc00::/7`.
    if ((b[0] & 0xfe) == 0xfc) {
      return Error(
          "Refusing network fetch targeting IPv6 unique-local address '" +
          displayHost + "' (MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS, #386)");
    }
  }

  return None();
}


Try<string> percentDecodeHost(const string& input)
{
  string out;
  out.reserve(input.size());

  for (size_t i = 0; i < input.size(); ++i) {
    if (input[i] != '%') {
      out.push_back(input[i]);
      continue;
    }

    if (i + 2 >= input.size() ||
        !isxdigit(static_cast<unsigned char>(input[i + 1])) ||
        !isxdigit(static_cast<unsigned char>(input[i + 2]))) {
      return Error("Malformed percent-encoding in network hostname (#386)");
    }

    const string hex = input.substr(i + 1, 2);
    const unsigned long byteVal = ::strtoul(hex.c_str(), nullptr, 16);
    if (byteVal <= 0x20UL || byteVal >= 0x7fUL) {
      return Error("Invalid percent-encoded control character in hostname (#386)");
    }

    const char decoded = static_cast<char>(byteVal);
    if (decoded == '/' || decoded == '\\' || decoded == '@' ||
        decoded == '?' || decoded == '#') {
      return Error("Invalid percent-encoded delimiter in hostname (#386)");
    }

    out.push_back(decoded);
    i += 2;
  }

  return out;
}


Option<Error> validateHostTarget(const string& rawHost)
{
  string host = strings::trim(rawHost);
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }

  for (unsigned char c : host) {
    if (c <= 0x20 || c == 0x7f || c == '\\' || c == '/' ||
        c == '@' || c == '?' || c == '#') {
      return Error(
          "Invalid character in network hostname '" + rawHost + "' (#386)");
    }
  }

  // For IPv6 literals (`:` present), strip any `%zone` or `%25zone` index.
  // For non-IPv6 hostnames/IPv4 literals, percent-decode `%xx` sequences first
  // so `http://%31%36%39.254.169.254/` and `http://metadata.google.%69nternal/`
  // cannot bypass hostname/IP validation (#386).
  if (host.find(':') != string::npos) {
    const size_t zonePos = host.find('%');
    if (zonePos != string::npos) {
      host = host.substr(0, zonePos);
    }
  } else if (host.find('%') != string::npos) {
    Try<string> decoded = percentDecodeHost(host);
    if (decoded.isError()) {
      return Error(decoded.error());
    }
    host = *decoded;
    if (host.find(':') != string::npos) {
      const size_t zonePos = host.find('%');
      if (zonePos != string::npos) {
        host = host.substr(0, zonePos);
      }
    }
  }

  host = strings::lower(strings::trim(host));
  while (!host.empty() && host.back() == '.') {
    host.pop_back();
  }

  if (host.empty()) {
    return Error("Network URI target is missing a hostname");
  }

  // Always block well-known cloud metadata hostnames (#386).
  if (host == "metadata.google.internal" ||
      strings::endsWith(host, ".metadata.google.internal") ||
      host == "metadata.goog" ||
      strings::endsWith(host, ".metadata.goog") ||
      host == "metadata" ||
      host == "instance-data" ||
      strings::endsWith(host, ".instance-data") ||
      host == "metadata.tencentyun.com" ||
      strings::endsWith(host, ".metadata.tencentyun.com") ||
      host == "metadata.azure.com" ||
      strings::endsWith(host, ".metadata.azure.com")) {
    return Error(
        "Refusing network fetch targeting cloud metadata hostname '" +
        rawHost + "' (#386)");
  }

  const bool disallowPrivate = disallowPrivateIps();
  if (disallowPrivate) {
    if (host == "localhost" ||
        strings::endsWith(host, ".localhost") ||
        strings::endsWith(host, ".local") ||
        strings::endsWith(host, ".internal")) {
      return Error(
          "Refusing network fetch targeting local/private hostname '" +
          rawHost + "' (MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS, #386)");
    }
  }

  const Option<uint32_t> ipv4 = tryParseIPv4HostOrder(host);
  if (ipv4.isSome()) {
    return validateIPv4HostOrder(*ipv4, rawHost, disallowPrivate);
  }

#ifndef __WINDOWS__
  struct in6_addr addr6;
  if (::inet_pton(AF_INET6, host.c_str(), &addr6) == 1) {
    return validateIPv6Bytes(addr6.s6_addr, rawHost, disallowPrivate);
  }

  // If `host` is a DNS name, resolve it via `getaddrinfo` and verify every
  // returned A/AAAA record against the forbidden metadata/link-local (and
  // optional private IP) ranges so wildcard DNS services (e.g.
  // `169.254.169.254.nip.io`) cannot bypass literal checks (#386).
  struct addrinfo hints;
  ::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo* res = nullptr;
  if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) == 0 &&
      res != nullptr) {
    for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
      if (p->ai_family == AF_INET && p->ai_addr != nullptr) {
        const struct sockaddr_in* sin =
          reinterpret_cast<const struct sockaddr_in*>(p->ai_addr);
        Option<Error> err = validateIPv4HostOrder(
            ntohl(sin->sin_addr.s_addr), rawHost, disallowPrivate);
        if (err.isSome()) {
          ::freeaddrinfo(res);
          return err;
        }
      } else if (p->ai_family == AF_INET6 && p->ai_addr != nullptr) {
        const struct sockaddr_in6* sin6 =
          reinterpret_cast<const struct sockaddr_in6*>(p->ai_addr);
        Option<Error> err = validateIPv6Bytes(
            sin6->sin6_addr.s6_addr, rawHost, disallowPrivate);
        if (err.isSome()) {
          ::freeaddrinfo(res);
          return err;
        }
      }
    }
    ::freeaddrinfo(res);
  }
#endif // __WINDOWS__

  return None();
}

} // namespace {


Option<Error> validateNetworkTarget(const URI& uri)
{
  const string scheme = strings::lower(strings::trim(uri.scheme()));
  if (scheme != "http" &&
      scheme != "https" &&
      scheme != "docker" &&
      scheme != "docker-manifest" &&
      scheme != "docker-blob") {
    return Error("Unsupported network URI scheme '" + uri.scheme() + "' (#386)");
  }

  if (!uri.has_host() || strings::trim(uri.host()).empty()) {
    return Error("Network URI is missing a host");
  }

  return validateHostTarget(uri.host());
}


Option<Error> validateUrlTarget(const string& url)
{
  const string trimmed = strings::trim(url);
  const size_t schemeEnd = trimmed.find("://");
  if (schemeEnd == string::npos || schemeEnd == 0) {
    return Error("Network URL is missing a scheme");
  }

  const string scheme = strings::lower(trimmed.substr(0, schemeEnd));
  if (scheme != "http" && scheme != "https") {
    return Error(
        "Unsupported network URL scheme '" + scheme +
        "'; only http and https are allowed (#386)");
  }

  const size_t authorityStart = schemeEnd + 3;
  const size_t authorityEnd = trimmed.find_first_of("/?#", authorityStart);
  string authority = authorityEnd == string::npos
    ? trimmed.substr(authorityStart)
    : trimmed.substr(authorityStart, authorityEnd - authorityStart);

  for (unsigned char c : authority) {
    if (c <= 0x20 || c == 0x7f || c == '\\') {
      return Error("Invalid character in network URL authority (#386)");
    }
  }

  const size_t atPos = authority.rfind('@');
  if (atPos != string::npos) {
    authority = authority.substr(atPos + 1);
  }

  if (authority.empty()) {
    return Error("Network URL is missing a host");
  }

  string host;
  if (authority.front() == '[') {
    const size_t closeBracket = authority.find(']');
    if (closeBracket == string::npos ||
        (closeBracket + 1 != authority.size() &&
         authority[closeBracket + 1] != ':')) {
      return Error("Malformed bracketed IPv6 host in network URL (#386)");
    }
    host = authority.substr(1, closeBracket - 1);
  } else {
    const size_t firstColon = authority.find(':');
    const size_t lastColon = authority.rfind(':');
    if (firstColon != string::npos && firstColon == lastColon) {
      host = authority.substr(0, firstColon);
    } else {
      host = authority;
    }
  }

  return validateHostTarget(host);
}


Option<Error> validateLocalSourcePath(
    const string& sourcePath,
    bool allowLocalUris,
    const Option<string>& allowedPrefix)
{
  if (!allowLocalUris) {
    return Error(
        "Local filesystem URI fetches ('" + sourcePath +
        "') are disabled (--fetcher_allow_local_uris=false, #399)");
  }

  if (allowedPrefix.isSome()) {
    const string trimmedPrefix = strings::trim(*allowedPrefix);
    if (trimmedPrefix.empty()) {
      return Error(
          "Invalid empty --fetcher_allowed_local_uri_prefix (#399)");
    }

    const Result<string> canonicalPrefix = os::realpath(trimmedPrefix);
    if (!canonicalPrefix.isSome()) {
      return Error(
          "Failed to resolve allowed local URI prefix '" + trimmedPrefix +
          "': " +
          (canonicalPrefix.isError()
             ? canonicalPrefix.error()
             : "No such file or directory"));
    }

    const Result<string> canonicalSource = os::realpath(sourcePath);
    if (!canonicalSource.isSome()) {
      return Error(
          "Failed to resolve local URI path '" + sourcePath + "': " +
          (canonicalSource.isError()
             ? canonicalSource.error()
             : "No such file or directory"));
    }

    const string prefixDir =
      strings::remove(*canonicalPrefix, "/", strings::SUFFIX);

    const bool insidePrefix =
      *canonicalSource == *canonicalPrefix ||
      (prefixDir.empty() && strings::startsWith(*canonicalSource, "/")) ||
      (!prefixDir.empty() &&
       strings::startsWith(*canonicalSource, prefixDir + "/"));

    if (!insidePrefix) {
      return Error(
          "Local URI path '" + sourcePath + "' resolves to '" +
          *canonicalSource + "', which is outside allowed prefix '" +
          *canonicalPrefix + "' (#399)");
    }
  }

  return None();
}


Option<Error> validateLocalSourcePath(const string& sourcePath)
{
  bool allowLocalUris = true;
  const Option<string> allowEnv =
    os::getenv("MEDIOS_FETCHER_ALLOW_LOCAL_URIS");
  if (allowEnv.isSome()) {
    const string normalized = strings::lower(strings::trim(*allowEnv));
    if (normalized == "false" || normalized == "0") {
      allowLocalUris = false;
    }
  }

  Option<string> allowedPrefix =
    os::getenv("MEDIOS_FETCHER_ALLOWED_LOCAL_PREFIX");
  if (allowedPrefix.isSome() && strings::trim(*allowedPrefix).empty()) {
    allowedPrefix = None();
  }

  return validateLocalSourcePath(sourcePath, allowLocalUris, allowedPrefix);
}

} // namespace uri {
} // namespace medios {
