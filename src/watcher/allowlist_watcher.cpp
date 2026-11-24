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

#include <string>
#include <vector>

#include <glog/logging.h>

#include <process/delay.hpp>
#include <process/id.hpp>

#include <stout/foreach.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "watcher/allowlist_watcher.hpp"

namespace medios {
namespace internal {

using std::string;
using std::vector;

using process::Process;

using lambda::function;


AllowlistWatcher::AllowlistWatcher(
    const Option<Path>& path,
    const Duration& watchInterval,
    const function<void(const Option<hashset<string>>& allowlist)>& subscriber,
    const Option<hashset<string>>& initialAllowlist)
  : ProcessBase(process::ID::generate("allowlist")),
    path(path),
    watchInterval(watchInterval),
    subscriber(subscriber),
    lastAllowlist(initialAllowlist) {}


void AllowlistWatcher::initialize()
{
  // If no allowlist file is given (loaded allowlist is in state
  // (1) absent), then there is no need to watch. In case the
  // subscriber's initial policy was not permissive (initial
  // allowlist is not in (1) absent), notify the subscriber that
  // there is no allowlist any more.
  if (path.isNone()) { // Accept all nodes.
    VLOG(1) << "No allowlist given";
    if (lastAllowlist.isSome()) {
      subscriber(None());
    }
  } else {
    watch();
  }
}


void AllowlistWatcher::watch()
{
  // Read the list of allowlisted nodes from local file.
  // TODO(vinod): Add support for reading from ZooKeeper.
  // TODO(vinod): Ensure this read is atomic w.r.t external
  // writes/updates to this file.
  Option<hashset<string>> allowlist;

  CHECK_SOME(path);
  Try<string> read = os::read(path->string());

  if (read.isError()) {
    LOG(ERROR) << "Error reading allowlist file: " << read.error() << ". "
               << "Retrying";
    allowlist = lastAllowlist;
  } else if (read->empty()) {
    VLOG(1) << "Empty allowlist file " << path->string();
    allowlist = hashset<string>();
  } else {
    hashset<string> hostnames;
    vector<string> lines = strings::tokenize(read.get(), "\n");
    foreach (const string& hostname, lines) {
      hostnames.insert(hostname);
    }
    allowlist = hostnames;
  }

  // Send the allowlist to subscriber, if necessary.
  if (allowlist != lastAllowlist) {
    subscriber(allowlist);
  }

  // Schedule the next check.
  lastAllowlist = allowlist;
  delay(watchInterval, self(), &AllowlistWatcher::watch);
}

} // namespace internal {
} // namespace medios {
