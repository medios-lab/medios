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

#include <errno.h>
#include <stdlib.h> // For random().

#include <algorithm>
#include <cmath>
#include <deque>
#include <iomanip>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <medios/type_utils.hpp>

#include <medios/authentication/secret_generator.hpp>

#include <medios/module/authenticatee.hpp>

#include <medios/state/leveldb.hpp>
#include <medios/state/in_memory.hpp>

#include <process/after.hpp>
#include <process/async.hpp>
#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/reap.hpp>
#include <process/time.hpp>

#include <process/ssl/flags.hpp>

#include <stout/bytes.hpp>
#include <stout/check.hpp>
#include <stout/duration.hpp>
#include <stout/exit.hpp>
#include <stout/fs.hpp>
#include <stout/json.hpp>
#include <stout/lambda.hpp>
#include <stout/net.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/utils.hpp>
#include <stout/uuid.hpp>

#include <stout/os/realpath.hpp>

#include "authentication/identity_assertion/identity_assertion_authenticator.hpp"

#include "common/authorization.hpp"
#include "common/protobuf_utils.hpp"
#include "common/resources_utils.hpp"
#include "common/status_utils.hpp"
#include "common/validation.hpp"

#include "credentials/credentials.hpp"

#include "hook/manager.hpp"

#ifdef __linux__
#include "linux/fs.hpp"
#endif // __linux__

#include "logging/logging.hpp"

#include "controller/detector/standalone.hpp"

#include "module/manager.hpp"

#include "node/compatibility.hpp"
#include "node/constants.hpp"
#include "node/flags.hpp"
#include "node/paths.hpp"
#include "node/node.hpp"
#include "node/state.pb.h"
#include "node/task_status_update_manager.hpp"

namespace http = process::http;

using google::protobuf::RepeatedPtrField;

using medios::SecretGenerator;

using medios::authorization::ACCESS_SANDBOX;

using medios::executor::Call;

using medios::controller::detector::ControllerDetector;

using medios::node::ContainerConfig;
using medios::node::ContainerTermination;
using medios::node::QoSController;
using medios::node::QoSCorrection;
using medios::node::ResourceEstimator;

using std::deque;
using std::list;
using std::map;
using std::ostream;
using std::ostringstream;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;

using process::after;
using process::async;
using process::wait; // Necessary on some OS's to disambiguate.
using process::Break;
using process::Clock;
using process::Continue;
using process::ControlFlow;
using process::Failure;
using process::Future;
using process::Owned;
using process::PID;
using process::Promise;
using process::Time;
using process::UPID;

using process::http::authentication::Principal;

namespace medios {
namespace internal {
namespace node {

using namespace state;

void ResourceProvider::addOperation(Operation* operation)
{
  const UUID& uuid = operation->uuid();

  CHECK(!operations.contains(uuid))
    << "Operation (uuid: " << uuid << ") already exists";

  operations.put(uuid, operation);
}


void ResourceProvider::removeOperation(Operation* operation)
{
  const UUID& uuid = operation->uuid();

  CHECK(operations.contains(uuid))
    << "Unknown operation (uuid: " << uuid << ")";

  operations.erase(uuid);
}

} // namespace node {
} // namespace internal {
} // namespace medios {
