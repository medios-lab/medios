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

#include <iostream>
#include <queue>
#include <vector>

#include <medios/type_utils.hpp>
#include <medios/version.hpp>

#include <medios/v1/medios.hpp>
#include <medios/v1/resources.hpp>
#include <medios/v1/scheduler.hpp>

#include <process/delay.hpp>
#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/protobuf.hpp>

#include <stout/check.hpp>
#include <stout/duration.hpp>
#include <stout/flags.hpp>
#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>

#include "common/parse.hpp"
#include "common/protobuf_utils.hpp"

#include "internal/devolve.hpp"

#include "logging/logging.hpp"

#include "v1/parse.hpp"

using std::cerr;
using std::cout;
using std::endl;
using std::queue;
using std::string;
using std::vector;

using google::protobuf::RepeatedPtrField;

using medios::internal::devolve;

using medios::v1::NodeID;
using medios::v1::CapabilityInfo;
using medios::v1::CheckInfo;
using medios::v1::CheckStatusInfo;
using medios::v1::CommandInfo;
using medios::v1::ContainerInfo;
using medios::v1::Credential;
using medios::v1::Environment;
using medios::v1::ExecutorInfo;
using medios::v1::FrameworkID;
using medios::v1::FrameworkInfo;
using medios::v1::Image;
using medios::v1::Label;
using medios::v1::Labels;
using medios::v1::Offer;
using medios::v1::Resource;
using medios::v1::Resources;
using medios::v1::RLimitInfo;
using medios::v1::TaskGroupInfo;
using medios::v1::TaskID;
using medios::v1::TaskInfo;
using medios::v1::TaskState;
using medios::v1::TaskStatus;
using medios::v1::Volume;

using medios::v1::scheduler::Call;
using medios::v1::scheduler::Event;
using medios::v1::scheduler::Medios;

using process::Future;
using process::Owned;


class Flags : public virtual flags::FlagsBase
{
public:
  Flags()
  {
    add(&Flags::controller,
        "controller",
        "Medios controller (e.g., IP:PORT).");

    add(&Flags::task,
        "task",
        "The value could be a JSON-formatted string of `TaskInfo` or a\n"
        "file path containing the JSON-formatted `TaskInfo`. Path must\n"
        "be of the form `file:///path/to/file` or `/path/to/file`.\n"
        "\n"
        "See the `TaskInfo` message in `medios.proto` for the expected\n"
        "format. NOTE: `node_id` need not to be set.\n"
        "\n"
        "Example:\n"
        "{\n"
        "  \"name\": \"Name of the task\",\n"
        "  \"task_id\": {\"value\" : \"Id of the task\"},\n"
        "  \"node_id\": {\"value\" : \"\"},\n"
        "  \"resources\": [\n"
        "    {\n"
        "      \"name\": \"cpus\",\n"
        "      \"type\": \"SCALAR\",\n"
        "      \"scalar\": {\n"
        "        \"value\": 0.1\n"
        "      }\n"
        "    },\n"
        "    {\n"
        "      \"name\": \"mem\",\n"
        "      \"type\": \"SCALAR\",\n"
        "      \"scalar\": {\n"
        "        \"value\": 32\n"
        "      }\n"
        "    }\n"
        "  ],\n"
        "  \"command\": {\n"
        "    \"value\": \"sleep 1000\"\n"
        "  }\n"
        "}");

    add(&Flags::task_group,
        "task_group",
        "The value could be a JSON-formatted string of `TaskGroupInfo` or a\n"
        "file path containing the JSON-formatted `TaskGroupInfo`. Path must\n"
        "be of the form `file:///path/to/file` or `/path/to/file`.\n"
        "\n"
        "See the `TaskGroupInfo` message in `medios.proto` for the expected\n"
        "format. NOTE: `node_id` need not to be set.\n"
        "\n"
        "Example:\n"
        "{\n"
        "  \"tasks\":\n"
        "     [\n"
        "        {\n"
        "         \"name\": \"Name of the task\",\n"
        "         \"task_id\": {\"value\" : \"Id of the task\"},\n"
        "         \"node_id\": {\"value\" : \"\"},\n"
        "         \"resources\": [{\n"
        "            \"name\": \"cpus\",\n"
        "            \"type\": \"SCALAR\",\n"
        "            \"scalar\": {\n"
        "                \"value\": 0.1\n"
        "             }\n"
        "           },\n"
        "           {\n"
        "            \"name\": \"mem\",\n"
        "            \"type\": \"SCALAR\",\n"
        "            \"scalar\": {\n"
        "                \"value\": 32\n"
        "             }\n"
        "          }],\n"
        "         \"command\": {\n"
        "            \"value\": \"sleep 1000\"\n"
        "           }\n"
        "       }\n"
        "     ]\n"
        "}");

    add(&Flags::name,
        "name",
        "Name for the command.");

    add(&Flags::shell,
        "shell",
        "Determine the command is a shell or not. If not, 'command' will be\n"
        "treated as executable value and arguments (TODO).",
        true);

    // TODO(alexr): Once MESOS-4882 lands, elaborate on what `command` can
    // mean: an executable, a shell command, an entrypoint for a container.
    add(&Flags::command,
        "command",
        "Command to launch.");

    add(&Flags::environment,
        "env",
        "Shell command environment variables.\n"
        "The value could be a JSON formatted string of environment variables\n"
        "(i.e., {\"name1\": \"value1\"}) or a file path containing the JSON\n"
        "formatted environment variables. Path should be of the form\n"
        "'file:///path/to/file'.");

    add(&Flags::resources,
        "resources",
        "Resources for the command.",
        "cpus:1;mem:128");

    add(&Flags::checkpoint,
        "checkpoint",
        "Enable checkpointing for the framework.",
        false);

    add(&Flags::framework_capabilities,
        "framework_capabilities",
        "Comma-separated list of optional framework capabilities to enable.\n"
        "RESERVATION_REFINEMENT and TASK_KILLING_STATE are always enabled.\n"
        "PARTITION_AWARE is enabled unless --no-partition-aware is specified.");

    add(&Flags::containerizer,
        "containerizer",
        "Containerizer to be used.",
        "medios");

    add(&Flags::effective_capabilities,
        "effective_capabilities",
        "JSON representation of effective system capabilities that should be\n"
        "granted to the command.\n"
        "\n"
        "Example:\n"
        "{\n"
        "   \"capabilities\": [\n"
        "       \"NET_RAW\",\n"
        "       \"SYS_ADMIN\"\n"
        "     ]\n"
        "}");

    add(&Flags::bounding_capabilities,
        "bounding_capabilities",
        "JSON representation of system capabilities bounding set that should\n"
        "be applied to the command.\n"
        "\n"
        "Example:\n"
        "{\n"
        "   \"capabilities\": [\n"
        "       \"NET_RAW\",\n"
        "       \"SYS_ADMIN\"\n"
        "     ]\n"
        "}");

    add(&Flags::rlimits,
        "rlimits",
        "JSON representation of resource limits for the command. For\n"
        "example, the following sets the limit for CPU time to be one\n"
        "second, and the size of created files to be unlimited:\n"
        "{\n"
        "  \"rlimits\": [\n"
        "    {\n"
        "      \"type\":\"RLMT_CPU\",\n"
        "      \"soft\":\"1\",\n"
        "      \"hard\":\"1\"\n"
        "    },\n"
        "    {\n"
        "      \"type\":\"RLMT_FSIZE\"\n"
        "    }\n"
        "  ]\n"
        "}");

    add(&Flags::role,
        "role",
        "Role to use when registering.",
        "*");

    add(&Flags::kill_after,
        "kill_after",
        "Specifies a delay after which the task is killed\n"
        "(e.g., 10secs, 2mins, etc).");

    add(&Flags::networks,
        "networks",
        "Comma-separated list of networks that the container will join,\n"
        "e.g., `net1,net2`.");

    add(&Flags::constraints,
        "constraints",
        "Only launch on nodes whose attributes satisfy these constraints.\n"
        "Semicolon-separated `name:value` pairs matched against the node's\n"
        "text attributes, e.g. `os:linux;rack:r1`. An offer must satisfy\n"
        "every constraint, in addition to the resource requirements, to be\n"
        "accepted. Exact text match only.");

    add(&Flags::principal,
        "principal",
        "The principal to use for framework authentication.");

    add(&Flags::secret,
        "secret",
        "The secret to use for framework authentication.");

    add(&Flags::volumes,
        "volumes",
        "The value could be a JSON-formatted string of volumes or a\n"
        "file path containing the JSON-formatted volumes. Path must\n"
        "be of the form `file:///path/to/file` or `/path/to/file`.\n"
        "\n"
        "See the `Volume` message in `medios.proto` for the expected format.\n"
        "\n"
        "Example:\n"
        "[\n"
        "  {\n"
        "    \"container_path\":\"/path/to/container\",\n"
        "    \"mode\":\"RW\",\n"
        "    \"host_path\":\"/path/on/host\"\n"
        "  }\n"
        "]");

    add(&Flags::content_type,
        "content_type",
        "The content type to use for scheduler protocol messages. 'json'\n"
        "and 'protobuf' are valid choices.",
        "protobuf");

    add(&Flags::tty,
        "tty",
        "Attach a TTY to the task being launched",
        false);

    add(&Flags::partition_aware,
        "partition_aware",
        "Enable partition-awareness for the framework.",
        true);

    add(&Flags::version,
        "version",
        "Show version and exit.",
        false);
  }

  string controller;
  Option<string> name;
  Option<TaskInfo> task;
  Option<TaskGroupInfo> task_group;
  bool shell;
  Option<string> command;
  Option<hashmap<string, string>> environment;
  string resources;
  bool checkpoint;
  Option<std::set<string>> framework_capabilities;
  Option<JSON::Array> volumes;
  string containerizer;
  Option<CapabilityInfo> effective_capabilities;
  Option<CapabilityInfo> bounding_capabilities;
  Option<RLimitInfo> rlimits;
  string role;
  Option<Duration> kill_after;
  Option<string> networks;
  Option<string> constraints;
  Option<string> principal;
  Option<string> secret;
  string content_type;
  bool partition_aware;
  bool tty;
  bool version;
};


class CommandScheduler : public process::Process<CommandScheduler>
{
public:
  CommandScheduler(
      const FrameworkInfo& _frameworkInfo,
      const string& _controller,
      medios::ContentType _contentType,
      const Option<Duration>& _killAfter,
      const Option<Credential>& _credential,
      const Option<TaskInfo>& _task,
      const Option<TaskGroupInfo>& _taskGroup,
      const Option<string>& _networks,
      const std::vector<std::pair<std::string, std::string>>& _constraints)
    : state(DISCONNECTED),
      frameworkInfo(_frameworkInfo),
      controller(_controller),
      contentType(_contentType),
      killAfter(_killAfter),
      credential(_credential),
      task(_task),
      taskGroup(_taskGroup),
      networks(_networks),
      constraints(_constraints),
      launched(false),
      terminatedTaskCount(0) {}

  ~CommandScheduler() override {}

protected:
  void initialize() override
  {
    // We initialize the library here to ensure that callbacks are only invoked
    // after the process has spawned.
    medios.reset(new Medios(
      controller,
      contentType,
      process::defer(self(), &Self::connected),
      process::defer(self(), &Self::disconnected),
      process::defer(self(), &Self::received, lambda::_1),
      credential));
  }

  void connected()
  {
    state = CONNECTED;

    doReliableRegistration();
  }

  void disconnected()
  {
    state = DISCONNECTED;
  }

  void doReliableRegistration()
  {
    if (state == SUBSCRIBED || state == DISCONNECTED) {
      return;
    }

    Call call;
    call.set_type(Call::SUBSCRIBE);

    if (frameworkInfo.has_id()) {
      call.mutable_framework_id()->CopyFrom(frameworkInfo.id());
    }

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(frameworkInfo);

    medios->send(call);

    process::delay(Seconds(1), self(), &Self::doReliableRegistration);
  }

  void killTask(const TaskID& taskId, const NodeID& nodeId)
  {
    cout << "Asked to kill task '" << taskId
         << "' on node '" << nodeId << "'" << endl;

    Call call;
    call.set_type(Call::KILL);

    CHECK(frameworkInfo.has_id());
    call.mutable_framework_id()->CopyFrom(frameworkInfo.id());

    Call::Kill* kill = call.mutable_kill();
    kill->mutable_task_id()->CopyFrom(taskId);
    kill->mutable_node_id()->CopyFrom(nodeId);

    medios->send(call);
  }

  // True if the offering node's text attributes satisfy every --constraints
  // entry. An empty constraint set matches any node.
  bool offerSatisfiesConstraints(const Offer& offer) const
  {
    foreach (const auto& constraint, constraints) {
      bool matched = false;
      foreach (const medios::v1::Attribute& attribute, offer.attributes()) {
        if (attribute.name() == constraint.first &&
            attribute.type() == medios::v1::Value::TEXT &&
            attribute.text().value() == constraint.second) {
          matched = true;
          break;
        }
      }

      if (!matched) {
        return false;
      }
    }

    return true;
  }

  void offers(const vector<Offer>& offers)
  {
    CHECK_EQ(SUBSCRIBED, state);

    foreach (const Offer& offer, offers) {
      // Strip the allocation from the offer since we use a single role.
      Resources offered = offer.resources();
      offered.unallocate();

      Resources requiredResources;

      CHECK_NE(task.isSome(), taskGroup.isSome())
        << "Either task or task group should be set but not both";

      if (task.isSome()) {
        requiredResources = Resources(task->resources());
      } else {
        foreach (const TaskInfo& _task, taskGroup->tasks()) {
          requiredResources += Resources(_task.resources());
        }
      }

      if (!launched &&
          offerSatisfiesConstraints(offer) &&
          offered.toUnreserved().contains(requiredResources)) {
        TaskInfo _task;
        TaskGroupInfo _taskGroup;

        if (task.isSome()) {
          _task = task.get();
          _task.mutable_node_id()->MergeFrom(offer.node_id());

          // Takes resources first from the specified role, then from '*'.
          Option<Resources> resources = [&]() {
            if (frameworkInfo.role() == "*") {
              return offered.find(Resources(_task.resources()));
            } else {
              Resource::ReservationInfo reservation;
              reservation.set_type(Resource::ReservationInfo::STATIC);
              reservation.set_role(frameworkInfo.role());

              return offered.find(
                  Resources(_task.resources()).pushReservation(reservation));
            }
          }();

          CHECK_SOME(resources);

          _task.mutable_resources()->CopyFrom(resources.get());
        } else {
          foreach (TaskInfo _task, taskGroup->tasks()) {
            _task.mutable_node_id()->MergeFrom(offer.node_id());

            // Takes resources first from the specified role, then from '*'.
            Option<Resources> resources = [&]() {
              if (frameworkInfo.role() == "*") {
                return offered.find(Resources(_task.resources()));
              } else {
                Resource::ReservationInfo reservation;
                reservation.set_type(Resource::ReservationInfo::STATIC);
                reservation.set_role(frameworkInfo.role());

                return offered.find(
                    Resources(_task.resources()).pushReservation(reservation));
              }
            }();

            CHECK_SOME(resources);

            _task.mutable_resources()->CopyFrom(resources.get());
            _taskGroup.add_tasks()->CopyFrom(_task);
          }
       }
       Call call;
       call.set_type(Call::ACCEPT);

       CHECK(frameworkInfo.has_id());
       call.mutable_framework_id()->CopyFrom(frameworkInfo.id());

       Call::Accept* accept = call.mutable_accept();
       accept->add_offer_ids()->CopyFrom(offer.id());

       Offer::Operation* operation = accept->add_operations();

       if (task.isSome()) {
         operation->set_type(Offer::Operation::LAUNCH);
         operation->mutable_launch()->add_task_infos()->CopyFrom(_task);
       } else {
         operation->set_type(Offer::Operation::LAUNCH_GROUP);

         ExecutorInfo* executorInfo =
           operation->mutable_launch_group()->mutable_executor();

         executorInfo->set_type(ExecutorInfo::DEFAULT);
         executorInfo->mutable_executor_id()->set_value(
             "default-executor");

         executorInfo->mutable_framework_id()->CopyFrom(frameworkInfo.id());
         executorInfo->mutable_resources()->CopyFrom(
             Resources::parse("cpus:0.1;mem:32;disk:32").get());

         // Setup any CNI networks that the `task_group` needs to
         // join, in case the `--networks` flag was specified.
         if (networks.isSome() && !networks->empty()) {
           ContainerInfo* containerInfo = executorInfo->mutable_container();
           containerInfo->set_type(ContainerInfo::MEDIOS);

           foreach (const string& network,
                    strings::tokenize(networks.get(), ",")) {
             containerInfo->add_network_infos()->set_name(network);
           }
         }

         operation->mutable_launch_group()->mutable_task_group()->CopyFrom(
             _taskGroup);
       }

       medios->send(call);

       if (task.isSome()) {
         cout << "Submitted task '" << task->name() << "' to node '"
              << offer.node_id() << "'" << endl;
       } else {
         vector<TaskID> taskIds;

         foreach (const TaskInfo& _task, taskGroup->tasks()) {
           taskIds.push_back(_task.task_id());
         }

         cout << "Submitted task group with tasks "<< taskIds
              << " to node '" << offer.node_id() << "'" << endl;
       }

       launched = true;
      } else {
        Call call;
        call.set_type(Call::DECLINE);

        CHECK(frameworkInfo.has_id());
        call.mutable_framework_id()->CopyFrom(frameworkInfo.id());

        Call::Decline* decline = call.mutable_decline();
        decline->add_offer_ids()->CopyFrom(offer.id());

        medios->send(call);

        call.Clear();
        call.set_type(Call::SUPPRESS);
        call.mutable_framework_id()->CopyFrom(frameworkInfo.id());

        medios->send(call);
      }
    }
  }

  void received(queue<Event> events)
  {
    while (!events.empty()) {
      Event event = events.front();
      events.pop();

      switch (event.type()) {
        case Event::SUBSCRIBED: {
          frameworkInfo.mutable_id()->
            CopyFrom(event.subscribed().framework_id());

          state = SUBSCRIBED;

          cout << "Subscribed with ID " << frameworkInfo.id() << endl;
          break;
        }

        case Event::OFFERS: {
          offers(google::protobuf::convert(event.offers().offers()));
          break;
        }

        case Event::UPDATE: {
          update(event.update().status());
          break;
        }

        case Event::ERROR: {
          EXIT(EXIT_FAILURE)
            << "Received an ERROR event: " << event.error().message();

          break;
        }

        case Event::HEARTBEAT:
        case Event::INVERSE_OFFERS:
        case Event::FAILURE:
        case Event::RESCIND:
        case Event::RESCIND_INVERSE_OFFER:
        case Event::UPDATE_OPERATION_STATUS:
        case Event::MESSAGE: {
          break;
        }

        case Event::UNKNOWN: {
          LOG(WARNING) << "Received an UNKNOWN event and ignored";
          break;
        }
      }
    }
  }

  void update(const TaskStatus& status)
  {
    CHECK_EQ(SUBSCRIBED, state);

    cout << "Received status update " << status.state()
         << " for task '" << status.task_id() << "'" << endl;

    if (status.has_message()) {
      cout << "  message: '" << status.message() << "'" << endl;
    }
    if (status.has_source()) {
      cout << "  source: " << TaskStatus::Source_Name(status.source()) << endl;
    }
    if (status.has_reason()) {
      cout << "  reason: " << TaskStatus::Reason_Name(status.reason()) << endl;
    }
    if (status.has_healthy()) {
      cout << "  healthy?: " << status.healthy() << endl;
    }
    if (status.has_check_status()) {
      cout << "  check status: " << status.check_status() << endl;
    }
    if (status.has_limitation() && !status.limitation().resources().empty()) {
      cout << "  resource limit violation: "
           << status.limitation().resources() << endl;
    }

    if (status.has_uuid()) {
      Call call;
      call.set_type(Call::ACKNOWLEDGE);

      CHECK(frameworkInfo.has_id());
      call.mutable_framework_id()->CopyFrom(frameworkInfo.id());

      Call::Acknowledge* acknowledge = call.mutable_acknowledge();
      acknowledge->mutable_node_id()->CopyFrom(status.node_id());
      acknowledge->mutable_task_id()->CopyFrom(status.task_id());
      acknowledge->set_uuid(status.uuid());

      medios->send(call);
    }

    // If a task kill delay has been specified, schedule task kill.
    if (killAfter.isSome() && TaskState::TASK_RUNNING == status.state()) {
      delay(killAfter.get(),
            self(),
            &Self::killTask,
            status.task_id(),
            status.node_id());
    }

    if (medios::internal::protobuf::isTerminalState(devolve(status).state())) {
      CHECK_NE(task.isSome(), taskGroup.isSome())
        << "Either task or task group should be set but not both";

      if (task.isSome()) {
        terminate(self());
      } else {
        terminatedTaskCount++;

        if (terminatedTaskCount == taskGroup->tasks().size()) {
          terminate(self());
        }
      }
    }
  }

private:
  enum State
  {
    DISCONNECTED,
    CONNECTED,
    SUBSCRIBED
  } state;

  FrameworkInfo frameworkInfo;
  const string controller;
  medios::ContentType contentType;
  const Option<Duration> killAfter;
  const Option<Credential> credential;
  const Option<TaskInfo> task;
  const Option<TaskGroupInfo> taskGroup;
  const Option<string> networks;
  const std::vector<std::pair<std::string, std::string>> constraints;
  bool launched;
  int terminatedTaskCount;
  Owned<Medios> medios;
};


// TODO(jojy): Consider breaking down the method for each 'containerizer'.
static Result<ContainerInfo> getContainerInfo(
    const string& containerizer,
    const Option<vector<Volume>>& volumes,
    const Option<string>& networks,
    const Option<CapabilityInfo>& effective_capabilities,
    const Option<CapabilityInfo>& bounding_capabilities,
    const Option<RLimitInfo>& rlimits,
    const bool tty)
{
  if (containerizer.empty()) {
    return None();
  }

  ContainerInfo containerInfo;

  if (volumes.isSome()) {
    foreach (const Volume& volume, volumes.get()) {
      containerInfo.add_volumes()->CopyFrom(volume);
    }
  }

  if (containerizer == "medios") {
    if (!tty &&
        effective_capabilities.isNone() &&
        bounding_capabilities.isNone() &&
        rlimits.isNone() &&
        (networks.isNone() || networks->empty()) &&
        (volumes.isNone() || volumes->empty())) {
      return None();
    }

    containerInfo.set_type(ContainerInfo::MEDIOS);

    if (networks.isSome() && !networks->empty()) {
      foreach (const string& network,
               strings::tokenize(networks.get(), ",")) {
        containerInfo.add_network_infos()->set_name(network);
      }
    }

    if (effective_capabilities.isSome()) {
      containerInfo
        .mutable_linux_info()
        ->mutable_effective_capabilities()
        ->CopyFrom(effective_capabilities.get());
    }

    if (bounding_capabilities.isSome()) {
      containerInfo
        .mutable_linux_info()
        ->mutable_bounding_capabilities()
        ->CopyFrom(bounding_capabilities.get());
    }

    if (rlimits.isSome()) {
      containerInfo.mutable_rlimit_info()->CopyFrom(rlimits.get());
    }

    if (tty) {
      containerInfo.mutable_tty_info();
    }

    return containerInfo;
  }

  return Error("Unsupported containerizer: " + containerizer);
}


int main(int argc, char** argv)
{
  Flags flags;
  medios::ContentType contentType = medios::ContentType::PROTOBUF;

  // Load flags from command line only.
  Try<flags::Warnings> load = flags.load(None(), argc, argv);

  // TODO(marco): this should be encapsulated entirely into the
  // FlagsBase API - possibly with a 'guard' that prevents FlagsBase
  // from calling ::exit(EXIT_FAILURE) after calling usage() (which
  // would be the default behavior); see MESOS-2766.
  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  if (flags.version) {
    cout << "medios-execute " << MEDIOS_VERSION << endl;
    return EXIT_SUCCESS;
  }

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  medios::internal::logging::initialize(argv[0], false);

  // Log any flag warnings.
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  if (flags.task.isSome() && flags.task_group.isSome()) {
    EXIT(EXIT_FAILURE) << flags.usage(
        "Either task or task group should be set but not both."
        " Provide either '--task' OR '--task_group'");
  } else if (flags.task.isNone() && flags.task_group.isNone()) {
    if (flags.name.isNone()) {
      EXIT(EXIT_FAILURE) << flags.usage("Missing required option --name");
    }

    if (flags.shell && flags.command.isNone()) {
      EXIT(EXIT_FAILURE) << flags.usage("Missing required option --command");
    }
  } else {
    // Either --task or --task_group is set.
    if (flags.name.isSome() ||
        flags.command.isSome() ||
        flags.environment.isSome() ||
        flags.volumes.isSome()) {
      EXIT(EXIT_FAILURE) << flags.usage(
          "'--name, --command, --env, --volumes' can only be set when both "
          "'--task'"
          " and '--task_group' are not set");
    }

    if (flags.task.isSome() && flags.networks.isSome()) {
      EXIT(EXIT_FAILURE) << flags.usage(
          "'--networks' can only be set when"
          " '--task' is not set");
    }
  }

  if (flags.content_type == "json" ||
      flags.content_type == medios::APPLICATION_JSON) {
    contentType = medios::ContentType::JSON;
  } else if (flags.content_type == "protobuf" ||
             flags.content_type == medios::APPLICATION_PROTOBUF) {
    contentType = medios::ContentType::PROTOBUF;
  } else {
    EXIT(EXIT_FAILURE) << "Invalid content type '" << flags.content_type << "'";
  }

  Result<string> user = os::user();
  if (!user.isSome()) {
    if (user.isError()) {
      EXIT(EXIT_FAILURE) << "Failed to get username: " << user.error();
    } else {
      EXIT(EXIT_FAILURE) << "No username for uid " << ::getuid();
    }
  }

  Option<hashmap<string, string>> environment = None();

  if (flags.environment.isSome()) {
    environment = flags.environment.get();
  }

  // Always enable the following capabilities.
  vector<FrameworkInfo::Capability::Type> frameworkCapabilities = {
    FrameworkInfo::Capability::RESERVATION_REFINEMENT,
    FrameworkInfo::Capability::TASK_KILLING_STATE,
    FrameworkInfo::Capability::REVOCABLE_RESOURCES,
  };

  // Enable PARTITION_AWARE unless disabled by the user.
  if (flags.partition_aware) {
    frameworkCapabilities.push_back(
        FrameworkInfo::Capability::PARTITION_AWARE);
  }

  if (flags.framework_capabilities.isSome()) {
    foreach (const string& capability, flags.framework_capabilities.get()) {
      FrameworkInfo::Capability::Type type;

      if (!FrameworkInfo::Capability::Type_Parse(capability, &type)) {
        EXIT(EXIT_FAILURE)
          << "Flags '--framework_capabilities' specifies an unknown"
          << " capability '" << capability << "'";
      }

      if (type != FrameworkInfo::Capability::GPU_RESOURCES) {
        EXIT(EXIT_FAILURE)
          << "Flags '--framework_capabilities' specifies an unsupported"
          << " capability '" << capability << "'";
      }

      frameworkCapabilities.push_back(type);
    }
  }

  Option<vector<Volume>> volumes = None();

  if (flags.volumes.isSome()) {
    Try<RepeatedPtrField<Volume>> parse =
      ::protobuf::parse<RepeatedPtrField<Volume>>(flags.volumes.get());

    if (parse.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to convert '--volumes' to protobuf: " << parse.error();
    }

    vector<Volume> _volumes;

    foreach (const Volume& volume, parse.get()) {
      _volumes.push_back(volume);
    }

    volumes = _volumes;
  }

  FrameworkInfo frameworkInfo;
  frameworkInfo.set_user(user.get());
  frameworkInfo.set_name("medios-execute instance");
  frameworkInfo.set_role(flags.role);
  frameworkInfo.set_checkpoint(flags.checkpoint);
  Try<string> hostname = net::hostname();
  if (hostname.isSome()) {
    frameworkInfo.set_hostname(hostname.get());
  }
  foreach (const FrameworkInfo::Capability::Type& capability,
           frameworkCapabilities) {
    frameworkInfo.add_capabilities()->set_type(capability);
  }

  Option<Credential> credential = None();

  string sanitizedUser;
  for (char c : user.get()) {
    const bool valid =
      (c >= 'a' && c <= 'z') ||
      (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') ||
      c == '.' || c == '_' || c == '-';
    sanitizedUser.push_back(valid ? c : '-');
  }
  if (sanitizedUser.empty()) {
    sanitizedUser = "local";
  }

  const string defaultTaskPrincipal = [&]() -> string {
    if (flags.principal.isSome()) {
      const string& p = flags.principal.get();
      if (strings::startsWith(p, "operator:") ||
          strings::startsWith(p, "team:") ||
          strings::startsWith(p, "run:")) {
        return p;
      }
    }
    return "operator:" + sanitizedUser;
  }();

  if (flags.principal.isSome()) {
    frameworkInfo.set_principal(flags.principal.get());

    Credential credential_;
    credential_.set_principal(flags.principal.get());

    if (flags.secret.isSome()) {
      credential_.set_secret(flags.secret.get());
    }

    credential = credential_;
  } else {
    frameworkInfo.set_principal(defaultTaskPrincipal);
  }

  Option<TaskInfo> taskInfo = flags.task;
  Option<TaskGroupInfo> taskGroup = flags.task_group;

  if (flags.task.isNone() && flags.task_group.isNone()) {
    TaskInfo task;
    task.set_name(flags.name.get());
    task.mutable_task_id()->set_value(flags.name.get());
    task.set_principal(defaultTaskPrincipal);

    static const Try<Resources> resources = Resources::parse(flags.resources);

    if (resources.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to parse resources '" << flags.resources << "': "
        << resources.error();
    }

    task.mutable_resources()->CopyFrom(resources.get());

    CommandInfo* commandInfo = task.mutable_command();

    if (flags.shell) {
      CHECK_SOME(flags.command);

      commandInfo->set_shell(true);
      commandInfo->set_value(flags.command.get());
    } else {
      // TODO(gilbert): Treat 'command' as executable value and arguments.
      commandInfo->set_shell(false);
    }

    if (flags.environment.isSome()) {
      Environment* environment_ = commandInfo->mutable_environment();
      foreachpair (
          const string& name, const string& value, environment.get()) {
        Environment::Variable* environmentVariable =
          environment_->add_variables();

        environmentVariable->set_name(name);
        environmentVariable->set_value(value);
      }
    }

    Result<ContainerInfo> containerInfo =
      getContainerInfo(
        flags.containerizer,
        volumes,
        flags.networks,
        flags.effective_capabilities,
        flags.bounding_capabilities,
        flags.rlimits,
        flags.tty);

    if (containerInfo.isError()){
      EXIT(EXIT_FAILURE) << containerInfo.error();
    }

    if (containerInfo.isSome()) {
      task.mutable_container()->CopyFrom(containerInfo.get());
    }

    taskInfo = task;
  }

  if (taskInfo.isSome() &&
      (!taskInfo->has_principal() || taskInfo->principal().empty())) {
    taskInfo->set_principal(defaultTaskPrincipal);
  }

  if (taskGroup.isSome()) {
    for (int i = 0; i < taskGroup->tasks_size(); ++i) {
      TaskInfo* groupTask = taskGroup->mutable_tasks(i);
      if (!groupTask->has_principal() || groupTask->principal().empty()) {
        groupTask->set_principal(defaultTaskPrincipal);
      }
    }
  }

  std::vector<std::pair<string, string>> constraints;
  if (flags.constraints.isSome()) {
    foreach (const string& entry,
             strings::tokenize(flags.constraints.get(), ";")) {
      const vector<string> kv = strings::tokenize(entry, ":");
      if (kv.size() != 2) {
        cerr << "Invalid --constraints entry '" << entry
             << "'; expected name:value" << endl;
        return EXIT_FAILURE;
      }
      constraints.emplace_back(kv[0], kv[1]);
    }
  }

  Owned<CommandScheduler> scheduler(
      new CommandScheduler(
        frameworkInfo,
        flags.controller,
        contentType,
        flags.kill_after,
        credential,
        taskInfo,
        taskGroup,
        flags.networks,
        constraints));

  process::spawn(scheduler.get());
  process::wait(scheduler.get());

  return EXIT_SUCCESS;
}
