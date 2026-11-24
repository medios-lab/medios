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

// Gang-placement simulation, spatial only. A cluster of nodes with one
// topology label and slot capacities under background load; a gang of N
// members, each demanding `d` slots on a single node with a matching
// label. Compares two placement strategies on identical scenarios:
//
//   naive   — members in arrival order, first-fit over nodes in index
//             order, fail on the first unplaceable member.
//   anchor  — most-constrained-first: members ordered by feasible-set
//             size ascending; the most constrained member (the anchor)
//             is the only backtrack point. The anchor is retried across
//             each of its feasible nodes; the rest are placed greedily.
//
// Reported per (N, utilization): feasibility rate, placement effort
// (nodes examined, anchor backtracks), and the size of the smallest
// feasible set at scenario start, aggregated separately for feasible
// and infeasible outcomes. Output is CSV on stdout.
//
// No medios types and no allocator code: the question under test is a
// property of feasible-set intersection, not of any implementation.

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace {

struct Node
{
  int type;
  int free;
};

struct Member
{
  int type;
  int demand;
};

struct Effort
{
  int64_t nodesExamined = 0;
  int64_t backtracks = 0;
};

struct Config
{
  int nodes = 100;
  int slotsPerNode = 8;
  int trials = 2000;
  unsigned int seed = 42;
};


// Background load is itself a packed workload: tasks of the same size
// mix as gang members, placed first-fit in random node order until the
// target utilization is reached. Per-slot random occupancy would leave
// almost no fully free nodes at any utilization, making every
// whole-node member infeasible regardless of strategy.
std::vector<Node> makeCluster(
    const Config& config, double utilization, std::mt19937& rng)
{
  static const int demands[] = {1, 2, 4, 8};

  std::bernoulli_distribution typeDist(0.5);
  std::uniform_int_distribution<int> demandDist(0, 3);

  std::vector<Node> cluster(config.nodes);
  for (Node& node : cluster) {
    node.type = typeDist(rng) ? 1 : 0;
    node.free = config.slotsPerNode;
  }

  std::vector<size_t> visit(cluster.size());
  std::iota(visit.begin(), visit.end(), size_t{0});

  const int totalSlots = config.nodes * config.slotsPerNode;
  int used = 0;
  int stuck = 0;

  while (used < static_cast<int>(utilization * totalSlots) && stuck < 100) {
    const int demand = demands[demandDist(rng)];
    std::shuffle(visit.begin(), visit.end(), rng);

    bool placed = false;
    for (size_t i : visit) {
      if (cluster[i].free >= demand) {
        cluster[i].free -= demand;
        used += demand;
        placed = true;
        break;
      }
    }

    stuck = placed ? 0 : stuck + 1;
  }

  return cluster;
}


// `correlation` is the probability that a member requires the gang's
// home type rather than an independently drawn one. 0 reproduces
// independent members (the 0.9^N model); 1 puts the whole gang in
// contention for the same node population.
std::vector<Member> makeGang(int size, double correlation, std::mt19937& rng)
{
  static const int demands[] = {1, 2, 4, 8};

  std::bernoulli_distribution typeDist(0.5);
  std::bernoulli_distribution correlated(correlation);
  std::uniform_int_distribution<int> demandDist(0, 3);

  const int homeType = typeDist(rng) ? 1 : 0;

  std::vector<Member> gang(size);
  for (Member& member : gang) {
    member.type = correlated(rng) ? homeType : (typeDist(rng) ? 1 : 0);
    member.demand = demands[demandDist(rng)];
  }

  return gang;
}


// Number of nodes that could host the member under the current
// availability. Every probed node counts toward effort.
int feasibleCount(
    const std::vector<Node>& cluster, const Member& member, Effort* effort)
{
  int count = 0;
  for (const Node& node : cluster) {
    ++effort->nodesExamined;
    if (node.type == member.type && node.free >= member.demand) {
      ++count;
    }
  }

  return count;
}


// First node that can host the member, scanning in index order from
// `start`; -1 if none. Every probed node counts toward effort.
int firstFit(
    const std::vector<Node>& cluster,
    const Member& member,
    size_t start,
    Effort* effort)
{
  for (size_t i = start; i < cluster.size(); ++i) {
    ++effort->nodesExamined;
    if (cluster[i].type == member.type &&
        cluster[i].free >= member.demand) {
      return static_cast<int>(i);
    }
  }

  return -1;
}


bool placeNaive(
    std::vector<Node> cluster, const std::vector<Member>& gang, Effort* effort)
{
  for (const Member& member : gang) {
    int node = firstFit(cluster, member, 0, effort);
    if (node < 0) {
      return false;
    }
    cluster[static_cast<size_t>(node)].free -= member.demand;
  }

  return true;
}


bool placeAnchor(
    const std::vector<Node>& cluster,
    std::vector<Member> gang,
    Effort* effort)
{
  // Rank members by feasible-set size on the untouched cluster; the
  // precomputation cost is charged to this strategy's effort.
  std::vector<int> feasible(gang.size());
  for (size_t i = 0; i < gang.size(); ++i) {
    feasible[i] = feasibleCount(cluster, gang[i], effort);
  }

  std::vector<size_t> order(gang.size());
  std::iota(order.begin(), order.end(), size_t{0});
  std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
    return feasible[a] < feasible[b];
  });

  const Member& anchor = gang[order[0]];

  if (feasible[order[0]] == 0) {
    return false;
  }

  // The anchor is the only backtrack point: try it on each of its
  // feasible nodes; for each anchoring, place the remaining members
  // greedily in most-constrained-first order.
  size_t anchorStart = 0;
  while (true) {
    Effort attempt;
    int anchorNode = firstFit(cluster, anchor, anchorStart, &attempt);
    effort->nodesExamined += attempt.nodesExamined;

    if (anchorNode < 0) {
      return false;
    }

    std::vector<Node> state = cluster;
    state[static_cast<size_t>(anchorNode)].free -= anchor.demand;

    bool placed = true;
    for (size_t i = 1; i < order.size(); ++i) {
      int node = firstFit(state, gang[order[i]], 0, effort);
      if (node < 0) {
        placed = false;
        break;
      }
      state[static_cast<size_t>(node)].free -= gang[order[i]].demand;
    }

    if (placed) {
      return true;
    }

    ++effort->backtracks;
    anchorStart = static_cast<size_t>(anchorNode) + 1;
  }
}


// Reference packer: members sorted by demand descending, each placed
// on the matching-type node with the smallest sufficient free
// capacity. For divisible demand sizes (1|2|4|8) this is expected to
// decide feasibility exactly; --selfcheck validates that against
// exhaustive search rather than assuming it.
bool placeBestFitDecreasing(
    std::vector<Node> cluster, std::vector<Member> gang, Effort* effort)
{
  std::stable_sort(gang.begin(), gang.end(), [](const Member& a,
                                                const Member& b) {
    return a.demand > b.demand;
  });

  for (const Member& member : gang) {
    int best = -1;
    for (size_t i = 0; i < cluster.size(); ++i) {
      ++effort->nodesExamined;
      if (cluster[i].type == member.type &&
          cluster[i].free >= member.demand &&
          (best < 0 || cluster[i].free < cluster[static_cast<size_t>(best)].free)) {
        best = static_cast<int>(i);
      }
    }

    if (best < 0) {
      return false;
    }
    cluster[static_cast<size_t>(best)].free -= member.demand;
  }

  return true;
}


bool placeExhaustive(std::vector<Node>& cluster, std::vector<Member>& gang,
                     size_t index)
{
  if (index == gang.size()) {
    return true;
  }

  for (Node& node : cluster) {
    if (node.type == gang[index].type && node.free >= gang[index].demand) {
      node.free -= gang[index].demand;
      if (placeExhaustive(cluster, gang, index + 1)) {
        node.free += gang[index].demand;
        return true;
      }
      node.free += gang[index].demand;
    }
  }

  return false;
}


// Compares the best-fit-decreasing reference against exhaustive search
// on small random instances. A zero mismatch count is the evidence
// that the reference decides feasibility exactly for this demand mix.
int selfcheck(unsigned int seed)
{
  std::mt19937 rng(seed);

  Config tiny;
  tiny.nodes = 5;
  tiny.slotsPerNode = 8;

  int mismatches = 0;
  const int instances = 5000;

  for (int i = 0; i < instances; ++i) {
    std::uniform_real_distribution<double> uDist(0.3, 0.9);
    std::uniform_int_distribution<int> nDist(2, 6);

    const std::vector<Node> cluster = makeCluster(tiny, uDist(rng), rng);
    std::vector<Member> gang = makeGang(nDist(rng), 0.5, rng);

    Effort scratch;
    const bool bfd = placeBestFitDecreasing(cluster, gang, &scratch);

    std::vector<Node> mutableCluster = cluster;
    std::stable_sort(gang.begin(), gang.end(), [](const Member& a,
                                                  const Member& b) {
      return a.demand > b.demand;
    });
    const bool exact = placeExhaustive(mutableCluster, gang, 0);

    if (bfd != exact) {
      ++mismatches;
      std::cerr << "mismatch at instance " << i << ": bfd=" << bfd
                << " exhaustive=" << exact << '\n';
    }
  }

  std::cout << "selfcheck: " << instances << " instances, " << mismatches
            << " mismatches\n";
  return mismatches == 0 ? 0 : 1;
}


struct Aggregate
{
  int successes = 0;
  int64_t nodesExamined = 0;
  int64_t backtracks = 0;
  int64_t minFeasibleOnSuccess = 0;
  int64_t minFeasibleOnFailure = 0;

  void record(bool success, const Effort& effort, int minFeasible)
  {
    successes += success ? 1 : 0;
    nodesExamined += effort.nodesExamined;
    backtracks += effort.backtracks;
    (success ? minFeasibleOnSuccess : minFeasibleOnFailure) += minFeasible;
  }
};


void report(
    const std::string& strategy,
    int gangSize,
    double utilization,
    double correlation,
    int trials,
    const Aggregate& a)
{
  const int failures = trials - a.successes;

  std::cout << strategy << ',' << gangSize << ',' << utilization << ','
            << correlation << ','
            << static_cast<double>(a.successes) / trials << ','
            << static_cast<double>(a.nodesExamined) / trials << ','
            << static_cast<double>(a.backtracks) / trials << ','
            << (a.successes > 0
                  ? static_cast<double>(a.minFeasibleOnSuccess) / a.successes
                  : -1.0)
            << ','
            << (failures > 0
                  ? static_cast<double>(a.minFeasibleOnFailure) / failures
                  : -1.0)
            << '\n';
}

} // namespace


int main(int argc, char** argv)
{
  Config config;

  for (int i = 1; i < argc; ++i) {
    const std::string flag = argv[i];
    if (flag == "--selfcheck") {
      return selfcheck(config.seed);
    }
    if (i + 1 >= argc) {
      std::cerr << "missing value for flag: " << flag << '\n';
      return 1;
    }
    const int value = std::stoi(argv[++i]);
    if (flag == "--nodes") config.nodes = value;
    else if (flag == "--trials") config.trials = value;
    else if (flag == "--seed") config.seed = static_cast<unsigned int>(value);
    else {
      std::cerr << "unknown flag: " << flag << '\n';
      return 1;
    }
  }

  const int gangSizes[] = {2, 4, 8, 16, 24, 32, 48, 64};
  const double utilizations[] = {0.50, 0.70, 0.85};
  const double correlations[] = {0.0, 0.5, 1.0};

  std::cout << "strategy,gang_size,utilization,correlation,feasibility_rate,"
               "mean_nodes_examined,mean_backtracks,"
               "mean_min_feasible_on_success,mean_min_feasible_on_failure\n";

  for (double correlation : correlations) {
    for (double utilization : utilizations) {
      for (int gangSize : gangSizes) {
        std::mt19937 rng(config.seed);

        Aggregate naive;
        Aggregate anchor;
        Aggregate reference;

        for (int trial = 0; trial < config.trials; ++trial) {
          const std::vector<Node> cluster =
            makeCluster(config, utilization, rng);
          const std::vector<Member> gang =
            makeGang(gangSize, correlation, rng);

          // The bottleneck predictor: the smallest feasible set among
          // members at scenario start, before any placement.
          Effort scratch;
          int minFeasible = config.nodes;
          for (const Member& member : gang) {
            minFeasible =
              std::min(minFeasible, feasibleCount(cluster, member, &scratch));
          }

          Effort naiveEffort;
          const bool naiveOk = placeNaive(cluster, gang, &naiveEffort);
          naive.record(naiveOk, naiveEffort, minFeasible);

          Effort anchorEffort;
          const bool anchorOk = placeAnchor(cluster, gang, &anchorEffort);
          anchor.record(anchorOk, anchorEffort, minFeasible);

          Effort referenceEffort;
          const bool referenceOk =
            placeBestFitDecreasing(cluster, gang, &referenceEffort);
          reference.record(referenceOk, referenceEffort, minFeasible);
        }

        report("naive", gangSize, utilization, correlation,
               config.trials, naive);
        report("anchor", gangSize, utilization, correlation,
               config.trials, anchor);
        report("bfd", gangSize, utilization, correlation,
               config.trials, reference);
      }
    }
  }

  return 0;
}
