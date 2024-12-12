#pragma once

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <stdarg.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace chfs {

enum class RaftRole { Follower, Candidate, Leader };

struct RaftNodeConfig {
  int node_id;
  uint16_t port;
  std::string ip_address;
};

template <typename StateMachine, typename Command> class RaftNode {

#define RAFT_LOG(fmt, args...)                                                 \
  do {                                                                         \
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(          \
                   std::chrono::system_clock::now().time_since_epoch())        \
                   .count();                                                   \
    char buf[512];                                                             \
    sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now,       \
            __FILE__, __LINE__, my_id, current_term, role, ##args);            \
    thread_pool->enqueue([=]() { std::cerr << buf; });                         \
  } while (0);

public:
  RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
  ~RaftNode();

  /* interfaces for test */
  void set_network(std::map<int, bool> &network_availablility);
  void set_reliable(bool flag);
  int get_list_state_log_num();
  int rpc_count();
  std::vector<u8> get_snapshot_direct();

private:
  /*
   * Start the raft node.
   * Please make sure all of the rpc request handlers have been registered
   * before this method.
   */
  auto start() -> int;

  /*
   * Stop the raft node.
   */
  auto stop() -> int;

  /* Returns whether this node is the leader, you should also return the current
   * term. */
  auto is_leader() -> std::tuple<bool, int>;

  /* Checks whether the node is stopped */
  auto is_stopped() -> bool;

  /*
   * Send a new command to the raft nodes.
   * The returned tuple of the method contains three values:
   * 1. bool:  True if this raft node is the leader that successfully appends
   * the log, false If this node is not the leader.
   * 2. int: Current term.
   * 3. int: Log index.
   */
  auto new_command(std::vector<u8> cmd_data, int cmd_size)
      -> std::tuple<bool, int, int>;

  /* Save a snapshot of the state machine and compact the log. */
  auto save_snapshot() -> bool;

  /* Get a snapshot of the state machine */
  auto get_snapshot() -> std::vector<u8>;

  /* Internal RPC handlers */
  auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
  auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
  auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

  /* RPC helpers */
  void send_request_vote(int target, RequestVoteArgs arg);
  void handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                 const RequestVoteReply reply);

  void send_append_entries(int target, AppendEntriesArgs<Command> arg);
  void handle_append_entries_reply(int target,
                                   const AppendEntriesArgs<Command> arg,
                                   const AppendEntriesReply reply);

  void send_install_snapshot(int target, InstallSnapshotArgs arg);
  void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                     const InstallSnapshotReply reply);

  /* background workers */
  void run_background_ping();
  void run_background_election();
  void run_background_commit();
  void run_background_apply();

  /* Data structures */
  bool network_stat; /* for test */

  std::mutex mtx;         /* A big lock to protect the whole data structure. */
  std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
  std::unique_ptr<StateMachine> state; /*  The state machine that applies the
                                          raft log, e.g. a kv store. */

  std::unique_ptr<RpcServer>
      rpc_server; /* RPC server to recieve and handle the RPC requests. */
  std::map<int, std::unique_ptr<RpcClient>>
      rpc_clients_map; /* RPC clients of all raft nodes including this node. */
  std::vector<RaftNodeConfig> node_configs; /* Configuration for all nodes */
  int my_id; /* The index of this node in rpc_clients, start from 0. */

  std::atomic_bool stopped;

  RaftRole role;
  int current_term;
  int leader_id;

  std::unique_ptr<std::thread> background_election;
  std::unique_ptr<std::thread> background_ping;
  std::unique_ptr<std::thread> background_commit;
  std::unique_ptr<std::thread> background_apply;

  /* Lab3: Your code here */
  int commit_idx{-1};
  int voted_for{-1};
  std::vector<bool> vote_result;
  std::vector<bool> match_count;
  uint64_t election_timer;
  uint64_t follower_timeout;
  uint64_t candidate_timeout;
  std::vector<log_entry<Command>> log_entries;

  // for leaders
  std::vector<int> match_idx;
  int last_applied_idx{0};

  uint64_t get_current_timer() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  void start_election();
  void send_heartbeat();
  void get_random_timeout();
  void convert_to_follower(int term);
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id,
                                          std::vector<RaftNodeConfig> configs)
    : network_stat(true), node_configs(configs), my_id(node_id), stopped(true),
      role(RaftRole::Follower), current_term(0), leader_id(-1) {
  auto my_config = node_configs[my_id];

  /* launch RPC server */
  rpc_server =
      std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

  /* Register the RPCs. */
  rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
  rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
  rpc_server->bind(RAFT_RPC_CHECK_LEADER,
                   [this]() { return this->is_leader(); });
  rpc_server->bind(RAFT_RPC_IS_STOPPED,
                   [this]() { return this->is_stopped(); });
  rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                   [this](std::vector<u8> data, int cmd_size) {
                     return this->new_command(data, cmd_size);
                   });
  rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT,
                   [this]() { return this->save_snapshot(); });
  rpc_server->bind(RAFT_RPC_GET_SNAPSHOT,
                   [this]() { return this->get_snapshot(); });

  rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) {
    return this->request_vote(arg);
  });
  rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) {
    return this->append_entries(arg);
  });
  rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) {
    return this->install_snapshot(arg);
  });

  /* Lab3: Your code here */
  thread_pool = std::make_unique<ThreadPool>(32);
  state = std::make_unique<StateMachine>();
  election_timer = get_current_timer();
  get_random_timeout();
  match_idx.resize(configs.size(), 0);

  std::shared_ptr<BlockManager> bm = std::make_shared<BlockManager>(
      "/tmp/raft_log/node_" + std::to_string(my_id));
  log_storage = std::make_unique<RaftLog<Command>>(bm);

  log_entries.push_back(log_entry<Command>(0, 0, Command()));
  match_count.resize(configs.size(), false);
  vote_result.resize(configs.size(), false);

  rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode() {
  stop();

  thread_pool.reset();
  rpc_server.reset();
  state.reset();
  log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
  /* Lab3: Your code here */
  RAFT_LOG("start node");

  std::map<int, bool> network;
  for (auto &&config : node_configs) {
    network[config.node_id] = true;
  }
  set_network(network);

  stopped.store(false);

  background_election =
      std::make_unique<std::thread>(&RaftNode::run_background_election, this);
  background_ping =
      std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
  background_commit =
      std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
  background_apply =
      std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
  /* Lab3: Your code here */
  stopped.store(true);
  if (background_election) {
    background_election->join();
  }
  if (background_ping) {
    background_ping->join();
  }
  if (background_commit) {
    background_commit->join();
  }
  if (background_apply) {
    background_apply->join();
  }
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
  /* Lab3: Your code here */
  if (role == RaftRole::Leader) {
    return std::make_tuple(true, current_term);
  }
  return std::make_tuple(false, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
  return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data,
                                                  int cmd_size)
    -> std::tuple<bool, int, int> {
  /* Lab3: Your code here */

  std::unique_lock<std::mutex> lock(mtx);

  bool flag = false;

  int log_idx = log_entries.back().index + 1;

  if (role == RaftRole::Leader) {
    flag = true;
    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);

    log_entry<Command> entry(current_term, log_idx, cmd);
    log_entries.push_back(entry);

    match_idx[my_id] += 1;
    match_count[my_id] = true;
  }
  return std::make_tuple(flag, current_term, log_idx);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
  /* Lab3: Your code here */
  return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  return state->snapshot();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::start_election() {
  /* Lab3: Your code here */
  RAFT_LOG("start election, term %d", current_term);

  // if the node is not stopped, it will become candidate.
  role = RaftRole::Candidate;
  current_term++;
  voted_for = my_id;

  vote_result.assign(rpc_clients_map.size(), false);
  vote_result[my_id] = true;

  get_random_timeout();
  // send request vote to all the nodes.
  for (auto &&client : rpc_clients_map) {
    if (client.first != my_id) {
      RequestVoteArgs arg(current_term, my_id, log_entries.back().index,
                          log_entries.back().term);

      thread_pool->enqueue(&RaftNode::send_request_vote, this, client.first,
                           arg);
    }
  }
  election_timer = get_current_timer();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_heartbeat() {
  /* Lab3: Your code here */

  if (role == RaftRole::Leader) {
    // if the node is leader, it will send heartbeat to all the nodes.
    for (auto &&client : rpc_clients_map) {
      if (client.second && client.first != my_id) {
        AppendEntriesArgs<Command> arg(current_term, my_id,
                                       log_entries.back().index,
                                       log_entries.back().term, {}, commit_idx);
        thread_pool->enqueue(&RaftNode::send_append_entries, this, client.first,
                             arg);
      }
    }
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::get_random_timeout() {
  /* Lab3: Your code here */
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> can(150, 300);
  std::uniform_int_distribution<> fol(700, 900);

  candidate_timeout = can(gen);
  follower_timeout = fol(gen);
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::convert_to_follower(int term) {
  /* Lab3: Your code here */

  role = RaftRole::Follower;
  current_term = term;
  voted_for = -1;
}
/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args)
    -> RequestVoteReply {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  election_timer = get_current_timer();

  if (args.term < current_term) {
    return RequestVoteReply(current_term, false);
  }
  if (args.term > current_term) {
    convert_to_follower(args.term);
  }
  if (voted_for < 0 || voted_for == args.candidateId) {
    if (args.lastLogTerm > log_entries.back().term ||
        (args.lastLogTerm == log_entries.back().term &&
         args.lastLogIndex >= log_entries.back().index)) {
      RAFT_LOG("vote for node %d, term %d", args.candidateId, current_term);
      voted_for = args.candidateId;

      return RequestVoteReply(args.term, true);
    }
  }
  return RequestVoteReply(current_term, false);
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    int target, const RequestVoteArgs arg, const RequestVoteReply reply) {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);

  election_timer = get_current_timer();

  if (reply.term > current_term) {
    convert_to_follower(reply.term);
    return;
  }
  // otherwise it will count the vote.
  if (role == RaftRole::Candidate && reply.voteGranted) {
    // count the vote
    vote_result[target] = true;
    int votes = std::accumulate(vote_result.begin(), vote_result.end(), 0);
    // if votes count is larger than half of the nodes, it will become leader.
    if (votes > node_configs.size() / 2) {
      role = RaftRole::Leader;
      RAFT_LOG("become leader, term %d", current_term);
      send_heartbeat();
      vote_result.assign(node_configs.size(), false);
    }
  }
  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(
    RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
  /* Lab3: Your code here */

  std::unique_lock<std::mutex> lock(mtx);

  election_timer = get_current_timer();

  leader_id = rpc_arg.leaderId;

  AppendEntriesArgs<Command> args =
      transform_rpc_append_entries_args<Command>(rpc_arg);

  if (args.term < current_term) {
    return AppendEntriesReply(current_term, false);
  }
  // if this is candidate, it must become follower.
  if (args.term > current_term || role == RaftRole::Candidate) {
    convert_to_follower(args.term);
  }
  // heartbeat
  if (args.entries.size() == 0) {
    vote_result.assign(vote_result.size(), false);
    if (args.leaderCommit > commit_idx) {
      commit_idx = std::min(rpc_arg.leaderCommit, log_entries.back().index);
    }
    return AppendEntriesReply(current_term, true);
  }
  // if log doesn't contain an entry at prevLogIndex whose term matches
  // prevLogTerm, return false.
  auto prev_entry_iter = std::find_if(log_entries.begin(), log_entries.end(),
                                      [args](const log_entry<Command> &entry) {
                                        return entry.index == args.prevLogIndex;
                                      });
  log_entry<Command> prev_entry = *prev_entry_iter;
  if (prev_entry.term != args.prevLogTerm) {
    return AppendEntriesReply(current_term, false);
  }
  // if an existing entry conflicts with a new one (same index but different
  // terms), delete the existing entry and all that follow it.
  if (args.prevLogIndex < log_entries.back().index) {
    log_entries.erase(log_entries.begin() + args.prevLogIndex + 1,
                      log_entries.end());
  }
  // append any new entries not already in the log
  log_entries.insert(log_entries.end(), args.entries.begin(),
                     args.entries.end());
  // if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of
  // last new entry)
  if (args.leaderCommit > commit_idx) {
    commit_idx = std::min(rpc_arg.leaderCommit, log_entries.back().index);
  }
  return AppendEntriesReply(current_term, true);
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    int node_id, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);

  election_timer = get_current_timer();

  if (reply.term > current_term) {
    convert_to_follower(reply.term);
    return;
  }
  if (role != RaftRole::Leader) {
    return;
  }
  // heartbeat.
  if (arg.entries.empty()) {
    return;
  }
  if (reply.success) {
    match_idx[node_id] = std::max(match_idx[node_id],
                                  (int)arg.entries.size() + arg.prevLogIndex);
    match_count[node_id] = true;
    int matched_num =
        std::accumulate(match_count.begin(), match_count.end(), 0);
    if (matched_num > match_count.size() / 2) {
      commit_idx = std::max(commit_idx, match_idx[my_id]);
      match_count.assign(match_count.size(), false);
    }

  } else {
  }
  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args)
    -> InstallSnapshotReply {
  /* Lab3: Your code here */
  return InstallSnapshotReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(
    int node_id, const InstallSnapshotArgs arg,
    const InstallSnapshotReply reply) {
  /* Lab3: Your code here */
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id,
                                                        RequestVoteArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }
  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_request_vote_reply(target_id, arg,
                              res.unwrap()->as<RequestVoteReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(
    int target_id, AppendEntriesArgs<Command> arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_append_entries_reply(target_id, arg,
                                res.unwrap()->as<AppendEntriesReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(
    int target_id, InstallSnapshotArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_install_snapshot_reply(target_id, arg,
                                  res.unwrap()->as<InstallSnapshotReply>());
  } else {
    // RPC fails
  }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
  // Periodly check the liveness of the leader.

  // Work for followers and candidates.

  std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      lock.lock();
      uint64_t current_timer = get_current_timer();
      if (role == RaftRole::Follower) {
        if (current_timer - election_timer > follower_timeout) {
          start_election();
        }
      } else if (role == RaftRole::Candidate) {
        if (current_timer - election_timer > candidate_timeout) {
          start_election();
        }
      }
      lock.unlock();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
  // Periodly send logs to the follower.

  // Only work for the leader.

  std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      lock.lock();
      if (role == RaftRole::Leader) {
        // if the node is leader, it will send logs to all the nodes.
        for (auto &&client : rpc_clients_map) {
          if (client.second && client.first != my_id) {
            int lbound_idx = match_idx[client.first];
            std::vector<log_entry<Command>> entries;
            entries.assign(log_entries.begin() + lbound_idx + 1,
                           log_entries.end());
            if (!entries.empty()) {
              AppendEntriesArgs<Command> arg(
                  current_term, my_id, log_entries[lbound_idx].index,
                  log_entries[lbound_idx].term, entries, commit_idx);
              thread_pool->enqueue(&RaftNode::send_append_entries, this,
                                   client.first, arg);
            }
          }
        }
      }
      lock.unlock();

      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
  // Periodly apply committed logs the state machine

  // Work for all the nodes.
  std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      lock.lock();
      if (commit_idx > last_applied_idx) {
        for (int i = last_applied_idx + 1; i <= commit_idx; i++) {
          state->apply_log(log_entries[i].command);
        }
        last_applied_idx = commit_idx;
      }

      lock.unlock();

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
  // Periodly send empty append_entries RPC to the followers.

  // Only work for the leader.
  std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      lock.lock();
      if (role == RaftRole::Leader) {
        // if the node is leader, it will send heartbeat to all the nodes.
        send_heartbeat();
      }
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
  }

  return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(
    std::map<int, bool> &network_availability) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  /* turn off network */
  if (!network_availability[my_id]) {
    for (auto &&client : rpc_clients_map) {
      if (client.second != nullptr)
        client.second.reset();
    }

    return;
  }

  for (auto node_network : network_availability) {
    int node_id = node_network.first;
    bool node_status = node_network.second;

    if (node_status && rpc_clients_map[node_id] == nullptr) {
      RaftNodeConfig target_config;
      for (auto config : node_configs) {
        if (config.node_id == node_id)
          target_config = config;
      }

      rpc_clients_map[node_id] = std::make_unique<RpcClient>(
          target_config.ip_address, target_config.port, true);
    }

    if (!node_status && rpc_clients_map[node_id] != nullptr) {
      rpc_clients_map[node_id].reset();
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      client.second->set_reliable(flag);
    }
  }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num() {
  /* only applied to ListStateMachine*/
  std::unique_lock<std::mutex> lock(mtx);

  return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count() {
  int sum = 0;
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      sum += client.second->count();
    }
  }

  return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
  if (is_stopped()) {
    return std::vector<u8>();
  }

  std::unique_lock<std::mutex> lock(mtx);

  return state->snapshot();
}

} // namespace chfs