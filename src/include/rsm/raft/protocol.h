#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;

    MSGPACK_DEFINE(
        term,
        candidateId,
        lastLogIndex,
        lastLogTerm
    )

    RequestVoteArgs() {}

    RequestVoteArgs(int term, int candidateId, int lastLogIndex, int lastLogTerm):
        term(term), candidateId(candidateId), lastLogIndex(lastLogIndex), lastLogTerm(lastLogTerm) {}
};

struct RequestVoteReply {
    /* Lab3: Your code here */

    int term;
    int voteGranted;

    MSGPACK_DEFINE(
        term,
        voteGranted
    )

    RequestVoteReply() {}

    RequestVoteReply(int term, int voteGranted):
        term(term), voteGranted(voteGranted) {}
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<log_entry<Command>> entries;
    int leaderCommit;

    AppendEntriesArgs() {}

    AppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm, std::vector<log_entry<Command>> entries, int leaderCommit):
        term(term), leaderId(leaderId), prevLogIndex(prevLogIndex), prevLogTerm(prevLogTerm), entries(entries), leaderCommit(leaderCommit) {}
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */

    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<int> entries_index;
    std::vector<int> entries_term;
    std::vector<u8> entries_command;
    int leaderCommit;

    MSGPACK_DEFINE(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries_index,
        entries_term,
        entries_command,
        leaderCommit
    )

    RpcAppendEntriesArgs() {}

    RpcAppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm, std::vector<int> entries_index, std::vector<int> entries_term, std::vector<u8> entries_command, int leaderCommit):
        term(term), leaderId(leaderId), prevLogIndex(prevLogIndex), prevLogTerm(prevLogTerm), entries_index(entries_index), entries_term(entries_term), entries_command(entries_command), leaderCommit(leaderCommit) {}
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */

    std::vector<int> entries_index;
    std::vector<int> entries_term;
    std::vector<u8> entries_command;

    for (auto &&entry: arg.entries) {
        entries_index.push_back(entry.index);
        entries_term.push_back(entry.term);
        entries_command.push_back(entry.command.value);
    }

    return RpcAppendEntriesArgs(
        arg.term,
        arg.leaderId,
        arg.prevLogIndex,
        arg.prevLogTerm,
        entries_index,
        entries_term,
        entries_command,
        arg.leaderCommit
    );
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    std::vector<log_entry<Command>> entries;

    for (int i = 0; i < rpc_arg.entries_index.size(); i++) {
        entries.push_back(log_entry<Command>(rpc_arg.entries_term[i], rpc_arg.entries_index[i], rpc_arg.entries_command[i]));
    }

    return AppendEntriesArgs<Command>(
        rpc_arg.term,
        rpc_arg.leaderId,
        rpc_arg.prevLogIndex,
        rpc_arg.prevLogTerm,
        entries,
        rpc_arg.leaderCommit
    );
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term;
    int success;

    MSGPACK_DEFINE(
        term,
        success
    )

    AppendEntriesReply() {}

    AppendEntriesReply(int term, int success):
        term(term), success(success) {}
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

} /* namespace chfs */