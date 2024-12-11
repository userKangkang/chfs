#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
struct log_entry {
    int term;
    int index;
    Command command;

    log_entry(): term(-1), index(-1) {}

    log_entry(int term, int index, Command command):
        term(term), index(index), command(command) {}
};

template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    /* Lab3: Your code here */
    void append_log_entry(int term, int index, Command command);
    std::vector<log_entry<Command>> get_log_entries(int start_index, int end_index);
    void delete_log_entries(int start_index, int end_index);
    log_entry<Command> get_log_entry_by_index(int index);
    int size() { return log_entries.size(); }
    int get_last_log_term() { return log_entries.empty() ? 0 : log_entries.back().term; }
    int get_last_log_index() { return log_entries.empty() ? 0 : log_entries.back().index; }
private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    std::vector<log_entry<Command>> log_entries;
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
{
    /* Lab3: Your code here */
    bm_ = bm;
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

template <typename Command>
void RaftLog<Command>::append_log_entry(int term, int index, Command command)
{
    /* Lab3: Your code here */
    log_entry<Command> entry;
    entry.term = term;
    entry.index = index;
    entry.command = command;
    log_entries.push_back(entry);
}

template <typename Command>
std::vector<log_entry<Command>> RaftLog<Command>::get_log_entries(int start_index, int end_index)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<log_entry<Command>> entries;
    for (auto &&entry: log_entries) {
        if (entry.index >= start_index && entry.index <= end_index) {
            entries.push_back(entry);
        }
    }
    return entries;
}

template <typename Command>
log_entry<Command> RaftLog<Command>::get_log_entry_by_index(int index)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    for (auto &&entry: log_entries) {
        if (entry.index == index) {
            return entry;
        }
    }
    return log_entry<Command>();
}

template <typename Command>
void RaftLog<Command>::delete_log_entries(int start_index, int end_index)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    for (auto it = log_entries.begin(); it != log_entries.end(); ) {
        if (it->index >= start_index && it->index <= end_index) {
            it = log_entries.erase(it);
        } else {
            it++;
        }
    }
}

/* Lab3: Your code here */

} /* namespace chfs */
