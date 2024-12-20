#pragma once

#include "common/config.h"
#include "common/macros.h"
#include "block/manager.h"
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <cstring>

namespace chfs {

#define BLOCK_SIZE 4096

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
    void read_all(int &current_term, int &voted_for, std::vector<log_entry<Command>> &log_entries);
    void write_metadata(int current_term, int voted_for);
    void write_log(const std::vector<log_entry<Command>> &log_entries);
    
private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    const u32 metadata_block = 0;
    const u32 log_block = 1;
    const u32 snapshot_block = 17;
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm):bm_(bm)
{
    /* Lab3: Your code here */
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
    bm_.reset();
}


/* Lab3: Your code here */

template <typename Command>
void RaftLog<Command>::read_all(int &current_term, int &voted_for, std::vector<log_entry<Command>> &log_entries)
{
    std::lock_guard<std::mutex> lock(mtx);

    u8 metadata_buffer[BLOCK_SIZE];

    bm_->read_block(metadata_block, metadata_buffer);
    int *ptr = reinterpret_cast<int *>(metadata_buffer);
    current_term = ptr[0];
    voted_for = ptr[1];

    int log_num;
    log_num = ptr[2];

    log_entries.clear();

    if (current_term == 0) {
        current_term = 0;
        voted_for = -1;

        log_entries.push_back(log_entry<Command>(0, 0, Command()));
        return;
    }

    const u32 tuple_size = 12; // 4 for index, 4 for term, 4 for command

    u32 log_blocks_num = (log_num * tuple_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    std::vector<u8> log_buffer(BLOCK_SIZE * log_blocks_num);

    for (u32 i = 0; i < log_blocks_num; i++) {
        bm_->read_block(log_block + i, log_buffer.data() + i * BLOCK_SIZE);
    }

    log_buffer.resize(log_num * tuple_size);

    u8 *log_ptr = reinterpret_cast<u8*>(log_buffer.data());


    int term, index;
    for (u32 i = 0; i < log_num; i++) {
        memcpy((void*)(&term), (void*)(log_ptr + i * tuple_size), 4);
        memcpy((void*)(&index), (void*)(log_ptr + i * tuple_size + 4), 4);
        std::vector<u8> command_data(4);
        memcpy((void*)command_data.data(), (void*)(log_ptr + i * tuple_size + 8), 4);
        Command command;
        command.deserialize(command_data, 4);
        log_entries.push_back(log_entry<Command>(term, index, command));
    }

}

template <typename Command>
void RaftLog<Command>::write_metadata(int current_term, int voted_for)
{
    std::lock_guard<std::mutex> lock(mtx);

    u8 metadata_buffer[BLOCK_SIZE];
    bm_->read_block(metadata_block, metadata_buffer);

    int *ptr = reinterpret_cast<int *>(metadata_buffer);
    ptr[0] = current_term;
    ptr[1] = voted_for;

    bm_->write_block(metadata_block, metadata_buffer);
    bm_->sync(metadata_block);

}

template <typename Command>
void RaftLog<Command>::write_log(const std::vector<log_entry<Command>> &log_entries)
{
    std::lock_guard<std::mutex> lock(mtx);

    u8 metadata_buffer[BLOCK_SIZE];

    bm_->read_block(metadata_block, metadata_buffer);

    int log_num = log_entries.size();
    int *ptr = reinterpret_cast<int *>(metadata_buffer);
    ptr[2] = log_num;
    bm_->write_block(metadata_block, metadata_buffer);
    bm_->sync(metadata_block);

    const u32 tuple_size = 12; // 4 for index, 4 for term, 4 for command

    u32 log_blocks_num = (log_num * tuple_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    std::vector<u8> log_buffer(log_num * tuple_size);

    u8 *log_ptr = reinterpret_cast<u8*>(log_buffer.data());

    for (u32 i = 0; i < log_num; i++) {
        memcpy((void*)(log_ptr + i * tuple_size), (void*)(&(log_entries[i].term)), 4);
        memcpy((void*)(log_ptr + i * tuple_size + 4), (void*)(&(log_entries[i].index)), 4);
        std::vector<u8> command_data = log_entries[i].command.serialize(4);
        memcpy((void*)(log_ptr + i * tuple_size + 8), (void*)(command_data.data()), 4);
    }

    u32 remain = log_num * tuple_size;

    for (u32 i = 0; i < log_blocks_num; i++) {
        if (i == log_blocks_num - 1) {
            bm_->write_partial_block(log_block + i, log_buffer.data() + i * BLOCK_SIZE, 0, remain);
            bm_->sync(log_block + i);
            break;
        }
        bm_->write_block(log_block + i, log_buffer.data() + i * BLOCK_SIZE);
        bm_->sync(log_block + i);
        remain -= BLOCK_SIZE;
    }
}

}; // namespace chfs