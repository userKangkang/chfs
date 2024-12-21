#include <string>
#include <tuple>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    std::tuple<int, int, std::vector<std::string>> Coordinator::askTask(int worker_id) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        std::vector<std::string> task_files;
        std::unique_lock<std::mutex> uniqueLock(this->mtx, std::defer_lock);
        if (mapped_index < files.size()) {
            uniqueLock.lock();
            task_files.push_back(files[mapped_index++]);
            mapped_success_wid_index[mapped_index] = OnGoing;
            uniqueLock.unlock();
            return std::make_tuple(MAP, mapped_index, task_files);
        } else if (isMapDone()) {
            uniqueLock.lock();
            int if_files_size = if_files.size();
            for (int i = 1; i <= nReduce && i <= if_files_size; i++) {
                std::string reduce_filename = if_files.front();
                task_files.push_back(reduce_filename);
                if_files.pop();
            }
            reduced_index += std::min(nReduce, if_files_size);
            uniqueLock.unlock();
            return std::make_tuple(REDUCE, reduced_index, task_files);
        }
        // wait for all the workers to finish the map task
        return std::make_tuple(NONE, -1, task_files);
    }

    int Coordinator::submitTask(int taskType, int index, std::string if_filename) {
        // Lab4 : Your code goes here.
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        switch (taskType) {
          case MAP:
            mapped_success_wid_index[index] = task_stat::Done;
            if_files.push(if_filename);
            break;
          case REDUCE: {
            if (if_filename.compare("***finish***") == 0) {
                isFinished = true;
                break;
            }
            reduced_success_wid_index[index] = task_stat::Done;
            break;
          }
        }
        return 0;
    }

    bool Coordinator::isMapDone() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        if (mapped_success_wid_index.size() < files.size()) {
            return false;
        }
        for (auto &pair : mapped_success_wid_index) {
            if (pair.second == OnGoing) {
                return false;
            }
        }
        return true;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index, std::string if_filename) { return this->submitTask(taskType, index, if_filename); });
        rpc_server->run(true, 1);
        this->mapped_index = 0;
        this->reduced_index = 0;
        this->nReduce = nReduce;

    }
}