#include <bits/types/FILE.h>
#include <iostream>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <tuple>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>

#include "common/config.h"
#include "distributed/client.h"
#include "map_reduce/protocol.h"

using namespace chfs;

namespace mapReduce {

    int Worker::worker_count = 1;

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
        worker_id = worker_count++;
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.
        inode_id_t mapped_inode_id = chfs_client->lookup(1, filename).unwrap();
        auto attr_res = chfs_client->get_type_attr(mapped_inode_id);
        auto length = attr_res.unwrap().second.size;
        auto read_res = chfs_client->read_file(mapped_inode_id, 0, length);
        auto read_res_content = read_res.unwrap();
        std::string read_content = std::string(read_res_content.begin(), read_res_content.end());
        std::vector<KeyVal> mapped = Map(read_content);
        std::string content_out;
        for (const auto &keyVal : mapped) {
            content_out += keyVal.key + " " + std::to_string(keyVal.val) + "\n";
        }
        std::string if_filename = "mr-" + std::to_string(index) + "-0";
        inode_id_t if_inode_id = chfs_client->mknode(ChfsClient::FileType::REGULAR, 1, if_filename).unwrap();
        std::vector<u8> content_out_vu8(content_out.begin(), content_out.end());
        chfs_client->write_file(if_inode_id, 0, content_out_vu8);

        doSubmit(MAP, index, if_filename);
    }

    void Worker::doReduce(int index, std::vector<std::string> &reduce_files) {
        // Lab4: Your code goes here.
        std::vector<std::vector<KeyVal>> key_vals;
        // read the content of each file, and push them to key_vals
        if (reduce_files.empty() || (reduce_files.size() == 1 && reduce_files[0].compare(outPutFile) == 0)) {
            doSubmit(REDUCE, worker_id, "***finish***");
            return;
        }
        for (const auto &file : reduce_files) {
            if (file.compare(outPutFile) == 0 || file.compare("") == 0) {
                continue;
            }
            inode_id_t reduce_inode_id = chfs_client->lookup(1, file).unwrap();
            auto attr_res = chfs_client->get_type_attr(reduce_inode_id);
            auto length = attr_res.unwrap().second.size;
            auto read_res = chfs_client->read_file(reduce_inode_id, 0, length);
            auto read_res_content = read_res.unwrap();
            std::string read_content = std::string(read_res_content.begin(), read_res_content.end());
            std::stringstream stringstream(read_content);
            std::string key;
            u32 value;
            std::vector<KeyVal> key_val;
            while (stringstream >> key >> value) {
                key_val.emplace_back(key, value);
            }
            key_vals.push_back(key_val);
        }
        std::vector<KeyVal> reduced = Reduce(key_vals);
        // read the result file, so we can merge the result with new pairs.

        inode_id_t result_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        auto attr_res = chfs_client->get_type_attr(result_inode_id);
        auto length = attr_res.unwrap().second.size;
        auto read_res = chfs_client->read_file(result_inode_id, 0, length);
        auto read_res_content = read_res.unwrap();
        std::string read_content = std::string(read_res_content.begin(), read_res_content.end());
        std::stringstream stringstream(read_content);
        std::string key, value;
        std::unordered_map<std::string, int> reduced_map;
        while (stringstream >> key >> value) {
            reduced_map[key] = std::stoul(value);
        }
        for (const auto &keyVal : reduced) {
            reduced_map[keyVal.key] += keyVal.val;
        }
        std::string content_out;
        for (const auto &pair : reduced_map) {
            content_out += pair.first + " " + std::to_string(pair.second) + "\n";
        }
        std::vector<u8> content_out_vu8(content_out.begin(), content_out.end());
        chfs_client->write_file(result_inode_id, 0, content_out_vu8);

        doSubmit(REDUCE, index, outPutFile);
    }

    void Worker::doSubmit(mr_tasktype taskType, int index, std::string if_filename) {
        // Lab4: Your code goes here.
        auto submit_res = mr_client->call("submit_task", int(taskType), index, if_filename);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.

            // ask for tasks for a period of time.
            auto ask_res = mr_client->call("ask_task", worker_id);
            auto [taskType, index, taskFiles] = ask_res.unwrap()->get().as<std::tuple<int, int, std::vector<std::string>>>();
            switch (taskType) {
                case MAP: {
                    doMap(index, taskFiles.front());
                    break;
                }
                case REDUCE: {
                    doReduce(index, taskFiles);
                    break;
                }

                default: break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(40));
        }
    }
}