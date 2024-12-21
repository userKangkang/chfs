#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

using chfs::u8;

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here

        // Read the content of each file, and push them to mapped vector
        std::string content;
        std::vector<std::vector<KeyVal>> mapped;
        for (const auto &file : files) {
            auto res_lookup = chfs_client->lookup(1, file);
            auto inode_id = res_lookup.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, length);
            auto char_vec = res_read.unwrap();
            content = std::string(char_vec.begin(), char_vec.end());
            mapped.push_back(Map(content));
            content.clear();
        }

        // Reduce the mapped vector
        std::vector<KeyVal> reduced = Reduce(mapped);

        // Write the reduced vector to the output file
        std::string content_out;
        for (const auto &keyVal : reduced) {
            content_out += keyVal.key + " " + std::to_string(keyVal.val) + "\n";
        }
        auto res_lookup = chfs_client->lookup(1, outPutFile);
        auto inode_id = res_lookup.unwrap();
        std::vector<u8> content_out_vu8(content_out.begin(), content_out.end());
        chfs_client->write_file(inode_id, 0, content_out_vu8);
    }
}