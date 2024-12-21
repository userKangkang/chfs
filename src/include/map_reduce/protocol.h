#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include <queue>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

using chfs::u32;

//Lab4: Free to modify this file

namespace mapReduce {
    struct KeyVal {
        KeyVal(const std::string &key, const u32 &val) : key(key), val(val) {}
        KeyVal(){}
        std::string key;
        u32 val;
    };

    enum mr_tasktype {
        NONE = 0,
        MAP,
        REDUCE
    };

    std::vector<KeyVal> Map(const std::string &content);

    std::vector<KeyVal> Reduce(std::vector<std::vector<KeyVal>> &key_vals);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                       resultFile(resultFile), client(std::move(client)) {}
    };

    class SequentialMapReduce {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
        void doWork();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };

    enum task_stat {
        OnGoing,
        Done
    };

    class Coordinator {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
        std::tuple<int, int, std::vector<std::string>> askTask(int);
        int submitTask(int taskType, int index, std::string if_filename);
        bool Done();
        bool isMapDone();

    private:
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server;

        int mapped_index;
        int reduced_index;
        int nReduce;

        std::map<int, int> mapped_success_wid_index;
        std::map<int, int> reduced_success_wid_index;

        std::queue<std::string> if_files;
    };

    class Worker {
    public:
        static int worker_count;

        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int index, std::vector<std::string> &reduce_files);
        void doSubmit(mr_tasktype taskType, int index, std::string if_filename);

        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;
        bool shouldStop = false;
        int worker_id;
    };
}