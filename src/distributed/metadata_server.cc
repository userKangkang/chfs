#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // make a node based on its type.
  inode_id_t inode_id = 0;
  if(type == 1) {
    // regular file inode must get changed in metadata server.
    inode_id = operation_->mkfile(parent, name.data()).unwrap();
  } else {
    inode_id = operation_->mkdir(parent, name.data()).unwrap();
  }
  // inode is metadata, not need to store in data_server. 
  
  return inode_id;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  auto res = operation_->unlink(parent, name.data()).is_ok();
  // if not ok, this is a directory... then?

  return res;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  auto res = operation_->lookup(parent, name.data());

  return res.is_ok() ? res.unwrap() : 0;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  // get the block id of metadata server.
  block_id_t metadata_bid = operation_->inode_manager_->get(id).unwrap();
  // read the inode data
  usize block_size = operation_->block_manager_->block_size();
  u8 inode_data[block_size];
  operation_->block_manager_->read_block(metadata_bid, inode_data);
  Inode *inode_ptr = (Inode*)(inode_data);

  usize block_num = inode_ptr->get_block_map_num_metadata();
  std::vector<BlockInfo> res;
  for(int i = 0; i < block_num; i += 2) {
    res.push_back(inode_ptr->get_tuple_metadata(i));
  }

  return res;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  // allocate blocks in one data server first.
  mac_id_t randomed_mac_id = generator.rand(1, num_data_servers);
  auto res = clients_[randomed_mac_id]->call("alloc_block");
  // get return result.
  auto pair = res.unwrap()->as<std::pair<block_id_t, version_t>>();
  // get the block id of metadata server.
  block_id_t metadata_bid = operation_->inode_manager_->get(id).unwrap();
  // read the inode data
  usize block_size = operation_->block_manager_->block_size();
  u8 inode_data[block_size];
  operation_->block_manager_->read_block(metadata_bid, inode_data);
  Inode *inode_ptr = (Inode*)(inode_data);
  // how to check the inode's block array's current index?
  // maybe we should fold it in the `Inode`
  inode_ptr->set_block_direct_metadata(pair.first, randomed_mac_id, pair.second);
  operation_->block_manager_->write_block(metadata_bid, inode_data);
  
  return {pair.first, randomed_mac_id, pair.second};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  // free block in data server.
  auto free_data_server = clients_[machine_id]->call("free_block", block_id);
  auto data_res = free_data_server.unwrap()->as<bool>();
  if(!data_res) {
    std::cout << "can't free block in data server." << std::endl;
    return false;
  }
  // free block in metadata server.
  // get the block id of metadata server.
  block_id_t metadata_bid = operation_->inode_manager_->get(id).unwrap();
  // read the inode data
  usize block_size = operation_->block_manager_->block_size();
  u8 inode_data[block_size];
  operation_->block_manager_->read_block(metadata_bid, inode_data);
  Inode *inode_ptr = (Inode*)(inode_data);
  bool metadata_res = inode_ptr->free_certain_block(block_id, machine_id);
  operation_->block_manager_->write_block(metadata_bid, inode_data);
  return metadata_res;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  std::list<chfs::DirectoryEntry> read_res;
  read_directory(operation_.get(), node, read_res);

  std::vector<std::pair<std::string, inode_id_t>> return_res;
  for(auto entry: read_res) {
    return_res.push_back({entry.name, entry.id});
  }

  return return_res;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  block_id_t metadata_bid = operation_->inode_manager_->get(id).unwrap();
  // read the inode data
  usize block_size = operation_->block_manager_->block_size();
  u8 inode_data[block_size];
  operation_->block_manager_->read_block(metadata_bid, inode_data);
  Inode *inode_ptr = (Inode*)(inode_data);
  FileAttr attr = inode_ptr->get_attr();
  return {attr.size, attr.atime, attr.ctime, attr.mtime, (u8)inode_ptr->get_type()};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs