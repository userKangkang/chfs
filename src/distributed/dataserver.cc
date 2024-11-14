#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 0, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });
  server_->bind("get_block_size", [this]() {
    return this->get_block_size();
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);

  // allocate some version block first.
  usize block_size = block_allocator_->bm->block_size();
  usize block_num = block_allocator_->bm->total_blocks();
  usize version_block_num = block_num * sizeof(u32) / block_size;
  for(int i = 0; i < version_block_num; i++) {
    version_blocks.push_back(block_allocator_->allocate().unwrap());
  }
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { 
  usize version_blocks_num = version_blocks.size();
  for(int i = 0; i < version_blocks_num; i++) {
    block_allocator_->deallocate(version_blocks[i]);
  }
  server_.reset();
}

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
  if(!block_allocator_->check_valid(block_id)) {
    return {};
  }
  if(version != -1 && version != get_block_version(block_id)) {
    std::cout << "version: " << version << " block_version: " << get_block_version(block_id) << std::endl;
    return {};
  }
  std::vector<u8> block_data;
  block_data.resize(block_allocator_->bm->block_size());
  block_allocator_->bm->read_block(block_id, block_data.data());
  std::vector<u8> res_data;
  for(usize i = offset; i < offset + len; i++) {
    res_data.push_back(block_data[i]);
  }

  return res_data;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  if(!block_allocator_->check_valid(block_id)) {
    return false;
  }
  auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
  return res.is_ok();
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  auto res = block_allocator_->allocate().unwrap();
  version_t new_version = get_block_version(res) + 1;
  update_block_version(res, new_version);
  return {res, new_version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  block_allocator_->bm->zero_block(block_id);
  auto res = block_allocator_->deallocate(block_id);
  update_block_version(block_id, get_block_version(block_id) + 1);
  return res.is_ok();
}

auto DataServer::get_block_size() -> usize {
  return block_allocator_->bm->block_size();
}

auto DataServer::get_block_version(block_id_t bid) -> version_t {
  usize version_block_index = bid / (get_block_size() / sizeof(u32));
  usize version_block_offset = (bid % (get_block_size() / sizeof(u32))) * sizeof(u32);
  auto res = read_data(version_blocks[version_block_index], version_block_offset, sizeof(u32), -1);
  version_t version = (version_t)(*res.data());
  return version;
}

auto DataServer::update_block_version(block_id_t bid, version_t version) -> void {
  usize version_block_index = bid / (get_block_size() / sizeof(u32));
  usize version_block_offset = (bid % (get_block_size() / sizeof(u32))) * sizeof(u32);
  std::vector<u8> update_vector;
  update_vector.resize(4);
  memcpy(update_vector.data(), &version, sizeof(u32));
  write_data(version_blocks[version_block_index], version_block_offset, update_vector);
}
} // namespace chfs