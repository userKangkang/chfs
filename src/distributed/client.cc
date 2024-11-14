#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto mknode_res = metadata_server_->call("mknode", (u32)type, parent, name);
  if(mknode_res.is_err()) {
    return ErrorType::AlreadyExist;
  }
  return ChfsResult<inode_id_t>(mknode_res.unwrap()->as<inode_id_t>());
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto unlink_res = metadata_server_->call("unlink", parent, name);
  if(unlink_res.is_err() || !unlink_res.unwrap()->as<bool>()) {
    return ErrorType::BadResponse;
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto lookup_res = metadata_server_->call("lookup", parent, name);
  if(lookup_res.is_err()) {
    return ErrorType::BadResponse;
  }
  return ChfsResult<inode_id_t>(lookup_res.unwrap()->as<inode_id_t>());
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  auto readdir_res = metadata_server_->call("readdir", id);
  if(readdir_res.is_err()) {
    return ErrorType::BadResponse;
  }
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(
    readdir_res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>()
  );
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  auto get_type_attr_res = metadata_server_->call("get_type_attr", id);
  if(get_type_attr_res.is_err()) {
    return ErrorType::BadResponse;
  }
  auto res = get_type_attr_res.unwrap()->as<std::pair<u32, std::tuple<u64, u64, u64, u64>>>();
  std::pair<InodeType, FileAttr> trans_res;
  trans_res.first = InodeType(res.first);
  auto [atime, mtime, ctime, size] = res.second;
  trans_res.second.atime = atime;
  trans_res.second.ctime = ctime;
  trans_res.second.mtime = mtime;
  trans_res.second.size = size;
  return ChfsResult<std::pair<InodeType, FileAttr>>(
    trans_res
  );
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  auto get_block_res = metadata_server_->call("get_block_map", id);
  if(get_block_res.is_err()) {
    return ErrorType::BadResponse;
  }
  std::vector<BlockInfo> data_blocks = get_block_res.unwrap()->as<std::vector<BlockInfo>>();
  usize rest_size = size, block_size = data_servers_[0]->call("get_block_size").unwrap()->as<usize>();
  std::vector<u8> res_data;
  usize idx = offset / block_size;
  usize read_offset = offset % block_size;
  usize read_size = rest_size > block_size - read_offset ? block_size - read_offset : rest_size;
  while(rest_size) {
    auto [block_id, mac_id, version] = data_blocks[idx];
    std::vector<u8> v = data_servers_[mac_id]->call("read_block", block_id, read_offset, read_size).unwrap()->as<std::vector<u8>>();
    idx++;
    rest_size -= read_size;
    read_offset = 0;
    read_size = rest_size > block_size ? block_size : rest_size;
    res_data.insert(res_data.end(), v.begin(), v.end());
  }
  return ChfsResult<std::vector<u8>>(
    res_data
  );
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto get_block_res = metadata_server_->call("get_block_map", id);
  if(get_block_res.is_err()) {
    return ErrorType::BadResponse;
  }
  std::vector<BlockInfo> data_blocks = get_block_res.unwrap()->as<std::vector<BlockInfo>>();
  usize rest_size = data.size(), block_size = data_servers_[0]->call("get_block_size").unwrap()->as<usize>();
  usize idx = offset / block_size;
  usize write_offset = offset % block_size;
  usize write_size = rest_size > block_size - write_offset ? block_size - write_offset : rest_size;
  usize vector_offset = 0;
  while(rest_size) {
    auto [block_id, mac_id, version] = data_blocks[idx];
    std::vector<u8> v(data.begin() + vector_offset, data.begin() + vector_offset + write_size);
    data_servers_[mac_id]->call("write_block", block_id, write_offset, v).unwrap()->as<std::vector<u8>>();
    idx++;
    rest_size -= write_size;
    vector_offset += write_size;
    write_offset = 0;
    write_size = rest_size > block_size ? block_size : rest_size;
  } 
  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  auto free_file_block_res = metadata_server_->call("unlink", id, block_id, mac_id);
  if(free_file_block_res.is_err() || !free_file_block_res.unwrap()->as<bool>()) {
    return ErrorType::BadResponse;
  }
  return KNullOk;
}

} // namespace chfs