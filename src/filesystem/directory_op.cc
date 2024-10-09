#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  list.push_back({filename, id});
  src = dir_list_to_string(list);
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  std::istringstream iss(src);
  std::string segment;
  std::string name;
  std::string inode;
  
  while(std::getline(iss, segment, '/')) {
    std::istringstream segment_ss(segment);

    if(std::getline(segment_ss, name, ':') && std::getline(segment_ss, inode)) {
        list.push_back({name, std::stoull(inode)});
    }
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  for(auto it: list) {
    if(it.name.compare(filename) == 0) {
      list.remove_if([it](DirectoryEntry entry) {
        return it.name == entry.name;
      });
      break;
    }
  }
  res = dir_list_to_string(list);

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  std::string file_content(fs->read_file(id).unwrap().begin(), fs->read_file(id).unwrap().end());

  parse_directory(file_content, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  block_id_t bid = inode_manager_->get(id).unwrap();
  std::vector<u8> buffer;
  buffer.reserve(block_manager_->block_size());
  block_manager_->read_block(bid, buffer.data());
  std::string dir(buffer.begin(), buffer.end());
  parse_directory(dir, list);
  for(auto it: list) {
    if(it.name.compare(name) == 0) {
       return ChfsResult<inode_id_t>(it.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;
  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  block_id_t bid = inode_manager_->get(id).unwrap();
  std::vector<u8> buffer;
  buffer.reserve(block_manager_->block_size());
  block_manager_->read_block(bid, buffer.data());
  std::string dir(buffer.begin(), buffer.end());
  parse_directory(dir, list);
  for(auto it: list) {
    if(it.name.compare(name) == 0) {
       return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
  }
  block_id_t alloc_bid = block_allocator_->allocate().unwrap();
  block_id_t iid = inode_manager_->allocate_inode(type, alloc_bid).unwrap();
  list.push_back({name, iid});
  dir = dir_list_to_string(list);
  buffer.insert(buffer.begin(), dir.begin(), dir.end());
  block_manager_->write_block(bid, buffer.data());

  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(iid));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {
  std::list<DirectoryEntry> list;
  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  block_id_t bid = inode_manager_->get(parent).unwrap();
  std::vector<u8> buffer;
  buffer.reserve(block_manager_->block_size());
  block_manager_->read_block(bid, buffer.data());
  std::string dir(buffer.begin(), buffer.end());
  parse_directory(dir, list);
  for(auto it: list) {
    if(it.name.compare(name) == 0) {
      remove_file(it.id);
      list.remove_if([it](DirectoryEntry entry) {
        return it.name == entry.name;
      });
      break;
    }
  }
  
  return KNullOk;
}

} // namespace chfs
