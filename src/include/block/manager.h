//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// manager.h
//
// Identification: src/include/block/manager.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <memory>
#include "common/config.h"
#include "common/macros.h"
#include "common/result.h"

namespace chfs {
// TODO

class BlockIterator;

/**
 * BlockManager implements a block device to read/write block devices
 * Note that the block manager is **not** thread-safe.
 */
class BlockManager {
  friend class BlockIterator;

protected:
  const usize block_sz = 4096;

  const usize redo_log_block_num = 1024;
  const usize redo_metadata_num_per_block = 256; // txn_id | block_id / -1 (commit flag)
  const usize redo_metadata_block_num = 4;

  std::string file_name_;
  int fd;
  u8 *block_data;
  usize block_cnt;
  bool in_memory; // whether we use in-memory to emulate the block manager
  bool maybe_failed;
  usize write_fail_cnt;

  // for redo-log.
  [[maybe_unused]] usize log_start_block{0};
  [[maybe_unused]] std::vector<std::pair<block_id_t, txn_id_t>> log_block_txns;
  [[maybe_unused]] usize log_metadata_block{0};
  [[maybe_unused]] std::shared_ptr<usize> global_txn_number_;
  [[maybe_unused]] bool is_write_fail_per_txn{false};

public:
  /**
   * Creates a new block manager that writes to a file-backed block device.
   * @param block_file the file name of the  file to write to
   */
  explicit BlockManager(const std::string &file);

  /**
   * Creates a new block manager that writes to a file-backed block device.
   * @param block_file the file name of the  file to write to
   * @param block_cnt the number of expected blocks in the device. If the
   * device's blocks are more or less than it, the manager should adjust the
   * actual block cnt.
   */
  BlockManager(const std::string &file, usize block_cnt);

  /**
   * Creates a memory-backed block manager that writes to a memory block device.
   * Note that this is commonly used for testing.
   * Maybe it can be used for non-volatile memory, but who knows.
   *
   * @param block_count the number of blocks in the device
   * @param block_size the size of each block
   */
  BlockManager(usize block_count, usize block_size);

  /**
   * Creates a new block manager that writes to a file-backed block device.
   * It reserves some blocks for recording logs.
   * 
   * @param block_file the file name of the  file to write to
   * @param block_cnt the number of blocks in the device
   * @param is_log_enabled whether to enable log
   */
  BlockManager(const std::string &file, usize block_cnt, bool is_log_enabled
    , std::shared_ptr<usize> global_txn_number = nullptr);
   
  /**
   * Set Coomit Log.
   */

  virtual ~BlockManager();

  /**
   * Write a block to the internal block device.  This is a write-through one,
   * i.e., no cache.
   * @param block_id id of the block
   * @param block_data raw block data
   */
  virtual auto write_block(block_id_t block_id, const u8 *block_data)
      -> ChfsNullResult;

  /**
   * Write a partial block to the internal block device.
   */
  virtual auto write_partial_block(block_id_t block_id, const u8 *block_data,
                                   usize offset, usize len) -> ChfsNullResult;

  /**
   * Read a block to the internal block device.
   * @param block_id id of the block
   * @param block_data raw block data buffer to store the result
   */
  virtual auto read_block(block_id_t block_id, u8 *block_data)
      -> ChfsNullResult;

  /**
   * Clear the content of a block
   * @param block_id id of the block
   */
  virtual auto zero_block(block_id_t block_id) -> ChfsNullResult;

  auto total_storage_sz() const -> usize {
    return this->block_cnt * this->block_sz;
  }

  /**
   * Get the total number of blocks in the block manager
   */
  auto total_blocks() const -> usize { return this->block_cnt; }

  /**
   * Get the block size of the device managed by the manager
   */
  auto block_size() const -> usize { return this->block_sz; }

  /**
   * Get the block data pointer of the manager
   */
  auto unsafe_get_block_ptr() const -> u8 * { return this->block_data; }

  /**
   * flush the data of a block into disk
   */
  auto sync(block_id_t block_id) -> ChfsNullResult;

  /**
   * Flush the page cache
   */
  auto flush() -> ChfsNullResult;

  /**
   * Mark the block manager as may fail state
   */
  auto set_may_fail(bool may_fail) -> void {
    this->maybe_failed = may_fail;
  }

  /**
   * Append redo-log.
   * 
   * @param txn_id: transaction id of the modification of block.
   * @param block_id: id of modified block.
   * @param vector: modified data of the block.
   */
  auto append_redo_log(txn_id_t txn_id, block_id_t block_id, const u8* vector) -> void;

  /**
   * Recover data by redo-log.
   */
  auto recover() -> void;

  /**
   * Return whether the write error occurs in single tnx,
   * and then set it false.
   */
  auto is_write_fail_txn() -> bool {
    if(!maybe_failed) {
      return false;
    }
    if(is_write_fail_per_txn) {
      is_write_fail_per_txn = false;
      return true;
    }
    return false;
  }
};

/**
 * A class to simplify iterating blocks in the block manager.
 *
 * Note that we don't provide a conventional iterator interface, because
 * each block read/write may return error due to failed reading/writing blocks.
 */
class BlockIterator {
  BlockManager *bm;
  u64 cur_block_off;
  block_id_t start_block_id;
  block_id_t end_block_id;

  std::vector<u8> buffer;

public:
  /**
   * Creates a new block iterator.
   *
   * @param bm the block manager to iterate
   * @param start_block_id the start block id of the iterator
   * @param end_block_id the end block id of the iterator
   */
  static auto create(BlockManager *bm, block_id_t start_block_id,
                     block_id_t end_block_id) -> ChfsResult<BlockIterator>;

  /**
   * Iterate to the cur_block_off to an offset
   *
   * **Assumption**: a previous call of has_next() returns true
   *
   * @param offset the offset to iterate to
   *
   * @return Ok(the iterator itself)
   *         Err(DONE) // the iteration is done, i.e., we have passed the
   * end_block_id Other errors
   */
  auto next(usize offset) -> ChfsNullResult;

  auto has_next() -> bool {
    return this->cur_block_off <
           (this->end_block_id - this->start_block_id) * bm->block_sz;
  }

  /**
   *  Assumption: a prior call of has_next() must return true
   */
  auto flush_cur_block() -> ChfsNullResult {
    auto target_block_id =
        this->start_block_id + this->cur_block_off / bm->block_sz;
    return this->bm->write_block(target_block_id, this->buffer.data());
  }

  auto get_cur_byte() const -> u8 {
    return this->buffer[this->cur_block_off % bm->block_sz];
  }

  template <typename T> auto unsafe_get_value_ptr() -> T * {
    return reinterpret_cast<T *>(this->buffer.data() +
                                 this->cur_block_off % bm->block_sz);
  }
};

} // namespace chfs
