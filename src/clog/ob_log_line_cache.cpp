/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX CLOG

#include "ob_log_line_cache.h"

#include "lib/allocator/ob_malloc.h"  // ob_malloc
#include "lib/ob_running_mode.h"      // mini_mode
#include "lib/oblog/ob_log_module.h"  // LOG_*
#include "lib/container/ob_array.h"   // ObArray
#include "clog/ob_log_utils.h"        // top_k

namespace oceanbase {
using namespace common;
namespace clog {

ObLogLineCache::ObLogLineCache()
    : inited_(false),
      blocks_(nullptr),
      block_array_size_(0),
      block_token_(),
      block_alloc_(),
      line_futex_(),
      block_wash_cond_(),
      cached_line_count_(0),
      get_line_count_(0),
      load_line_count_(0),
      block_hit_count_(0),
      line_hit_count_(0),
      block_conflict_count_(0),
      exceed_mem_limit_count_(0),
      washed_line_count_(0),
      lru_washed_line_count_(0),
      washed_line_read_size_(0),
      repeated_block_load_count_(0),
      repeated_block_load_interval_(0)
{}

int ObLogLineCache::init(
    const int64_t max_cache_file_count, const int64_t fixed_cached_size_in_file_count, const char* label)
{
  int ret = OB_SUCCESS;
  int64_t max_cache_block_count = (max_cache_file_count * BLOCK_COUNT_PER_FILE);
  int64_t prealloc_block_count = (fixed_cached_size_in_file_count * BLOCK_COUNT_PER_FILE);

  Block* blocks = nullptr;
  int64_t block_array_size = !lib::is_mini_mode() ? DEFAULT_BLOCK_ARRAY_SIZE : MINI_MODE_BLOCK_ARRAY_SIZE;
  if (OB_UNLIKELY(inited_)) {
    LOG_WARN("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(max_cache_file_count <= 0) || OB_UNLIKELY(fixed_cached_size_in_file_count <= 0) ||
             OB_UNLIKELY(fixed_cached_size_in_file_count > max_cache_file_count)) {
    LOG_WARN("invalid argument", K(max_cache_file_count), K(fixed_cached_size_in_file_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY((CLOG_FILE_SIZE % BLOCK_SIZE) != 0) || OB_UNLIKELY((BLOCK_SIZE % LINE_SIZE) != 0)) {
    LOG_WARN("invalid block_size or line_size", LITERAL_K(BLOCK_SIZE), LITERAL_K(LINE_SIZE), LITERAL_K(CLOG_FILE_SIZE));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(block_token_.init(max_cache_block_count, label))) {
    LOG_WARN("init block token fail", K(ret), K(max_cache_block_count), K(max_cache_file_count));
  } else if (OB_FAIL(block_alloc_.init(prealloc_block_count, label))) {
    LOG_WARN("init block alloc fail", K(ret), K(prealloc_block_count));
  } else if (OB_ISNULL(blocks = (Block*)ob_malloc(sizeof(Block) * block_array_size, "LogLineBlocks"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc blocks failed", K(ret));
  } else {
    blocks_ = blocks;
    block_array_size_ = block_array_size;
    for (int64_t index = 0; index < block_array_size_; index++) {
      new (&blocks_[index]) Block();
      blocks_[index].reset();
    }
    cached_line_count_ = 0;
    get_line_count_ = 0;
    load_line_count_ = 0;
    block_hit_count_ = 0;
    line_hit_count_ = 0;
    block_conflict_count_ = 0;
    exceed_mem_limit_count_ = 0;
    washed_line_count_ = 0;
    lru_washed_line_count_ = 0;
    washed_line_read_size_ = 0;
    repeated_block_load_count_ = 0;
    repeated_block_load_interval_ = 0;
    inited_ = true;
  }
  return ret;
}

void ObLogLineCache::destroy()
{
  inited_ = false;

  // reclaim all allocated blocks
  if (block_token_.get_used_count() > 0) {
    int64_t total_washed_block_count = 0;
    int64_t total_washed_line_count = 0;
    int64_t cached_block_count_before_wash = block_token_.get_used_count();

    // Compulsory elimination, the expiration time is set to 0
    (void)do_wash_(0, total_washed_block_count, total_washed_line_count);

    // after the elimination, there are still blocks in use
    if (block_token_.get_used_count() > 0) {
      LOG_WARN("Line Cache still have blocks used after force-wash",
          K(total_washed_block_count),
          K(total_washed_line_count),
          K(cached_block_count_before_wash),
          "cached_block_count",
          block_token_.get_used_count());
    }
  }

  block_token_.destroy();
  block_alloc_.destroy();
  cached_line_count_ = 0;
  get_line_count_ = 0;
  load_line_count_ = 0;
  block_hit_count_ = 0;
  line_hit_count_ = 0;
  block_conflict_count_ = 0;
  exceed_mem_limit_count_ = 0;
  washed_line_count_ = 0;
  lru_washed_line_count_ = 0;
  washed_line_read_size_ = 0;
  repeated_block_load_count_ = 0;
  repeated_block_load_interval_ = 0;
  if (blocks_ != nullptr) {
    ob_free(blocks_);
    blocks_ = nullptr;
  }
  block_array_size_ = 0;
}

// block conflict case
// Keep trying to eliminate the target block until the elimination is successful or timeout
int ObLogLineCache::handle_when_block_conflict_(
    Block& block, const int64_t block_index, const file_id_t file_id, const int64_t end_tstamp)
{
  int ret = OB_SUCCESS;
  bool done = false;
  bool wash_succeed = false;
  int64_t block_washed_line_count = 0;
  int64_t block_washed_line_read_size = 0;
  int64_t expire_time = 0;  // expires immediately, compulsory elimination
  TokenValueType block_id = static_cast<TokenValueType>(block_index);

  while (OB_SUCCESS == ret && !done) {
    if (block.is_empty() || file_id == block.get_file_id()) {
      // current block is empty, or the current block is loaded with the data of this file, mark as complete
      done = true;
    } else {
      int64_t cur_tstamp = ObTimeUtility::current_time();

      // execute forced elimination of block
      if (OB_FAIL(do_wash_block_(
              block_id, expire_time, cur_tstamp, wash_succeed, block_washed_line_count, block_washed_line_read_size))) {
        LOG_WARN("do_wash_block_ fail", K(ret), K(block_id), K(expire_time), K(cur_tstamp));
      } else {
        if (wash_succeed) {
          // eliminated uccessfully
          done = true;
        } else {
          // eliminated failed, wait
          int64_t left_time = end_tstamp - cur_tstamp;
          if (left_time <= 0) {
            ret = OB_TIMEOUT;
          } else {
            // wait for the next block to be eliminated
            // FIXME:
            // 1. Essentially, should wait for the current block to be eliminated or free,
            //    here waiting for any block to be eliminated is a simplified approach.
            // 2. In current cond implementation, ObCond can only wake up one waiting in each signal,
            //    can not broadcast, most threads will wait for timeout mostly.
            //
            // Considering that conflict case are relatively rare, and once a conflict is encountered,
            // it indicates that the concurrency is relatively large, system availability should be prioritized
            // and concurrency should be reduced at this time.Therefore, the above implementation defects are
            // acceptable.
            int64_t wait_time_on_block_conflict = WAIT_TIME_ON_BLOCK_CONFLICT;
            int64_t wait_time = std::min(left_time, wait_time_on_block_conflict);
            block_wash_cond_.timedwait(wait_time);
          }
        }
      }
    }
  }

  return ret;
}

int ObLogLineCache::handle_when_block_not_hit_(
    const int64_t block_index, const file_id_t file_id, const int64_t end_tstamp, bool& exceed_mem_limit_occured)
{
  int ret = OB_SUCCESS;
  bool done = false;

  // have exceeds the memory limit case encountered or not
  exceed_mem_limit_occured = false;

  while (OB_SUCCESS == ret && !done) {
    int64_t wait_time = 0;

    if (OB_SUCC(prepare_block_(block_index, file_id))) {
      // success
      done = true;
    } else if (OB_EXCEED_MEM_LIMIT == ret) {
      // Exceed total memory limit, eliminate the oldest block
      ret = OB_SUCCESS;
      exceed_mem_limit_occured = true;
      if (OB_FAIL(do_lru_wash_())) {
        LOG_WARN("do_lru_wash_ fail", K(ret));
      } else {
        if (block_token_.get_free_count() > 0) {
          // check if there is a block token available currently
          // if yes, continue to prepare
        } else {
          wait_time = WAIT_TIME_ON_EXCEED_MEM_LIMIT;
        }
      }
    } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      // failed to allocate memory, just wait for a while
      ret = OB_SUCCESS;
      wait_time = WAIT_TIME_ON_ALLOCATE_MEM_FAIL;
      // failure to allocate memory is considered as exceeding the memory limit
      exceed_mem_limit_occured = true;
    } else if (OB_ITEM_NOT_MATCH == ret) {
      // does not match, content conflicts, return directly, let the caller function handle
    } else {
      // failed
      LOG_WARN("prepare_block_ fail", K(ret), K(block_index), K(file_id));
    }

    // perform wait operation
    if (OB_SUCCESS == ret && wait_time > 0) {
      int64_t left_time = end_tstamp - ObTimeUtility::current_time();
      wait_time = std::min(left_time, wait_time);

      if (wait_time <= 0) {
        ret = OB_TIMEOUT;
      } else {
        block_wash_cond_.timedwait(wait_time);
      }
    }
  }

  return ret;
}

int ObLogLineCache::get_line(
    const file_id_t file_id, const offset_t offset, const int64_t timeout_us, char*& line, bool& need_load_data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    LOG_WARN("not inited", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_entry_invalid(file_id, offset))) {
    LOG_WARN("invalid argument", K(file_id), K(offset), K(timeout_us));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t block_index = calc_block_index(file_id, offset);
    int64_t retry_times = 0;
    int64_t end_tstamp = ObTimeUtility::current_time() + timeout_us;
    Block& block = blocks_[block_index];
    bool exceed_mem_limit_occured = false;
    bool block_conflicted = false;

    ATOMIC_INC(&get_line_count_);
    GetLineTimeoutReason::REASON reason = GetLineTimeoutReason::GET_LINE_INIT;
    while (OB_SUCCESS == ret && NULL == line) {
      if (OB_SUCC(block.get_line(file_id, offset, line_futex_, timeout_us, line, need_load_data))) {
        // success
        if (0 == retry_times) {
          // only the first successful hit is fortuned
          ATOMIC_INC(&block_hit_count_);
        }

        if (!need_load_data) {
          ATOMIC_INC(&line_hit_count_);
        }
      } else if (OB_ITEM_NOT_MATCH == ret) {
        // block conflict
        block_conflicted = true;
        ret = OB_SUCCESS;
        if (OB_FAIL(handle_when_block_conflict_(block, block_index, file_id, end_tstamp))) {
          if (OB_TIMEOUT == ret) {
            // timeout
            reason = GetLineTimeoutReason::BLOCK_CONFLICT_TIMEOUT;
          } else {
            LOG_WARN("handle_when_block_conflict_ fail", K(ret), K(block_index), K(file_id), K(block), K(end_tstamp));
          }
        }
      } else if (OB_ITEM_NOT_SETTED == ret) {
        bool local_exceed_mem_limit_occured = false;
        // block missed
        if (OB_FAIL(handle_when_block_not_hit_(block_index, file_id, end_tstamp, local_exceed_mem_limit_occured))) {
          if (OB_TIMEOUT == ret) {
            // timeout
            reason = GetLineTimeoutReason::BLOCK_NOT_HIT_TIMEOUT;
          } else if (OB_ITEM_NOT_MATCH == ret) {
            // block conflct, retry directly
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("handle_when_block_not_hit_ fail", K(ret), K(block_index), K(file_id), K(end_tstamp), K(block));
          }
        } else {
          if (local_exceed_mem_limit_occured) {
            exceed_mem_limit_occured = true;
          }
        }
      } else if (OB_TIMEOUT == ret) {
        // timeout
        reason = GetLineTimeoutReason::WAIT_LOAD_LINE_TIMEOUT;
      } else {
        LOG_WARN("block get_line fail", K(ret), K(file_id), K(offset), K(timeout_us));
      }

      // finally increase the number of retries
      retry_times++;
    }

    if (OB_TIMEOUT == ret) {
      // timeout waiting for file loading
      LOG_WARN("[LINE_CACHE] get_line timeout",
          K(ret),
          K(file_id),
          K(offset),
          K(block),
          "timeout_reson",
          GetLineTimeoutReason::get_str(reason));
    }

    // modify statistics below
    if (block_conflicted) {
      ATOMIC_INC(&block_conflict_count_);
    }
    if (exceed_mem_limit_occured) {
      ATOMIC_INC(&exceed_mem_limit_count_);
    }
  }
  return ret;
}

int ObLogLineCache::do_wash_block_(const TokenValueType block_index, const int64_t expire_time,
    const int64_t cur_tstamp, bool& wash_succeed, int64_t& block_washed_line_count,
    int64_t& block_washed_line_read_size)
{
  int ret = OB_SUCCESS;
  char* washed_lines = NULL;
  TokenIDType token_id = INVALID_TOKEN_ID;
  wash_succeed = false;
  block_washed_line_count = 0;

  if (OB_UNLIKELY(block_index < 0 || nullptr == blocks_ || block_index >= block_array_size_)) {
    LOG_WARN("invalid block index", K(ret), K(block_index));
    ret = OB_INVALID_ARGUMENT;
  } else {
    Block& block = blocks_[block_index];

    // perform a wash operation
    block.wash(expire_time,
        cur_tstamp,
        wash_succeed,
        washed_lines,
        block_washed_line_count,
        block_washed_line_read_size,
        token_id);

    if (wash_succeed) {
      // If the elimination is successful, the corresponding resources will be recycled
      if (OB_ISNULL(washed_lines)) {
        LOG_WARN("washed_lines is NULL, unexpected error", KP(washed_lines), K(block));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(block_token_.free(token_id, block_index))) {
        LOG_WARN("free token value fail", K(ret), K(token_id), K(block_index));
      } else {
        block_alloc_.free(washed_lines);
        washed_lines = NULL;
        token_id = INVALID_TOKEN_ID;

        (void)ATOMIC_FAA(&cached_line_count_, -block_washed_line_count);

        (void)ATOMIC_FAA(&washed_line_count_, block_washed_line_count);
        (void)ATOMIC_FAA(&washed_line_read_size_, block_washed_line_read_size);
      }

      // eventually notify others
      block_wash_cond_.signal();
    }
  }
  return ret;
}

struct LRUBlock {
  typedef ObLogLineCache::TokenValueType BlockIDType;

  BlockIDType block_id_;
  int64_t last_access_timestamp_;

  LRUBlock() : block_id_(ObLogLineCache::INVALID_TOKEN_VALUE), last_access_timestamp_(0)
  {}

  LRUBlock(const BlockIDType block_id, const int64_t last_access_tstamp)
      : block_id_(block_id), last_access_timestamp_(last_access_tstamp)
  {}

  TO_STRING_KV(K_(block_id), K_(last_access_timestamp));
};

struct LRUBlockComp {
  bool operator()(const LRUBlock& a, const LRUBlock& b)
  {
    return a.last_access_timestamp_ < b.last_access_timestamp_;
  }
};

typedef ObArray<LRUBlock> LRUBlockArray;

// eliminate the oldest visited block
// FIXME: This algorithm takes the slowest K blocks for elimination every time.
//        If the slowest K blocks have a reference count, the elimination will fail.
int ObLogLineCache::do_lru_wash_()
{
  int ret = OB_SUCCESS;
  LRUBlockArray total_array;
  LRUBlockArray top_k_array;
  int64_t total_read_size = 0;
  int64_t total_washed_block_count = 0;
  int64_t total_washed_line_count = 0;
  int64_t valid_block_count = block_token_.get_used_count();
  int64_t lru_wash_count = valid_block_count * LRU_WASH_PERCENT / 100;
  int64_t cur_tstamp = ObTimeUtility::current_time();
  TokenValueType* tokens = block_token_.tokens_;
  int64_t total_token_count = block_token_.get_total_count();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (lru_wash_count <= 0) {
    // no need to eliminate
  } else if (NULL != tokens) {
    // first traverse all tokens and record all the block information being accessed
    for (int64_t index = 0; OB_SUCCESS == ret && index < total_token_count; index++) {
      TokenValueType block_id = ATOMIC_LOAD(tokens + index);
      if (INVALID_TOKEN_VALUE != block_id) {
        Block& block = blocks_[block_id];
        LRUBlock lru_block(block_id, block.get_last_access_timestamp());
        if (OB_FAIL(total_array.push_back(lru_block))) {
          LOG_WARN("push_back lru_block fail", K(ret), K(total_array), K(lru_block));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      LRUBlockComp cmp;
      // calculate the oldest K blocks
      if (OB_FAIL(top_k(total_array, lru_wash_count, top_k_array, cmp))) {
        LOG_WARN("compute top_k fail", K(ret), K(total_array), K(lru_wash_count), K(top_k_array));
      } else {
        // traverse all the blocks to be eliminated and perform the elimination operation
        for (int64_t index = 0; OB_SUCCESS == ret && index < top_k_array.count(); index++) {
          const LRUBlock& lru_block = top_k_array.at(index);
          TokenValueType block_id = lru_block.block_id_;

          if (INVALID_TOKEN_VALUE != block_id) {
            bool wash_succeed = false;
            int64_t block_washed_line_count = 0;
            int64_t block_washed_line_read_size = 0;
            int64_t expire_time = 0;
            Block& block = blocks_[block_id];

            // first check whether the original access timestamp is still maintained,
            // if not, it will not be eliminated to avoid accidental injury
            if (lru_block.last_access_timestamp_ != block.get_last_access_timestamp()) {
              // not eliminated
            }
            // forced out
            else if (OB_FAIL(do_wash_block_(block_id,
                         expire_time,
                         cur_tstamp,
                         wash_succeed,
                         block_washed_line_count,
                         block_washed_line_read_size))) {
              LOG_WARN("wash block fail", K(ret), K(block_id), K(expire_time), K(cur_tstamp), K(wash_succeed));
            } else if (wash_succeed) {
              total_washed_block_count++;
              total_washed_line_count += block_washed_line_count;
              total_read_size += block_washed_line_read_size;
            }
          }
        }

        if (top_k_array.count() > 0) {
          if (REACH_TIME_INTERVAL(PRINT_WASH_STAT_INFO_INTERVAL)) {
            int64_t read_size_per_line = (0 == total_washed_line_count ? 0 : total_read_size / total_washed_line_count);

            LOG_INFO("[LINE_CACHE] lru_wash",
                K(valid_block_count),
                K(lru_wash_count),
                "top_k_count",
                top_k_array.count(),
                K(total_washed_block_count),
                K(total_washed_line_count),
                K(read_size_per_line),
                "cached_block_count",
                block_token_.get_used_count(),
                K_(cached_line_count));
          }

          (void)ATOMIC_FAA(&lru_washed_line_count_, total_washed_line_count);
        }
      }
    }
  }
  return ret;
}

int ObLogLineCache::do_wash_(
    const int64_t expire_time, int64_t& total_washed_block_count, int64_t& total_washed_line_count)
{
  int ret = OB_SUCCESS;
  int64_t total_read_size = 0;
  total_washed_block_count = 0;
  total_washed_line_count = 0;

  if (block_token_.get_used_count() > 0) {
    TokenValueType* tokens = block_token_.tokens_;
    int64_t total_token_count = block_token_.get_total_count();
    int64_t cur_tstamp = ObTimeUtility::current_time();

    // Traverse all tokens, find all the blocks that use tokens, and judge whether they can be recycled
    if (NULL != tokens) {
      for (int64_t index = 0; OB_SUCCESS == ret && index < total_token_count; index++) {
        TokenValueType block_id = ATOMIC_LOAD(tokens + index);

        if (INVALID_TOKEN_VALUE != block_id) {
          bool wash_succeed = false;
          int64_t block_washed_line_count = 0;
          int64_t block_washed_line_read_size = 0;

          // perform the actual block elimination operation
          if (OB_FAIL(do_wash_block_(block_id,
                  expire_time,
                  cur_tstamp,
                  wash_succeed,
                  block_washed_line_count,
                  block_washed_line_read_size))) {
            LOG_WARN("wash block fail", K(ret), K(block_id), K(expire_time), K(index));
          } else if (wash_succeed) {
            total_washed_block_count++;
            total_washed_line_count += block_washed_line_count;
            total_read_size += block_washed_line_read_size;
          }
        }
      }

      if (total_washed_block_count > 0) {
        if (REACH_TIME_INTERVAL(PRINT_WASH_STAT_INFO_INTERVAL)) {
          int64_t read_size_per_line = (0 == total_washed_line_count ? 0 : total_read_size / total_washed_line_count);

          LOG_INFO("[LINE_CACHE] wash",
              K(expire_time),
              K(total_washed_block_count),
              K(total_washed_line_count),
              K(read_size_per_line),
              "cached_block_count",
              block_token_.get_used_count(),
              K_(cached_line_count));
        }
      }
    }
  }

  return ret;
}

void ObLogLineCache::wash()
{
  int ret = OB_SUCCESS;
  int64_t expire_time = DATA_EXPIRE_TIME_US;  // use normal expiration time
  int64_t total_washed_block_count = 0;
  int64_t total_washed_line_count = 0;

  if (OB_UNLIKELY(!inited_)) {
    LOG_WARN("not inited", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(do_wash_(expire_time, total_washed_block_count, total_washed_line_count))) {
    LOG_WARN("do_wash_ fail", K(ret), K(expire_time));
  } else {
    // success
  }
}

void ObLogLineCache::stat()
{
  static int64_t last_get_line_count = 0;
  static int64_t last_load_line_count = 0;
  static int64_t last_block_hit_count = 0;
  static int64_t last_line_hit_count = 0;
  static int64_t last_block_conflict_count = 0;
  static int64_t last_exceed_mem_limit_count = 0;
  static int64_t last_washed_line_count = 0;
  static int64_t last_lru_washed_line_count = 0;
  static int64_t last_washed_line_read_size = 0;
  static int64_t last_repeated_block_load_count = 0;
  static int64_t last_repeated_block_load_interval = 0;
  static int64_t last_stat_tstamp = ObTimeUtility::current_time();

  int64_t cur_tstamp = ObTimeUtility::current_time();

  int64_t cur_get_line_count = ATOMIC_LOAD(&get_line_count_);
  int64_t cur_load_line_count = ATOMIC_LOAD(&load_line_count_);
  int64_t cur_block_hit_count = ATOMIC_LOAD(&block_hit_count_);
  int64_t cur_line_hit_count = ATOMIC_LOAD(&line_hit_count_);
  int64_t cur_block_conflict_count = ATOMIC_LOAD(&block_conflict_count_);
  int64_t cur_exceed_mem_limit_count = ATOMIC_LOAD(&exceed_mem_limit_count_);
  int64_t cur_washed_line_count = ATOMIC_LOAD(&washed_line_count_);
  int64_t cur_lru_washed_line_count = ATOMIC_LOAD(&lru_washed_line_count_);
  int64_t cur_washed_line_read_size = ATOMIC_LOAD(&washed_line_read_size_);
  int64_t cur_repeated_block_load_count = ATOMIC_LOAD(&repeated_block_load_count_);
  int64_t cur_repeated_block_load_interval = ATOMIC_LOAD(&repeated_block_load_interval_);

  double delta_time_sec = static_cast<double>((cur_tstamp - last_stat_tstamp) / 1000000);
  double delta_get_line_count = static_cast<double>(cur_get_line_count - last_get_line_count);
  double delta_load_line_count = static_cast<double>(cur_load_line_count - last_load_line_count);
  double delta_block_hit_count = static_cast<double>(cur_block_hit_count - last_block_hit_count);
  double delta_line_hit_count = static_cast<double>(cur_line_hit_count - last_line_hit_count);
  double delta_block_conflict_count = static_cast<double>(cur_block_conflict_count - last_block_conflict_count);
  double delta_exceed_mem_limit_count = static_cast<double>(cur_exceed_mem_limit_count - last_exceed_mem_limit_count);
  double delta_washed_line_count = static_cast<double>(cur_washed_line_count - last_washed_line_count);
  double delta_lru_washed_line_count = static_cast<double>(cur_lru_washed_line_count - last_lru_washed_line_count);
  double delta_washed_line_read_size = static_cast<double>(cur_washed_line_read_size - last_washed_line_read_size);
  double delta_repeated_block_load_count =
      static_cast<double>(cur_repeated_block_load_count - last_repeated_block_load_count);
  double delta_repeated_block_load_interval_ms =
      static_cast<double>((cur_repeated_block_load_interval - last_repeated_block_load_interval) / 1000);

  // read the disk ratio
  double M_size = static_cast<double>(1024 * 1024);
  double read_disk_ratio = (0 == delta_get_line_count ? 0 : delta_load_line_count / delta_get_line_count);
  double read_disk_size_M = delta_load_line_count * static_cast<double>(LINE_SIZE) / M_size;
  double read_disk_size_per_sec_M = (0 == delta_time_sec ? 0 : read_disk_size_M / delta_time_sec);

  // block hit rate
  double block_hit_ratio = (0 == delta_get_line_count ? 0 : delta_block_hit_count / delta_get_line_count);
  // line hit rate
  double line_hit_ratio = (0 == delta_get_line_count ? 0 : delta_line_hit_count / delta_get_line_count);
  // block conflict rate
  double block_conflict_ratio = (0 == delta_get_line_count ? 0 : delta_block_conflict_count / delta_get_line_count);
  // memory rate exceeded
  double exceed_mem_limit_ratio = (0 == delta_get_line_count ? 0 : delta_exceed_mem_limit_count / delta_get_line_count);

  // calculate data utilization based on eliminated lines
  double total_washed_size = delta_washed_line_count * LINE_SIZE;
  double data_use_ratio = (0 == delta_washed_line_count ? 0 : delta_washed_line_read_size / total_washed_size);

  // count of wash lines per second
  double wash_line_count_per_sec = (0 == delta_time_sec ? 0 : delta_washed_line_count / delta_time_sec);
  double lru_wash_line_count_per_sec = (0 == delta_time_sec ? 0 : delta_lru_washed_line_count / delta_time_sec);

  // count of repeated loading blocks per second
  double repeated_block_load_count_per_sec =
      (0 == delta_time_sec ? 0 : delta_repeated_block_load_count / delta_time_sec);

  // calculate the average time interval between repeated loading of blocks
  double repeated_block_load_interval_ms =
      (0 == delta_repeated_block_load_count ? 0
                                            : delta_repeated_block_load_interval_ms / delta_repeated_block_load_count);

  int64_t cached_block_count = get_cached_block_count();
  int64_t cached_line_count = get_cached_line_count();
  int64_t valid_line_per_block = ((0 == cached_block_count) ? 0 : cached_line_count / cached_block_count);

  LOG_INFO("[LINE_CACHE] STAT",
      K(cached_block_count),
      K(cached_line_count),
      K(valid_line_per_block),
      "cached_mem_size(M)",
      get_cached_mem_size() / (1024 * 1024),
      "get_line_count",
      static_cast<int64_t>(delta_get_line_count),
      "read_disk_ratio(%)",
      read_disk_ratio * 100,
      K(read_disk_size_per_sec_M),
      "block_hit_ratio(%)",
      block_hit_ratio * 100,
      "line_hit_ratio(%)",
      line_hit_ratio * 100,
      "data_use_ratio(%)",
      data_use_ratio * 100,
      K(repeated_block_load_count_per_sec),
      K(repeated_block_load_interval_ms),
      "block_conflict_ratio(%)",
      block_conflict_ratio * 100,
      "exceed_mem_limit_ratio(%)",
      exceed_mem_limit_ratio * 100,
      K(wash_line_count_per_sec),
      K(lru_wash_line_count_per_sec));

  last_get_line_count = cur_get_line_count;
  last_load_line_count = cur_load_line_count;
  last_block_hit_count = cur_block_hit_count;
  last_line_hit_count = cur_line_hit_count;
  last_block_conflict_count = cur_block_conflict_count;
  last_exceed_mem_limit_count = cur_exceed_mem_limit_count;
  last_washed_line_count = cur_washed_line_count;
  last_lru_washed_line_count = cur_lru_washed_line_count;
  last_washed_line_read_size = cur_washed_line_read_size;
  last_repeated_block_load_count = cur_repeated_block_load_count;
  last_repeated_block_load_interval = cur_repeated_block_load_interval;
  last_stat_tstamp = cur_tstamp;
}

int ObLogLineCache::prepare_block_(const int64_t block_index, const file_id_t file_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    TokenIDType token_id = INVALID_TOKEN_ID;
    char* block_ptr = NULL;
    int64_t last_access_tstamp = 0;
    // convert block id to token value
    TokenValueType token_value = static_cast<TokenValueType>(block_index);
    Block& block = blocks_[block_index];

    // assign token id first
    if (OB_FAIL(block_token_.alloc(token_id, token_value))) {
      if (OB_EXCEED_MEM_LIMIT == ret) {
        // memory limit exceeded
      } else {
        LOG_WARN("alloc block token fail", K(ret), K(token_id), K(token_value));
      }
    }
    // assign block
    else if (OB_ISNULL(block_ptr = block_alloc_.alloc())) {
      LOG_WARN("line cache allocate memory for block fail",
          "cached_block_count",
          block_token_.get_used_count(),
          K(cached_line_count_));
      // the actual physical memory is not enough
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    // prepare block
    else if (OB_FAIL(block.prepare(file_id, token_id, block_ptr, last_access_tstamp))) {
      if (OB_ENTRY_EXIST == ret || OB_ITEM_NOT_MATCH == ret) {
        // exist or not match
        LOG_WARN("[LINE_CACHE] prepare block conflict with others",
            K(ret),
            K(file_id),
            K(token_id),
            KP(block_ptr),
            K(block));
      } else {
        LOG_WARN("line cache prepare block fail", K(ret), K(file_id), K(token_id), KP(block_ptr), K(block));
      }
    } else {
      // prepare success
      // Check whether the load has been repeated recently
      if (last_access_tstamp > 0) {
        int64_t cur_ts = ObTimeUtility::current_time();
        int64_t delta_time = cur_ts - last_access_tstamp;

        // only count the repeated loading in a short period of time
        if (delta_time <= MIN_INTERVAL_CONSIDER_AS_REPEATED_LAOADING) {
          ATOMIC_INC(&repeated_block_load_count_);
          (void)ATOMIC_FAA(&repeated_block_load_interval_, delta_time);
        }
      }
    }

    // In all cases where the prepare is not successful, the corresponding
    // resources are recovered, including OB_ENTRY_EXIST
    if (OB_SUCCESS != ret) {
      if (INVALID_TOKEN_ID != token_id) {
        int tmp_ret = block_token_.free(token_id, token_value);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("free block token fail", K(tmp_ret), K(ret), K(token_id), K(token_value));
        } else {
          token_id = INVALID_TOKEN_ID;
        }
      }

      if (NULL != block_ptr) {
        block_alloc_.free(block_ptr);
        block_ptr = NULL;
      }
    }

    // If the prepare block returns entry exists, reset ret
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogLineCache::revert_line(const file_id_t file_id, const offset_t offset, const int64_t read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_entry_invalid(file_id, offset))) {
    LOG_WARN("invalid argument", K(file_id), K(offset));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t block_index = calc_block_index(file_id, offset);
    Block& block = blocks_[block_index];

    if (OB_FAIL(block.revert_line(file_id, offset, read_size))) {
      LOG_WARN("block revert_line fail", K(ret), K(file_id), K(offset), K(read_size), K(block), K(block_index));
    }
  }
  return ret;
}

int ObLogLineCache::mark_line_status(const file_id_t file_id, const offset_t offset, const bool is_load_ready)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_entry_invalid(file_id, offset))) {
    LOG_WARN("invalid argument", K(file_id), K(offset));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t block_index = calc_block_index(file_id, offset);
    Block& block = blocks_[block_index];

    if (OB_FAIL(block.mark_line_status(file_id, offset, is_load_ready, line_futex_))) {
      LOG_WARN("block mark_line_status fail", K(ret), K(file_id), K(offset), K(is_load_ready), K(block));
    } else {
      if (is_load_ready) {
        ATOMIC_INC(&cached_line_count_);
        ATOMIC_INC(&load_line_count_);
      }
    }
  }
  return ret;
}

/////////////////////////////////// BlockAlloc //////////////////////////////////

ObLogLineCache::BlockAlloc::BlockAlloc() : buf_(NULL), label_(nullptr), free_list_()
{}

int ObLogLineCache::BlockAlloc::init(const int64_t prealloc_block_cnt, const char* label)
{
  int ret = OB_SUCCESS;
  int64_t prealloc_size = prealloc_block_cnt * BLOCK_SIZE;
  int64_t align_size = CLOG_DIO_ALIGN_SIZE;

  if (OB_UNLIKELY(NULL != buf_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(prealloc_block_cnt <= 0)) {
    LOG_WARN("invalid argument", K(prealloc_block_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(free_list_.init(prealloc_block_cnt, global_default_allocator, label))) {
    LOG_WARN("init free list fail", K(ret), K(prealloc_block_cnt));
  } else if (OB_ISNULL(buf_ = (char*)ob_malloc_align(align_size, prealloc_size, label))) {
    LOG_WARN("allocate memory fail", K(buf_), K(prealloc_size), K(prealloc_block_cnt));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t index = 0; OB_SUCCESS == ret && index < prealloc_block_cnt; index++) {
      char* block = buf_ + index * BLOCK_SIZE;
      if (OB_FAIL(free_list_.push(block))) {
        LOG_WARN("push block into free list fail", K(ret), KP(block), K(index), K(prealloc_block_cnt));
      }
    }

    if (OB_SUCCESS == ret) {
      label_ = label;
    }
  }

  return ret;
}

void ObLogLineCache::BlockAlloc::destroy()
{
  if (NULL != buf_) {
    ob_free_align(buf_);
    buf_ = NULL;
  }

  free_list_.destroy();
  label_ = nullptr;
}

char* ObLogLineCache::BlockAlloc::alloc()
{
  int ret = OB_SUCCESS;
  char* ptr = NULL;
  if (OB_SUCC(free_list_.pop(ptr))) {
    if (OB_ISNULL(ptr)) {
      LOG_WARN("free list pop NULL ptr",
          KP(ptr),
          "free_list_total",
          free_list_.get_total(),
          "free_list_capacity",
          free_list_.capacity());
    }
  } else {
    ptr = static_cast<char*>(ob_malloc_align(CLOG_DIO_ALIGN_SIZE, BLOCK_SIZE, label_));
  }
  return ptr;
}

void ObLogLineCache::BlockAlloc::free(char* ptr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ptr)) {
    char* end_of_buf = buf_ + free_list_.capacity() * BLOCK_SIZE;

    // determine whether is prealloc out
    if (ptr >= buf_ && ptr < end_of_buf) {
      if (OB_FAIL(free_list_.push(ptr))) {
        LOG_WARN("Line Cache free block fail, push into free list error",
            K(ret),
            KP(ptr),
            KP(buf_),
            K(free_list_.capacity()),
            K(free_list_.get_total()),
            K(free_list_.get_free()));
      } else {
        ptr = NULL;
      }
    } else {
      ob_free_align(ptr);
      ptr = NULL;
    }
  }
}

////////////////////////// BlockToken ///////////////////////////

ObLogLineCache::BlockToken::BlockToken() : tokens_(NULL), free_list_()
{}

ObLogLineCache::BlockToken::~BlockToken()
{
  destroy();
}

int ObLogLineCache::BlockToken::push_free_list_(const TokenIDType token_id)
{
  // The free list does not support scenarios with a value of 0,
  // therefore, 1 is manually added here.
  void* value = reinterpret_cast<void*>(token_id + 1);
  return free_list_.push(value);
}

int ObLogLineCache::BlockToken::pop_free_list_(TokenIDType& token_id)
{
  int ret = OB_SUCCESS;
  void* pop_val = NULL;
  if (OB_SUCC(free_list_.pop(pop_val))) {
    token_id = static_cast<TokenIDType>(reinterpret_cast<int64_t>(pop_val) - 1);
  }
  return ret;
}

int ObLogLineCache::BlockToken::init(const int64_t total_count, const char* label)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != tokens_)) {
    LOG_WARN("init twice", KP(tokens_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(total_count <= 0) || OB_UNLIKELY(total_count > MAX_TOKEN_ID)) {
    LOG_WARN("invalid argument", K(total_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(free_list_.init(total_count, global_default_allocator, label))) {
    LOG_WARN("init free list fail", K(ret), K(total_count));
  } else {
    int64_t alloc_size = total_count * sizeof(TokenValueType);
    tokens_ = static_cast<TokenValueType*>(ob_malloc(alloc_size, label));

    if (OB_ISNULL(tokens_)) {
      LOG_WARN("allocate memory for token array fail", KP(tokens_));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (TokenIDType index = 0; OB_SUCCESS == ret && index < total_count; index++) {
        if (OB_FAIL(push_free_list_(index))) {
          LOG_WARN("push free list fail", K(ret), K(index));
        } else {
          tokens_[index] = INVALID_TOKEN_VALUE;
        }
      }
    }
  }
  return ret;
}

void ObLogLineCache::BlockToken::destroy()
{
  if (NULL != tokens_) {
    ob_free(tokens_);
    tokens_ = NULL;
  }

  free_list_.destroy();
}

int ObLogLineCache::BlockToken::alloc(TokenIDType& token_id, const TokenValueType token_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tokens_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pop_free_list_(token_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // free list not exists, return exceed the memory limit directly.
      ret = OB_EXCEED_MEM_LIMIT;
    } else {
      LOG_WARN("pop item from free list fail", K(ret));
    }
  } else {
    // check the validity of the token
    if (OB_UNLIKELY(token_id < 0 || token_id >= free_list_.capacity())) {
      LOG_WARN("token_id from free_list is invalid", K(token_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // set the corresponding token value
      ATOMIC_STORE(&tokens_[token_id], token_value);
    }
  }
  return ret;
}

int ObLogLineCache::BlockToken::free(const TokenIDType token_id, const TokenValueType token_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tokens_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(token_id < 0 || token_id >= free_list_.capacity())) {
    LOG_WARN("invalid argument", K(token_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // mark token value as invalid
    TokenValueType old_value = ATOMIC_CAS(tokens_ + token_id, token_value, INVALID_TOKEN_VALUE);

    if (OB_UNLIKELY(old_value != token_value)) {
      LOG_WARN("token_value does not match target value", K(old_value), K(token_value), K(token_id));
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(push_free_list_(token_id))) {
      LOG_WARN("push into free list fail", K(ret), K(token_id));
    }
  }
  return ret;
}

////////////////////////// BlockKey ///////////////////////////

int ObLogLineCache::BlockKey::prepare_begin(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  bool done = false;
  Value cur_v;

  if (OB_UNLIKELY(0 == file_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    cur_v.key_ = ATOMIC_LOAD(&v_.key_);

    while (!done && OB_SUCCESS == ret) {
      if (file_id == cur_v.file_id_) {
        ret = OB_ENTRY_EXIST;
      } else if (0 != cur_v.file_id_) {
        ret = OB_ITEM_NOT_MATCH;
      } else { /* 0 == cur_v.file_id_ */
        // assume ref_cnt must be 0
        if (OB_UNLIKELY(0 != cur_v.ref_cnt_)) {
          LOG_WARN("ref_cnt is not 0, unexpected error", K(cur_v.key_), K(cur_v.file_id_), K(cur_v.ref_cnt_));
          ret = OB_ERR_UNEXPECTED;
        } else {
          Value new_v, old_v;

          old_v.key_ = cur_v.key_;
          new_v.file_id_ = file_id;
          new_v.ref_cnt_ = -1;  // lock on

          cur_v.key_ = ATOMIC_CAS(&v_.key_, cur_v.key_, new_v.key_);

          // If CAS is successful, the conversion is successful
          if (cur_v.key_ == old_v.key_) {
            done = true;
          }
        }
      }
    }
  }

  return ret;
}

void ObLogLineCache::BlockKey::prepare_end(const file_id_t file_id)
{
  // for performance considerations, no verification here
  Value cur_v;
  cur_v.file_id_ = file_id;
  cur_v.ref_cnt_ = 0;  // unlock
  ATOMIC_SET(&v_.key_, cur_v.key_);
}

int ObLogLineCache::BlockKey::get(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  bool done = false;
  Value cur_v;

  if (OB_UNLIKELY(0 == file_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    cur_v.key_ = ATOMIC_LOAD(&v_.key_);

    while (!done && OB_SUCCESS == ret) {
      if (0 == cur_v.file_id_) {
        ret = OB_ITEM_NOT_SETTED;
      } else if (file_id != cur_v.file_id_) {
        ret = OB_ITEM_NOT_MATCH;
      } else {  // file_id == cur_v.file_id_
        // If ref_cnt is less than 0, it means locked, busy waiting at this time
        if (cur_v.ref_cnt_ < 0) {
          PAUSE();

          // get the current value
          cur_v.key_ = ATOMIC_LOAD(&v_.key_);
        } else {
          Value new_v, old_v;

          old_v.key_ = cur_v.key_;
          new_v.key_ = cur_v.key_;
          new_v.ref_cnt_++;

          cur_v.key_ = ATOMIC_CAS(&v_.key_, cur_v.key_, new_v.key_);

          // if CAS succeeds, it succeeds
          if (cur_v.key_ == old_v.key_) {
            done = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObLogLineCache::BlockKey::revert(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  bool done = false;
  Value cur_v;
  cur_v.key_ = ATOMIC_LOAD(&v_.key_);

  if (OB_UNLIKELY(file_id != cur_v.file_id_)) {
    LOG_WARN("file_id does not match", K(cur_v.file_id_), K(file_id), K(cur_v.ref_cnt_));
    ret = OB_ITEM_NOT_MATCH;
  } else {
    while (!done && OB_SUCCESS == ret) {
      if (cur_v.ref_cnt_ <= 0) {
        LOG_WARN("reference count is not positive", K(cur_v.file_id_), K(cur_v.ref_cnt_), K(file_id));
        ret = OB_ERR_UNEXPECTED;
      } else {
        Value new_v, old_v;

        old_v.key_ = cur_v.key_;
        new_v.key_ = cur_v.key_;
        new_v.ref_cnt_--;  // decrease the reference count by 1

        cur_v.key_ = ATOMIC_CAS(&v_.key_, cur_v.key_, new_v.key_);

        if (cur_v.key_ == old_v.key_) {
          done = true;
        }
      }
    }
  }
  return ret;
}

bool ObLogLineCache::BlockKey::wash_begin()
{
  bool done = false;
  bool can_wash = false;
  Value cur_v;
  cur_v.key_ = ATOMIC_LOAD(&v_.key_);

  while (!done) {
    // If the file_id is invalid or the reference count is not 0, it cannot be eliminated,
    // indicating that either the Block is invalid or someone is quoting
    if (0 == cur_v.file_id_ || 0 != cur_v.ref_cnt_) {
      can_wash = false;
      done = true;
    } else {  // 0 != cur_v.file_id_ && 0 == cur_v.ref_cnt_
      Value new_v, old_v;
      old_v.key_ = cur_v.key_;
      new_v.key_ = cur_v.key_;
      new_v.ref_cnt_ = -1;  // lock

      cur_v.key_ = ATOMIC_CAS(&v_.key_, cur_v.key_, new_v.key_);

      if (cur_v.key_ == old_v.key_) {
        can_wash = true;
        done = true;
      }
    }
  }

  return can_wash;
}

void ObLogLineCache::BlockKey::wash_end(const bool wash_succeed)
{
  // for performance considerations, there is no longer a status check
  Value cur_v;
  cur_v.key_ = ATOMIC_LOAD(&v_.key_);

  // if the elimination is successful, set to the initial state
  if (wash_succeed) {
    cur_v.file_id_ = 0;
    cur_v.ref_cnt_ = 0;
  } else {
    // elimination failed, file_id remains as it is, and then unlock
    cur_v.ref_cnt_ = 0;
  }

  ATOMIC_SET(&v_.key_, cur_v.key_);
}

////////////////////////// Block ///////////////////////////

ObLogLineCache::Block::Block()
    : key_(), lines_(NULL), token_id_(INVALID_TOKEN_ID), read_size_(0), last_access_timestamp_(0)
{
  for (int64_t index = 0; index < LINE_COUNT_PER_BLOCK; index++) {
    line_status_[index] = EMPTY;
  }
}

void ObLogLineCache::Block::reset()
{
  // key_ not joinreset

  lines_ = NULL;
  token_id_ = INVALID_TOKEN_ID;
  for (int64_t index = 0; index < LINE_COUNT_PER_BLOCK; index++) {
    line_status_[index] = EMPTY;
  }
  read_size_ = 0;

  //  last_access_timestamp_ do not participate in reset
}

void ObLogLineCache::Block::update_access_timestamp()
{
  ATOMIC_STORE(&last_access_timestamp_, ObTimeUtility::current_time());
}

int ObLogLineCache::Block::prepare(
    const file_id_t file_id, const TokenIDType token_id, char* lines, int64_t& last_access_tstamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_id <= 0) || OB_UNLIKELY(token_id < 0) || OB_ISNULL(lines) ||
      OB_UNLIKELY(!ObLogLineCache::is_aligned(lines))) {
    LOG_WARN("invalid argument", K(file_id), K(token_id), KP(lines));
    ret = OB_INVALID_ARGUMENT;
  }
  // seize Block and get the sole prepare right
  else if (OB_FAIL(key_.prepare_begin(file_id))) {
    if (OB_ENTRY_EXIST == ret) {
      // someone prepared
    } else if (OB_ITEM_NOT_MATCH == ret) {
      // check whether conflicts
      // this kind of case is rare, print WARN
      LOG_WARN("Line Cache prepare block conflict",
          K(ret),
          "target_file_id",
          file_id,
          "cur_file_id",
          key_.get_file_id(),
          K(*this));
    } else {
      LOG_WARN("Line Cache BlockKey prepare fail", K(ret), K(key_), K(file_id));
    }
  } else {
    // preemption is successful, initialize each variable
    token_id_ = token_id;
    lines_ = lines;
    read_size_ = 0;

    // get the timestamp of the last update
    last_access_tstamp = ATOMIC_LOAD(&last_access_timestamp_);

    // update access timestamp
    update_access_timestamp();

    // finish prepare
    key_.prepare_end(file_id);
  }
  return ret;
}

int ObLogLineCache::Block::do_get_line_(
    const int64_t line_index, LineFutex& futex, const int64_t timeout_us, char*& line, bool& need_load)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(lines_)) {
    LOG_WARN("lines_ is invalid", KP(lines_));
    ret = OB_NOT_INIT;
  } else {
    int64_t wait_time = timeout_us;
    int64_t end_tstamp = ObTimeUtility::current_time() + timeout_us;
    LineStatusType* line_status = line_status_ + line_index;
    LineStatusType cur_val = ATOMIC_LOAD(line_status);

    line = NULL;
    need_load = false;

    // cycle waiting state changing to READY
    // The reason why we need to wait in a loop here is because the state transition
    // of LINE will loop between EMPTY <-> LOADING
    while (OB_SUCCESS == ret && NULL == line) {
      if (EMPTY == cur_val) {
        // line is empty, try to get the right to load
        cur_val = ATOMIC_CAS(line_status, EMPTY, LOADING);
        // EMPTY -> LOADING is successful, get the right to load
        if (EMPTY == cur_val) {
          line = lines_ + (line_index * LINE_SIZE);
          need_load = true;
        }
      }

      if (NULL == line) {
        // if the status is always equal to LOADING, wait in a loop
        if (LOADING == cur_val) {
          if (wait_time <= 0) {
            ret = OB_TIMEOUT;
          } else if (OB_FAIL(futex.wait(line_status, LOADING, wait_time))) {
            if (OB_TIMEOUT != ret) {
              LOG_WARN("wait futex fail", K(ret), K(*line_status), K(timeout_us));
            }
          } else {
            cur_val = ATOMIC_LOAD(line_status);
            wait_time = end_tstamp - ObTimeUtility::current_time();
          }
        }

        if (OB_SUCCESS == ret && READY == cur_val) {
          // READY state directly obtains LINE, and the mark as no need to be loaded
          line = lines_ + (line_index * LINE_SIZE);
          need_load = false;
        }
      }
    }
  }
  return ret;
}

int ObLogLineCache::Block::get_line(const file_id_t file_id, const offset_t offset, LineFutex& futex,
    const int64_t timeout_us, char*& line, bool& need_load)
{
  int ret = OB_SUCCESS;
  int64_t line_index = line_index_in_block(offset);

  if (OB_UNLIKELY(ObLogLineCache::is_entry_invalid(file_id, offset))) {
    LOG_WARN("invalid argument", K(file_id), K(offset));
    ret = OB_INVALID_ARGUMENT;
  }
  // get the right to read, increase the reference count
  else if (OB_FAIL(key_.get(file_id))) {
    if (OB_ITEM_NOT_MATCH == ret) {
      // conflict
    } else if (OB_ITEM_NOT_SETTED == ret) {
      // the current block is invalid
    }
  } else {
    // In the process of acquiring the line, may wait, and this period of time may be considered can be eliminated.
    // Therefore, first update the access timestamp to avoid being eliminated by execution.
    update_access_timestamp();

    // perform specific get operations
    if (OB_FAIL(do_get_line_(line_index, futex, timeout_us, line, need_load))) {
      if (OB_TIMEOUT == ret) {
        // timeout
      } else {
        LOG_WARN("Line Cache block do_get_line_ fail",
            K(ret),
            K(line_index),
            K(timeout_us),
            K(file_id),
            K(offset),
            K(*this));
      }
    }

    // if getting the line fails, revert the read operation and decrease the reference count
    if (OB_SUCCESS != ret) {
      int revert_ret = key_.revert(file_id);
      if (OB_SUCCESS != revert_ret) {
        LOG_WARN("BlockKey revert fail", K(revert_ret), K(file_id), K(key_));
      }
    }
  }
  return ret;
}

int ObLogLineCache::Block::revert_line(const file_id_t file_id, const offset_t offset, const int64_t read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObLogLineCache::is_entry_invalid(file_id, offset))) {
    LOG_WARN("invalid argument", K(file_id), K(offset));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // FIXME: Essentially, only the read operation should update the access timestamp.
    // If the revert operation is updated, the block cannot be released quickly.
    // However, in the future, we still need to evaluate whether revert should be
    // updated if the line is locally cached after being got.
    // update_access_timestamp();

    // increase the amount of data read
    (void)ATOMIC_FAA(&read_size_, read_size);

    // perform revert, subtract the reference count
    if (OB_FAIL(key_.revert(file_id))) {
      LOG_WARN("BlockKey revert fail", K(ret), K(file_id), K(key_));
    }
  }
  return ret;
}

// Note: This operation should be called after the get() operation and between
// the revert() operations, otherwise the correctness is not guaranteed.
int ObLogLineCache::Block::mark_line_status(
    const file_id_t file_id, const offset_t offset, const bool is_load_ready, LineFutex& futex)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObLogLineCache::is_entry_invalid(file_id, offset))) {
    LOG_WARN("invalid argument", K(file_id), K(offset));
    ret = OB_INVALID_ARGUMENT;
  }
  // check file_id
  else if (OB_UNLIKELY(file_id != key_.get_file_id())) {
    LOG_WARN("file_id does not match", K(key_), K(file_id), K(offset));
    ret = OB_ITEM_NOT_MATCH;
  }
  // reference count must be greater than 0, otherwise it cannot be executed
  else if (OB_UNLIKELY(key_.get_ref_cnt() <= 0)) {
    LOG_WARN("Block reference count is not positive, unexpected error", K(key_), K(file_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t line_index = line_index_in_block(offset);
    LineStatusType* line_status = line_status_ + line_index;
    // if the external load fails, the state rolls back to EMPTY
    LineStatusType target_status = is_load_ready ? READY : EMPTY;

    int old_val = ATOMIC_CAS(line_status, LOADING, target_status);

    if (OB_UNLIKELY(old_val != LOADING)) {
      LOG_WARN("invalid line status which is not LOADING when revert line",
          K(file_id),
          K(offset),
          K(line_index),
          K(*line_status),
          K(old_val),
          K(is_load_ready));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // wake up all waiting threads
      futex.wake(line_status, INT64_MAX);

      // update access timestamp
      update_access_timestamp();
    }
  }
  return ret;
}

void ObLogLineCache::Block::wash(const int64_t expire_time, const int64_t cur_tstamp, bool& wash_succeed,
    char*& washed_lines, int64_t& washed_line_count, int64_t& washed_line_read_size, TokenIDType& token_id)
{
  int64_t last_access_tstamp = ATOMIC_LOAD(&last_access_timestamp_);
  int64_t delta_time = cur_tstamp - last_access_tstamp;
  bool can_expire_wash = (last_access_tstamp > 0 && delta_time >= expire_time);

  wash_succeed = false;
  washed_lines = NULL;
  washed_line_count = 0;
  token_id = INVALID_TOKEN_ID;
  washed_line_read_size = 0;

  if (can_expire_wash) {
    // When the timeout is reached, check whether it is really washable
    // if can, acquire the mutex at the same time to perform the wash operation
    if (!key_.wash_begin()) {
      LOG_TRACE(
          "[LINE_CACHE] expired block can not wash", K(delta_time), K(expire_time), K(last_access_tstamp), K(*this));
    } else {
      // reload the data and judge based on the latest data
      last_access_tstamp = ATOMIC_LOAD(&last_access_timestamp_);
      delta_time = cur_tstamp - last_access_tstamp;
      can_expire_wash = (last_access_tstamp > 0 && delta_time >= expire_time);

      if (can_expire_wash) {
        // traverse all the lines to see which lines have loaded data
        for (int64_t index = 0; index < LINE_COUNT_PER_BLOCK; index++) {
          if (READY == line_status_[index]) {
            washed_line_count++;
          }
        }

        token_id = token_id_;
        washed_lines = lines_;
        wash_succeed = true;
        washed_line_read_size = ATOMIC_LOAD(&read_size_);

        LOG_TRACE("[LINE_CACHE] wash expired block", K(expire_time), K(delta_time), K(*this));

        // reset all domains
        reset();
      }

      // after the operation is completed, notify key that wash is complete
      key_.wash_end(wash_succeed);
    }
  }
}

const char* ObLogLineCache::GetLineTimeoutReason::get_str(REASON reason)
{
  const char* result = "GET_LINE_INIT";
  switch (reason) {
    case GET_LINE_INIT:
      result = "GET_LINE_INIT";
      break;
    case WAIT_LOAD_LINE_TIMEOUT:
      result = "WAIT_LOAD_LINE_TIMEOUT";
      break;
    case BLOCK_CONFLICT_TIMEOUT:
      result = "BLOCK_CONFLICT_TIMEOUT";
      break;
    case BLOCK_NOT_HIT_TIMEOUT:
      result = "BLOCK_NOT_HIT_TIMEOUT";
      break;
    default:
      result = "UNEXPECTED_REASON";
      break;
  }
  return result;
}

}  // namespace clog
}  // namespace oceanbase
