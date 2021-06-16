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

#ifndef OCEANBASE_CLOG_OB_LOG_LINE_CACHE_
#define OCEANBASE_CLOG_OB_LOG_LINE_CACHE_

#include "lib/lock/ob_latch.h"       // ObLatch
#include "common/ob_queue_thread.h"  // ObCond

#include "clog/ob_log_define.h"  // CLOG_FILE_SIZE
#include "clog/ob_dist_futex.h"  // ObDistFutex

namespace oceanbase {
namespace clog {

/**
 * CLOG file global cache, cache data in units of Line.
 * Use BLOCK as the unit to manage Line and allocate memory,
 *
 * Design Points:
 * 1. Limit the overall memory usage, if the memory usage reaches the limit, get the memory through elimination.
 * 2. Apply and release memory in units of BLOCK, the purpose is to optimize the
 *    Cache elimination process and avoid scanning too much memory at a time.
 * 3. Cache data in Line as the unit to avoid reducing the effect of disk read amplification.
 * 4. In order to reduce conflicts, one million BLOCK addressing space is allocated
 *    at a time to try to avoid mutual influence between different streams.
 * 5. Line memory must be aligned according to CLOG_DIO_ALIGN_SIZE, to use this memory to read disk data.
 * 6. In order to optimize the cost of memory allocation waste by memory alignment, memory will be pre-allocated.
 *
 * */
class ObLogLineCache {
public:
  static const int64_t LINE_SIZE = 1L << 16L;  // 64K
  // BLOCK size
  // Note: BLOCK_SIZE must be able to divide CLOG_FILE_SIZE, ensure a file will not cross BLOCK.
  static const int64_t BLOCK_SIZE = (1L << 20L);  // 1M

  static const int64_t LINE_COUNT_PER_BLOCK = BLOCK_SIZE / LINE_SIZE;
  static const int64_t BLOCK_COUNT_PER_FILE = CLOG_FILE_SIZE / BLOCK_SIZE;
  // default block array size
  // the larger the array, the smaller the conflict
  static const int64_t DEFAULT_BLOCK_ARRAY_SIZE = 1024L * 1024L;
  static const int64_t MINI_MODE_BLOCK_ARRAY_SIZE = 256L * 1024L;

  // In order to quickly eliminate the loaded memory, the data access time is defined here.
  // It is recommended to consider this factor in all places where Line Cache is used.
  // The shorter the data access time, the faster the block elimination
  static const int64_t DATA_ACCESS_TIME_US = 500 * 1000L;

  // Reserve time for data access, optimize elimination and reduce misjudgment
  static const int64_t DATA_ACCESS_RESERVE_TIME_US = 100 * 1000L;

  // Data elimination time
  // Based on the data access time, calculate the elimination time and add extra
  static const int64_t DATA_EXPIRE_TIME_US = DATA_ACCESS_TIME_US + DATA_ACCESS_RESERVE_TIME_US;

  // consider BLOCK as the smallest time interval for repeated loading
  // if a BLOCK is loaded twice in a short time, it is considered to be repeated loading
  static const int64_t MIN_INTERVAL_CONSIDER_AS_REPEATED_LAOADING = 10L * 1000L * 1000L;

  // LRU elimination rate, percentage
  static const int64_t LRU_WASH_PERCENT = 5;  // 5% can ensure eliminate up to 100 elements each time

  // waiting time when block conflicts
  static const int64_t WAIT_TIME_ON_BLOCK_CONFLICT = 1000L;

  // waiting time when the memory limit is exceeded
  static const int64_t WAIT_TIME_ON_EXCEED_MEM_LIMIT = 1000L;

  // waiting time when memory allocation failure
  static const int64_t WAIT_TIME_ON_ALLOCATE_MEM_FAIL = 1000L;

  // time interval for printing obsolete information
  static const int64_t PRINT_WASH_STAT_INFO_INTERVAL = 1 * 1000L * 1000L;

public:
  ObLogLineCache();
  ~ObLogLineCache()
  {
    destroy();
  }

  int init(const int64_t max_cache_file_count, const int64_t fixed_cached_size_in_file_count, const char* label);
  void destroy();

  // whether to align according to DIO
  static bool is_aligned(char* ptr)
  {
    return (0 == (reinterpret_cast<int64_t>(ptr) & (CLOG_DIO_ALIGN_SIZE - 1)));
  }

  static inline offset_t offset_in_line(const offset_t offset)
  {
    return offset % static_cast<offset_t>(LINE_SIZE);
  }

  static inline int64_t line_index_in_block(const offset_t offset)
  {
    return (offset % BLOCK_SIZE) / LINE_SIZE;
  }

  static inline int64_t calc_block_index(const file_id_t file_id, const offset_t offset)
  {
    return ((file_id - 1) * BLOCK_COUNT_PER_FILE + offset / BLOCK_SIZE) % DEFAULT_BLOCK_ARRAY_SIZE;
  }

  int64_t get_cached_line_count() const
  {
    return ATOMIC_LOAD(&cached_line_count_);
  }
  int64_t get_cached_block_count() const
  {
    return block_token_.get_used_count();
  }
  // Get the size of the cache memory
  int64_t get_cached_mem_size() const
  {
    return get_cached_block_count() * BLOCK_SIZE;
  }

  // get Line data
  // NOTE:
  // 1. When the line data is invalid, only one thread can obtain the right to load the data,
  //    and the other threads can only wait until the data is loaded.
  // 2. The process of obtaining Line data may need to allocate memory.
  //    When the total Cache memory limit is exceeded, OB_EXCEED_MEM_LIMIT will be returned.
  //
  //
  // @param [in] file_id          target file
  // @param [in] offset           file offset
  // @param [in] timeout_us       timeout, microsecond unit
  // @param [out] line            line buffer returned
  // @param [out] need_load_data  whether line needs to load data, true means that the line buffer data
  //                              is invalid and the data needs to be loaded
  //
  // @retval OB_SUCCESS           success
  // @retval OB_ITEM_NOT_MATCH    conflict
  // @retval OB_EXCEED_MEM_LIMIT  total memory limit exceeded
  // @retval OB_TIMEOUT           timeout, locking timeout, or the target line is being loaded by other threads,
  //                              waiting for "load success" timeout
  // @retval other error code     failed
  int get_line(
      const file_id_t file_id, const offset_t offset, const int64_t timeout_us, char*& line, bool& need_load_data);

  // return back line data
  //
  // read_size: When returning back the line, the amount of data read from the line
  int revert_line(const file_id_t file_id, const offset_t offset, const int64_t read_size);

  // mark the line status, whether is ready
  //
  // @param file_id         target file
  // @param offset          file offset, to locate the corresponding Line
  // @param is_load_ready   loading completed or not
  //
  // @retval OB_SUCCESS     success
  // @retval other errcode  failed
  int mark_line_status(const file_id_t file_id, const offset_t offset, const bool is_load_ready);

  // check entry is valid or not
  inline static bool is_entry_invalid(const file_id_t file_id, const offset_t offset)
  {
    return (OB_UNLIKELY(0 == file_id) || OB_UNLIKELY(offset < 0) || OB_UNLIKELY(offset >= CLOG_FILE_SIZE));
  }

  // eliminate expired blocks
  void wash();

  // perform statistics
  // single thread call
  void stat();

public:
  // assume that the number of Token and Block will not exceed int32_t
  typedef int32_t TokenIDType;
  typedef int32_t TokenValueType;
  static const int32_t MAX_TOKEN_ID = INT32_MAX;
  static const int32_t INVALID_TOKEN_ID = -1;
  static const int32_t INVALID_TOKEN_VALUE = -1;
  typedef common::ObLatch LatchType;
  struct WLatchGuard {
    WLatchGuard(LatchType& latch) : guard_(latch, common::ObLatchIds::CLOG_EXTERNAL_EXEC_LOCK)
    {}
    common::ObLatchWGuard guard_;
  };
  struct RLatchGuard {
    RLatchGuard(LatchType& latch) : guard_(latch, common::ObLatchIds::CLOG_EXTERNAL_EXEC_LOCK)
    {}
    common::ObLatchRGuard guard_;
  };

  // private member function
private:
  int do_wash_(const int64_t expire_time, int64_t& washed_block_count, int64_t& washed_line_count);
  int do_lru_wash_();
  int do_wash_block_(const TokenValueType block_index, const int64_t expire_time, const int64_t cur_tstamp,
      bool& wash_succeed, int64_t& block_washed_line_count, int64_t& block_washed_line_read_size);
  int prepare_block_(const int64_t block_index, const file_id_t file_id);

  struct Block;
  int handle_when_block_conflict_(
      Block& block, const int64_t block_index, const file_id_t file_id, const int64_t end_tstamp);
  int handle_when_block_not_hit_(
      const int64_t block_index, const file_id_t file_id, const int64_t end_tstamp, bool& exceed_mem_limit_occured);

private:
  // Use [file_id, ref_cnt] to identify the structure of a Block,
  // which is also used to synchronize and mutually exclusive operations.
  //
  // state machine transition:
  //
  // 1. initial: [0, 0]
  //
  // 2. prepare_begin: prepare operation starts, locks on, prevents other operations
  //                   from entering, and prepares to initialize the Block
  //    [0, 0] =>  [file_id, -1]
  //
  // 3. prepare_end: prepare operation ends, release lock and let other operations enter
  //    [file_id, -1]  =>  [file_id, 0]
  //
  // 3. get: read operations, the reference count is incremented by 1, where cnt >= 0
  //    [file_id, cnt]  => [file_id, cnt + 1]
  //
  // 4. revert: return back operation, the reference count is decremented by 1, where: cnt> 0
  //    [file_id, cnt] => [file_id, cnt - 1]
  //
  // 5. wash_begin: elimination operation starts, locks on to prevent other operations from entering
  //    [file_id, 0] => [file_id, -1]
  //
  // 6. wash_end: end of elimination operation
  //    1. If elimination succeed, switch to the initial state: [file_id, -1] => [0, 0]
  //    2. If elimination fails, rollback as before:           [file_id, -1] => [file_id, 0]
  struct BlockKey {
    union Value {
      struct {
        uint32_t file_id_;  // file ID, caller guarantee that BLOCK will not cross files
        int32_t ref_cnt_;   // reference count
      };

      int64_t key_;
    };

    Value v_;

    //**************** member function ****************
    BlockKey()
    {
      v_.file_id_ = 0;
      v_.ref_cnt_ = 0;
    }

    int prepare_begin(const file_id_t file_id);
    void prepare_end(const file_id_t file_id);
    int get(const file_id_t file_id);
    int revert(const file_id_t file_id);

    // return can be eliminated or not
    bool wash_begin();
    void wash_end(const bool wash_succeed);

    file_id_t get_file_id() const
    {
      return ATOMIC_LOAD(&v_.file_id_);
    }
    int32_t get_ref_cnt() const
    {
      return ATOMIC_LOAD(&v_.ref_cnt_);
    }

    bool is_empty() const
    {
      return 0 == ATOMIC_LOAD(&v_.key_);
    }

    TO_STRING_KV("file_id", v_.file_id_, "ref_cnt", v_.ref_cnt_);
  };

  struct Block {
    // state transition of a single LINE:
    // EMPTY    -> LOADING    Someone has the loading right and is loading
    // LOADING  -> EMPTY      load failed, rollback status
    // LOADING  -> READY      load succeed, data is valid
    // READY    -> EMPTY      The BLOCK that belongs to is eliminated fully, clear state
    enum LineStatus {
      EMPTY = 0,    // empty
      LOADING = 1,  // data loading
      READY = 2,    // data prepared
    };
    typedef int8_t LineStatusType;
    typedef common::ObDistFutex<LineStatusType> LineFutex;  // Line level Futex

    // ********************** member variables **********************

    // Key of Block, functions include:
    // 1. Uniquely identify a Block
    // 2. As a "read-write lock", mutually exclusive "read-write" and
    //    "write-write" operations, allowing "read-read" concurrency.
    // 3. As a reference count, identifies counts of using on reading the block.
    //
    // In view of the above functions, this variable does not participate in the "reset()" logic.
    BlockKey key_;

    // data variables
    char* lines_;                                       // Line array
    TokenIDType token_id_;                              // corresponding Token
    LineStatusType line_status_[LINE_COUNT_PER_BLOCK];  // mark each Line status

    // statistical variables
    int64_t read_size_;  // the size of the data being read

    // timestamp of last update
    // 1. The value of this variable will increase only, the purpose is to count the
    //    block reuse, so it does not participate in the "reset()" logic
    // 2. frequently accessed concurrently by multiple threads
    volatile int64_t last_access_timestamp_;

    // ********************** member variables **********************
    Block();
    void reset();
    int prepare(const file_id_t file_id, const TokenIDType token_id, char* lines, int64_t& last_access_tstamp);
    int get_line(const file_id_t file_id, const offset_t offset, LineFutex& futex, const int64_t timeout_us,
        char*& line, bool& need_load_data);
    int revert_line(const file_id_t file_id, const offset_t offset, const int64_t read_size);
    int mark_line_status(const file_id_t file_id, const offset_t offset, const bool is_load_ready, LineFutex& futex);
    void wash(const int64_t expire_time, const int64_t cur_tstamp, bool& wash_succeed, char*& washed_lines,
        int64_t& washed_line_count, int64_t& washed_line_read_size, TokenIDType& token_id);
    void update_access_timestamp();
    int64_t get_last_access_timestamp() const
    {
      return ATOMIC_LOAD(&last_access_timestamp_);
    }
    bool is_empty() const
    {
      return key_.is_empty();
    }
    file_id_t get_file_id() const
    {
      return key_.get_file_id();
    }

    TO_STRING_KV(K_(key), KP_(lines), K_(token_id), K_(read_size), K_(last_access_timestamp));

  private:
    int do_get_line_(
        const int64_t line_index, LineFutex& futex, const int64_t timeout_us, char*& line, bool& need_load);
  };

  // Manage block allocation and release modules, the purpose is to
  // manage all allocated blocks and facilitate the recycling of blocks.
  //
  // 1. every time a Block is allocated, a Token must be applied
  // 2. every time a Block is released, a Token must be returned back
  //
  // The Token ID that has not been applied is stored in FreeList.
  // TokenValueType maps Block ID to indicate which block is holding this token.
  //
  // BlockToken maintains the final consistency of the mapping relationship between <token_id, block_id>.
  // Calling convention:
  // 1. Consistency guarantee: If Block maps Token, then Token must map Block;
  //                           on the contrary, it is not guaranteed.
  // 2. expected situation: Token t1 maps Block b1, but Block b1 does not map Token t1
  // 3. In the implementation, first allocate the Token and set the corresponding block_id;
  //    then compete with others concurrently to set the token_id to the block
  struct BlockToken {
    typedef common::ObFixedQueue<void> FreeList;

    TokenValueType* tokens_;  // Token Value, record which block corresponds to
    FreeList free_list_;      // Token idle list

    BlockToken();
    ~BlockToken();

    int init(const int64_t total_count, const char* label);
    void destroy();

    // assign a token
    int alloc(TokenIDType& token_id, const TokenValueType token_value);
    int free(const TokenIDType token_id, const TokenValueType token_value);

    int64_t get_free_count() const
    {
      return free_list_.get_total();
    }
    int64_t get_used_count() const
    {
      return free_list_.get_free();
    }
    int64_t get_total_count() const
    {
      return free_list_.capacity();
    }

  private:
    int push_free_list_(const TokenIDType token_id);
    int pop_free_list_(TokenIDType& token_id);
  };

  // Block memory allocator
  // Used to allocate BLOCK aligned by CLOG_DIO_ALIGN_SIZE
  struct BlockAlloc {
    typedef common::ObFixedQueue<char> FreeList;

    char* buf_;  // Pre-allocated buffer
    const char* label_;
    FreeList free_list_;

    BlockAlloc();
    ~BlockAlloc()
    {
      destroy();
    }

    int init(const int64_t prealloc_block_cnt, const char* label);
    void destroy();

    char* alloc();
    void free(char* ptr);
  };

  struct GetLineTimeoutReason final {
    enum REASON {
      GET_LINE_INIT = 0,
      WAIT_LOAD_LINE_TIMEOUT = 1,
      BLOCK_CONFLICT_TIMEOUT = 2,
      BLOCK_NOT_HIT_TIMEOUT = 3,
    };

    static const char* get_str(REASON reason);
  };

private:
  int inited_;
  Block* blocks_;
  int64_t block_array_size_;
  BlockToken block_token_;
  BlockAlloc block_alloc_;

  // Futex at the line level
  // Used to wait for Line Ready
  Block::LineFutex line_futex_;

  // block elimination condition variable
  // FIXME: ObCond can only wake up one waiter, cond which can be broadcast should be used here
  common::ObCond block_wash_cond_;

  // cached Line combing
  int64_t cached_line_count_ CACHE_ALIGNED;

  // Statistics
  int64_t get_line_count_ CACHE_ALIGNED;  // count of calls to get_line
  int64_t load_line_count_;               // count of lines actually loaded
  int64_t block_hit_count_;               // block hits
  int64_t line_hit_count_;                // line hits
  int64_t block_conflict_count_;          // count of conflicts in the case of block misses
  int64_t exceed_mem_limit_count_;     // count of times the memory limit is exceeded when block misses and apply memory
  int64_t washed_line_count_;          // count of lines eliminated
  int64_t lru_washed_line_count_;      // count of lines eliminated by the lru algorithm
  int64_t washed_line_read_size_;      // length of the data actually read from the eliminated line
  int64_t repeated_block_load_count_;  // count of blocks loaded repeatedly in a short time
  int64_t repeated_block_load_interval_;  // The interval between repeated loading of the block

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogLineCache);
};

}  // namespace clog
}  // namespace oceanbase
#endif
