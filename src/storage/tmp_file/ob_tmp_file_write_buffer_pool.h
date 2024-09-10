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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_WRITE_BUFFER_POOL_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_WRITE_BUFFER_POOL_H_

#include "lib/lock/ob_tc_rwlock.h"
#include "lib/container/ob_array.h"
#include "lib/queue/ob_link.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpWriteBufferPool;
enum class PageEntryType
{
  INVALID = -1,
  DATA = 0,
  META = 1
};

struct ObTmpFilePageUniqKey
{
public:
  ObTmpFilePageUniqKey() : type_(PageEntryType::INVALID), virtual_page_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID) {}
  explicit ObTmpFilePageUniqKey(const int64_t virtual_page_id) :
               type_(PageEntryType::DATA), virtual_page_id_(virtual_page_id) {}
  explicit ObTmpFilePageUniqKey(const int64_t tree_level, const int64_t level_page_index) :
               type_(PageEntryType::META), tree_level_(tree_level),
               level_page_index_ (level_page_index) {}

  void reset()
  {
    virtual_page_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    type_ = PageEntryType::INVALID;
  }

  OB_INLINE bool is_valid() const
  {
    return type_ != PageEntryType::INVALID &&
           type_ == PageEntryType::META ?
           0 <= tree_level_ && 0 <= level_page_index_ :
           ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != virtual_page_id_;
  }
  bool operator==(const ObTmpFilePageUniqKey other) const
  {
    return type_ == other.type_ && virtual_page_id_ == other.virtual_page_id_;
  }
  bool operator!=(const ObTmpFilePageUniqKey other) const
  {
    return type_ != other.type_ || virtual_page_id_ != other.virtual_page_id_;
  }

public:
  PageEntryType type_;
  union {
    int64_t virtual_page_id_; // page_offset / page_size
    struct { //The specific value for the tree pages
      int64_t tree_level_:16;
      int64_t level_page_index_:48;
    };
  };
  TO_STRING_KV(K(type_), K(virtual_page_id_), K(tree_level_), K(level_page_index_));
};

struct ObPageEntry final
{
  friend class ObTmpWriteBufferPool;
public:
  ObPageEntry()
    : buf_(nullptr),
      fd_(-1),
      state_(State::INVALID),
      next_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      page_key_() {}
  ObPageEntry(const int64_t fd, const uint32_t next_page_id, char *buf)
    : buf_(buf),
      fd_(fd),
      state_(State::INVALID),
      next_page_id_(next_page_id),
      page_key_() {}

  int switch_state(const int64_t op);

  TO_STRING_KV(K(fd_), K(page_key_), K(next_page_id_), K(state_), KP(buf_));
public:
  struct State
  {
  public:
    static const int32_t N = -1;          // illegal state
    static const int32_t INVALID = 0;     // page entry is INVALID after page is freed or before allocating
    static const int32_t INITED = 1;      // page entry is INITED after allocating
    static const int32_t LOADING = 2;     // page entry is LOADING after sending async io to read page from disk
    static const int32_t CACHED = 3;      // page entry is CACHED when page is clean
    static const int32_t DIRTY = 4;       // page entry is DIRTY after page is written
    static const int32_t WRITE_BACK = 5;  // page entry is WRITE_BACK when sending async io to write page to disk
    static const int32_t MAX = 6;
  public:
    static bool is_valid(const int32_t state){
      return state > N && state < MAX;
    }
  };

  struct Ops
  {
  public:
    static const int64_t INVALID = -1;
    static const int64_t ALLOC = 0;
    static const int64_t LOAD = 1;
    static const int64_t LOAD_FAIL = 2;
    static const int64_t LOAD_SUCC = 3;
    static const int64_t DELETE = 4;
    static const int64_t WRITE = 5;
    static const int64_t WRITE_BACK = 6;
    static const int64_t WRITE_BACK_FAILED = 7;
    static const int64_t WRITE_BACK_SUCC = 8;
    static const int64_t MAX = 9;
  public:
    static bool is_valid(const int64_t op){
      return op > INVALID && op < MAX;
    }
  };
public:
  char *buf_;
  int64_t fd_;
  int32_t state_;
  uint32_t next_page_id_;
  ObTmpFilePageUniqKey page_key_;
};

// preallocate a set of pages for the tmp file to write data. the pages are divided into data and meta types.
// data type pages can use up to 90% of the entire buffer pool space, while meta type pages have no upper limit.
// we build ObTmpWriteBufferPool upon this assumption: caller ensure only 1 writer operating a page entry at a time,
// and write operation must be exclusive with other r/w operations, therefore we have no need to limit the concurrency
// for every single page entry here.
class ObTmpWriteBufferPool final
{
public:
  // block size: 2MB - 24KB (header), use block size smaller than 2MB to avoid redundant AObject header
  static const int64_t WBP_BLOCK_SIZE = 2 * 1024 * 1024 - 24 * 1024;
  static const int64_t BLOCK_PAGE_NUMS = WBP_BLOCK_SIZE / ObTmpFileGlobal::PAGE_SIZE;    // 253 pages per block (24KB for header)
  static const int64_t INITIAL_POOL_SIZE = WBP_BLOCK_SIZE;
  static const int64_t INITIAL_PAGE_NUMS = INITIAL_POOL_SIZE / ObTmpFileGlobal::PAGE_SIZE;

public:
  ObTmpWriteBufferPool();
  ~ObTmpWriteBufferPool();
  int init();
  void destroy();

public:
  // 1. according to the type_ of page_key, allocate a meta page or data page and set its state to INITED
  // 2. return OB_ALLOCATE_TMP_FILE_PAGE_FAILED if data page number exceeds limits
  // 3. always allow to alloc a meta page
  int alloc_page(const int64_t fd,
                 const ObTmpFilePageUniqKey page_key,
                 uint32_t &new_page_id,
                 char *&buf);

  // read the content of a page and keep its original page state
  int read_page(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key,
                char *&buf, uint32_t &next_page_id);

  // set prev_page_id.next_page_id to point to 'page_id' without changing the page status
  int link_page(const int64_t fd, const uint32_t page_id, const uint32_t prev_page_id,
                const ObTmpFilePageUniqKey prev_page_key);

  // free given pages with INITED/CACHED/DIRTY state,
  // return OB_STATE_NOT_MATCH if try to delete pages with other states
  int free_page(const int64_t fd, const uint32_t page_id,
                const ObTmpFilePageUniqKey page_key, uint32_t &next_page_id);

  /**
   * truncate a page from beginning to 'truncate_size' [0, truncate_size - 1], set data to 0;
   * truncate_page will not change page state. if page is already flushed to disk, truncate_page
   * only truncate data in buffer pool and will not mark page as dirty
   */
  int truncate_page(const int64_t fd, const uint32_t page_id,
                    const ObTmpFilePageUniqKey page_key,
                    const int64_t truncate_size);
public:
  /**
   *  given page_id, output next_page_id if existed;
   *  return OB_ITER_END for no more pages, others for error
   */
  int get_next_page_id(const int64_t fd,
                       const uint32_t page_id,
                       const ObTmpFilePageUniqKey page_key,
                       uint32_t &next_page_id);

  /**
   * iter from the given page_id to end of the page list of a tmp file,
   * to find a page which has a same virtual_page_id
   * @param[in] virtual_page_id: the page we want to read(it equal to page_offset / page_size)
   * @param[in] begin_page_id: the first cached page id of the tmp file
   * @param[out] page_id: the target page
   */
  int get_page_id_by_virtual_id(const int64_t fd,
                            const int64_t virtual_page_id,
                            const uint32_t begin_page_id,
                            uint32_t &page_id);

  /**
   * get page_virtual_id of the given page_id,
   * return OB_SEARCH_NOT_FOUND for page is INVALID,
   */
  int get_page_virtual_id(const int64_t fd, const uint32_t page_id, int64_t &virtual_page_id);

public:
  int64_t get_swap_size();
  int64_t get_memory_limit();
  bool is_exist(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  bool is_inited(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  bool is_loading(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  bool is_cached(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  bool is_dirty(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  bool is_write_back(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  int notify_dirty(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  int notify_load(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  int notify_load_succ(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  int notify_load_fail(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  int notify_write_back(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  int notify_write_back_succ(const int64_t fd, const uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  int notify_write_back_fail(int64_t fd, uint32_t page_id, const ObTmpFilePageUniqKey page_key);
  int64_t get_max_page_num();
  int64_t get_data_page_num();
  int64_t get_dirty_page_num();
  int64_t get_write_back_page_num();
  int64_t get_dirty_meta_page_num();
  int64_t get_dirty_data_page_num();
  int64_t get_dirty_page_percentage();
  int64_t get_cannot_be_evicted_page_percentage();
  int64_t get_max_data_page_num();
  int64_t get_meta_page_num();
  int64_t get_free_data_page_num();
  void print_statistics();
private:
  static double MAX_DATA_PAGE_USAGE_RATIO; // control data pages ratio, can be preempted by meta pages
  // only for unittest
  OB_INLINE void set_max_data_page_usage_ratio_(const double ratio)
  {
    MAX_DATA_PAGE_USAGE_RATIO = ratio;
  }
  int read_page_(const int64_t fd,
                 const uint32_t page_id,
                 const ObTmpFilePageUniqKey page_key,
                 char *&buf,
                 uint32_t &next_page_id);
  int alloc_page_(const int64_t fd,
                  const ObTmpFilePageUniqKey page_key,
                  uint32_t &new_page_id,
                  char *&buf);
  int inner_alloc_page_(const int64_t fd,
                        const ObTmpFilePageUniqKey page_key,
                        uint32_t &new_page_id,
                        char *&buf);
  // check if the specified PageEntryType has available space
  bool has_free_page_(PageEntryType type);
  // ATTENTION! access fat_, needs to be protected by r-lock
  OB_INLINE bool is_valid_page_id_(const uint32_t page_id) const
  {
    return page_id != ObTmpFileGlobal::INVALID_PAGE_ID && page_id >= 0 &&
           page_id < fat_.count() && OB_NOT_NULL(fat_[page_id].buf_);
  }
  int expand_();
  int reduce_();
  DISALLOW_COPY_AND_ASSIGN(ObTmpWriteBufferPool);
private:
  common::ObArray<ObPageEntry> fat_;   // file allocation table
  common::TCRWLock lock_;            // holds w-lock when expanding and shrinking fat_, holds r-lock when reading fat_
  common::ObFIFOAllocator allocator_;
  bool is_inited_;
  int64_t capacity_;                   // in bytes
  int64_t dirty_page_num_;
  int64_t used_page_num_;
  uint32_t first_free_page_id_;
  int64_t wbp_memory_limit_;           // in bytes
  int64_t default_wbp_memory_limit_;   // if this var is valid, the wbp memory limit will always be it.
                                       // currently, this var is only modified in ut.
  int64_t last_access_tenant_config_ts_;
  int64_t meta_page_cnt_;
  int64_t data_page_cnt_;
  int64_t dirty_meta_page_cnt_;
  int64_t dirty_data_page_cnt_;
  int64_t write_back_data_cnt_;
  int64_t write_back_meta_cnt_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif
