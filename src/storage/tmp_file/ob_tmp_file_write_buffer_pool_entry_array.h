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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_WRITE_BUFFER_POOL_ENTRY_ARRAY_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_WRITE_BUFFER_POOL_ENTRY_ARRAY_H_

#include "lib/container/ob_array.h"
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
  bool is_block_beginning_; // means buf_ points to the beginning of a memory block,
                            // allocator only free page entry with this flag set to true.
  char *buf_;
  int64_t fd_;
  int32_t state_;
  uint32_t next_page_id_;
  ObTmpFilePageUniqKey page_key_;
};

// wrap ObArray to support release memory
class ObTmpWriteBufferPoolEntryArray
{
public:
  ObTmpWriteBufferPoolEntryArray() : is_inited_(false), size_(0), buckets_() {}
  ~ObTmpWriteBufferPoolEntryArray() { destroy(); }
  int init();
  void destroy();
  int push_back(const ObPageEntry &entry);
  void pop_back();
  int64_t size() const { return size_; }
  int64_t count() const { return size(); }

  inline ObPageEntry &operator[] (const int64_t idx)
  {
    const int64_t bucket_idx = idx / MAX_BUCKET_CAPACITY;
    const int64_t bucket_offset = idx % MAX_BUCKET_CAPACITY;
    return buckets_[bucket_idx].at(bucket_offset);
  }
private:
  // by reserving 10000 entries(48 bytes), the array buffer will be upper aligned to accommodate 10085 entries
  static const int64_t MAX_BUCKET_CAPACITY = 10085;

  int add_new_bucket_();
private:
  bool is_inited_;
  int64_t size_;
  ObArray<ObArray<ObPageEntry>> buckets_;
  ModulePageAllocator allocator_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif
