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

#ifndef OCEANBASE_CLOG_ILOG_PER_FILE_CACHE_H_
#define OCEANBASE_CLOG_ILOG_PER_FILE_CACHE_H_

#include "lib/allocator/page_arena.h"
#include "ob_log_entry.h"
#include "ob_file_id_cache.h"

namespace oceanbase {
namespace clog {
class ObIlogStorage;
class ObIRawIndexIterator;

// to buffer raw entries just read from ilog file
struct RawArray {
  ObIndexEntry* arr_;  // prealloc enough space (ILOG_ENTRY_COUNT_PER_FILE)
  int64_t count_;      // real count

  RawArray() : arr_(NULL), count_(0)
  {}
  bool is_valid() const
  {
    return NULL != arr_ && count_ > 0;
  }
  TO_STRING_KV(KP(arr_), K(count_));
};

class ObIlogPerFileCache {
public:
  ObIlogPerFileCache()
      : is_inited_(false),
        file_id_(common::OB_INVALID_FILE_ID),
        pf_allocator_(NULL),
        cursor_arr_(NULL),
        arr_size_(0),
        tail_pos_(0)
  {}
  ~ObIlogPerFileCache()
  {
    destroy();
  }
  int init(const file_id_t file_id, const int64_t array_size, common::PageArena<>* pf_allocator);
  bool is_valid() const
  {
    return is_inited_;
  }
  void destroy();

public:
  // append cursor to sorted array
  int append_cursor(const ObLogCursorExt& cursor);
  int query_cursor(const common::ObPartitionKey& pkey, const uint64_t query_log_id, const uint64_t min_log_id,
      const uint64_t max_log_id, const offset_t start_offset_index, ObGetCursorResult& result);
  int locate_by_timestamp(const common::ObPartitionKey& pkey, const int64_t start_ts, const uint64_t min_log_id,
      const uint64_t max_log_id, const offset_t start_offset_index, uint64_t& target_log_id,
      int64_t& target_log_timestamp);

private:
  int binary_search_timestamp_from_cursor_array_(const ObLogCursorExt* csr_arr, const int64_t array_len,
      const int64_t start_ts, const ObLogCursorExt*& target_cursor);

private:
  bool is_inited_;
  file_id_t file_id_;
  common::PageArena<>* pf_allocator_;
  ObLogCursorExt* cursor_arr_;  // sorted cursor array
  int64_t arr_size_;
  int64_t tail_pos_;
};

// return
//   e1  < e2   =>   -1
//   e1  > e2   =>    1
//   e1 == e2   =>    0
int ilog_entry_comparator(const void* e1, const void* e2);

class ObIlogPerFileCacheBuilder {
public:
  ObIlogPerFileCacheBuilder() : ilog_storage_(NULL), file_id_cache_(NULL)
  {}

public:
  int init(ObIlogStorage* ilog_storage, ObFileIdCache* file_id_cache);
  void destroy()
  {
    ilog_storage_ = NULL;
    file_id_cache_ = NULL;
  }
  int build_cache(const file_id_t file_id, ObIlogPerFileCache* pf_cache, common::PageArena<>& pf_page_arena,
      ObIlogStorageQueryCost& csr_cost);

private:
  class BackfillGenerator {
  public:
    BackfillGenerator()
    {
      reset();
    }
    int init(const file_id_t file_id, ObFileIdCache* file_id_cache);
    void reset();
    int track(const ObIndexEntry& ilog_entry, const int32_t array_index);
    int close();

  private:
    int do_track_(const common::ObPartitionKey& pkey, const uint64_t log_id, const int32_t array_index);
    int end_cur_pkey_();

  private:
    file_id_t file_id_;
    ObFileIdCache* file_id_cache_;
    offset_t cur_offset_;

    common::ObPartitionKey cur_pkey_;
    uint64_t cur_min_log_id_;
    uint64_t cur_max_log_id_;
    offset_t cur_start_offset_index_;
  };

private:
  inline ObLogCursorExt trim_to_cursor_(const ObIndexEntry& ilog_entry);
  int prepare_sorted_raw_array_(const file_id_t file_id, ObIRawIndexIterator* raw_ilog_iter,
      common::ObIAllocator& raw_entry_allocator, RawArray& raw_array, ObIlogStorageQueryCost& csr_cost);
  int build_pf_cache_(const file_id_t file_id, const RawArray& raw_array, ObIlogPerFileCache* pf_cache);
  static const int64_t ILOG_ENTRY_COUNT_PER_FILE = 2 * 1000 * 1000;

private:
  ObIlogStorage* ilog_storage_;
  ObFileIdCache* file_id_cache_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_ILOG_PER_FILE_CACHE_H_
