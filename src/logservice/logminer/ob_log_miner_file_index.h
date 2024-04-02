/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_MINER_FILE_INDEX_H_
#define OCEANBASE_LOG_MINER_FILE_INDEX_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_log_miner_progress_range.h"

namespace oceanbase
{
namespace oblogminer
{

class FileIndexItem
{
public:
  FileIndexItem() { reset(); }
  FileIndexItem(const int64_t file_id, const int64_t min_commit_ts, const int64_t max_commit_ts)
  { reset(file_id, min_commit_ts, max_commit_ts); }
  ~FileIndexItem() { reset(); }

  void reset() {
    file_id_ = -1;
    range_.reset();
  }

  void reset(const int64_t file_id, const int64_t min_commit_ts, const int64_t max_commit_ts)
  {
    file_id_ = file_id;
    range_.min_commit_ts_ = min_commit_ts;
    range_.max_commit_ts_ = max_commit_ts;
  }

  bool operator==(const FileIndexItem &that) const
  {
    return file_id_ == that.file_id_ && range_ == that.range_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    K(file_id_),
    K(range_)
  )

  int64_t file_id_;
  ObLogMinerProgressRange range_;
};

class FileIndex
{
public:
  FileIndex():
      alloc_(),
      index_file_len_(0),
      index_array_() {}
  ~FileIndex() { reset(); }

  void reset() {
    index_file_len_ = 0;
    index_array_.reset();
    alloc_.reset();
  }

  int insert_index_item(const int64_t file_id, const int64_t min_commit_ts, const int64_t max_commit_ts);

  int insert_index_item(const FileIndexItem &item);

  int get_index_item(const int64_t file_id, FileIndexItem *&item) const;

  int64_t get_index_file_len() const;

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    "file_num", index_array_.count()
  )
private:
  int alloc_index_item_(FileIndexItem *&item);

private:
  ObArenaAllocator alloc_;
  int64_t index_file_len_;
  ObSEArray<FileIndexItem* , 16> index_array_;
};

}
}

#endif