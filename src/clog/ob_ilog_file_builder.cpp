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

#include "ob_ilog_file_builder.h"
#include "common/ob_partition_key.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/checksum/ob_crc64.h"
#include "ob_ilog_memstore.h"
#include "ob_ilog_per_file_cache.h"  // for RawArray
#include "ob_log_define.h"           // for ObLogCursorExt
#include "ob_log_file_trailer.h"
#include "ob_remote_log_query_engine.h"  // for ObPartitionLogInfo
#include "ob_ilog_store.h"

namespace oceanbase {
using namespace common;
namespace clog {
class ObIlogFileBuilder::PrepareRawArrayFunctor {
public:
  explicit PrepareRawArrayFunctor(RawArray& raw_array) : raw_array_(raw_array), curr_idx_(0)
  {}
  ~PrepareRawArrayFunctor()
  {}

public:
  bool operator()(const ObPartitionLogInfo& partition_log_info, ObLogCursorExt& log_cursor_ext)
  {
    bool bool_ret = false;
    int ret = OB_SUCCESS;
    ObIndexEntry ilog_entry;
    if (curr_idx_ >= raw_array_.count_) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR, "too many cursors, unexpected", K(ret), K(curr_idx_), K(raw_array_.count_));
    } else if (OB_FAIL(ilog_entry.init(partition_log_info.get_partition_key(),
                   partition_log_info.get_log_id(),
                   log_cursor_ext.get_file_id(),
                   log_cursor_ext.get_offset(),
                   log_cursor_ext.get_size(),
                   log_cursor_ext.get_submit_timestamp(),
                   log_cursor_ext.get_accum_checksum(),
                   log_cursor_ext.is_batch_committed()))) {
      CSR_LOG(ERROR, "index_entry init failed", K(ret), K(partition_log_info), K(log_cursor_ext));
    } else if (OB_FAIL(raw_array_.arr_[curr_idx_].shallow_copy(ilog_entry))) {
      CSR_LOG(ERROR, "raw_array shallow_copy failed", K(ret));
    } else {
      bool_ret = true;
      curr_idx_++;
    }
    return bool_ret;
  }

private:
  RawArray& raw_array_;
  int64_t curr_idx_;

private:
  DISALLOW_COPY_AND_ASSIGN(PrepareRawArrayFunctor);
};

class ObIlogFileBuilder::UpdateStartOffsetFunctor {
public:
  explicit UpdateStartOffsetFunctor(const offset_t start_offset) : start_offset_(start_offset)
  {}
  ~UpdateStartOffsetFunctor()
  {}

public:
  bool operator()(const common::ObPartitionKey& partition_key, IndexInfoBlockEntry& index_info_block_entry)
  {
    bool bool_ret = false;
    if (start_offset_ < 0) {
      CSR_LOG(ERROR, "start_offset_ invalid, unexpected", K(partition_key), K(start_offset_));
    } else {
      index_info_block_entry.start_offset_ = start_offset_;
      bool_ret = true;
    }
    return bool_ret;
  }

private:
  offset_t start_offset_;

private:
  DISALLOW_COPY_AND_ASSIGN(UpdateStartOffsetFunctor);
};

class ObIlogFileBuilder::BuildArrayMapFunctor {
public:
  explicit BuildArrayMapFunctor(IndexInfoBlockMap& index_info_block_map) : index_info_block_map_(index_info_block_map)
  {}
  ~BuildArrayMapFunctor()
  {}

public:
  bool operator()(const common::ObPartitionKey& partition_key, IndexInfoBlockEntry& index_info_block_entry)
  {
    int ret = OB_SUCCESS;
    bool bool_ret = false;
    if (OB_FAIL(index_info_block_map_.insert(partition_key, index_info_block_entry))) {
      CSR_LOG(ERROR, "index_info_block_map_ insert failed", K(ret));
    } else {
      bool_ret = true;
    }
    return bool_ret;
  }

private:
  IndexInfoBlockMap& index_info_block_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(BuildArrayMapFunctor);
};

class ObIlogFileBuilder::BuildMemberListMapFunctor {
public:
  explicit BuildMemberListMapFunctor(MemberListMap& memberlist_map) : memberlist_map_(memberlist_map)
  {}
  ~BuildMemberListMapFunctor()
  {}

public:
  bool operator()(const common::ObPartitionKey& partition_key, MemberListInfo& memberlist_info)
  {
    int ret = OB_SUCCESS;
    bool bool_ret = false;
    if (OB_FAIL(memberlist_map_.insert(partition_key, memberlist_info))) {
      CSR_LOG(ERROR, "memberlist_map_ insert failed", K(ret));
    } else {
      bool_ret = true;
    }
    return bool_ret;
  }

private:
  MemberListMap& memberlist_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(BuildMemberListMapFunctor);
};

ObIlogFileBuilder::ObIlogFileBuilder()
    : is_inited_(false),
      ilog_memstore_(NULL),
      buffer_(NULL),
      total_size_(0),
      index_info_block_map_(),
      memberlist_map_(),
      pinned_memory_(NULL),
      page_arena_(),
      time_guard_("ObIlogFileBuilder")
{}

ObIlogFileBuilder::~ObIlogFileBuilder()
{
  destroy();
}

int ObIlogFileBuilder::init(ObIlogMemstore* ilog_memstore, PinnedMemory* pinned_memory)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CSR_LOG(ERROR, "ObIlogFileBuilder init twice", K(ret));
  } else if (OB_ISNULL(ilog_memstore) || OB_ISNULL(pinned_memory)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), KP(ilog_memstore));
  } else {
    is_inited_ = true;
    ilog_memstore_ = ilog_memstore;
    buffer_ = NULL;
    total_size_ = 0;
    pinned_memory_ = pinned_memory;
    page_arena_.set_label(ObModIds::OB_ILOG_FILE_BUILDER);
  }
  return ret;
}

void ObIlogFileBuilder::destroy()
{
  is_inited_ = false;
  ilog_memstore_ = NULL;
  if (NULL != buffer_) {
    ob_free_align(buffer_);
    buffer_ = NULL;
  }
  total_size_ = 0;
  index_info_block_map_.destroy();
  memberlist_map_.destroy();
  page_arena_.free();
}

int ObIlogFileBuilder::get_file_buffer(char*& buffer, int64_t& size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogFileBuilder is not inited", K(ret));
  } else if (NULL == buffer_ && OB_FAIL(build_file_())) {
    CSR_LOG(ERROR, "build_file_ failed", K(ret));
  } else if (NULL == buffer_ || total_size_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "result unexpected", KP(buffer_), K(total_size_));
  } else {
    buffer = buffer_;
    size = total_size_;
  }
  return ret;
}

int ObIlogFileBuilder::build_file_()
{
  int ret = OB_SUCCESS;
  RawArray raw_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogFileBuilder is not inited", K(ret));
  } else if (OB_FAIL(prepare_raw_array_(raw_array))) {
    CSR_LOG(WARN, "prepare_raw_array_ failed", K(ret));
  } else if (OB_FAIL(build_file_buffer_(raw_array))) {
    CSR_LOG(WARN, "build_file_buffer_ failed", K(ret));
  } else {
    CSR_LOG(INFO, "ObIlogFileBuilder build_file success", K(ret), K(time_guard_));
  }
  return ret;
}

int ObIlogFileBuilder::prepare_raw_array_(RawArray& raw_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogFileBuilder is not inited", K(ret));
  } else {
    CLOG_LOG(INFO, "start prepare_raw_array_");
    raw_array.count_ = ilog_memstore_->log_cursor_ext_info_.count();  // friend
    const int64_t size = raw_array.count_ * sizeof(ObIndexEntry);
    raw_array.arr_ = reinterpret_cast<ObIndexEntry*>(
        pinned_memory_->is_valid(size) ? pinned_memory_->get_ptr() : page_arena_.alloc(size));
    PrepareRawArrayFunctor functor(raw_array);
    if (OB_ISNULL(raw_array.arr_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "raw_array alloc failed", K(ret), K(raw_array.count_));
    } else if (OB_FAIL(ilog_memstore_->log_cursor_ext_info_.for_each(functor))) {  // friend
      CSR_LOG(ERROR, "log_cursor_ext_info_ for_each failed", K(ret));
    } else {
      qsort(raw_array.arr_, raw_array.count_, sizeof(ObIndexEntry), ilog_entry_comparator);
    }
  }
  CLOG_LOG(INFO, "prepare_raw_array_ success", K(raw_array.count_), KP(raw_array.arr_));
  time_guard_.click(__FUNCTION__);
  return ret;
}

int ObIlogFileBuilder::build_file_buffer_(const RawArray& raw_array)
{
  int ret = OB_SUCCESS;
  int64_t index_info_block_map_capacity = ilog_memstore_->partition_meta_info_.count();
  int64_t memberlist_map_capacity = ilog_memstore_->partition_memberlist_info_.count();
  if (0 == index_info_block_map_capacity) {
    index_info_block_map_capacity = 1;
  }
  if (0 == memberlist_map_capacity) {
    memberlist_map_capacity = 1;
  }
  if (!raw_array.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(raw_array));
  } else if (OB_FAIL(index_info_block_map_.init(ObModIds::OB_ILOG_FILE_BUILDER, index_info_block_map_capacity))) {
    CSR_LOG(ERROR, "index_info_block_map_ init failed", K(ret));
  } else if (OB_FAIL(memberlist_map_.init(ObModIds::OB_ILOG_FILE_BUILDER, memberlist_map_capacity))) {
    CSR_LOG(ERROR, "memberlist_map_ init failed", K(ret));
  } else {
    const int64_t ILOG_MIN_INFO_BLOCK_SIZE = 64 * 1024;  // 64k
    const int64_t cursor_cnt = raw_array.count_;
    ObLogCursorExt tmp_cursor;
    total_size_ =
        upper_align(tmp_cursor.get_serialize_size() * cursor_cnt, CLOG_DIO_ALIGN_SIZE) +
        upper_align(std::max(index_info_block_map_.get_serialize_size() + memberlist_map_.get_serialize_size(),
                        ILOG_MIN_INFO_BLOCK_SIZE),
            CLOG_DIO_ALIGN_SIZE) +
        upper_align(CLOG_TRAILER_SIZE, CLOG_DIO_ALIGN_SIZE);
    buffer_ =
        reinterpret_cast<char*>(ob_malloc_align(CLOG_DIO_ALIGN_SIZE, total_size_, ObModIds::OB_ILOG_FILE_BUILDER));
    const offset_t trailer_offset = static_cast<offset_t>(total_size_ - CLOG_TRAILER_SIZE);
    offset_t info_block_start_offset = OB_INVALID_OFFSET;
    offset_t after_info_block_offset = OB_INVALID_OFFSET;
    offset_t after_memberlist_block_offset = OB_INVALID_OFFSET;
    int64_t file_content_checksum = 0;
    if (NULL != buffer_) {
      memset(buffer_, 0, total_size_);
    }
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "buffer_ alloc failed", K(ret));
    } else if (OB_FAIL(build_cursor_array_(raw_array, info_block_start_offset))) {
      CSR_LOG(ERROR, "build_cursor_array_ failed", K(ret), K(raw_array));
    } else if (OB_FAIL(build_array_hash_map_(ilog_memstore_->partition_meta_info_, index_info_block_map_))) {
      CSR_LOG(ERROR, "build_array_hash_map_ failed", K(ret));
    } else if (OB_FAIL(build_memberlist_map_(ilog_memstore_->partition_memberlist_info_, memberlist_map_))) {
      CSR_LOG(ERROR, "build_memberlist_map_ failed", K(ret));
    } else if (OB_FAIL(build_info_block_(index_info_block_map_, info_block_start_offset, after_info_block_offset))) {
      CSR_LOG(ERROR, "build_info_block_ failed", K(ret), K(info_block_start_offset));
    } else if (OB_FAIL(
                   build_memberlist_block_(memberlist_map_, after_info_block_offset, after_memberlist_block_offset))) {
      CSR_LOG(ERROR, "build_memberlist_block_ failed", K(ret), K(after_info_block_offset));
    } else if (OB_FAIL(calculate_checksum_(file_content_checksum, after_memberlist_block_offset))) {
      CSR_LOG(ERROR, "calculate_checksum_ failed", K(ret), K(after_memberlist_block_offset));
    } else if (OB_FAIL(build_file_trailer_(trailer_offset,
                   info_block_start_offset,
                   after_info_block_offset - info_block_start_offset,
                   after_info_block_offset,
                   after_memberlist_block_offset - after_info_block_offset,
                   file_content_checksum))) {
      CSR_LOG(ERROR,
          "build_file_trailer_ failed",
          K(ret),
          K(trailer_offset),
          K(info_block_start_offset),
          "info_block_size",
          after_info_block_offset - info_block_start_offset);
    } else {
      // do nothing
    }
  }
  time_guard_.click(__FUNCTION__);
  return ret;
}

int ObIlogFileBuilder::build_array_hash_map_(
    PartitionMetaInfoMap& partition_meta_info, IndexInfoBlockMap& index_info_block_map)
{
  int ret = OB_SUCCESS;
  BuildArrayMapFunctor functor(index_info_block_map);
  if (OB_FAIL(partition_meta_info.for_each(functor))) {
    CSR_LOG(ERROR, "partition_meta_info_ for_each failed", K(ret));
  } else {
    // do nothing
  }
  time_guard_.click(__FUNCTION__);
  return ret;
}

int ObIlogFileBuilder::build_memberlist_map_(
    PartitionMemberListMap& partition_memberlist_map, MemberListMap& memberlist_map)
{
  int ret = OB_SUCCESS;
  BuildMemberListMapFunctor functor(memberlist_map);
  if (OB_FAIL(partition_memberlist_map.for_each(functor))) {
    CSR_LOG(ERROR, "partition_memberlist_map for_each failed", K(ret));
  } else {
    // do nothing
  }
  time_guard_.click(__FUNCTION__);
  return ret;
}

int ObIlogFileBuilder::build_cursor_array_(const RawArray& raw_array, offset_t& next_offset)
{
  int ret = OB_SUCCESS;
  if (!raw_array.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(raw_array));
  } else {
    const int64_t cursor_cnt = raw_array.count_;
    int64_t curr_pos = 0;
    ObLogCursorExt tmp_cursor;
    const int64_t buffer_limit = cursor_cnt * tmp_cursor.get_serialize_size();
    common::ObPartitionKey last_partition;
    uint64_t last_log_id = OB_INVALID_ID;
    uint64_t min_log_id = OB_INVALID_ID;
    uint64_t max_log_id = OB_INVALID_ID;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cursor_cnt; idx++) {
      const ObIndexEntry& index_entry = raw_array.arr_[idx];
      const common::ObPartitionKey& curr_partition = index_entry.get_partition_key();
      const uint64_t curr_log_id = index_entry.get_log_id();
      if (curr_partition != last_partition) {
        const offset_t start_offset = static_cast<offset_t>(idx * tmp_cursor.get_serialize_size());
        UpdateStartOffsetFunctor functor(start_offset);
        if (OB_FAIL(ilog_memstore_->partition_meta_info_.operate(curr_partition, functor))) {
          CSR_LOG(ERROR, "partition_meta_info_ operate failed", K(ret), K(curr_partition));
        } else {
          IndexInfoBlockEntry index_info_entry;
          if (OB_FAIL(ilog_memstore_->partition_meta_info_.get(curr_partition, index_info_entry))) {
            CSR_LOG(ERROR, "partition_log_info_ get failed", K(ret), K(curr_partition));
          } else {
            last_partition = curr_partition;
            last_log_id = OB_INVALID_ID;
            min_log_id = index_info_entry.min_log_id_;
            max_log_id = index_info_entry.max_log_id_;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (check_ilog_continous_(min_log_id, max_log_id, last_log_id, curr_log_id)) {
          ObLogCursorExt tmp_cursor;
          tmp_cursor.reset(index_entry.get_file_id(),
              index_entry.get_offset(),
              index_entry.get_size(),
              index_entry.get_accum_checksum(),
              index_entry.get_submit_timestamp(),
              index_entry.is_batch_committed());
          if (OB_FAIL(tmp_cursor.serialize(buffer_, buffer_limit, curr_pos))) {
            CSR_LOG(ERROR, "tmp_cursor serialize failed", K(ret));
          } else {
            last_log_id = curr_log_id;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR,
              "index entry is not continous!!!",
              K(ret),
              K(last_partition),
              K(last_log_id),
              K(curr_partition),
              K(curr_log_id),
              K(min_log_id),
              K(max_log_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      next_offset = static_cast<offset_t>(upper_align(curr_pos, CLOG_DIO_ALIGN_SIZE));
    }
  }
  time_guard_.click(__FUNCTION__);
  return ret;
}

int ObIlogFileBuilder::build_info_block_(
    const IndexInfoBlockMap& index_info_block_map, const offset_t info_block_start_offset, offset_t& next_offset)
{
  int ret = OB_SUCCESS;
  int64_t pos = info_block_start_offset;
  if (OB_FAIL(index_info_block_map.serialize(buffer_, total_size_, pos))) {
    CSR_LOG(ERROR, "index_info_block_map_ serialize failed", K(ret));
  } else {
    next_offset = static_cast<offset_t>(pos);
  }
  time_guard_.click(__FUNCTION__);
  return ret;
}

int ObIlogFileBuilder::build_memberlist_block_(
    const MemberListMap& memberlist_map, const offset_t memberlist_block_start_offset, offset_t& next_offset)
{
  int ret = OB_SUCCESS;
  int64_t pos = memberlist_block_start_offset;
  if (OB_FAIL(memberlist_map.serialize(buffer_, total_size_, pos))) {
    CSR_LOG(ERROR, "memberlist_map_ serialize failed", K(ret));
  } else {
    next_offset = static_cast<offset_t>(pos);
  }
  time_guard_.click(__FUNCTION__);
  return ret;
}

int ObIlogFileBuilder::calculate_checksum_(int64_t& file_content_checksum, const int64_t size) const
{
  int ret = OB_SUCCESS;
  file_content_checksum = ob_crc64(buffer_, size);
  return ret;
}

int ObIlogFileBuilder::build_file_trailer_(const offset_t trailer_start_offset, const offset_t info_block_start_offset,
    const int32_t info_block_size, const offset_t memberlist_block_start_offset, const int32_t memberlist_block_size,
    const int64_t file_content_checksum)
{
  int ret = OB_SUCCESS;
  ObIlogFileTrailerV2 trailer;
  int64_t pos = trailer_start_offset;
  if (OB_FAIL(trailer.init(info_block_start_offset,
          info_block_size,
          memberlist_block_start_offset,
          memberlist_block_size,
          file_content_checksum))) {
    CSR_LOG(ERROR, "trailer init failed", K(ret));
  } else if (OB_FAIL(trailer.serialize(buffer_, total_size_, pos))) {
    CSR_LOG(ERROR, "trailer serialize failed", K(ret));
  }
  time_guard_.click(__FUNCTION__);
  return ret;
}

bool ObIlogFileBuilder::check_ilog_continous_(
    const uint64_t min_log_id, const uint64_t max_log_id, const uint64_t last_log_id, const uint64_t curr_log_id)
{
  bool bool_ret = false;
  if (last_log_id == OB_INVALID_ID || curr_log_id == last_log_id + 1) {
    bool_ret = true;
  }
  // When index_info_block only exists one log, last_log_id is
  // OB_INVALID_ID, so add one more judgement
  if (last_log_id == OB_INVALID_ID && min_log_id == max_log_id && curr_log_id != min_log_id) {
    bool_ret = false;
  }
  return bool_ret;
}

}  // namespace clog
}  // namespace oceanbase
