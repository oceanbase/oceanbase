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

#define USING_LOG_PREFIX STORAGE
#include "storage/compaction/ob_medium_compaction_info.h"

namespace oceanbase
{
using namespace storage;

namespace compaction
{

/*
 * ObParallelMergeInfo
 * */

void ObParallelMergeInfo::destroy()
{
  if (list_size_ > 0 && nullptr != parallel_end_key_list_ && nullptr != allocator_) {
    for (int i = 0; i < list_size_; ++i) {
      parallel_end_key_list_[i].destroy(*allocator_);
    }
    list_size_ = 0;
    allocator_->free(parallel_end_key_list_);
    parallel_end_key_list_ = nullptr;
    allocator_ = nullptr;
  }
  parallel_info_ = 0;
}

int ObParallelMergeInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (0 == list_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to serialize parallel_merge_info", K(ret), K(list_size_));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        parallel_info_);
    for (int i = 0; OB_SUCC(ret) && i < list_size_; ++i) {
      if (OB_FAIL(parallel_end_key_list_[i].serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to encode concurrent cnt", K(ret), K(i), K(list_size_), K(parallel_end_key_list_[i]));
      }
    }
  }
  return ret;
}

int ObParallelMergeInfo::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, parallel_info_);
    if (OB_FAIL(ret)) {
    } else if (0 == list_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("list size is invalid", K(ret), K(list_size_));
    } else {
      allocator_ = &allocator;
      void *alloc_buf = nullptr;
      if (OB_ISNULL(alloc_buf = allocator.alloc(sizeof(ObStoreRowkey) * list_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc store rowkey array", K(ret), K(list_size_));
      } else {
        parallel_end_key_list_ = new(alloc_buf) ObStoreRowkey[list_size_];
      }
      for (int i = 0; OB_SUCC(ret) && i < list_size_; ++i) {
        if (OB_FAIL(parallel_end_key_list_[i].deserialize(allocator, buf, data_len, pos))) {
          LOG_WARN("failed to encode concurrent cnt", K(ret), K(i), K(list_size_), K(data_len), K(pos));
        }
      }
      if (OB_FAIL(ret)) {
        destroy(); // free parallel_end_key_list_ in destroy
      }
    }
  }
  return ret;
}

int64_t ObParallelMergeInfo::get_serialize_size() const
{
  int64_t len = 0;
  if (list_size_ > 0) {
    len += serialization::encoded_length_vi32(parallel_info_);
    for (int i = 0; i < list_size_; ++i) {
      len += parallel_end_key_list_[i].get_serialize_size();
    }
  }
  return len;
}

int ObParallelMergeInfo::generate_from_range_array(
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &paral_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(0 != list_size_ || nullptr != parallel_end_key_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel merge info is not empty", K(ret), KPC(this));
  } else {
    int64_t sum_range_cnt = 0;
    for (int64_t i = 0; i < paral_range.count(); ++i) {
      sum_range_cnt += paral_range.at(i).count();
    }
    if (sum_range_cnt <= VALID_CONCURRENT_CNT || sum_range_cnt > UINT8_MAX) {
      // do nothing
    } else if (FALSE_IT(list_size_ = sum_range_cnt - 1)) {
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStoreRowkey) * list_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret), K(paral_range));
    } else {
      allocator_ = &allocator;
      parallel_end_key_list_ = new(buf) ObStoreRowkey[list_size_];
      int64_t cnt = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < paral_range.count() && cnt < list_size_; ++i) {
        const ObIArray<ObStoreRange> &range_array = paral_range.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < range_array.count() && cnt < list_size_; ++j) {
          if (OB_FAIL(range_array.at(j).get_end_key().deep_copy(parallel_end_key_list_[cnt++], allocator))) {
            LOG_WARN("failed to deep copy end key", K(ret), K(i), K(range_array), K(j), K(cnt));
          }
        }
      } // end of loop array
    }
  }
  LOG_DEBUG("parallel range info", K(ret), KPC(this), K(paral_range), K(paral_range.count()), K(paral_range.at(0)));

  if (OB_FAIL(ret)) {
    destroy();
  } else if (get_serialize_size() > MAX_PARALLEL_RANGE_SERIALIZE_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_DEBUG("parallel range info is too large to sync", K(ret), KPC(this));
    destroy();
  }
  return ret;
}

int ObParallelMergeInfo::init(
    common::ObIAllocator &allocator,
    const ObParallelMergeInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other parallel info is invalid", K(ret), K(other));
  } else {
    list_size_ = other.list_size_;
    allocator_ = &allocator;
    if (list_size_ > 0) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStoreRowkey) * list_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate", K(ret), K(other));
      } else {
        parallel_end_key_list_ = new (buf) ObStoreRowkey[list_size_];
        for (int i = 0; OB_SUCC(ret) && i < list_size_; ++i) {
          if (OB_FAIL(other.parallel_end_key_list_[i].deep_copy(parallel_end_key_list_[i], allocator))) {
            LOG_WARN("failed to deep copy end key", K(ret), K(i), K(other.parallel_end_key_list_[i]));
          }
        }
        if (OB_FAIL(ret)) {
          destroy();
        }
      } // else
    }
  }
  return ret;
}

int64_t ObParallelMergeInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(list_size));
    J_COMMA();
    for (int i = 0; i < list_size_; ++i) {
      J_KV(K(i), "key", parallel_end_key_list_[i]);
      J_COMMA();
    }
    J_OBJ_END();
  }
  return pos;
}

/*
 * ObMediumCompactionInfo
 * */
const char *ObMediumCompactionInfo::ObCompactionTypeStr[] = {
    "MEDIUM_COMPACTION",
    "MAJOR_COMPACTION",
};

const char *ObMediumCompactionInfo::get_compaction_type_str(enum ObCompactionType type)
{
  const char *str = "";
  if (type >= COMPACTION_TYPE_MAX || type < MEDIUM_COMPACTION) {
    str = "invalid_type";
  } else {
    str = ObCompactionTypeStr[type];
  }
  return str;
}

ObMediumCompactionInfo::ObMediumCompactionInfo()
  : ObIMultiSourceDataUnit(),
    medium_compat_version_(MEIDUM_COMPAT_VERSION),
    compaction_type_(COMPACTION_TYPE_MAX),
    contain_parallel_range_(false),
    medium_merge_reason_(ObAdaptiveMergePolicy::NONE),
    is_schema_changed_(false),
    reserved_(0),
    cluster_id_(0),
    data_version_(0),
    medium_snapshot_(0),
    storage_schema_(),
    parallel_merge_info_()
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_TYPE_MAX) == ARRAYSIZEOF(ObCompactionTypeStr), "compaction type str len is mismatch");
}

ObMediumCompactionInfo::~ObMediumCompactionInfo()
{
  reset();
}

int ObMediumCompactionInfo::init(
    ObIAllocator &allocator,
    const ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!medium_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(medium_info));
  } else if (OB_FAIL(storage_schema_.init(allocator, medium_info.storage_schema_))) {
    LOG_WARN("failed to init storage schema", K(ret), K(medium_info));
  } else if (OB_FAIL(parallel_merge_info_.init(allocator, medium_info.parallel_merge_info_))) {
    LOG_WARN("failed to init parallel merge info", K(ret), K(medium_info));
  } else {
    info_ = medium_info.info_;
    cluster_id_ = medium_info.cluster_id_;
    medium_snapshot_ = medium_info.medium_snapshot_;
    data_version_ = medium_info.data_version_;
  }
  return ret;
}

bool ObMediumCompactionInfo::is_valid() const
{
  return COMPACTION_TYPE_MAX != compaction_type_
      && medium_snapshot_ > 0
      && data_version_ > 0
      && storage_schema_.is_valid()
      && parallel_merge_info_.is_valid();
}

void ObMediumCompactionInfo::reset()
{
  info_ = 0;
  medium_compat_version_ = 0;
  compaction_type_ = COMPACTION_TYPE_MAX;
  contain_parallel_range_ = false;
  medium_merge_reason_ = ObAdaptiveMergePolicy::NONE;
  is_schema_changed_ = false;
  cluster_id_ = 0;
  medium_snapshot_ = 0;
  data_version_ = 0;
  storage_schema_.reset();
  parallel_merge_info_.destroy();
}

int ObMediumCompactionInfo::deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == src || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src), KP(allocator));
  } else if (OB_UNLIKELY(memtable::MultiSourceDataUnitType::MEDIUM_COMPACTION_INFO != src->type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "type", src->type(), KP(allocator));
  } else {
    ret = init(*allocator, *static_cast<const ObMediumCompactionInfo *>(src));
  }
  return ret;
}

int ObMediumCompactionInfo::save_storage_schema(
    ObIAllocator &allocator,
    const storage::ObStorageSchema &storage_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!storage_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(storage_schema));
  } else if (OB_FAIL(storage_schema_.init(allocator, storage_schema))) {
    LOG_WARN("failed to init storage schema", K(ret), K(storage_schema));
  }
  return ret;
}

int ObMediumCompactionInfo::gene_parallel_info(
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &paral_range)
{
  int ret = OB_SUCCESS;
  contain_parallel_range_ = false;
  if (OB_FAIL(parallel_merge_info_.generate_from_range_array(allocator, paral_range))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WARN("failed to generate parallel merge info", K(ret), K(paral_range));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (parallel_merge_info_.list_size_ > 0) {
    contain_parallel_range_ = true;
    LOG_INFO("success to gene parallel info", K(ret), K(contain_parallel_range_), K(parallel_merge_info_));
  }
  return ret;
}

int ObMediumCompactionInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else {
    LST_DO_CODE(
        OB_UNIS_ENCODE,
        info_,
        cluster_id_,
        medium_snapshot_,
        data_version_,
        storage_schema_);
    if (contain_parallel_range_) {
      LST_DO_CODE(
          OB_UNIS_ENCODE,
          parallel_merge_info_);
    }
    LOG_DEBUG("ObMediumCompactionInfo::serialize", K(ret), K(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObMediumCompactionInfo::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
        info_,
        cluster_id_,
        medium_snapshot_,
        data_version_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(storage_schema_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize storage schema", K(ret));
    } else if (contain_parallel_range_) {
      if (OB_FAIL(parallel_merge_info_.deserialize(allocator, buf, data_len, pos))) {
        LOG_WARN("failed to deserialize parallel merge info", K(ret), K(buf), K(data_len), K(pos));
      }
    } else {
      clear_parallel_range();
      LOG_DEBUG("ObMediumCompactionInfo::deserialize", K(ret), K(buf), K(data_len), K(pos));
    }
  }
  return ret;
}

int64_t ObMediumCompactionInfo::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(
      OB_UNIS_ADD_LEN,
      info_,
      cluster_id_,
      medium_snapshot_,
      data_version_,
      storage_schema_);
  if (contain_parallel_range_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, parallel_merge_info_);
  }
  return len;
}

void ObMediumCompactionInfo::gene_info(
    char* buf, const int64_t buf_len, int64_t &pos) const
{
  J_KV("compaction_type", ObMediumCompactionInfo::get_compaction_type_str((ObCompactionType)compaction_type_),
      K(medium_snapshot_), K_(parallel_merge_info));
}

} //namespace compaction
} // namespace oceanbase
