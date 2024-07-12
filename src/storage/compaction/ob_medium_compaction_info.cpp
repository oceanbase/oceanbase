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
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

namespace oceanbase
{
using namespace storage;
using namespace blocksstable;

namespace compaction
{

/*
 * ObParallelMergeInfo
 * */

template<typename T>
void ObParallelMergeInfo::destroy(T *&array)
{
  if (nullptr != array && nullptr != allocator_) {
    for (int i = 0; i < list_size_; ++i) {
      array[i].destroy(*allocator_);
    }
    allocator_->free(array);
    array = nullptr;
  }
}

void ObParallelMergeInfo::destroy()
{
  if (list_size_ > 0) {
    destroy(parallel_store_rowkey_list_);
    destroy(parallel_datum_rowkey_list_);
    list_size_ = 0;
  }
  allocator_ = nullptr;
  parallel_info_ = 0;
}

// CAREFUL! parallel_info_ contains list_size, no need serialize array count of rowkey list
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
    if (PARALLEL_INFO_VERSION_V0 == compat_) {
      for (int i = 0; OB_SUCC(ret) && i < list_size_; ++i) {
        if (OB_FAIL(parallel_store_rowkey_list_[i].serialize(buf, buf_len, pos))) {
          LOG_WARN("failed to encode concurrent cnt", K(ret), K(i), K(list_size_), K(parallel_store_rowkey_list_[i]));
        }
      }
    } else if (PARALLEL_INFO_VERSION_V1 == compat_) {
      for (int i = 0; OB_SUCC(ret) && i < list_size_; ++i) {
        if (OB_FAIL(parallel_datum_rowkey_list_[i].serialize(buf, buf_len, pos))) {
          LOG_WARN("failed to encode concurrent cnt", K(ret), K(i), K(list_size_), K(parallel_datum_rowkey_list_[i]));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid compat version", KR(ret), K_(compat));
    }
  }
  return ret;
}

#define ALLOC_ROWKEY_ARRAY(array_ptr, T) \
  void *alloc_buf = nullptr; \
  if (OB_ISNULL(alloc_buf = allocator.alloc(sizeof(T) * list_size_))) { \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    LOG_WARN("failed to alloc rowkey array", K(ret), K(list_size_)); \
  } else { \
    array_ptr = new(alloc_buf) T[list_size_]; \
  }

// CAREFUL! parallel_info_ contains list_size, no need deserialize array count of rowkey list
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
      if (OB_FAIL(ret)) {
      } else if (PARALLEL_INFO_VERSION_V0 == compat_) {
        ALLOC_ROWKEY_ARRAY(parallel_store_rowkey_list_, ObStoreRowkey);
        for (int i = 0; OB_SUCC(ret) && i < list_size_; ++i) {
          // need to deserialize StoreRowkey
          if (OB_FAIL(parallel_store_rowkey_list_[i].deserialize(allocator, buf, data_len, pos))) {
            LOG_WARN("failed to decode concurrent cnt", K(ret), K(i), K(list_size_), K(data_len), K(pos));
          }
        } // end of for
      } else if (PARALLEL_INFO_VERSION_V1 == compat_) {
        ALLOC_ROWKEY_ARRAY(parallel_datum_rowkey_list_, ObDatumRowkey);
        ObStorageDatum tmp_storage_datum[OB_INNER_MAX_ROWKEY_COLUMN_NUMBER];
        ObDatumRowkey tmp_datum_rowkey;
        tmp_datum_rowkey.assign(tmp_storage_datum, OB_INNER_MAX_ROWKEY_COLUMN_NUMBER);
        for (int i = 0; OB_SUCC(ret) && i < list_size_; ++i) {
          if (OB_FAIL(tmp_datum_rowkey.deserialize(buf, data_len, pos))) {
            LOG_WARN("failed to decode concurrent cnt", K(ret), K(i), K(list_size_), K(data_len), K(pos));
          } else if (OB_FAIL(tmp_datum_rowkey.deep_copy(parallel_datum_rowkey_list_[i] /*dst*/, allocator))) {
            LOG_WARN("failed to deep copy datum rowkey", KR(ret), K(i), K(tmp_datum_rowkey));
          }
        } // end of for
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid compat version", KR(ret), K_(compat));
      }
      if (OB_FAIL(ret)) {
        destroy(); // free parallel_end_key_list_ in destroy
      }
    }
  }
  return ret;
}

// CAREFUL! parallel_info_ contains list_size, no need serialize array count of rowkey list
int64_t ObParallelMergeInfo::get_serialize_size() const
{
  int64_t len = 0;
  if (list_size_ > 0) {
    len += serialization::encoded_length_vi32(parallel_info_);
    if (PARALLEL_INFO_VERSION_V0 == compat_) {
      for (int i = 0; i < list_size_; ++i) {
        len += parallel_store_rowkey_list_[i].get_serialize_size();
      }
    } else if (PARALLEL_INFO_VERSION_V1 == compat_) {
      for (int i = 0; i < list_size_; ++i) {
        len += parallel_datum_rowkey_list_[i].get_serialize_size();
      }
    }
  }
  return len;
}

int ObParallelMergeInfo::generate_from_range_array(
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &paral_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != list_size_
      || nullptr != parallel_store_rowkey_list_
      || nullptr != parallel_datum_rowkey_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel merge info is not empty", K(ret), KPC(this));
  } else {
    int64_t sum_range_cnt = 0;
    for (int64_t i = 0; i < paral_range.count(); ++i) {
      sum_range_cnt += paral_range.at(i).count();
    }
    if (sum_range_cnt <= VALID_CONCURRENT_CNT || sum_range_cnt > UINT8_MAX) {
      // do nothing
    } else {
      list_size_ = sum_range_cnt - 1;
      allocator_ = &allocator;
      uint64_t compat_version = 0;
      if (OB_FAIL(MTL(ObTenantTabletScheduler*)->get_min_data_version(compat_version))) {
        LOG_WARN("failed to get min data version", KR(ret));
      } else if (compat_version < DATA_VERSION_4_2_0_0) { // sync store_rowkey_list
        ret = generate_store_rowkey_list(allocator, paral_range);
      } else { // sync datum_rowkey_list
        ret = generate_datum_rowkey_list(allocator, paral_range);
      }
    }
  }
  LOG_DEBUG("parallel range info", K(ret), KPC(this), K(paral_range), K(paral_range.count()), K(paral_range.at(0)));
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObParallelMergeInfo::generate_datum_rowkey_list(
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &paral_range)
{
  int ret = OB_SUCCESS;
  compat_ = PARALLEL_INFO_VERSION_V1;
  ALLOC_ROWKEY_ARRAY(parallel_datum_rowkey_list_, ObDatumRowkey);
  int64_t cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < paral_range.count() && cnt < list_size_; ++i) {
    const ObIArray<ObStoreRange> &range_array = paral_range.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < range_array.count() && cnt < list_size_; ++j) {
      if (OB_FAIL(parallel_datum_rowkey_list_[cnt++].from_rowkey(range_array.at(j).get_end_key().get_rowkey(), allocator))) {
        LOG_WARN("failed to deep copy end key", K(ret), K(j), "src_key", range_array.at(j).get_end_key());
      }
    }
  } // end of loop array
  return ret;
}

int ObParallelMergeInfo::generate_store_rowkey_list(
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &paral_range)
{
  int ret = OB_SUCCESS;
  compat_ = PARALLEL_INFO_VERSION_V0;
  ALLOC_ROWKEY_ARRAY(parallel_store_rowkey_list_, ObStoreRowkey);
  int64_t cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < paral_range.count() && cnt < list_size_; ++i) {
    const ObIArray<ObStoreRange> &range_array = paral_range.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < range_array.count() && cnt < list_size_; ++j) {
      if (OB_FAIL(range_array.at(j).get_end_key().deep_copy(parallel_store_rowkey_list_[cnt++]/*dst*/, allocator))) {
        LOG_WARN("failed to deep copy end key", K(ret), K(j), "src_key", range_array.at(j).get_end_key());
      }
    }
  } // end of loop array
  return ret;
}

template<typename T>
int ObParallelMergeInfo::deep_copy_list(common::ObIAllocator &allocator, const T *src, T *&dst)
{
  int ret = OB_SUCCESS;
  ALLOC_ROWKEY_ARRAY(dst, T);
  for (int i = 0; OB_SUCC(ret) && i < list_size_; ++i) {
    if (OB_FAIL(src[i].deep_copy(dst[i], allocator))) {
      LOG_WARN("failed to deep copy end key", K(ret), K(i), K(src[i]));
    }
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
    compat_ = other.compat_;
    list_size_ = other.list_size_;
    allocator_ = &allocator;
    if (list_size_ > 0) {
      if (PARALLEL_INFO_VERSION_V0 == compat_) {
        ret = deep_copy_list(allocator, other.parallel_store_rowkey_list_, parallel_store_rowkey_list_);
      } else if (PARALLEL_INFO_VERSION_V1 == compat_) {
        ret = deep_copy_list(allocator, other.parallel_datum_rowkey_list_, parallel_datum_rowkey_list_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid compat version", KR(ret), K_(compat));
      }
      if (OB_FAIL(ret)) {
        destroy();
      }
    }
  }
  return ret;
}

int ObParallelMergeInfo::deep_copy_datum_rowkey(
    const int64_t idx,
    ObIAllocator &input_allocator,
    blocksstable::ObDatumRowkey &rowkey) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || idx >= list_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", KR(ret), K(idx), K_(list_size));
  } else if (PARALLEL_INFO_VERSION_V0 == compat_) {
    ObDatumRowkeyHelper rowkey_helper;
    ObDatumRowkey tmp_datum_rowkey;
    if (OB_FAIL(rowkey_helper.convert_datum_rowkey(parallel_store_rowkey_list_[idx].get_rowkey()/*src*/, tmp_datum_rowkey/*dst*/))) {
      STORAGE_LOG(WARN, "failed to convert to datum rowkey", K(ret), K(idx), K(parallel_store_rowkey_list_[idx]));
    } else if (OB_FAIL(tmp_datum_rowkey.deep_copy(rowkey/*dst*/, input_allocator))) {
      STORAGE_LOG(WARN, "failed to deep copy datum rowkey", KR(ret), K(tmp_datum_rowkey));
    }
  } else if (PARALLEL_INFO_VERSION_V1 == compat_) {
    if (OB_FAIL(parallel_datum_rowkey_list_[idx].deep_copy(rowkey/*dst*/, input_allocator))) {
      STORAGE_LOG(WARN, "failed to deep copy end key", K(ret), K(idx), K(parallel_datum_rowkey_list_[idx]));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid compat version", KR(ret), K_(compat));
  }
  return ret;
}

int64_t ObParallelMergeInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(list_size), K_(compat));
    if (list_size_ > 0) {
      J_COMMA();
    }

    if (PARALLEL_INFO_VERSION_V0 == compat_) {
      for (int i = 0; i < list_size_; ++i) {
        if (i > 0) {
          J_COMMA();
        }
        J_KV(K(i), "key", parallel_store_rowkey_list_[i]);
      }
    } else if (PARALLEL_INFO_VERSION_V1 == compat_) {
      for (int i = 0; i < list_size_; ++i) {
        if (i > 0) {
          J_COMMA();
        }
        J_KV(K(i), "key", parallel_datum_rowkey_list_[i]);
      }
    }
    J_OBJ_END();
  }
  return pos;
}

OB_SERIALIZE_MEMBER_SIMPLE(
    ObMediumCompactionInfoKey,
    medium_snapshot_);

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
  if (is_valid_compaction_type(type)) {
    str = ObCompactionTypeStr[type];
  } else {
    str = "invalid_type";
  }
  return str;
}

ObMediumCompactionInfo::ObMediumCompactionInfo()
  : medium_compat_version_(MEDIUM_COMPAT_VERSION_V4),
    compaction_type_(COMPACTION_TYPE_MAX),
    contain_parallel_range_(false),
    medium_merge_reason_(ObAdaptiveMergePolicy::NONE),
    is_schema_changed_(false),
    tenant_id_(0),
    co_major_merge_type_(ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE),
    is_skip_tenant_major_(false),
    reserved_(0),
    cluster_id_(0),
    data_version_(0),
    medium_snapshot_(0),
    last_medium_snapshot_(0),
    storage_schema_(),
    parallel_merge_info_()
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_TYPE_MAX) == ARRAYSIZEOF(ObCompactionTypeStr), "compaction type str len is mismatch");
}

ObMediumCompactionInfo::~ObMediumCompactionInfo()
{
  reset();
}

int ObMediumCompactionInfo::assign(ObIAllocator &allocator,
                                   const ObMediumCompactionInfo &medium_info)
{
  return init(allocator, medium_info);
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
    last_medium_snapshot_ = medium_info.last_medium_snapshot_;
    data_version_ = medium_info.data_version_;
  }
  return ret;
}

int ObMediumCompactionInfo::init_data_version(const uint64_t compat_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(compat_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input compat version", K(ret), K(compat_version));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_1_0_0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data version to schedule medium compaction", K(ret), K(compat_version));
  } else {
    data_version_ = compat_version;
    if (compat_version < DATA_VERSION_4_2_0_0) {
      medium_compat_version_ = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION;
    } else if (compat_version < DATA_VERSION_4_2_1_0) {
      medium_compat_version_ = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2;
    } else if (compat_version < DATA_VERSION_4_2_1_2) {
      medium_compat_version_ = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V3;
    } else {
      medium_compat_version_ = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V4;
    }
  }
  return ret;
}

bool ObMediumCompactionInfo::is_valid() const
{
  return COMPACTION_TYPE_MAX != compaction_type_
      && medium_snapshot_ > 0
      && data_version_ > 0
      && storage_schema_.is_valid()
      && parallel_merge_info_.is_valid()
      && (MEDIUM_COMPAT_VERSION == medium_compat_version_
        || (MEDIUM_COMPAT_VERSION_V2 <= medium_compat_version_ && last_medium_snapshot_ != 0));
}

void ObMediumCompactionInfo::reset()
{
  info_ = 0;
  medium_compat_version_ = 0;
  compaction_type_ = COMPACTION_TYPE_MAX;
  contain_parallel_range_ = false;
  medium_merge_reason_ = ObAdaptiveMergePolicy::NONE;
  is_schema_changed_ = false;
  co_major_merge_type_ = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE;
  tenant_id_ = 0;
  cluster_id_ = 0;
  medium_snapshot_ = 0;
  last_medium_snapshot_ = 0;
  data_version_ = 0;
  storage_schema_.reset();
  parallel_merge_info_.destroy();
}

bool ObMediumCompactionInfo::should_throw_for_standby_cluster() const
{
  bool bret = false;
  if (medium_compat_version_ < MEDIUM_COMPAT_VERSION_V3) {
    // for old version medium, should throw if cluster_id is different
    bret = !cluster_id_equal() && is_medium_compaction();
  } else {
    // for new version medium, all medium should be kept
    bret = false;
  }
  return bret;
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
    }
  } else if (parallel_merge_info_.get_size() > 0) {
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
    if (OB_SUCC(ret) && contain_parallel_range_) {
      LST_DO_CODE(
          OB_UNIS_ENCODE,
          parallel_merge_info_);
    }
    if (OB_SUCC(ret) && MEDIUM_COMPAT_VERSION_V2 <= medium_compat_version_) {
      LST_DO_CODE(
        OB_UNIS_ENCODE,
        last_medium_snapshot_);
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
      LOG_WARN("failed to deserialize storage schema", K(ret), K(buf), K(data_len), K(pos));
    } else if (contain_parallel_range_) {
      if (OB_FAIL(parallel_merge_info_.deserialize(allocator, buf, data_len, pos))) {
        LOG_WARN("failed to deserialize parallel merge info", K(ret), K(buf), K(data_len), K(pos));
      }
    } else {
      clear_parallel_range();
      LOG_DEBUG("ObMediumCompactionInfo::deserialize", K(ret), K(buf), K(data_len), K(pos));
    }
    if (OB_SUCC(ret) && MEDIUM_COMPAT_VERSION_V2 <= medium_compat_version_) {
      LST_DO_CODE(
        OB_UNIS_DECODE,
        last_medium_snapshot_);
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
  if (MEDIUM_COMPAT_VERSION_V2 <= medium_compat_version_) {
    LST_DO_CODE(
      OB_UNIS_ADD_LEN,
      last_medium_snapshot_);
  }
  return len;
}

void ObMediumCompactionInfo::gene_info(
    char* buf, const int64_t buf_len, int64_t &pos) const
{
  J_KV("compaction_type", ObMediumCompactionInfo::get_compaction_type_str((ObCompactionType)compaction_type_),
      K(medium_snapshot_), K_(last_medium_snapshot), K_(parallel_merge_info));
}

int64_t ObMediumCompactionInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV("compaction_type", ObMediumCompactionInfo::get_compaction_type_str((ObCompactionType)compaction_type_),
      "medium_merge_reason", ObAdaptiveMergePolicy::merge_reason_to_str(medium_merge_reason_),
      K_(medium_snapshot), K_(last_medium_snapshot), K_(tenant_id), K_(cluster_id),
      K_(medium_compat_version), K_(data_version), K_(is_schema_changed), K_(storage_schema),
      "co_major_merge_type", ObCOMajorMergePolicy::co_major_merge_type_to_str(static_cast<ObCOMajorMergePolicy::ObCOMajorMergeType>(co_major_merge_type_)),
      K_(is_skip_tenant_major), K_(contain_parallel_range), K_(parallel_merge_info));
    J_OBJ_END();
  }
  return pos;
}

} //namespace compaction
} // namespace oceanbase
