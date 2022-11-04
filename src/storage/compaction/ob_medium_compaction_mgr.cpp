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
#include "storage/compaction/ob_medium_compaction_mgr.h"

namespace oceanbase
{
namespace compaction
{
/*
 * ObMediumCompactionInfo
 * */
const char *ObMediumCompactionInfo::ObCompactionTypeStr[] = {
    "MAJOR_COMPACTION",
    "MEDIUM_COMPACTION",
};

const char *ObMediumCompactionInfo::get_compaction_type_str(enum ObCompactionType type)
{
  const char *str = "";
  if (type >= COMPACTION_TYPE_MAX || type < MAJOR_COMPACTION) {
    str = "invalid_type";
  } else {
    str = ObCompactionTypeStr[type];
  }
  return str;
}

ObMediumCompactionInfo::ObMediumCompactionInfo()
  : medium_compat_version_(MEIDUM_COMPAT_VERSION),
    compaction_type_(COMPACTION_TYPE_MAX),
    is_schema_changed_(false),
    reserved_(0),
    cluster_id_(0),
    medium_snapshot_(0),
    medium_log_ts_(0),
    storage_schema_()
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
  } else {
    info_ = medium_info.info_;
    cluster_id_ = medium_info.cluster_id_;
    medium_snapshot_ = medium_info.medium_snapshot_;
    medium_log_ts_ = medium_info.medium_log_ts_;
    is_schema_changed_ = medium_info.is_schema_changed_;
  }
  return ret;
}

bool ObMediumCompactionInfo::is_valid() const
{
  return COMPACTION_TYPE_MAX != compaction_type_
      && medium_snapshot_ > 0
      && medium_log_ts_ > 0
      && storage_schema_.is_valid();
}

void ObMediumCompactionInfo::reset()
{
  info_ = 0;
  medium_compat_version_ = 0;
  compaction_type_ = COMPACTION_TYPE_MAX;
  cluster_id_ = 0;
  medium_snapshot_ = 0;
  medium_log_ts_ = 0;
  is_schema_changed_ = false;
  storage_schema_.reset();
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
        medium_log_ts_,
        storage_schema_);
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
          medium_log_ts_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(storage_schema_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize storage schema", K(ret));
    } else {
      LOG_DEBUG("ObMediumCompactionInfo::deserialize", K(ret), K(buf), K(data_len), K(pos));
    }
  }
  return ret;
}

int64_t ObMediumCompactionInfo::get_serialize_size() const
{
  int64_t len = 0;
    LST_DO_CODE(OB_UNIS_ADD_LEN,
        info_,
        cluster_id_,
        medium_snapshot_,
        medium_log_ts_,
        storage_schema_);
  return len;
}

void ObMediumCompactionInfo::gene_info(
    char* buf, const int64_t buf_len, int64_t &pos) const
{
  J_KV("compaction_type", ObMediumCompactionInfo::get_compaction_type_str((ObCompactionType)compaction_type_), K(medium_snapshot_));
}

/*
 * ObMediumCompactionInfoList
 * */

const int64_t ObMediumCompactionInfoList::MAX_SERIALIZE_SIZE;

ObMediumCompactionInfoList::ObMediumCompactionInfoList(ObMediumListType medium_list_type)
  : is_inited_(false),
    medium_list_type_(medium_list_type),
    cur_medium_snapshot_(0),
    allocator_(nullptr)
{
}

ObMediumCompactionInfoList::~ObMediumCompactionInfoList()
{
  reset();
}

int ObMediumCompactionInfoList::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    allocator_ = &allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObMediumCompactionInfoList::init(common::ObIAllocator &allocator,
    const ObMediumCompactionInfoList *input_list)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (FALSE_IT(allocator_ = &allocator)) {
  } else if (nullptr == input_list) {
    // do nothing
  } else if (OB_FAIL(append_list_with_deep_copy(*input_list))) {
    LOG_WARN("failed to deep copy list", K(ret), K(input_list));
  } else {
    cur_medium_snapshot_ = input_list->get_cur_medium_snapshot();
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

void ObMediumCompactionInfoList::reset()
{
  if (OB_NOT_NULL(allocator_)) {
    DLIST_REMOVE_ALL_NORET(info, medium_info_list_) {
      info->~ObMediumCompactionInfo();
      allocator_->free(info);
    }
    medium_info_list_.reset();
  }
  is_inited_ = false;
  allocator_ = nullptr;
}

int ObMediumCompactionInfoList::add_medium_compaction_info(const ObMediumCompactionInfo &input_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("list is not init", K(ret));
  } else if (OB_FAIL(inner_deep_copy_node(input_info))) {
    LOG_WARN("failed to init medium info", K(ret), K(input_info));
  }
  return ret;
}

int ObMediumCompactionInfoList::get_specified_snapshot_info(
    const int64_t snapshot,
    const ObMediumCompactionInfo *&ret_info) const
{
  ret_info = nullptr;
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("list is not init", K(ret));
  } else if (OB_UNLIKELY(snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot));
  } else {
    DLIST_FOREACH_X(info, medium_info_list_, OB_SUCC(ret)) {
      if (OB_UNLIKELY(!info->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid medium info", K(ret), KPC(info));
      } else if (snapshot == info->medium_snapshot_) {
        ret_info = info;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && nullptr == ret_info) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObMediumCompactionInfoList::save_medium_compaction_info(const ObMediumCompactionInfoList &input_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("list is not init", K(ret));
  } else if (OB_FAIL(append_list_with_deep_copy(input_list))) {
    LOG_WARN("failed to deep copy list", K(ret), K(input_list));
  }
  return ret;
}

int ObMediumCompactionInfoList::inner_deep_copy_node(
    const ObMediumCompactionInfo &input_info)
{
  int ret = OB_SUCCESS;
  ObMediumCompactionInfo *new_info = nullptr;
  void *alloc_buf = nullptr;

  if (OB_UNLIKELY(medium_info_list_.get_size() > 0
      && medium_info_list_.get_last()->medium_snapshot_ >= input_info.medium_snapshot_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input medium info is invalid for list", K(ret), K(medium_info_list_), KPC(new_info));
  } else if (OB_ISNULL(alloc_buf = allocator_->alloc(sizeof(ObMediumCompactionInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (FALSE_IT(new_info = new (alloc_buf) ObMediumCompactionInfo())) {
  } else if (OB_FAIL(new_info->init(*allocator_, input_info))) {
    LOG_WARN("failed to init medium info", K(ret), K(input_info));
  } else if (OB_UNLIKELY(!medium_info_list_.add_last(new_info))) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to add into medium info list", K(ret), KPC(new_info));
  } else {
    LOG_DEBUG("success to deep copy apeend medium info", K(ret), KPC(new_info));
  }

  if (OB_FAIL(ret) && nullptr != new_info) {
    new_info->~ObMediumCompactionInfo();
    allocator_->free(new_info);
    new_info = nullptr;
  }
  return ret;
}

int ObMediumCompactionInfoList::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, new_pos, cur_medium_snapshot_))) {
    STORAGE_LOG(WARN, "failed to serialize cur medium snapshot", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, new_pos, medium_info_list_.get_size()))) {
    LOG_WARN("failed to serialize medium status", K(ret), K(buf_len));
  } else {
    DLIST_FOREACH_X(info, medium_info_list_, OB_SUCC(ret)) {
      if (OB_FAIL(info->serialize(buf, buf_len, new_pos))) {
        LOG_WARN("failed to serialize medium compaction info", K(ret), K(buf), K(buf_len), K(new_pos), KPC(info));
      } else {
        LOG_DEBUG("success to serialize medium info", K(ret), KPC(info));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

int ObMediumCompactionInfoList::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t list_count = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, new_pos, &cur_medium_snapshot_))) {
    LOG_WARN("failed to deserialize cur medium snapshot", K(ret), K(data_len));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, new_pos, &list_count))) {
    LOG_WARN("failed to serialize medium status", K(ret), K(data_len));
  } else if (OB_UNLIKELY(list_count < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected list count", K(ret), K(list_count));
  } else if (list_count > 0) {
    void *alloc_buf = nullptr;
    ObMediumCompactionInfo *new_info = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < list_count; ++i) {
      if (OB_ISNULL(alloc_buf = allocator.alloc(sizeof(ObMediumCompactionInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else if (FALSE_IT(new_info = new (alloc_buf) ObMediumCompactionInfo())) {
      } else if (OB_FAIL(new_info->deserialize(allocator, buf, data_len, new_pos))) {
        LOG_WARN("failed to deserialize medium info", K(ret));
      } else if (!medium_info_list_.add_last(new_info)) {
        ret = OB_ERR_SYS;
        LOG_WARN("failed to add into medium info list", K(ret), KPC(new_info));
      } else {
        LOG_DEBUG("success to deserialize medium info", K(ret), K(new_info));
      }

      if (OB_FAIL(ret) && nullptr != new_info) {
        new_info->~ObMediumCompactionInfo();
        allocator.free(new_info);
        new_info = nullptr;
      }
    } // end of for
  }
  if (OB_SUCC(ret)) {
    allocator_ = &allocator;
    is_inited_ = true;
    pos = new_pos;
  }
  return ret;
}

int64_t ObMediumCompactionInfoList::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(cur_medium_snapshot_);
  len += serialization::encoded_length_vi64(medium_info_list_.get_size());
  DLIST_FOREACH_NORET(info, medium_info_list_){
    len += info->get_serialize_size();
  }
  return len;
}

void ObMediumCompactionInfoList::gene_info(
    char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV("size", size());
    J_COMMA();
    BUF_PRINTF("info_list");
    J_COLON();
    J_OBJ_START();
    if (size() > 0) {
      int i = 0;
      DLIST_FOREACH_NORET(info, medium_info_list_){
        BUF_PRINTF("[%d]:", i++);
        info->gene_info(buf, buf_len, pos);
        if (i != size()) {
          BUF_PRINTF(";");
        }
      }
    } else {
      J_EMPTY_OBJ();
    }
    J_OBJ_END();
    J_OBJ_END();
  }
}

} //namespace compaction
} // namespace oceanbase
