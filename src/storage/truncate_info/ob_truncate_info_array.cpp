//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "share/compaction/ob_compaction_info_param.h"
namespace oceanbase
{
using namespace share;
namespace storage
{
/*
 * ObTruncateInfoArray
 * */

ObTruncateInfoArray::ObTruncateInfoArray()
  : truncate_info_array_(),
    allocator_(nullptr),
    src_(TRUN_SRC_MAX),
    is_inited_(false)
{
}

ObTruncateInfoArray::~ObTruncateInfoArray()
{
  reset();
}

int ObTruncateInfoArray::init_for_first_creation(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    allocator_ = &allocator;
    src_ = TRUN_SRC_MDS;
    is_inited_ = true;
  }
  return ret;
}

int ObTruncateInfoArray::init_with_kv_cache_array(
  ObIAllocator &allocator,
  const ObArrayWrap<ObTruncateInfo> &input_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(input_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to init truncate info array", KR(ret), K(input_array));
  } else {
    allocator_ = &allocator;
    src_ = TRUN_SRC_KV_CACHE;
    is_inited_ = true;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < input_array.count(); ++idx) {
      if (OB_FAIL(append_with_deep_copy(input_array.at(idx)))) {
        LOG_WARN("failed to append", KR(ret), K(idx), K(input_array.at(idx)));
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int ObTruncateInfoArray::append_with_deep_copy(const ObTruncateInfo &truncate_info)
{
  int ret = OB_SUCCESS;
  ObTruncateInfo *info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(*allocator_, info))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(info->assign(*allocator_, truncate_info))) {
    LOG_WARN("failed to copy truncate info", K(ret), K(truncate_info));
  } else {
    ret = inner_append_and_sort(*info);
  }

  if (OB_FAIL(ret)) {
    ObTabletObjLoadHelper::free(*allocator_, info);
  }

  return ret;
}

int ObTruncateInfoArray::append_ptr(ObTruncateInfo &truncate_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ret = inner_append_and_sort(truncate_info);
  }
  return ret;
}

int ObTruncateInfoArray::inner_append_and_sort(ObTruncateInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(truncate_info_array_.push_back(&info))) {
    LOG_WARN("failed to push back to array", K(ret));
  } else {
    lib::ob_sort(truncate_info_array_.begin(), truncate_info_array_.end(), compare);
  }
  return ret;
}

int ObTruncateInfoArray::append(
    const mds::MdsDumpKey &key,
    const mds::MdsDumpNode &node)
{
  int ret = OB_SUCCESS;
  void *buffer = nullptr;
  ObTruncateInfo *truncate_info = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    const ObString &user_data = node.user_data_;
    int64_t pos = 0;
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(*allocator_, truncate_info))) {
      LOG_WARN("failed to alloc and new", K(ret));
    } else if (OB_FAIL(truncate_info->deserialize(*allocator_, user_data.ptr(), user_data.length(), pos))) {
      LOG_WARN("failed to deserialize truncate info", K(ret));
    } else if (OB_FAIL(truncate_info_array_.push_back(truncate_info))) {
      LOG_WARN("failed to push back to array", K(ret));
    } else {
      lib::ob_sort(truncate_info_array_.begin(), truncate_info_array_.end(), compare);
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != truncate_info) {
      truncate_info->~ObTruncateInfo();
      allocator_->free(truncate_info);
    }
  }

  return ret;
}

bool ObTruncateInfoArray::compare(
    const ObTruncateInfo *lhs,
    const ObTruncateInfo *rhs)
{
  bool bret = true;
  if (lhs->commit_version_ == rhs->commit_version_) {
    bret = lhs->key_ < rhs->key_;
  } else {
    bret = lhs->commit_version_ < rhs->commit_version_;
  }
  return bret;
}

void ObTruncateInfoArray::reset_list()
{
  if (OB_NOT_NULL(allocator_)) {
    for (int64_t idx = 0; idx < count(); ++idx) {
      ObTabletObjLoadHelper::free(*allocator_, truncate_info_array_.at(idx));
    }
    truncate_info_array_.reset();
    allocator_ = nullptr;
  }
}

void ObTruncateInfoArray::reset()
{
  reset_list();
  is_inited_ = false;
}

int ObTruncateInfoArray::assign(ObIAllocator &allocator, const ObTruncateInfoArray &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();
    allocator_ = &allocator;
    for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); ++i) {
      const ObTruncateInfo *src_info = other.truncate_info_array_.at(i);
      if (OB_ISNULL(src_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, truncate info is null", K(ret), K(i), KP(src_info));
      } else if (OB_FAIL(append_with_deep_copy(*src_info))) {
        LOG_WARN("failed to append truncate info", K(ret), K(i), KPC(src_info));
      }
    } // for

    if (OB_FAIL(ret)) {
      reset_list();
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTruncateInfoArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  int64_t new_pos = pos;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("truncate info array is invalid", K(ret), KPC(this));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, new_pos, count()))) {
    LOG_WARN("failed to serialize truncate info array count", K(ret), K(buf_len));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count(); ++idx) {
      ObTruncateInfo *info = truncate_info_array_.at(idx);
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null info", KR(ret), K(idx), K_(truncate_info_array));
      } else if (OB_FAIL(info->serialize(buf, buf_len, new_pos))) {
        LOG_WARN("failed to serialize truncate info", K(ret), K(buf), K(buf_len), K(new_pos), KPC(info));
      } else {
        LOG_DEBUG("success to serialize truncate info", K(ret), KPC(info));
      }
    } // for
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

int ObTruncateInfoArray::deserialize(
    ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t array_cnt = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, new_pos, &array_cnt))) {
    LOG_WARN("failed to deserialize array count", K(ret), K(data_len));
  } else if (OB_UNLIKELY(array_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K(array_cnt));
  } else if (array_cnt > 0) {
    allocator_ = &allocator;
    void *alloc_buf = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < array_cnt; ++i) {
      ObTruncateInfo *new_info = nullptr;
      if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, new_info))) {
        LOG_WARN("failed to alloc and new", K(ret));
      } else if (OB_FAIL(new_info->deserialize(allocator, buf, data_len, new_pos))) {
        LOG_WARN("failed to deserialize truncate info", K(ret));
      } else if (OB_FAIL(truncate_info_array_.push_back(new_info))) {
        LOG_WARN("failed to add into truncate info array", K(ret), KPC(new_info));
      } else {
        LOG_DEBUG("success to deserialize truncate info", K(ret), K(new_info));
      }

      if (OB_FAIL(ret)) {
        ObTabletObjLoadHelper::free(allocator, new_info);
      }
    } // end of for
  }
  if (OB_FAIL(ret)) {
    reset_list();
  } else if (OB_UNLIKELY(!inner_is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("truncate info array is invalid", K(ret), KPC(this));
  } else {
    lib::ob_sort(truncate_info_array_.begin(), truncate_info_array_.end(), compare);
    is_inited_ = true;
    pos = new_pos;
  }
  return ret;
}

int64_t ObTruncateInfoArray::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(count());
  for (int64_t idx = 0; idx < count(); ++idx) {
    ObTruncateInfo *info = truncate_info_array_.at(idx);
    if (OB_NOT_NULL(info)) {
      len += static_cast<const ObTruncateInfo *>(info)->get_serialize_size();
    }
  }
  return len;
}

void ObTruncateInfoArray::sort_array()
{
  lib::ob_sort(truncate_info_array_.begin(), truncate_info_array_.end(), compare);
}

void ObTruncateInfoArray::gene_info(
    char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV("count", count());
    J_COMMA();
    BUF_PRINTF("info_array");
    J_COLON();
    J_OBJ_START();
    if (count() > 0) {
      for (int64_t idx = 0; idx < count(); ++idx) {
        BUF_PRINTF("[%ld]:", idx);
        if (OB_NOT_NULL(truncate_info_array_.at(idx))) {
          static_cast<const ObTruncateInfo *>(truncate_info_array_.at(idx))->gene_info(buf, buf_len, pos);
          if (idx != count()) {
            BUF_PRINTF(";");
          }
        }
      }
    } else {
      J_EMPTY_OBJ();
    }
    J_OBJ_END();
    J_OBJ_END();
  }
}

} // namespace storage
} // namespace oceanbase
