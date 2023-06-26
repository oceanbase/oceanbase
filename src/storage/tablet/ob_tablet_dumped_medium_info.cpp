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

#include "storage/tablet/ob_tablet_dumped_medium_info.h"
#include <algorithm>
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
ObTabletDumpedMediumInfo::ObTabletDumpedMediumInfo()
  : is_inited_(false),
    allocator_(nullptr),
    medium_info_list_()
{
}

ObTabletDumpedMediumInfo::~ObTabletDumpedMediumInfo()
{
  reset();
}

void ObTabletDumpedMediumInfo::reset()
{
  if (OB_NOT_NULL(allocator_)) {
    for (int64_t i = 0; i < medium_info_list_.count(); ++i) {
      compaction::ObMediumCompactionInfo *medium_info = medium_info_list_[i];
      if (OB_ISNULL(medium_info)) {
        LOG_ERROR_RET(OB_ERR_SYS, "medium info is null", KP(medium_info), K(i));
      } else {
        medium_info->compaction::ObMediumCompactionInfo::~ObMediumCompactionInfo();
        allocator_->free(medium_info);
      }
    }
  }

  medium_info_list_.reset();
  is_inited_ = false;
}

int ObTabletDumpedMediumInfo::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    allocator_ = &allocator;
    is_inited_ = true;
  }

  return ret;
}

int ObTabletDumpedMediumInfo::assign(const ObTabletDumpedMediumInfo &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();

    for (int64_t i = 0; OB_SUCC(ret) && i < other.medium_info_list_.count(); ++i) {
      compaction::ObMediumCompactionInfo *medium_info = nullptr;
      const compaction::ObMediumCompactionInfo *src_medium_info = other.medium_info_list_.at(i);
      if (OB_ISNULL(src_medium_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src medium info is null", K(ret), KP(src_medium_info), K(i));
      } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, medium_info))) {
        LOG_WARN("failed to alloc and new", K(ret));
      } else if (OB_FAIL(medium_info->assign(allocator, *src_medium_info))) {
        LOG_WARN("failed to copy mds dump kv", K(ret));
      } else if (OB_FAIL(medium_info_list_.push_back(medium_info))) {
        LOG_WARN("failed to push back to array", K(ret));
      }

      if (OB_FAIL(ret)) {
        if (nullptr != medium_info) {
          medium_info->reset();
          allocator.free(medium_info);
        }
      }
    }

    if (OB_FAIL(ret)) {
      reset();
    } else {
      allocator_ = &allocator;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTabletDumpedMediumInfo::append(
    const mds::MdsDumpKey &key,
    const mds::MdsDumpNode &node)
{
  int ret = OB_SUCCESS;
  void *buffer = nullptr;
  compaction::ObMediumCompactionInfo *medium_info = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    const common::ObString &user_data = node.user_data_;
    int64_t pos = 0;
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(*allocator_, medium_info))) {
      LOG_WARN("failed to alloc and new", K(ret));
    } else if (OB_FAIL(medium_info->deserialize(*allocator_, user_data.ptr(), user_data.length(), pos))) {
      LOG_WARN("failed to deserialize medium info", K(ret));
    } else if (OB_FAIL(medium_info_list_.push_back(medium_info))) {
      LOG_WARN("failed to push back to array", K(ret));
    } else {
      std::sort(medium_info_list_.begin(), medium_info_list_.end(), compare);
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != medium_info) {
      medium_info->compaction::ObMediumCompactionInfo::~ObMediumCompactionInfo();
      allocator_->free(medium_info);
    }
  }

  return ret;
}

int ObTabletDumpedMediumInfo::append(const compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  compaction::ObMediumCompactionInfo *info = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(*allocator_, info))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(info->assign(*allocator_, medium_info))) {
    LOG_WARN("failed to copy medium info", K(ret), K(medium_info));
  } else if (OB_FAIL(medium_info_list_.push_back(info))) {
    LOG_WARN("failed to push back to array", K(ret), KPC(info));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != info) {
      info->compaction::ObMediumCompactionInfo::~ObMediumCompactionInfo();
      allocator_->free(info);
    }
  }

  return ret;
}

bool ObTabletDumpedMediumInfo::is_valid() const
{
  bool valid = true;
  int ret = OB_SUCCESS;

  if (medium_info_list_.empty()) {
    valid = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && valid && i < medium_info_list_.count(); ++i) {
      const compaction::ObMediumCompactionInfo *medium_info = medium_info_list_.at(i);
      if (OB_ISNULL(medium_info)) {
        ret = OB_ERR_UNEXPECTED;
        valid = false;
        LOG_ERROR("mds dump kv is null", K(ret), KP(medium_info), K(i));
      } else if (!medium_info->is_valid()) {
        valid = false;
      }
    }
  }

  return valid;
}

int ObTabletDumpedMediumInfo::get_min_medium_info_key(compaction::ObMediumCompactionInfoKey &key) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (medium_info_list_.empty()) {
    ret = OB_EMPTY_RESULT;
    LOG_INFO("no medium info exists", K(ret));
  } else {
    ObTabletDumpedMediumInfoIterator iter;
    ObArenaAllocator arena_allocator("iter");
    if (OB_FAIL(iter.init(arena_allocator, *this))) {
      LOG_WARN("failed to init", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.get_next_key(key))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next medium info", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTabletDumpedMediumInfo::get_max_medium_info_key(compaction::ObMediumCompactionInfoKey &key) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (medium_info_list_.empty()) {
    ret = OB_EMPTY_RESULT;
    LOG_INFO("no medium info exists", K(ret));
  } else {
    ObTabletDumpedMediumInfoIterator iter;
    ObArenaAllocator arena_allocator("iter");
    if (OB_FAIL(iter.init(arena_allocator, *this))) {
      LOG_WARN("failed to init", K(ret));
    } else if (OB_FAIL(iter.get_next_key(key))) {
      LOG_WARN("failed to get next medium info", K(ret));
    }
  }

  return ret;
}

int64_t ObTabletDumpedMediumInfo::get_min_medium_snapshot() const
{
  return medium_info_list_.empty() ? 0 : medium_info_list_.at(0)->medium_snapshot_;
}

int64_t ObTabletDumpedMediumInfo::get_max_medium_snapshot() const
{
  return medium_info_list_.empty() ? 0 : medium_info_list_.at(medium_info_list_.count() - 1)->medium_snapshot_;
}

int ObTabletDumpedMediumInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t count = medium_info_list_.count();
  int64_t new_pos = pos;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, new_pos, count))) {
    LOG_WARN("failed to serialize", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const compaction::ObMediumCompactionInfo *medium_info = medium_info_list_.at(i);
      if (OB_ISNULL(medium_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("medium info is null", K(ret), KP(medium_info), K(i));
      } else if (OB_FAIL(medium_info->serialize(buf, buf_len, new_pos))) {
        LOG_WARN("failed to serialize medium info", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    pos = new_pos;
  }

  return ret;
}

int ObTabletDumpedMediumInfo::deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t count = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::decode(buf, buf_len, new_pos, count))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(medium_info_list_.reserve(count))) {
    LOG_WARN("failed to reserve memory for array", K(ret), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      compaction::ObMediumCompactionInfo *medium_info = nullptr;
      void *buffer = allocator.alloc(sizeof(compaction::ObMediumCompactionInfo));
      if (OB_ISNULL(buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (FALSE_IT(medium_info = new (buffer) compaction::ObMediumCompactionInfo())) {
      } else if (OB_FAIL(medium_info->deserialize(allocator, buf, buf_len, new_pos))) {
        LOG_WARN("failed to deserialize", K(ret));
      } else if (OB_FAIL(medium_info_list_.push_back(medium_info))) {
        LOG_WARN("failed to push back to array", K(ret));
      }

      if (OB_FAIL(ret)) {
        if (nullptr != medium_info) {
          medium_info->reset();
        }
        if (nullptr != buffer) {
          allocator.free(buffer);
        }
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < medium_info_list_.count(); ++i) {
        compaction::ObMediumCompactionInfo *medium_info = medium_info_list_[i];
        if (OB_ISNULL(medium_info)) {
          LOG_ERROR("medium info kv is null", KP(medium_info), K(i));
        } else {
          medium_info->reset();
          allocator.free(medium_info);
        }
      }
      medium_info_list_.reset();
    }
  }

  if (OB_SUCC(ret)) {
    pos = new_pos;
    allocator_ = &allocator;
    is_inited_ = true;
  }

  return ret;
}

int64_t ObTabletDumpedMediumInfo::get_serialize_size() const
{
  int64_t size = 0;
  int ret = OB_SUCCESS;
  const int64_t count = medium_info_list_.count();

  size += serialization::encoded_length(count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const compaction::ObMediumCompactionInfo *medium_info = medium_info_list_.at(i);
    if (OB_ISNULL(medium_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("medium info kv is null", K(ret), KP(medium_info), K(i));
    } else {
      size += medium_info->get_serialize_size();
    }
  }

  return size;
}

int64_t ObTabletDumpedMediumInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(is_inited), KP_(allocator));
    J_COMMA();

    J_NAME("medium_info");
    J_COLON();
    J_ARRAY_START();
    for (int64_t i = 0; i < medium_info_list_.count(); ++i) {
      const compaction::ObMediumCompactionInfo *info = medium_info_list_[i];
      if (i != 0) {
        J_COMMA();
      }

      if (OB_ISNULL(info)) {
        BUF_PRINTO(info);
      } else {
        BUF_PRINTO(*info);
      }
    }
    J_ARRAY_END();
    J_OBJ_END();
  }

  return pos;
}

int64_t ObTabletDumpedMediumInfo::simple_to_string(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    const int64_t count = medium_info_list_.count();

    databuff_printf(buf, buf_len, pos, "{");
    databuff_print_json_kv(buf, buf_len, pos, "count", count);
    databuff_printf(buf, buf_len, pos, ", ");
    databuff_printf(buf, buf_len, pos, "elements:[");
    for (int64_t i = 0; i < count; ++i) {
      const compaction::ObMediumCompactionInfo *info = medium_info_list_.at(i);
      if (i != 0) {
        databuff_printf(buf, buf_len, pos, ", ");
      }

      if (OB_ISNULL(info)) {
        databuff_printf(buf, buf_len, pos, "{");
        databuff_print_json_kv(buf, buf_len, pos, "i", i);
        databuff_print_json_kv_comma(buf, buf_len, pos, "info", info);
        databuff_printf(buf, buf_len, pos, "}");
      } else {
        databuff_printf(buf, buf_len, pos, "{");
        databuff_print_json_kv(buf, buf_len, pos, "i", i);
        databuff_print_json_kv_comma(buf, buf_len, pos, "medium_compat_version", info->medium_compat_version_);
        databuff_print_json_kv_comma(buf, buf_len, pos, "cluster_id", info->cluster_id_);
        databuff_print_json_kv_comma(buf, buf_len, pos, "data_version", info->data_version_);
        databuff_print_json_kv_comma(buf, buf_len, pos, "medium_snapshot", info->medium_snapshot_);
        databuff_printf(buf, buf_len, pos, "}");
      }
    }
    databuff_printf(buf, buf_len, pos, "]");
    databuff_printf(buf, buf_len, pos, "}");
  }

  return pos;
}

bool ObTabletDumpedMediumInfo::compare(
    const compaction::ObMediumCompactionInfo *lhs,
    const compaction::ObMediumCompactionInfo *rhs)
{
  return lhs->medium_snapshot_ < rhs->medium_snapshot_;
}


ObTabletDumpedMediumInfoIterator::ObTabletDumpedMediumInfoIterator()
  : is_inited_(false),
    idx_(0),
    allocator_(nullptr),
    medium_info_list_()
{
}

ObTabletDumpedMediumInfoIterator::~ObTabletDumpedMediumInfoIterator()
{
  reset();
}

int ObTabletDumpedMediumInfoIterator::init(
    common::ObIAllocator &allocator,
    const ObTabletDumpedMediumInfo &dumped_medium_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    const common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> &array = dumped_medium_info.medium_info_list_;
    compaction::ObMediumCompactionInfo* info = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
      compaction::ObMediumCompactionInfo* src_medium_info = array.at(i);
      info = nullptr;
      if (OB_ISNULL(src_medium_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, src medium info is null", K(ret), K(i), KP(src_medium_info));
      } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, info))) {
        LOG_WARN("failed to alloc and new", K(ret));
      } else if (OB_FAIL(info->assign(allocator, *src_medium_info))) {
        LOG_WARN("failed to copy medium info", K(ret), KPC(src_medium_info));
      } else if (OB_FAIL(medium_info_list_.push_back(info))) {
        LOG_WARN("failed to push back to array", K(ret));
      }

      if (OB_FAIL(ret)) {
        if (nullptr != info) {
          allocator.free(info);
        }
      }
    }

    if (OB_FAIL(ret)) {
      reset();
    } else {
      std::sort(medium_info_list_.begin(), medium_info_list_.end(), ObTabletDumpedMediumInfo::compare);

      idx_ = 0;
      allocator_ = &allocator;
      is_inited_ = true;
    }
  }

  return ret;
}

void ObTabletDumpedMediumInfoIterator::reset()
{
  if (OB_NOT_NULL(allocator_)) {
    for (int64_t i = 0; i < medium_info_list_.count(); ++i) {
      compaction::ObMediumCompactionInfo* medium_info = medium_info_list_.at(i);
      if (OB_ISNULL(medium_info)) {
        LOG_ERROR_RET(OB_ERR_SYS, "medium info is null", K(ret), KP(medium_info), K(i));
      } else {
        medium_info->compaction::ObMediumCompactionInfo::~ObMediumCompactionInfo();
        allocator_->free(medium_info);
      }
    }
  }

  allocator_ = nullptr;
  medium_info_list_.reset();
  idx_ = 0;
  is_inited_ = false;
}

int ObTabletDumpedMediumInfoIterator::get_next_key(compaction::ObMediumCompactionInfoKey &key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (idx_ == medium_info_list_.count()) {
    ret = OB_ITER_END;
  } else {
    const compaction::ObMediumCompactionInfo *medium_info = medium_info_list_.at(idx_);
    key = medium_info->medium_snapshot_;
    ++idx_;
  }

  return ret;
}

int ObTabletDumpedMediumInfoIterator::get_next_medium_info(
    common::ObIAllocator &allocator,
    compaction::ObMediumCompactionInfoKey &key,
    compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (idx_ == medium_info_list_.count()) {
    ret = OB_ITER_END;
  } else {
    int64_t pos = 0;
    const compaction::ObMediumCompactionInfo *info = medium_info_list_.at(idx_);
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, medium info is null", K(ret), K_(idx), KP(info));
    } else if (OB_FAIL(medium_info.assign(allocator, *info))) {
      LOG_WARN("failed to copy medium info", K(ret), KPC(info));
    } else {
      key = info->medium_snapshot_;
      ++idx_;
    }
  }

  return ret;
}

int ObTabletDumpedMediumInfoIterator::get_next_medium_info(
    compaction::ObMediumCompactionInfoKey &key,
    const compaction::ObMediumCompactionInfo *&medium_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (idx_ == medium_info_list_.count()) {
    ret = OB_ITER_END;
    LOG_DEBUG("iter end", K(ret), K_(idx));
  } else {
    const compaction::ObMediumCompactionInfo *info = medium_info_list_.at(idx_);
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, medium info is null", K(ret), K_(idx), KP(info));
    } else {
      key = info->medium_snapshot_;
      medium_info = info;
      ++idx_;
    }
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
