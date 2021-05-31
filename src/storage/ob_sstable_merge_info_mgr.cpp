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
#include "ob_sstable_merge_info_mgr.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
using namespace common;
namespace storage {
/**
 * ------------------------------------------------------------------ObSSTableMergeInfoIter-------------------------------------------------------------
 */
ObSSTableMergeInfoIterator::ObSSTableMergeInfoIterator()
    : major_info_idx_(0), major_info_cnt_(0), minor_info_idx_(0), minor_info_cnt_(0), is_opened_(false)
{}

ObSSTableMergeInfoIterator::~ObSSTableMergeInfoIterator()
{}

int ObSSTableMergeInfoIterator::open()
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObSSTableMergeInfoIterator has been opened, ", K(ret));
  } else {
    major_info_idx_ = 0;
    minor_info_idx_ = 0;
    major_info_cnt_ = ObSSTableMergeInfoMgr::get_instance().major_info_cnt_;
    minor_info_cnt_ = ObSSTableMergeInfoMgr::get_instance().minor_info_cnt_;
    is_opened_ = true;
  }
  return ret;
}

int ObSSTableMergeInfoIterator::get_next_merge_info(ObSSTableMergeInfo& merge_info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSTableMergeInfoIterator has not been inited, ", K(ret));
  } else if (major_info_idx_ < major_info_cnt_) {
    if (OB_FAIL(ObSSTableMergeInfoMgr::get_instance().get_major_info(major_info_idx_, merge_info))) {
      STORAGE_LOG(WARN, "Fail to get merge info, ", K(ret), K_(major_info_idx));
    } else {
      major_info_idx_++;
    }
  } else if (minor_info_idx_ < minor_info_cnt_) {
    if (OB_FAIL(ObSSTableMergeInfoMgr::get_instance().get_minor_info(minor_info_idx_, merge_info))) {
      STORAGE_LOG(WARN, "Fail to get merge info, ", K(ret), K_(minor_info_idx));
    } else {
      minor_info_idx_++;
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

void ObSSTableMergeInfoIterator::reset()
{
  major_info_idx_ = 0;
  major_info_cnt_ = 0;
  minor_info_idx_ = 0;
  minor_info_cnt_ = 0;
  is_opened_ = false;
}

/**
 * ------------------------------------------------------------------ObSSTableMergeInfoMgr---------------------------------------------------------------
 */
ObSSTableMergeInfoMgr::ObSSTableMergeInfoMgr()
    : is_inited_(false),
      lock_(),
      allocator_(ObModIds::OB_SSTABLE_MERGE_INFO, OB_MALLOC_BIG_BLOCK_SIZE),
      merge_info_map_(),
      major_merge_infos_(NULL),
      minor_merge_infos_(NULL),
      major_info_max_cnt_(0),
      minor_info_max_cnt_(0),
      major_info_cnt_(0),
      minor_info_cnt_(0),
      major_info_idx_(0),
      minor_info_idx_(0)
{}

ObSSTableMergeInfoMgr::~ObSSTableMergeInfoMgr()
{
  destroy();
}

int ObSSTableMergeInfoMgr::init(const int64_t memory_limit)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSSTableMergeInfo has already been initiated", K(ret));
  } else if (OB_UNLIKELY(memory_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(memory_limit));
  } else {
    void* buf = NULL;
    ObSSTableMergeInfoValue* info_value = NULL;
    int64_t max_info_cnt = min(GCONF.get_server_memory_limit(), memory_limit) /
                           (sizeof(ObSSTableMergeInfo) + sizeof(ObSSTableMergeInfoValue));
    if (max_info_cnt < 2) {
      max_info_cnt = 2;
    }

    major_info_max_cnt_ = minor_info_max_cnt_ = max_info_cnt / 2;
    if (OB_FAIL(merge_info_map_.create(
            major_info_max_cnt_, ObModIds::OB_SSTABLE_MERGE_INFO, ObModIds::OB_SSTABLE_MERGE_INFO))) {
      STORAGE_LOG(WARN, "Fail to create merge info map, ", K(ret));
    } else if (OB_FAIL(alloc_info_array(major_merge_infos_, major_info_max_cnt_))) {
      STORAGE_LOG(WARN, "Fail to alloc major merge infos, ", K(ret), K_(major_info_max_cnt));
    } else if (OB_FAIL(alloc_info_array(minor_merge_infos_, minor_info_max_cnt_))) {
      STORAGE_LOG(WARN, "Fail to alloc minor merge infos, ", K(ret), K_(minor_info_max_cnt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i <= max_info_cnt; ++i) {
        if (NULL == (buf = allocator_.alloc(sizeof(ObSSTableMergeInfoValue)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
        } else {
          info_value = new (buf) ObSSTableMergeInfoValue();
          if (OB_FAIL(free_info_values_.push_back(info_value))) {
            STORAGE_LOG(WARN, "Fail to push info value to free list, ", K(ret), K(i), K(max_info_cnt));
          }
        }
      }

      if (OB_SUCC(ret)) {
        major_info_cnt_ = 0;
        minor_info_cnt_ = 0;
        major_info_idx_ = 0;
        minor_info_idx_ = 0;
        is_inited_ = true;
        STORAGE_LOG(INFO, "Success to init ObSSTableMergeInfoMgr, ", K_(major_info_max_cnt), K_(minor_info_max_cnt));
      }
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObSSTableMergeInfoMgr::destroy()
{
  merge_info_map_.destroy();
  free_info_values_.reset();
  major_merge_infos_ = NULL;
  minor_merge_infos_ = NULL;
  major_info_max_cnt_ = 0;
  minor_info_max_cnt_ = 0;
  major_info_cnt_ = 0;
  minor_info_cnt_ = 0;
  major_info_idx_ = 0;
  minor_info_idx_ = 0;
  allocator_.reset();
  is_inited_ = false;
}

ObSSTableMergeInfoMgr& ObSSTableMergeInfoMgr::get_instance()
{
  static ObSSTableMergeInfoMgr instance_;
  return instance_;
}

int ObSSTableMergeInfoMgr::add_sstable_merge_info(const ObSSTableMergeInfo& merge_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSTableMergeInfoMgr is not initialized", K(ret));
  } else if (OB_UNLIKELY(0 == merge_info.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(merge_info));
  } else {
    ObSSTableMergeInfoKey info_key(merge_info.table_id_, merge_info.partition_id_);
    ObSSTableMergeInfoValue* info_value = NULL;

    SpinWLockGuard guard(lock_);
    if (OB_FAIL(merge_info_map_.get_refactored(info_key, info_value))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        info_value = NULL;
        if (OB_FAIL(free_info_values_.pop_back(info_value))) {
          STORAGE_LOG(WARN, "Fail to pop free info, ", K(ret));
        } else if (OB_FAIL(merge_info_map_.set_refactored(info_key, info_value))) {
          STORAGE_LOG(WARN, "Fail to set info key to map, ", K(ret), K(info_key));
        }

        if (OB_FAIL(ret)) {
          if (NULL != info_value) {
            (void)free_info_values_.push_back(info_value);
          }
        }
      }
    }

    if (OB_SUCC(ret) && NULL != info_value) {
      if (merge_info.is_major_merge()) {
        if (major_info_cnt_ < major_info_max_cnt_) {
          ++major_info_cnt_;
        }
        ObSSTableMergeInfo& dest_info = *(major_merge_infos_[major_info_idx_ % major_info_max_cnt_]);
        major_info_idx_ = (major_info_idx_ + 1) % major_info_max_cnt_;

        release_info(dest_info);
        dest_info = merge_info;
        info_value->major_version_ = merge_info.version_.major_;
        info_value->minor_version_ = 0;
        info_value->snapshot_version_ = merge_info.snapshot_version_;
      } else {
        if (minor_info_cnt_ < minor_info_max_cnt_) {
          ++minor_info_cnt_;
        }
        ObSSTableMergeInfo& dest_info = *(minor_merge_infos_[minor_info_idx_ % minor_info_max_cnt_]);
        minor_info_idx_ = (minor_info_idx_ + 1) % minor_info_max_cnt_;

        release_info(dest_info);
        dest_info = merge_info;
        if (merge_info.merge_type_ == MINI_MERGE) {
          info_value->minor_version_++;
          info_value->snapshot_version_ = merge_info.snapshot_version_;
          dest_info.version_ = ObVersion((int32_t)info_value->major_version_, (int32_t)info_value->minor_version_);
          info_value->insert_row_count_ += merge_info.insert_row_count_;
          info_value->update_row_count_ += merge_info.update_row_count_;
          info_value->delete_row_count_ += merge_info.delete_row_count_;
        }
      }
      info_value->ref_cnt_++;
    }
  }

  return ret;
}

int ObSSTableMergeInfoMgr::alloc_info_array(ObSSTableMergeInfo**& merge_infos, const int64_t array_size)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObSSTableMergeInfo* merge_info = NULL;

  if (NULL == (buf = allocator_.alloc(sizeof(ObSSTableMergeInfo*) * array_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(array_size));
  } else {
    MEMSET(buf, 0, sizeof(ObSSTableMergeInfo*) * array_size);
    merge_infos = reinterpret_cast<ObSSTableMergeInfo**>(buf);

    for (int64_t i = 0; OB_SUCC(ret) && i < array_size; ++i) {
      if (NULL == (buf = allocator_.alloc(sizeof(ObSSTableMergeInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(i), K(array_size));
      } else {
        merge_info = new (buf) ObSSTableMergeInfo();
        merge_infos[i] = merge_info;
      }
    }
  }
  return ret;
}

void ObSSTableMergeInfoMgr::release_info(ObSSTableMergeInfo& merge_info)
{
  int ret = OB_SUCCESS;
  if (0 != merge_info.table_id_) {
    ObSSTableMergeInfoKey info_key(merge_info.table_id_, merge_info.partition_id_);
    ObSSTableMergeInfoValue* info_value = NULL;

    if (OB_SUCC(merge_info_map_.get_refactored(info_key, info_value))) {
      info_value->ref_cnt_--;
      if (info_value->ref_cnt_ <= 0) {
        (void)merge_info_map_.erase_refactored(info_key);
        info_value->reset();
        if (OB_FAIL(free_info_values_.push_back(info_value))) {
          STORAGE_LOG(ERROR, "Fail to push info value, ", K(ret));
        }
      }
    }
  }
}

int ObSSTableMergeInfoMgr::get_major_info(const int64_t idx, ObSSTableMergeInfo& merge_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSTableMergeInfoMgr has not been inited, ", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (idx < 0 || idx >= major_info_cnt_) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid idx, ", K(ret), K(idx), K_(major_info_cnt));
    } else {
      merge_info = *(major_merge_infos_[idx]);
    }
  }
  return ret;
}

int ObSSTableMergeInfoMgr::get_minor_info(const int64_t idx, ObSSTableMergeInfo& merge_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSTableMergeInfoMgr has not been inited, ", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (idx < 0 || idx >= minor_info_cnt_) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid idx, ", K(ret), K(idx), K_(minor_info_cnt));
    } else {
      merge_info = *(minor_merge_infos_[idx]);
    }
  }
  return ret;
}

int ObSSTableMergeInfoMgr::get_modification_infos(ObIArray<ObTableModificationInfo>& infos)
{
  int ret = OB_SUCCESS;
  infos.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSTableMergeInfoMgr has not been inited, ", K(ret));
  } else {
    ObTableModificationInfo info;
    ObSSTableMergeInfoValue* info_value = nullptr;
    SpinRLockGuard guard(lock_);
    for (ObSSTableMergeInfoMap::iterator iter = merge_info_map_.begin(); OB_SUCC(ret) && iter != merge_info_map_.end();
         iter++) {
      if (OB_ISNULL(info_value = iter->second)) {
      } else {
        info.table_id_ = iter->first.table_id_;
        info.partition_id_ = iter->first.partition_id_;
        info.insert_row_count_ = info_value->insert_row_count_;
        info.update_row_count_ = info_value->update_row_count_;
        info.delete_row_count_ = info_value->delete_row_count_;
        info.max_snapshot_version_ = info_value->snapshot_version_;
        if (OB_FAIL(infos.push_back(info))) {
          STORAGE_LOG(WARN, "Fail to push back modify info", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
