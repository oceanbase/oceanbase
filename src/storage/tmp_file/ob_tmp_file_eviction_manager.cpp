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

#include "storage/tmp_file/ob_tmp_file_eviction_manager.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{
void ObTmpFileEvictionManager::destroy()
{
  {
    ObSpinLockGuard guard(meta_list_lock_);
    file_meta_eviction_list_.reset();
  }
  {
    ObSpinLockGuard guard(data_list_lock_);
    file_data_eviction_list_.reset();
  }
}

int64_t ObTmpFileEvictionManager::get_file_size()
{
  int64_t file_size = 0;
  {
    ObSpinLockGuard guard(meta_list_lock_);
    file_size += file_meta_eviction_list_.get_size();
  }
  {
    ObSpinLockGuard guard(data_list_lock_);
    file_size += file_data_eviction_list_.get_size();
  }
  return file_size;
}

int ObTmpFileEvictionManager::add_file(const bool is_meta, ObSharedNothingTmpFile &file)
{
  int ret = OB_SUCCESS;
  ObSpinLock &lock = is_meta ? meta_list_lock_ : data_list_lock_;
  TmpFileEvictionList &eviction_list = is_meta ? file_meta_eviction_list_ : file_data_eviction_list_;
  ObSharedNothingTmpFile::ObTmpFileNode &eviction_node = is_meta ? file.get_meta_eviction_node() : file.get_data_eviction_node();

  ObSpinLockGuard guard(lock);
  file.inc_ref_cnt();
  if (OB_UNLIKELY(!eviction_list.add_last(&eviction_node))) {
    file.dec_ref_cnt();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add node", KR(ret));
  }

  return ret;
}

int ObTmpFileEvictionManager::remove_file(ObSharedNothingTmpFile &file)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remove_file(true, file))) {
    LOG_WARN("fail to remove file from meta list", KR(ret), K(file));
  } else if (OB_FAIL(remove_file(false, file))) {
    LOG_WARN("fail to remove file from data list", KR(ret), K(file));
  }
  return ret;
}

int ObTmpFileEvictionManager::remove_file(const bool is_meta, ObSharedNothingTmpFile &file)
{
  int ret = OB_SUCCESS;
  ObSpinLock &lock = is_meta ? meta_list_lock_ : data_list_lock_;
  TmpFileEvictionList &eviction_list = is_meta ? file_meta_eviction_list_ : file_data_eviction_list_;
  ObSharedNothingTmpFile::ObTmpFileNode &eviction_node = is_meta ? file.get_meta_eviction_node() : file.get_data_eviction_node();
  ObSpinLockGuard guard(lock);
  if (OB_NOT_NULL(eviction_node.get_next())) {
    if (OB_UNLIKELY(!eviction_list.remove(&eviction_node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove node", KR(ret), K(file));
    } else {
      file.dec_ref_cnt();
    }
  }

  return ret;
}

int ObTmpFileEvictionManager::evict(const int64_t expected_evict_page_num, int64_t &actual_evict_page_num)
{
  int ret = OB_SUCCESS;
  int64_t remain_evict_page_num = expected_evict_page_num;
  actual_evict_page_num = 0;

  int64_t actual_evict_data_page_num = 0;
  if (OB_FAIL(evict_file_from_list_(false/*is_meta*/, remain_evict_page_num, actual_evict_data_page_num))) {
    LOG_WARN("fail to evict file from list", KR(ret), K(remain_evict_page_num), K(actual_evict_data_page_num));
  } else {
    remain_evict_page_num -= actual_evict_data_page_num;
    actual_evict_page_num += actual_evict_data_page_num;
  }
  LOG_DEBUG("evict data pages over", KR(ret), K(expected_evict_page_num), K(remain_evict_page_num),
                                     K(actual_evict_page_num), K(actual_evict_data_page_num));

  int64_t actual_evict_meta_page_num = 0;
  if (FAILEDx(evict_file_from_list_(true/*is_meta*/, remain_evict_page_num, actual_evict_meta_page_num))) {
    LOG_WARN("fail to evict file from list", KR(ret), K(remain_evict_page_num), K(actual_evict_meta_page_num));
  } else {
    remain_evict_page_num -= actual_evict_meta_page_num;
    actual_evict_page_num += actual_evict_meta_page_num;
  }
  LOG_DEBUG("evict meta pages over", KR(ret), K(expected_evict_page_num), K(remain_evict_page_num),
                                    K(actual_evict_page_num), K(actual_evict_meta_page_num));

  return ret;
}

int ObTmpFileEvictionManager::evict_file_from_list_(const bool &is_meta,
                                                    const int64_t expected_evict_page_num,
                                                    int64_t &actual_evict_page_num)
{
  int ret = OB_SUCCESS;
  bool is_empty_list = false;
  int64_t remain_evict_page_num = expected_evict_page_num;
  actual_evict_page_num = 0;

  // attention:
  // in order to avoid repeated inserting file node into list by flush manager,
  // even thought evict manager has popped file from list,
  // we also keep the file's `is_in_meta_eviction_list_` or `is_in_data_eviction_list_` be true.
  // in evict_page() of file, if `remain_flushed_file_page_num` > 0, it will reinsert file node
  // into eviction list; otherwise, it will set `is_in_meta_eviction_list_` or `is_in_data_eviction_list_` be false

  int64_t list_cnt =
    is_meta ? file_meta_eviction_list_.get_size() : file_data_eviction_list_.get_size();

  while(OB_SUCC(ret) && remain_evict_page_num > 0 && !is_empty_list && list_cnt-- > 0) {
    ObTmpFileHandle file_handle;
    int64_t actual_evict_file_page_num = 0;
    int64_t remain_flushed_file_page_num = 0;
    if (OB_FAIL(pop_file_from_list_(is_meta, file_handle))) {
      if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
        is_empty_list = true;
        LOG_DEBUG("no tmp file in list", K(is_meta), K(expected_evict_page_num),
                                         K(remain_evict_page_num), K(actual_evict_page_num));
      } else {
        LOG_WARN("fail to pop file from list", KR(ret), K(is_meta));
      }
    } else if (OB_ISNULL(file_handle.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file handle is invalid", KR(ret), K(file_handle));
    } else if (is_meta) {
      if (OB_FAIL(file_handle.get()->evict_meta_pages(remain_evict_page_num,
                                                      actual_evict_file_page_num))) {
        LOG_WARN("fail to evict meta pages", KR(ret), K(file_handle), K(remain_evict_page_num),
                 K(actual_evict_file_page_num));
      }
    } else {
      if (OB_FAIL(file_handle.get()->evict_data_pages(remain_evict_page_num,
                                                      actual_evict_file_page_num,
                                                      remain_flushed_file_page_num))) {
        LOG_WARN("fail to evict data pages", KR(ret), K(file_handle), K(remain_evict_page_num),
                 K(actual_evict_page_num), K(remain_flushed_file_page_num));
      } else if (OB_UNLIKELY(remain_evict_page_num > actual_evict_file_page_num && remain_flushed_file_page_num > 1)) {
        // we allow to not evict the last data page
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("evict_data_pages unexpected finishes before expected pages are eliminated",
                  KR(ret), K(is_meta), K(file_handle), K(remain_evict_page_num),
                  K(actual_evict_page_num), K(actual_evict_file_page_num),
                  K(remain_flushed_file_page_num));
      }
    }

    if (OB_SUCC(ret)) {
      remain_evict_page_num -= actual_evict_file_page_num;
      actual_evict_page_num += actual_evict_file_page_num;
    }
  }

  return ret;
}

int ObTmpFileEvictionManager::pop_file_from_list_(const bool &is_meta, ObTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  file_handle.reset();
  ObSharedNothingTmpFile *file = nullptr;
  ObSpinLock &lock = is_meta ? meta_list_lock_ : data_list_lock_;
  TmpFileEvictionList &eviction_list = is_meta ? file_meta_eviction_list_ : file_data_eviction_list_;

  ObSpinLockGuard guard(lock);
  if (eviction_list.is_empty()) {
    ret = OB_EMPTY_RESULT;
    LOG_DEBUG("eviction_list is empty", K(is_meta));
  } else if (OB_ISNULL(file = &eviction_list.remove_first()->file_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file is null", KR(ret));
  } else if (OB_FAIL(file_handle.init(file))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init file", KR(ret), KP(file));
  } else {
    file->dec_ref_cnt();
  }
  return ret;
}

} // end namespace tmp_file
} // end namespace oceanbase
