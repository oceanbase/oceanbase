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

#include "storage/tmp_file/ob_tmp_file_flush_priority_manager.h"

namespace oceanbase
{
namespace tmp_file
{
//----------------------- ObTmpFileFlushPriorityManager -------------------//
int ObTmpFileFlushPriorityManager::init()
{
  int ret = OB_SUCCESS;
  STATIC_ASSERT(ARRAYSIZEOF(data_flush_lists_) == (int64_t)FileList::MAX,
                "data_flush_lists_ size mismatch enum FileList count");
  STATIC_ASSERT(ARRAYSIZEOF(data_list_locks_) == (int64_t)FileList::MAX,
                "data_list_locks_ size mismatch enum FileList count");
  STATIC_ASSERT(ARRAYSIZEOF(meta_flush_lists_) == (int64_t)FileList::MAX,
                "meta_flush_lists_ size mismatch enum FileList count");
  STATIC_ASSERT(ARRAYSIZEOF(meta_list_locks_) == (int64_t)FileList::MAX,
                "meta_list_locks_ size mismatch enum FileList count");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileFlushPriorityManager inited twice", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("ObTmpFileFlushPriorityManager init succ", K(is_inited_));
  }
  return ret;
}

void ObTmpFileFlushPriorityManager::destroy()
{
  is_inited_ = false;
  for (int64_t i = 0; i < FileList::MAX; i++) {
    ObSpinLockGuard guard(data_list_locks_[i]);
    data_flush_lists_[i].reset();
  }
  for (int64_t i = 0; i < FileList::MAX; i++) {
    ObSpinLockGuard guard(meta_list_locks_[i]);
    meta_flush_lists_[i].reset();
  }
}

int64_t ObTmpFileFlushPriorityManager::get_file_size()
{
  int64_t size = 0;
  is_inited_ = false;
  for (int64_t i = 0; i < FileList::MAX; i++) {
    ObSpinLockGuard guard(data_list_locks_[i]);
    size += data_flush_lists_[i].get_size();
  }
  for (int64_t i = 0; i < FileList::MAX; i++) {
    ObSpinLockGuard guard(meta_list_locks_[i]);
    size += meta_flush_lists_[i].get_size();
  }
  return size;
}

// attention:
// call this function with protection of ObSharedNothingTmpFile's meta_lock
int ObTmpFileFlushPriorityManager::insert_data_flush_list(ObSharedNothingTmpFile &file, const int64_t dirty_page_size)
{
  int ret = OB_SUCCESS;
  FileList flush_idx = FileList::MAX;

  if (OB_FAIL(get_data_list_idx_(dirty_page_size, flush_idx))) {
    LOG_WARN("fail to get data list idx", KR(ret), K(dirty_page_size));
  } else if (OB_FAIL(insert_flush_list_(false/*is_meta*/, file, flush_idx))) {
    LOG_WARN("fail to insert data flush list", KR(ret), K(file), K(dirty_page_size));
  } else {
    LOG_DEBUG("insert_data_flush_list succ", K(file), K(dirty_page_size));
  }

  return ret;
}

// attention:
// call this function with protection of ObSharedNothingTmpFile's meta_lock
int ObTmpFileFlushPriorityManager::insert_meta_flush_list(ObSharedNothingTmpFile &file,
                                                          const int64_t non_rightmost_dirty_page_num,
                                                          const int64_t rightmost_dirty_page_num)
{
  int ret = OB_SUCCESS;
  FileList flush_idx = FileList::MAX;

  if (OB_FAIL(get_meta_list_idx_(non_rightmost_dirty_page_num, rightmost_dirty_page_num, flush_idx))) {
    LOG_WARN("fail to get meta list idx", KR(ret), K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
  } else if (OB_FAIL(insert_flush_list_(true/*is_meta*/, file, flush_idx))) {
    LOG_WARN("fail to insert meta flush list", KR(ret), K(file), K(flush_idx),
             K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
  } else {
    LOG_DEBUG("insert_meta_flush_list succ", K(file), K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
  }

  return ret;
}

int ObTmpFileFlushPriorityManager::insert_flush_list_(const bool is_meta, ObSharedNothingTmpFile &file,
                                                      const FileList flush_idx)
{
  int ret = OB_SUCCESS;
  ObSharedNothingTmpFile::ObTmpFileNode &flush_node = is_meta ? file.get_meta_flush_node() : file.get_data_flush_node();
  ObSpinLock* locks = is_meta ? meta_list_locks_ : data_list_locks_;
  ObTmpFileFlushList *flush_lists =  is_meta ? meta_flush_lists_ : data_flush_lists_;

  if (OB_UNLIKELY(flush_node.get_next() != nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file already in flush list", KR(ret), K(flush_node));
  } else if (flush_idx < FileList::L1 || flush_idx >= FileList::MAX){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush list idx", KR(ret), K(flush_idx));
  } else {
    ObSpinLockGuard guard(locks[flush_idx]);
    file.inc_ref_cnt();
    if (OB_UNLIKELY(!flush_lists[flush_idx].add_last(&flush_node))) {
      file.dec_ref_cnt();
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add node to list", KR(ret), K(flush_idx), K(flush_node));
    } else if (is_meta) {
      file.set_meta_page_flush_level(flush_idx);
    } else {
      file.set_data_page_flush_level(flush_idx);
    }
  }

  return ret;
}

// attention:
// call this function with protection of ObSharedNothingTmpFile's meta_lock
int ObTmpFileFlushPriorityManager::update_data_flush_list(ObSharedNothingTmpFile &file, const int64_t dirty_page_size)
{
  int ret = OB_SUCCESS;
  FileList new_flush_idx = FileList::MAX;

  if (OB_FAIL(get_data_list_idx_(dirty_page_size, new_flush_idx))) {
    LOG_WARN("fail to get data list idx", KR(ret), K(dirty_page_size));
  } else if (OB_FAIL(update_flush_list_(false/*is_meta*/, file, new_flush_idx))) {
    LOG_WARN("fail to update data flush list", KR(ret), K(file), K(dirty_page_size));
  } else {
    LOG_DEBUG("update_data_flush_list succ", K(file), K(dirty_page_size));
  }
  return ret;
}

// attention:
// call this function with protection of ObSharedNothingTmpFile's meta_lock
int ObTmpFileFlushPriorityManager::update_meta_flush_list(ObSharedNothingTmpFile &file,
                                                          const int64_t non_rightmost_dirty_page_num,
                                                          const int64_t rightmost_dirty_page_num)
{
  int ret = OB_SUCCESS;
  FileList new_flush_idx = FileList::MAX;

  if (OB_FAIL(get_meta_list_idx_(non_rightmost_dirty_page_num, rightmost_dirty_page_num, new_flush_idx))) {
    LOG_WARN("fail to get meta list idx", KR(ret), K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
  } else if (OB_FAIL(update_flush_list_(true/*is_meta*/, file, new_flush_idx))) {
    LOG_WARN("fail to update meta flush list", KR(ret), K(file), K(new_flush_idx),
             K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
  } else {
    LOG_DEBUG("update_meta_flush_list succ", K(file), K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
  }
  return ret;
}

int ObTmpFileFlushPriorityManager::update_flush_list_(const bool is_meta, ObSharedNothingTmpFile &file,
                                                      const FileList new_flush_idx)
{
  int ret = OB_SUCCESS;
  ObSharedNothingTmpFile::ObTmpFileNode &flush_node = is_meta ? file.get_meta_flush_node() : file.get_data_flush_node();
  ObSpinLock* locks = is_meta ? meta_list_locks_ : data_list_locks_;
  ObTmpFileFlushList *flush_lists =  is_meta ? meta_flush_lists_ : data_flush_lists_;
  int64_t cur_flush_idx = is_meta ? file.get_meta_page_flush_level() : file.get_data_page_flush_level();

  if (cur_flush_idx < FileList::L1 || cur_flush_idx >= FileList::MAX){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush list idx", KR(ret), K(new_flush_idx));
  } else if (new_flush_idx == cur_flush_idx) {
    // no need to update
  } else { // need to move file into a new flush list
    bool is_in_flushing = false;
    {
      ObSpinLockGuard guard(locks[cur_flush_idx]);
      if (OB_ISNULL(flush_node.get_next())) {
        // before we lock the list, flush task mgr has popped the node from list and is operating it, do nothing
        is_in_flushing = true;
      } else if (OB_UNLIKELY(!flush_lists[cur_flush_idx].remove(&flush_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to remove node from old list", KR(ret), K(cur_flush_idx));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_in_flushing) {
      // the node will be added into flush list again by flush task mgr
      // do nothing
    } else {
      ObSpinLockGuard guard(locks[new_flush_idx]);
      if (OB_UNLIKELY(!flush_lists[new_flush_idx].add_last(&flush_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add node to new list", KR(ret), K(new_flush_idx));
      } else {
        if (is_meta) {
          file.set_meta_page_flush_level(new_flush_idx);
        } else {
          file.set_data_page_flush_level(new_flush_idx);
        }
      }
    }
  }
  return ret;
}

int ObTmpFileFlushPriorityManager::remove_file(ObSharedNothingTmpFile &file)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remove_file(true /*is_meta*/, file))) {
    LOG_WARN("fail to remove file from meta flush list", KR(ret));
  } else if (OB_FAIL(remove_file(false /*is_meta*/, file))) {
    LOG_WARN("fail to remove file from data flush list", KR(ret));
  }
  return ret;
}

// attention:
// call this function with protection of ObSharedNothingTmpFile's meta_lock
int ObTmpFileFlushPriorityManager::remove_file(const bool is_meta, ObSharedNothingTmpFile &file)
{
  int ret = OB_SUCCESS;
  int64_t flush_idx = is_meta ? file.get_meta_page_flush_level() : file.get_data_page_flush_level();
  ObSharedNothingTmpFile::ObTmpFileNode &flush_node = is_meta ? file.get_meta_flush_node() : file.get_data_flush_node();
  if (FileList::INVALID == flush_idx) {
    // file doesn't exist in the flushing list
    // do nothing
  } else if (flush_idx < FileList::L1 || flush_idx >= FileList::MAX){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid flush list idx", KR(ret), K(flush_idx));
  } else {
    ObSpinLock* locks = is_meta ? meta_list_locks_ : data_list_locks_;
    ObTmpFileFlushList *flush_lists = is_meta ? meta_flush_lists_ : data_flush_lists_;
    ObSpinLockGuard guard(locks[flush_idx]);
    if (OB_ISNULL(flush_node.get_next())) {
      // node has not been inserted, do nothing
    } else if (OB_ISNULL(flush_lists[flush_idx].remove(&flush_node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove node from list", KR(ret), K(flush_idx));
    } else {
      file.dec_ref_cnt();
      if (is_meta) {
        file.set_meta_page_flush_level(-1);
      } else {
        file.set_data_page_flush_level(-1);
      }
      LOG_DEBUG("remove file succ", K(file), K(is_meta));
    }
  }
  return ret;
}

int ObTmpFileFlushPriorityManager::popN_from_file_list(const bool is_meta, const int64_t list_idx,
                                                       const int64_t expected_count, int64_t &actual_count,
                                                       ObArray<ObTmpFileHandle> &file_handles)
{
  int ret = OB_SUCCESS;
  ObSpinLock* locks = is_meta ? meta_list_locks_ : data_list_locks_;
  ObTmpFileFlushList *flush_lists = is_meta ? meta_flush_lists_ : data_flush_lists_;
  actual_count = 0;

  if (OB_UNLIKELY(list_idx < FileList::L1 || list_idx >= FileList::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(list_idx));
  } else {
    ObSpinLockGuard guard(locks[list_idx]);
    while (OB_SUCC(ret) && !flush_lists[list_idx].is_empty() && actual_count < expected_count) {
      ObSharedNothingTmpFile *file = nullptr;
      if (OB_ISNULL(file = &flush_lists[list_idx].remove_first()->file_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file is null", KR(ret));
      } else if (OB_FAIL(file_handles.push_back(file))) {
        LOG_WARN("fail to push back", KR(ret), KP(file));
        int tmp_ret = OB_SUCCESS;
        ObSharedNothingTmpFile::ObTmpFileNode &node = is_meta ? file->get_meta_flush_node() : file->get_data_flush_node();
        if (OB_UNLIKELY(!flush_lists[list_idx].add_last(&node))) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to add node to list", KR(tmp_ret), K(list_idx), KP(file));
        }
      } else {
        LOG_DEBUG("pop file succ", KPC(file), K(is_meta));
        file->dec_ref_cnt(); // ref_cnt of flush list
        actual_count++;
      }
    } // end while
  }

  return ret;
}

int ObTmpFileFlushPriorityManager::get_meta_list_idx_(const int64_t non_rightmost_dirty_page_num,
                                                      const int64_t rightmost_dirty_page_num, FileList &idx)
{
  int ret = OB_SUCCESS;
  idx = FileList::MAX;
  if (non_rightmost_dirty_page_num + rightmost_dirty_page_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
  } else if (non_rightmost_dirty_page_num == 0) {   // all dirty pages is the rightmost page
    idx = FileList::L5;
  } else if (non_rightmost_dirty_page_num < 16) {   // (0KB, 128KB)
    idx = FileList::L4;
  } else if (non_rightmost_dirty_page_num < 128) {  // [128KB, 1MB)
    idx = FileList::L3;
  } else if (non_rightmost_dirty_page_num < 256) {  // [1MB, 2MB)
    idx = FileList::L2;
  } else {                                          // [2MB, INFINITE)
    idx = FileList::L1;
  }
  return ret;
}

int ObTmpFileFlushPriorityManager::get_data_list_idx_(const int64_t dirty_page_size, FileList &idx)
{
  int ret = OB_SUCCESS;
  idx = FileList::MAX;
  if (dirty_page_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dirty_page_size));
  } else if (dirty_page_size < (1 << 13)) {   // page_size 8KB, (0, 8KB)
    idx = FileList::L5;
  } else if (dirty_page_size < (1 << 17)) {   // [8KB, 128KB)
    idx = FileList::L4;
  } else if (dirty_page_size < (1 << 20)) {   // [128KB, 1MB)
    idx = FileList::L3;
  } else if (dirty_page_size < (1 << 21)) {   // [1MB, 2MB)
    idx = FileList::L2;
  } else {                                    // [2MB, INFINITE)
    idx = FileList::L1;
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
