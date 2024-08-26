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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileFlushPriorityManager
{
public:
  ObTmpFileFlushPriorityManager()
    : is_inited_(false),
      data_flush_lists_(),
      data_list_locks_(),
      meta_flush_lists_(),
      meta_list_locks_()
  {}
  ~ObTmpFileFlushPriorityManager() {}
  int init();
  void destroy();

private:
  enum FileList {
    INVALID = -1,
    L1 = 0, // [2MB, INFINITE)
    L2,     // [1MB, 2MB)
    L3,     // [128KB, 1MB)
    L4,     // data_list: [8KB, 128KB); meta_list: (0KB, 128KB)
    L5,     // data_list: (0, 8KB); meta_list: 0KB
    MAX
  };
  typedef common::ObDList<ObSharedNothingTmpFile::ObTmpFileNode> ObTmpFileFlushList;
  friend class ObTmpFileFlushListIterator;

public:
  int insert_data_flush_list(ObSharedNothingTmpFile &file, const int64_t dirty_page_size);
  int insert_meta_flush_list(ObSharedNothingTmpFile &file, const int64_t non_rightmost_dirty_page_num,
                             const int64_t rightmost_dirty_page_num);
  int update_data_flush_list(ObSharedNothingTmpFile &file, const int64_t dirty_page_size);
  int update_meta_flush_list(ObSharedNothingTmpFile &file, const int64_t non_rightmost_dirty_page_num,
                             const int64_t rightmost_dirty_page_num);
  int remove_file(ObSharedNothingTmpFile &file);
  int remove_file(const bool is_meta, ObSharedNothingTmpFile &file);
  int popN_from_file_list(const bool is_meta, const int64_t list_idx,
                          const int64_t expected_count, int64_t &actual_count,
                          ObArray<ObTmpFileHandle> &file_handles);
  int64_t get_file_size();
private:
  int get_meta_list_idx_(const int64_t non_rightmost_dirty_page_num,
                         const int64_t rightmost_dirty_page_num, FileList &idx);
  int get_data_list_idx_(const int64_t dirty_page_size, FileList &idx);
  int insert_flush_list_(const bool is_meta, ObSharedNothingTmpFile &file,
                         const FileList flush_idx);
  int update_flush_list_(const bool is_meta, ObSharedNothingTmpFile &file,
                         const FileList new_flush_idx);

private:
  bool is_inited_;
  ObTmpFileFlushList data_flush_lists_[FileList::MAX];
  ObSpinLock data_list_locks_[FileList::MAX];
  ObTmpFileFlushList meta_flush_lists_[FileList::MAX];
  ObSpinLock meta_list_locks_[FileList::MAX];
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_
