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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_i_tmp_file.h"
#include "lib/container/ob_array.h"

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
  typedef common::ObDList<ObITmpFile::ObTmpFileNode> ObTmpFileFlushList;
  friend class ObTmpFileFlushListIterator;

public:
  int insert_data_flush_list(ObITmpFile &file, const int64_t dirty_page_size);
  int insert_meta_flush_list(ObITmpFile &file, const int64_t non_rightmost_dirty_page_num,
                             const int64_t rightmost_dirty_page_num);
  int update_data_flush_list(ObITmpFile &file, const int64_t dirty_page_size);
  int update_meta_flush_list(ObITmpFile &file, const int64_t non_rightmost_dirty_page_num,
                             const int64_t rightmost_dirty_page_num);
  int remove_file(ObITmpFile &file);
  int remove_file(const bool is_meta, ObITmpFile &file);
  int popN_from_file_list(const bool is_meta, const int64_t list_idx,
                          const int64_t expected_count, int64_t &actual_count,
                          ObArray<ObITmpFileHandle> &file_handles);
  int64_t get_file_size();
private:
  typedef ObTmpFileGlobal::FileList FileList;
  int get_file_flush_node_(const bool is_meta, ObITmpFile &file, ObITmpFile::ObTmpFileNode *&flush_node);
  int get_file_flush_level_(const bool is_meta, ObITmpFile &file, FileList &flush_level);
  int get_meta_list_idx_(const int64_t non_rightmost_dirty_page_num,
                         const int64_t rightmost_dirty_page_num, FileList &idx);
  int get_data_list_idx_(const int64_t dirty_page_size, FileList &idx);
  int set_flush_page_level_(const bool is_meta, const FileList flush_idx, ObITmpFile &file);
  int insert_flush_list_(const bool is_meta, ObITmpFile &file,
                         const FileList flush_idx);
  int update_flush_list_(const bool is_meta, ObITmpFile &file,
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
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_
