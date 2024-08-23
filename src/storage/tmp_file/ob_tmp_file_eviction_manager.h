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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_ELIMINATION_MANAGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_ELIMINATION_MANAGER_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileEvictionManager
{
public:
  typedef ObSharedNothingTmpFile::ObTmpFileNode TmpFileNode;
  typedef common::ObDList<TmpFileNode> TmpFileEvictionList;

public:
  ObTmpFileEvictionManager() : data_list_lock_(), file_data_eviction_list_(),
                               meta_list_lock_(), file_meta_eviction_list_() {}
  ~ObTmpFileEvictionManager() { destroy(); }
  void destroy();
  int64_t get_file_size();
  int add_file(const bool is_meta, ObSharedNothingTmpFile &file);
  int remove_file(ObSharedNothingTmpFile &file);
  int remove_file(const bool is_meta, ObSharedNothingTmpFile &file);
  int evict(const int64_t expected_evict_page_num, int64_t &actual_evict_page_num);

private:
  int evict_file_from_list_(const bool &is_meta, const int64_t expected_evict_page_num, int64_t &actual_evict_page_num);
  int pop_file_from_list_(const bool &is_meta, ObTmpFileHandle &file_handle);
private:
  ObSpinLock data_list_lock_;
  TmpFileEvictionList file_data_eviction_list_;
  ObSpinLock meta_list_lock_;
  TmpFileEvictionList file_meta_eviction_list_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_ELIMINATION_MANAGER_H_
