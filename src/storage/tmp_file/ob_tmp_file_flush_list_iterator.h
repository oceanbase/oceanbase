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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_LIST_ITERATOR_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_LIST_ITERATOR_H_

#include "storage/tmp_file/ob_tmp_file_flush_priority_manager.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace tmp_file
{

typedef ObTmpFileGlobal::FlushCtxState FlushCtxState;

class ObTmpFileFlushListIterator
{
public:
  ObTmpFileFlushListIterator();
  ~ObTmpFileFlushListIterator();

  int init(ObTmpFileFlushPriorityManager *prio_mgr);
  int clear();
  int reset();
  void destroy();
  int next(const FlushCtxState iter_stage, bool &is_meta, ObTmpFileHandle &file_handle);

  TO_STRING_KV(K(is_inited_), K(cur_caching_list_idx_), K(cur_caching_list_is_meta_),
               K(cur_iter_dir_idx_), K(cur_iter_file_idx_), K(cached_file_num_), K(cached_dir_num_))
private:
  int reinsert_files_into_flush_list_(const int64_t start_file_idx, const int64_t end_file_idx);
  FlushCtxState cal_current_flush_stage_();
  int init_caching_list_with_flush_stage_(const FlushCtxState iter_stage);
  int acquire_final_list_of_flush_stage_(const FlushCtxState iter_stage,
                                         ObTmpFileFlushPriorityManager::FileList &list_idx);
  int cache_files_(const FlushCtxState iter_stage);
  int build_file_wrappers_(const ObArray<ObTmpFileHandle> &file_handles);
  int build_dir_wrappers_();
  int cache_big_files_(const ObArray<ObTmpFileHandle> &file_handles);
  int cache_small_files_(const ObArray<ObTmpFileHandle> &file_handles);
  int get_flushing_file_dirty_page_num_(const ObSharedNothingTmpFile &file, int64_t &page_num);
  int check_cur_idx_status_();
  int advance_big_file_idx_();
  int advance_small_file_idx_();
  int advance_dir_idx_();
  int advance_caching_list_idx_();
private:
  struct ObFlushingTmpFileWrapper
  {
    ObFlushingTmpFileWrapper() : is_inited_(false), is_meta_(false), file_handle_() {}
    ~ObFlushingTmpFileWrapper() { reset(); };
    int init(const bool is_meta, const ObTmpFileHandle &file_handle);
    void reset();
    bool operator <(const ObFlushingTmpFileWrapper &other);
    TO_STRING_KV(K(is_inited_), K(is_meta_), K(file_handle_));

    bool is_inited_;
    bool is_meta_;
    ObTmpFileHandle file_handle_;
  };

  struct ObFlushingTmpFileDirWrapper
  {
    ObFlushingTmpFileDirWrapper() : is_inited_(false), is_meta_(false), page_num_(0),
                                    start_file_idx_(-1), end_file_idx_(-1) {}
    ~ObFlushingTmpFileDirWrapper() { reset(); };
    int init(const bool is_meta, const int64_t page_num, const int64_t start_file_idx, const int64_t end_file_idx);
    void reset();
    bool operator <(const ObFlushingTmpFileDirWrapper &other);
    TO_STRING_KV(K(is_inited_), K(is_meta_), K(page_num_), K(start_file_idx_), K(end_file_idx_));

    bool is_inited_;
    bool is_meta_;
    int64_t page_num_;
    int64_t start_file_idx_;
    int64_t end_file_idx_;
  };
private:
  static constexpr int64_t MAX_CACHE_NUM = 256;
  static constexpr int64_t BIG_FILE_CACHE_NUM = 8;
  bool is_inited_;
  ObTmpFileFlushPriorityManager *prio_mgr_;
  ObArray<ObFlushingTmpFileWrapper> files_;
  ObArray<ObFlushingTmpFileDirWrapper> dirs_;
  bool cur_caching_list_is_meta_;
  ObTmpFileFlushPriorityManager::FileList cur_caching_list_idx_;
  int64_t cur_iter_dir_idx_;
  int64_t cur_iter_file_idx_;
  int64_t cached_file_num_;
  int64_t cached_dir_num_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_LIST_ITERATOR_H_
