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

#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tmp_file/ob_tmp_file_flush_list_iterator.h"
#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"

namespace oceanbase
{
namespace tmp_file
{
ObTmpFileFlushListIterator::ObTmpFileFlushListIterator() :
  is_inited_(false), files_(), dirs_(), cur_caching_list_is_meta_(false),
  cur_caching_list_idx_(FileList::L1),
  cur_iter_dir_idx_(-1), cur_iter_file_idx_(-1),
  cached_file_num_(0), cached_dir_num_(0)
{}

ObTmpFileFlushListIterator::~ObTmpFileFlushListIterator()
{
  destroy();
}

int ObTmpFileFlushListIterator::init(ObTmpFileFlushPriorityManager *prio_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(prio_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(prio_mgr));
  } else if (FALSE_IT(files_.set_attr(ObMemAttr(MTL_ID(), "TFFlushIterFile")))) {
  } else if (OB_FAIL(files_.prepare_allocate(MAX_CACHE_NUM))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  } else if (FALSE_IT(dirs_.set_attr(ObMemAttr(MTL_ID(), "TFFlushIterDir")))) {
  } else if (OB_FAIL(dirs_.prepare_allocate(MAX_CACHE_NUM))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  } else {
    is_inited_ = true;
    prio_mgr_ = prio_mgr;
  }
  return ret;
}

int ObTmpFileFlushListIterator::clear()
{
  int ret = OB_SUCCESS;

  // reinsert unused cached file into flush list
  FlushCtxState cur_stage = cal_current_flush_stage_();
  if (cur_stage < FlushCtxState::FSM_F1 || cur_stage >= FlushCtxState::FSM_FINISHED) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected flush stage", KR(ret), K(cur_stage));
  } else if (0 == cached_file_num_) {
    // no need to reinsert files, do nothing
  } else if (FlushCtxState::FSM_F1 == cur_stage) {
    if (OB_FAIL(reinsert_files_into_flush_list_(cur_iter_file_idx_, cached_file_num_ - 1))){
      LOG_ERROR("fail to reinsert files into flush list", KR(ret), K(cur_iter_file_idx_), K(cached_file_num_));
    }
  } else {
    for (int64_t i = cur_iter_dir_idx_; OB_SUCC(ret) && (i >= 0 && i < cached_dir_num_); i++) {
      if (OB_UNLIKELY(!dirs_[i].is_inited_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("uninitialized dir is unexpected", KR(ret), K(dirs_[i]));
      } else {
        int64_t start_file_idx = dirs_[i].start_file_idx_;
        int64_t end_file_idx = dirs_[i].end_file_idx_;
        if (i == cur_iter_dir_idx_) {
          start_file_idx = cur_iter_file_idx_;
          if (start_file_idx < dirs_[i].start_file_idx_ || start_file_idx > dirs_[i].end_file_idx_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("start file idx not in cur dir range", KR(ret), K(start_file_idx), K(dirs_[i]));
          }
        }
        if (FAILEDx(reinsert_files_into_flush_list_(start_file_idx, end_file_idx))) {
          LOG_ERROR("fail to reinsert files into flush list",
              KR(ret), K(i), K(dirs_[i]), K(start_file_idx), K(end_file_idx));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    cur_caching_list_is_meta_ = false;
    cur_caching_list_idx_ = FileList::L1;
    cur_iter_dir_idx_ = -1;
    cur_iter_file_idx_ = -1;
    cached_file_num_ = 0;
    cached_dir_num_ = 0;
    for (int64_t i = 0; i < files_.count(); ++i) {
      files_[i].reset();
    }
    for (int64_t i = 0; i < dirs_.count(); ++i) {
      dirs_[i].reset();
    }
  }

  return ret;
}

int ObTmpFileFlushListIterator::reset()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    if (OB_FAIL(clear())) {
      LOG_WARN("fail to clear", KR(ret));
    }
    is_inited_ = false;
  }
  return ret;
}

void ObTmpFileFlushListIterator::destroy()
{
  reset();
}

int ObTmpFileFlushListIterator::reinsert_files_into_flush_list_(const int64_t start_file_idx,
                                                                const int64_t end_file_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = start_file_idx; OB_SUCC(ret) && (i >= 0 && i <= end_file_idx); i++) {
    if (OB_UNLIKELY(i < 0 || i >= cached_file_num_ || cached_file_num_ > files_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid file idx", KR(ret), K(i), K(cached_file_num_), K(files_));
    } else if (OB_UNLIKELY(!files_[i].is_inited_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("uninitialized file is unexpected", KR(ret), K(i), K(files_[i]));
    } else {
      ObITmpFileHandle &file_handle = files_[i].file_handle_;
      if (OB_ISNULL(file_handle.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret));
      } else if (files_[i].is_meta_) {
        ObSharedNothingTmpFile *sn_file = nullptr;
        if (OB_UNLIKELY(file_handle.get()->get_mode() != ObITmpFile::ObTmpFileMode::SHARED_NOTHING)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected file mode", KR(ret), KPC(file_handle.get()));
        } else if (FALSE_IT(sn_file = static_cast<ObSharedNothingTmpFile *>(file_handle.get()))) {
        } else if (sn_file->is_in_meta_flush_list()) {
          // do nothing, because meta flush node may be re-inserted after
          // tmp file insert meta tree item; do not handle data flush node here
          // because data node will not be re-inserted during flushing procedure
        } else if (OB_FAIL(sn_file->reinsert_meta_flush_node())) {
          LOG_WARN("fail to reinsert flush node", KR(ret), K(files_[i]));
        }
      } else if (OB_FAIL(file_handle.get()->reinsert_data_flush_node())) {
        LOG_WARN("fail to reinsert flush node", KR(ret), K(files_[i]));
      }
    }
  }
  return ret;
}

int ObTmpFileFlushListIterator::next(const FlushCtxState iter_stage, ObITmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  FlushCtxState cur_stage = FlushCtxState::FSM_FINISHED;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(dirs_.count() != MAX_CACHE_NUM ||
                         cached_dir_num_ > MAX_CACHE_NUM ||
                         cached_dir_num_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(files_.count()), K(cached_dir_num_));
  } else if (OB_UNLIKELY(files_.count() != MAX_CACHE_NUM ||
                         cached_file_num_ > MAX_CACHE_NUM ||
                         cached_file_num_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(files_.count()), K(cached_file_num_));
  } else if (OB_UNLIKELY(FlushCtxState::FSM_FINISHED <= iter_stage || iter_stage < FlushCtxState::FSM_F1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid iter_stage", KR(ret), K(iter_stage));
  } else if (FALSE_IT(cur_stage = cal_current_flush_stage_())) {
  } else if (OB_UNLIKELY(FlushCtxState::FSM_FINISHED <= cur_stage || FlushCtxState::FSM_F1 > cur_stage)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_stage));
  } else if (OB_UNLIKELY(cur_stage > iter_stage)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid iter_stage", KR(ret), K(iter_stage), K(cur_stage));
  } else if (cur_stage < iter_stage ||
             (FlushCtxState::FSM_F1 == cur_stage && cur_iter_file_idx_ == cached_file_num_) ||
             (FlushCtxState::FSM_F1 < cur_stage && cur_iter_dir_idx_ == cached_dir_num_)) {
    // code will run here when:
    // 1. expected iterating flush stage is over than current flush stage;
    // 2. all cached file has been iterated
    if (OB_FAIL(clear())) {
      LOG_WARN("fail to clear", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == cached_file_num_ && OB_FAIL(cache_files_(iter_stage))) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("fail to cache files", KR(ret));
    } else {
      LOG_WARN("fail to cache files", KR(ret));
    }
  } else if (OB_FAIL(check_cur_idx_status_())) {
    LOG_WARN("fail to check cur idx status", KR(ret));
  } else {
    file_handle = files_[cur_iter_file_idx_].file_handle_;
    if (FileList::L1 == cur_caching_list_idx_) {
      if (OB_FAIL(advance_big_file_idx_())) {
        LOG_WARN("fail to advance big file idx", KR(ret));
      }
    } else if (OB_FAIL(advance_small_file_idx_())) {
      LOG_WARN("fail to advance small file idx", KR(ret));
    }
  }
  LOG_DEBUG("try to get next file", KR(ret), K(iter_stage), K(cur_stage),
            K(cur_iter_file_idx_), K(cached_file_num_), K(file_handle));
  return ret;
}

int ObTmpFileFlushListIterator::cache_files_(const FlushCtxState iter_stage)
{
  int ret = OB_SUCCESS;
  FileList end_list_idx = FileList::MAX;
  const int64_t target_cache_file_num = iter_stage == FlushCtxState::FSM_F1 ?
                                        BIG_FILE_CACHE_NUM : MAX_CACHE_NUM;
  ObArray<ObITmpFileHandle> file_handles;

  if (OB_UNLIKELY(0 != cached_dir_num_ || 0 != cached_file_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cached num", KR(ret), K(cached_dir_num_), K(cached_file_num_));
  } else if (OB_FAIL(init_caching_list_with_flush_stage_(iter_stage))) {
    LOG_WARN("fail to init caching list", KR(ret), K(iter_stage));
  } else if (OB_FAIL(acquire_final_list_of_flush_stage_(iter_stage, end_list_idx))) {
    LOG_WARN("fail to acquire final list of flush stage", KR(ret), K(iter_stage));
  } else if (OB_FAIL(file_handles.prepare_allocate_and_keep_count(target_cache_file_num))) {
    LOG_WARN("fail to prepare allocate", KR(ret), K(target_cache_file_num));
  } else { // pop enough files from priority manager for caching
    int64_t remain_cache_file_num = target_cache_file_num;
    bool cache_over = false;
    while (OB_SUCC(ret) && !cache_over && remain_cache_file_num > 0) {
      int64_t actual_cache_file_num = 0;
      if (OB_FAIL(prio_mgr_->popN_from_file_list(cur_caching_list_is_meta_, cur_caching_list_idx_,
                                                 remain_cache_file_num, actual_cache_file_num,
                                                 file_handles))) {
        LOG_WARN("fail to pop N from file list", KR(ret), K(cur_caching_list_idx_));
      } else if (FALSE_IT(remain_cache_file_num -= actual_cache_file_num)) {
      } else if (OB_UNLIKELY(remain_cache_file_num < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cache file num", KR(ret), K(target_cache_file_num),
                 K(remain_cache_file_num), K(actual_cache_file_num));
      } else if (0 == remain_cache_file_num) { // cache enough files
        cache_over = true;
      } else { // remain_cache_file_num > 0
        if (cur_caching_list_idx_ == end_list_idx) { // has reached the last list of this flush stage
          if (file_handles.empty()) {
            ret = OB_ITER_END;
            LOG_DEBUG("iter end in current flush stage", KR(ret), K(iter_stage),
                      K(cur_caching_list_idx_), K(remain_cache_file_num));
          } else { // cache files successful, but the num is not enough
            cache_over = true;
          }
        } else if (OB_FAIL(advance_caching_list_idx_())) { // pop files of the next list of this flush stage
          LOG_WARN("fail to advance caching list idx", KR(ret));
        }
      }
    } // end while
  }

  if (OB_SUCC(ret)) {
    if (FileList::L1 == cur_caching_list_idx_) {
      if (OB_FAIL(cache_big_files_(file_handles))) {
        LOG_WARN("fail to cache big files", KR(ret));
      }
    } else if (OB_FAIL(cache_small_files_(file_handles))) {
      LOG_WARN("fail to cache big files", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
    cached_file_num_ = 0; // to guarantee that the iterator must not reinsert cached part files into priority list
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_LIKELY(OB_SUCCESS == tmp_ret) && i < file_handles.count(); ++i) {
      ObITmpFile *tmp_file = file_handles[i].get();
      if (OB_ISNULL(tmp_file)) {
        // could not happen, just skip
      } else if (cur_caching_list_is_meta_) {
        ObSharedNothingTmpFile *sn_file = nullptr;
        if (OB_UNLIKELY(tmp_file->get_mode() != ObITmpFile::ObTmpFileMode::SHARED_NOTHING)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected file mode", KR(ret), KPC(tmp_file));
        } else if (FALSE_IT(sn_file = static_cast<ObSharedNothingTmpFile *>(tmp_file))) {
        } else if (OB_FAIL(sn_file->reinsert_meta_flush_node())) {
          LOG_WARN("fail to reinsert flush node", KR(ret), K(files_[i]));
        }
      } else if (OB_TMP_FAIL(tmp_file->reinsert_data_flush_node())) {
        LOG_ERROR("fail to reinsert flush node", KR(tmp_ret), K(i), K(file_handles[i]));
      }
    }
  }
  return ret;
}

int ObTmpFileFlushListIterator::cache_big_files_(const ObArray<ObITmpFileHandle> &file_handles)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_file_wrappers_(file_handles))) {
    LOG_WARN("fail to build file wrappers", KR(ret));
  } else {
    cur_iter_file_idx_ = 0;
    cur_iter_dir_idx_ = -1; // no need to aggregate files into dir
    cached_dir_num_ = 0;
  }
  return ret;
}

int ObTmpFileFlushListIterator::cache_small_files_(const ObArray<ObITmpFileHandle> &file_handles)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_file_wrappers_(file_handles))) {
    LOG_WARN("fail to build file wrappers", KR(ret));
  } else if (OB_FAIL(build_dir_wrappers_())) {
    LOG_WARN("fail to build dir wrappers", KR(ret));
  } else {
    cur_iter_file_idx_ = dirs_[0].start_file_idx_;
    cur_iter_dir_idx_ = 0;
  }
  return ret;
}

int ObTmpFileFlushListIterator::build_file_wrappers_(const ObArray<ObITmpFileHandle> &file_handles)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_handles.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_handles.empty()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_handles.count(); ++i) {
      if (i >= files_.count()) {
        ret = OB_ERROR_OUT_OF_RANGE;
        LOG_ERROR("index is out of range", KR(ret), K(i), K(files_));
      } else if (OB_FAIL(files_[i].init(cur_caching_list_is_meta_, file_handles[i]))) {
        LOG_WARN("fail to init tmp file handle wrapper", KR(ret), K(i), K(file_handles[i]), K(files_[i]));

        for (int64_t j = 0; j < i; ++j) {
          files_[j].reset();
        }
      }
    }

    if (OB_SUCC(ret)) {
      lib::ob_sort(files_.begin(), files_.begin() + file_handles.count());
      cached_file_num_ = file_handles.count();
    }
  }
  return ret;
}

int ObTmpFileFlushListIterator::build_dir_wrappers_()
{
  int ret = OB_SUCCESS;
  int cached_dir_num = 0;

  if (OB_UNLIKELY(cached_file_num_ <= 0 || cached_file_num_ > files_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cached file num", KR(ret), K(cached_file_num_));
  } else { // we assume that files_ has been initialized and sorted
    int64_t dir_id = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
    int64_t start_file_idx = -1;
    int64_t end_file_idx = -1;
    int64_t page_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < cached_file_num_; ++i) {
      ObITmpFile *file = nullptr;
      int64_t file_dirty_page_num = 0;
      if (OB_UNLIKELY(!files_[i].is_inited_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tmp file wrapper is not inited", KR(ret), K(files_[i]));
      } else if (OB_ISNULL(file = files_[i].file_handle_.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file ptr is null", K(ret), KP(file));
      } else if (OB_FAIL(get_flushing_file_dirty_page_num_(*file, file_dirty_page_num))) {
        LOG_WARN("fail to get flushing file dirty page num", KR(ret));
      } else if (file->get_dir_id() != dir_id) {
        if (0 != i) {
          end_file_idx = i - 1;
          if (cached_dir_num >= dirs_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("cached_dir_num is equal or bigger than dirs_.count",
                KR(ret), K(cached_dir_num), K(dirs_));
          } else if (OB_FAIL(dirs_[cached_dir_num].init(cur_caching_list_is_meta_, page_num,
                                                 start_file_idx, end_file_idx))) {
            LOG_WARN("fail to init tmp file dir wrapper", KR(ret), K(i), K(dirs_[cached_dir_num]), K(page_num),
                                                          K(start_file_idx), K(end_file_idx));
          } else {
            cached_dir_num++;
          }
        }

        if (OB_SUCC(ret)) {
          dir_id = file->get_dir_id();
          start_file_idx = i;
          page_num = file_dirty_page_num;
        }
      } else {
        page_num += file_dirty_page_num;
      }

      if (OB_SUCC(ret) && cached_file_num_ - 1 == i) {
        end_file_idx = i;
        if (cached_dir_num >= dirs_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("cached_dir_num is equal or bigger than dirs_.count",
              KR(ret), K(cached_dir_num), K(dirs_));
        } else if (OB_FAIL(dirs_[cached_dir_num].init(cur_caching_list_is_meta_, page_num,
                                               start_file_idx, end_file_idx))) {
          LOG_WARN("fail to init tmp file dir wrapper", KR(ret), K(page_num),
                                                        K(start_file_idx), K(end_file_idx));
        } else {
          cached_dir_num++;
        }
      }
    } // end for

  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < cached_dir_num; i++) {
      dirs_[i].reset();
    }
    for (int64_t i = 0; i < cached_file_num_; i++) {
      files_[i].reset();
      cached_file_num_ = 0;
    }
  } else {
    lib::ob_sort(dirs_.begin(), dirs_.begin() + cached_dir_num);
    cached_dir_num_ = cached_dir_num;
  }

  return ret;
}

int ObTmpFileFlushListIterator::get_flushing_file_dirty_page_num_(const ObITmpFile &file, int64_t &page_num)
{
  int ret = OB_SUCCESS;
  FlushCtxState cur_stage = cal_current_flush_stage_();
  if (OB_UNLIKELY(cur_stage < FlushCtxState::FSM_F1 || cur_stage > FlushCtxState::FSM_F5)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected flush stage", KR(ret), K(cur_stage));
  } else if (cur_stage <= FlushCtxState::FSM_F3) {
    ObITmpFile &mutable_file_ref = const_cast<ObITmpFile &>(file);
    int64_t dirty_page_size = mutable_file_ref.get_dirty_data_page_size_with_lock();
    page_num = upper_align(dirty_page_size, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
  } else if (OB_UNLIKELY(file.get_mode() != ObITmpFile::ObTmpFileMode::SHARED_NOTHING)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file mode", KR(ret), K(file));
  } else { // stage > FlushCtxState::FSM_F3 is flushing meta page
    int64_t non_rightmost_dirty_page_num = 0;
    int64_t rightmost_dirty_page_num = 0;
    ObSharedNothingTmpFile &mutable_file_ref = static_cast<ObSharedNothingTmpFile &>(const_cast<ObITmpFile &>(file));
    mutable_file_ref.get_dirty_meta_page_num_with_lock(non_rightmost_dirty_page_num, rightmost_dirty_page_num);
    if (cur_stage == FlushCtxState::FSM_F4) {
      page_num = non_rightmost_dirty_page_num;
    } else if (cur_stage == FlushCtxState::FSM_F5) {
      page_num = rightmost_dirty_page_num;
    }
  }
  return ret;
}

int ObTmpFileFlushListIterator::advance_caching_list_idx_()
{
  int ret = OB_SUCCESS;
  if (!cur_caching_list_is_meta_ && FileList::L5 == cur_caching_list_idx_) {
    cur_caching_list_is_meta_ = true;
    cur_caching_list_idx_ = FileList::L1;
  } else {
    switch(cur_caching_list_idx_) {
      case FileList::L1:
        cur_caching_list_idx_ = FileList::L2;
        break;
      case FileList::L2:
        cur_caching_list_idx_ = FileList::L3;
        break;
      case FileList::L3:
        cur_caching_list_idx_ = FileList::L4;
        break;
      case FileList::L4:
        cur_caching_list_idx_ = FileList::L5;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(cur_caching_list_idx_));
        break;
    }
  }

  return ret;
}

int ObTmpFileFlushListIterator::check_cur_idx_status_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_iter_file_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_file_idx_));
  } else if (FileList::L1 == cur_caching_list_idx_) {
    // the file in L1 list will not be flushed with an aggregating dir.
    // thus, it is no need to check dir
    if (OB_UNLIKELY(cur_iter_file_idx_ >= cached_file_num_ || cached_file_num_ > files_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", KR(ret), K(cur_iter_file_idx_), K(cached_file_num_));
    }
  } else if (OB_UNLIKELY(cur_iter_dir_idx_ < 0 || cur_iter_dir_idx_ >= cached_dir_num_ ||
        cached_dir_num_ > dirs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_dir_idx_), K(cached_dir_num_), K(dirs_));
  } else if (OB_UNLIKELY(!dirs_[cur_iter_dir_idx_].is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_dir_idx_), K(dirs_[cur_iter_dir_idx_]));
  } else if (OB_UNLIKELY(cur_iter_file_idx_ < dirs_[cur_iter_dir_idx_].start_file_idx_ ||
                         cur_iter_file_idx_ > dirs_[cur_iter_dir_idx_].end_file_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_dir_idx_), K(cur_iter_file_idx_),
              K(dirs_[cur_iter_dir_idx_]));
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!files_[cur_iter_file_idx_].is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_file_idx_), K(files_[cur_iter_file_idx_]));
  }

  return ret;
}

int ObTmpFileFlushListIterator::advance_big_file_idx_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(FileList::L1 != cur_caching_list_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid caching list idx", KR(ret), K(cur_caching_list_idx_));
  } else if (OB_UNLIKELY(cur_iter_file_idx_ < 0 || cur_iter_file_idx_ >= cached_file_num_ ||
        cached_file_num_ > files_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file idx", KR(ret), K(cur_iter_file_idx_), K(cached_file_num_), K(files_));
  } else {
    files_[cur_iter_file_idx_].reset();
    cur_iter_file_idx_++;
  }
  return ret;
}

int ObTmpFileFlushListIterator::advance_small_file_idx_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_caching_list_idx_ <= FileList::L1 ||
                  cur_caching_list_idx_ > FileList::L5)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid caching list idx", KR(ret), K(cur_caching_list_idx_));
  } else if (OB_UNLIKELY(cur_iter_dir_idx_ < 0 || cur_iter_dir_idx_ >= cached_dir_num_ ||
        cached_dir_num_ > dirs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dir idx", KR(ret), K(cur_iter_dir_idx_), K(cached_dir_num_), K(dirs_));
  } else if (OB_UNLIKELY(cur_iter_file_idx_ < 0 ||  cur_iter_file_idx_ >= cached_file_num_ ||
        cached_file_num_ > files_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_file_idx_), K(cached_file_num_));
  } else if (OB_UNLIKELY(cur_iter_file_idx_ < dirs_[cur_iter_dir_idx_].start_file_idx_ ||
                         cur_iter_file_idx_ > dirs_[cur_iter_dir_idx_].end_file_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file idx", KR(ret), K(cur_iter_file_idx_), K(dirs_[cur_iter_dir_idx_]));
  } else {
    files_[cur_iter_file_idx_].reset();
    cur_iter_file_idx_++;
    if (cur_iter_file_idx_ > dirs_[cur_iter_dir_idx_].end_file_idx_) {
      if (OB_FAIL(advance_dir_idx_())) {
        LOG_WARN("fail to advance dir idx", KR(ret));
      } else if (cur_iter_dir_idx_ < cached_dir_num_) {
        if (OB_UNLIKELY(!dirs_[cur_iter_dir_idx_].is_inited_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status", KR(ret), K(cur_iter_dir_idx_), K(dirs_[cur_iter_dir_idx_]));
        } else {
          cur_iter_file_idx_ = dirs_[cur_iter_dir_idx_].start_file_idx_;
        }
      } else {
        // iter end
      }
    }
  }
  return ret;
}

int ObTmpFileFlushListIterator::advance_dir_idx_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(FileList::L1 == cur_caching_list_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid caching list idx", KR(ret), K(cur_caching_list_idx_));
  } else if (OB_UNLIKELY(cur_iter_dir_idx_ < 0 || cur_iter_dir_idx_ >= cached_dir_num_ ||
        cached_dir_num_ > dirs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dir idx", KR(ret), K(cur_iter_dir_idx_), K(cached_dir_num_), K(dirs_));
  } else {
    dirs_[cur_iter_dir_idx_].reset();
    cur_iter_dir_idx_++;
  }

  return ret;
}

FlushCtxState ObTmpFileFlushListIterator::cal_current_flush_stage_()
{
  FlushCtxState stage = FlushCtxState::FSM_FINISHED;
  if (OB_UNLIKELY(cur_caching_list_idx_ < FileList::L1 ||
                  cur_caching_list_idx_ > FileList::L5)) {
    // stage = FlushCtxState::FSM_FINISHED;
  } else if (!cur_caching_list_is_meta_) {
    if (cur_caching_list_idx_ == FileList::L1) {
      stage = FlushCtxState::FSM_F1;
    } else if (cur_caching_list_idx_ <= FileList::L4) {
      stage = FlushCtxState::FSM_F2;
    } else if (cur_caching_list_idx_ == FileList::L5) {
      stage = FlushCtxState::FSM_F3;
    }
  } else {
    if (cur_caching_list_idx_ <= FileList::L4) {
      stage = FlushCtxState::FSM_F4;
    } else if (cur_caching_list_idx_ == FileList::L5) {
      stage = FlushCtxState::FSM_F5;
    }
  }

  return stage;
}

int ObTmpFileFlushListIterator::init_caching_list_with_flush_stage_(const FlushCtxState iter_stage)
{
  int ret = OB_SUCCESS;
  if (cal_current_flush_stage_() == iter_stage) {
    // no need to change caching list
  } else if (iter_stage == FlushCtxState::FSM_F1) {
    cur_caching_list_is_meta_ = false;
    cur_caching_list_idx_ = FileList::L1;
  } else if (iter_stage == FlushCtxState::FSM_F2) {
    cur_caching_list_is_meta_ = false;
    cur_caching_list_idx_ = FileList::L2;
  } else if (iter_stage == FlushCtxState::FSM_F3) {
    cur_caching_list_is_meta_ = false;
    cur_caching_list_idx_ = FileList::L5;
  } else if (iter_stage == FlushCtxState::FSM_F4) {
    cur_caching_list_is_meta_ = true;
    cur_caching_list_idx_ = FileList::L1;
  } else if (iter_stage == FlushCtxState::FSM_F5) {
    cur_caching_list_is_meta_ = true;
    cur_caching_list_idx_ = FileList::L5;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stage", KR(ret), K(iter_stage));
  }

  return ret;
}

int ObTmpFileFlushListIterator::acquire_final_list_of_flush_stage_(const FlushCtxState iter_stage,
                                                                   FileList &list_idx)
{
  int ret = OB_SUCCESS;
  switch(iter_stage) {
    case FlushCtxState::FSM_F1:
      list_idx = FileList::L1;
      break;
    case FlushCtxState::FSM_F2:
      list_idx = FileList::L4;
      break;
    case FlushCtxState::FSM_F3:
      list_idx = FileList::L5;
      break;
    case FlushCtxState::FSM_F4:
      list_idx = FileList::L4;
      break;
    case FlushCtxState::FSM_F5:
      list_idx = FileList::L5;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid stage", KR(ret), K(iter_stage));
  }
  return ret;
}

int ObTmpFileFlushListIterator::ObFlushingTmpFileWrapper::init(const bool is_meta, const ObITmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(file_handle.get())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_handle.get()));
  } else {
    is_meta_ = is_meta;
    file_handle_ = file_handle;
    is_inited_ = true;
  }
  return ret;
}

void ObTmpFileFlushListIterator::ObFlushingTmpFileWrapper::reset()
{
  if (IS_INIT) {
    is_inited_ = false;
    is_meta_ = false;
    file_handle_.reset();
  }
}

bool ObTmpFileFlushListIterator::ObFlushingTmpFileWrapper::operator <(const ObFlushingTmpFileWrapper &other)
{
  int ret = OB_SUCCESS;
  bool b_ret = false;
  if (OB_UNLIKELY(!other.is_inited_ || !is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(other), KPC(this));
  } else if (OB_ISNULL(other.file_handle_.get()) || OB_ISNULL(file_handle_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to compare file handle with nullptr", KR(ret), K(file_handle_), K(other.file_handle_));
  } else if (!is_meta_ && other.is_meta_) {
    b_ret = true;
  } else if (is_meta_ && !other.is_meta_) {
    b_ret = false;
  } else if (file_handle_.get()->get_dir_id() < other.file_handle_.get()->get_dir_id()) {
    b_ret = true;
  } else if (file_handle_.get()->get_dir_id() > other.file_handle_.get()->get_dir_id()) {
    b_ret = false;
  } else if (is_meta_) {
    if (OB_UNLIKELY(file_handle_.get()->get_mode() != ObITmpFile::ObTmpFileMode::SHARED_NOTHING ||
                    other.file_handle_.get()->get_mode() != ObITmpFile::ObTmpFileMode::SHARED_NOTHING)) {
      ret = OB_ERR_UNEXPECTED;
      b_ret = false;
      LOG_ERROR("unexpected file mode", KR(ret), K(file_handle_), K(other.file_handle_));
    } else {
      ObSharedNothingTmpFile *sn_file = static_cast<ObSharedNothingTmpFile *>(file_handle_.get());
      ObSharedNothingTmpFile *sn_other_file = static_cast<ObSharedNothingTmpFile *>(other.file_handle_.get());
      b_ret = sn_file->get_meta_page_flush_level() < sn_other_file->get_meta_page_flush_level();
    }
  } else {
    b_ret = file_handle_.get()->get_data_page_flush_level() < other.file_handle_.get()->get_data_page_flush_level();
  }
  return b_ret;
}

int ObTmpFileFlushListIterator::ObFlushingTmpFileDirWrapper::init(const bool is_meta, const int64_t page_num,
                                                                  const int64_t start_file_idx,
                                                                  const int64_t end_file_idx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(page_num < 0 || start_file_idx < 0 || start_file_idx > end_file_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(page_num), K(start_file_idx), K(end_file_idx));
  } else {
    is_inited_ = true;
    is_meta_ = is_meta;
    page_num_ = page_num;
    start_file_idx_ = start_file_idx;
    end_file_idx_ = end_file_idx;
  }
  return ret;
}

void ObTmpFileFlushListIterator::ObFlushingTmpFileDirWrapper::reset()
{
  if (IS_INIT) {
    is_meta_ = false;
    page_num_ = 0;
    start_file_idx_ = -1;
    end_file_idx_ = -1;
    is_inited_ = false;
  }
}

bool ObTmpFileFlushListIterator::ObFlushingTmpFileDirWrapper::operator <(const ObFlushingTmpFileDirWrapper &other)
{
  int ret = OB_SUCCESS;
  bool b_ret = false;
  if (OB_UNLIKELY(!other.is_inited_ || !is_inited_)) {
    LOG_WARN("unexpected status", K(other), KPC(this));
  } else if (!is_meta_ && other.is_meta_) {
    b_ret = true;
  } else if (is_meta_ && !other.is_meta_) {
    b_ret = false;
  } else {
    b_ret = page_num_ > other.page_num_;
  }
  return b_ret;
}
}  // end namespace tmp_file
}  // end namespace oceanbase
