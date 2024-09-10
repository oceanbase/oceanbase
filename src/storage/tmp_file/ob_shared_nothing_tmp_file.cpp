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

#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_eviction_manager.h"
#include "storage/tmp_file/ob_tmp_file_flush_priority_manager.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_tmp_file_flush_manager.h"
#include "storage/tmp_file/ob_tmp_file_io_ctx.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/random/ob_random.h"
#include "storage/tmp_file/ob_tmp_file_page_cache_controller.h"

namespace oceanbase
{
namespace tmp_file
{

int ObSNTmpFileInfo::init(
    const ObCurTraceId::TraceId &trace_id,
    const uint64_t tenant_id,
    const int64_t dir_id,
    const int64_t fd,
    const int64_t file_size,
    const int64_t truncated_offset,
    const bool is_deleting,
    const int64_t cached_page_num,
    const int64_t write_back_data_page_num,
    const int64_t flushed_data_page_num,
    const int64_t ref_cnt,
    const int64_t write_req_cnt,
    const int64_t unaligned_write_req_cnt,
    const int64_t read_req_cnt,
    const int64_t unaligned_read_req_cnt,
    const int64_t total_read_size,
    const int64_t last_access_ts,
    const int64_t last_modify_ts,
    const int64_t birth_ts,
    const void* const tmp_file_ptr,
    const char* const label)
{
  int ret = OB_SUCCESS;
  trace_id_ = trace_id;
  tenant_id_ = tenant_id;
  dir_id_ = dir_id;
  fd_ = fd;
  file_size_ = file_size;
  truncated_offset_ = truncated_offset;
  is_deleting_ = is_deleting;
  cached_data_page_num_ = cached_page_num;
  write_back_data_page_num_ = write_back_data_page_num;
  flushed_data_page_num_ = flushed_data_page_num;
  ref_cnt_ = ref_cnt;
  write_req_cnt_ = write_req_cnt;
  unaligned_write_req_cnt_ = unaligned_write_req_cnt;
  read_req_cnt_ = read_req_cnt;
  unaligned_read_req_cnt_ = unaligned_read_req_cnt;
  total_read_size_ = total_read_size;
  last_access_ts_ = last_access_ts;
  last_modify_ts_ = last_modify_ts;
  birth_ts_ = birth_ts;
  tmp_file_ptr_ = tmp_file_ptr;
  if (NULL != label) {
    label_.assign_strive(label);
  }
  return ret;
}

void ObSNTmpFileInfo::reset()
{
  trace_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  dir_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  file_size_ = 0;
  truncated_offset_ = 0;
  is_deleting_ = false;
  cached_data_page_num_ = 0;
  write_back_data_page_num_ = 0;
  flushed_data_page_num_ = 0;
  ref_cnt_ = 0;
  write_req_cnt_ = 0;
  unaligned_write_req_cnt_ = 0;
  read_req_cnt_ = 0;
  unaligned_read_req_cnt_ = 0;
  total_read_size_ = 0;
  last_access_ts_ = -1;
  last_modify_ts_ = -1;
  birth_ts_ = -1;
  tmp_file_ptr_ = nullptr;
  label_.reset();
  meta_tree_epoch_ = 0;
  meta_tree_level_cnt_ = 0;
  meta_size_ = 0;
  cached_meta_page_num_ = 0;
  write_back_meta_page_num_ = 0;
  all_type_page_flush_cnt_ = 0;
}

ObTmpFileHandle::ObTmpFileHandle(ObSharedNothingTmpFile *tmp_file)
  : ptr_(tmp_file)
{
  if (ptr_ != nullptr) {
    ptr_->inc_ref_cnt();
  }
}

ObTmpFileHandle::ObTmpFileHandle(const ObTmpFileHandle &handle)
  : ptr_(nullptr)
{
  operator=(handle);
}

ObTmpFileHandle & ObTmpFileHandle::operator=(const ObTmpFileHandle &other)
{
  if (other.get() != ptr_) {
    reset();
    ptr_ = other.get();
    if (ptr_ != nullptr) {
      ptr_->inc_ref_cnt();
    }
  }
  return *this;
}

void ObTmpFileHandle::reset()
{
  if (ptr_ != nullptr) {
    ptr_->dec_ref_cnt();
    if (ptr_->get_ref_cnt() == 0) {
      ptr_->~ObSharedNothingTmpFile();
    }
    ptr_ = nullptr;
  }
}

int ObTmpFileHandle::init(ObSharedNothingTmpFile *tmp_file)
{
  int ret = OB_SUCCESS;

  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(ptr_));
  } else if (OB_ISNULL(tmp_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(tmp_file));
  } else {
    ptr_ = tmp_file;
    ptr_->inc_ref_cnt();
  }

  return ret;
}

void ObSharedNothingTmpFile::InnerFlushContext::reset()
{
  meta_finished_continuous_flush_info_num_ = 0;
  data_finished_continuous_flush_info_num_ = 0;
  meta_flush_infos_.reset();
  data_flush_infos_.reset();
  flush_seq_ = ObTmpFileGlobal::INVALID_FLUSH_SEQUENCE;
  need_to_wait_for_the_previous_data_flush_req_to_complete_ = false;
  need_to_wait_for_the_previous_meta_flush_req_to_complete_ = false;
}

int ObSharedNothingTmpFile::InnerFlushContext::update_finished_continuous_flush_info_num(const bool is_meta, const int64_t end_pos)
{
  int ret = OB_SUCCESS;
  if (is_meta) {
    if (OB_UNLIKELY(end_pos < meta_finished_continuous_flush_info_num_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected flush info num", K(ret), K(end_pos), K(meta_finished_continuous_flush_info_num_));
    } else {
      meta_finished_continuous_flush_info_num_ = end_pos;
    }
  } else {
    if (OB_UNLIKELY(end_pos < data_finished_continuous_flush_info_num_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected flush info num", K(ret), K(end_pos), K(data_finished_continuous_flush_info_num_));
    } else {
      data_finished_continuous_flush_info_num_ = end_pos;
    }
  }
  return ret;
}

ObSharedNothingTmpFile::InnerFlushInfo::InnerFlushInfo()
  : update_meta_data_done_(false),
    flush_data_page_disk_begin_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
    flush_data_page_num_(-1),
    flush_virtual_page_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
    file_size_(0),
    flush_meta_page_array_()
{
  flush_meta_page_array_.set_attr(ObMemAttr(MTL_ID(), "TFFlushMetaArr"));
}

void ObSharedNothingTmpFile::InnerFlushInfo::reset()
{
  update_meta_data_done_ = false;
  flush_data_page_disk_begin_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  flush_data_page_num_ = -1;
  flush_virtual_page_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  file_size_ = 0;
  flush_meta_page_array_.reset();
}

int ObSharedNothingTmpFile::InnerFlushInfo::init_by_tmp_file_flush_info(const ObTmpFileFlushInfo& flush_info)
{
  int ret = OB_SUCCESS;
  flush_data_page_disk_begin_id_ = flush_info.flush_data_page_disk_begin_id_;
  flush_data_page_num_ = flush_info.flush_data_page_num_;
  flush_virtual_page_id_ = flush_info.flush_virtual_page_id_;
  file_size_ = flush_info.file_size_;
  if (!flush_info.flush_meta_page_array_.empty()
      && OB_FAIL(flush_meta_page_array_.assign(flush_info.flush_meta_page_array_))) {
    LOG_WARN("fail to assign flush_meta_page_array_", KR(ret), K(flush_info));
  }
  return ret;
}

ObSharedNothingTmpFile::ObSharedNothingTmpFile()
    : tmp_file_block_manager_(nullptr),
      callback_allocator_(nullptr),
      page_cache_controller_(nullptr),
      flush_prio_mgr_(nullptr),
      eviction_mgr_(nullptr),
      wbp_(nullptr),
      page_idx_cache_(),
      is_inited_(false),
      is_deleting_(false),
      is_in_data_eviction_list_(false),
      is_in_meta_eviction_list_(false),
      data_page_flush_level_(-1),
      meta_page_flush_level_(-1),
      tenant_id_(OB_INVALID_TENANT_ID),
      dir_id_(ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
      fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
      ref_cnt_(0),
      truncated_offset_(0),
      read_offset_(0),
      file_size_(0),
      flushed_data_page_num_(0),
      write_back_data_page_num_(0),
      cached_page_nums_(0),
      begin_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      begin_page_virtual_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
      flushed_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      flushed_page_virtual_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
      end_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      meta_tree_(),
      data_flush_node_(*this),
      meta_flush_node_(*this),
      data_eviction_node_(*this),
      meta_eviction_node_(*this),
      meta_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      last_page_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      multi_write_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      truncate_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      inner_flush_ctx_(),
      trace_id_(),
      write_req_cnt_(0),
      unaligned_write_req_cnt_(0),
      read_req_cnt_(0),
      unaligned_read_req_cnt_(0),
      total_read_size_(0),
      last_access_ts_(-1),
      last_modify_ts_(-1),
      birth_ts_(-1),
      label_()
{
}

ObSharedNothingTmpFile::~ObSharedNothingTmpFile()
{
  destroy();
}

int ObSharedNothingTmpFile::init(const uint64_t tenant_id, const int64_t fd, const int64_t dir_id,
                                 ObTmpFileBlockManager *block_manager,
                                 ObIAllocator *callback_allocator,
                                 ObIAllocator *wbp_index_cache_allocator,
                                 ObIAllocator *wbp_index_cache_bkt_allocator,
                                 ObTmpFilePageCacheController *pc_ctrl,
                                 const char* label)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (!is_valid_tenant_id(tenant_id) || ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
             ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID == dir_id ||
             OB_ISNULL(block_manager) || OB_ISNULL(callback_allocator) ||
             OB_ISNULL(wbp_index_cache_allocator) || OB_ISNULL(wbp_index_cache_bkt_allocator) ||
             OB_ISNULL(pc_ctrl)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(fd), K(dir_id),
                                 KP(block_manager), KP(callback_allocator), KP(pc_ctrl),
                                 KP(wbp_index_cache_allocator), KP(wbp_index_cache_bkt_allocator));
  } else if (OB_FAIL(page_idx_cache_.init(fd, &pc_ctrl->get_write_buffer_pool(),
                                          wbp_index_cache_allocator, wbp_index_cache_bkt_allocator))) {
    LOG_WARN("fail to init page idx array", KR(ret), K(fd));
  } else if (OB_FAIL(meta_tree_.init(fd, &pc_ctrl->get_write_buffer_pool(), callback_allocator))) {
    LOG_WARN("fail to init meta tree", KR(ret), K(fd));
  } else {
    is_inited_ = true;
    tmp_file_block_manager_ = block_manager;
    callback_allocator_ = callback_allocator;
    page_cache_controller_ = pc_ctrl;
    eviction_mgr_ = &pc_ctrl->get_eviction_manager();
    flush_prio_mgr_ = &pc_ctrl->get_flush_priority_mgr();
    wbp_ = &pc_ctrl->get_write_buffer_pool();
    tenant_id_ = tenant_id;
    dir_id_ = dir_id;
    fd_ = fd;
    ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
    if (nullptr != cur_trace_id) {
      trace_id_ = *cur_trace_id;
    } else {
      trace_id_.init(GCONF.self_addr_);
    }
    last_access_ts_ = ObTimeUtility::current_time();
    last_modify_ts_ = ObTimeUtility::current_time();
    birth_ts_ = ObTimeUtility::current_time();
    if (NULL != label) {
      label_.assign_strive(label);
    }
  }

  LOG_INFO("tmp file init over", KR(ret), K(fd), K(dir_id));
  return ret;
}

int ObSharedNothingTmpFile::destroy()
{
  int ret = OB_SUCCESS;
  int64_t fd_backup = fd_;
  int64_t free_cnt = 0;

  LOG_INFO("tmp file destroy start", KR(ret), K(fd_), KPC(this));

  if (cached_page_nums_ > 0) {
    uint32_t cur_page_id = begin_page_id_;
    while (OB_SUCC(ret) && cur_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("begin page virtual id is invalid", KR(ret), K(fd_), K(begin_page_virtual_id_));
      } else if (OB_FAIL(wbp_->free_page(fd_, cur_page_id, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
        LOG_WARN("fail to free page", KR(ret), K(fd_), K(cur_page_id), K(begin_page_virtual_id_));
      } else {
        free_cnt++;
        cur_page_id = next_page_id;
        begin_page_virtual_id_ += 1;
      }
    }
  }
  if (OB_SUCC(ret) && cached_page_nums_ != free_cnt) {
    LOG_ERROR("tmp file destroy, cached_page_nums_ and free_cnt are not equal", KR(ret), K(fd_), K(free_cnt), KPC(this));
  }

  LOG_INFO("tmp file destroy, free wbp page phase over", KR(ret), K(fd_), KPC(this));

  if (FAILEDx(meta_tree_.clear(truncated_offset_, file_size_))) {
    LOG_WARN("fail to clear", KR(ret), K(fd_), K(truncated_offset_), K(file_size_));
  }

  LOG_INFO("tmp file destroy, meta_tree_ clear phase over", KR(ret), K(fd_), KPC(this));

  if (OB_SUCC(ret)) {
    reset();
  }

  LOG_INFO("tmp file destroy over", KR(ret), "fd", fd_backup);
  return ret;
}

void ObSharedNothingTmpFile::reset()
{
  tmp_file_block_manager_ = nullptr;
  callback_allocator_ = nullptr;
  page_cache_controller_ = nullptr;
  flush_prio_mgr_ = nullptr;
  eviction_mgr_ = nullptr;
  wbp_ = nullptr;
  page_idx_cache_.destroy();
  is_inited_ = false;
  is_deleting_ = false;
  is_in_data_eviction_list_ = false;
  is_in_meta_eviction_list_ = false;
  data_page_flush_level_ = -1;
  meta_page_flush_level_ = -1;
  tenant_id_ = OB_INVALID_TENANT_ID;
  dir_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  ref_cnt_ = 0;
  truncated_offset_ = 0;
  read_offset_ = 0;
  file_size_ = 0;
  flushed_data_page_num_ = 0;
  write_back_data_page_num_ = 0;
  cached_page_nums_ = 0;
  begin_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  meta_tree_.reset();
  data_flush_node_.unlink();
  meta_flush_node_.unlink();
  data_eviction_node_.unlink();
  meta_eviction_node_.unlink();
  inner_flush_ctx_.reset();
  /******for virtual table begin******/
  trace_id_.reset();
  write_req_cnt_ = 0;
  unaligned_write_req_cnt_ = 0;
  read_req_cnt_ = 0;
  unaligned_read_req_cnt_ = 0;
  total_read_size_ = 0;
  last_access_ts_ = -1;
  last_modify_ts_ = -1;
  birth_ts_ = -1;
  label_.reset();
  /******for virtual table end******/
}

bool ObSharedNothingTmpFile::is_deleting()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return is_deleting_;
}

bool ObSharedNothingTmpFile::can_remove()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return is_deleting_ && get_ref_cnt() == 1;
}

int ObSharedNothingTmpFile::delete_file()
{
  int ret = OB_SUCCESS;
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (OB_UNLIKELY(is_deleting_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the file is deleting", KR(ret), KPC(this));
  } else if (OB_FAIL(eviction_mgr_->remove_file(*this))) {
    LOG_WARN("fail to remove file from eviction manager", KR(ret), KPC(this));
  } else if (OB_FAIL(flush_prio_mgr_->remove_file(*this))) {
    LOG_WARN("fail to remove file from flush priority manager", KR(ret),KPC(this));
  } else {
    // read, write, truncate, flush and evict function will fail when is_deleting_ == true.
    is_deleting_ = true;
  }

  LOG_INFO("tmp file delete", KR(ret), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::aio_pread(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedNothingTmpFile has not been inited", KR(ret), K(tenant_id_), KPC(this));
  } else {
    common::TCRWLock::RLockGuard guard(meta_lock_);
    if (io_ctx.get_read_offset_in_file() < 0) {
      io_ctx.set_read_offset_in_file(read_offset_);
    }
    if (0 != io_ctx.get_read_offset_in_file() % ObTmpFileGlobal::PAGE_SIZE
        || 0 != io_ctx.get_todo_size() % ObTmpFileGlobal::PAGE_SIZE) {
      io_ctx.set_is_unaligned_read(true);
    }

    LOG_DEBUG("start to inner read tmp file", K(fd_), K(io_ctx.get_read_offset_in_file()),
                                              K(io_ctx.get_todo_size()), K(io_ctx.get_done_size()), KPC(this));
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
    } else if (OB_UNLIKELY(is_deleting_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("attempt to read a deleting file", KR(ret), K(fd_));
    } else if (OB_UNLIKELY(io_ctx.get_read_offset_in_file() >= file_size_)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", KR(ret), K(fd_), K(file_size_), K(io_ctx));
    } else if (io_ctx.get_read_offset_in_file() < truncated_offset_ &&
              OB_FAIL(inner_read_truncated_part_(io_ctx))) {
      LOG_WARN("fail to read truncated part", KR(ret), K(fd_), K(io_ctx));
    } else if (OB_UNLIKELY(io_ctx.get_todo_size() == 0)) {
      // do nothing
    } else {
      // Iterate to read disk data.
      int64_t wbp_begin_offset = cal_wbp_begin_offset_();
      if (OB_UNLIKELY(wbp_begin_offset < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected wbp begin offset", KR(ret), K(fd_), K(wbp_begin_offset), K(io_ctx));
      } else if (io_ctx.get_read_offset_in_file() < wbp_begin_offset) {
        const int64_t expected_read_disk_size = MIN(io_ctx.get_todo_size(),
                                                    wbp_begin_offset - io_ctx.get_read_offset_in_file());

        if (OB_UNLIKELY(expected_read_disk_size < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected read disk size", KR(ret), K(fd_), K(expected_read_disk_size), K(wbp_begin_offset), K(io_ctx));
        } else if (expected_read_disk_size == 0) {
          // do nothing
        } else if (OB_FAIL(inner_read_from_disk_(expected_read_disk_size, io_ctx))) {
          LOG_WARN("fail to read tmp file from disk", KR(ret), K(fd_), K(expected_read_disk_size),
                  K(wbp_begin_offset), K(io_ctx));
        } else {
          LOG_DEBUG("finish disk read", K(fd_), K(io_ctx.get_read_offset_in_file()),
                                        K(io_ctx.get_todo_size()),
                                        K(io_ctx.get_done_size()),
                                        K(wbp_begin_offset), K(expected_read_disk_size));
        }
      }

      // Iterate to read memory data (in write buffer pool).
      if (OB_SUCC(ret) && io_ctx.get_todo_size() > 0) {
        if (OB_UNLIKELY(0 == cached_page_nums_)) {
          ret = OB_ITER_END;
          LOG_WARN("iter end", KR(ret), K(fd_), K(io_ctx));
        } else if (OB_FAIL(inner_read_from_wbp_(io_ctx))) {
          LOG_WARN("fail to read tmp file from wbp", KR(ret), K(fd_), K(io_ctx));
        } else {
          LOG_DEBUG("finish wbp read", K(fd_), K(io_ctx.get_read_offset_in_file()),
                                       K(io_ctx.get_todo_size()),
                                       K(io_ctx.get_done_size()));
        }
      }
    }

    LOG_DEBUG("inner read finish once", KR(ret), K(fd_),
                                        K(io_ctx.get_read_offset_in_file()),
                                        K(io_ctx.get_todo_size()),
                                        K(io_ctx.get_done_size()), KPC(this));
  }
  return ret;
}

int ObSharedNothingTmpFile::inner_read_truncated_part_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(io_ctx.get_read_offset_in_file() >= truncated_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read offset should be less than truncated offset", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid io_ctx", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_UNLIKELY(io_ctx.get_todo_size() == 0)) {
    // do nothing
  } else {
    int64_t read_size = MIN(truncated_offset_ - io_ctx.get_read_offset_in_file(),
                            io_ctx.get_todo_size());
    char *read_buf = io_ctx.get_todo_buffer();
    if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(read_buf, read_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid buf range", KR(ret), K(fd_), K(read_buf), K(read_size), K(io_ctx));
    } else if (FALSE_IT(MEMSET(read_buf, 0, read_size))) {
    } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(read_size));
    } else if (OB_UNLIKELY(io_ctx.get_todo_size() > 0 &&
                           truncated_offset_ == file_size_)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", KR(ret), K(fd_), K(file_size_), K(truncated_offset_), K(io_ctx));
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_read_from_disk_(const int64_t expected_read_disk_size,
                                                  ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObSharedNothingTmpFileDataItem> data_items;
  if (OB_FAIL(meta_tree_.search_data_items(io_ctx.get_read_offset_in_file(),
                                           expected_read_disk_size, data_items))) {
    LOG_WARN("fail to search data items", KR(ret), K(fd_), K(expected_read_disk_size), K(io_ctx));
  } else if (OB_UNLIKELY(data_items.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no data item found", KR(ret), K(fd_), K(expected_read_disk_size), K(io_ctx));
  }

  // Iterate to read each block.
  int64_t remain_read_size = expected_read_disk_size;
  int64_t actual_read_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < data_items.count() && 0 < remain_read_size; i++) {
    const int64_t block_index = data_items[i].block_index_;
    const int64_t read_start_virtual_page_id = get_page_virtual_id_from_offset_(io_ctx.get_read_offset_in_file(),
                                                                                false /*is_open_interval*/);
    const int64_t start_page_id_in_data_item = read_start_virtual_page_id - data_items[i].virtual_page_id_;
    const int64_t begin_offset_in_block =
        (data_items[i].physical_page_id_ + start_page_id_in_data_item) * ObTmpFileGlobal::PAGE_SIZE;
    const int64_t end_offset_in_block =
        (data_items[i].physical_page_id_ + data_items[i].physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
    const int64_t begin_read_offset_in_block = begin_offset_in_block +
                                               (0 == i?
                                                get_page_offset_from_file_or_block_offset_(io_ctx.get_read_offset_in_file()) : 0);
    const int64_t end_read_offset_in_block = (data_items.count() - 1  == i?
                                              begin_read_offset_in_block + remain_read_size :
                                              end_offset_in_block);
    int64_t actual_block_read_size = 0;

    ObTmpBlockValueHandle block_value_handle;
    if (!io_ctx.is_disable_block_cache() &&
        OB_SUCC(ObTmpBlockCache::get_instance().get_block(ObTmpBlockCacheKey(block_index, MTL_ID()),
                                                          block_value_handle))) {
      LOG_DEBUG("hit block cache", K(block_index), K(fd_), K(io_ctx));
      char *read_buf = io_ctx.get_todo_buffer();
      const int64_t read_size = end_read_offset_in_block - begin_read_offset_in_block;
      ObTmpFileIOCtx::ObBlockCacheHandle block_cache_handle(block_value_handle, read_buf,
                                                            begin_read_offset_in_block,
                                                            read_size);
      if (OB_FAIL(io_ctx.get_block_cache_handles().push_back(block_cache_handle))) {
        LOG_WARN("Fail to push back into block_handles", KR(ret), K(fd_));
      } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
        LOG_WARN("fail to update data size", KR(ret), K(read_size));
      } else {
        remain_read_size -= read_size;
        actual_read_size += read_size;
        LOG_DEBUG("succ to read data from cached block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(read_size), K(actual_read_size),
              K(data_items[i]), K(io_ctx));
      }
    } else if (OB_ENTRY_NOT_EXIST != ret && OB_SUCCESS != ret) {
      LOG_WARN("fail to get block", KR(ret), K(fd_), K(block_index));
    } else { // not hit block cache, read page from disk.
      ret = OB_SUCCESS;
      if (io_ctx.is_disable_page_cache()) {
        if (OB_FAIL(inner_seq_read_from_block_(block_index,
                                               begin_read_offset_in_block,
                                               end_read_offset_in_block,
                                               io_ctx, actual_block_read_size))) {
          LOG_WARN("fail to seq read from block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(actual_block_read_size), K(actual_read_size),
              K(data_items[i]), K(io_ctx));
        } else {
          remain_read_size -= actual_block_read_size;
          actual_read_size += actual_block_read_size;
          LOG_DEBUG("succ to seq read from block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(actual_block_read_size), K(actual_read_size),
              K(data_items[i]), K(io_ctx));
        }
      } else {
        if (OB_FAIL(inner_rand_read_from_block_(block_index,
                                                begin_read_offset_in_block,
                                                end_read_offset_in_block,
                                                io_ctx, actual_block_read_size))) {
          LOG_WARN("fail to rand read from block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(actual_block_read_size), K(actual_read_size),
              K(data_items[i]), K(io_ctx));
        } else {
          remain_read_size -= actual_block_read_size;
          actual_read_size += actual_block_read_size;
          LOG_DEBUG("succ to rand read from block",
              KR(ret), K(fd_), K(block_index), K(begin_offset_in_block), K(end_offset_in_block),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(actual_block_read_size), K(actual_read_size),
              K(data_items[i]), K(io_ctx));
        }
      }
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_seq_read_from_block_(const int64_t block_index,
                                                       const int64_t begin_read_offset_in_block,
                                                       const int64_t end_read_offset_in_block,
                                                       ObTmpFileIOCtx &io_ctx,
                                                       int64_t &actual_read_size)
{
  int ret = OB_SUCCESS;
  const int64_t expected_read_size = end_read_offset_in_block - begin_read_offset_in_block;
  ObTmpFileBlockHandle block_handle;
  actual_read_size = 0;

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX == block_index ||
                  expected_read_size <= 0 || expected_read_size > OB_DEFAULT_MACRO_BLOCK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(block_index),
                                 K(begin_read_offset_in_block),
                                 K(end_read_offset_in_block));
  } else if (OB_FAIL(tmp_file_block_manager_->get_tmp_file_block_handle(block_index, block_handle))) {
    LOG_WARN("fail to get tmp file block_handle", KR(ret), K(fd_), K(block_index));
  } else if (OB_ISNULL(block_handle.get()) || OB_UNLIKELY(!block_handle.get()->get_macro_block_id().is_valid())) {
    LOG_WARN("fail to get tmp file block_handle", KR(ret), K(fd_), K(block_handle));
  } else {
    char *read_buf = io_ctx.get_todo_buffer();
    ObTmpFileIOCtx::ObIOReadHandle io_read_handle(read_buf,
                                                  0 /*offset_in_src_data_buf_*/,
                                                  expected_read_size, block_handle);
    if (OB_FAIL(io_ctx.get_io_handles().push_back(io_read_handle))) {
      LOG_WARN("Fail to push back into io_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(ObTmpPageCache::get_instance().direct_read(
               block_handle.get()->get_macro_block_id(), expected_read_size, begin_read_offset_in_block,
               io_ctx.get_io_flag(), io_ctx.get_io_timeout_ms(), *callback_allocator_,
               io_ctx.get_io_handles().at(io_ctx.get_io_handles().count()-1).handle_))) {
      LOG_WARN("fail to cached_read", KR(ret), K(fd_), K(block_handle), K(expected_read_size),
                                      K(begin_read_offset_in_block), K(io_ctx));
    }
  }

  // Update read offset and read size.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(io_ctx.update_data_size(expected_read_size))) {
    LOG_WARN("fail to update data size", KR(ret), K(expected_read_size));
  } else {
    actual_read_size = expected_read_size;
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_rand_read_from_block_(const int64_t block_index,
                                                        const int64_t begin_read_offset_in_block,
                                                        const int64_t end_read_offset_in_block,
                                                        ObTmpFileIOCtx &io_ctx,
                                                        int64_t &actual_read_size)
{
  int ret = OB_SUCCESS;
  const int64_t begin_page_idx_in_block = get_page_id_in_block_(begin_read_offset_in_block);
  const int64_t end_page_idx_in_block = get_page_id_in_block_(end_read_offset_in_block - 1); // -1 for changing open interval to close interval
  ObTmpFileBlockPageBitmap bitmap;
  ObTmpFileBlockPageBitmapIterator iterator;
  ObArray<ObTmpPageValueHandle> page_value_handles;

  if (OB_FAIL(collect_pages_in_block_(block_index, begin_page_idx_in_block, end_page_idx_in_block,
                                      bitmap, page_value_handles))) {
    LOG_WARN("fail to collect pages in block", KR(ret), K(fd_), K(block_index),
                                               K(begin_page_idx_in_block),
                                               K(end_page_idx_in_block));
  } else if (OB_FAIL(iterator.init(&bitmap, begin_page_idx_in_block, end_page_idx_in_block))) {
    LOG_WARN("fail to init iterator", KR(ret), K(fd_), K(block_index),
                                      K(begin_page_idx_in_block), K(end_page_idx_in_block));
  } else {
    int64_t has_read_cached_page_num = 0;
    while (OB_SUCC(ret) && iterator.has_next()) {
      bool is_in_cache = false;
      int64_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      int64_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      int64_t begin_read_offset = -1;
      int64_t end_read_offset = -1;
      if (OB_FAIL(iterator.next_range(is_in_cache, begin_page_id, end_page_id))) {
        LOG_WARN("fail to next range", KR(ret), K(fd_));
      } else if (OB_UNLIKELY(begin_page_id > end_page_id || begin_page_id < 0 ||  end_page_id < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid range", KR(ret), K(fd_), K(begin_page_id), K(end_page_id));
      } else {
        begin_read_offset = begin_page_id == begin_page_idx_in_block ?
                            begin_read_offset_in_block :
                            get_page_begin_offset_by_virtual_id_(begin_page_id);
        end_read_offset = end_page_id == end_page_idx_in_block ?
                          end_read_offset_in_block :
                          get_page_end_offset_by_virtual_id_(end_page_id);
      }

      if (OB_FAIL(ret)) {
      } else if (is_in_cache) {
        if (OB_FAIL(inner_read_continuous_cached_pages_(begin_read_offset, end_read_offset,
                                                        page_value_handles, has_read_cached_page_num,
                                                        io_ctx))) {
          LOG_WARN("fail to inner read continuous cached pages", KR(ret), K(fd_), K(begin_read_offset),
                                                                 K(end_read_offset), K(io_ctx));
        } else {
          has_read_cached_page_num += (end_page_id - begin_page_id + 1);
        }
      } else {
        if (OB_FAIL(inner_read_continuous_uncached_pages_(block_index, begin_read_offset,
                                                          end_read_offset, io_ctx))) {
          LOG_WARN("fail to inner read continuous uncached pages", KR(ret), K(fd_), K(block_index),
                                                                   K(begin_read_offset),
                                                                   K(end_read_offset),
                                                                   K(io_ctx));
        }
      }

      if (OB_SUCC(ret)) {
        const int64_t read_size = end_read_offset - begin_read_offset;
        actual_read_size += read_size;
      }
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::collect_pages_in_block_(const int64_t block_index,
                                                    const int64_t begin_page_idx_in_block,
                                                    const int64_t end_page_idx_in_block,
                                                    ObTmpFileBlockPageBitmap &bitmap,
                                                    ObIArray<ObTmpPageValueHandle> &page_value_handles)
{
  int ret = OB_SUCCESS;
  bitmap.reset();
  page_value_handles.reset();

  for (int64_t page_idx_in_block = begin_page_idx_in_block;
       OB_SUCC(ret) && page_idx_in_block <= end_page_idx_in_block;
       page_idx_in_block++) {
    ObTmpPageCacheKey key(block_index, page_idx_in_block, tenant_id_);
    ObTmpPageValueHandle p_handle;
    if (OB_SUCC(ObTmpPageCache::get_instance().get_page(key, p_handle))) {
      if (OB_FAIL(page_value_handles.push_back(p_handle))) {
        LOG_WARN("fail to push back", KR(ret), K(fd_), K(key));

        // if fail to push back, we will treat this page as uncached page.
        ret = OB_SUCCESS;
        if (OB_FAIL(bitmap.set_bitmap(page_idx_in_block, false))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(fd_), K(key));
        }
      } else if (OB_FAIL(bitmap.set_bitmap(page_idx_in_block, true))) {
        LOG_WARN("fail to set bitmap", KR(ret), K(fd_), K(key));
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(bitmap.set_bitmap(page_idx_in_block, false))) {
        LOG_WARN("fail to set bitmap", KR(ret), K(fd_), K(key));
      }
    } else {
      LOG_WARN("fail to get page from cache", KR(ret), K(fd_), K(key));
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_read_continuous_uncached_pages_(const int64_t block_index,
                                                                  const int64_t begin_read_offset_in_block,
                                                                  const int64_t end_read_offset_in_block,
                                                                  ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObTmpPageCacheKey> page_keys;
  const int64_t begin_page_idx = get_page_id_in_block_(begin_read_offset_in_block);
  const int64_t end_page_idx = get_page_id_in_block_(end_read_offset_in_block - 1); // -1 to change open interval to close interval
  const int64_t block_read_begin_offset = get_page_begin_offset_by_file_or_block_offset_(begin_read_offset_in_block);
  const int64_t block_read_end_offset = get_page_end_offset_by_file_or_block_offset_(end_read_offset_in_block);
  const int64_t block_read_size = block_read_end_offset - block_read_begin_offset;  // read and cached completed pages from disk
  const int64_t usr_read_begin_offset = begin_read_offset_in_block;
  const int64_t usr_read_end_offset = end_read_offset_in_block;
  // from loaded disk block buf, from "offset_in_block_buf" read "usr_read_size" size data to user's read buf
  const int64_t offset_in_block_buf = usr_read_begin_offset - block_read_begin_offset;
  const int64_t usr_read_size = block_read_size -
                                (usr_read_begin_offset - block_read_begin_offset) -
                                (block_read_end_offset - usr_read_end_offset);

  for (int64_t page_id = begin_page_idx; OB_SUCC(ret) && page_id <= end_page_idx; page_id++) {
    ObTmpPageCacheKey key(block_index, page_id, tenant_id_);
    if (OB_FAIL(page_keys.push_back(key))) {
      LOG_WARN("fail to push back", KR(ret), K(fd_), K(key));
    }
  }

  ObTmpFileBlockHandle block_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_file_block_manager_->get_tmp_file_block_handle(block_index, block_handle))) {
    LOG_WARN("fail to get tmp file block_handle", KR(ret), K(fd_), K(block_index));
  } else {
    char *user_read_buf = io_ctx.get_todo_buffer();
    ObTmpFileIOCtx::ObIOReadHandle io_read_handle(user_read_buf,
                                                  offset_in_block_buf,
                                                  usr_read_size, block_handle);
    if (OB_FAIL(io_ctx.get_io_handles().push_back(io_read_handle))) {
      LOG_WARN("Fail to push back into io_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(ObTmpPageCache::get_instance().cached_read(page_keys,
               block_handle.get()->get_macro_block_id(), block_read_begin_offset,
               io_ctx.get_io_flag(), io_ctx.get_io_timeout_ms(), *callback_allocator_,
               io_ctx.get_io_handles().at(io_ctx.get_io_handles().count()-1).handle_))) {
      LOG_WARN("fail to cached_read", KR(ret), K(fd_), K(block_handle),
                                      K(block_read_begin_offset), K(io_ctx));
    } else if (OB_FAIL(io_ctx.update_data_size(usr_read_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(usr_read_size));
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_read_continuous_cached_pages_(const int64_t begin_read_offset_in_block,
                                                                const int64_t end_read_offset_in_block,
                                                                const ObArray<ObTmpPageValueHandle> &page_value_handles,
                                                                const int64_t start_array_idx,
                                                                ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t begin_page_idx = get_page_id_in_block_(begin_read_offset_in_block);
  const int64_t end_page_idx = get_page_id_in_block_(end_read_offset_in_block - 1); // -1 for changing open interval to close interval
  const int64_t iter_array_cnt = end_page_idx - begin_page_idx + 1;

  if (OB_UNLIKELY(start_array_idx < 0 || start_array_idx + iter_array_cnt > page_value_handles.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array idx", KR(ret), K(fd_), K(start_array_idx), K(iter_array_cnt),
             K(begin_page_idx), K(end_page_idx), K(page_value_handles.count()));
  }

  int64_t read_offset = begin_read_offset_in_block;
  int64_t cur_array_idx = start_array_idx;
  for (int64_t cached_page_idx = begin_page_idx; OB_SUCC(ret) && cached_page_idx <= end_page_idx; cached_page_idx++) {
    ObTmpPageValueHandle p_handle = page_value_handles.at(cur_array_idx++);
    char *read_buf = io_ctx.get_todo_buffer();
    const int64_t cur_page_end_offset = get_page_end_offset_by_virtual_id_(cached_page_idx);
    const int64_t read_size = MIN(cur_page_end_offset, end_read_offset_in_block) - read_offset;
    const int64_t read_offset_in_page = get_page_offset_from_file_or_block_offset_(read_offset);
    ObTmpFileIOCtx::ObPageCacheHandle page_handle(p_handle, read_buf,
                                                  read_offset_in_page,
                                                  read_size);
    if (OB_FAIL(io_ctx.get_page_cache_handles().push_back(page_handle))) {
      LOG_WARN("Fail to push back into page_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(read_size));
    } else {
      read_offset += read_size;
    }
  } // end for

  return ret;
}

int ObSharedNothingTmpFile::inner_read_from_wbp_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t begin_read_page_virtual_id = get_page_virtual_id_from_offset_(io_ctx.get_read_offset_in_file(),
                                                                              false /*is_open_interval*/);
  uint32_t begin_read_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  int64_t wbp_begin_offset = cal_wbp_begin_offset_();

  if (OB_UNLIKELY(wbp_begin_offset < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected wbp begin offset", KR(ret), K(fd_), K(wbp_begin_offset), K(io_ctx));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_read_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected begin read page virtual id", KR(ret), K(fd_), K(begin_read_page_virtual_id), K(io_ctx));
  } else if (io_ctx.get_read_offset_in_file() < wbp_begin_offset + ObTmpFileGlobal::PAGE_SIZE) {
    begin_read_page_id = begin_page_id_;
  } else if (OB_FAIL(page_idx_cache_.binary_search(begin_read_page_virtual_id, begin_read_page_id))) {
    LOG_WARN("fail to find page index in array", KR(ret), K(fd_), K(io_ctx));
  } else if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_read_page_id &&
             OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, begin_read_page_virtual_id, begin_page_id_, begin_read_page_id))) {
    LOG_WARN("fail to get page id by virtual id", KR(ret), K(fd_), K(begin_read_page_virtual_id), K(begin_page_id_));
  } else if (OB_UNLIKELY(begin_read_page_id == ObTmpFileGlobal::INVALID_PAGE_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid page index", KR(ret), K(fd_), K(begin_read_page_id));
  }

  uint32_t curr_page_id = begin_read_page_id;
  int64_t curr_page_virtual_id = begin_read_page_virtual_id;
  while (OB_SUCC(ret) && io_ctx.get_todo_size() > 0) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *data_page = nullptr;
    if (OB_FAIL(wbp_->read_page(fd_, curr_page_id, ObTmpFilePageUniqKey(curr_page_virtual_id), data_page, next_page_id))) {
      LOG_WARN("fail to fetch page", KR(ret), K(fd_), K(curr_page_id), K(curr_page_virtual_id), K(io_ctx));
    } else if (OB_ISNULL(data_page)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data page is null", KR(ret), K(fd_), K(curr_page_id));
    } else {
      const int64_t read_offset_in_page = get_page_offset_from_file_or_block_offset_(io_ctx.get_read_offset_in_file());
      const int64_t read_size = MIN3(ObTmpFileGlobal::PAGE_SIZE - read_offset_in_page,
                                     io_ctx.get_todo_size(),
                                     file_size_ - io_ctx.get_read_offset_in_file());
      char *read_buf = io_ctx.get_todo_buffer();
      if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(read_buf, read_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid buf range", KR(ret), K(fd_), K(read_buf), K(read_size), K(io_ctx));
      } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
        LOG_WARN("fail to update data size", KR(ret), K(fd_), K(read_size));
      } else {
        MEMCPY(read_buf, data_page + read_offset_in_page, read_size);
        curr_page_id = next_page_id;
        curr_page_virtual_id += 1;
      }
    }

    if (OB_SUCC(ret) && io_ctx.get_todo_size() > 0 && io_ctx.get_read_offset_in_file() == file_size_) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", KR(ret), K(fd_), K(io_ctx));
    }
  }

  return ret;
}

// attention:
// in order to avoid blocking reading, flushing and evicting pages,
// write operation only adds write lock for meta_lock when it try to update meta data.
// due to the write operation is appending write, if the meta data doesn't been modified,
// all operation could not write or read the new appending data.
int ObSharedNothingTmpFile::aio_write(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("aio write start", K(fd_), K(file_size_), K(io_ctx));

  ObSpinLockGuard guard(multi_write_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedNothingTmpFile has not been inited", KR(ret), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // this check is just a hint.
    // although is_deleting_ == false, it might be set as true in the processing of inner_write().
    // we will check is_deleting_ again when try to update meta data in the inner_write()
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to write a deleting file", KR(ret), K(fd_));
  } else {
    bool is_unaligned_write = 0 != file_size_ % ObTmpFileGlobal::PAGE_SIZE
                  || 0 != io_ctx.get_todo_size() % ObTmpFileGlobal::PAGE_SIZE;
    while (OB_SUCC(ret) && io_ctx.get_todo_size() > 0) {
      if (OB_FAIL(inner_write_(io_ctx))) {
        if (OB_ALLOCATE_TMP_FILE_PAGE_FAILED == ret) {
          ret = OB_SUCCESS;
          if (TC_REACH_COUNT_INTERVAL(10)) {
            LOG_INFO("alloc mem failed, try to evict pages", K(fd_), K(file_size_), K(io_ctx), KPC(this));
          }
          if (OB_FAIL(page_cache_controller_->invoke_swap_and_wait(
                  MIN(io_ctx.get_todo_size(), ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_PAGE_NUM * ObTmpFileGlobal::PAGE_SIZE),
                  io_ctx.get_io_timeout_ms()))) {
            LOG_WARN("fail to invoke swap and wait", KR(ret), K(io_ctx), K(fd_));
          }
        } else {
          LOG_WARN("fail to inner write", KR(ret), K(fd_), K(io_ctx), KPC(this));
        }
      }
    } // end while
    if (OB_SUCC(ret)) {
      write_req_cnt_++;
      if (is_unaligned_write) {
        unaligned_write_req_cnt_++;
      }
      last_modify_ts_ = ObTimeUtility::current_time();
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("aio write finish", KR(ret), K(fd_), K(file_size_), K(io_ctx));
  } else {
    LOG_DEBUG("aio write failed", KR(ret), K(fd_), K(file_size_), K(io_ctx), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_write_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  if (has_unfinished_page_()) {
    if (OB_FAIL(inner_fill_tail_page_(io_ctx))) {
      LOG_WARN("fail to fill tail page", KR(ret), K(fd_), K(io_ctx));
    } else if (OB_UNLIKELY(has_unfinished_page_() && io_ctx.get_todo_size() > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tmp file page is unaligned", KR(ret), K(fd_));
    }
  }

  // each batch at most write TMP_FILE_WRITE_BATCH_PAGE_NUM pages
  while (OB_SUCC(ret) && io_ctx.get_todo_size() > 0) {
    if (OB_FAIL(inner_write_continuous_pages_(io_ctx))) {
      LOG_WARN("fail to write continuous pages", KR(ret), K(fd_), K(io_ctx));
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_fill_tail_page_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard last_page_lock_guard(last_page_lock_);
  const bool is_in_disk = (0 == cached_page_nums_);

  if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
  } else if (is_in_disk) {
    if (OB_FAIL(load_disk_tail_page_and_rewrite_(io_ctx))) {
      LOG_WARN("fail to load disk tail page and rewrite", KR(ret),K(fd_),  K(io_ctx));
    }
  } else {
    if (OB_FAIL(append_write_memory_tail_page_(io_ctx))) {
      LOG_WARN("fail to append write memory tail page", KR(ret), K(fd_), K(io_ctx));
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::load_disk_tail_page_and_rewrite_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t has_written_size = get_page_offset_from_file_or_block_offset_(file_size_);
  const int64_t begin_page_virtual_id = get_page_virtual_id_from_offset_(file_size_,
                                                                         true /*is_open_interval*/);
  const int64_t need_write_size = MIN(ObTmpFileGlobal::PAGE_SIZE - has_written_size, io_ctx.get_todo_size());
  char *write_buff = io_ctx.get_todo_buffer();
  uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ObSharedNothingTmpFileDataItem data_item;
  bool block_meta_tree_flushing = false;
  bool has_update_file_meta = false;

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("begin page virtual id is invalid", KR(ret), K(fd_), K(begin_page_virtual_id), K(file_size_));
  } else if (OB_UNLIKELY(has_written_size + need_write_size > ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need write size is invalid", KR(ret), K(fd_), K(has_written_size), K(need_write_size));
  } else if (OB_ISNULL(write_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", KR(ret), K(fd_), K(write_buff));
  } else if (OB_FAIL(meta_tree_.prepare_for_write_tail(data_item))) {
    LOG_WARN("fail to prepare for write tail", KR(ret), K(fd_));
  } else {
    block_meta_tree_flushing = true;
  }

  if (OB_SUCC(ret)) {
    blocksstable::MacroBlockId macro_block_id;
    int64_t last_page_begin_offset_in_block =
                      (data_item.physical_page_id_ + data_item.physical_page_num_ - 1)
                      * ObTmpFileGlobal::PAGE_SIZE;
    char *page_buf = nullptr;
    if (OB_FAIL(tmp_file_block_manager_->get_macro_block_id(data_item.block_index_, macro_block_id))) {
      LOG_WARN("fail to get macro block id", KR(ret), K(fd_), K(data_item.block_index_));
    } else if (OB_UNLIKELY(!macro_block_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block id is invalid", KR(ret), K(fd_), K(data_item.block_index_));
    } else if (OB_FAIL(wbp_->alloc_page(fd_, ObTmpFilePageUniqKey(begin_page_virtual_id), new_page_id, page_buf))) {
      LOG_WARN("fail to alloc page", KR(ret), K(fd_), K(begin_page_virtual_id),
               K(new_page_id), KP(page_buf));
    } else {
      // load last unfilled page from disk
      blocksstable::ObMacroBlockHandle mb_handle;
      blocksstable::ObMacroBlockReadInfo info;
      info.io_desc_ = io_ctx.get_io_flag();
      info.macro_block_id_ = macro_block_id;
      info.size_ = has_written_size;
      info.offset_ = last_page_begin_offset_in_block;
      info.buf_ = page_buf;
      info.io_callback_ = nullptr;
      info.io_timeout_ms_ = io_ctx.get_io_timeout_ms();

      if (OB_FAIL(mb_handle.async_read(info))) {
        LOG_ERROR("fail to async write block", KR(ret), K(fd_), K(info));
      } else if (OB_FAIL(mb_handle.wait())) {
        LOG_WARN("fail to wait", KR(ret), K(fd_), K(info));
      } else if (mb_handle.get_data_size() < has_written_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to read expected size", KR(ret), K(fd_), K(info), K(has_written_size));
      } else if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(write_buff, need_write_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid buf range", KR(ret), K(fd_), K(write_buff), K(need_write_size), K(io_ctx));
      } else {
        // fill last page in memory
        MEMCPY(page_buf + has_written_size, write_buff, need_write_size);
      }

      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        if (OB_TMP_FAIL(wbp_->free_page(fd_, new_page_id, ObTmpFilePageUniqKey(begin_page_virtual_id), unused_page_id))) {
          LOG_WARN("fail to free page", KR(tmp_ret), K(fd_), K(begin_page_virtual_id), K(new_page_id));
        }
        new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      } else if (OB_FAIL(wbp_->notify_dirty(fd_, new_page_id, ObTmpFilePageUniqKey(begin_page_virtual_id)))) {
        LOG_WARN("fail to notify dirty", KR(ret), K(fd_), K(new_page_id));
      }
    }
  }

  // update meta data
  if (OB_SUCC(ret)) {
    common::TCRWLock::WLockGuard guard(meta_lock_);
    if (OB_UNLIKELY(is_deleting_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file is deleting", KR(ret), K(fd_));
    } else if (OB_FAIL(meta_tree_.finish_write_tail(data_item, true /*release_tail_in_disk*/))) {
      LOG_WARN("fail to finish write tail page", KR(ret), K(fd_));
    } else if (OB_FAIL(io_ctx.update_data_size(need_write_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(need_write_size));
    } else if (OB_FAIL(page_idx_cache_.push(new_page_id))) {
      LOG_WARN("fail to push back page idx array", KR(ret), K(fd_), K(new_page_id));
    } else {
      cached_page_nums_ = 1;
      file_size_ += need_write_size;
      begin_page_id_ = new_page_id;
      begin_page_virtual_id_ = begin_page_virtual_id;
      end_page_id_ = new_page_id;
      has_update_file_meta = true;
    }

    if (FAILEDx(insert_or_update_data_flush_node_())) {
      LOG_WARN("fail to insert or update flush data list", KR(ret), K(fd_), KPC(this));
    } else if (OB_FAIL(insert_or_update_meta_flush_node_())) {
      LOG_WARN("fail to insert or update flush meta list", KR(ret), K(fd_), KPC(this));
    }

    LOG_DEBUG("load_disk_tail_page_and_rewrite_ end", KR(ret), K(fd_), K(end_page_id_), KPC(this));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (block_meta_tree_flushing) {
      if (OB_TMP_FAIL(meta_tree_.finish_write_tail(data_item, false /*release_tail_in_disk*/))) {
        LOG_WARN("fail to modify items after tail load", KR(tmp_ret), K(fd_));
      }
    }

    uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    if (new_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      if (OB_TMP_FAIL(wbp_->free_page(fd_, new_page_id, ObTmpFilePageUniqKey(begin_page_virtual_id), unused_page_id))) {
        LOG_WARN("fail to free page", KR(tmp_ret), K(fd_), K(new_page_id));
      } else if (has_update_file_meta) {
        cached_page_nums_ = 0;
        file_size_ -= need_write_size;
        begin_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
        begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
        page_idx_cache_.reset();
      }
    }
  }
  return ret;
}

// attention:
// last_page_lock_ could promise that there are not truncating task,
// evicting task or flush generator task for the last page.
// however, it might happen that append write for the last page is processing after
// flush generator task has been processed over.
// thus, this function should consider the concurrence problem between append write and callback of flush task.
int ObSharedNothingTmpFile::append_write_memory_tail_page_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  char *page_buff = nullptr;
  const int64_t has_written_size = get_page_offset_from_file_or_block_offset_(file_size_);
  const int64_t need_write_size = MIN(ObTmpFileGlobal::PAGE_SIZE - has_written_size,
                                      io_ctx.get_todo_size());
  const int64_t end_page_virtual_id = get_page_virtual_id_from_offset_(file_size_,
                                                                       true /*is_open_interval*/);
  char *write_buff = io_ctx.get_todo_buffer();
  uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ObSharedNothingTmpFileDataItem rightest_data_item;

  if (OB_UNLIKELY(has_written_size + need_write_size > ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need write size is invalid", KR(ret), K(fd_), K(has_written_size), K(need_write_size));
  } else if (OB_ISNULL(write_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", KR(ret), K(fd_), K(write_buff));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == end_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("end page virtual id is invalid", KR(ret), K(fd_), K(end_page_virtual_id), K(file_size_));
  } else if (OB_FAIL(wbp_->read_page(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id),
                                     page_buff, unused_page_id))) {
    LOG_WARN("fail to fetch page", KR(ret), K(fd_), K(end_page_id_), K(end_page_virtual_id));
  } else if (OB_ISNULL(page_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page buff is null", KR(ret), K(fd_), K(end_page_id_));
  } else if ((wbp_->is_write_back(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id)) ||
              wbp_->is_cached(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id))) &&
             OB_FAIL(meta_tree_.prepare_for_write_tail(rightest_data_item))) {
    LOG_WARN("fail to prepare for write tail", KR(ret), K(fd_),
             K(end_page_id_), K(end_page_virtual_id), K(rightest_data_item));
  } else if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(write_buff, need_write_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf range", KR(ret), K(fd_), K(write_buff), K(need_write_size), K(io_ctx));
  } else {
    MEMCPY(page_buff + has_written_size, write_buff, need_write_size);
  }

  // update meta data
  if (OB_SUCC(ret)) {
    common::TCRWLock::WLockGuard guard(meta_lock_);
    const bool is_cached = wbp_->is_cached(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id));
    const bool is_write_back = wbp_->is_write_back(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id));
    if (OB_UNLIKELY(is_deleting_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file is deleting", KR(ret), K(fd_));
    } else if (is_cached || is_write_back) {
      // due to the appending writing for the last page which is flushing or flushed into disk,
      // the page carbon in memory and disk will be different.
      // thus, we need to rollback the flush status of the last page,
      if (OB_FAIL(wbp_->notify_dirty(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id)))) {
        LOG_WARN("fail to notify dirty", KR(ret), K(fd_), K(end_page_id_), K(end_page_virtual_id));
      } else if (is_cached) {
        // for the last page, if the status of flushed_page_id_ page is not cached,
        // we will treat this page as a non-flushed page
        flushed_data_page_num_--;
        if (0 == flushed_data_page_num_) {
          LOG_INFO("flushed_page_id_ has been written", KPC(this));
          flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        }
      } else if (is_write_back) {
        write_back_data_page_num_--;
      }

      // modify meta tree
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(meta_tree_.finish_write_tail(rightest_data_item, false /*release_tail_in_disk*/))) {
          LOG_WARN("fail to modify finish write tail page", KR(tmp_ret), K(fd_));
        }
      } else {
        // add file into flush list if file is not flushing,
        // because we produce 1 dirty data page and 1 dirty meta page after writing tail page
        if (OB_FAIL(meta_tree_.finish_write_tail(rightest_data_item, true /*release_tail_in_disk*/))) {
          LOG_WARN("fail to finish write tail page", KR(ret), K(fd_));
        } else if (OB_FAIL(insert_or_update_data_flush_node_())) {
          LOG_WARN("fail to insert or update flush data list", KR(ret), K(fd_), KPC(this));
        } else if (OB_FAIL(insert_or_update_meta_flush_node_())) {
          LOG_WARN("fail to insert or update flush meta list", KR(ret), K(fd_), KPC(this));
        }
      }
    }

    if (FAILEDx(io_ctx.update_data_size(need_write_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(need_write_size));
    } else {
      file_size_ += need_write_size;
    }

    LOG_DEBUG("append_write_memory_tail_page_ end", KR(ret), K(fd_), K(end_page_id_), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::inner_write_continuous_pages_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  bool is_alloc_failed = false;
  int64_t write_size = 0;
  ObArray<uint32_t> page_entry_idxs;
  bool has_update_file_meta = false;

  // write pages
  if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_FAIL(alloc_and_write_pages_(io_ctx, page_entry_idxs, write_size))) {
    if (OB_ALLOCATE_TMP_FILE_PAGE_FAILED == ret) {
      // this error code will return to caller after modifing meta data of file based on written data pages
      ret = OB_SUCCESS;
      is_alloc_failed = true;
      if (TC_REACH_COUNT_INTERVAL(10)) {
        LOG_INFO("fail to alloc memory", K(tenant_id_), K(fd_), K(write_size),
                                         K(page_entry_idxs), K(io_ctx));
      }
    } else {
      LOG_WARN("fail to batch write pages", KR(ret), K(fd_), K(io_ctx));
    }
  }

  // Update meta data.
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(page_entry_idxs.empty() || write_size <= 0)) {
    if (!is_alloc_failed) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page entry idxs is empty", KR(ret), K(fd_), K(page_entry_idxs), K(write_size));
    } else {
      // do nothing, no need to update meta data
    }
  } else {
    common::TCRWLock::WLockGuard guard(meta_lock_);
    const int64_t end_page_virtual_id = cached_page_nums_ == 0 ?
                                        ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID :
                                        get_page_virtual_id_from_offset_(file_size_, true /*is_open_interval*/);
    const int64_t old_begin_page_id = begin_page_id_;
    const int64_t old_begin_page_virtual_id_ = begin_page_virtual_id_;
    const int64_t old_end_page_id = end_page_id_;
    if (OB_UNLIKELY(is_deleting_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file is deleting", KR(ret), K(fd_));
    } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID != end_page_id_ &&
                           ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == end_page_virtual_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("end page virtual id is invalid", KR(ret), K(fd_), K(end_page_virtual_id), K(file_size_));
    } else if (OB_UNLIKELY((ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_ &&
                            ObTmpFileGlobal::INVALID_PAGE_ID != end_page_id_) ||
                            (ObTmpFileGlobal::INVALID_PAGE_ID != begin_page_id_ &&
                            ObTmpFileGlobal::INVALID_PAGE_ID == end_page_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("begin or end page id is invalid", KR(ret), K(fd_),
                                                  K(begin_page_id_),
                                                  K(end_page_id_));
    } else if (OB_FAIL(io_ctx.update_data_size(write_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(io_ctx));
    } else {
      if (ObTmpFileGlobal::INVALID_PAGE_ID != end_page_id_ &&
          OB_FAIL(wbp_->link_page(fd_, page_entry_idxs.at(0), end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id)))) {
        LOG_WARN("fail to link page", KR(ret), K(fd_), K(page_entry_idxs.at(0)),
                 K(end_page_id_), K(end_page_virtual_id));
      } else {
        if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_) {
          begin_page_id_ = page_entry_idxs.at(0);
          begin_page_virtual_id_ = get_page_virtual_id_from_offset_(file_size_, false /*is_open_interval*/);
        }
        end_page_id_ = page_entry_idxs.at(page_entry_idxs.count() - 1);
        file_size_ += write_size;
        cached_page_nums_ += page_entry_idxs.count();
        has_update_file_meta = true;
      }

      if (FAILEDx(insert_or_update_data_flush_node_())) {
        LOG_WARN("fail to insert or update data flush list", KR(ret), K(fd_), KPC(this));
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      int64_t free_page_virtual_id = file_size_ == 0 ? 0 : end_page_virtual_id + 1;
      for (int64_t i = 0; i < page_entry_idxs.count() && OB_LIKELY(OB_SUCCESS == tmp_ret); i++) {
        if (OB_TMP_FAIL(tmp_ret = wbp_->free_page(fd_, page_entry_idxs[i], ObTmpFilePageUniqKey(free_page_virtual_id), unused_page_id))) {
          LOG_WARN("fail to free page", KR(tmp_ret), K(fd_), K(i), K(free_page_virtual_id), K(page_entry_idxs[i]));
        } else {
          free_page_virtual_id += 1;
        }
      }
      if (has_update_file_meta) {
        begin_page_id_ = old_begin_page_id;
        begin_page_virtual_id_ = old_begin_page_virtual_id_;
        end_page_id_ = old_end_page_id;
        cached_page_nums_ -= page_entry_idxs.count();
        file_size_ -= write_size;
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < page_entry_idxs.count() && OB_LIKELY(tmp_ret == OB_SUCCESS); i++) {
        if (OB_TMP_FAIL(page_idx_cache_.push(page_entry_idxs[i]))) {
          LOG_WARN("fail to push page idx array", KR(tmp_ret), K(fd_));
        }
      }
    }
  } // end update meta data.

  // reset allocation failure status
  ret = is_alloc_failed && OB_SUCC(ret) ? OB_ALLOCATE_TMP_FILE_PAGE_FAILED : ret;
  return ret;
}

int ObSharedNothingTmpFile::alloc_and_write_pages_(const ObTmpFileIOCtx &io_ctx,
                                                   ObIArray<uint32_t> &alloced_page_id,
                                                   int64_t &actual_write_size)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  int64_t new_page_virtual_id = get_page_virtual_id_from_offset_(file_size_, false /*is_open_interval*/);
  uint32_t previous_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  alloced_page_id.reset();
  actual_write_size = 0;

  if (OB_UNLIKELY(has_unfinished_page_())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the file has unfinished page", KR(ret), K(fd_));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == new_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new page virtual id is invalid", KR(ret), K(fd_), K(file_size_), K(new_page_virtual_id));
  } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
  } else {
    const int64_t expected_write_size = io_ctx.get_todo_size();
    while (OB_SUCC(ret) && actual_write_size < expected_write_size &&
           alloced_page_id.count() < ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_PAGE_NUM) {
      char *page_buf = nullptr;
      uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      if (OB_FAIL(wbp_->alloc_page(fd_, ObTmpFilePageUniqKey(new_page_virtual_id), new_page_id, page_buf))) {
        LOG_WARN("fail to alloc_page", KR(ret), K(fd_), K(new_page_virtual_id),
                 K(actual_write_size), K(expected_write_size));
      } else if (OB_ISNULL(page_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("page_buf is null", KR(ret), K(fd_), KP(page_buf));
      } else if (OB_UNLIKELY(new_page_id == ObTmpFileGlobal::INVALID_PAGE_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid page id", KR(ret), K(fd_), K(new_page_id));
      } else {
        int64_t write_size = common::min(ObTmpFileGlobal::PAGE_SIZE,
                                         expected_write_size - actual_write_size);
        const char *write_buf = io_ctx.get_todo_buffer() + actual_write_size;

        if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(write_buf, write_size))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid buf range", KR(ret), K(fd_), K(write_buf), K(write_size), K(io_ctx));
        } else if (FALSE_IT(MEMCPY(page_buf, write_buf, write_size))) {
        } else if (FALSE_IT(actual_write_size += write_size)) {
        } else if (OB_FAIL(wbp_->notify_dirty(fd_, new_page_id, ObTmpFilePageUniqKey(new_page_virtual_id)))) {
          LOG_WARN("fail to notify dirty", KR(ret), K(fd_), K(new_page_id), K(new_page_virtual_id));
        } else if (previous_page_id != ObTmpFileGlobal::INVALID_PAGE_ID &&
                   OB_FAIL(wbp_->link_page(fd_, new_page_id, previous_page_id,
                           ObTmpFilePageUniqKey(new_page_virtual_id - 1)))) {
          LOG_WARN("fail to link page", KR(ret), K(fd_), K(new_page_id), K(previous_page_id),
                   K(new_page_virtual_id));
        } else if (OB_FAIL(alloced_page_id.push_back(new_page_id))) {
          LOG_WARN("wbp fail to push back page id", KR(ret), K(fd_), K(new_page_id));
        } else {
          previous_page_id = new_page_id;
          new_page_virtual_id += 1;
        }
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(page_buf)) {
        int tmp_ret = OB_SUCCESS;
        uint32_t tmp_pid = ObTmpFileGlobal::INVALID_PAGE_ID; // unused
        if (OB_TMP_FAIL(wbp_->free_page(fd_, new_page_id, ObTmpFilePageUniqKey(new_page_virtual_id), tmp_pid))) {
          LOG_WARN("fail to free page", KR(tmp_ret), K(fd_), K(new_page_id));
        }
      }
    } // end while
  }
  LOG_DEBUG("alloc_and_write", KR(ret), K(fd_), K(io_ctx), K(file_size_),
                               K(end_page_id_), K(actual_write_size), K(alloced_page_id));
  return ret;
}

// Attention!!!!
// in order to prevent concurrency problems of eviction list
// from the operation from eviction manager and flush manager,
// before eviction manager calls this function,
// it removes the file's node from its' eviction list,
// but still keeps the file's `is_in_data_eviction_list_` be true.
// when this function run over,
// if `remain_flushed_page_num` > 0, this function will reinsert node into the list of eviction manager again;
// otherwise, this function will set `is_in_data_eviction_list_` be false.
int ObSharedNothingTmpFile::evict_data_pages(const int64_t expected_evict_page_num,
                                             int64_t &actual_evict_page_num,
                                             int64_t &remain_flushed_page_num)
{
  int ret = OB_SUCCESS;
  bool lock_last_page = false;
  common::TCRWLock::WLockGuard meta_lock_guard(meta_lock_);
  const int64_t end_page_virtual_id = get_page_virtual_id_from_offset_(file_size_, true /*is_open_interval*/);
  actual_evict_page_num = 0;
  remain_flushed_page_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedNothingTmpFile has not been inited", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // actual_evict_page_num = 0;
    // remain_flushed_page_num = 0;
    is_in_data_eviction_list_ = false;
    LOG_INFO("try to evict data pages when file is deleting", K(fd_), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(expected_evict_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(expected_evict_page_num));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == end_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid end page virtual id", KR(ret), K(fd_), K(end_page_virtual_id), K(file_size_), KPC(this));
  } else if (OB_UNLIKELY(0 == flushed_data_page_num_)) {
    is_in_data_eviction_list_ = false;
    LOG_INFO("tmp file has no flushed data pages need to be evicted", KR(ret), K(fd_), K(flushed_data_page_num_), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == flushed_page_id_ ||
                         ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == flushed_page_virtual_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush status", KR(ret), K(fd_), K(flushed_page_id_), K(flushed_page_virtual_id_), KPC(this));
  } else if (OB_UNLIKELY(!is_in_data_eviction_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN( "the file is not in data eviction list", K(fd_), K(is_in_data_eviction_list_), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid begin page virtual id", KR(ret), K(fd_), K(begin_page_virtual_id_), KPC(this));
  } else {
    bool need_to_evict_last_page = false;
    if (flushed_data_page_num_ == cached_page_nums_ && expected_evict_page_num >= flushed_data_page_num_ &&
        wbp_->is_cached(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id))) {
      need_to_evict_last_page = true;
      // last page should be evicted, try to lock the last page.
      if (OB_FAIL(last_page_lock_.trylock())) {
        // fail to get the lock of last data page, it will not be evicted.
        ret = OB_SUCCESS;
      } else {
        lock_last_page = true;
      }
    }

    int64_t remain_evict_page_num = 0;
    if (lock_last_page) {
      remain_evict_page_num = flushed_data_page_num_;
    } else if (need_to_evict_last_page) { // need_to_evict_last_page && !lock_last_page
      remain_evict_page_num = flushed_data_page_num_ - 1;
    } else {
      remain_evict_page_num = MIN(flushed_data_page_num_, expected_evict_page_num);
    }

    const int64_t evict_end_virtual_id = begin_page_virtual_id_ + remain_evict_page_num;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(page_idx_cache_.truncate(evict_end_virtual_id))) {
      LOG_WARN("fail to truncate page idx cache", KR(ret), K(fd_), K(evict_end_virtual_id), KPC(this));
    }

    // evict data pages
    const bool evict_last_page = lock_last_page;
    while (OB_SUCC(ret) && remain_evict_page_num > 0) {
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      uint32_t first_cached_page_idx = ObTmpFileGlobal::INVALID_PAGE_ID;

      if (OB_UNLIKELY(!wbp_->is_cached(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the page is not cached", KR(ret), K(fd_), K(begin_page_id_), K(begin_page_virtual_id_), KPC(this));
      } else if (OB_FAIL(wbp_->free_page(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
        LOG_WARN("fail to free page", KR(ret), K(fd_), K(begin_page_id_), K(begin_page_virtual_id_), K(next_page_id), KPC(this));
      } else {
        if (begin_page_id_ == flushed_page_id_) {
          flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        }
        begin_page_id_ = next_page_id;
        if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_) {
          end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        } else {
          begin_page_virtual_id_ += 1;
        }
        actual_evict_page_num++;
        remain_evict_page_num--;
        flushed_data_page_num_--;
        cached_page_nums_--;
      }
    } // end while

    if (OB_FAIL(ret)) {
    } else if (flushed_data_page_num_ > 0) {
      if (OB_FAIL(eviction_mgr_->add_file(false/*is_meta*/, *this))) {
        LOG_WARN("fail to add file to eviction mgr", KR(ret), KPC(this));
      }
    } else {
      is_in_data_eviction_list_ = false;
      LOG_DEBUG("all data pages of file have been evicted", KPC(this));
    }

    if (OB_SUCC(ret)) {
      remain_flushed_page_num = flushed_data_page_num_;
    }

    if (lock_last_page) {
      last_page_lock_.unlock();
    }

    LOG_DEBUG("evict data page of tmp file over", KR(ret), K(fd_), K(is_deleting_),
              K(expected_evict_page_num), K(actual_evict_page_num),
              K(remain_flushed_page_num),
              K(begin_page_id_), K(begin_page_virtual_id_),
              K(flushed_page_id_), K(flushed_page_virtual_id_), KPC(this));
  }

  return ret;
}

// Attention!!!!
// in order to prevent concurrency problems of eviction list
// from the operation from eviction manager and flush manager,
// before eviction manager calls this function,
// it removes the file's node from its' eviction list,
// but still keeps the file's `is_in_meta_eviction_list_` be true.
// when this function run over,
// if `remain_flushed_page_num` > 0, this function will reinsert node into the list of eviction manager again;
// otherwise, this function will set `is_in_meta_eviction_list_` be false.
int ObSharedNothingTmpFile::evict_meta_pages(const int64_t expected_evict_page_num,
                                             int64_t &actual_evict_page_num)
{
  int ret = OB_SUCCESS;
  // TODO: wanyue.wy, wuyuefei.wyf
  // if meta_tree_ could protect the consistency by itself, this lock could be removed
  common::TCRWLock::WLockGuard meta_lock_guard(meta_lock_);
  actual_evict_page_num = 0;
  int64_t total_need_evict_page_num = 0;
  int64_t total_need_evict_rightmost_page_num = 0;
  int64_t remain_need_evict_page_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedNothingTmpFile has not been inited", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // actual_evict_page_num = 0;
    is_in_meta_eviction_list_ = false;
    LOG_INFO("try to evict data pages when file is deleting", K(fd_), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(expected_evict_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(expected_evict_page_num));
  } else if (OB_UNLIKELY(!is_in_meta_eviction_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN( "the file is not in meta eviction list", K(fd_), K(is_in_meta_eviction_list_), KPC(this));
  } else if (OB_FAIL(meta_tree_.get_need_evict_page_num(total_need_evict_page_num, total_need_evict_rightmost_page_num))) {
    LOG_WARN( "fail to get need evict page num", KR(ret), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(total_need_evict_page_num <= 0)) {
    is_in_meta_eviction_list_ = false;
    LOG_INFO("meta tree has no flushed pages need to be evicted", K(fd_), K(total_need_evict_page_num), KPC(this));
  } else if (OB_FAIL(meta_tree_.evict_meta_pages(expected_evict_page_num,
                          ObTmpFileTreeEvictType::FULL, actual_evict_page_num))) {
    LOG_WARN("fail to evict meta pages", KR(ret), K(fd_), K(expected_evict_page_num), KPC(this));
  } else {
    remain_need_evict_page_num = total_need_evict_page_num - actual_evict_page_num;
    if (remain_need_evict_page_num > 0) {
      if (OB_FAIL(eviction_mgr_->add_file(true/*is_meta*/, *this))) {
        LOG_WARN("fail to add file to eviction mgr", KR(ret), K(fd_), KPC(this));
      }
    } else {
      is_in_meta_eviction_list_ = false;
      LOG_DEBUG("all meta pages are evicted", KPC(this));
    }
    LOG_DEBUG("evict meta page of tmp file over", KR(ret), K(fd_), K(is_deleting_),
              K(expected_evict_page_num), K(actual_evict_page_num), K(remain_need_evict_page_num), KPC(this));
  }

  return ret;
}

// Attention!!
// 1. currently truncate() only gc the memory resource for data page. it doesn't free disk space.
//    thus, it will not modify meta tree and not affect flushing mechanism
// 2. if truncate_offset is not the begin or end offset of page,
//    we will fill zero from begin_offset to truncate_offset for truncated page in wbp.
// 3. truncate_offset is a open interval number, which means the offset before than it need to be truncated
int ObSharedNothingTmpFile::truncate(const int64_t truncate_offset)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard truncate_lock_guard(truncate_lock_);
  ObSpinLockGuard last_page_lock_guard(last_page_lock_);
  common::TCRWLock::WLockGuard guard(meta_lock_);
  int64_t wbp_begin_offset = cal_wbp_begin_offset_();
  LOG_INFO("start to truncate a temporary file", KR(ret), K(fd_), K(truncate_offset), K(wbp_begin_offset), KPC(this));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedNothingTmpFile has not been inited", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to truncate a deleting file", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(wbp_begin_offset < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected wbp begin offset", KR(ret), K(fd_), K(wbp_begin_offset), K(truncate_offset), KPC(this));
  } else if (OB_UNLIKELY(truncate_offset <= 0 || truncate_offset > file_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid truncate_offset", KR(ret), K(fd_), K(truncate_offset), K(file_size_), KPC(this));
  } else if (OB_UNLIKELY(truncate_offset <= truncated_offset_)) {
    // do nothing
  } else {
    // release disk space between truncated_offset_ and truncate_offset
    if (OB_FAIL(meta_tree_.truncate(truncated_offset_, truncate_offset))) {
      LOG_WARN("fail to truncate meta tree", KR(ret), K(fd_), K(truncated_offset_), K(truncate_offset), KPC(this));
    }

    if (FAILEDx(inner_truncate_(truncate_offset, wbp_begin_offset))) {
      LOG_WARN("fail to truncate data page", KR(ret), K(fd_), K(truncate_offset), KPC(this));
    } else {
      truncated_offset_ = truncate_offset;
      last_modify_ts_ = ObTimeUtility::current_time();
    }
  }

  LOG_INFO("truncate a temporary file over", KR(ret), K(truncate_offset), K(wbp_begin_offset), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::inner_truncate_(const int64_t truncate_offset, const int64_t wbp_begin_offset)
{
  int ret = OB_SUCCESS;
  uint32_t truncate_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (OB_UNLIKELY(wbp_begin_offset < 0 || truncate_offset <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid offset", KR(ret), K(fd_), K(wbp_begin_offset), K(truncate_offset));
  } else if (truncate_offset > wbp_begin_offset) {
    const int64_t truncate_page_virtual_id = get_page_virtual_id_from_offset_(truncate_offset,
                                                                              true /*is_open_interval*/);
    const int64_t truncate_offset_in_page = get_page_offset_from_file_or_block_offset_(truncate_offset);
    if (OB_FAIL(page_idx_cache_.binary_search(truncate_page_virtual_id, truncate_page_id))) {
      LOG_WARN("fail to find page index in array", KR(ret), K(fd_), K(truncate_page_virtual_id));
    } else if (ObTmpFileGlobal::INVALID_PAGE_ID != truncate_page_id) {
      // the page index of truncate_offset is in the range of cached page index.
      // truncate all page indexes whose offset is smaller than truncate_offset.
      const int64_t truncate_page_virtual_id_in_cache = truncate_offset_in_page == 0 ?
                                                        truncate_page_virtual_id + 1 :
                                                        truncate_page_virtual_id;
      if (OB_FAIL(page_idx_cache_.truncate(truncate_page_virtual_id_in_cache))) {
        LOG_WARN("fail to truncate page idx cache", KR(ret), K(fd_), K(truncate_page_virtual_id), K(truncate_page_virtual_id_in_cache));
      }
    } else { // ObTmpFileGlobal::INVALID_PAGE_ID == truncate_page_id
      // the page index of truncate_offset is smaller than the smallest cached page index.
      // we need to find truncate_page_id by iterating wbp_.
      if (OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, truncate_page_virtual_id, begin_page_id_, truncate_page_id))) {
        LOG_WARN("fail to get page id by virtual id", KR(ret), K(fd_), K(truncate_offset), K(begin_page_id_));
      }
    }

    if (OB_SUCC(ret)) {
      // truncate complete pages except the last page
      while(OB_SUCC(ret) && begin_page_id_ != truncate_page_id) {
        if (OB_FAIL(truncate_the_first_wbp_page_())) {
          LOG_WARN("fail to truncate first wbp page", KR(ret), K(fd_),
                   K(begin_page_id_), K(truncate_page_id), K(end_page_id_));
        }
      }

      // truncate the last page
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(truncate_page_virtual_id != begin_page_virtual_id_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected begin page virtual id", KR(ret), K(fd_), K(truncate_page_virtual_id), K(begin_page_virtual_id_));
        } else if (0 == truncate_offset_in_page) {
          if (OB_FAIL(truncate_the_first_wbp_page_())) {
            LOG_WARN("fail to truncate first wbp page", KR(ret), K(fd_),
                    K(begin_page_id_), K(truncate_offset_in_page), K(end_page_id_));
          }
        } else { // truncate_offset_in_page is in the middle of page, fill zero before the truncate_offset of page
          if (OB_FAIL(wbp_->truncate_page(fd_, truncate_page_id,
                                          ObTmpFilePageUniqKey(truncate_page_virtual_id),
                                          truncate_offset_in_page))) {
            LOG_WARN("fail to truncate page", KR(ret), K(fd_), K(truncate_page_id), K(truncate_offset_in_page));
          } else {
            LOG_INFO("truncate part of page", KR(ret), K(truncate_page_id), K(truncate_offset),
                     K(truncate_offset_in_page), K(wbp_begin_offset), KPC(this));
          }
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::truncate_the_first_wbp_page_()
{
  int ret = OB_SUCCESS;
  bool is_flushed_page = false;
  bool is_write_back_page = false;
  uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_ ||
      ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("begin_page_id_ is already INVALID", KR(ret), K(fd_), K(begin_page_id_),
             K(begin_page_virtual_id_));
  } else if (wbp_->is_cached(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_))) {
    // [begin_page_id_, flushed_page_id_] has been flushed
    is_flushed_page = true;
  } else if (wbp_->is_write_back(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_))) {
    is_write_back_page = true;
  }

  if (FAILEDx(wbp_->free_page(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
    LOG_WARN("fail to free page", KR(ret), K(fd_), K(begin_page_id_), K(begin_page_virtual_id_));
  } else {
    if (is_flushed_page) {
      if (flushed_data_page_num_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("flushed_data_page_num_ is unexpected", KR(ret), KPC(this));
      } else {
        flushed_data_page_num_--;
        if (0 == flushed_data_page_num_) {
          if (OB_UNLIKELY(flushed_page_id_ != begin_page_id_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("flushed_page_id_ or flushed_data_page_num_ is unexpected", KR(ret), KPC(this));
          } else {
            flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
            flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
            LOG_INFO("all flushed page has been truncated", KPC(this));
          }
        }
      }
    } else if (is_write_back_page) {
      write_back_data_page_num_--;
    }

    if (OB_SUCC(ret)) {
      cached_page_nums_--;
      begin_page_id_ = next_page_id;
      if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_) {
        end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
        begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      } else {
        begin_page_virtual_id_ += 1;
      }
    }
  }
  return ret;
}

void ObSharedNothingTmpFile::update_read_offset(int64_t read_offset)
{
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (read_offset > read_offset_) {
    read_offset_ = read_offset;
  }
}

void ObSharedNothingTmpFile::get_dirty_meta_page_num_with_lock(int64_t &non_rightmost_dirty_page_num, int64_t &rightmost_dirty_page_num)
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  get_dirty_meta_page_num(non_rightmost_dirty_page_num, rightmost_dirty_page_num);
}

void ObSharedNothingTmpFile::get_dirty_meta_page_num(int64_t &non_rightmost_dirty_page_num, int64_t &rightmost_dirty_page_num) const
{
  int ret = OB_SUCCESS;
  int64_t total_need_flush_page_num = 0;
  if (OB_FAIL(meta_tree_.get_need_flush_page_num(total_need_flush_page_num, rightmost_dirty_page_num))) {
    non_rightmost_dirty_page_num = 1;
    rightmost_dirty_page_num = 1;
    LOG_WARN("fail to get need flush page num", KR(ret), KPC(this));
  } else {
    non_rightmost_dirty_page_num = total_need_flush_page_num - rightmost_dirty_page_num;
  }
}

int64_t ObSharedNothingTmpFile::get_dirty_data_page_size_with_lock()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return get_dirty_data_page_size();
}

int64_t ObSharedNothingTmpFile::get_dirty_data_page_size() const
{
  int ret = OB_SUCCESS;

  int64_t dirty_size = 0;
  // cached_page_nums == flushed_data_page_num + dirty_data_page_num
  if (0 == cached_page_nums_ || flushed_data_page_num_ == cached_page_nums_) {
    dirty_size = 0;
  } else {
    if (0 == file_size_ % ObTmpFileGlobal::PAGE_SIZE) {
      dirty_size =
        (cached_page_nums_ - flushed_data_page_num_ - write_back_data_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
    } else {
      dirty_size =
        (cached_page_nums_ - flushed_data_page_num_ - write_back_data_page_num_ - 1) * ObTmpFileGlobal::PAGE_SIZE
        + file_size_ % ObTmpFileGlobal::PAGE_SIZE;
    }
  }

  if (dirty_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dirty_size is unexpected", KR(ret), K(dirty_size), KPC(this));
    dirty_size = 1;
  }
  return dirty_size;
}

int64_t ObSharedNothingTmpFile::get_file_size()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return file_size_;
}

int64_t ObSharedNothingTmpFile::cal_wbp_begin_offset()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return cal_wbp_begin_offset_();
}

int64_t ObSharedNothingTmpFile::cal_wbp_begin_offset_() const
{
  int ret = OB_SUCCESS;
  int64_t res = -1;
  if (0 == cached_page_nums_) {
    res = file_size_;
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_ ||
                         begin_page_virtual_id_ * ObTmpFileGlobal::PAGE_SIZE !=
                         get_page_end_offset_by_file_or_block_offset_(file_size_) -
                         cached_page_nums_ * ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("begin_page_offset_in_file_ is unexpected", KR(ret),
             K(begin_page_virtual_id_), K(file_size_), K(cached_page_nums_), KPC(this));
  } else {
    res = begin_page_virtual_id_ * ObTmpFileGlobal::PAGE_SIZE;
  }

  return res;
}

void ObSharedNothingTmpFile::set_read_stats_vars(const bool is_unaligned_read, const int64_t read_size)
{
  common::TCRWLock::WLockGuard guard(meta_lock_);
  read_req_cnt_++;
  if (is_unaligned_read) {
    unaligned_read_req_cnt_++;
  }
  total_read_size_ += read_size;
  last_access_ts_ = ObTimeUtility::current_time();
}

int ObSharedNothingTmpFile::copy_info_for_virtual_table(ObSNTmpFileInfo &tmp_file_info)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuardWithTimeout lock_guard(meta_lock_, 100 * 1000L, ret);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_file_info.init(trace_id_, tenant_id_,
                                        dir_id_, fd_, file_size_,
                                        truncated_offset_, is_deleting_,
                                        cached_page_nums_, write_back_data_page_num_,
                                        flushed_data_page_num_, ref_cnt_,
                                        write_req_cnt_, unaligned_write_req_cnt_,
                                        read_req_cnt_, unaligned_read_req_cnt_,
                                        total_read_size_, last_access_ts_,
                                        last_modify_ts_, birth_ts_,
                                        this, label_.ptr()))) {
    LOG_WARN("fail to init tmp_file_info", KR(ret), KPC(this));
  } else if (OB_FAIL(meta_tree_.copy_info(tmp_file_info))) {
    LOG_WARN("fail to copy tree info", KR(ret), KPC(this));
  }
  return ret;
};

int ObSharedNothingTmpFile::remove_flush_node(const bool is_meta)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (OB_FAIL(flush_prio_mgr_->remove_file(is_meta, *this))) {
    LOG_WARN("fail to remove flush node", KR(ret), K(is_meta), KPC(this));
  }
  return ret;
}

int ObSharedNothingTmpFile::reinsert_flush_node(const bool is_meta)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (OB_FAIL(reinsert_flush_node_(is_meta))) {
    LOG_WARN("fail to reinsert flush node", KR(ret), K(is_meta), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::reinsert_flush_node_(const bool is_meta)
{
  int ret = OB_SUCCESS;
  ObTmpFileNode &flush_node = is_meta ? meta_flush_node_ : data_flush_node_;

  if (OB_UNLIKELY(nullptr != flush_node.get_next())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush node should not have next", KR(ret), K(fd_), K(is_meta));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // do nothing
  } else if (is_meta) {
    int64_t non_rightmost_dirty_page_num = 0;
    int64_t rightmost_dirty_page_num = 0;
    get_dirty_meta_page_num(non_rightmost_dirty_page_num, rightmost_dirty_page_num);
    if (0 == non_rightmost_dirty_page_num && 0 == rightmost_dirty_page_num) {
      // no need to reinsert
      meta_page_flush_level_ = -1;
    } else if (OB_FAIL(flush_prio_mgr_->insert_meta_flush_list(*this, non_rightmost_dirty_page_num,
                                                               rightmost_dirty_page_num))) {
      LOG_WARN("fail to insert meta flush list", KR(ret), K(fd_),
               K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num));
    }
  } else {
    int64_t dirty_page_size = get_dirty_data_page_size();
    if (0 == dirty_page_size) {
      // no need to reinsert
      data_page_flush_level_ = -1;
    } else if (OB_FAIL(flush_prio_mgr_->insert_data_flush_list(*this, dirty_page_size))) {
      LOG_WARN("fail to insert data flush list", KR(ret), K(fd_), K(dirty_page_size));
    }
  }

  return ret;
}

bool ObSharedNothingTmpFile::is_in_meta_flush_list()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return OB_NOT_NULL(meta_flush_node_.get_next());
}

bool ObSharedNothingTmpFile::is_flushing()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return inner_flush_ctx_.is_flushing();
}

// ATTENTION! need to be protected by meta_lock_
int ObSharedNothingTmpFile::insert_or_update_data_flush_node_()
{
  int ret = OB_SUCCESS;
  if (!inner_flush_ctx_.is_flushing()) {
    int64_t dirty_data_page_size = get_dirty_data_page_size();
    if (data_page_flush_level_ >= 0 && OB_ISNULL(data_flush_node_.get_next())) {
      // flush task mgr is processing this file.
      // it will add this file to according flush list in the end
    } else if (0 == dirty_data_page_size) {
      // do nothing
    } else if (data_page_flush_level_ < 0) {
      if (OB_FAIL(flush_prio_mgr_->insert_data_flush_list(*this, dirty_data_page_size))) {
        LOG_WARN("fail to get list idx", KR(ret), K(dirty_data_page_size), KPC(this));
      }
    } else if (OB_FAIL(flush_prio_mgr_->update_data_flush_list(*this, dirty_data_page_size))) {
      LOG_WARN("fail to update flush list", KR(ret), K(dirty_data_page_size), KPC(this));
    }
  }
  return ret;
}

// ATTENTION! call this need to be protected it in meta_lock_
int ObSharedNothingTmpFile::insert_or_update_meta_flush_node_()
{
  int ret = OB_SUCCESS;
  if (!inner_flush_ctx_.is_flushing()) {
    int64_t non_rightmost_dirty_page_num = 0;
    int64_t rightmost_dirty_page_num = 0;
    get_dirty_meta_page_num(non_rightmost_dirty_page_num, rightmost_dirty_page_num);
    if (meta_page_flush_level_ >= 0 && OB_ISNULL(meta_flush_node_.get_next())) {
      // flush task mgr is processing this file.
      // it will add this file to according flush list in the end
    } else if (0 == non_rightmost_dirty_page_num + rightmost_dirty_page_num) {
      // do nothing
    } else if (meta_page_flush_level_ < 0) {
      if (OB_FAIL(flush_prio_mgr_->insert_meta_flush_list(*this, non_rightmost_dirty_page_num,
                                                          rightmost_dirty_page_num))) {
        LOG_WARN("fail to get list idx", KR(ret),
            K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num), KPC(this));
      }
    } else if (OB_FAIL(flush_prio_mgr_->update_meta_flush_list(*this, non_rightmost_dirty_page_num,
                                                               rightmost_dirty_page_num))) {
      LOG_WARN("fail to update flush list", KR(ret),
          K(non_rightmost_dirty_page_num), K(rightmost_dirty_page_num), KPC(this));
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::cal_end_position_(
    ObArray<InnerFlushInfo> &flush_infos,
    const int64_t start_pos,
    int64_t &end_pos,
    int64_t &flushed_data_page_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_pos >= flush_infos.count() || start_pos < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flushed info num",
        KR(ret), K(start_pos), K(flush_infos.count()), K(inner_flush_ctx_), KPC(this));
  } else if (OB_UNLIKELY(flush_infos[start_pos].has_data() && flush_infos[start_pos].has_meta())) {
    // we require that each flush info only represents one type of data or meta
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush info", KR(ret), K(start_pos), K(flush_infos[start_pos]), KPC(this));
  } else if (flush_infos[start_pos].update_meta_data_done_) {
    for (end_pos = start_pos; OB_SUCC(ret) && end_pos < flush_infos.count(); end_pos++) {
      if (OB_UNLIKELY(flush_infos[end_pos].has_data() && flush_infos[end_pos].has_meta()) ||
          OB_UNLIKELY(!flush_infos[end_pos].has_data() && !flush_infos[end_pos].has_meta())) {
        ret = OB_ERR_UNEXPECTED; // flush_info contains one and only one type of pages
        LOG_WARN("invalid flush info", KR(ret), K(flush_infos[end_pos]), KPC(this));
      } else if (flush_infos[end_pos].update_meta_data_done_) {
        if (flush_infos[end_pos].has_data()) {
          flushed_data_page_num += flush_infos[end_pos].flush_data_page_num_;
        }
      } else {
        break;
      }
    } // end for
  }
  return ret;
}

int ObSharedNothingTmpFile::update_meta_after_flush(const int64_t info_idx, const bool is_meta, bool &reset_ctx)
{
  int ret = OB_SUCCESS;
  reset_ctx = false;
  common::TCRWLock::WLockGuard guard(meta_lock_);
  int64_t start_pos = is_meta ? inner_flush_ctx_.meta_finished_continuous_flush_info_num_
                              : inner_flush_ctx_.data_finished_continuous_flush_info_num_;
  int64_t end_pos = start_pos;
  ObArray<InnerFlushInfo> &flush_infos_ = is_meta ? inner_flush_ctx_.meta_flush_infos_
                                                  : inner_flush_ctx_.data_flush_infos_;
  int64_t flushed_data_page_num = 0;
  if (OB_UNLIKELY(info_idx < 0 || info_idx >= flush_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid idx", KR(ret), K(info_idx), K(is_meta), KPC(this));
  } else if (FALSE_IT(flush_infos_[info_idx].update_meta_data_done_ = true)) {
  } else if (OB_FAIL(cal_end_position_(flush_infos_, start_pos, end_pos, flushed_data_page_num))) {
    LOG_WARN("fail to cal end position for update after flush", KR(ret), K(info_idx), KPC(this));
  } else if (start_pos < end_pos) { // have new continuous finished flush infos
    if (is_meta && OB_FAIL(update_meta_tree_after_flush_(start_pos, end_pos))) {
      // meta tree may be updated before file meta infos if data_flush_info IO hang or
      // data_flush_info could not insert data items
      LOG_WARN("fail to update meta tree", KR(ret), K(start_pos), K(end_pos), KPC(this));
    } else if (!is_meta && OB_FAIL(update_file_meta_after_flush_(start_pos, end_pos, flushed_data_page_num))) {
      LOG_WARN("fail to update file meta", KR(ret), K(start_pos), K(end_pos), K(flushed_data_page_num), KPC(this));
    } else {
      reset_ctx = true;
      if (is_meta) {
        inner_flush_ctx_.need_to_wait_for_the_previous_meta_flush_req_to_complete_ = true;
      } else {
        inner_flush_ctx_.need_to_wait_for_the_previous_data_flush_req_to_complete_ = true;
      }
    }
    LOG_DEBUG("update meta based a set of flush info over", KR(ret), K(info_idx), K(start_pos),
              K(end_pos), K(inner_flush_ctx_), KPC(this));
  }

  if (FAILEDx(inner_flush_ctx_.update_finished_continuous_flush_info_num(is_meta, end_pos))) {
    LOG_WARN("fail to update finished continuous flush info num", KR(ret), K(start_pos), K(end_pos), KPC(this));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (inner_flush_ctx_.is_data_finished()) {
      if (OB_ISNULL(data_flush_node_.get_next()) && OB_TMP_FAIL(reinsert_flush_node_(false /*is_meta*/))) {
        LOG_ERROR("fail to reinsert data flush node", KR(tmp_ret), K(is_meta), K(inner_flush_ctx_), KPC(this));
      }
      inner_flush_ctx_.need_to_wait_for_the_previous_data_flush_req_to_complete_ = false;
    }
    if (inner_flush_ctx_.is_meta_finished()) {
      if (OB_ISNULL(meta_flush_node_.get_next()) && OB_TMP_FAIL(reinsert_flush_node_(true /*is_meta*/))) {
        LOG_ERROR("fail to reinsert meta flush node", KR(tmp_ret), K(is_meta), K(inner_flush_ctx_), KPC(this));
      }
      inner_flush_ctx_.need_to_wait_for_the_previous_meta_flush_req_to_complete_ = false;
    }

    if (inner_flush_ctx_.is_all_finished()) {
      inner_flush_ctx_.reset();
    }
  }

  LOG_DEBUG("update_meta_after_flush finish", KR(ret), K(info_idx), K(is_meta), K(inner_flush_ctx_), KPC(this));
  return ret;
}

// 1. skip truncated flush infos
// 2. abort updating file meta for tail data page if it is appending written after generating flushing task
int ObSharedNothingTmpFile::remove_useless_page_in_data_flush_infos_(const int64_t start_pos,
                                                                     const int64_t end_pos,
                                                                     const int64_t flushed_data_page_num,
                                                                     int64_t &new_start_pos,
                                                                     int64_t &new_flushed_data_page_num)
{
  int ret = OB_SUCCESS;
  ObArray<InnerFlushInfo> &flush_infos_ = inner_flush_ctx_.data_flush_infos_;
  new_start_pos = start_pos;
  new_flushed_data_page_num = flushed_data_page_num;

  if (OB_UNLIKELY(start_pos >= end_pos || end_pos < 1 || flushed_data_page_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", KR(ret), K(fd_), K(start_pos), K(end_pos), K(flushed_data_page_num));
  } else if (start_pos >= flush_infos_.count() || end_pos > flush_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", KR(ret), K(fd_), K(start_pos), K(end_pos), K(flush_infos_), KPC(this));
  } else {
    // skip truncated flush infos
    for (int64_t i = start_pos; OB_SUCC(ret) && i < end_pos; i++) {
      int64_t flushed_start_offset = get_page_begin_offset_by_virtual_id_(flush_infos_[i].flush_virtual_page_id_);
      int64_t flushed_end_offset = flushed_start_offset +
                                   flush_infos_[i].flush_data_page_num_ * ObTmpFileGlobal::PAGE_SIZE;
      if (truncated_offset_ <= flushed_start_offset) {
        // the following flushing pages are not affected by truncate_offset
        break;
      } else if (truncated_offset_ < flushed_end_offset) {
        // although a page is truncated partly, it will also be flushed whole page
        int64_t flush_info_flushed_size = flushed_end_offset - truncated_offset_;
        int64_t flush_info_data_page_num =
                common::upper_align(flush_info_flushed_size, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
        uint32_t flush_info_virtual_page_id =
                common::lower_align(truncated_offset_, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
        new_flushed_data_page_num -= (flush_infos_[i].flush_data_page_num_ - flush_info_data_page_num);

        LOG_INFO("due to truncating, modify flush info", KR(ret), K(fd_),
                 K(start_pos), K(end_pos), K(flushed_data_page_num), K(i),
                 K(new_start_pos), K(new_flushed_data_page_num),
                 K(truncated_offset_), K(flushed_start_offset), K(flushed_end_offset), K(flush_infos_[i]),
                 K(flush_info_data_page_num), K(flush_info_virtual_page_id));
        flush_infos_[i].flush_data_page_num_ = flush_info_data_page_num;
        flush_infos_[i].flush_virtual_page_id_ = flush_info_virtual_page_id;
        break;
      } else {
        // all pages of this flush_info have been truncated, abort it
        new_start_pos++;
        new_flushed_data_page_num -= flush_infos_[i].flush_data_page_num_;
        LOG_INFO("due to truncating, abort flush info", KR(ret), K(fd_),
                 K(start_pos), K(end_pos), K(flushed_data_page_num), K(i),
                 K(new_start_pos), K(new_flushed_data_page_num),
                 K(truncated_offset_), K(flushed_start_offset), K(flushed_end_offset), K(flush_infos_[i]));
      }
    } // end for

    // abort updating file meta for tail data page
    if (OB_SUCC(ret) && new_flushed_data_page_num > 0 && new_start_pos < end_pos) {
      if (flush_infos_[end_pos - 1].file_size_ > 0 && flush_infos_[end_pos - 1].file_size_ != file_size_) {
        // the last page has been written after flush task has been generated.
        // we will discard the flushed last page when update meta data
        // (append write has correct the meta data of the last page, here only need to ignore it)

        int64_t discard_page_virtual_id = flush_infos_[end_pos - 1].flush_virtual_page_id_ +
                                          flush_infos_[end_pos - 1].flush_data_page_num_ - 1;
        uint32_t discard_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

        if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == discard_page_virtual_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("discard page virtual id is invalid", KR(ret), K(fd_),
                   K(discard_page_virtual_id), K(flush_infos_[end_pos - 1]));
        } else if (OB_FAIL(get_physical_page_id_in_wbp(discard_page_virtual_id, discard_page_id))) {
          LOG_WARN("fail to get physical page id in wbp", KR(ret), K(fd_), K(discard_page_virtual_id));
        } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == discard_page_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("discard page id is invalid", KR(ret), K(fd_), K(discard_page_id));
        } else if (OB_UNLIKELY(!wbp_->is_dirty(fd_, discard_page_id, ObTmpFilePageUniqKey(discard_page_virtual_id)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("discard page is not dirty", KR(ret), K(fd_), K(discard_page_id), K(discard_page_virtual_id));
        } else {
          new_flushed_data_page_num -= 1;
          flush_infos_[end_pos - 1].flush_data_page_num_ -= 1;
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::update_file_meta_after_flush_(const int64_t start_pos,
                                                          const int64_t end_pos,
                                                          const int64_t flushed_data_page_num)
{
  int ret = OB_SUCCESS;
  ObArray<InnerFlushInfo> &flush_infos_ = inner_flush_ctx_.data_flush_infos_;
  LOG_DEBUG("update_file_meta_after_flush start",
      KR(ret), K(start_pos), K(end_pos), K(flushed_data_page_num), KPC(this));
  int64_t new_start_pos = start_pos;
  int64_t new_flushed_data_page_num = flushed_data_page_num;

  if (OB_UNLIKELY(start_pos >= end_pos || end_pos < 1 || flushed_data_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K(fd_), K(end_pos), K(flushed_data_page_num));
  } else if (start_pos >= flush_infos_.count() || end_pos > flush_infos_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K(start_pos), K(end_pos), K(flush_infos_), KPC(this));
  } else if (OB_FAIL(remove_useless_page_in_data_flush_infos_(start_pos, end_pos, flushed_data_page_num,
                                                         new_start_pos, new_flushed_data_page_num))) {
    LOG_WARN("fail to remove useless page in flush infos", KR(ret), K(fd_), K(start_pos), K(end_pos),
             K(flushed_data_page_num), K(end_pos));
  } else if (0 == new_flushed_data_page_num) {
    // do nothing
  } else if (OB_UNLIKELY(new_flushed_data_page_num < 0 || new_start_pos >= end_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K(fd_), K(start_pos), K(end_pos), K(flushed_data_page_num),
              K(new_start_pos), K(new_flushed_data_page_num));
  } else { // exist multiple continuous pages have been flushed over
    uint32_t last_flushed_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    uint32_t cur_flush_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    int64_t cur_flush_page_virtual_id = flush_infos_[new_start_pos].flush_virtual_page_id_;
    if (OB_FAIL(get_physical_page_id_in_wbp(flush_infos_[new_start_pos].flush_virtual_page_id_, cur_flush_page_id))) {
      LOG_WARN("fail to get physical page id in wbp",
               KR(ret),K(fd_),  K(start_pos), K(end_pos), K(flushed_data_page_num),
               K(new_start_pos), K(new_flushed_data_page_num),
               K(flush_infos_[new_start_pos]));
    } else {
      int64_t write_back_succ_data_page_num = 0;

      // update each flush info
      for (int64_t i = new_start_pos; OB_SUCC(ret) && i < end_pos; ++i) {
        const InnerFlushInfo& flush_info = flush_infos_[i];
        const int64_t cur_flushed_data_page_num = flush_info.flush_data_page_num_;

        if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == cur_flush_page_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid next flush page id", KR(ret), K(fd_), K(cur_flush_page_id),
                   K(begin_page_id_), K(flushed_page_id_), K(end_page_id_));
        } else {
          int64_t cur_page_virtual_id_in_flush_info = cur_flush_page_virtual_id;
          // update each page of flush info
          for (int64_t j = 0; OB_SUCC(ret) && j < cur_flushed_data_page_num; ++j) {
            if (OB_FAIL(wbp_->notify_write_back_succ(fd_, cur_flush_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id_in_flush_info)))) {
              LOG_WARN("fail to mark page as clean", KR(ret), K(fd_),
                       K(flushed_page_id_),  K(cur_page_virtual_id_in_flush_info), K(cur_flush_page_id),
                       K(flushed_data_page_num), K(new_flushed_data_page_num));
            } else if (FALSE_IT(last_flushed_page_id = cur_flush_page_id)) {
            } else if (OB_FAIL(wbp_->get_next_page_id(fd_, cur_flush_page_id,
                    ObTmpFilePageUniqKey(cur_page_virtual_id_in_flush_info), cur_flush_page_id))) {
              LOG_WARN("fail to get next page id", KR(ret), K(fd_), K(cur_flush_page_id),
                  K(cur_page_virtual_id_in_flush_info));
            } else {
              cur_page_virtual_id_in_flush_info += 1;
            }
          } // end for
          if (OB_SUCC(ret)) {
            write_back_succ_data_page_num += cur_flushed_data_page_num;
            cur_flush_page_virtual_id += cur_flushed_data_page_num;
          }
        }
      } // end for

      // update file meta
      if (OB_SUCC(ret)) {
        if (write_back_succ_data_page_num != new_flushed_data_page_num) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("write_back_succ_data_page_num not correct",
              KR(ret), K(fd_), K(write_back_succ_data_page_num),
              K(flushed_data_page_num), K(new_flushed_data_page_num), KPC(this));
        } else {
          flushed_page_id_ = last_flushed_page_id;
          flushed_page_virtual_id_ = cur_flush_page_virtual_id - 1;
          flushed_data_page_num_ += new_flushed_data_page_num;
          write_back_data_page_num_ -= new_flushed_data_page_num;
          if (write_back_data_page_num_ < 0) {
            int tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("write_back_data_page_num_  is unexpected", KR(tmp_ret), K(fd_),
                K(write_back_data_page_num_), K(flushed_data_page_num), K(new_flushed_data_page_num), KPC(this));
            write_back_data_page_num_ = 0;
          }

          if (!is_deleting_ && !is_in_data_eviction_list_ && OB_ISNULL(data_eviction_node_.get_next())) {
            if (OB_FAIL(eviction_mgr_->add_file(false/*is_meta*/, *this))) {
              LOG_WARN("fail to insert into eviction list", KR(ret), K(fd_));
            } else {
              is_in_data_eviction_list_ = true;
            }
          }
        }
      } // update file meta over
    }
  } // handle continuous success flush infos over

  LOG_DEBUG("update_file_meta_after_flush finish", KR(ret), K(fd_),
            K(start_pos), K(end_pos), K(flushed_data_page_num),
            K(new_start_pos), K(new_flushed_data_page_num), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::update_meta_tree_after_flush_(const int64_t start_pos, const int64_t end_pos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObArray<InnerFlushInfo> &flush_infos_ = inner_flush_ctx_.meta_flush_infos_;
  for (int64_t i = start_pos; i < end_pos; i++) {
    // ATTENTION! need to alloc memory inside, caller must retry update meta data if alloc fail
    if (OB_FAIL(meta_tree_.update_after_flush(flush_infos_[i].flush_meta_page_array_))) {
      LOG_ERROR("fail to update meta items", KR(ret), K(fd_), K(flush_infos_[i]), KPC(this));
    } else {
      LOG_INFO("succ to update meta items", KR(ret), K(fd_), K(flush_infos_[i]), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_deleting_ && !is_in_meta_eviction_list_ && OB_ISNULL(meta_eviction_node_.get_next())) {
      int64_t total_need_evict_page_num = 1;
      int64_t total_need_evict_rightmost_page_num = 1;
      if (OB_TMP_FAIL(meta_tree_.get_need_evict_page_num(total_need_evict_page_num,
                                                         total_need_evict_rightmost_page_num))) {
        LOG_ERROR("fail to get_need_evict_page_num", KR(tmp_ret),
            K(total_need_evict_page_num), K(total_need_evict_rightmost_page_num), KPC(this));
      }

      if (total_need_evict_page_num > 0) {
        if (OB_FAIL(eviction_mgr_->add_file(true/*is_meta*/, *this))) {
          LOG_WARN("fail to insert into eviction list", KR(ret), K(fd_));
        } else {
          is_in_meta_eviction_list_ = true;
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::generate_data_flush_info_(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileDataFlushContext &data_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;

  uint32_t copy_begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  int64_t copy_begin_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;

  uint32_t copy_end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  if (OB_FAIL(cal_next_flush_page_id_from_flush_ctx_or_file_(data_flush_context,
                                                             copy_begin_page_id,
                                                             copy_begin_page_virtual_id))) {
    LOG_WARN("fail to calculate next_flush_page_id", KR(ret),
        K(flush_task), K(info), K(data_flush_context), KPC(this));
  } else if (ObTmpFileGlobal::INVALID_PAGE_ID == copy_begin_page_id) {
    ret = OB_ITER_END;
    LOG_DEBUG("no more data to flush", KR(ret), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == copy_begin_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("next_flush_page_virtual_id is invalid", KR(ret), K(fd_), K(copy_begin_page_id),
             K(copy_begin_page_virtual_id), K(flush_task), K(info), K(data_flush_context),
             K(flush_sequence), K(need_flush_tail), KPC(this));
  } else if (OB_FAIL(get_flush_end_page_id_(copy_end_page_id, need_flush_tail))) {
    LOG_WARN("fail to get_flush_end_page_id_", KR(ret),
        K(flush_task), K(info), K(data_flush_context), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_FLUSH_SEQUENCE != inner_flush_ctx_.flush_seq_
              && flush_sequence != inner_flush_ctx_.flush_seq_
              && flush_sequence != flush_task.get_flush_seq())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush sequence not match",
        KR(ret), K(inner_flush_ctx_.flush_seq_), K(flush_task), KPC(this));
  } else if (OB_FAIL(copy_flush_data_from_wbp_(flush_task, info, data_flush_context,
                                               copy_begin_page_id, copy_begin_page_virtual_id,
                                               copy_end_page_id,
                                               flush_sequence, need_flush_tail))) {
    LOG_WARN("fail to copy flush data from wbp", KR(ret),
        K(flush_task), K(info), K(data_flush_context), KPC(this));
  }

  LOG_DEBUG("generate_data_flush_info_ end",
      KR(ret), K(fd_), K(data_flush_context), K(info), K(flush_task), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::generate_data_flush_info(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileDataFlushContext &data_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;
  info.reset();

  if (!truncate_lock_.try_rdlock()) {
    ret = OB_ITER_END;
    LOG_WARN("fail to get truncate lock", KR(ret), K(fd_), KPC(this));
  } else {
    common::TCRWLock::RLockGuard guard(meta_lock_);
    if (inner_flush_ctx_.need_to_wait_for_the_previous_data_flush_req_to_complete_) {
      ret = OB_ITER_END;
      LOG_INFO("need_to_wait_for_the_previous_data_flush_req_to_complete_", KR(ret), K(fd_), KPC(this));
    } else if (OB_FAIL(generate_data_flush_info_(flush_task, info,
                                                 data_flush_context, flush_sequence, need_flush_tail))) {
      STORAGE_LOG(WARN, "fail to generate_data_flush_info_", KR(ret), K(flush_task),
                  K(info), K(data_flush_context), K(flush_sequence), K(need_flush_tail), KPC(this));
    }
    if (OB_FAIL(ret)) {
      truncate_lock_.unlock();
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::copy_flush_data_from_wbp_(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileDataFlushContext &data_flush_context,
    const uint32_t copy_begin_page_id,
    const int64_t copy_begin_page_virtual_id,
    const uint32_t copy_end_page_id,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;
  char *buf = flush_task.get_data_buf();
  int64_t write_offset = flush_task.get_total_page_num() * ObTmpFileGlobal::PAGE_SIZE;

  bool has_last_page_lock = false;
  int64_t next_disk_page_id = flush_task.get_next_free_page_id();
  int64_t flushing_page_num = 0;
  int64_t origin_info_num = inner_flush_ctx_.data_flush_infos_.size();
  int64_t cur_page_file_offset = -1;
  uint32_t cur_flushed_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t cur_page_id = copy_begin_page_id;
  int64_t cur_page_virtual_id = copy_begin_page_virtual_id;

  if (OB_ISNULL(buf) || OB_UNLIKELY(OB_STORAGE_OBJECT_MGR.get_macro_object_size() <= write_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf or write_offset", KR(ret), KP(buf), K(write_offset), K(flush_task), KPC(this));
  } else if (OB_FAIL(inner_flush_ctx_.data_flush_infos_.push_back(InnerFlushInfo()))) {
    LOG_WARN("fail to push back empty flush info", KR(ret), K(fd_), K(info), K(flush_task), KPC(this));
  }
  while (OB_SUCC(ret) && cur_page_id != copy_end_page_id && write_offset < OB_STORAGE_OBJECT_MGR.get_macro_object_size()) {
    if (need_flush_tail && cur_page_id == end_page_id_ && file_size_ % ObTmpFileGlobal::PAGE_SIZE != 0) {
      if (OB_SUCC(last_page_lock_.trylock())) {
        has_last_page_lock = true;
      } else {
        LOG_WARN("fail to get last page lock", KR(ret), K(fd_));
        ret = OB_SUCCESS; // ignore error to continue flushing the copied data
        break;
      }
    }
    char *page_buf = nullptr;
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    bool original_state_is_dirty = wbp_->is_dirty(fd_, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    if (OB_FAIL(wbp_->read_page(fd_, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), page_buf, next_page_id))) {
      LOG_WARN("fail to read page", KR(ret), K(fd_), K(cur_page_id));
    } else if (OB_FAIL(wbp_->notify_write_back(fd_, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id)))) {
      LOG_WARN("fail to notify write back", KR(ret), K(fd_), K(cur_page_id));
    } else if (OB_UNLIKELY(!flush_task.check_buf_range_valid(buf, ObTmpFileGlobal::PAGE_SIZE))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid buffer range", KR(ret), K(fd_), K(write_offset), KP(buf));
    } else {
      // ObTmpPageCacheKey cache_key(flush_task.get_block_index(),
      //                             write_offset / ObTmpFileGlobal::PAGE_SIZE, tenant_id_);
      // ObTmpPageCacheValue cache_value(page_buf);
      // ObTmpPageCache::get_instance().try_put_page_to_cache(cache_key, cache_value);

      MEMCPY(buf + write_offset, page_buf, ObTmpFileGlobal::PAGE_SIZE);
      write_offset += ObTmpFileGlobal::PAGE_SIZE;
      flushing_page_num += 1;
      cur_flushed_page_id = cur_page_id;
      cur_page_id = next_page_id;
      cur_page_virtual_id += 1;
      if (original_state_is_dirty) {
        write_back_data_page_num_++;
      }
    }
  }

  if (OB_SUCC(ret) && 0 == flushing_page_num) {
    ret = OB_ITER_END;
  }
  if (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID == cur_flushed_page_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cur_flushed_page_id is unexpected", KR(ret), K(cur_flushed_page_id), KPC(this));
  }

  if (OB_SUCC(ret)) {
    int64_t flush_info_idx = inner_flush_ctx_.data_flush_infos_.size() - 1;
    // set flush_info in flush_task
    info.flush_data_page_disk_begin_id_ = next_disk_page_id;
    info.flush_data_page_num_ = flushing_page_num;
    info.batch_flush_idx_ = flush_info_idx;
    info.flush_virtual_page_id_ = copy_begin_page_virtual_id;
    info.has_last_page_lock_ = has_last_page_lock;
    // record file_size to check if the last page is appended while flushing
    if (has_last_page_lock) {
      info.file_size_ = file_size_;
    }

    info.fd_ = fd_;
    // set flush_info in file inner_flush_ctx
    if (OB_FAIL(info.file_handle_.init(this))) {
      LOG_WARN("fail to init tmp file handle", KR(ret), K(fd_), K(flush_task), KPC(this));
    } else if (OB_FAIL(inner_flush_ctx_.data_flush_infos_.at(flush_info_idx).init_by_tmp_file_flush_info(info))) {
      LOG_WARN("fail to init_by_tmp_file_flush_info", KR(ret), K(fd_), K(flush_task), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    // maintain flushed_page_id recorded in flush_mgr
    data_flush_context.set_flushed_page_id(cur_flushed_page_id);
    data_flush_context.set_flushed_page_virtual_id(cur_page_virtual_id - 1);
    data_flush_context.set_next_flush_page_id(cur_page_id);
    data_flush_context.set_next_flush_page_virtual_id(cur_page_virtual_id);
    data_flush_context.set_is_valid(true);
    if (has_last_page_lock) {
      data_flush_context.set_has_flushed_last_partially_written_page(true);
    }
    flush_task.set_data_length(write_offset);

    inner_flush_ctx_.flush_seq_ = flush_sequence;
  } else {
    LOG_WARN("fail to generate data flush info", KR(ret), K(fd_), K(need_flush_tail),
        K(flush_sequence), K(data_flush_context), K(info), K(flush_task), KPC(this));
    if (inner_flush_ctx_.data_flush_infos_.size() == origin_info_num + 1) {
      inner_flush_ctx_.data_flush_infos_.pop_back();
    }
    if (has_last_page_lock) {
      last_page_lock_.unlock();
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::generate_meta_flush_info_(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileTreeFlushContext &meta_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;

  ObArray<InnerFlushInfo> &flush_infos_ = inner_flush_ctx_.meta_flush_infos_;
  int64_t origin_info_num = flush_infos_.size();

  const int64_t block_index = flush_task.get_block_index();
  char *buf = flush_task.get_data_buf();
  int64_t write_offset = flush_task.get_total_page_num() * ObTmpFileGlobal::PAGE_SIZE;

  ObTmpFileTreeEvictType flush_type = need_flush_tail ?
                                      ObTmpFileTreeEvictType::FULL : ObTmpFileTreeEvictType::MAJOR;
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_FLUSH_SEQUENCE != inner_flush_ctx_.flush_seq_
        && flush_sequence != inner_flush_ctx_.flush_seq_
        && flush_sequence != flush_task.get_flush_seq())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush sequence not match", KR(ret), K(flush_sequence), K(inner_flush_ctx_.flush_seq_),
             K(flush_task), KPC(this));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(OB_STORAGE_OBJECT_MGR.get_macro_object_size() <= write_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf or write_offset", KR(ret), KP(buf), K(write_offset), K(flush_task), KPC(this));
  } else if (OB_FAIL(flush_infos_.push_back(InnerFlushInfo()))) {
    LOG_WARN("fail to push back empty flush info", KR(ret), K(fd_), K(info), K(flush_task), KPC(this));
  } else if (OB_FAIL(meta_tree_.flush_meta_pages_for_block(block_index, flush_type, buf, write_offset,
                                                           meta_flush_context, info.flush_meta_page_array_))) {
    LOG_WARN("fail to flush meta pages for block", KR(ret), K(fd_), K(flush_task), K(meta_flush_context), KPC(this));
  } else if (0 == info.flush_meta_page_array_.count()) {
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    int64_t flush_info_idx = flush_infos_.size() - 1;
    info.batch_flush_idx_ = flush_info_idx;

    info.fd_ = fd_;
    // set flush_info in flush_task
    if (OB_FAIL(info.file_handle_.init(this))) {
      LOG_WARN("fail to init tmp file handle", KR(ret), K(fd_), K(flush_task), KPC(this));
    } else if (OB_FAIL(inner_flush_ctx_.meta_flush_infos_.at(flush_info_idx).init_by_tmp_file_flush_info(info))) {
      LOG_WARN("fail to init_by_tmp_file_flush_info", KR(ret), K(fd_), K(flush_task), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    flush_task.set_data_length(write_offset);
    inner_flush_ctx_.flush_seq_ = flush_sequence;
  } else {
    LOG_WARN("fail to generate meta flush info", KR(ret), K(fd_), K(flush_task),
        K(meta_flush_context), K(flush_sequence), K(need_flush_tail));
    if (flush_infos_.size() == origin_info_num + 1) {
      flush_infos_.pop_back();
    }
  }

  LOG_INFO("generate_meta_flush_info_ end", KR(ret), K(fd_), K(need_flush_tail),
           K(inner_flush_ctx_), K(meta_flush_context), K(info), K(flush_task), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::generate_meta_flush_info(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileTreeFlushContext &meta_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;
  info.reset();

  common::TCRWLock::RLockGuard guard(meta_lock_);
  if (inner_flush_ctx_.need_to_wait_for_the_previous_meta_flush_req_to_complete_) {
    ret = OB_ITER_END;
    LOG_INFO("need_to_wait_for_the_previous_meta_flush_req_to_complete_", KR(ret), K(fd_), KPC(this));
  } else if (OB_FAIL(generate_meta_flush_info_(flush_task, info,
                                               meta_flush_context, flush_sequence, need_flush_tail))) {
    STORAGE_LOG(WARN, "fail to generate_meta_flush_info_", KR(ret), K(flush_task),
                K(info), K(meta_flush_context), K(flush_sequence), K(need_flush_tail), KPC(this));
  }

  return ret;
}

int ObSharedNothingTmpFile::insert_meta_tree_item(const ObTmpFileFlushInfo &info, int64_t block_index)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (info.has_data()) {
    ObSharedNothingTmpFileDataItem data_item;
    data_item.block_index_ = block_index;
    data_item.physical_page_id_ = info.flush_data_page_disk_begin_id_;
    data_item.physical_page_num_ = info.flush_data_page_num_;
    data_item.virtual_page_id_ = info.flush_virtual_page_id_;
    ObSEArray<ObSharedNothingTmpFileDataItem, 1> data_items;

    if (OB_FAIL(data_items.push_back(data_item))) {
      LOG_WARN("fail to push back data item", KR(ret), K(info), K(block_index), KPC(this));
    } else if (OB_FAIL(meta_tree_.prepare_for_insert_items())) {
      LOG_WARN("fail to prepare for insert items", KR(ret), K(info), K(block_index), KPC(this));
    } else if (OB_FAIL(meta_tree_.insert_items(data_items))) {
      LOG_WARN("fail to insert data items", KR(ret), K(info), K(block_index), KPC(this));
    }

    if (OB_SUCC(ret) && info.has_last_page_lock_) {
      last_page_lock_.unlock();
    }

    if (OB_SUCC(ret)) {
      truncate_lock_.unlock();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush info does not contain data info", KR(ret), K(info), KPC(this));
  }

  if (!is_deleting_ && !is_in_meta_eviction_list_ && OB_ISNULL(meta_eviction_node_.get_next())) {
    int64_t total_need_evict_page_num = 1;
    int64_t total_need_evict_rightmost_page_num = 1;
    if (OB_TMP_FAIL(meta_tree_.get_need_evict_page_num(total_need_evict_page_num,
                                                       total_need_evict_rightmost_page_num))) {
      LOG_ERROR("fail to get_need_evict_page_num", KR(tmp_ret),
          K(total_need_evict_page_num), K(total_need_evict_rightmost_page_num), KPC(this));
    }

    if (total_need_evict_page_num > 0) {
      if (OB_TMP_FAIL(eviction_mgr_->add_file(true/*is_meta*/, *this))) {
        LOG_WARN("fail to insert into eviction list", KR(ret), K(fd_), KPC(this));
      } else {
        is_in_meta_eviction_list_ = true;
      }
    }
  }

  // reinsert meta flush node during flushing to allow meta pages to be flushed if
  // insert_meta_tree_item need tp allocate new meta pages
  if (!is_deleting_ && OB_ISNULL(meta_flush_node_.get_next())) {
    if (OB_TMP_FAIL(reinsert_flush_node_(true/*is_meta*/))) {
      LOG_WARN("fail to reinsert flush node", KR(ret), K(fd_), K(info), K(block_index), KPC(this));
    }
  }

  LOG_DEBUG("insert_meta_tree_item end", KR(ret), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::cal_next_flush_page_id_from_flush_ctx_or_file_(
    const ObTmpFileDataFlushContext &data_flush_context,
    uint32_t &next_flush_page_id,
    int64_t &next_flush_page_virtual_id)
{
  int ret = OB_SUCCESS;
  if (data_flush_context.is_valid()) {
    // use next_flush_page_id from data_flush_ctx
    if (data_flush_context.has_flushed_last_partially_written_page()) {
      next_flush_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      next_flush_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    } else {
      next_flush_page_id = data_flush_context.get_next_flush_page_id();
      next_flush_page_virtual_id = data_flush_context.get_next_flush_page_virtual_id();
      int64_t truncate_page_virtual_id = get_page_virtual_id_from_offset_(truncated_offset_,
                                                                          false /*is_open_interval*/);
      if (ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != truncate_page_virtual_id &&
          truncate_page_virtual_id > data_flush_context.get_flushed_page_virtual_id()) {
        if (OB_FAIL(get_next_flush_page_id_(next_flush_page_id, next_flush_page_virtual_id))) {
          LOG_ERROR("origin next flush page has been truncated, fail to get_next_flush_page_id_",
              KR(ret), K(data_flush_context), KPC(this));
        } else {
          LOG_INFO("origin next flush page has been truncated",
              KR(ret), K(next_flush_page_id), K(next_flush_page_virtual_id), K(data_flush_context), KPC(this));
        }
      }

      if (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != truncate_page_virtual_id &&
          truncate_page_virtual_id <= data_flush_context.get_flushed_page_virtual_id()) {
        uint32_t last_flushed_page_id = data_flush_context.get_flushed_page_id();
        int64_t last_flushed_page_virtual_id = data_flush_context.get_flushed_page_virtual_id();
        if (ObTmpFileGlobal::INVALID_PAGE_ID != last_flushed_page_id) {
          if (!wbp_->is_write_back(fd_, last_flushed_page_id, ObTmpFilePageUniqKey(last_flushed_page_virtual_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("last flushed page state not match",
                KR(ret), K(last_flushed_page_id), K(data_flush_context), KPC(this));
          }
        }
      }
    }
  } else {
    // cal next_flush_page_id from file meta when doing flush for the first time
    if (OB_FAIL(get_next_flush_page_id_(next_flush_page_id, next_flush_page_virtual_id))) {
      LOG_WARN("fail to get_next_flush_page_id_", KR(ret), K(fd_), K(data_flush_context));
    }
  }

  if (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID != next_flush_page_id) {
    if (!wbp_->is_dirty(fd_, next_flush_page_id, ObTmpFilePageUniqKey(next_flush_page_virtual_id)) &&
        !wbp_->is_write_back(fd_, next_flush_page_id, ObTmpFilePageUniqKey(next_flush_page_virtual_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("next_flush_page_id state not validate",
          KR(ret), K(next_flush_page_id), K(data_flush_context), KPC(this));
    }
  }

  return ret;
}

// output next page id after flushed_page_id_ in normal case，or output flushed_page_id for write operation appending last page
int ObSharedNothingTmpFile::get_next_flush_page_id_(uint32_t& next_flush_page_id, int64_t& next_flush_page_virtual_id) const
{
  int ret = OB_SUCCESS;
  next_flush_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  next_flush_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  if (ObTmpFileGlobal::INVALID_PAGE_ID == flushed_page_id_) {
    // all pages are dirty
    if (OB_UNLIKELY((ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_ &&
                     ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != begin_page_virtual_id_) ||
                    (ObTmpFileGlobal::INVALID_PAGE_ID != begin_page_id_ &&
                     ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_) )) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("begin_page_id_ and begin_page_virtual_id_ not match", KR(ret), K(begin_page_id_), K(begin_page_virtual_id_), KPC(this));
    } else {
      next_flush_page_id = begin_page_id_;
      next_flush_page_virtual_id = begin_page_virtual_id_;
    }
  } else if (wbp_->is_dirty(fd_, flushed_page_id_, ObTmpFilePageUniqKey(flushed_page_virtual_id_))) {
    // start from flushed_page_id_ if flushed_page_id_ is dirty caused by appending write
    if (OB_UNLIKELY((ObTmpFileGlobal::INVALID_PAGE_ID == flushed_page_id_ &&
                     ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != flushed_page_virtual_id_) ||
                    (ObTmpFileGlobal::INVALID_PAGE_ID != flushed_page_id_ &&
                     ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == flushed_page_virtual_id_) )) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("flushed_page_id_ and flushed_page_virtual_id_ not match",
                KR(ret), K(flushed_page_id_), K(flushed_page_virtual_id_), KPC(this));
    } else {
      next_flush_page_id = flushed_page_id_;
      next_flush_page_virtual_id = flushed_page_virtual_id_;
    }
  } else if (OB_FAIL(wbp_->get_next_page_id(fd_, flushed_page_id_, ObTmpFilePageUniqKey(flushed_page_virtual_id_), next_flush_page_id))){
    // start from the next page, could return INVALID_PAGE_ID if flushed_page_id_ == end_page_id_
    LOG_WARN("fail to get next page id", KR(ret), K(fd_), K(begin_page_id_), K(flushed_page_id_), K(end_page_id_));
  } else if (ObTmpFileGlobal::INVALID_PAGE_ID == next_flush_page_id) {
    next_flush_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  } else {
    next_flush_page_virtual_id = flushed_page_virtual_id_ + 1;
  }

  LOG_DEBUG("get_next_flush_page_id_", KR(ret), K(fd_), K(begin_page_id_), K(flushed_page_id_), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::get_physical_page_id_in_wbp(const int64_t virtual_page_id, uint32_t& page_id) const
{
  int ret = OB_SUCCESS;
  int64_t end_page_virtual_id = cached_page_nums_ == 0 ?
                                ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID :
                                get_page_virtual_id_from_offset_(file_size_, true /*is_open_interval*/);
  page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == virtual_page_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(virtual_page_id));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_ ||
                         ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_ ||
                         ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == end_page_virtual_id ||
                         ObTmpFileGlobal::INVALID_PAGE_ID == end_page_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no pages exist in wbp", KR(ret), K(begin_page_id_), K(begin_page_virtual_id_),
             K(end_page_id_), K(end_page_virtual_id), KPC(this));
  } else if (OB_UNLIKELY(virtual_page_id < begin_page_virtual_id_ || virtual_page_id > end_page_virtual_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the page doesn't exist in wbp", KR(ret), K(virtual_page_id), K(end_page_virtual_id), K(begin_page_virtual_id_), KPC(this));
  } else if (virtual_page_id == begin_page_virtual_id_) {
    page_id = begin_page_id_;
  } else if (ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == flushed_page_virtual_id_ ||
      virtual_page_id < flushed_page_virtual_id_) {
    if (OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, virtual_page_id, begin_page_id_, page_id))) {
      LOG_WARN("fail to get page id by virtual id", KR(ret), K(virtual_page_id), K(begin_page_id_), KPC(this));
    }
  } else if (virtual_page_id == flushed_page_virtual_id_) {
    page_id = flushed_page_id_;
  } else if (virtual_page_id == end_page_virtual_id) {
    page_id = end_page_id_;
  } else { // virtual_page_id < end_page_virtual_id
    if (OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, virtual_page_id, flushed_page_id_, page_id))) {
      LOG_WARN("fail to get page id by virtual id", KR(ret), K(virtual_page_id), K(flushed_page_id_), KPC(this));
    }
  }

  return ret;
}

// output the last page to be flushed
int ObSharedNothingTmpFile::get_flush_end_page_id_(uint32_t& flush_end_page_id, const bool need_flush_tail) const
{
  int ret = OB_SUCCESS;
  const int64_t INVALID_PAGE_ID = ObTmpFileGlobal::INVALID_PAGE_ID;
  flush_end_page_id = INVALID_PAGE_ID;
  if (file_size_ % ObTmpFileGlobal::PAGE_SIZE == 0) {
    flush_end_page_id = INVALID_PAGE_ID;
  } else { // determined whether to flush last page based on flag if last page is not full
    flush_end_page_id = need_flush_tail ? INVALID_PAGE_ID : end_page_id_;
  }

  LOG_DEBUG("get_flush_end_page_id_", K(fd_), K(need_flush_tail), K(flush_end_page_id), KPC(this));
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
