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

#include "storage/tmp_file/ob_i_tmp_file.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "storage/tmp_file/ob_tmp_file_flush_priority_manager.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_tmp_file_io_ctx.h"

namespace oceanbase
{
namespace tmp_file
{

int ObTmpFileInfo::init(
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
    const int64_t birth_ts,
    const void* const tmp_file_ptr,
    const char* const label,
    const int64_t write_req_cnt,
    const int64_t unaligned_write_req_cnt,
    const int64_t write_persisted_tail_page_cnt,
    const int64_t lack_page_cnt,
    const int64_t last_modify_ts,
    const int64_t read_req_cnt,
    const int64_t unaligned_read_req_cnt,
    const int64_t total_truncated_page_read_cnt,
    const int64_t total_kv_cache_page_read_cnt,
    const int64_t total_uncached_page_read_cnt,
    const int64_t total_wbp_page_read_cnt,
    const int64_t truncated_page_read_hits,
    const int64_t kv_cache_page_read_hits,
    const int64_t uncached_page_read_hits,
    const int64_t wbp_page_read_hits,
    const int64_t total_read_size,
    const int64_t last_access_ts)
{
  int ret = OB_SUCCESS;
  // common info
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
  birth_ts_ = birth_ts;
  tmp_file_ptr_ = tmp_file_ptr;
  if (NULL != label) {
    label_.assign_strive(label);
  }

  // write info
  write_info_.write_req_cnt_ = write_req_cnt;
  write_info_.unaligned_write_req_cnt_ = unaligned_write_req_cnt;
  write_info_.write_persisted_tail_page_cnt_ = write_persisted_tail_page_cnt;
  write_info_.lack_page_cnt_ = lack_page_cnt;
  write_info_.last_modify_ts_ = last_modify_ts;

  // read info
  read_info_.read_req_cnt_ = read_req_cnt;
  read_info_.unaligned_read_req_cnt_ = unaligned_read_req_cnt;
  read_info_.total_truncated_page_read_cnt_ = total_truncated_page_read_cnt;
  read_info_.total_kv_cache_page_read_cnt_ = total_kv_cache_page_read_cnt;
  read_info_.total_uncached_page_read_cnt_ = total_uncached_page_read_cnt;
  read_info_.total_wbp_page_read_cnt_ = total_wbp_page_read_cnt;
  read_info_.truncated_page_read_hits_ = truncated_page_read_hits;
  read_info_.kv_cache_page_read_hits_ = kv_cache_page_read_hits;
  read_info_.uncached_page_read_hits_ = uncached_page_read_hits;
  read_info_.wbp_page_read_hits_ = wbp_page_read_hits;
  read_info_.total_read_size_ = total_read_size;
  read_info_.last_access_ts_ = last_access_ts;
  return ret;
}

void ObTmpFileInfo::reset()
{
  // common info
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
  birth_ts_ = -1;
  tmp_file_ptr_ = nullptr;
  label_.reset();
  write_info_.reset();
  read_info_.reset();
}

void ObTmpFileInfo::ObTmpFileWriteInfo::reset()
{
  write_req_cnt_ = 0;
  unaligned_write_req_cnt_ = 0;
  write_persisted_tail_page_cnt_ = 0;
  lack_page_cnt_ = 0;
  last_modify_ts_ = -1;
}

void ObTmpFileInfo::ObTmpFileReadInfo::reset()
{
  read_req_cnt_ = 0;
  unaligned_read_req_cnt_ = 0;
  total_truncated_page_read_cnt_ = 0;
  total_kv_cache_page_read_cnt_ = 0;
  total_uncached_page_read_cnt_ = 0;
  total_wbp_page_read_cnt_ = 0;
  truncated_page_read_hits_ = 0;
  kv_cache_page_read_hits_ = 0;
  uncached_page_read_hits_ = 0;
  wbp_page_read_hits_ = 0;
  total_read_size_ = 0;
  last_access_ts_ = -1;
}

ObITmpFileHandle::ObITmpFileHandle(ObITmpFile *tmp_file)
  : ptr_(tmp_file)
{
  if (ptr_ != nullptr) {
    ptr_->inc_ref_cnt();
  }
}

ObITmpFileHandle::ObITmpFileHandle(const ObITmpFileHandle &handle)
  : ptr_(nullptr)
{
  operator=(handle);
}

ObITmpFileHandle & ObITmpFileHandle::operator=(const ObITmpFileHandle &other)
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

void ObITmpFileHandle::reset()
{
  if (ptr_ != nullptr) {
    ptr_->dec_ref_cnt();
    if (ptr_->get_ref_cnt() == 0) {
      ptr_->~ObITmpFile();
    }
    ptr_ = nullptr;
  }
}

int ObITmpFileHandle::init(ObITmpFile *tmp_file)
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

ObITmpFile::ObITmpFile()
    : is_inited_(false),
      mode_(ObTmpFileMode::INVALID),
      tenant_id_(OB_INVALID_TENANT_ID),
      dir_id_(ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
      fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
      is_deleting_(false),
      is_sealed_(false),
      ref_cnt_(0),
      truncated_offset_(0),
      read_offset_(0),
      file_size_(0),
      cached_page_nums_(0),
      write_back_data_page_num_(0),
      begin_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      begin_page_virtual_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
      end_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      diag_log_print_cnt_(0),
      data_page_flush_level_(-1),
      data_flush_node_(*this),
      meta_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      multi_write_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      last_page_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      wbp_(nullptr),
      flush_prio_mgr_(nullptr),
      page_idx_cache_(),
      callback_allocator_(nullptr),
      trace_id_(),
      birth_ts_(-1),
      label_(),
      write_req_cnt_(0),
      unaligned_write_req_cnt_(0),
      write_persisted_tail_page_cnt_(0),
      lack_page_cnt_(0),
      last_modify_ts_(-1),
      read_req_cnt_(0),
      unaligned_read_req_cnt_(0),
      total_truncated_page_read_cnt_(0),
      total_kv_cache_page_read_cnt_(0),
      total_uncached_page_read_cnt_(0),
      total_wbp_page_read_cnt_(0),
      truncated_page_read_hits_(0),
      kv_cache_page_read_hits_(0),
      uncached_page_read_hits_(0),
      wbp_page_read_hits_(0),
      total_read_size_(0),
      last_access_ts_(-1)
{
}

ObITmpFile::~ObITmpFile()
{
  reset();
}

int ObITmpFile::init(const int64_t tenant_id,
                     const int64_t dir_id,
                     const int64_t fd,
                     ObTmpWriteBufferPool *wbp,
                     ObTmpFileFlushPriorityManager *flush_prio_mgr,
                     ObIAllocator *callback_allocator,
                     ObIAllocator *wbp_index_cache_allocator,
                     ObIAllocator *wbp_index_cache_bkt_allocator,
                     const char* label)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                         ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID == dir_id ||
                         !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(fd), K(dir_id));
  } else if (OB_ISNULL(wbp) || OB_ISNULL(flush_prio_mgr) || OB_ISNULL(callback_allocator) ||
             OB_ISNULL(wbp_index_cache_allocator) || OB_ISNULL(wbp_index_cache_bkt_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), KP(wbp), KP(flush_prio_mgr), KP(callback_allocator),
             KP(wbp_index_cache_allocator), KP(wbp_index_cache_bkt_allocator));
  } else if (OB_FAIL(page_idx_cache_.init(fd, wbp, wbp_index_cache_allocator,
                                          wbp_index_cache_bkt_allocator))) {
    LOG_WARN("fail to init page idx array", KR(ret), K(fd));
  } else {
    is_inited_ = true;
    dir_id_ = dir_id;
    fd_ = fd;
    tenant_id_ = tenant_id;
    wbp_ = wbp;
    flush_prio_mgr_ = flush_prio_mgr;
    callback_allocator_ = callback_allocator;

    /******for virtual table begin******/
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
    /******for virtual table end******/
  }
  return ret;
}

void ObITmpFile::reset()
{
  if (is_inited_) {
    is_inited_ = false;
    mode_ = ObTmpFileMode::INVALID;
    tenant_id_ = OB_INVALID_TENANT_ID;
    dir_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
    fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    is_deleting_ = false;
    is_sealed_ = false;
    ref_cnt_ = 0;
    truncated_offset_ = 0;
    read_offset_ = 0;
    file_size_ = 0;
    cached_page_nums_ = 0;
    write_back_data_page_num_ = 0;
    begin_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    diag_log_print_cnt_ = 0;
    data_page_flush_level_ = -1;
    data_flush_node_.unlink();
    wbp_ = nullptr;
    flush_prio_mgr_ = nullptr;
    page_idx_cache_.destroy();
    callback_allocator_ = nullptr;
    /******for virtual table begin******/
    // common info
    trace_id_.reset();
    birth_ts_ = -1;
    label_.reset();
    // write info
    write_req_cnt_ = 0;
    unaligned_write_req_cnt_ = 0;
    write_persisted_tail_page_cnt_ = 0;
    lack_page_cnt_ = 0;
    last_modify_ts_ = -1;
    // read info
    read_req_cnt_ = 0;
    unaligned_read_req_cnt_ = 0;
    total_truncated_page_read_cnt_ = 0;
    total_kv_cache_page_read_cnt_ = 0;
    total_uncached_page_read_cnt_ = 0;
    total_wbp_page_read_cnt_ = 0;
    truncated_page_read_hits_ = 0;
    kv_cache_page_read_hits_ = 0;
    uncached_page_read_hits_ = 0;
    wbp_page_read_hits_ = 0;
    total_read_size_ = 0;
    last_access_ts_ = -1;
    /******for virtual table end******/
  }
}

int ObITmpFile::delete_file()
{
  int ret = OB_SUCCESS;
  LOG_INFO("tmp file delete start", K(fd_));
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (IS_INIT && !is_deleting_) {
    LOG_INFO("tmp file inner delete start", KR(ret), KPC(this));
    if (OB_FAIL(inner_delete_file_())) {
      LOG_WARN("fail to inner delete file", KR(ret), KPC(this));
    } else {
      // read, write, truncate, flush and evict function will fail when is_deleting_ == true.
      is_deleting_ = true;
    }
  }

  LOG_INFO("tmp file delete over", KR(ret), KPC(this));
  return ret;
}

int ObITmpFile::seal()
{
  int ret = OB_SUCCESS;
  LOG_INFO("tmp file seal start", K(fd_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else {
    ObSpinLockGuard write_guard(multi_write_lock_);
    if (OB_UNLIKELY(is_sealed_)) {
      ret = OB_ERR_TMP_FILE_ALREADY_SEALED;
      LOG_WARN("tmp file has been sealed", KR(ret), KPC(this));
    } else if (OB_FAIL(inner_seal_())) {
      LOG_WARN("fail to seal", KR(ret), KPC(this));
    }
    LOG_INFO("tmp file seal over", KR(ret), KPC(this));
  }
  return ret;
}

int ObITmpFile::aio_pread(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("aio pread start", KR(ret), K(fd_), K(io_ctx));
  common::TCRWLock::RLockGuard guard(meta_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file has not been inited", KR(ret), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to read a deleting file", KR(ret), K(fd_));
  } else {
    if (io_ctx.get_read_offset_in_file() < 0) {
      io_ctx.set_read_offset_in_file(read_offset_);
    }
    if (0 != io_ctx.get_read_offset_in_file() % ObTmpFileGlobal::PAGE_SIZE
        || 0 != io_ctx.get_todo_size() % ObTmpFileGlobal::PAGE_SIZE) {
      io_ctx.set_is_unaligned_read(true);
    }

    LOG_DEBUG("start to inner read tmp file", K(fd_), K(io_ctx), KPC(this));
    if (OB_UNLIKELY(!io_ctx.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx), K(read_offset_));
    } else if (OB_UNLIKELY(io_ctx.get_read_offset_in_file() >= file_size_)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", KR(ret), K(fd_), K(file_size_), K(io_ctx));
    } else if (io_ctx.get_read_offset_in_file() < truncated_offset_ &&
              OB_FAIL(inner_read_truncated_part_(io_ctx))) {
      LOG_WARN("fail to read truncated part", KR(ret), K(fd_), K(io_ctx), K(truncated_offset_));
    } else if (OB_UNLIKELY(io_ctx.get_todo_size() == 0)) {
      // do nothing
    } else {
      // Iterate to read disk data.
      int64_t wbp_begin_offset = cal_wbp_begin_offset_();
      if (OB_UNLIKELY(wbp_begin_offset < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected wbp begin offset", KR(ret), K(fd_), K(wbp_begin_offset), K(io_ctx), KPC(this));
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
                   K(wbp_begin_offset), K(io_ctx), KPC(this));
        } else {
          LOG_DEBUG("finish disk read", K(fd_), K(io_ctx.get_read_offset_in_file()),
                                        K(io_ctx.get_todo_size()),
                                        K(io_ctx.get_done_size()),
                                        K(wbp_begin_offset), K(expected_read_disk_size), KPC(this));
        }
      }

      // Iterate to read memory data (in write buffer pool).
      if (OB_SUCC(ret) && io_ctx.get_todo_size() > 0) {
        const int64_t aligned_begin_offset = get_page_begin_offset_(io_ctx.get_read_offset_in_file());
        if (OB_UNLIKELY(0 == cached_page_nums_)) {
          ret = OB_ITER_END;
          LOG_WARN("iter end", KR(ret), K(fd_), K(io_ctx));
        } else if (OB_FAIL(inner_read_from_wbp_(io_ctx))) {
          LOG_WARN("fail to read tmp file from wbp", KR(ret), K(fd_), K(io_ctx), KPC(this));
        } else {
          const int64_t aligned_end_offset = get_page_end_offset_(io_ctx.get_read_offset_in_file());
          const int64_t total_wbp_page_read_cnt = (aligned_end_offset - aligned_begin_offset) / ObTmpFileGlobal::PAGE_SIZE;
          io_ctx.update_read_wbp_page_stat(total_wbp_page_read_cnt);
          LOG_DEBUG("finish wbp read", K(fd_), K(io_ctx.get_read_offset_in_file()),
                                       K(io_ctx.get_todo_size()),
                                       K(io_ctx.get_done_size()), K(wbp_begin_offset), KPC(this));
        }
      }
    }
  }
  LOG_DEBUG("aio pread over", KR(ret), K(fd_), KPC(this), K(io_ctx));
  return ret;
}

int ObITmpFile::inner_read_truncated_part_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(io_ctx.get_read_offset_in_file() >= truncated_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read offset should be less than truncated offset", KR(ret), K(fd_), K(io_ctx), K(truncated_offset_));
  } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid io_ctx", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_UNLIKELY(io_ctx.get_todo_size() == 0)) {
    // do nothing
  } else {
    int64_t total_truncated_page_read_cnt = 0;
    const int64_t origin_read_offset = io_ctx.get_read_offset_in_file();
    int64_t read_size = MIN(truncated_offset_ - io_ctx.get_read_offset_in_file(),
                            io_ctx.get_todo_size());
    char *read_buf = io_ctx.get_todo_buffer();
    if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(read_buf, read_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid buf range", KR(ret), K(fd_), K(read_buf), K(read_size), K(io_ctx));
    } else if (FALSE_IT(MEMSET(read_buf, 0, read_size))) {
    } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(read_size));
    } else if (FALSE_IT(total_truncated_page_read_cnt = (get_page_end_offset_(io_ctx.get_read_offset_in_file()) -
                                                         get_page_begin_offset_(origin_read_offset)) /
                                                        ObTmpFileGlobal::PAGE_SIZE)) {
    } else if (FALSE_IT(io_ctx.update_read_truncated_stat(total_truncated_page_read_cnt))) {
    } else if (OB_UNLIKELY(io_ctx.get_todo_size() > 0 &&
                           truncated_offset_ == file_size_)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", KR(ret), K(fd_), K(file_size_), K(truncated_offset_), K(io_ctx));
    }
  }

  return ret;
}

int ObITmpFile::inner_read_from_wbp_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t begin_read_page_virtual_id = get_page_virtual_id_(io_ctx.get_read_offset_in_file(),
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
    LOG_WARN("fail to find page index in array", KR(ret), K(fd_), K(io_ctx), K(begin_read_page_virtual_id), K(page_idx_cache_));
  } else if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_read_page_id &&
             OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, begin_read_page_virtual_id, begin_page_id_, begin_read_page_id))) {
    LOG_WARN("fail to get page id by virtual id", KR(ret), K(fd_), K(begin_read_page_virtual_id), K(begin_page_id_));
  } else if (OB_UNLIKELY(begin_read_page_id == ObTmpFileGlobal::INVALID_PAGE_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid page index", KR(ret), K(fd_), K(begin_read_page_id), KPC(this));
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
      LOG_WARN("data page is null", KR(ret), K(fd_), K(curr_page_id), K(curr_page_virtual_id));
    } else {
      const int64_t read_offset_in_page = get_offset_in_page_(io_ctx.get_read_offset_in_file());
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
int ObITmpFile::aio_write(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("aio write start", K(fd_), K(io_ctx));
  ObSpinLockGuard guard(multi_write_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file has not been inited", KR(ret), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // this check is just a hint.
    // although is_deleting_ == false, it might be set as true in the processing of inner_write().
    // we will check is_deleting_ again when try to update meta data in the inner_write()
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to write a deleting file", KR(ret), K(fd_));
  } else if (OB_UNLIKELY(is_sealed_)) {
    ret = OB_ERR_TMP_FILE_ALREADY_SEALED;
    LOG_WARN("attempt to write a sealed file", KR(ret), K(fd_));
  } else {
    bool is_unaligned_write = 0 != file_size_ % ObTmpFileGlobal::PAGE_SIZE ||
                              0 != io_ctx.get_todo_size() % ObTmpFileGlobal::PAGE_SIZE;
    io_ctx.set_is_unaligned_write(is_unaligned_write);
    while (OB_SUCC(ret) && io_ctx.get_todo_size() > 0) {
      if (OB_FAIL(inner_write_(io_ctx))) {
        if (OB_ALLOCATE_TMP_FILE_PAGE_FAILED == ret) {
          io_ctx.add_lack_page_cnt();
          ret = OB_SUCCESS;
          if (TC_REACH_COUNT_INTERVAL(10)) {
            LOG_INFO("alloc mem failed, try to evict pages", K(fd_), K(file_size_), K(io_ctx), KPC(this));
          }
          if (OB_FAIL(swap_page_to_disk_(io_ctx))) {
            LOG_WARN("fail to swap page to disk", KR(ret), K(fd_), K(io_ctx));
          }
        } else {
          LOG_WARN("fail to inner write", KR(ret), K(fd_), K(io_ctx), KPC(this));
        }
      }
    } // end while
  }

  if (OB_SUCC(ret)) {
    // ATTENTION! we print tmp file data members here without meta_lock_.
    static const int64_t PRINT_LOG_FILE_SIZE = 100 * 1024 * 1024; // 100MB
    int64_t cur_print_cnt = file_size_ / PRINT_LOG_FILE_SIZE;
    if (cur_print_cnt > ATOMIC_LOAD(&diag_log_print_cnt_)) {
      ATOMIC_INC(&diag_log_print_cnt_);
      LOG_INFO("aio write finish", K(fd_), K(io_ctx), KPC(this));
    } else {
      LOG_DEBUG("aio write finish", KR(ret), K(fd_), K(file_size_), K(io_ctx));
    }
  } else {
    LOG_DEBUG("aio write failed", KR(ret), K(fd_), K(file_size_), K(io_ctx), KPC(this));
  }

  return ret;
}

int ObITmpFile::inner_write_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("inner write start", K(fd_), K(file_size_), K(io_ctx));
  if (has_unfinished_page_()) {
    if (OB_FAIL(inner_fill_tail_page_(io_ctx))) {
      LOG_WARN("fail to fill tail page", KR(ret), K(fd_), K(io_ctx), KPC(this));
    } else if (OB_UNLIKELY(has_unfinished_page_() && io_ctx.get_todo_size() > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tmp file page is unaligned", KR(ret), K(fd_));
    }
  }

  // each batch at most write TMP_FILE_WRITE_BATCH_PAGE_NUM pages
  while (OB_SUCC(ret) && io_ctx.get_todo_size() > 0) {
    if (OB_FAIL(inner_write_continuous_pages_(io_ctx))) {
      LOG_WARN("fail to write continuous pages", KR(ret), K(fd_), K(io_ctx), KPC(this));
    }
  }

  LOG_DEBUG("inner write over", KR(ret), K(fd_), K(file_size_), K(io_ctx));
  return ret;
}

int ObITmpFile::inner_fill_tail_page_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("inner fill tail page start", K(fd_), K(file_size_), K(io_ctx));
  ObSpinLockGuard last_page_lock_guard(last_page_lock_);
  const bool is_in_disk = (0 == cached_page_nums_);

  if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
  } else if (is_in_disk) {
    if (OB_FAIL(load_disk_tail_page_and_rewrite_(io_ctx))) {
      LOG_WARN("fail to load disk tail page and rewrite", KR(ret), K(fd_), K(io_ctx));
    } else {
      io_ctx.add_write_persisted_tail_page_cnt();
    }
  } else {
    if (OB_FAIL(append_write_memory_tail_page_(io_ctx))) {
      LOG_WARN("fail to append write memory tail page", KR(ret), K(fd_), K(io_ctx));
    }
  }

  LOG_DEBUG("inner fill tail page over", KR(ret), K(fd_), K(io_ctx), KPC(this));
  return ret;
}

int ObITmpFile::inner_write_continuous_pages_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  bool is_alloc_failed = false;
  int64_t write_size = 0;
  ObArray<uint32_t> page_entry_idxs;
  bool has_update_file_meta = false;
  LOG_DEBUG("inner write continuous pages start", K(fd_), K(file_size_), K(io_ctx));

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
      LOG_WARN("page entry idxs is empty", KR(ret), K(fd_), K(page_entry_idxs), K(write_size), KPC(this));
    } else {
      // do nothing, no need to update meta data
    }
  } else {
    common::TCRWLock::WLockGuard guard(meta_lock_);
    LOG_DEBUG("inner write continuous pages update meta start", K(fd_), KPC(this), K(io_ctx));
    const int64_t end_page_virtual_id = cached_page_nums_ == 0 ?
                                        ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID :
                                        get_page_virtual_id_(file_size_, true /*is_open_interval*/);
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
          begin_page_virtual_id_ = get_page_virtual_id_(file_size_, false /*is_open_interval*/);
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
    LOG_DEBUG("inner write continuous pages update meta over", KR(ret), K(fd_), KPC(this), K(io_ctx));
  } // end update meta data.

  // reset allocation failure status
  ret = is_alloc_failed && OB_SUCC(ret) ? OB_ALLOCATE_TMP_FILE_PAGE_FAILED : ret;
  LOG_DEBUG("inner write continuous pages over", KR(ret), K(fd_), K(file_size_), K(io_ctx));
  return ret;
}

int ObITmpFile::alloc_and_write_pages_(const ObTmpFileIOCtx &io_ctx,
                                       ObArray<uint32_t> &alloced_page_id,
                                       int64_t &actual_write_size)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  const int64_t new_begin_page_virtual_id = get_page_virtual_id_(file_size_, false /*is_open_interval*/);
  uint32_t previous_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  alloced_page_id.reset();
  actual_write_size = 0;
  LOG_DEBUG("alloc and write pages start", K(fd_), K(file_size_), K(io_ctx));

  if (OB_UNLIKELY(has_unfinished_page_())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the file has unfinished page", KR(ret), K(fd_));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == new_begin_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new page virtual id is invalid", KR(ret), K(fd_), K(file_size_), K(new_begin_page_virtual_id));
  } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_FAIL(alloced_page_id.prepare_allocate_and_keep_count(
                                     ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_PAGE_NUM))) {
    LOG_WARN("fail to prepare allocate", KR(ret), K(fd_));
  } else {
    const int64_t expected_write_size = io_ctx.get_todo_size();
    int64_t new_page_virtual_id = new_begin_page_virtual_id;
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

    if (OB_FAIL(ret) && ret != OB_ALLOCATE_TMP_FILE_PAGE_FAILED && !alloced_page_id.empty()) {
      int64_t invalid_page_virtual_id = new_begin_page_virtual_id;
      for (int64_t i = 0; i < alloced_page_id.count(); i++) {
        int tmp_ret = OB_SUCCESS;
        uint32_t invalid_page_id = alloced_page_id.at(i);
        uint32_t tmp_pid = ObTmpFileGlobal::INVALID_PAGE_ID; // unused
        if (OB_TMP_FAIL(wbp_->free_page(fd_, invalid_page_id, ObTmpFilePageUniqKey(invalid_page_virtual_id), tmp_pid))) {
          LOG_WARN("fail to free page", KR(tmp_ret), K(fd_), K(invalid_page_id));
        } else {
          invalid_page_virtual_id += 1;
        }
      } // end for
      alloced_page_id.reset();
    }
  }
  LOG_DEBUG("alloc and write pages over", KR(ret), K(fd_), K(io_ctx), K(file_size_),
                                         K(end_page_id_), K(actual_write_size), K(alloced_page_id), KPC(this));
  return ret;
}

// Attention!!
// 1. if truncate_offset is not the begin or end offset of page,
//    we will fill zero from begin_offset to truncate_offset for truncated page in wbp.
// 2. truncate_offset is a open interval number, which means the offset before than it need to be truncated
int ObITmpFile::truncate(const int64_t truncate_offset)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("truncate start", K(fd_));
  ObSpinLockGuard last_page_lock_guard(last_page_lock_);
  common::TCRWLock::WLockGuard guard(meta_lock_);
  int64_t wbp_begin_offset = cal_wbp_begin_offset_();
  LOG_INFO("start to truncate a temporary file", KR(ret), K(fd_), K(truncate_offset), K(wbp_begin_offset), KPC(this));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file has not been inited", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to truncate a deleting file", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(truncate_offset <= 0 || truncate_offset > file_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid truncate_offset", KR(ret), K(fd_), K(truncate_offset), K(file_size_), KPC(this));
  } else if (OB_UNLIKELY(wbp_begin_offset < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected wbp begin offset", KR(ret), K(fd_), K(wbp_begin_offset), K(truncate_offset), KPC(this));
  } else if (OB_UNLIKELY(truncate_offset <= truncated_offset_)) {
    // do nothing
  } else if (OB_FAIL(truncate_cached_pages_(truncate_offset, wbp_begin_offset))) {
    LOG_WARN("fail to truncate cached pages", KR(ret), K(fd_), K(truncate_offset), K(wbp_begin_offset), KPC(this));
  } else if (OB_FAIL(truncate_persistent_pages_(truncate_offset))) {
    LOG_WARN("fail to truncate persistent pages", KR(ret), K(fd_), K(truncate_offset), KPC(this));
  } else {
    truncated_offset_ = truncate_offset;
    last_modify_ts_ = ObTimeUtility::current_time();
  }

  LOG_INFO("truncate over", KR(ret), K(truncate_offset), K(wbp_begin_offset), KPC(this));
  return ret;
}

int ObITmpFile::truncate_cached_pages_(const int64_t truncate_offset, const int64_t wbp_begin_offset)
{
  int ret = OB_SUCCESS;
  uint32_t truncate_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (OB_UNLIKELY(wbp_begin_offset < 0 || truncate_offset <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid offset", KR(ret), K(fd_), K(wbp_begin_offset), K(truncate_offset));
  } else if (truncate_offset > wbp_begin_offset) {
    const int64_t truncate_page_virtual_id = get_page_virtual_id_(truncate_offset,
                                                                  true /*is_open_interval*/);
    const int64_t truncate_offset_in_page = get_offset_in_page_(truncate_offset);
    if (OB_FAIL(page_idx_cache_.binary_search(truncate_page_virtual_id, truncate_page_id))) {
      LOG_WARN("fail to find page index in array", KR(ret), K(fd_), K(truncate_page_virtual_id));
    } else if (ObTmpFileGlobal::INVALID_PAGE_ID != truncate_page_id) {
      // the page index of truncate_offset is in the range of cached page index.
      // truncate all page indexes whose offset is smaller than truncate_offset.
      const int64_t truncate_page_virtual_id_in_cache = truncate_offset_in_page == 0 ?
                                                        truncate_page_virtual_id + 1 :
                                                        truncate_page_virtual_id;
      if (OB_FAIL(page_idx_cache_.truncate(truncate_page_virtual_id_in_cache))) {
        LOG_WARN("fail to truncate page idx cache", KR(ret), K(fd_), K(truncate_page_virtual_id),
                 K(truncate_page_virtual_id_in_cache), K(truncate_offset));
      }
    } else { // ObTmpFileGlobal::INVALID_PAGE_ID == truncate_page_id
      // the page index of truncate_offset is smaller than the smallest cached page index.
      // we need to find truncate_page_id by iterating wbp_.
      if (OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, truncate_page_virtual_id, begin_page_id_, truncate_page_id))) {
        LOG_WARN("fail to get page id by virtual id", KR(ret), K(fd_), K(truncate_page_virtual_id),
                 K(truncate_offset), K(begin_page_id_));
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

bool ObITmpFile::can_remove()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return is_deleting_ && get_ref_cnt() == 1;
}

bool ObITmpFile::is_deleting()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return is_deleting_;
}

bool ObITmpFile::is_sealed()
{
  ObSpinLockGuard guard(multi_write_lock_);
  return is_sealed_;
}

bool ObITmpFile::is_flushing()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return is_flushing_();
}

int64_t ObITmpFile::get_file_size()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return file_size_;
}

int64_t ObITmpFile::cal_wbp_begin_offset()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return cal_wbp_begin_offset_();
}

int64_t ObITmpFile::cal_wbp_begin_offset_() const
{
  int ret = OB_SUCCESS;
  int64_t res = -1;
  if (0 == cached_page_nums_) {
    res = file_size_;
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_ ||
                         begin_page_virtual_id_ * ObTmpFileGlobal::PAGE_SIZE !=
                         get_page_end_offset_(file_size_) -
                         cached_page_nums_ * ObTmpFileGlobal::PAGE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("begin_page_offset_in_file_ is unexpected", KR(ret), KPC(this));
  } else {
    res = begin_page_virtual_id_ * ObTmpFileGlobal::PAGE_SIZE;
  }

  return res;
}

void ObITmpFile::update_read_offset(int64_t read_offset)
{
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (read_offset > read_offset_) {
    read_offset_ = read_offset;
  }
}

int64_t ObITmpFile::get_dirty_data_page_size_with_lock()
{
  common::TCRWLock::RLockGuard guard(meta_lock_);
  return get_dirty_data_page_size_();
}

int ObITmpFile::reinsert_data_flush_node()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("reinsert_data_flush_node start", K(fd_));
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (OB_FAIL(reinsert_data_flush_node_())) {
    LOG_WARN("fail to reinsert flush node", KR(ret), KPC(this));
  }
  LOG_DEBUG("reinsert_data_flush_node over", KR(ret), K(fd_), KPC(this));

  return ret;
}

int ObITmpFile::reinsert_data_flush_node_()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("reinsert_data_flush_node_ start", K(fd_), KPC(this));

  if (OB_UNLIKELY(nullptr != data_flush_node_.get_next())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush node should not have next", KR(ret), K(fd_));
  } else if (OB_UNLIKELY(is_deleting_)) {
    // do nothing
  } else {
    int64_t dirty_page_size = get_dirty_data_page_size_();
    if (0 == dirty_page_size) {
      // no need to reinsert
      data_page_flush_level_ = -1;
    } else if (OB_FAIL(flush_prio_mgr_->insert_data_flush_list(*this, dirty_page_size))) {
      LOG_WARN("fail to insert data flush list", KR(ret), K(fd_), K(dirty_page_size));
    }
  }
  LOG_DEBUG("reinsert_data_flush_node_ over", K(fd_), KPC(this));

  return ret;
}

int ObITmpFile::remove_data_flush_node()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("remove_data_flush_node start", K(fd_));
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (OB_FAIL(flush_prio_mgr_->remove_file(false, *this))) {
    LOG_WARN("fail to remove flush node", KR(ret), KPC(this));
  }
  LOG_DEBUG("remove_data_flush_node over", KR(ret), K(fd_), KPC(this));

  return ret;
}

// ATTENTION! need to be protected by meta_lock_
int ObITmpFile::insert_or_update_data_flush_node_()
{
  int ret = OB_SUCCESS;

  if (!is_flushing_()) {
    int64_t dirty_data_page_size = get_dirty_data_page_size_();
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

void ObITmpFile::set_read_stats_vars(const ObTmpFileIOCtx &ctx, const int64_t read_size)
{
  common::TCRWLock::WLockGuard guard(meta_lock_);
  inner_set_read_stats_vars_(ctx, read_size);
}

void ObITmpFile::inner_set_read_stats_vars_(const ObTmpFileIOCtx &ctx, const int64_t read_size)
{
  read_req_cnt_++;
  if (ctx.is_unaligned_read()) {
    unaligned_read_req_cnt_++;
  }
  total_read_size_ += read_size;
  last_access_ts_ = ObTimeUtility::current_time();
  total_truncated_page_read_cnt_ += ctx.get_total_truncated_page_read_cnt();
  total_kv_cache_page_read_cnt_ += ctx.get_total_kv_cache_page_read_cnt();
  total_uncached_page_read_cnt_ += ctx.get_total_uncached_page_read_cnt();
  total_wbp_page_read_cnt_ += ctx.get_total_wbp_page_read_cnt();
  truncated_page_read_hits_ += ctx.get_truncated_page_read_hits();
  kv_cache_page_read_hits_ += ctx.get_kv_cache_page_read_hits();
  uncached_page_read_hits_ += ctx.get_uncached_page_read_hits();
  wbp_page_read_hits_ += ctx.get_wbp_page_read_hits();
}

void ObITmpFile::set_write_stats_vars(const ObTmpFileIOCtx &ctx)
{
  common::TCRWLock::WLockGuard guard(meta_lock_);
  inner_set_write_stats_vars_(ctx);
}

void ObITmpFile::inner_set_write_stats_vars_(const ObTmpFileIOCtx &ctx)
{
  write_req_cnt_++;
  if (ctx.is_unaligned_write()) {
    unaligned_write_req_cnt_++;
  }
  write_persisted_tail_page_cnt_ += ctx.get_write_persisted_tail_page_cnt();
  lack_page_cnt_ += ctx.get_lack_page_cnt();
  last_modify_ts_ = ObTimeUtility::current_time();
}

}  // end namespace tmp_file
}  // end namespace oceanbase
