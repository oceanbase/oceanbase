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

#include "log_block_mgr.h"
#include <algorithm>                                    // std::sort
#include <cstdio>                                       // renameat
#include <fcntl.h>                                      // ::open
#include "lib/ob_define.h"                              // some constexpr
#include "lib/ob_errno.h"                               // OB_SUCCESS...
#include "lib/container/ob_se_array_iterator.h"         // ObSEArrayIterator
#include "log_define.h"                                 // convert_sys_errno
#include "share/ob_errno.h"                             // OB_NO_SUCH_FILE_OR_DIRECTORY
#include "log_writer_utils.h"                           // LogWriteBuf
#include "lsn.h"                                        // LSN
#include "log_io_utils.h"                               // openat_with_retry

namespace oceanbase
{
using namespace common;
namespace palf
{

LogBlockMgr::LogBlockMgr() : curr_writable_handler_(),
                             curr_writable_block_id_(LOG_INVALID_BLOCK_ID),
                             log_block_size_(LOG_INVALID_LSN_VAL),
                             block_id_cache_lock_(common::ObLatchIds::PALF_LOG_ENGINE_LOCK),
                             min_block_id_(LOG_INVALID_BLOCK_ID),
                             max_block_id_(LOG_INVALID_BLOCK_ID),
                             log_block_pool_(NULL),
                             dir_fd_(-1),
                             align_size_(-1),
                             align_buf_size_(-1),
                             is_inited_(false)
{
}

LogBlockMgr::~LogBlockMgr()
{
  destroy();
}

int LogBlockMgr::init(const char *log_dir,
                      const block_id_t initial_block_id,
                      const int64_t align_size,
                      const int64_t align_buf_size,
                      int64_t log_block_size,
                      ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (NULL == log_dir || LOG_INVALID_LSN_VAL == log_block_size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (-1 == (dir_fd_ = ::open(log_dir, O_DIRECTORY | O_RDONLY))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "::open failed", K(ret), K(log_dir));
  } else if (OB_FAIL(curr_writable_handler_.init(dir_fd_, log_block_size, align_size, align_buf_size))) {
    PALF_LOG(ERROR, "init curr_writable_handler_ failed", K(ret), K(log_dir));
  } else if (OB_FAIL(do_scan_dir_(log_dir, initial_block_id, log_block_pool))) {
    PALF_LOG(ERROR, "do_scan_dir_ failed", K(ret), K(log_dir));
  } else if (OB_FAIL(try_recovery_last_block_(log_dir))) {
    PALF_LOG(ERROR, "try_recovery_last_block_ failed", K(ret), KPC(this));
  } else {
    MEMCPY(log_dir_, log_dir, OB_MAX_FILE_NAME_LENGTH);
    log_block_size_ = log_block_size;
    log_block_pool_ = log_block_pool;
    align_size_ = align_size;
    align_buf_size_ = align_buf_size;
    is_inited_ = true;
    PALF_LOG(INFO, "LogBlockMgr init success", K(ret), K(log_dir_), K(log_block_size));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

// NB: only call this function in 'truncate_prefix_block'
void LogBlockMgr::reset(const block_id_t init_block_id)
{
  ObSpinLockGuard guard(block_id_cache_lock_);
  min_block_id_ = init_block_id;
  max_block_id_ = init_block_id;
  curr_writable_handler_.close();
  curr_writable_block_id_ = LOG_INVALID_BLOCK_ID;
  PALF_LOG(INFO, "LogBlockMgr reset success", K(init_block_id), K(min_block_id_), K(max_block_id_));
}

void LogBlockMgr::destroy()
{
  PALF_LOG(INFO, "destroy LogBlockMgr success");
  is_inited_ = false;
  align_size_ = -1;
  align_buf_size_ = -1;
  if (-1 != dir_fd_) {
    close_with_ret(dir_fd_);
    dir_fd_ = -1;
  }
  log_block_pool_ = NULL;
  curr_writable_handler_.destroy();
  curr_writable_block_id_ = LOG_INVALID_BLOCK_ID;
  log_block_size_ = LOG_INVALID_LSN_VAL;
  min_block_id_ = LOG_INVALID_BLOCK_ID;
  max_block_id_ = LOG_INVALID_BLOCK_ID;
  MEMSET(log_dir_, '\0', OB_MAX_FILE_NAME_LENGTH);
}

int LogBlockMgr::switch_next_block(const block_id_t next_block_id)
{
  int ret = OB_SUCCESS;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (true == is_valid_block_id(curr_writable_block_id_) && next_block_id != curr_writable_block_id_ + 1) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "block_id is not continous, unexpected error", K(ret), K(next_block_id), K(curr_writable_block_id_));
  } else if (OB_FAIL(block_id_to_string(next_block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
    PALF_LOG(ERROR, "block_id_to_string failed", K(ret), KPC(this), K(next_block_id));
  } else if (OB_FAIL(log_block_pool_->create_block_at(dir_fd_, block_path, log_block_size_))) {
    PALF_LOG(ERROR, "create_block_at failed", K(ret), KPC(this), K(next_block_id));
  } else if (OB_FAIL(curr_writable_handler_.switch_next_block(block_path))) {
    PALF_LOG(ERROR, "switch_next_block failed", K(ret));
  } else {
    curr_writable_block_id_ = next_block_id;
    ObSpinLockGuard guard(block_id_cache_lock_);
    // NB: just only set 'max_block_id_' is continous with 'prev_block_id'.
    max_block_id_ = next_block_id + 1;
    PALF_LOG(INFO, "switch_next_block success", KPC(this));
  }
  return ret;
}

int LogBlockMgr::pwrite(const block_id_t block_id,
                        const offset_t offset,
                        const char *buf,
                        const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buf) || 0 >= buf_len || log_block_size_ < offset) {
    ret = OB_INVALID_ARGUMENT;
  } else if (block_id != curr_writable_block_id_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error, the block_id is not same with curr_writable_handler_",
        K(ret), K(block_id), KPC(this));
  } else if (OB_FAIL(curr_writable_handler_.pwrite(offset, buf, buf_len))) {
    PALF_LOG(ERROR, "LogBlockHandler pwrite failed", K(ret), KPC(this),
        K(block_id), K(offset));
  } else {
    PALF_LOG(TRACE, "LogBlockMgr pwrite success", K(ret), KPC(this), K(block_id), K(offset));
  }
  return ret;
}

int LogBlockMgr::writev(const block_id_t block_id,
                        const offset_t offset,
                        const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == write_buf.is_valid() || log_block_size_ < offset) {
    ret = OB_INVALID_ARGUMENT;
  } else if (block_id != curr_writable_block_id_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error, the block_id is not same with curr_writable_handler_",
        K(ret), K(block_id), K(curr_writable_block_id_), KPC(this));
  } else if (OB_FAIL(curr_writable_handler_.writev(offset, write_buf))) {
    PALF_LOG(ERROR, "LogBlockHandler writev failed", K(ret), K(curr_writable_handler_), K(block_id),
        K(offset), K(log_dir_));
  } else {
    PALF_LOG(TRACE, "LogBlockMgr writev success", K(ret), K(block_id), K(offset));
  }
  return ret;
}

int LogBlockMgr::truncate(const block_id_t block_id, const offset_t offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    // NB: physical block is start at 4K, therefore 'offset' must be greater or equal
    // than 4K.
  } else if (false == is_valid_block_id(block_id) || 0ul == offset) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(ret), K(block_id), K(offset));
  } else if (OB_FAIL(do_truncate_(block_id, offset))) {
    PALF_LOG(WARN, "do_truncate_ failed", K(ret), K(block_id), K(offset));
  } else {
    PALF_LOG(INFO, "truncate success", K(ret), K(block_id), K(offset), K(min_block_id_), K(max_block_id_));
  }
  return ret;
}

int LogBlockMgr::get_block_id_range(block_id_t &min_block_id,
                                    block_id_t &max_block_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObSpinLockGuard guard(block_id_cache_lock_);
    min_block_id = min_block_id_;
    max_block_id = max_block_id_;
    if (min_block_id == max_block_id) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      // NB: [min_block_id, max_block_id), make use easily, tansform to [min_block_id, max_block_id]
      --max_block_id;
    }
  }
  return ret;
}

int LogBlockMgr::delete_block(block_id_t block_id)
{
  int ret = OB_SUCCESS;
  // NB: after restart, 'min_block_id_' is valid as long as there are blocks on disk.
  const block_id_t tmp_min_block_id = min_block_id_;
  if (LOG_INVALID_BLOCK_ID == tmp_min_block_id || block_id < tmp_min_block_id) {
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    PALF_LOG(WARN, "the block to be delted not exist", K(ret), K(block_id), K(tmp_min_block_id));
  } else if (block_id != tmp_min_block_id) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "the block to be delted is not continous with min block", K(ret), K(block_id), K(tmp_min_block_id));
  } else {
    // when there is no block on disk, need close curr_writable_handler_, otherwise, the next write will be failed.
    {
      ObSpinLockGuard guard(block_id_cache_lock_);
      min_block_id_++;
    }
    if (min_block_id_ == max_block_id_) {
      curr_writable_handler_.close();
      curr_writable_block_id_ = LOG_INVALID_BLOCK_ID;
    }
    if (OB_FAIL(do_delete_block_(block_id))) {
      PALF_LOG(WARN, "do_delete_block_ failed", K(ret), K(block_id), K(log_dir_));
    } else {
      PALF_LOG(INFO, "delete_block success", K(ret), K(block_id));
    }
  }
  return ret;
}

int LogBlockMgr::load_block_handler(const block_id_t block_id, const offset_t offset)
{
  int ret = OB_SUCCESS;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  // just only load last not aligned data.
  if (OB_FAIL(block_id_to_string(block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
    PALF_LOG(ERROR, "block_id_to_string failed", K(ret), K(block_id));
  } else if (OB_FAIL(curr_writable_handler_.open(block_path))) {
    PALF_LOG(WARN, "open block failed", K(ret), K(block_id));
  } else if (OB_FAIL(curr_writable_handler_.load_data(offset))) {
    PALF_LOG(WARN, "load_data failed", K(ret), K(block_id), K(offset));
  } else {
    curr_writable_block_id_ = block_id;
    PALF_LOG(INFO, "load_block_handler success", K(block_id), K(offset));
  }
  return ret;
}

int LogBlockMgr::create_tmp_block_handler(const block_id_t block_id)
{
  int ret = OB_SUCCESS;
	char tmp_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
	if (OB_FAIL(block_id_to_tmp_string(block_id, tmp_block_path, OB_MAX_FILE_NAME_LENGTH))) {
		PALF_LOG(ERROR, "block_id_to_tmp_string failed", K(ret), KPC(this), K(block_id));
	} else if (OB_FAIL(curr_writable_handler_.close())) {
    PALF_LOG(ERROR, "curr_writable_handler_ close success");
  } else if (FALSE_IT(curr_writable_handler_.destroy())) {
  } else if (OB_FAIL(curr_writable_handler_.init(dir_fd_, log_block_size_, align_size_, align_buf_size_))) {
    PALF_LOG(ERROR, "curr_writable_handler_ init failed", K(ret), KPC(this));
  } else if (OB_FAIL(log_block_pool_->create_block_at(dir_fd_, tmp_block_path, log_block_size_))) {
  } else if (OB_FAIL(curr_writable_handler_.open(tmp_block_path))) {
    PALF_LOG(ERROR, "create_tmp_block failed", K(ret), KPC(this), K(block_id));
  } else {
		curr_writable_block_id_ = block_id;
		PALF_LOG(INFO, "create_tmp_block_handler success", K(ret), KPC(this), K(block_id));
  }
  return ret;
}

int LogBlockMgr::delete_block_from_back_to_front_until(const block_id_t block_id)
{
  return delete_block_from_back_to_front_until_(block_id);
}

int LogBlockMgr::rename_tmp_block_handler_to_normal(const block_id_t block_id)
{
  int ret = OB_SUCCESS;
	// 1. rename "block_id.tmp" to "block_id.flashback"
	// 2. delete "block_id", make sure each block has returned into BlockPool
	// 3. rename "block_id.flashback" to "block_id"
	// NB: for restart, the block which named 'block_id.flashback' must be renamed to 'block_id'
	char tmp_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
	char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
	char flashback_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (block_id != curr_writable_block_id_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "block_id is not same as curr_writable_handler_, unexpected error",
        K(ret), K(block_id), K(curr_writable_block_id_), KPC(this));
  } else if (OB_FAIL(block_id_to_string(block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
		PALF_LOG(ERROR, "block_id_to_string failed", K(ret), K(block_id));
  } else if (OB_FAIL(block_id_to_tmp_string(block_id, tmp_block_path, OB_MAX_FILE_NAME_LENGTH))) {
		PALF_LOG(ERROR, "block_id_to_tmp_string failed", K(ret), K(block_id));
  } else if (OB_FAIL(block_id_to_flashback_string(block_id, flashback_block_path, OB_MAX_FILE_NAME_LENGTH))) {
		PALF_LOG(ERROR, "block_id_to_flashback_string failed", K(ret), K(block_id));
	} else if (OB_FAIL(do_rename_and_fsync_(tmp_block_path, flashback_block_path))) {
    PALF_LOG(ERROR, "do_rename_and_fsync_ failed", K(ret), KPC(this));
	} else if(OB_FAIL(do_delete_block_(block_id))) {
		PALF_LOG(ERROR, "do_delete_block_ failed", K(ret), KPC(this), K(block_id));
	} else if (OB_FAIL(do_rename_and_fsync_(flashback_block_path, block_path))) {
    PALF_LOG(ERROR, "do_rename_and_fsync_ failed", K(ret), KPC(this));
  } else {
    PALF_LOG(INFO, "rename_tmp_block_handler_to_normal success", K(ret), KPC(this));
  }
  return ret;
}

// step1: firstly, delete each block after lsn.block_id_;
// step2: secondly, truncate data in curr_lsn_;
// step3: keep last dio_aligned_buf_.
// NB: attention to truncate and delete concurrently
// For block gc, truncate will not concurrent with delete(truncate and delete block will not
// operate the same range).
// For migrate(truncate prefix), only one log io worker to execute truncate suffix and prefix.
// TODO by runlin: 即使truncate_prefix_block的工作由GC线程完成, 也不应该存在并发
int LogBlockMgr::do_truncate_(const block_id_t block_id,
                              const offset_t offset)
{
  int ret = OB_SUCCESS;
	char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (OB_FAIL(delete_block_from_back_to_front_until_(block_id))) {
    PALF_LOG(ERROR, "delete_block_from_back_to_front_until_ failed", K(ret), KPC(this),
        K(block_id), K(offset));
  } else if (block_id != curr_writable_block_id_) {
		ret = OB_ERR_UNEXPECTED;
		PALF_LOG(ERROR, "unexpected error, block id is not same sa curr_writable_block_id_", K(ret),
				KPC(this), K(block_id));
	} else if (OB_FAIL(block_id_to_string(block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
    PALF_LOG(ERROR, "block_id_to_string failed", K(ret), K(block_id), KPC(this));
  } else if (OB_FAIL(curr_writable_handler_.close())) {
    PALF_LOG(ERROR, "close curr_writable_handler_ failed", K(ret), K(block_id), KPC(this));
  } else if (OB_FAIL(curr_writable_handler_.open(block_path))) {
    PALF_LOG(ERROR, "open block after delete_block_from_back_to_front_until_ failed",
        K(ret), K(block_id), KPC(this));
  } else if (OB_FAIL(curr_writable_handler_.truncate(offset))) {
    PALF_LOG(WARN, "truncate curr_writable_handler_ failed", K(ret), K(block_id), K(offset));
  } else if (OB_FAIL(check_after_truncate_(block_path, offset))) {
    PALF_LOG(ERROR, "check_after_truncate_ failed", K(ret), K(block_id), K(offset));
  } else if (OB_FAIL(curr_writable_handler_.load_data(offset))) {
    PALF_LOG(WARN, "load_data failed", K(ret), K(block_id), K(offset));
  } else {
    PALF_LOG(INFO, "do_truncate_ success", K(ret), K(block_id), K(offset), K(min_block_id_), K(max_block_id_));
  }

  return ret;
}

int LogBlockMgr::check_after_truncate_(const char *block_path, const offset_t offset)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char *buf = NULL;
  const int buf_len = 8*1024;
  char *expected_data = NULL;
  offset_t read_offset = lower_align(offset, LOG_DIO_ALIGN_SIZE);
  const int backoff = offset - read_offset;
  // The min length of PADDING is 4K, therefore, read_offset may be 64MB-4KB,
  // in_read_size is 4K, otherwise, in_read_size is 8K.
  const int in_read_size = MIN(buf_len, log_block_size_-read_offset);
  OB_ASSERT(backoff < LOG_DIO_ALIGN_SIZE);
  if (OB_FAIL(openat_with_retry(dir_fd_, block_path, LOG_READ_FLAG, FILE_OPEN_MODE, fd))) {
    PALF_LOG(ERROR, "openat_with_retry failed", KPC(this), K(block_path));
  } else if (NULL == (expected_data = \
      reinterpret_cast<char*>(ob_malloc(buf_len, "LogBlockMgr")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "malloc failed", KPC(this));
  } else if (FALSE_IT(memset(expected_data, 0, buf_len))) {
  } else if (NULL == (buf = \
      reinterpret_cast<char*>(ob_malloc_align(LOG_DIO_ALIGN_SIZE, buf_len, "LogBlockMgr")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "malloc failed", KPC(this));
  } else if (in_read_size!= (ob_pread(fd, buf, in_read_size, read_offset))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "ob_pread failed", KPC(this), K(fd), K(offset));
    // TODO by runlin: after support reuse block, need use another method.
  } else if (0 != MEMCMP(buf+backoff, expected_data, in_read_size-backoff)) {
    ret = OB_ERR_UNEXPECTED;
    while (OB_FAIL(ret)) {
      PALF_LOG(ERROR, "after truncate, data is not zero", KPC(this), K(fd), K(offset),
          KP(buf), KP(expected_data), K(in_read_size), K(backoff));
      usleep(1000*1000);
    }
  } else {
    PALF_LOG(INFO, "check_after_truncate_ success", KPC(this), K(block_path), K(offset));
  }

  if (-1 != fd && OB_FAIL(close_with_ret(fd))) {
    PALF_LOG(ERROR, "close_with_ret failed", KPC(this), K(block_path));
  }
  if (NULL != buf) {
    ob_free_align(buf);
  }
  if (NULL != expected_data) {
    ob_free(expected_data);
  }
  return ret;
}

int LogBlockMgr::delete_block_from_back_to_front_until_(const block_id_t block_id)
{
  int ret = OB_SUCCESS;
  // NB: [min_block_id_, max_block_id_) is the real range;
  block_id_t curr_block_id = max_block_id_ - 1;
  for (; true == is_valid_block_id(curr_block_id) && curr_block_id > block_id && OB_SUCC(ret);
         curr_block_id--) {
    {
      ObSpinLockGuard guard(block_id_cache_lock_);
      max_block_id_--;
    }
    if (OB_FAIL(do_delete_block_(curr_block_id))) {
      PALF_LOG(WARN, "do_delete_block_ failed", K(ret), K(curr_block_id));
    } else {
			curr_writable_block_id_ = block_id;
    }
  }
  return ret;
}

int LogBlockMgr::do_delete_block_(const block_id_t block_id)
{
  int ret = OB_SUCCESS;
  char tmp_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (OB_FAIL(block_id_to_string(block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
    PALF_LOG(WARN, "block_id_to_string failed", K(ret), K(log_dir_), K(block_id));
  } else if (OB_FAIL(block_id_to_tmp_string(block_id, tmp_block_path, OB_MAX_FILE_NAME_LENGTH))) {
    PALF_LOG(WARN, "block_id_to_tmp_string failed", K(ret), K(log_dir_), K(block_id));
  } else if (OB_FAIL(do_rename_and_fsync_(block_path, tmp_block_path))) {
    PALF_LOG(ERROR, "do_rename_and_fsync_ failed", K(ret), K(log_dir_), K(block_id));
  } else if (OB_FAIL(log_block_pool_->remove_block_at(dir_fd_, tmp_block_path))) {
    PALF_LOG(ERROR, "remove_block_at failed", K(ret), KPC(this), K(block_id));
  } else {
    PALF_LOG(INFO, "do_delete_block_ success", K(ret), KPC(this));
  }
  return ret;
}

int LogBlockMgr::do_scan_dir_(const char *dir,
                              const block_id_t initial_block_id,
                              ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  if (LOG_INVALID_BLOCK_ID != min_block_id_ || LOG_INVALID_BLOCK_ID != max_block_id_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "unexpected error, the cache data must be invalid", K(ret), K(min_block_id_), K(max_block_id_));
  } else {
    TrimLogDirectoryFunctor functor(dir, log_block_pool);
    if (OB_FAIL(scan_dir(dir, functor))) {
      PALF_LOG(WARN, "scan_dir failed", K(ret), K(dir));
    } else {
      ObSpinLockGuard guard(block_id_cache_lock_);
      const block_id_t min_block_id = functor.get_min_block_id();
      const block_id_t max_block_id = functor.get_max_block_id();
      if (LOG_INVALID_BLOCK_ID == min_block_id && LOG_INVALID_BLOCK_ID == max_block_id) {
        min_block_id_ = initial_block_id;
        max_block_id_ = initial_block_id;
      } else {
        // NB: [min_block_id, max_block_id)
        min_block_id_ = min_block_id;
        max_block_id_ = max_block_id + 1;
      }
    }
  }
  return ret;
}

int LogBlockMgr::do_rename_and_fsync_(const char *old_block_path, const char *new_block_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(renameat_with_retry(dir_fd_, old_block_path, dir_fd_, new_block_path))) {
    PALF_LOG(ERROR, "renameat_with_retry failed", K(ret), KPC(this), K(old_block_path), K(new_block_path));
  } else if (OB_FAIL(fsync_with_retry(dir_fd_))) {
    PALF_LOG(ERROR, "fsync_with_retry failed", K(ret), KPC(this), K(old_block_path), K(new_block_path));
  } else {}
  return ret;
}

bool LogBlockMgr::empty_() const
{
  return  min_block_id_ == max_block_id_;
}

int LogBlockMgr::try_recovery_last_block_(const char *log_dir)
{
  int ret = OB_SUCCESS;
  int64_t file_size = 0;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  block_id_t block_id = max_block_id_ - 1;
  int fd = -1;
  if (true == empty_()) {
    PALF_LOG(INFO, "dir is empty, no need to recovery last block");
  } else if (OB_FAIL(convert_to_normal_block(log_dir, block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
    PALF_LOG(WARN, "convert_to_normal_block failed", K(ret), K(block_id));
  } else if (OB_FAIL(FileDirectoryUtils::get_file_size(block_path, file_size))) {
    PALF_LOG(WARN, "get_file_size failed", K(ret), K(block_path));
  } else if (file_size == PALF_PHY_BLOCK_SIZE) {
    PALF_LOG(INFO, "last block no need to recovery", K(block_id));
  } else if (-1 == ::truncate(block_path, PALF_PHY_BLOCK_SIZE)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "ftruncate failed", K(ret), KPC(this), K(file_size));
  } else {
    PALF_LOG(INFO, "try_recovery_last_block_ success", "origin_size", file_size);
  }
  if (-1 != fd) {
    ::close(fd);
  }
  return ret;
}

} // end of logservice
} // end of oceanbase
