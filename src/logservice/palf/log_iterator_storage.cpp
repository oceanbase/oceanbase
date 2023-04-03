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

#include "log_iterator_storage.h"
#include "lib/function/ob_function.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_utility.h"
#include "log_storage_interface.h"
#include "log_define.h"
#include "log_reader_utils.h"
#include "lsn.h"
namespace oceanbase
{
namespace palf
{
IteratorStorage::IteratorStorage() :
  start_lsn_(),
  end_lsn_(),
  read_buf_(),
  block_size_(0),
  log_storage_(NULL),
  is_inited_(false) {}

IteratorStorage::~IteratorStorage()
{
  destroy();
}

int IteratorStorage::init(
    const LSN &start_lsn,
    const int64_t block_size,
    const GetFileEndLSN &get_file_end_lsn,
    ILogStorage *log_storage)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else {
    start_lsn_ = start_lsn;
    end_lsn_ = start_lsn;
    read_buf_.reset();
    block_size_ = block_size;
    log_storage_ = log_storage;
    get_file_end_lsn_ = get_file_end_lsn;
    is_inited_ = true;
    PALF_LOG(TRACE, "IteratorStorage init success", KPC(this));
  }
  return ret;
}

void IteratorStorage::destroy()
{
  is_inited_ = false;
  start_lsn_.reset();
  end_lsn_.reset();
  read_buf_.reset();
  block_size_ = 0;
  log_storage_ = NULL;
}

void IteratorStorage::reuse(const LSN &start_lsn)
{
  start_lsn_ = start_lsn;
  end_lsn_ = start_lsn;
}

// read data from 'read_buf_'
int IteratorStorage::pread(
    const int64_t pos,
    const int64_t in_read_size,
    char *&buf,
    int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  const int64_t real_in_read_size = MIN(in_read_size, get_file_end_lsn_() - (start_lsn_ + pos));
  int64_t real_pos = pos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  // there is no valid data
  } else if (0 > pos || 0 > real_in_read_size) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(pos), K(in_read_size), KPC(this));
  } else if (pos > get_valid_data_len_()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "want to read position is greater than max valid data len", K(pos), KPC(this));
  } else if (0 == real_in_read_size) {
    ret = OB_ITER_END;
    PALF_LOG(WARN, "IteratorStorage has iterate end", K(ret), KPC(this));
  } else if (OB_FAIL(read_data_from_storage_(real_pos, real_in_read_size, buf, out_read_size))) {
    PALF_LOG(WARN, "read_data_from_storage_ failed", K(ret), K(pos), K(in_read_size), KP(buf), KPC(this));
  } else {
    start_lsn_ = start_lsn_ + real_pos;
    end_lsn_ = start_lsn_ + out_read_size;
    PALF_LOG(TRACE, "IteratorStorage pread success", K(ret), K(pos), K(in_read_size), K(real_in_read_size),
        K(*buf), KP(buf), K(out_read_size), KPC(this));
  }
  return ret;
}

MemoryStorage::MemoryStorage() : buf_(NULL),
                                 buf_len_(0),
                                 start_lsn_(),
                                 log_tail_(),
                                 is_inited_(false)
{
}

MemoryStorage::~MemoryStorage()
{
  destroy();
}

int MemoryStorage::init(const LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (false == start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(start_lsn));
  } else {
    buf_ = NULL;
    buf_len_ = 0;
    log_tail_ = start_lsn_ = start_lsn;
    is_inited_ = true;
    PALF_LOG(TRACE, "MemoryStorage init success", K(ret), KPC(this));
  }
  return ret;
}

void MemoryStorage::destroy()
{
  is_inited_ = false;
  buf_ = NULL;
  buf_len_ = 0;
  log_tail_.reset();
  start_lsn_.reset();
}

int MemoryStorage::append(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    buf_ = buf;
    buf_len_ = buf_len;
    start_lsn_ = log_tail_;
    log_tail_ = log_tail_ + buf_len;
    PALF_LOG(TRACE, "IteratorStorage append success", K(ret), KPC(this));
  }
  return ret;
}

int MemoryStorage::pread(const LSN &lsn, const int64_t in_read_size, ReadBuf &read_buf, int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == lsn.is_valid() || 0 >= in_read_size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (lsn >= log_tail_) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
  } else if (lsn < start_lsn_) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
  } else {
    const offset_t pos = lsn - start_lsn_;
    read_buf.buf_ = const_cast<char*>(buf_) + pos;
    out_read_size = MIN(log_tail_ - lsn, in_read_size);
    read_buf.buf_len_ = out_read_size;
    PALF_LOG(TRACE, "MemoryStorage pread success", K(ret), K(pos), K(lsn), K(in_read_size), KPC(this), K(read_buf), K(out_read_size));
  }
  return ret;
}

MemoryIteratorStorage:: ~MemoryIteratorStorage()
{
  destroy();
}

void MemoryIteratorStorage::destroy()
{
  IteratorStorage::destroy();
}

int MemoryIteratorStorage::read_data_from_storage_(
    int64_t &pos,
    const int64_t in_read_size,
    char *&buf,
    int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  const LSN start_lsn = start_lsn_ + pos;
  if (OB_FAIL(log_storage_->pread(start_lsn, in_read_size, read_buf_, out_read_size))) {
    PALF_LOG(WARN, "MemoryIteratorStorage pread failed", K(ret), KPC(this), K(start_lsn));
  } else {
    PALF_LOG(TRACE, "MemoryIteratorStorage read_data_from_storage_ success", K(ret), KPC(this), K(start_lsn));
    buf = read_buf_.buf_;
  }
  return ret;
}

DiskIteratorStorage::~DiskIteratorStorage()
{
  destroy();
}

void DiskIteratorStorage::destroy()
{
  free_read_buf(read_buf_);
  IteratorStorage::destroy();
}

int DiskIteratorStorage::read_data_from_storage_(
    int64_t &pos,
    const int64_t in_read_size,
    char *&buf,
    int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  int64_t remain_valid_data_size = 0;
  if (OB_FAIL(ensure_memory_layout_correct_(pos, in_read_size, remain_valid_data_size))) {
    PALF_LOG(WARN, "ensure_memory_layout_correct_ failed", K(ret), K(pos), K(in_read_size), KPC(this));
  } else {
    // avoid read repeated data from disk
    const LSN curr_round_read_lsn = start_lsn_ + pos + remain_valid_data_size;
    const int64_t real_in_read_size = in_read_size - remain_valid_data_size;
    read_buf_.buf_ += remain_valid_data_size;
    if (0ul == real_in_read_size) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "real read size is zero, unexpected error!!!", K(ret), K(real_in_read_size));
    } else if (OB_FAIL(log_storage_->pread(curr_round_read_lsn,
            real_in_read_size,
            read_buf_, out_read_size))) {
      PALF_LOG(WARN, "ILogStorage pread failed", K(ret), K(pos), K(in_read_size), KPC(this));
    }
    read_buf_.buf_ -= remain_valid_data_size;
    if (OB_SUCC(ret)) {
      buf = read_buf_.buf_;
      out_read_size += remain_valid_data_size;
      // check the 'read_buf_' whether has LogBlockHeader, if has, the return LSN need decrese MAX_INFO_BLOCK_SIZE
      PALF_LOG(TRACE, "read_data_from_storage_ success", K(ret), KPC(this), K(pos), K(in_read_size),
          K(curr_round_read_lsn), K(remain_valid_data_size), K(real_in_read_size), K(out_read_size));
    }
  }
  return ret;
}

int DiskIteratorStorage::ensure_memory_layout_correct_(
    const int64_t pos,
    const int64_t in_read_size,
    int64_t &remain_valid_data_size)
{
  int ret = OB_SUCCESS;
  const int64_t max_valid_buf_len = read_buf_.buf_len_ - LOG_DIO_ALIGN_SIZE;
  ReadBuf tmp_read_buf = read_buf_;
  // buf not enough, need alloc or expand
  if (in_read_size > max_valid_buf_len) {
    ret = alloc_read_buf("DiskIteratorStorage", in_read_size, tmp_read_buf);
    PALF_LOG(TRACE, "need alloc read buf", K(ret), KPC(this), K(tmp_read_buf));
  }
  if (OB_SUCC(ret)) {
    PALF_LOG(TRACE, "before ensure_memory_layout_correct_", KPC(this), K(in_read_size), K(remain_valid_data_size));
    // memmove tail valid part data to header
    do_memove_(tmp_read_buf, pos, remain_valid_data_size);
    read_buf_ = tmp_read_buf;
    PALF_LOG(TRACE, "after ensure_memory_layout_correct_", KPC(this), K(in_read_size), K(remain_valid_data_size));
  }
  return ret;
}

void DiskIteratorStorage::do_memove_(ReadBuf &dst, const int64_t pos, int64_t &valid_tail_part_size)
{
  valid_tail_part_size = lower_align(get_valid_data_len_() - pos, LOG_DIO_ALIGN_SIZE);
  OB_ASSERT(valid_tail_part_size >= 0);
  if (false == read_buf_.is_valid()) {
    // do nothing
    PALF_LOG(TRACE, "src is invalid, no need memove", K(dst), K(read_buf_), KPC(this), K(pos));
  } else {
    OB_ASSERT(valid_tail_part_size < dst.buf_len_);
    MEMMOVE(dst.buf_, read_buf_.buf_ + pos, valid_tail_part_size);
    PALF_LOG(TRACE, "do_memove_ success", K(dst), KPC(this), K(valid_tail_part_size), K(pos));
    if (read_buf_ != dst) {
      PALF_LOG(TRACE, "src is not same as dst, need free src", K(dst), KPC(this));
      free_read_buf(read_buf_);
    }
  }
}
} // end namespace palf
} // end namespace oceanbase
