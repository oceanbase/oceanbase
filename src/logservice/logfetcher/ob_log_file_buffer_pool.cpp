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
 *
 * Fetching log-related RPC implementation
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_file_buffer_pool.h"

namespace oceanbase
{
namespace logfetcher
{

////////////////////////////// LogFileDataBuffer //////////////////////////////

LogFileDataBuffer::LogFileDataBuffer():
    host_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    access_ts_(OB_INVALID_TIMESTAMP),
    max_write_cnt_(0),
    cur_write_cnt_(0),
    complete_cnt_(0),
    start_lsn_(),
    is_dynamic_(false),
    owner_(nullptr),
    buffer_size_(0),
    data_buffer_(nullptr)
{
  reset_write_log_arr_();
}

int LogFileDataBuffer::init(LogFileDataBufferPool *host,
    const int64_t max_wr_cnt,
    uint64_t tenant_id,
    bool is_dynamic)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(host)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("host is null, invalid", K(host));
  } else {
    host_ = host;
    tenant_id_ = tenant_id;
    max_write_cnt_ = max_wr_cnt;
    is_dynamic_ = is_dynamic;
    access_ts_ = ObTimeUtility::current_time();
    reset_write_log_arr_();
    if (OB_FAIL(prepare_write_buffer_())) {
      LOG_ERROR("failed to prepare write buffer", K(max_write_cnt_));
    }
  }

  return ret;
}

int LogFileDataBuffer::prepare(const int64_t cur_wr_cnt,
    const palf::LSN &start_lsn,
    const void *owner)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cur_wr_cnt > max_write_cnt_ || ! start_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("cur_wr_cnt is greater thant max_write_cnt or start_lsn is invalid",
        K(cur_wr_cnt), K(start_lsn));
  } else {
    reset_write_log_arr_();
    cur_write_cnt_ = cur_wr_cnt;
    start_lsn_ = start_lsn;
    access_ts_ = ObTimeUtility::current_time();
    owner_ = owner;
  }

  return ret;
}

void LogFileDataBuffer::reset_param()
{
  owner_ = nullptr;
  (void) reset_write_log_arr_();
  start_lsn_.reset();
  cur_write_cnt_ = 0;
  complete_cnt_ = 0;
}

void LogFileDataBuffer::revert()
{
  if (OB_ISNULL(host_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "host is null, cannot revert");
  } else {
    access_ts_ = ObTimeUtility::current_time();
    host_->revert(this);
  }
}

void LogFileDataBuffer::destroy()
{
  if (nullptr != data_buffer_) {
    ob_free(data_buffer_);
    data_buffer_ = nullptr;
  }
  reset_write_log_arr_();
  tenant_id_ = OB_INVALID_TENANT_ID;
  access_ts_ = OB_INVALID_TIMESTAMP;
  buffer_size_ = 0;
  max_write_cnt_ = 0;
  cur_write_cnt_ = 0;
  complete_cnt_ = 0;
  start_lsn_.reset();
  is_dynamic_ = false;
  owner_ = nullptr;
}

int LogFileDataBuffer::write_data(const int32_t seq_no,
    const char *data,
    const int64_t data_len,
    const palf::LSN data_start_lsn,
    const palf::LSN next_req_lsn,
    const share::SCN replayable_point,
    const obrpc::ObCdcFetchRawStatus &fetch_status,
    const obrpc::FeedbackType feed_back,
    const int err,
    const obrpc::ObRpcResultCode &rcode,
    const int64_t rpc_cb_start_time,
    const int64_t sub_rpc_send_time)
{
  int ret = OB_SUCCESS;

  if (seq_no >= max_write_cnt_ || seq_no < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("seq no is greater thant max write cnt", K(seq_no), KP(data), K(data_len), K(data_start_lsn));
  } else if (! data_start_lsn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get invalid write_lsn, unexpected", K(data_start_lsn), KP(data), K(data_len), K(seq_no));
  } else if (0 > data_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get invalid data or data_len", KP(data), K(data_len));
  } else if (write_logs_[seq_no].is_valid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("there is a write log for seq_no", K(seq_no), "write_log", write_logs_[seq_no],
        K(data_start_lsn));
  } else if (data_len > 0 && nullptr != data) {
    // write data and meta
    if (data_start_lsn >= start_lsn_) {
      const palf::offset_t write_offset = data_start_lsn - start_lsn_;
      if (write_offset + data_len > buffer_size_) {
        ret = OB_SIZE_OVERFLOW;
        LOG_ERROR("data_size is too large to fill into the buffer", K(data_start_lsn), K(start_lsn_),
            K(data_len));
      } else {
        MEMCPY(data_buffer_ + write_offset, data, data_len);
        write_logs_[seq_no].reset(data_start_lsn, next_req_lsn, data_len, replayable_point,
            fetch_status, feed_back, err, rcode, rpc_cb_start_time, sub_rpc_send_time);
      }
    } else {
      //  ----------------------------------------------------
      //  |     <-------------  data_offset  ----------->   |
      // data_start_lsn                              start_lsn_
      const palf::offset_t data_offset = start_lsn_ - data_start_lsn;
      if (data_len >= buffer_size_ + data_offset) {
        // |    <--------------------          data_len         -------------------->              |
        // -----------------------------------------------------------------------------------------
        // |      data_offset     |       <------------ buffer_size_ --------------->    |         |
        // data_start_lsn    start_lsn_                                               buffer_end  data_end
        ret = OB_SIZE_OVERFLOW;
        LOG_ERROR("data_size is too large to fill into the buffer", K(data_start_lsn), K(start_lsn_),
            K(data_len));
      } else if (data_len > data_offset){
        //                                                      data_end
        // |        <------------ data_len ------------>          |
        // -------------------------------------------------------------------------------
        // |      data_offset     |       <------------ buffer_size_ --------------->    |
        // data_start_lsn    start_lsn_                                               buffer_end
        MEMCPY(data_buffer_, data + data_offset, data_len - data_offset);
        write_logs_[seq_no].reset(start_lsn_, next_req_lsn, data_len - data_offset, replayable_point,
            fetch_status, feed_back, err, rcode, rpc_cb_start_time, sub_rpc_send_time);
      } else {
        //               data_end
        // |<-- data_len -->|
        // -------------------------------------------------------------------------------
        // |      data_offset     |       <------------ buffer_size_ --------------->    |
        // data_start_lsn    start_lsn_                                               buffer_end
        write_logs_[seq_no].reset(start_lsn_, next_req_lsn, 0, replayable_point, fetch_status,
            feed_back, err, rcode, rpc_cb_start_time, sub_rpc_send_time);
      }
    }

    if (OB_FAIL(ret)) {
      write_logs_[seq_no].reset();
    } else {
      ATOMIC_INC(&complete_cnt_);
    }
  } else {
    // no data, just write meta
    palf::LSN meta_lsn = data_start_lsn > start_lsn_ ? data_start_lsn : start_lsn_;
    write_logs_[seq_no].reset(meta_lsn, next_req_lsn, data_len, replayable_point,
        fetch_status, feed_back, err, rcode, rpc_cb_start_time, sub_rpc_send_time);
    ATOMIC_INC(&complete_cnt_);
  }

  return ret;
}

int LogFileDataBuffer::get_data(const char *&data,
    int64_t &data_len,
    int32_t &valid_rpc_cnt,
    bool &is_readable,
    bool &is_active,
    obrpc::ObCdcFetchRawSource &data_end_source,
    share::SCN &replayable_point,
    ObIArray<RawLogDataRpcStatus> &sub_rpc_status_arr)
{
  int ret = OB_SUCCESS;
  bool data_end = false;
  palf::LSN end_lsn = start_lsn_;
  data = nullptr;
  data_len = 0;
  valid_rpc_cnt = 0;
  is_readable = false;
  is_active = false;
  replayable_point = share::SCN::max_scn();

  for (int i = 0; OB_SUCC(ret) && i < max_write_cnt_ && write_logs_[i].is_valid_; i++) {
    if (! data_end) {
      if (end_lsn != write_logs_[i].write_lsn_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("write_lsn doesn't match end_lsn, unexpcected", K(end_lsn), K(write_logs_[i]));
      } else if (OB_SUCCESS != write_logs_[i].sub_rpc_status_.err_ ||
          OB_SUCCESS != write_logs_[i].sub_rpc_status_.rcode_.rcode_) {
        // if a rpc response return err, regard this rpc response
        data_end = true;
        LOG_INFO("get failed sub rpc response", K(i), "status", write_logs_[i].sub_rpc_status_);
      } else if (write_logs_[i].write_size_ <= 0) {
        // how to find out that there is no newer log in server ?
        // if there is no log for the first request, we regard that
        // there is no newer log in server.
        if (0 == i) {
          is_readable = true;
        }
        is_active = true;
        data_end = true;
        LOG_TRACE("get write_logs whose write_size less or equal than zero, data_end",
            K(write_logs_[i]), K(i));
      } else {
        if (end_lsn + write_logs_[i].write_size_ > write_logs_[i].next_req_lsn_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("end_lsn would be greater than next_req_lsn, unexpected", K(end_lsn), K(write_logs_[i]));
        } else {
          end_lsn = end_lsn + write_logs_[i].write_size_;
          valid_rpc_cnt++;
          replayable_point = share::SCN::min(replayable_point, write_logs_[i].replayable_point_);
          if (end_lsn < write_logs_[i].next_req_lsn_) {
            is_readable = true;
            is_active = true;
            data_end = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        data_end_source = write_logs_[i].sub_rpc_status_.fetch_status_.get_source();
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(sub_rpc_status_arr.push_back(write_logs_[i].sub_rpc_status_))) {
      LOG_ERROR("failed to push back sub_rpc_status", K(i), K(write_logs_[i]));
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    data_len = end_lsn - start_lsn_;
    data = data_buffer_;

    if (0 == data_len && !is_readable) {
      // not readable even if end_lsn % PALF_BLOCK_SIZE = 0 because first rpc failed,
      // which could be deduced from the condition.
    } else if (0 == end_lsn.val_ % palf::PALF_BLOCK_SIZE) {
      is_readable = true;
    }

    if (! is_readable) {
      LOG_INFO("result not readable", K(end_lsn), KPC(this));
    }
  }

  return ret;
}

int LogFileDataBuffer::set_buffer_(char *buf, const size_t buf_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr != data_buffer_ || 0 != buffer_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("data buffer is not null or buffer_size is not equal to zero", KP(data_buffer_),
        K(buffer_size_), KP(buf), K(buf_size));
  } else {
    data_buffer_ = buf;
    buffer_size_ = buf_size;
  }

  return ret;
}

void LogFileDataBuffer::reset_write_log_arr_()
{
  for (int i = 0; i < RawLogFileRpcRequest::MAX_SEND_REQ_CNT; i++) {
    write_logs_[i].reset();
  }
}

int LogFileDataBuffer::prepare_write_buffer_()
{
  int ret = OB_SUCCESS;

  char *tmp_buffer = nullptr;
  const int64_t tmp_buf_size = is_dynamic_ ?
      max_write_cnt_ * obrpc::ObCdcFetchRawLogResp::FETCH_BUF_LEN : palf::PALF_PHY_BLOCK_SIZE;
  if (OB_ISNULL(tmp_buffer = static_cast<char*>(ob_malloc(tmp_buf_size,
    lib::ObMemAttr(tenant_id_, "LogFileWrtBuf"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate write buffer", K(tenant_id_));
  } else if (OB_FAIL(set_buffer_(tmp_buffer, tmp_buf_size))) {
    LOG_ERROR("failed to set buffer", K(data_buffer_), K(buffer_size_), K(tmp_buffer),
        K(tmp_buf_size));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_buffer)) {
    ob_free(tmp_buffer);
  }

  return ret;
}

////////////////////////////// LogFileDataBufferPool //////////////////////////////

LogFileDataBufferPool::LogFileDataBufferPool():
    is_inited_(false),
    lock_(ObLatchIds::DEFAULT_SPIN_RWLOCK),
    tenant_id_(OB_INVALID_TENANT_ID),
    max_hold_buffer_cnt_(0),
    allocated_req_cnt_(0),
    free_list_()
    {}

int LogFileDataBufferPool::init(const uint64_t tenant_id,
    const int64_t max_write_cnt,
    const int64_t max_hold_buffer_cnt)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("log file data buffer pool has been inited");
  } else if (OB_INVALID_TENANT_ID == tenant_id || max_hold_buffer_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument for logfiledatabufferpool", K(tenant_id), K(max_hold_buffer_cnt));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    max_write_cnt_ = max_write_cnt;
    max_hold_buffer_cnt_ = max_hold_buffer_cnt;
    LOG_INFO("LogFileDataBufferPool init succ", K(tenant_id_), K(max_write_cnt_), K(max_hold_buffer_cnt_));
  }

  return ret;
}

void LogFileDataBufferPool::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    int64_t buf_cnt = 0;
    while (OB_SUCC(ret)) {
      ObLink *buf = nullptr;
      if (OB_FAIL(free_list_.pop(buf))) {
        if (OB_EAGAIN != ret) {
          LOG_ERROR("failed to pop buffer from free_list");
        }
      } else if (OB_ISNULL(buf)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "get null buf from free list, unexpected", K(buf));
      } else {
        LogFileDataBuffer *data_buf = static_cast<LogFileDataBuffer*>(buf);
        data_buf->destroy();
        OB_DELETE(LogFileDataBuffer, "LogFDataBuf", data_buf);
        buf_cnt++;
      }
    }

    if (buf_cnt < allocated_req_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("freed item is less than allocated item, memory may leak", K(buf_cnt), K(allocated_req_cnt_));
    }

    tenant_id_ = OB_INVALID_TENANT_ID;
    max_write_cnt_ = 0;
    max_hold_buffer_cnt_ = 0;
    allocated_req_cnt_ = 0;
    is_inited_ = false;
  }
}

int LogFileDataBufferPool::get(const int64_t cur_wr_cnt,
    const palf::LSN &start_lsn,
    const void *owner,
    LogFileDataBuffer *&buffer)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("LogFileDataBufferPool hasn't been inited");
  } else {
    ObLink *link_node = nullptr;
    LogFileDataBuffer *tmp_buffer = nullptr;
    buffer = nullptr;
    if (cur_wr_cnt > max_write_cnt_/2) {
      if (OB_FAIL(free_list_.pop(link_node))) {
        if (OB_EAGAIN != ret) {
          LOG_ERROR("failed to pop link node from free_list");
        } else if (OB_FAIL(alloc_static_buffer_(tmp_buffer))) {
          LOG_WARN("failed to allocate static_buffer", K(buffer), K(allocated_req_cnt_), K(max_hold_buffer_cnt_));
        } else {
          link_node = tmp_buffer;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(buffer = static_cast<LogFileDataBuffer*>(link_node))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("buffer is null when popped from free_list", K(buffer));
        } else if (OB_FAIL(buffer->prepare(cur_wr_cnt, start_lsn, owner))) {
          LOG_ERROR("buffer failed to prepare", K(cur_wr_cnt), K(start_lsn), K(owner));
        }
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(buffer)) {
        int tmp_ret = OB_SUCCESS;
        buffer->reset_param();
        if (OB_TMP_FAIL(free_list_.push_front(buffer))) {
          OB_DELETE(LogFileDataBuffer, "LogFDataBuf", buffer);
          LOG_ERROR_RET(tmp_ret, "failed to push buffer into free_list, free it", KP(buffer));
          update_allocated_req_cnt_(-1);
        }
      }
    } else {
      if (OB_ISNULL(buffer = OB_NEW(LogFileDataBuffer,
          lib::ObMemAttr(tenant_id_, "DLogFDataBuf")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memory for dynamic buffer");
      } else if (OB_FAIL(buffer->init(this, cur_wr_cnt, tenant_id_, true))) {
        LOG_ERROR("failed to init dynamic LogFileDatabuffer", K(cur_wr_cnt), K(tenant_id_));
      } else if (OB_FAIL(buffer->prepare(cur_wr_cnt, start_lsn, owner))) {
        LOG_ERROR("dynamic buffer failed to prepare", K(cur_wr_cnt), K(start_lsn), K(owner));
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(buffer)) {
        OB_DELETE(LogFileDataBuffer, "DLogFDataBuf", buffer);
      }
    }
  }

  return ret;
}

void LogFileDataBufferPool::revert(LogFileDataBuffer *buffer)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("log file data buffer reverted is null", K(buffer));
  } else if (buffer->is_dynamic()) {
    OB_DELETE(LogFileDataBuffer, "DLogFdataBuf", buffer);
  } else {
    buffer->reset_param();
    if (OB_FAIL(free_list_.push_front(buffer))) {
      OB_DELETE(LogFileDataBuffer, "LogFDataBuf", buffer);
      LOG_ERROR("failed to revert buffer back into free list, free it", KP(buffer));
      update_allocated_req_cnt_(-1);
    }
  }
}

int LogFileDataBufferPool::alloc_static_buffer_(LogFileDataBuffer *&buffer)
{
  int ret = OB_SUCCESS;
  LogFileDataBuffer *static_buffer = nullptr;
  SpinWLockGuard guard(lock_);

  if (allocated_req_cnt_ >= max_hold_buffer_cnt_) {
    ret = OB_EAGAIN;
    LOG_WARN("allocated_req_cnt_ is greater or equal than max_hold_buffer_cnt_",
        K(allocated_req_cnt_), K(max_hold_buffer_cnt_));
  } else if (OB_ISNULL(static_buffer= OB_NEW(LogFileDataBuffer,
      lib::ObMemAttr(tenant_id_, "LogFDataBuf")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc LogFileDataBuf struct", K(tenant_id_), K(max_hold_buffer_cnt_));
  } else if (OB_FAIL(static_buffer->init(this, max_write_cnt_, tenant_id_, false))) {
    LOG_ERROR("static LogFileDataBuffer failed to init", K(max_write_cnt_), K(tenant_id_));
  } else {
    buffer = static_buffer;
    allocated_req_cnt_++;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(static_buffer)) {
    OB_DELETE(LogFileDataBuffer, "LogFDataBuf", static_buffer);
    buffer = nullptr;
  }

  return ret;
}

void LogFileDataBufferPool::update_allocated_req_cnt_(const int64_t delta)
{
  SpinWLockGuard guard(lock_);
  allocated_req_cnt_ += delta;
}

void LogFileDataBufferPool::try_recycle_expired_buffer()
{
  int ret = OB_SUCCESS;
  ObLink *link_node = nullptr;
  int64_t recycled_cnt = 0;
  ObSpLinkQueue tmp_list;
  while (OB_SUCC(free_list_.pop(link_node))) {
    LogFileDataBuffer *buffer = static_cast<LogFileDataBuffer*>(link_node);
    if (OB_ISNULL(buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get null buffer from buffer pool, unexpected", K(buffer));
      recycled_cnt++;
    } else if (buffer->is_expired()) {
      buffer->destroy();
      OB_DELETE(LogFileDataBuffer, "LogFDataBuf", buffer);
      buffer = nullptr;
      recycled_cnt++;
    } else if (OB_FAIL(tmp_list.push(buffer))) {
      LOG_ERROR("failed to push buffer into tmp_list when recycling");
      buffer->destroy();
      OB_DELETE(LogFileDataBuffer, "LogFDataBuf", buffer);
      buffer = nullptr;
      recycled_cnt++;
    }
  }
  // ignore ret
  while (OB_SUCC(tmp_list.pop(link_node))) {
    LogFileDataBuffer *buffer = static_cast<LogFileDataBuffer*>(link_node);
    if (OB_ISNULL(buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get null buffer from buffer pool, unexpected", K(buffer));
      recycled_cnt++;
    } else if (OB_FAIL(free_list_.push(buffer))) {
      LOG_ERROR("failed to push buffer into free_list when recycling");
      buffer->destroy();
      OB_DELETE(LogFileDataBuffer, "LogFDataBuf", buffer);
      buffer = nullptr;
      recycled_cnt++;
    }
  }
  // ignore ret
  update_allocated_req_cnt_(-recycled_cnt);
}

}
}