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

#include "ob_log_common.h"
#include "ob_clog_config.h"
#include "ob_log_timer_utility.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/utility/utility.h"

namespace oceanbase {
using namespace common;
namespace clog {
char ObNewLogFileBuf::buf_[CLOG_DIO_ALIGN_SIZE] __attribute__((aligned(CLOG_DIO_ALIGN_SIZE)));

static class NEW_LOG_FILE_BUF_CONSTRUCTOR {
public:
  NEW_LOG_FILE_BUF_CONSTRUCTOR()
  {
    const char* mark_str = "NL";
    const int64_t mark_length = static_cast<int64_t>(STRLEN(mark_str));
    const int64_t buf_length = static_cast<int64_t>(sizeof(ObNewLogFileBuf::buf_));
    memset(ObNewLogFileBuf::buf_, 0, sizeof(ObNewLogFileBuf::buf_));
    for (int64_t i = 0; i + mark_length <= buf_length; i += mark_length) {
      STRCPY(ObNewLogFileBuf::buf_ + i, mark_str);
    }
  }
  ~NEW_LOG_FILE_BUF_CONSTRUCTOR()
  {}
} new_log_file_buf_constructor_;

int close_fd(const int fd)
{
  int ret = OB_SUCCESS;
  if (0 > fd) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(fd));
  } else {
    if (0 != close(fd)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "close fd error", K(ret), K(fd), KERRMSG);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObLogInfo)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_valid()) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = proposal_id_.serialize(buf, buf_len, new_pos))) {
    CLOG_LOG(WARN, "encode proposal_id fail", K(ret));
  } else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, new_pos, log_id_))) {
    CLOG_LOG(WARN, "encode log_id fail", K(ret));
  } else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, new_pos, submit_timestamp_))) {
    CLOG_LOG(WARN, "encode submit_timestamp_ fail", K(ret));
  } else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, new_pos, size_))) {
    CLOG_LOG(WARN, "encode size_ fail", K(ret));
  } else if (new_pos + size_ > buf_len) {
    ret = OB_SERIALIZE_ERROR;
  } else {
    MEMCPY(buf + new_pos, buff_, size_);
    new_pos += size_;
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLogInfo)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t tmp_log_id = 0;
  if (NULL == buf || pos < 0 || pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = proposal_id_.deserialize(buf, data_len, new_pos))) {
    CLOG_LOG(WARN, "decode proposal_id_ fail", K(ret));
  } else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, new_pos, &tmp_log_id))) {
    CLOG_LOG(WARN, "decode log_id fail", K(ret));
  } else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, new_pos, &submit_timestamp_))) {
    CLOG_LOG(WARN, "decode submit_timestamp_ fail", K(ret));
  } else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, new_pos, &size_))) {
    CLOG_LOG(WARN, "decode size_ fail", K(ret));
  } else if (new_pos + size_ > data_len) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    log_id_ = static_cast<uint64_t>(tmp_log_id);
    buff_ = const_cast<char*>(buf + new_pos);
    new_pos += size_;
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogInfo)
{
  return proposal_id_.get_serialize_size() + serialization::encoded_length_i64(log_id_) +
         serialization::encoded_length_i64(submit_timestamp_) + serialization::encoded_length_i64(size_) + size_;
}

int64_t aio_write(const int fd, void* buf, const int64_t count, const int64_t offset, const int64_t timeout_,
    iocb& iocb_, io_context_t& ctx_, io_event& ioevent_)
{
  int64_t ret = 0;
  ObLogTimerUtility timer;
  timer.start_timer();
  if (fd < 0 || OB_ISNULL(buf) || count < 0 || offset < 0 || timeout_ < 0) {
    ret = -1;
    CLOG_LOG(ERROR, "aio_write invalid arguments", K(fd), K(buf), K(count), K(offset), K(timeout_));
  } else {
    struct iocb* iocb_ptr = NULL;
    int io_submit_err_code = 0;
    io_prep_pwrite(&iocb_, fd, buf, count, offset);
    iocb_ptr = &iocb_;

    if (1 != (io_submit_err_code = io_submit(ctx_, 1, &iocb_ptr))) {
      ret = -1;
      CLOG_LOG(ERROR, "io_submit", K(ret), KERRNOMSG(-io_submit_err_code), K(offset), K(fd), K(count));
    } else {
      struct timespec timeout;
      int64_t start_time = ObTimeUtility::current_time();
      int wait_ret = 0;
      timeout.tv_sec = timeout_ / 1000000;            // Convert microseconds to seconds
      timeout.tv_nsec = (timeout_ % 1000000) * 1000;  // Convert microseconds to nanoseconds
      int tmp_ret = OB_SUCCESS;
      while (OB_SUCCESS == tmp_ret) {
        if (1 == (wait_ret = io_getevents(ctx_, 1, 1, &ioevent_, &timeout))) {
          if (0 != ioevent_.res2) {
            tmp_ret = OB_IO_ERROR;
            CLOG_LOG(WARN, "write fail", "e.res2", ioevent_.res2);
          } else if (static_cast<int>(ioevent_.res) < 0 || count != static_cast<int64_t>(ioevent_.res)) {
            tmp_ret = OB_IO_ERROR;
            CLOG_LOG(WARN,
                "ioevent error",
                KERRNOMSG(-static_cast<int>(ioevent_.res)),
                "expect write_len",
                count,
                "actual write len",
                ioevent_.res);
          } else {
            break;
          }
        } else if (0 == wait_ret) {
          int64_t end_time = ObTimeUtility::current_time();
          if (end_time - start_time > timeout_) {
            CLOG_LOG(WARN, "wait io event timeout", "wait time", (end_time - start_time), "expected timeout", timeout_);
            tmp_ret = OB_AIO_TIMEOUT;
          } else {
            tmp_ret = OB_IO_ERROR;
            CLOG_LOG(WARN, "wait io event error", K(ret), KERRNOMSG(-wait_ret));
          }
        }
      }
      if (OB_SUCCESS != tmp_ret) {
        ret = -1;
        CLOG_LOG(ERROR, "aio_write failed", K(ret), K(tmp_ret));
      } else {
        ret = count;
      }
    }
  }
  timer.finish_timer(__FILE__, __LINE__, CLOG_PERF_WARN_THRESHOLD);
  return ret;
}
}  // end namespace clog
}  // end namespace oceanbase
