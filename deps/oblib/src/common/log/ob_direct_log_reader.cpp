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

#include "common/log/ob_direct_log_reader.h"

using namespace oceanbase::common;

ObDirectLogReader::ObDirectLogReader()
{
}

ObDirectLogReader::~ObDirectLogReader()
{
}

int ObDirectLogReader::read_log(LogCommand &cmd, uint64_t &log_seq,
                                char *&log_data, int64_t &data_len)
{
  int ret = OB_SUCCESS;

  ObLogEntry entry;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "ObSingleLogReader has not been initialized", K(ret));
  } else if (OB_FAIL(read_header(entry)) && OB_READ_NOTHING != ret) {
    SHARE_LOG(ERROR, "read_header failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (log_buffer_.get_remain_data_len() < entry.get_log_data_len()) {
      if (OB_FAIL(read_log_())) {
        SHARE_LOG(ERROR, "read log error", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (log_buffer_.get_remain_data_len() < entry.get_log_data_len()) {
      ret = OB_LAST_LOG_NOT_COMPLETE;
      SHARE_LOG(ERROR, "last log not complete",
                     K(file_id_), K(log_buffer_.get_remain_data_len()),
                     K(entry.get_log_data_len()));
      SHARE_LOG(WARN, "log_buffer_",
                      KP(log_buffer_.get_data()), K(log_buffer_.get_limit()),
                      K(log_buffer_.get_position()), K(log_buffer_.get_capacity()));
      hex_dump(log_buffer_.get_data(),
               static_cast<int32_t>(log_buffer_.get_limit()),
               true,
               OB_LOG_LEVEL_WARN);
    } else if (OB_FAIL(entry.check_data_integrity(
        log_buffer_.get_data() + log_buffer_.get_position()))) {
      // overwrite ret on purpose
      ret = OB_LAST_LOG_RUINNED;
      SHARE_LOG(ERROR, "check_data_integrity failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (last_log_seq_ != 0 &&
        (OB_LOG_SWITCH_LOG == entry.cmd_ ?
         (last_log_seq_ != entry.seq_ && last_log_seq_ + 1 != entry.seq_) :
         (last_log_seq_ + 1) != entry.seq_)) {
      SHARE_LOG(ERROR, "the log sequence is not continuous",
                     K(last_log_seq_), K(entry.seq_), K(entry.cmd_));
      ret = OB_ERROR;
    }
  }

  if (OB_SUCC(ret)) {
    last_log_seq_ = entry.seq_;
    cmd = static_cast<LogCommand>(entry.cmd_);
    log_seq = entry.seq_;
    log_data = log_buffer_.get_data() + log_buffer_.get_position();
    data_len = entry.get_log_data_len();
    log_buffer_.get_position() += data_len;
  }

  if (OB_SUCC(ret)) {
    SHARE_LOG(DEBUG, "LOG ENTRY:",
                     "SEQ", entry.seq_,
                     "CMD", cmd,
                     "DATA_LEN", data_len,
                     "POS", pos_);
    pos_ += entry.header_.header_length_ + entry.header_.data_length_;
  }

  return ret;
}

