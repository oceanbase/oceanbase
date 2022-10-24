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

#include "common/log/ob_log_reader.h"

using namespace oceanbase::common;

ObLogReader::ObLogReader()
  : cur_log_file_id_(0),
    cur_log_seq_id_(0),
    max_log_file_id_(0),
    log_file_reader_(NULL),
    is_inited_(false),
    is_wait_(false),
    has_max_(false)
{
}

ObLogReader::~ObLogReader()
{
}

int ObLogReader::init(ObSingleLogReader *reader,
                      const char *log_dir,
                      const uint64_t log_file_id_start,
                      const uint64_t log_seq,
                      bool is_wait)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    SHARE_LOG(ERROR, "ObLogReader has been initialized before");
    ret = OB_INIT_TWICE;
  } else {
    if (OB_ISNULL(reader) || OB_ISNULL(log_dir)) {
      SHARE_LOG(ERROR, "Parameter is invalid", KP(reader), KP(log_dir));
      ret = OB_INVALID_ARGUMENT;
    } else {
      if (OB_FAIL(reader->init(log_dir))) {
        SHARE_LOG(ERROR, "reader init error", KP(log_dir), K(ret));
      } else {
        log_file_reader_ = reader;
        cur_log_file_id_ = log_file_id_start;
        cur_log_seq_id_ = log_seq;
        max_log_file_id_ = 0;
        is_wait_ = is_wait;
        has_max_ = false;
        is_inited_ = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (0 != log_seq) {
      if (OB_FAIL(seek(log_seq))) {
        SHARE_LOG(ERROR, "seek log seq error", K(log_seq), K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    is_inited_ = false;
  }

  return ret;
}

int ObLogReader::read_log(LogCommand &cmd,
                          uint64_t &seq,
                          char *&log_data,
                          int64_t &data_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "ObLogReader has not been initialized", K(ret));
  } else if (OB_ISNULL(log_file_reader_)) {
    SHARE_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
    ret = OB_ERROR;
  } else {
    if (!log_file_reader_->is_opened()) {
      ret = open_log_(cur_log_file_id_);
    }
    if (OB_SUCC(ret)) {
      ret = read_log_(cmd, seq, log_data, data_len);
      if (OB_SUCC(ret)) {
        cur_log_seq_id_ = seq;
      }
      if (OB_SUCCESS == ret && OB_LOG_SWITCH_LOG == cmd) {
        SHARE_LOG(INFO, "reach the end of log", K(cur_log_file_id_));
        // Regardless of opening success or failure: cur_log_file_id++, log_file_reader_->pos will be set to zero,
        if (OB_FAIL(log_file_reader_->close())) {
          SHARE_LOG(ERROR, "log_file_reader_ close error", K(ret));
        } else if (OB_FAIL(open_log_(++cur_log_file_id_, seq))
            && OB_READ_NOTHING != ret) {
          SHARE_LOG(WARN, "open log failed", K(cur_log_file_id_), K(seq), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObLogReader::revise_log(const bool force)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "ObLogReader has not been initialized", K(ret));
  } else if (OB_ISNULL(log_file_reader_)) {
    SHARE_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
    ret = OB_ERROR;
  } else {
    LogCommand cmd;
    uint64_t seq = 0;
    char *log_data = NULL;
    int64_t data_len = 0;
    if (!log_file_reader_->is_opened()) {
      ret = open_log_(cur_log_file_id_);
    }
    while (OB_SUCC(ret)) {
      ret = read_log_(cmd, seq, log_data, data_len);
      if (OB_SUCC(ret)) {
        cur_log_seq_id_ = seq;
      }
      if (OB_SUCCESS == ret && OB_LOG_SWITCH_LOG == cmd) {
        SHARE_LOG(INFO, "reach the end of log", K(cur_log_file_id_));
        // Regardless of opening success or failure: cur_log_file_id++, log_file_reader_->pos will be set to zero,
        if (OB_FAIL(log_file_reader_->close())) {
          SHARE_LOG(ERROR, "log_file_reader_ close error", K(ret));
        } else if (OB_FAIL(open_log_(++cur_log_file_id_, seq))
            && OB_READ_NOTHING != ret) {
          SHARE_LOG(WARN, "open log failed", K(cur_log_file_id_), K(seq), K(ret));
        }
      }
    }
    if (OB_READ_NOTHING == ret) {
      ret = OB_SUCCESS;
    } else {
      if (OB_LAST_LOG_NOT_COMPLETE == ret || (force && OB_LAST_LOG_RUINNED == ret)) {
        uint64_t file_id = cur_log_file_id_ + 1;
        if (log_file_reader_->if_file_exist(file_id)) {
          SHARE_LOG(WARN, "the log is not the last", K(cur_log_file_id_), K(ret));
        } else {
          if (OB_FAIL(log_file_reader_->revise())) {
            SHARE_LOG(WARN, "revise log failed", K(cur_log_file_id_), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObLogReader::reset_file_id(const uint64_t log_id_start, const uint64_t log_seq_start)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "ObLogReader has not been initialized", K(ret));
  } else if (OB_ISNULL(log_file_reader_)) {
    SHARE_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
    ret = OB_ERROR;
  } else {
    if (log_file_reader_->is_opened()) {
      SHARE_LOG(INFO, "reset log file to", K(log_id_start), K(log_seq_start));
      if (OB_FAIL(log_file_reader_->close())) {
        SHARE_LOG(ERROR, "log file reader close error. ret =%d", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(open_log_(log_id_start))) {
        SHARE_LOG(ERROR, "fail to open log file.", K(cur_log_file_id_), K(ret));
      } else {
        cur_log_file_id_ = log_id_start;
        SHARE_LOG(INFO, "lc: cur_log_file_id_", K(cur_log_file_id_));
      }
    }
    if (OB_SUCC(ret)) {
      if (0 != log_seq_start) {
        ret = seek(log_seq_start);
      }
      if (OB_FAIL(ret)) {
        SHARE_LOG(ERROR, "fail to seek seq", K(ret));
      }
    }
  }
  return ret;
}

int ObLogReader::seek(uint64_t log_seq)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "ObLogReader has not been initialized", K(ret));
  } else if (OB_ISNULL(log_file_reader_)) {
    SHARE_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
    ret = OB_ERROR;
  } else {
    LogCommand cmd;
    uint64_t seq = 0;
    char *log_data = NULL;
    int64_t data_len = 0;

    if (0 == log_seq) {
      ret = OB_SUCCESS;
    } else {
      ret = read_log(cmd, seq, log_data, data_len);
      if (OB_READ_NOTHING == ret) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        SHARE_LOG(WARN, "seek failed", K(log_seq), K(ret));
      } else if (seq - 1 == log_seq) { // log_seq to seek is in the previous log file
        if (OB_FAIL(log_file_reader_->close())) {
          SHARE_LOG(ERROR, "log file reader close error", K(ret));
        } else if (OB_FAIL(open_log_(cur_log_file_id_))) {
          SHARE_LOG(ERROR, "open log error", K(cur_log_file_id_), K(ret));
        } else {}
      } else if (seq >= log_seq) {
        SHARE_LOG(WARN, "seek failed, the initial seq is bigger than log_seq, ",
                        K(seq), K(log_seq));
        ret = OB_ERROR;
      } else {
        while (OB_SUCC(ret) && seq < log_seq) {
          if (OB_FAIL(read_log(cmd, seq, log_data, data_len))) {
            SHARE_LOG(ERROR, "read_log failed", K(seq), K(ret));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObLogReader::open_log_(const uint64_t log_file_id,
                           const uint64_t last_log_seq/* = 0*/)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "ObLogReader has not been initialized", K(ret));
  } else if (OB_ISNULL(log_file_reader_)) {
    SHARE_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
    ret = OB_ERROR;
  } else if (is_wait_ && has_max_ && log_file_id > max_log_file_id_) {
    ret = OB_READ_NOTHING;
  } else {
    ret = log_file_reader_->open(log_file_id, last_log_seq);
    if (is_wait_) {
      if (OB_FILE_NOT_EXIST == ret) {
        SHARE_LOG(DEBUG, "log file doesnot exist", K(log_file_id));
        ret = OB_READ_NOTHING;
      } else if (OB_FAIL(ret)) {
        SHARE_LOG(WARN, "log_file_reader_ open error", K(cur_log_file_id_), K(ret));
      }
    } else {
      if (OB_FAIL(ret)) {
        SHARE_LOG(WARN, "log_file_reader_ open error", K(cur_log_file_id_), K(ret));
        if (OB_FILE_NOT_EXIST == ret) {
          ret = OB_READ_NOTHING;
        }
      }
    }
  }

  return ret;
}

int ObLogReader::read_log_(LogCommand &cmd,
                           uint64_t &log_seq,
                           char *&log_data,
                           int64_t &data_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "ObLogReader has not been initialized", K(ret));
  } else if (OB_ISNULL(log_file_reader_)) {
    SHARE_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
    ret = OB_ERROR;
  } else {
    if (OB_FAIL(log_file_reader_->read_log(cmd, log_seq, log_data, data_len))
        && OB_READ_NOTHING != ret) {
      SHARE_LOG(WARN, "log_file_reader_ read_log error", K(ret));
    }
  }

  return ret;
}
