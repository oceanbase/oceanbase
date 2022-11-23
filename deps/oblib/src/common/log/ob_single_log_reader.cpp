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

#include "common/log/ob_single_log_reader.h"
#include "lib/alloc/alloc_assist.h"
#include "common/log/ob_log_dir_scanner.h"
#include "common/log/ob_log_generator.h"

using namespace oceanbase::common;

const int64_t ObSingleLogReader::LOG_BUFFER_MAX_LENGTH = 1 << 21;

ObSingleLogReader::ObSingleLogReader()
{
  is_inited_ = false;
  file_id_ = 0;
  last_log_seq_ = 0;
  log_buffer_.reset();
  file_name_[0] = '\0';
  pos_ = 0;
  pread_pos_ = 0;
  dio_ = true;
}

ObSingleLogReader::~ObSingleLogReader()
{
  if (NULL != log_buffer_.get_data()) {
    ob_free(log_buffer_.get_data());
    log_buffer_.reset();
  }
}

int ObSingleLogReader::init(const char *log_dir)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(ERROR, "ObSingleLogReader init twice", K(ret));
  } else if (OB_UNLIKELY(NULL == log_dir)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "ObSingleLogReader init error, invalid argument", K(ret), KCSTRING(log_dir));
  } else {
    int32_t log_dir_len = static_cast<int32_t>(strlen(log_dir));
    if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH) {
      SHARE_LOG(ERROR, "log_dir is too long", KCSTRING(log_dir), K(log_dir_len));
      ret = OB_INVALID_ARGUMENT;
    } else {
      STRNCPY(log_dir_, log_dir, log_dir_len + 1);
    }
  }
  if (OB_SUCC(ret)) {
    if (NULL == log_buffer_.get_data()) {
      char *buf = static_cast<char *>(ob_malloc(LOG_BUFFER_MAX_LENGTH, ObModIds::OB_LOG_READER));
      if (OB_ISNULL(buf)) {
        ret = OB_ERROR;
        SHARE_LOG(ERROR, "ob_malloc for log_buffer_ failed", K(ret));
      } else if (!log_buffer_.set_data(buf, LOG_BUFFER_MAX_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(ERROR, "set data error", K(ret), KP(buf));
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret)) {
    SHARE_LOG(INFO, "ObSingleLogReader init successfully");
    last_log_seq_ = 0;

    is_inited_ = true;
  }
  return ret;
}

int ObSingleLogReader::get_max_log_file_id(uint64_t &max_log_file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "single log reader not init", K(ret));
  } else {
    ObLogDirScanner scanner;
    if (OB_FAIL(scanner.init(log_dir_))) {
      SHARE_LOG(WARN, "fail to init scanner", K(ret));
    } else {
      ret = scanner.get_max_log_id(max_log_file_id);
    }
  }
  return ret;
}

// 0 is valid file id, so as last_log_seq
int ObSingleLogReader::open(const uint64_t file_id, const uint64_t last_log_seq/* = 0*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "ObSingleLogReader not init", K(ret));
  } else {
    int err = snprintf(file_name_, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", log_dir_, file_id);
    if (OB_UNLIKELY(err < 0)) {
      ret = OB_ERROR;
      SHARE_LOG(ERROR, "snprintf file name error", K(ret), K(err), KCSTRING(log_dir_), K(file_id),
                KERRNOMSG(errno));
    } else if (err >= OB_MAX_FILE_NAME_LENGTH) {
      ret = OB_ERROR;
      SHARE_LOG(ERROR, "snprintf file_name error", K(ret), K(file_id), KERRNOMSG(errno));
    } else {
      int32_t fn_len = static_cast<int32_t>(strlen(file_name_));
      file_id_ = file_id;
      last_log_seq_ = last_log_seq;
      pos_ = 0;
      pread_pos_ = 0;
      log_buffer_.get_position() = 0;
      log_buffer_.get_limit() = 0;
      if (OB_SUCC(file_.open(ObString(fn_len, fn_len, file_name_), dio_))) {
        SHARE_LOG(INFO, "open log file success", KCSTRING(file_name_), K(file_id));
      } else if (OB_FILE_NOT_EXIST == ret) {
        SHARE_LOG(DEBUG, "log file not found", K(ret), KCSTRING(file_name_), K(file_id));
      } else {
        SHARE_LOG(WARN, "open file error", K(ret), KCSTRING(file_name_), KERRNOMSG(errno));
      }
    }
  }
  return ret;
}

int ObSingleLogReader::open_with_lucky(const uint64_t file_id, const uint64_t last_log_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    SHARE_LOG(ERROR, "single log reader not init");
    ret = OB_NOT_INIT;
  } else {
    if (file_id == file_id_) {
      if (last_log_seq != 0 && last_log_seq_ != 0 && last_log_seq != last_log_seq_) {
        ret = OB_DISCONTINUOUS_LOG;
        SHARE_LOG(ERROR, "open with lucky error", K(ret), K(last_log_seq_), K(last_log_seq), K(file_id));
      }
    } else if (OB_FAIL(close())) {
      SHARE_LOG(ERROR, "close error", K(ret));
    } else if (OB_FAIL(open(file_id, last_log_seq))) {
      if (OB_FILE_NOT_EXIST == ret) {
        SHARE_LOG(WARN, "open error", K(ret), K(file_id), K(last_log_seq));
      } else {
        SHARE_LOG(WARN, "open error", K(ret), K(file_id), K(last_log_seq));
      }
    }
  }
  return  ret;
}

int ObSingleLogReader::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    SHARE_LOG(ERROR, "single log reader not init");
    ret = OB_NOT_INIT;
  } else {
    file_.close();
    if (last_log_seq_ == 0) {
      SHARE_LOG(INFO, "close file, read no data from this log", KCSTRING(file_name_));
    } else {
      SHARE_LOG(INFO, "close file successfully", KCSTRING(file_name_), K(last_log_seq_));
    }
  }
  return ret;
}

int ObSingleLogReader::reset()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    SHARE_LOG(ERROR, "single log reader not init");
    ret = OB_NOT_INIT;
  } else {
    if (OB_SUCC(close())) {
      ob_free(log_buffer_.get_data());
      log_buffer_.reset();
      is_inited_ = false;
    }
  }
  return ret;
}

int ObSingleLogReader::revise()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    SHARE_LOG(ERROR, "single log reader not init");
    ret = OB_NOT_INIT;
  } else {
    file_.revise(pos_);
  }
  return ret;
}

bool ObSingleLogReader::if_file_exist(const uint64_t file_id)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  char file_name[OB_MAX_FILE_NAME_LENGTH];
  file_name[0] = '\0';
  int err = snprintf(file_name, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", log_dir_, file_id);
  if (OB_UNLIKELY(err < 0)) {
    ret = OB_ERROR;
    SHARE_LOG(ERROR, "snprintf file name error", K(ret), K(err), KCSTRING(log_dir_), K(file_id),
              "error", strerror(errno));
  } else if (err >= OB_MAX_FILE_NAME_LENGTH) {
    ret = OB_ERROR;
    SHARE_LOG(ERROR, "snprintf file_name error", K(ret), K(file_id), "error", strerror(errno));
  } else {
    if (0 == access(file_name, F_OK)) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObSingleLogReader::read_header(ObLogEntry &entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    SHARE_LOG(ERROR, "single log reader not init");
    ret = OB_NOT_INIT;
  } else {
    if (log_buffer_.get_remain_data_len() < entry.get_serialize_size()) {
      if (OB_FAIL(read_log_()) && OB_READ_NOTHING != ret) {
        SHARE_LOG(ERROR, "read_log_ error", K(ret));
      }
    }
    if (OB_SUCCESS != ret) {
      // do nothing
    } else if (log_buffer_.get_remain_data_len() < entry.get_serialize_size()) {
      if (log_buffer_.get_remain_data_len() == 0) {
        ret = OB_READ_NOTHING;
        SHARE_LOG(INFO, "reach the end of log", K(ret));
      } else {
        ret = OB_LAST_LOG_NOT_COMPLETE;
        SHARE_LOG(ERROR, "last log not complete", K(ret),
            "remain_data_len", log_buffer_.get_remain_data_len(),
            "entry_size", entry.get_serialize_size());
      }
    } else if (ObLogGenerator::is_eof(log_buffer_.get_data() + log_buffer_.get_position(),
                                      log_buffer_.get_limit() - log_buffer_.get_position())) {
      ret = OB_READ_NOTHING;
      pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
      log_buffer_.get_limit() = log_buffer_.get_position();
    } else if (OB_FAIL(entry.deserialize(log_buffer_.get_data(), log_buffer_.get_limit(),
                                         log_buffer_.get_position()))) {
      ret = OB_LAST_LOG_RUINNED;
      SHARE_LOG(ERROR, "log entry deserialize error", K(ret));
    } else if (OB_FAIL(entry.check_header_integrity())) {
      // overwrite ret on purpose
      ret = OB_LAST_LOG_RUINNED;
      SHARE_LOG(ERROR, "log entry check_header_integrity failed", K(ret));
    }
  }
  return ret;
}

int ObSingleLogReader::read_log_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "single log reader not init", K(ret));
  } else {
    if (log_buffer_.get_remain_data_len() > 0) {
      MEMMOVE(log_buffer_.get_data(), log_buffer_.get_data() + log_buffer_.get_position(),
              log_buffer_.get_remain_data_len());
      log_buffer_.get_limit() = log_buffer_.get_remain_data_len();
      log_buffer_.get_position() = 0;
    } else {
      log_buffer_.get_limit() = log_buffer_.get_position() = 0;
    }

    int64_t read_size = 0;
    ret = file_.pread(log_buffer_.get_data() + log_buffer_.get_limit(),
                      log_buffer_.get_capacity() - log_buffer_.get_limit(),
                      pread_pos_, read_size);
    SHARE_LOG(DEBUG, "pread", K(ret), K(pread_pos_), K(read_size), "buf_pos", log_buffer_.get_position(),
              "buf_limit", log_buffer_.get_limit());
    if (OB_FAIL(ret)) {
      SHARE_LOG(ERROR, "read log file error", K(ret), K(file_id_));
    } else {
      // comment this log due to too frequent invoke by replay thread
      // SHARE_LOG(DEBUG, "read data from log file", K(ret), K(file_id_), K(log_fd_));
      if (0 == read_size) {
        // comment this log due to too frequent invoke by replay thread
        // SHARE_LOG(DEBUG, "reach end of log file", K(file_id_));
        ret = OB_READ_NOTHING;
      } else {
        log_buffer_.get_limit() += read_size;
        pread_pos_ += read_size;
      }
    }
  }
  return ret;
}

