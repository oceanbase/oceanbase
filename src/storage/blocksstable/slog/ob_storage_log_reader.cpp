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

#define USING_LOG_PREFIX STORAGE_REDO

#include "share/redolog/ob_log_store_factory.h"
#include "share/ob_force_print_log.h"
#include "common/log/ob_log_generator.h"
#include "clog/ob_log_define.h"
#include "storage/blocksstable/slog/ob_storage_log_writer.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "ob_storage_log_reader.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace blocksstable {

ObStorageLogReader::ObStorageLogReader()
    : is_inited_(false), file_id_(0), last_log_seq_(0), pos_(0), pread_pos_(0), file_store_(NULL)
{
  log_buffer_.reset();
}

ObStorageLogReader::~ObStorageLogReader()
{
  if (NULL != log_buffer_.get_data()) {
    ob_free_align(log_buffer_.get_data());
    log_buffer_.reset();
  }
  ObLogStoreFactory::destroy(file_store_);
}

int ObStorageLogReader::init(const char* log_dir, const uint64_t log_file_id_start, const uint64_t log_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(NULL == log_dir)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "invalid argument", K(ret), K(log_dir));
  }

  if (OB_SUCC(ret)) {
    if (NULL == log_buffer_.get_data()) {
      ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_LOG_READER);
      char* buf =
          static_cast<char*>(ob_malloc_align(DIO_READ_ALIGN_SIZE, ObStorageLogWriter::LOG_ITEM_MAX_LENGTH, attr));
      if (OB_ISNULL(buf)) {
        ret = OB_ERROR;
        STORAGE_REDO_LOG(WARN, "ob_malloc for log_buffer_ failed", K(ret));
      } else if (!log_buffer_.set_data(buf, ObStorageLogWriter::LOG_ITEM_MAX_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(WARN, "set data error", K(ret), KP(buf));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == (file_store_ = ObLogStoreFactory::create(
                     log_dir, LOG_FILE_MAX_SIZE, clog::ObLogWritePoolType::SLOG_WRITE_POOL))) {
      ret = OB_INIT_FAIL;
      STORAGE_REDO_LOG(WARN, "create file store failed.", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    file_id_ = log_file_id_start;
    is_inited_ = true;

    // last_log_seq_ will be set in seek
    if (0 != log_seq && OB_FAIL(seek(log_seq))) {
      STORAGE_REDO_LOG(WARN, "seek log seq error", K(log_seq), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    is_inited_ = false;
  }

  return ret;
}

void ObStorageLogReader::reset()
{
  ObLogStoreFactory::destroy(file_store_);
  if (NULL != log_buffer_.get_data()) {
    ob_free_align(log_buffer_.get_data());
    log_buffer_.reset();
  }
  file_id_ = 0;
  last_log_seq_ = 0;
  pos_ = 0;
  pread_pos_ = 0;
  is_inited_ = false;
}

int ObStorageLogReader::read_log(LogCommand& cmd, uint64_t& seq, char*& log_data, int64_t& data_len)
{
  int ret = OB_SUCCESS;
  bool is_duplicate_log = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "not init", K(ret));
  } else {
    if (!file_store_->is_opened()) {
      if (OB_FAIL(open()) && OB_READ_NOTHING != ret) {
        STORAGE_REDO_LOG(WARN, "open error", K_(file_id), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      do {
        ret = get_next_log(cmd, seq, log_data, data_len, is_duplicate_log);
        if (OB_FAIL(check_switch_file(ret, cmd))) {
          if (OB_READ_NOTHING != ret && OB_EAGAIN != ret) {
            STORAGE_REDO_LOG(WARN, "check switch file fail", K(ret), K(cmd), K(seq), K(file_id_));
          }
        } else if (is_duplicate_log) {
          ret = OB_EAGAIN;
        }
      } while (OB_EAGAIN == ret);
    }
  }
  return ret;
}

int ObStorageLogReader::revise_log(const bool force)
{
  int ret = OB_SUCCESS;
  bool is_duplicate_log = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "not init", K(ret));
  } else {
    LogCommand cmd;
    uint64_t seq = 0;
    char* log_data = NULL;
    int64_t data_len = 0;
    if (!file_store_->is_opened()) {
      ret = open();
    }
    while (OB_SUCC(ret)) {
      ret = get_next_log(cmd, seq, log_data, data_len, is_duplicate_log);
      if (OB_FAIL(check_switch_file(ret, cmd))) {
        if (OB_READ_NOTHING == ret) {
          // iterator all slog, do nothing
        } else if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_REDO_LOG(WARN, "check switch file fail", K(ret), K(cmd), K(seq), K(file_id_));
        }
      }
    }
    if (OB_READ_NOTHING == ret) {
      ret = OB_SUCCESS;
    } else {
      if (OB_LAST_LOG_NOT_COMPLETE == ret || (force && OB_LAST_LOG_RUINNED == ret)) {
        bool is_exist = false;
        uint64_t file_id = file_id_ + 1;
        if (OB_FAIL(file_store_->exist(file_id, is_exist))) {
          STORAGE_REDO_LOG(WARN, "check file exist fail", K(ret), K(file_id));
        } else if (OB_UNLIKELY(is_exist)) {
          STORAGE_REDO_LOG(WARN, "the log is not the last", K_(file_id), K(ret));
        } else if (OB_FAIL(file_store_->ftruncate(file_id_, pos_))) {
          STORAGE_REDO_LOG(WARN, "revise log failed", K_(file_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObStorageLogReader::open()
{
  int ret = OB_SUCCESS;
  pos_ = 0;
  pread_pos_ = 0;
  log_buffer_.get_position() = 0;
  log_buffer_.get_limit() = 0;
  bool is_exist = false;
  if (OB_FAIL(file_store_->exist(file_id_, is_exist))) {
    STORAGE_REDO_LOG(WARN, "check file exist fail", K(ret), K(file_id_));
  } else if (OB_UNLIKELY(!is_exist)) {
    ret = OB_READ_NOTHING;
    STORAGE_REDO_LOG(INFO, "log file not found", K(ret), K_(file_id));
  } else if (OB_FAIL(file_store_->open(file_id_))) {
    STORAGE_REDO_LOG(WARN, "open file error", K(ret), K_(file_id), K(errno));
  } else {
    STORAGE_REDO_LOG(INFO, "open log file success", K_(file_id));
  }
  return ret;
}

int ObStorageLogReader::close()
{
  int ret = file_store_->close();
  if (last_log_seq_ == 0) {
    STORAGE_REDO_LOG(INFO, "close file, read no data from this log", K_(file_id));
  } else {
    STORAGE_REDO_LOG(INFO, "close file successfully", K_(file_id), K(last_log_seq_));
  }
  return ret;
}

int ObStorageLogReader::seek(uint64_t log_seq)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(ERROR, "not init", K(ret));
  } else {
    LogCommand cmd;
    uint64_t seq = 0;
    char* log_data = NULL;
    int64_t data_len = 0;

    if (0 == log_seq) {
      ret = OB_SUCCESS;
    } else {
      ret = read_log(cmd, seq, log_data, data_len);
      if (OB_READ_NOTHING == ret) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        STORAGE_REDO_LOG(WARN, "seek failed", K(log_seq), K(ret));
      } else if (seq - 1 == log_seq) {  // log_seq to seek is in the previous log file
        if (OB_FAIL(close())) {
          STORAGE_REDO_LOG(ERROR, "close error", K(ret));
        } else if (OB_FAIL(open())) {
          STORAGE_REDO_LOG(ERROR, "open log error", K_(file_id), K(ret));
        } else {
          // the first log of this file is exactly the first replay log, need to reset last_log_seq_
          last_log_seq_ = log_seq;
        }
      } else if (seq > log_seq) {
        STORAGE_REDO_LOG(WARN, "seek failed, the initial seq is bigger than log_seq, ", K(seq), K(log_seq));
        ret = OB_ERROR;
      } else {
        // find log until target log_seq
        while (OB_SUCC(ret) && seq < log_seq) {
          if (OB_FAIL(read_log(cmd, seq, log_data, data_len))) {
            STORAGE_REDO_LOG(ERROR, "read_log failed", K(seq), K(ret));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObStorageLogReader::get_next_log(
    LogCommand& cmd, uint64_t& log_seq, char*& log_data, int64_t& data_len, bool& is_duplicate)
{
  int ret = OB_SUCCESS;
  ObLogEntry entry;
  is_duplicate = false;

  if (OB_FAIL(get_next_entry(entry)) && OB_READ_NOTHING != ret) {
    if (OB_LAST_LOG_NOT_COMPLETE != ret) {
      STORAGE_REDO_LOG(ERROR, "read_header failed", K(ret));
    } else {
      STORAGE_REDO_LOG(WARN, "log entry not complete, may need revise", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (log_buffer_.get_remain_data_len() < entry.get_log_data_len()) {
      if (OB_FAIL(load_buf())) {
        STORAGE_REDO_LOG(ERROR, "read log error", K(ret));
      }
    }
    if (log_buffer_.get_remain_data_len() < entry.get_log_data_len()) {
      if (OB_FAIL(tackle_less_data_len(entry))) {
        STORAGE_REDO_LOG(WARN, "tackle_under_data_len fail", K(ret), K(entry));
      }
    } else if (OB_FAIL(entry.check_data_integrity(log_buffer_.get_data() + log_buffer_.get_position()))) {
      ret = OB_LAST_LOG_RUINNED;  // overwrite ret on purpose
      STORAGE_REDO_LOG(ERROR, "check_data_integrity failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_and_update_seq_number(entry, is_duplicate))) {
      STORAGE_REDO_LOG(ERROR, "check and update sequence number fail", K_(last_log_seq), K(entry));
    }
  }

  if (OB_SUCC(ret)) {
    cmd = static_cast<LogCommand>(entry.cmd_);
    log_seq = entry.seq_;
    log_data = log_buffer_.get_data() + log_buffer_.get_position();
    data_len = entry.get_log_data_len();
    log_buffer_.get_position() += data_len;

    STORAGE_REDO_LOG(DEBUG, "LOG ENTRY:", K(entry), K(is_duplicate), "POS", pos_);
    pos_ += entry.header_.header_length_ + entry.header_.data_length_;
  }

  return ret;
}

int ObStorageLogReader::check_and_update_seq_number(const ObLogEntry& entry, bool& is_duplicate)
{
  int ret = OB_SUCCESS;
  is_duplicate = false;
  if (0 == last_log_seq_) {
    // first read, no check
  } else {
    if ((OB_LOG_SWITCH_LOG == entry.cmd_ ? (last_log_seq_ != entry.seq_ && last_log_seq_ + 1 != entry.seq_)
                                         : (last_log_seq_ + 1) != entry.seq_)) {
      ret = OB_ERROR;
      STORAGE_REDO_LOG(ERROR, "the log sequence is not continuous", K(ret), K(last_log_seq_), K(entry));
    }
  }

  if (OB_SUCC(ret) && entry.seq_ > last_log_seq_) {
    last_log_seq_ = entry.seq_;
  }
  return ret;
}

int ObStorageLogReader::tackle_less_header_len(const common::ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  if (log_buffer_.get_remain_data_len() == 0) {
    ret = OB_READ_NOTHING;
    STORAGE_REDO_LOG(INFO, "reach the end of log", K(ret), K(file_id_));
  } else {
    ret = OB_LAST_LOG_NOT_COMPLETE;
    STORAGE_REDO_LOG(ERROR,
        "last log not complete",
        K(ret),
        "remain_data_len",
        log_buffer_.get_remain_data_len(),
        "entry_size",
        entry.get_serialize_size());
  }
  return ret;
}

int ObStorageLogReader::tackle_less_data_len(const common::ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  ret = OB_LAST_LOG_NOT_COMPLETE;
  STORAGE_REDO_LOG(ERROR, "last log not complete", K_(file_id), K_(log_buffer), K(entry));
  hex_dump(log_buffer_.get_data(), static_cast<int32_t>(log_buffer_.get_limit()), true, OB_LOG_LEVEL_WARN);
  return ret;
}

int ObStorageLogReader::get_cursor(common::ObLogCursor& cursor) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "not init", K(ret));
  } else {
    cursor.file_id_ = file_id_;
    cursor.log_id_ = last_log_seq_;
    cursor.offset_ = pos_;
  }
  return ret;
}

int ObStorageLogReader::get_next_cursor(common::ObLogCursor& cursor) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "not init", K(ret));
  } else {
    cursor.file_id_ = file_id_;
    cursor.log_id_ = last_log_seq_ + 1;
    cursor.offset_ = pos_;
  }
  return ret;
}

int ObStorageLogReader::load_buf()
{
  int ret = OB_SUCCESS;
  if ((0 != log_buffer_.get_capacity() % DIO_READ_ALIGN_SIZE) || (log_buffer_.get_remain_data_len() < 0) ||
      (log_buffer_.get_remain_data_len() > pread_pos_)) {  // Defense code
    ret = OB_LOG_NOT_ALIGN;
    STORAGE_REDO_LOG(WARN, "buf or read pos are not aligned", K(ret), K_(log_buffer), K_(pread_pos));
  } else if (0 != pread_pos_ % DIO_READ_ALIGN_SIZE) {
    // pread_pos_ should be 4k aligned because file handler returned read size is always 4k aligned,
    // if pread_pos_ is not aligned, it means file reaches end and file size is not 4k aligned,
    // then we have no need to load buf again
    ret = OB_READ_NOTHING;
    STORAGE_REDO_LOG(
        INFO, "pread_pos_ reaches the end of file, and file size is not 4k aligned", K(ret), K_(pread_pos));
  } else if (log_buffer_.get_remain_data_len() == log_buffer_.get_capacity()) {
    // do nothing if buf hasn't been consumed
    STORAGE_REDO_LOG(WARN, "buf remains same", K(ret), K_(log_buffer), K_(pread_pos));
  } else {
    int64_t read_size = 0;
    const int64_t remain_size = log_buffer_.get_remain_data_len();

    // Move the next log entry to the beginning of the buffer so that need to adjust pread_pos_
    // back to align the DIO read.
    pread_pos_ = lower_align(pread_pos_ - remain_size, DIO_READ_ALIGN_SIZE);
    log_buffer_.get_limit() = 0;
    log_buffer_.get_position() = (0 == remain_size) ? 0 : upper_align(remain_size, DIO_READ_ALIGN_SIZE) - remain_size;

    if (OB_FAIL(file_store_->read(log_buffer_.get_data(), log_buffer_.get_capacity(), pread_pos_, read_size))) {
      STORAGE_REDO_LOG(ERROR,
          "read log file error",
          K(ret),
          K_(file_id),
          K_(log_buffer),
          K_(pread_pos),
          K(read_size),
          K(remain_size));
    } else if (0 == read_size) {
      ret = OB_READ_NOTHING;
    } else {
      log_buffer_.get_limit() += read_size;
      pread_pos_ += read_size;
    }
  }
  return ret;
}

int ObStorageLogReader::check_switch_file(const int get_ret, const LogCommand cmd)
{
  int ret = get_ret;
  // Check switch file when meet below situation:
  // 1) Meet switch log command
  // Otherwise directly ret
  if (common::OB_SUCCESS == get_ret && OB_LOG_SWITCH_LOG == cmd) {
    STORAGE_REDO_LOG(INFO, "reach the end of log", K_(file_id));
    if (OB_FAIL(close())) {
      STORAGE_REDO_LOG(ERROR, "close error", K(ret));
    } else {
      // NOTE: no matter open succeed or failed, file_id_ and pos_ was advanced to next file
      file_id_++;
      if (OB_FAIL(open())) {
        if (OB_READ_NOTHING != ret) {
          STORAGE_REDO_LOG(WARN, "open next log failed", K_(file_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObStorageLogReader::get_next_entry(ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  // ObStorageLogItem is written to file in alignment. The address of ObSerializeRecordHeader
  // should always be aligned. As log_buffer_ size is also aligned, so record header will only
  // occur when remain_data_len is 0. Here, comparing entry serialize size is enough.
  if (log_buffer_.get_remain_data_len() < entry.get_serialize_size()) {
    if (OB_FAIL(load_buf()) && OB_READ_NOTHING != ret) {
      STORAGE_REDO_LOG(ERROR, "read_log_ error", K(ret));
    }
  }
  if (OB_SUCCESS != ret) {
    // do nothing
  } else if (log_buffer_.get_remain_data_len() < entry.get_serialize_size()) {
    if (OB_FAIL(tackle_less_header_len(entry) && OB_READ_NOTHING != ret)) {
      STORAGE_REDO_LOG(WARN, "tackle under header len fail", K(ret));
    }
  } else if (OB_FAIL(entry.deserialize(log_buffer_.get_data(), log_buffer_.get_limit(), log_buffer_.get_position()))) {
    ret = OB_LAST_LOG_RUINNED;
    STORAGE_REDO_LOG(ERROR, "log entry deserialize error", K(ret));
  } else if (OB_FAIL(entry.check_header_integrity())) {
    ret = OB_LAST_LOG_RUINNED;  // overwrite ret on purpose
    STORAGE_REDO_LOG(ERROR, "log entry check_header_integrity failed", K(ret));
  }
  return ret;
}
}  // namespace blocksstable
}  // namespace oceanbase
