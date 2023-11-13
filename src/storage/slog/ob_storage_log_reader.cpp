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

#include "common/log/ob_log_constants.h"
#include "common/log/ob_log_generator.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_force_print_log.h"
#include "share/ob_force_print_log.h"
#include "share/redolog/ob_log_file_handler.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/slog/ob_storage_log_reader.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;

namespace storage
{

ObStorageLogReader::ObStorageLogReader()
  : is_inited_(false), cursor_(),
    pread_pos_(0), file_handler_(),
    batch_num_(0), batch_index_(0)
{
  log_buffer_.reset();
  cursor_.reset();
}

ObStorageLogReader::~ObStorageLogReader()
{
  destroy();
}

int ObStorageLogReader::init(
    const char *log_dir,
    const ObLogCursor cursor,
    const blocksstable::ObLogFileSpec &log_file_spec,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "Init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == log_dir || !cursor.is_valid() ||
      !log_file_spec.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), K(log_dir), K(cursor), K(log_file_spec));
  } else {
    if (nullptr != log_buffer_.get_data()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_REDO_LOG(WARN, "Log buffer's data has been set",
          K(ret), KP(log_buffer_.get_data()));
    } else {
      const ObMemAttr attr(tenant_id, ObModIds::OB_LOG_READER);
      char *buf = static_cast<char *>(ob_malloc_align(OB_DIRECT_IO_ALIGN,
          ObLogConstants::LOG_ITEM_MAX_LENGTH, attr));
      if (nullptr == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_REDO_LOG(WARN, "Fail to allocate memory for log_buffer_", K(ret));
      } else if (!log_buffer_.set_data(buf, ObLogConstants::LOG_ITEM_MAX_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(WARN, "Fail to set data", K(ret), KP(buf));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(file_handler_.init(log_dir, LOG_FILE_MAX_SIZE, tenant_id))) {
      STORAGE_REDO_LOG(WARN, "Fail to create file handler.", K(ret));
    } else {
      cursor_ = cursor;
      pread_pos_ = cursor.offset_;
      is_inited_ = true;
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

void ObStorageLogReader::destroy()
{
  is_inited_ = false;
  cursor_.reset();
  pread_pos_ = 0;
  batch_index_ = 0;
  batch_num_ = 0;

  if (nullptr != log_buffer_.get_data()) {
    ob_free_align(log_buffer_.get_data());
    log_buffer_.reset();
  }
  file_handler_.destroy();
}

int ObStorageLogReader::read_log(
    ObStorageLogEntry &entry,
    char *&log_data,
    ObMetaDiskAddr &disk_addr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else {
    if (!file_handler_.is_opened()) {
      if (OB_FAIL(open()) && OB_READ_NOTHING != ret) {
        STORAGE_REDO_LOG(WARN, "Fail to open", K(ret), K(cursor_.file_id_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    do {
      if (OB_FAIL(get_next_log(entry, log_data, disk_addr)) && OB_READ_NOTHING != ret) {
        STORAGE_REDO_LOG(WARN, "Fail to get next log", K(ret));
      } else if (OB_READ_NOTHING == ret && OB_FAIL(check_switch_file())) {
        if (OB_READ_NOTHING != ret && OB_EAGAIN != ret) {
          STORAGE_REDO_LOG(WARN, "Fail to check switch file", K(ret), K(cursor_.file_id_));
        }
      }
    } while (OB_EAGAIN == ret);
  }

  return ret;
}

int ObStorageLogReader::get_finish_cursor(common::ObLogCursor &cursor) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else {
    cursor = cursor_;
  }
  return ret;
}

int ObStorageLogReader::check_switch_file()
{
  int ret = OB_SUCCESS;
  STORAGE_REDO_LOG(INFO, "reach the end of log", K(cursor_.file_id_));

  if (OB_FAIL(close())) {
    STORAGE_REDO_LOG(ERROR, "Fail to close file", K(ret));
  } else {
    // Even if the next file doesn't exist, we still make file id plus 1 and reset offset,
    // because writer needs to create new file and write slog according to this file id and offset
    cursor_.file_id_++;
    cursor_.offset_ = 0;
    if (OB_FAIL(open())) {
      if (OB_READ_NOTHING != ret) {
        STORAGE_REDO_LOG(WARN, "Fail to open next log file",
            K(cursor_.file_id_), K(ret));
      }
    } else {
      ret = OB_EAGAIN;
      pread_pos_ = 0;
      STORAGE_REDO_LOG(INFO, "Read log again", K(cursor_.file_id_), K(log_buffer_));
    }
  }

  return ret;
}

int ObStorageLogReader::open()
{
  int ret = OB_SUCCESS;

  log_buffer_.get_position() = 0;
  log_buffer_.get_limit() = 0;
  bool is_exist = false;

  if (OB_FAIL(file_handler_.exist(cursor_.file_id_, is_exist))) {
    STORAGE_REDO_LOG(WARN, "Fail to check file exist", K(ret), K(cursor_.file_id_));
  } else if (OB_UNLIKELY(!is_exist)) {
    ret = OB_READ_NOTHING;
    STORAGE_REDO_LOG(WARN, "Log file doesn't exist", K(ret), K(cursor_.file_id_));
  } else if (OB_FAIL(file_handler_.open(cursor_.file_id_, ObLogDefinition::LOG_READ_FLAG))) {
    STORAGE_REDO_LOG(WARN, "Fail to open file", K(ret), K(cursor_.file_id_));
  } else {
    STORAGE_REDO_LOG(INFO, "Successfully open slog file", K(cursor_.file_id_));
  }

  return ret;
}

int ObStorageLogReader::close()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(file_handler_.close())) {
    STORAGE_REDO_LOG(WARN, "Fail to close file", K(ret), K(cursor_.file_id_));
  } else {
    STORAGE_REDO_LOG(INFO, "Successfully close file", K(ret), K(cursor_.file_id_));
  }

  return ret;
}

int ObStorageLogReader::get_next_log(
    ObStorageLogEntry &entry,
    char *&log_data,
    ObMetaDiskAddr &disk_addr)
{
  int ret = OB_SUCCESS;
  entry.reset();

  if (batch_index_ == batch_num_) {
    ObStorageLogBatchHeader batch_header;
    if (OB_FAIL(get_next_batch_header(batch_header)) && OB_READ_NOTHING != ret) {
      STORAGE_REDO_LOG(WARN, "Batch header is not complete, ignore last batch", K(ret));
    } else if (OB_READ_NOTHING != ret && log_buffer_.get_remain_data_len() < batch_header.total_len_) {
      if (OB_FAIL(load_buf()) && OB_READ_NOTHING != ret) {
        STORAGE_REDO_LOG(ERROR, "Fail to read log to buffer", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (log_buffer_.get_remain_data_len() < batch_header.total_len_) {
      ret = OB_READ_NOTHING;
      FLOG_INFO("The last slog entry is incomplete", K(ret));
      hex_dump(log_buffer_.get_data(),
          static_cast<int32_t>(log_buffer_.get_limit()),
          true,
          OB_LOG_LEVEL_WARN);
    } else if (OB_FAIL(batch_header.check_data(log_buffer_.get_cur_pos()))) {
      ret = OB_LAST_LOG_RUINNED;
      STORAGE_REDO_LOG(WARN, "Fail to check data integrity.", K(ret), K(batch_header), K(entry),
          K(cursor_), K(batch_index_));
      hex_dump(log_buffer_.get_data(),
          static_cast<int32_t>(log_buffer_.get_limit()),
          true,
          OB_LOG_LEVEL_WARN);
    } else {
      batch_num_ = batch_header.cnt_;
      batch_index_ = 0;
      cursor_.offset_ += batch_header.get_serialize_size();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_entry(entry))) {
      STORAGE_REDO_LOG(WARN, "Log entry is not complete, ignore last log", K(ret));
    } else if (OB_UNLIKELY(log_buffer_.get_remain_data_len() < entry.data_len_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_REDO_LOG(WARN, "Length of this log is larger than remain length", K(ret));
    } else if (OB_FAIL(entry.check_data_integrity(log_buffer_.get_cur_pos()))) {
      ret = OB_LAST_LOG_RUINNED;
      STORAGE_REDO_LOG(WARN, "Fail to check data integrity.", K(ret), K(cursor_));
    } else if (OB_FAIL(check_and_update_seq_number(entry))) {
      STORAGE_REDO_LOG(WARN, "Fail to check and update sequence number", K(entry));
    }
  }

  if (OB_SUCC(ret)) {
    batch_index_++;
    log_data = log_buffer_.get_data() + log_buffer_.get_position();
    if (OB_FAIL(disk_addr.set_file_addr(cursor_.file_id_, cursor_.offset_, entry.data_len_ + entry.get_serialize_size()))) {
      STORAGE_REDO_LOG(WARN, "Fail to set file address", K(ret), K(cursor_));
    } else {
      log_buffer_.get_position() += entry.data_len_;
      cursor_.offset_ += entry.get_serialize_size() + entry.data_len_;
      FLOG_INFO("successfully get next log", K(entry), K(cursor_));
    }
  }

  return ret;
}

int ObStorageLogReader::get_next_batch_header(ObStorageLogBatchHeader &batch_header)
{
  int ret = OB_SUCCESS;

  if (log_buffer_.get_remain_data_len() < batch_header.get_serialize_size()) {
    if (OB_FAIL(load_buf()) && OB_READ_NOTHING != ret) {
      STORAGE_REDO_LOG(WARN, "Fail to read log to buffer", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(log_buffer_.get_remain_data_len() < batch_header.get_serialize_size())) {
      ret = OB_READ_NOTHING;
      STORAGE_REDO_LOG(WARN, "Reach the end of slog file and the last slog is incomplete.", K(ret));
    } else if (OB_FAIL(batch_header.deserialize(log_buffer_.get_data(),
        log_buffer_.get_limit(), log_buffer_.get_position()))) {
      ret = OB_LAST_LOG_RUINNED;
      STORAGE_REDO_LOG(WARN, "Fail to deserialize log entry", K(ret));
    } else if (OB_FAIL(batch_header.check_batch_header())) {
      ret = OB_LAST_LOG_RUINNED;
      STORAGE_REDO_LOG(WARN, "Fail to check batch header's integrity", K(ret), K_(log_buffer));
    }
  }

  return ret;
}

int ObStorageLogReader::get_next_entry(ObStorageLogEntry &entry)
{
  int ret = OB_SUCCESS;

  if (log_buffer_.get_remain_data_len() < entry.get_serialize_size()) {
    if (OB_FAIL(load_buf()) && OB_READ_NOTHING != ret) {
      STORAGE_REDO_LOG(WARN, "Fail to read log to buffer", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(log_buffer_.get_remain_data_len() < entry.get_serialize_size())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_REDO_LOG(WARN, "Length of log entry is larger than remain length", K(ret));
    } else if (OB_FAIL(entry.deserialize(log_buffer_.get_data(),
        log_buffer_.get_limit(), log_buffer_.get_position()))) {
      ret = OB_LAST_LOG_RUINNED;
      STORAGE_REDO_LOG(WARN, "log entry deserialize error", K(ret));
    } else if (OB_FAIL(entry.check_entry_integrity())) {
      ret = OB_LAST_LOG_RUINNED;
      STORAGE_REDO_LOG(WARN, "Fail to check log entry's header integrity", K(ret) ,K_(log_buffer));
    }
  }

  return ret;
}

int ObStorageLogReader::check_and_update_seq_number(const ObStorageLogEntry &entry)
{
  int ret = OB_SUCCESS;
  ObRedoLogMainType main_type = ObRedoLogMainType::OB_REDO_LOG_MAX;
  enum ObRedoLogSubType sub_type;
  ObIRedoModule::parse_cmd(entry.cmd_, main_type, sub_type);

  if (0 == cursor_.log_id_ || ObRedoLogMainType::OB_REDO_LOG_SYS == main_type) {
    STORAGE_REDO_LOG(INFO, "Skip check and update seq number", K(cursor_.log_id_), K(entry));
  } else if (OB_UNLIKELY(entry.seq_ != cursor_.log_id_)) {
    ret = OB_ERROR;
    STORAGE_REDO_LOG(WARN, "Log sequence is not continuous", K(ret), K(cursor_.log_id_), K(entry));
  } else {
    ++cursor_.log_id_;
  }

  return ret;
}

int ObStorageLogReader::load_buf()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(log_buffer_.get_remain_data_len() < 0 || log_buffer_.get_remain_data_len() > pread_pos_)) {
    ret = OB_LOG_NOT_ALIGN;
    STORAGE_REDO_LOG(WARN, "Buf or read pos are not aligned", K(ret), K_(log_buffer), K_(pread_pos));
  } else if (log_buffer_.get_remain_data_len() == log_buffer_.get_capacity()) {
    STORAGE_REDO_LOG(WARN, "Buf has not been consumed", K(ret), K_(log_buffer), K_(pread_pos));
  } else {
    int64_t read_size = 0;
    const int64_t remain_size = log_buffer_.get_remain_data_len();
    const int64_t read_count = log_buffer_.get_capacity() - remain_size;
    MEMMOVE(log_buffer_.get_data(), log_buffer_.get_cur_pos(), remain_size);
    log_buffer_.get_limit() = 0; // set limit as remain_size might be better?
    log_buffer_.get_position() = remain_size;

    if (OB_FAIL(file_handler_.read(log_buffer_.get_cur_pos(), read_count, pread_pos_, read_size))) {
      STORAGE_REDO_LOG(WARN, "Fail to read log file", K(ret), K(cursor_.file_id_), K_(log_buffer),
          K(read_count), K_(pread_pos), K(read_size), K(remain_size));
    } else if (0 == read_size) {
      ret = OB_READ_NOTHING;
    } else {
      log_buffer_.get_limit() += (remain_size + read_size);
      log_buffer_.get_position() = 0;
      pread_pos_ += read_size;
    }
  }

  return ret;
}

int ObStorageLogReader::read_log(
    const char *log_dir,
    const ObMetaDiskAddr &disk_addr,
    const int64_t buf_len,
    void *buf,
    int64_t &pos,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t file_id = 0;
  int64_t offset = 0;
  int64_t size = 0;
  ObStorageLogEntry entry;
  int64_t read_size = 0;
  ObLogFileHandler handler;

  // TODO remove log_file_spec
  blocksstable::ObLogFileSpec log_file_spec;
  log_file_spec.retry_write_policy_ = "normal";
  log_file_spec.log_create_policy_ = "normal";
  log_file_spec.log_write_policy_ = "truncate";

  if (OB_UNLIKELY(nullptr == log_dir || buf_len < disk_addr.size() ||
      !disk_addr.is_file() || nullptr == buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments.", K(ret));
  } else if (OB_FAIL(handler.init(log_dir, 256 << 20/*file_size*/, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to init log file handler.", K(ret));
  } else if (OB_FAIL(disk_addr.get_file_addr(file_id, offset, size))) {
    STORAGE_REDO_LOG(WARN, "Fail to get file address.", K(ret), K(disk_addr));
  } else if (OB_FAIL(open(file_id, handler))) {
    STORAGE_REDO_LOG(WARN, "Fail to open log file.", K(ret), K(file_id));
  } else if (OB_FAIL(handler.read(buf, size, offset, read_size))) {
    STORAGE_REDO_LOG(WARN, "Fail to read data to buffer", K(ret), K(size), K(offset));
  } else if (OB_UNLIKELY(read_size < size)) {
    ret = OB_DATA_OUT_OF_RANGE;
    STORAGE_REDO_LOG(WARN, "Read size is not equal to data size", K(ret), K(disk_addr));
  } else if (OB_FAIL(get_entry(buf, entry))) {
    STORAGE_REDO_LOG(WARN, "Fail to get entry.", K(ret), K(disk_addr));
  } else if (OB_FAIL(handler.close())) {
    STORAGE_REDO_LOG(WARN, "Fail to close file", K(ret), K(disk_addr));
  } else {
    pos = entry.get_serialize_size();
    STORAGE_REDO_LOG(TRACE, "Successfully read targeted log", K(ret), K(disk_addr), K(buf_len));
  }

  return ret;
}

int ObStorageLogReader::open(const int64_t file_id, ObLogFileHandler &handler)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_FAIL(handler.exist(file_id, is_exist))) {
    STORAGE_REDO_LOG(WARN, "Fail to check file exist", K(ret), K(file_id));
  } else if (OB_UNLIKELY(!is_exist)) {
    ret = OB_READ_NOTHING;
    STORAGE_REDO_LOG(WARN, "Log file doesn't exist", K(ret), K(file_id));
  } else if (OB_FAIL(handler.open(file_id, ObLogDefinition::LOG_READ_FLAG))) {
    STORAGE_REDO_LOG(WARN, "Fail to open file", K(ret), K(file_id));
  }
  return ret;
}

int ObStorageLogReader::get_entry(void *buf, ObStorageLogEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t entry_pos = 0;

  if (OB_FAIL(entry.deserialize(reinterpret_cast<char *>(buf), entry.get_serialize_size(), entry_pos))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "Fail to deserialize log entry", K(ret), K(buf));
  } else if (OB_FAIL(entry.check_entry_integrity())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "Entry header is ruined", K(ret), K(buf), K(entry));
  } else if (OB_FAIL(entry.check_data_integrity(reinterpret_cast<char *>(buf) + entry.get_serialize_size()))) {
    STORAGE_REDO_LOG(WARN, "Log is ruined.", K(ret));
  }

  return ret;
}

}
}
