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
#include "ob_base_storage_logger.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_running_mode.h"
#include "lib/file/ob_file.h"
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_debug_sync.h"
#include "share/ob_force_print_log.h"
#include "share/redolog/ob_log_store_factory.h"
#include "common/log/ob_log_dir_scanner.h"
#include "storage/ob_partition_log.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/slog/ob_storage_log_reader.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace blocksstable {

RLOCAL(int64_t, ObBaseStorageLogger::thread_trans_id_);

ObBaseStorageLogger::ObBaseStorageLogger() : ObStorageLogReplayer(), log_writer_()
{}

ObBaseStorageLogger::~ObBaseStorageLogger()
{
  destroy();
}

ObBaseStorageLogger& ObBaseStorageLogger::get_instance()
{
  static ObBaseStorageLogger instance_;
  return instance_;
}

int ObBaseStorageLogger::init(const char* log_dir, const int64_t max_log_file_size, ObISLogFilter* filter_before_parse,
    ObISLogFilter* filter_after_parse)
{
  int ret = OB_SUCCESS;
  char* entries = nullptr;
  char* buffers = nullptr;
  const int64_t MAX_CONCURRENT_TRANS_CNT = !lib::is_mini_mode() ? MAX(128, sysconf(_SC_NPROCESSORS_ONLN) * 2) : 64;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has been inited.", K(ret));
  } else if (NULL == log_dir || max_log_file_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments, ", K(ret), KP(log_dir), K(max_log_file_size));
  } else if (OB_FAIL(ObStorageLogReplayer::init(log_dir, filter_before_parse, filter_after_parse))) {
    STORAGE_REDO_LOG(WARN, "fail to init storage log replayer", K(ret));
  } else if (OB_FAIL(log_writer_.init(log_dir, max_log_file_size, NORMAL_LOG_ITEM_SIZE, MAX_CONCURRENT_TRANS_CNT))) {
    STORAGE_REDO_LOG(WARN, "Fail to init log_writer_", K(ret), K(log_dir), K(max_log_file_size));
  } else if (OB_FAIL(active_trans_.create(MAX_CONCURRENT_TRANS_CNT * 10, ObModIds::OB_CS_COMMIT_LOG))) {
    STORAGE_REDO_LOG(WARN, "Fail to create active trans map, ", K(ret));
  } else if (OB_FAIL(trans_entries_.init(MAX_CONCURRENT_TRANS_CNT))) {
    STORAGE_REDO_LOG(WARN, "Fail to init trans_entries, ", K(ret));
  } else if (nullptr == (entries = reinterpret_cast<char*>(
                             allocator_.alloc(sizeof(ObStorageLogActiveTrans) * MAX_CONCURRENT_TRANS_CNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_REDO_LOG(ERROR, "Fail to allocate memory for ActiveTransEntry.", K(ret));
  } else if (nullptr ==
             (buffers = reinterpret_cast<char*>(allocator_.alloc(TRANS_ENTRY_BUF_SIZE * MAX_CONCURRENT_TRANS_CNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_REDO_LOG(ERROR, "Fail to allocate memory for buffers.", K(ret));
  } else {
    ObStorageLogActiveTrans* entry = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_CONCURRENT_TRANS_CNT; ++i) {
      if (nullptr == (entry = new (entries + i * sizeof(ObStorageLogActiveTrans)) ObStorageLogActiveTrans())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(WARN, "Fail to construct ActiveTransEntry.", K(ret));
      } else if (OB_FAIL(entry->assign(buffers + i * TRANS_ENTRY_BUF_SIZE, TRANS_ENTRY_BUF_SIZE))) {
        STORAGE_REDO_LOG(WARN, "Fail to set buff, ", K(ret));
      } else if (OB_FAIL(trans_entries_.push(entry))) {
        STORAGE_REDO_LOG(WARN, "Fail to push entry, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      snprintf(log_dir_, OB_MAX_FILE_NAME_LENGTH, "%s", log_dir);
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    trans_id_ = 0;
    min_log_file_id_ = 1;
    log_writer_.destroy();
    active_trans_.destroy();
    trans_entries_.destroy();
    allocator_.reset();
    MEMSET(log_dir_, 0, sizeof(log_dir_));
    MEMSET(redo_modules_, 0, sizeof(redo_modules_));
  }

  return ret;
}

void ObBaseStorageLogger::destroy()
{
  if (is_inited_) {
    trans_id_ = 0;
    min_log_file_id_ = 1;
    log_writer_.destroy();
    OB_SLOG_DISK_MGR.destroy();
    active_trans_.destroy();
    trans_entries_.destroy();
    allocator_.reset();
    MEMSET(log_dir_, 0, sizeof(log_dir_));
    MEMSET(redo_modules_, 0, sizeof(redo_modules_));
    is_inited_ = false;
  }
}

int ObBaseStorageLogger::begin(enum LogCommand cmd)
{
  int ret = OB_SUCCESS;
  ObStorageLogActiveTrans* trans_entry = NULL;
  int64_t trans_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else if (OB_FAIL(trans_entries_.pop(trans_entry))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SLOG_REACH_MAX_CONCURRENCY;
      STORAGE_REDO_LOG(WARN, "Active transaction log entry reach maximum", K(ret), K(trans_entries_.capacity()));
    } else {
      STORAGE_REDO_LOG(WARN, "Fail to pop trans_entry, ", K(ret));
    }
  } else {
    STORAGE_REDO_LOG(DEBUG, "Begin trans, ", K(cmd));
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_SYS, OB_REDO_LOG_BEGIN);
    trans_entry->reuse();
    trans_id = ATOMIC_AAF(&trans_id_, 1);

    if (0 != thread_trans_id_) {
      int tmp_ret = OB_SUCCESS;
      const int64_t thread_trans_id = thread_trans_id_;
      STORAGE_REDO_LOG(ERROR, "some slog is not finish, clear it", K(thread_trans_id));
      if (OB_SUCCESS != (tmp_ret = erase_trans(thread_trans_id_))) {
        STORAGE_REDO_LOG(WARN, "Fail to erase trans, ", K(thread_trans_id), K(tmp_ret));
      }
      thread_trans_id_ = 0;
    }

    if (OB_SUCC(ret)) {
      storage::ObBeginTransLogEntry entry;
      const ObStorageLogAttribute log_attr(OB_SYS_TENANT_ID, OB_VIRTUAL_DATA_FILE_ID);
      if (OB_FAIL(trans_entry->append_log(trans_id, subcmd, log_attr, entry))) {
        STORAGE_REDO_LOG(WARN, "Fail to append log, ", K(trans_id), K(ret));
      } else {
        trans_entry->cmd_ = cmd;
        if (OB_FAIL(active_trans_.set_refactored(trans_id, trans_entry))) {
          STORAGE_REDO_LOG(WARN, "Fail to set active trans, ", K(trans_id));
        } else {
          STORAGE_REDO_LOG(DEBUG, "Success set active trans, ", K(trans_id));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // don't reset ret
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans_entries_.push(trans_entry))) {
        STORAGE_REDO_LOG(ERROR, "Fail to push trans_entry.", K(tmp_ret));
      }
    } else {
      thread_trans_id_ = trans_id;
    }
  }

  return ret;
}

int ObBaseStorageLogger::write_log(
    const int64_t subcmd, const ObStorageLogAttribute& log_attr, ObIBaseStorageLogEntry& data)
{
  int ret = OB_SUCCESS;
  ObStorageLogActiveTrans* trans_entry = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else if (OB_UNLIKELY(subcmd < 0 || !data.is_valid() || !log_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument, ", K(ret), K(subcmd), K(log_attr), K(data));
  } else {
    if (OB_FAIL(active_trans_.get_refactored(thread_trans_id_, trans_entry))) {
      STORAGE_REDO_LOG(ERROR, "There is not the transaction in active_trans, ", K(ret));
    } else if (NULL == trans_entry) {
      ret = OB_ERR_UNEXPECTED;
      const int64_t thread_trans_id = thread_trans_id_;
      STORAGE_REDO_LOG(WARN, "The trans_entry is NULL, ", K(thread_trans_id), K(ret));
    } else if (OB_FAIL(write_log(*trans_entry, subcmd, log_attr, data))) {
      const int64_t thread_trans_id = thread_trans_id_;
      STORAGE_REDO_LOG(WARN, "Fail to write log .", K(ret), K(subcmd), K(thread_trans_id));
    }
  }

  return ret;
}

int ObBaseStorageLogger::commit(int64_t& log_seq_num)
{
  int ret = E(EventTable::EN_COMMIT_SLOG) OB_SUCCESS;
  ObStorageLogActiveTrans* trans_entry = NULL;
  int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_SYS, OB_REDO_LOG_COMMIT);
  const ObStorageLogAttribute log_attr(OB_SYS_TENANT_ID, OB_VIRTUAL_DATA_FILE_ID);

  if (OB_FAIL(ret)) {
    STORAGE_REDO_LOG(WARN, "fail to commit log", K(ret));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else {
    const int64_t thread_trans_id = thread_trans_id_;
    STORAGE_REDO_LOG(DEBUG, "Commit trans", K(thread_trans_id));
    if (OB_FAIL(active_trans_.get_refactored(thread_trans_id_, trans_entry)) || NULL == trans_entry) {
      ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      STORAGE_REDO_LOG(WARN, "Fail to get trans_entry, ", K(thread_trans_id), K(ret));
    } else if (OB_FAIL(write_log(*trans_entry, subcmd, log_attr, trans_entry->valid_record_))) {
      STORAGE_REDO_LOG(WARN, "Fail to write commit log, ", K(thread_trans_id), K(ret));
    } else if (OB_FAIL(flush_log(*trans_entry, &log_seq_num))) {
      STORAGE_REDO_LOG(WARN, "Fail to flush log, ", K(thread_trans_id), K(ret));
    }

    // ignore ret
    if (NULL != trans_entry) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = erase_trans(thread_trans_id_))) {
        STORAGE_REDO_LOG(WARN, "Fail to erase trans, ", K(thread_trans_id), K(tmp_ret));
      }
    }
    thread_trans_id_ = 0;
  }

  return ret;
}

int ObBaseStorageLogger::do_savepoint(int64_t& savepoint)
{
  int ret = OB_SUCCESS;
  ObStorageLogActiveTrans* trans_entry = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else {
    const int64_t thread_trans_id = thread_trans_id_;
    if (OB_FAIL((active_trans_.get_refactored(thread_trans_id_, trans_entry)))) {
      STORAGE_REDO_LOG(WARN, "Fail to get trans_entry, ", K(thread_trans_id), K(ret));
    } else if (OB_FAIL(trans_entry->valid_record_.add_savepoint(trans_entry->log_count_ - 1))) {
      STORAGE_REDO_LOG(WARN, "Fail to write commit log, ", K(thread_trans_id), K(ret));
    } else {
      savepoint = trans_entry->log_count_ - 1;
    }
  }

  return ret;
}

int ObBaseStorageLogger::rollback(int64_t savepoint)
{
  int ret = OB_SUCCESS;
  ObStorageLogActiveTrans* trans_entry = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else if (savepoint < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument, ", K(savepoint), K(ret));
  } else {
    const int64_t thread_trans_id = thread_trans_id_;
    if (OB_FAIL(active_trans_.get_refactored(thread_trans_id_, trans_entry))) {
      STORAGE_REDO_LOG(WARN, "Fail to get trans_entry, ", K(thread_trans_id), K(ret));
    } else if (OB_FAIL(trans_entry->valid_record_.rollback(savepoint))) {
      STORAGE_REDO_LOG(WARN, "Fail to write commit log, ", K(thread_trans_id), K(ret));
    }
  }

  return ret;
}

int ObBaseStorageLogger::abort()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else {
    const int64_t thread_trans_id = thread_trans_id_;
    STORAGE_REDO_LOG(INFO, "Abort trans, ", K(thread_trans_id));
    if (OB_FAIL(erase_trans(thread_trans_id_))) {
      STORAGE_REDO_LOG(WARN, "Fail to erase trans, ", K(thread_trans_id), K(ret));
    }
    thread_trans_id_ = 0;
  }

  return ret;
}

int ObBaseStorageLogger::is_logger_ok(bool& is_ok)
{
  int ret = OB_SUCCESS;
  is_ok = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else {
    is_ok = log_writer_.is_ok();
    if (!is_ok) {
      STORAGE_REDO_LOG(WARN, "storage log writer is not ok");
    }
  }
  return ret;
}

int ObBaseStorageLogger::get_active_cursor(ObLogCursor& log_cursor)
{
  int ret = OB_SUCCESS;
  FindMinLogCursor finder;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else {
    common::DRWLock::WRLockGuard guard(log_sync_lock_);
    finder.log_cursor_ = log_writer_.get_cur_cursor();
    if (OB_FAIL(active_trans_.foreach_refactored(finder))) {
      STORAGE_REDO_LOG(WARN, "Fail to find the earliest log cursor of all active transactions currently, ", K(ret));
    } else {
      log_cursor = finder.log_cursor_;
    }
  }
  return ret;
}

int ObBaseStorageLogger::parse_log(const char* log_dir, const int64_t log_file_id, FILE* stream)
{
  int ret = OB_SUCCESS;
  ObStorageLogReader slog_reader;

  if (NULL == log_dir || NULL == stream || log_file_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument, ", K(log_dir), K(log_file_id), K(stream), K(ret));
  } else if (OB_FAIL(slog_reader.init(log_dir, log_file_id, 0))) {
    STORAGE_REDO_LOG(WARN, "Fail to init log_reader, ", K(ret));
  } else {
    LogCommand cmd;
    uint64_t seq = 0;
    ObBaseStorageLogHeader log_header;
    char* log_data = nullptr;
    int64_t data_len = 0;
    enum ObRedoLogMainType main_type;
    int32_t sub_type = 0;
    ObBaseStorageLogBuffer log_buffer;
    ObBufferReader valid_record_buffer(NULL, 0, 0);
    ObStorageLogValidRecordEntry* record = NULL;

    while (OB_SUCCESS == ret && OB_SUCC(slog_reader.read_log(cmd, seq, log_data, data_len))) {
      if (OB_LOG_SWITCH_LOG != cmd && OB_LOG_NOP != cmd && OB_LOG_CHECKPOINT != cmd) {
        log_buffer.reset();
        if (OB_FAIL(log_buffer.assign(log_data, data_len))) {
          STORAGE_REDO_LOG(WARN, "Fail to assign log_buffer, ", K(ret));
        } else {
          while (OB_SUCCESS == ret && OB_SUCC(log_buffer.read_log(log_header, log_data))) {
            ObIRedoModule::parse_subcmd(log_header.subcmd_, main_type, sub_type);
            if (0 > fprintf(stream,
                        "trans_id:%ld, log_seq:%ld, sub_seq:%ld, command:%d, main_type=%d ",
                        log_header.trans_id_,
                        seq,
                        log_header.log_seq_,
                        cmd,
                        main_type)) {
              ret = OB_IO_ERROR;
              STORAGE_REDO_LOG(WARN, "fprintf fail", K(ret), KP(stream), K(errno), KERRMSG);
            }
            if (OB_REDO_LOG_SYS == main_type) {
              if (OB_REDO_LOG_BEGIN == sub_type) {
                if (0 > fprintf(stream, " trans begin\n")) {
                  ret = OB_IO_ERROR;
                  STORAGE_REDO_LOG(WARN, "fprintf fail", K(ret), KP(stream), K(errno), KERRMSG);
                }
              } else if (OB_REDO_LOG_COMMIT == sub_type) {
                if (OB_FAIL(ObIRedoModule::parse_valid_record(log_data, log_header.log_len_, allocator_, record))) {
                  STORAGE_REDO_LOG(WARN, "parse valid record from commit entry failed.", K(ret), K(log_header));
                } else {
                  if (0 > fprintf(stream, " trans commit\n")) {
                    ret = OB_IO_ERROR;
                    STORAGE_REDO_LOG(WARN, "fprintf fail", K(ret), KP(stream), K(errno), KERRMSG);
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_REDO_LOG(ERROR, "Unexpected sub type, ", K(sub_type), K(ret));
              }
            } else {
              if (NULL == redo_modules_[main_type]) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_REDO_LOG(ERROR, "The redo module has not been registered, ", "redo_module", main_type, K(ret));
              } else if (OB_FAIL(redo_modules_[main_type]->parse(
                             log_header.subcmd_, log_data, log_header.log_len_, stream))) {
                STORAGE_REDO_LOG(ERROR, "Fail to parse sub cmd, ", K(ret), K(main_type), K(seq), K(log_header));
              }
            }
          }

          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }

    if (OB_READ_NOTHING == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObBaseStorageLogger::write_log(ObStorageLogActiveTrans& trans_entry, const int64_t subcmd,
    const ObStorageLogAttribute& log_attr, ObIBaseStorageLogEntry& data)
{
  int ret = OB_SUCCESS;

  if (!trans_entry.is_valid() || !data.is_valid() || subcmd < 0 || !log_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument, ", K(ret), K(trans_entry), K(subcmd), K(log_attr), K(data));
  } else if (OB_FAIL(trans_entry.append_log(thread_trans_id_, subcmd, log_attr, data))) {
    const int64_t thread_trans_id = thread_trans_id_;
    if (OB_BUF_NOT_ENOUGH == ret) {
      if (OB_FAIL(flush_log(trans_entry))) {
        STORAGE_REDO_LOG(WARN, "Fail to flush log, ", K(thread_trans_id), K(ret));
      } else if (OB_FAIL(trans_entry.append_log(thread_trans_id_, subcmd, log_attr, data))) {
        STORAGE_REDO_LOG(WARN, "Fail to append log, ", K(thread_trans_id), K(ret));
      }
    } else if (OB_SIZE_OVERFLOW == ret) {
      ObBaseStorageLogHeader log_header;
      // log_header.get_serialize_size() underlying encode length is variant according to the value
      // of integer. Here log_header hasn't been assigned so serialization length is not determined.
      // To ensure enough buffer length, padding the size of the class.
      const int64_t new_size =
          data.get_serialize_size() + log_header.get_serialize_size() + sizeof(ObBaseStorageLogHeader);
      const int64_t old_size = trans_entry.log_buffer_.capacity();
      char* new_buf = nullptr;
      char* old_buf = trans_entry.log_buffer_.data();
      const ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_CS_COMMIT_LOG);

      if (OB_FAIL(flush_log(trans_entry))) {
        STORAGE_REDO_LOG(WARN, "Fail to flush log, ", K(thread_trans_id), K(ret));
      } else if (NULL == (new_buf = reinterpret_cast<char*>(ob_malloc(new_size, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_REDO_LOG(ERROR, "Fail to allocate memory for buf.", K(ret));
      } else if (OB_FAIL(trans_entry.assign(new_buf, new_size))) {
        STORAGE_REDO_LOG(WARN, "Fail to set new buff, ", K(ret));
      } else if (OB_FAIL(trans_entry.append_log(thread_trans_id_, subcmd, log_attr, data))) {
        STORAGE_REDO_LOG(WARN, "Fail to append log, ", K(thread_trans_id), K(ret));
      } else if (OB_FAIL(flush_log(trans_entry))) {
        STORAGE_REDO_LOG(WARN, "Fail to flush log, ", K(thread_trans_id), K(ret));
      }

      if (nullptr != new_buf) {
        ob_free(new_buf);
        new_buf = nullptr;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans_entry.assign(old_buf, old_size))) {
          STORAGE_REDO_LOG(ERROR, "Fail to set back old buff, ", K(tmp_ret));
          ob_abort();
        }
      }
    } else {
      STORAGE_REDO_LOG(WARN, "append log error.", K(ret), K(thread_trans_id), K(subcmd));
    }
  }
  return ret;
}

int ObBaseStorageLogger::flush_log(ObStorageLogActiveTrans& trans_entry, int64_t* log_seq_num)
{
  int ret = OB_SUCCESS;
  ObLogCursor start_cursor;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else {
    const int64_t start_time = ObTimeUtility::fast_current_time();
    common::DRWLock::RDLockGuard guard(log_sync_lock_);
    STORAGE_REDO_LOG(DEBUG, "Begin flush sstable redo log");
    if (OB_FAIL(log_writer_.flush_log(trans_entry.cmd_, trans_entry.log_buffer_, start_cursor))) {
      STORAGE_REDO_LOG(WARN, "flush_log failed", K(ret), K(trans_entry));
    } else {
      // success
      if (!trans_entry.start_cursor_.is_valid()) {
        trans_entry.start_cursor_ = start_cursor;
      }
      if (NULL != log_seq_num) {
        *log_seq_num = start_cursor.log_id_;
      }
      trans_entry.log_buffer_.reuse();
      const int64_t end_time = ObTimeUtility::fast_current_time();
      STORAGE_REDO_LOG(INFO,
          "Success to flush sstable redo log",
          K(start_cursor),
          K(start_time),
          K(end_time),
          K(trans_entry),
          "flush_time",
          end_time - start_time);
    }
  }

  return ret;
}

int ObBaseStorageLogger::replay_over()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else if (OB_FAIL(log_writer_.start_log(finish_cursor_))) {
    STORAGE_REDO_LOG(WARN, "fail to start log", K(ret), K(finish_cursor_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < OB_REDO_LOG_MAX; ++i) {
      if (NULL != redo_modules_[i]) {
        if (OB_FAIL(redo_modules_[i]->enable_write_log())) {
          STORAGE_REDO_LOG(WARN, "Fail to start write commit log. ", "redo_module_idx", i, K(ret));
        }
      }
    }
  }

  return ret;
}

int ObBaseStorageLogger::erase_trans(const int64_t trans_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogActiveTrans* trans_entry = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObBaseStorageLogger has not been inited.", K(ret));
  } else if (trans_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument, ", K(trans_id), K(ret));
  } else if (OB_SUCC(active_trans_.erase_refactored(trans_id, &trans_entry))) {
    trans_entry->reuse();
    if (OB_FAIL(trans_entries_.push(trans_entry))) {
      STORAGE_REDO_LOG(ERROR, "Fail to push trans_entry, ", K(ret));
    } else {
      STORAGE_REDO_LOG(DEBUG, "Success erase trans, ", K(trans_id));
    }
  } else {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_REDO_LOG(WARN, "Unexpected erase trans error, ", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

void ObBaseStorageLogger::remove_useless_log_file(const int64_t end_file_id)
{
  // ignore ret, allow remove fail
  int ret = OB_SUCCESS;
  if (is_inited_) {
    for (; OB_SUCC(ret) && min_log_file_id_ < end_file_id; ++min_log_file_id_) {
      if (OB_FAIL(log_writer_.delete_log_file(min_log_file_id_))) {
        STORAGE_REDO_LOG(ERROR, "Fail to delete log file, ", K(ret), K_(min_log_file_id));
      } else {
        STORAGE_REDO_LOG(INFO, "Success to remove useless log file, ", K(min_log_file_id_));
      }
    }
  }
}

int ObBaseStorageLogger::get_using_disk_space(int64_t& using_space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = log_writer_.get_using_disk_space(using_space);
  }
  return ret;
}
} /* namespace blocksstable */
} /* namespace oceanbase */
