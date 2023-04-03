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

#include "ob_storage_log_replayer.h"
#include "share/ob_force_print_log.h"
#include "share/redolog/ob_log_file_handler.h"
#include "storage/slog/ob_storage_log_entry.h"
#include "storage/slog/ob_storage_log_reader.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{

ObRedoModuleReplayParam::ObRedoModuleReplayParam()
  : cmd_(0), buf_(nullptr), disk_addr_(), data_size_(0)
{
}

bool ObRedoModuleReplayParam::is_valid() const
{
  return nullptr != buf_ &&
         cmd_ >= 0 &&
         disk_addr_.is_valid();
}

ObStorageLogReplayer::ObStorageLogReplayer()
  : is_inited_(false), log_dir_(nullptr), log_file_spec_()
{
  MEMSET(redo_modules_, 0, sizeof(redo_modules_));
}

ObStorageLogReplayer::~ObStorageLogReplayer()
{
  destroy();
}

int ObStorageLogReplayer::init(const char *log_dir, const blocksstable::ObLogFileSpec &log_file_spec)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "Init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == log_dir)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument", K(ret), KP(log_dir));
  } else {
    log_dir_ = log_dir;
    log_file_spec_ = log_file_spec;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObStorageLogReplayer::destroy()
{
  log_dir_ = nullptr;
  MEMSET(redo_modules_, 0, sizeof(redo_modules_));
  is_inited_ = false;
}

int ObStorageLogReplayer::register_redo_module(enum ObRedoLogMainType cmd_prefix, ObIRedoModule *module)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(static_cast<int>(cmd_prefix) >= static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_MAX)
      || nullptr == module)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument", K(ret), K(static_cast<int>(cmd_prefix)));
  } else if (OB_UNLIKELY(nullptr != redo_modules_[static_cast<int>(cmd_prefix)])) {
    ret = OB_ENTRY_EXIST;
    STORAGE_REDO_LOG(WARN, "The cmd_prefix has been registered", K(ret), K(static_cast<int>(cmd_prefix)));
  } else {
    redo_modules_[static_cast<int>(cmd_prefix)] = module;
  }

  return ret;
}

int ObStorageLogReplayer::unregister_redo_module(enum ObRedoLogMainType cmd_prefix)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(static_cast<int>(cmd_prefix) >=
      static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_MAX))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument", K(ret), K(static_cast<int>(cmd_prefix)));
  } else if (nullptr == redo_modules_[static_cast<int>(cmd_prefix)]) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_REDO_LOG(WARN, "The cmd_prefix has not been registered", K(ret), K(static_cast<int>(cmd_prefix)));
  } else {
    redo_modules_[static_cast<int>(cmd_prefix)] = nullptr;
  }

  return ret;
}

int ObStorageLogReplayer::replay(
    const common::ObLogCursor &replay_start_cursor,
    common::ObLogCursor &finish_cursor,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogReader slog_reader;
  int64_t last_entry_seq = -1;
  bool is_empty_dir = false;
  ObStorageLogEntry dummy_entry;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(!replay_start_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument", K(ret), K(replay_start_cursor));
  } else if (OB_FAIL(slog_reader.init(log_dir_, replay_start_cursor, log_file_spec_, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to init slog reader", K(ret), K(log_dir_), K(replay_start_cursor));
  } else if (OB_FAIL(check_empty_dir(is_empty_dir, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to check empty dir", K(ret), K(is_empty_dir));
  } else if (is_empty_dir) {
    ret = OB_SUCCESS;
    finish_cursor.file_id_ = 1;
    finish_cursor.log_id_ = 1;
    finish_cursor.offset_ = 0;
    STORAGE_REDO_LOG(WARN, "There is no redo log", K(replay_start_cursor));
  } else {
    ObStorageLogEntry entry;
    char *log_data = nullptr;
    ObMetaDiskAddr disk_addr;
    enum ObRedoLogMainType main_type = ObRedoLogMainType::OB_REDO_LOG_MAX;
    enum ObRedoLogSubType sub_type;
    int64_t replay_cost[static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_MAX)] = {0};
    int64_t replay_cnt[static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_MAX)] = {0};
    int64_t replay_start_time = 0;

    while(OB_SUCC(ret) && OB_SUCC(slog_reader.read_log(entry, log_data, disk_addr))) {
      ObIRedoModule::parse_cmd(entry.cmd_, main_type, sub_type);
      if (ObRedoLogMainType::OB_REDO_LOG_SYS == main_type) {
        STORAGE_REDO_LOG(INFO, "Skip these kinds of log", K(entry.cmd_));
      } else {
        if (OB_UNLIKELY(nullptr == redo_modules_[static_cast<int>(main_type)])) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_REDO_LOG(WARN, "The module hasn't been registered", K(main_type));
        } else {
          replay_start_time = ObTimeUtility::current_time();
          ObRedoModuleReplayParam replay_param;
          replay_param.cmd_ = entry.cmd_;
          replay_param.buf_ = log_data;
          replay_param.disk_addr_ = disk_addr;
          replay_param.data_size_ = disk_addr.size() - dummy_entry.get_serialize_size();
          if (OB_FAIL(redo_modules_[static_cast<int>(main_type)]->replay(replay_param))) {
            STORAGE_REDO_LOG(WARN, "Fail to replay", K(ret), K(main_type),
                K(sub_type), K(replay_param));
          }
          replay_cost[static_cast<int>(main_type)] += ObTimeUtility::current_time() - replay_start_time;
          replay_cnt[static_cast<int>(main_type)]++;
        }
      }

      if (OB_SUCC(ret)) {
        last_entry_seq = entry.seq_;
      }
    } // while slog_reader.read_log

    if (OB_READ_NOTHING == ret) {
      ret = OB_SUCCESS;
    }
    FLOG_INFO("slog replay stat: ", K(ret),
        "tenant storage log cnt", replay_cnt[static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE)],
        "tenant storage log cost", replay_cost[static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE)],
        "tenant config log cnt", replay_cnt[static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT)],
        "tenant config log cost", replay_cost[static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT)]
        );
  }

  if (OB_SUCC(ret)) {
    if (-1 != last_entry_seq) {
      if (OB_FAIL(slog_reader.get_finish_cursor(finish_cursor))) {
        STORAGE_REDO_LOG(WARN, "Fail to get write_start_cursor", K(ret));
      }
    } else if (!is_empty_dir) {
      // if last_entry_seq is -1,
      // it means we never read one single slog after replay_start_cursor(the checkpoint cursor)
      // this is because this slog file may have reach the end,
      // in this situation we should move forward to next file.
      finish_cursor.file_id_ = replay_start_cursor.file_id_ + 1;
      finish_cursor.log_id_ = replay_start_cursor.log_id_;
      finish_cursor.offset_ = 0;
    }
  }

  return ret;
}

int ObStorageLogReplayer::replay_over()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_MAX); i++) {
    if (nullptr != redo_modules_[i]) {
      if (OB_FAIL(redo_modules_[i]->replay_over())) {
        STORAGE_REDO_LOG(WARN, "Fail to replay over", K(ret));
      }
    }
  }
  return ret;
}

int ObStorageLogReplayer::parse_log(
    const char *log_dir,
    const int64_t log_file_id,
    const blocksstable::ObLogFileSpec &log_file_spec,
    FILE *stream,
    const int64_t offset,
    const int64_t parse_count)
{
  int ret = OB_SUCCESS;
  ObStorageLogReader slog_reader;
  ObLogCursor cursor;
  cursor.file_id_ = log_file_id;
  cursor.log_id_ = 0;
  cursor.offset_ = offset;
  int64_t cnt = parse_count;

  if (OB_ISNULL(log_dir) || OB_ISNULL(stream) || OB_UNLIKELY(log_file_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments.", K(ret), KP(log_dir), K(log_file_id), KP(stream));
  } else if (OB_FAIL(slog_reader.init(log_dir, cursor, log_file_spec, OB_SERVER_TENANT_ID))) {
    STORAGE_REDO_LOG(WARN, "Fail to init log_reader, ", K(ret), KP(log_dir), K(cursor));
  } else {
    ObMetaDiskAddr disk_addr;
    char *log_data = nullptr;
    const ObStorageLogEntry dummy_entry;
    ObStorageLogEntry entry;
    char date[100];

    while (OB_SUCC(ret) && OB_SUCC(slog_reader.read_log(entry, log_data, disk_addr)) && 0 != cnt) {
      cnt--;
      MEMSET(date, 0, sizeof(date));
      ObRedoLogMainType main_type;
      ObRedoLogSubType sub_type;
      ObIRedoModule::parse_cmd(entry.cmd_, main_type, sub_type);
      int64_t time = entry.timestamp_ / 1000000L;
      int64_t ms = entry.timestamp_ % 1000000L / 1000L;
      strftime(date, sizeof(date), "%Y-%m-%d %H:%M:%S", localtime(static_cast<time_t *>(&time)));
      if (OB_UNLIKELY(0 > fprintf(stream, "\nmain_type:%d, sub_type:%d, log_seq:%lu, date:%s:%ld",
          main_type, sub_type, entry.seq_, date, ms))) {
        ret = OB_IO_ERROR;
        STORAGE_REDO_LOG(WARN, "Fail to print meta info", K(ret), KP(stream), K(errno));
      }

      if (ObRedoLogMainType::OB_REDO_LOG_INVALID == main_type ||
          ObRedoLogMainType::OB_REDO_LOG_MAX == main_type) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(WARN, "The main type is invalid", K(ret), K(main_type));
      } else if (ObRedoLogMainType::OB_REDO_LOG_SYS == main_type) {
        if (OB_UNLIKELY(0 > fprintf(stream, "\nnop log\n"))) {
          ret = OB_IO_ERROR;
          STORAGE_REDO_LOG(WARN, "Fail to print nop log", K(ret), KP(stream), K(errno));
        }
      } else if (OB_ISNULL(redo_modules_[static_cast<int>(main_type)])) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(WARN, "The redo module has not been registered.", K(ret), K(main_type));
      } else if (OB_FAIL(redo_modules_[static_cast<int>(main_type)]->parse(
          entry.cmd_, log_data, disk_addr.size() - dummy_entry.get_serialize_size(), stream))) {
        STORAGE_REDO_LOG(WARN, "Fail to parse slog.", K(ret), K(main_type), K(entry), K(disk_addr));
      }
    }

    if (OB_READ_NOTHING == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObStorageLogReplayer::print_slog(
    const char *buf,
    const int64_t len,
    const char* slog_name,
    ObIBaseStorageLogEntry &slog_entry,
    FILE *stream)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(slog_entry.deserialize(buf, len, pos))) {
    LOG_WARN("Fail to deserialize slog entry.", K(ret), KP(buf), K(len), K(pos));
  } else if (0 > fprintf(stream, "%s\n%s\n", slog_name, to_cstring(slog_entry))) {
    ret = OB_IO_ERROR;
    LOG_WARN("Fail to print slog to file.", K(ret));
  }
  return ret;
}

int ObStorageLogReplayer::check_empty_dir(bool &is_empty_dir, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObLogFileHandler file_handler;
  int64_t min_log_id = 0;
  int64_t max_log_id = 0;

  if (OB_FAIL(file_handler.init(log_dir_, 256 << 20/*file_size*/, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to init log file handler", K(ret));
  } else if (OB_FAIL(file_handler.get_file_id_range(min_log_id, max_log_id))
      && OB_ENTRY_NOT_EXIST != ret) {
    STORAGE_REDO_LOG(WARN, "Fail to get file id range", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    is_empty_dir = true;
  }

  return ret;
}

}
}
