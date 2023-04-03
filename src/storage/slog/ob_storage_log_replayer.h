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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_REPLAYER_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_REPLAYER_H_

#include "ob_storage_log_struct.h"
#include "common/log/ob_log_cursor.h"
#include "storage/blocksstable/ob_log_file_spec.h"

namespace oceanbase
{
namespace storage
{
class ObStorageLogReader;

struct ObRedoModuleReplayParam final
{
public:
  ObRedoModuleReplayParam();
  ~ObRedoModuleReplayParam() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(cmd), KP_(buf), K_(disk_addr));
  int32_t cmd_;
  const char *buf_;
  ObMetaDiskAddr disk_addr_;
  int64_t data_size_;
};

class ObIRedoModule
{
public:
  ObIRedoModule() {};
  virtual ~ObIRedoModule() = default;

  //replay the redo log.
  virtual int replay(const ObRedoModuleReplayParam &param) = 0;
  //parse the redo log to stream
  virtual int parse(
      const int32_t cmd,
      const char *buf,
      const int64_t len,
      FILE *stream) = 0;
  virtual int replay_over() { return common::OB_SUCCESS; }

  inline static int32_t gen_cmd(
      const enum ObRedoLogMainType cmd_prefix,
      const enum ObRedoLogSubType cmd_suffix);
  inline static void parse_cmd(
      const int32_t cmd,
      enum ObRedoLogMainType &cmd_prefid,
      enum ObRedoLogSubType &cmd_suffix);
};

class ObStorageLogReplayer
{
public:
  ObStorageLogReplayer();
  ~ObStorageLogReplayer();

  int init(const char *log_dir, const blocksstable::ObLogFileSpec &log_file_spec);
  void destroy();

  //NOT thread safe.
  //redo module register.
  int register_redo_module(enum ObRedoLogMainType cmd_prefix, ObIRedoModule *module);
  int unregister_redo_module(enum ObRedoLogMainType cmd_prefix);

  // NOT thread safe.
  // read checkpoints and replay redo logs
  int replay(
      const common::ObLogCursor &replay_start_cursor,
      common::ObLogCursor &finish_cursor,
      const uint64_t tenant_id);

  int replay_over();

  int parse_log(
      const char *log_dir,
      const int64_t log_file_id,
      const blocksstable::ObLogFileSpec &log_file_spec,
      FILE *stream,
      const int64_t offset,
      const int64_t parse_count);

  static int print_slog(
      const char *buf,
      const int64_t len,
      const char* slog_name,
      ObIBaseStorageLogEntry &slog_entry,
      FILE *stream);

private:
  int check_empty_dir(bool &is_empty_dir, const uint64_t tenant_id);

public:
  static const int MAX_SLOG_NAME_LEN = 128;

private:
  bool is_inited_;
  const char *log_dir_;
  blocksstable::ObLogFileSpec log_file_spec_;
  ObIRedoModule *redo_modules_[static_cast<int>(ObRedoLogMainType::OB_REDO_LOG_MAX)];

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageLogReplayer);
};

inline int32_t ObIRedoModule::gen_cmd(
    const enum ObRedoLogMainType cmd_prefix,
    const enum ObRedoLogSubType cmd_suffix)
{
  int32_t cmd = static_cast<int32_t>(cmd_prefix);
  cmd = (cmd << 16) + static_cast<int32_t>(cmd_suffix);
  return cmd;
}

inline void ObIRedoModule::parse_cmd(
    const int32_t cmd,
    enum ObRedoLogMainType &cmd_prefix,
    enum ObRedoLogSubType &cmd_suffix)
{
  cmd_prefix = static_cast<enum ObRedoLogMainType>(cmd >> 16);
  cmd_suffix = static_cast<enum ObRedoLogSubType>(cmd & 0x0000FFFF);
}

}
}
#endif
