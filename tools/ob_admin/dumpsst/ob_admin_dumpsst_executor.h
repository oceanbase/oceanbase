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

#ifndef OB_ADMIN_DUMPSST_EXECUTOR_H_
#define OB_ADMIN_DUMPSST_EXECUTOR_H_
#include "../ob_admin_executor.h"
#include "lib/container/ob_array.h"
#include "share/config/ob_config_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/ob_i_table.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_reload_config.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "storage/ob_tenant_file_super_block_checkpoint_reader.h"
#include "storage/ob_server_pg_meta_checkpoint_reader.h"

namespace oceanbase
{
namespace storage
{
  class ObBaseFileMgr;
  class ObPartitionMetaRedoModule;
}

namespace tools
{

enum ObAdminDumpsstCmd
{
  DUMP_MACRO_META,
  DUMP_SUPER_BLOCK,
  DUMP_MACRO_DATA,
  PRINT_MACRO_BLOCK,
  DUMP_SSTABLE,
  DUMP_SSTABLE_META,
  DUMP_MAX,
};

struct ObDumpMacroBlockContext final
{
public:
  ObDumpMacroBlockContext()
    : macro_id_(-1), micro_id_(-1), tenant_id_(common::OB_INVALID_ID), file_id_(-1)
  {}
  ~ObDumpMacroBlockContext() = default;
  bool is_valid() const { return macro_id_ >= 0; }
  TO_STRING_KV(K_(macro_id), K_(micro_id), K_(tenant_id), K_(file_id));
  int64_t macro_id_;
  int64_t micro_id_;
  uint64_t tenant_id_;
  int64_t file_id_;
};

class ObAdminDumpsstExecutor : public ObAdminExecutor
{
public:
  ObAdminDumpsstExecutor();
  virtual ~ObAdminDumpsstExecutor();
  virtual int execute(int argc, char *argv[]);
private:
  int parse_cmd(int argc, char *argv[]);
  void print_macro_block();
  void print_usage();
  void print_macro_meta();
  void print_super_block();
  int dump_macro_block(const ObDumpMacroBlockContext &context);
  void dump_sstable();
  void dump_sstable_meta();
  int open_store_file();
  int load_config();
  int replay_slog_to_get_sstable(storage::ObSSTable *&sstable);


  blocksstable::ObStorageEnv storage_env_;
  char data_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  char slog_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  char clog_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  char ilog_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  char clog_shm_path_[common::OB_MAX_FILE_NAME_LENGTH];
  char ilog_shm_path_[common::OB_MAX_FILE_NAME_LENGTH];
  char sstable_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  bool is_quiet_;
  bool in_csv_;
  ObAdminDumpsstCmd cmd_;
  storage::ObITable::TableKey table_key_;
  bool skip_log_replay_;
  ObDumpMacroBlockContext dump_macro_context_;
  observer::ObServerReloadConfig reload_config_;
  common::ObConfigManager config_mgr_;
};

class ObAdminSlogReplayer final
{
public:
  explicit ObAdminSlogReplayer(storage::ObBaseFileMgr &file_mgr,
                                  storage::ObPartitionMetaRedoModule &pg_mgr,
                                  char *slog_dir);
  virtual ~ObAdminSlogReplayer();

  int replay_slog();
  int init();
  void reset();
  int get_sstable(storage::ObITable::TableKey table_key, storage::ObSSTable *&sstable);

private:
  int read_checkpoint_and_replay_log(common::ObLogCursor &checkpoint);
  int replay_server_slog(
      const char *slog_dir,
      const common::ObLogCursor &replay_start_cursor,
      const blocksstable::ObStorageLogCommittedTransGetter &committed_trans_getter);
  int replay_pg_slog(const char *slog_dir,
      const common::ObLogCursor &replay_start_cursor,
      const blocksstable::ObStorageLogCommittedTransGetter &committed_trans_getter,
      common::ObLogCursor &checkpoint);

private:
  class ServerMetaSLogFilter : public blocksstable::ObISLogFilter
  {
  public:
    ServerMetaSLogFilter() = default;
    virtual ~ServerMetaSLogFilter() = default;
    virtual int filter(const ObISLogFilter::Param &param, bool &is_filtered) const override;
  };

  class PGMetaSLogFilter : public blocksstable::ObISLogFilter
  {
  public:
    explicit PGMetaSLogFilter(storage::ObBaseFileMgr &file_mgr) : file_mgr_(file_mgr) {};
    virtual ~PGMetaSLogFilter() = default;
    virtual int filter(const ObISLogFilter::Param &param, bool &is_filtered) const override;
  private:
    storage::ObBaseFileMgr &file_mgr_;
  };

  const common::ObAddr svr_addr_;
  blocksstable::ObStorageFileWithRef svr_root_;
  storage::ObBaseFileMgr &file_mgr_;
  storage::ObPartitionMetaRedoModule &pg_mgr_;
  blocksstable::ObServerSuperBlock super_block_;
  storage::ObServerPGMetaCheckpointReader pg_meta_reader_;
  storage::ObTenantFileSuperBlockCheckpointReader tenant_file_reader_;
  char *slog_dir_;

  DISALLOW_COPY_AND_ASSIGN(ObAdminSlogReplayer);
};

} //namespace tools
} //namespace oceanbase

#endif /* OB_ADMIN_DUMPSST_EXECUTOR_H_ */
