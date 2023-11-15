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

#ifndef OB_STORAGE_CKPT_SERVER_CHECKPOINT_SLOG_HANDLER_H_
#define OB_STORAGE_CKPT_SERVER_CHECKPOINT_SLOG_HANDLER_H_

#include "common/log/ob_log_cursor.h"
#include "lib/atomic/ob_atomic.h"
#include "observer/omt/ob_tenant_meta.h"
#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_reader.h"
#include "storage/ob_super_block_struct.h"
#include "storage/slog/ob_storage_log_replayer.h"

namespace oceanbase
{
namespace storage
{

struct ObMetaDiskAddr;

class ObRedoModuleReplayParam;

class ObStorageLogger;

class ObServerCheckpointSlogHandler : public ObIRedoModule
{
public:
  class ObWriteCheckpointTask : public common::ObTimerTask
  {
  public:
    static const int64_t FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL = 1000L * 1000L * 3600LL;  // 6h
    static const int64_t WRITE_CHECKPOINT_INTERVAL_US = 1000L * 1000L * 60L;             // 1min
    static const int64_t RETRY_WRITE_CHECKPOINT_MIN_INTERVAL = 1000L * 1000L * 300L;     // 5min
    static const int64_t MIN_WRITE_CHECKPOINT_LOG_CNT = 50000; // TODO(fenggu)

    explicit ObWriteCheckpointTask(ObServerCheckpointSlogHandler *handler) : handler_(handler) {}
    virtual ~ObWriteCheckpointTask() = default;
    virtual void runTimerTask() override;

  private:
    ObServerCheckpointSlogHandler *handler_;
  };

  typedef common::hash::ObHashMap<uint64_t, omt::ObTenantMeta> TENANT_META_MAP;

  ObServerCheckpointSlogHandler();
  ~ObServerCheckpointSlogHandler() = default;
  ObServerCheckpointSlogHandler(const ObServerCheckpointSlogHandler &) = delete;
  ObServerCheckpointSlogHandler &operator=(const ObServerCheckpointSlogHandler &) = delete;

  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  bool is_started() const { return ATOMIC_LOAD(&is_started_); }
  int get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId> &block_list);
  virtual int replay(const ObRedoModuleReplayParam &param) override;
  virtual int replay_over() override;

  static ObServerCheckpointSlogHandler &get_instance();

  int write_checkpoint(bool is_force);

  int load_all_tenant_metas(); // for obadmin
  int write_tenant_super_block_slog(const ObTenantSuperBlock &super_block);
  const TENANT_META_MAP &get_tenant_meta_map() const { return tenant_meta_map_for_replay_; } // for obadmin

private:
  virtual int parse(const int32_t cmd, const char *buf, const int64_t len, FILE *stream) override;

  int try_write_checkpoint_for_compat();
  int read_checkpoint(const ObServerSuperBlock &super_block);
  int replay_and_apply_server_slog(const common::ObLogCursor &replay_start_point);
  int replay_server_slog(const common::ObLogCursor &replay_start_point, common::ObLogCursor &replay_finish_point);

  int replay_create_tenant_prepare(const char *buf, const int64_t buf_len);
  int replay_create_tenant_commit(const char *buf, const int64_t buf_len);
  int replay_create_tenant_abort(const char *buf, const int64_t buf_len);

  int replay_delete_tenant_prepare(const char *buf, const int64_t buf_len);
  int replay_delete_tenant_commit(const char *buf, const int64_t buf_len);
  int replay_delete_tenant(const char *buf, const int64_t buf_len);
  int replay_update_tenant_unit(const char *buf, const int64_t buf_len);
  int replay_update_tenant_super_block(const char *buf, const int64_t buf_len);

  int set_meta_block_list(common::ObIArray<blocksstable::MacroBlockId> &meta_block_list);
  int apply_replay_result();
  int handle_tenant_creating(const uint64_t tenant_id);
  int handle_tenant_create_commit(const omt::ObTenantMeta &tenant_meta);

  int handle_tenant_deleting(const uint64_t tenant_id);
  int finish_slog_replay();
  static int online_ls();
  int mock_start(); // for test;

private:
  bool is_inited_;
  bool is_started_;
  bool is_writing_checkpoint_;
  ObStorageLogger *server_slogger_;
  common::TCRWLock lock_;  // protect block_handle
  ObMetaBlockListHandle server_meta_block_handle_;
  common::ObTimer task_timer_;
  ObWriteCheckpointTask write_ckpt_task_;
  TENANT_META_MAP tenant_meta_map_for_replay_; // only used when replay
};

}  // end namespace storage
}  // namespace oceanbase

#endif  // OB_STORAGE_CKPT_SERVER_CHECKPOINT_SLOG_HANDLER_H_
