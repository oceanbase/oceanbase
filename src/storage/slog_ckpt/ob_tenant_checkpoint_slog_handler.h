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

#ifndef OB_STORAGE_CKPT_TENANT_CHECKPOINT_SLOG_HANDLER_H_
#define OB_STORAGE_CKPT_TENANT_CHECKPOINT_SLOG_HANDLER_H_

#include "common/log/ob_log_cursor.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_reader.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/ob_super_block_struct.h"
#include "storage/slog/ob_storage_log_replayer.h"

namespace oceanbase
{

namespace share
{
class ObLSID;
}
namespace storage
{
class ObTenantSuperBlock;
struct ObMetaDiskAddr;
class ObTenantStorageCheckpointWriter;
class ObRedoModuleReplayParam;
class ObStorageLogger;
class ObLSTabletService;
class ObLSHandle;

class ObTenantCheckpointSlogHandler : public ObIRedoModule
{
public:
  class ObWriteCheckpointTask : public common::ObTimerTask
  {
  public:
    static const int64_t FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL = 1000L * 1000L * 3600LL;  // 6h
    static const int64_t WRITE_CHECKPOINT_INTERVAL_US = 1000L * 1000L * 60L;             // 1min
    static const int64_t RETRY_WRITE_CHECKPOINT_MIN_INTERVAL = 1000L * 1000L * 300L;     // 5min
    static const int64_t MIN_WRITE_CHECKPOINT_LOG_CNT = 50000; // TODO(fenggu)

    explicit ObWriteCheckpointTask(ObTenantCheckpointSlogHandler *handler) : handler_(handler) {}
    virtual ~ObWriteCheckpointTask() = default;
    virtual void runTimerTask() override;

  private:
    ObTenantCheckpointSlogHandler *handler_;
  };

  ObTenantCheckpointSlogHandler();
  ~ObTenantCheckpointSlogHandler() = default;
  ObTenantCheckpointSlogHandler(const ObTenantCheckpointSlogHandler &) = delete;
  ObTenantCheckpointSlogHandler &operator=(const ObTenantCheckpointSlogHandler &) = delete;

  static int mtl_init(ObTenantCheckpointSlogHandler *&handler);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  int read_tablet_checkpoint_by_addr(
    const ObMetaDiskAddr &addr, char *item_buf, int64_t &item_buf_len);

  int get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId> &block_list);
  virtual int replay(const ObRedoModuleReplayParam &param) override;
  virtual int replay_over() override;
  int write_checkpoint(bool is_force);
  int read_from_disk_addr(const ObMetaDiskAddr &phy_addr, char *buf, const int64_t buf_len, char *&r_buf, int64_t &r_len);

private:
  virtual int parse(const int32_t cmd, const char *buf, const int64_t len, FILE *stream) override;
  int replay_checkpoint_and_slog(const ObTenantSuperBlock &super_block);
  int replay_checkpoint(const ObTenantSuperBlock &super_block);
  int replay_ls_meta(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len);
  int replay_tablet(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len);
  int replay_dup_table_ls_meta(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len);
  int update_tablet_meta_addr_and_block_list(ObTenantStorageCheckpointWriter &ckpt_writer);
  int replay_tenant_slog(const common::ObLogCursor &start_point);
  int replay_load_tablets();

  int inner_replay_update_ls_slog(const ObRedoModuleReplayParam &param);
  int inner_replay_create_ls_slog(const ObRedoModuleReplayParam &param);
  int inner_replay_create_ls_commit_slog(const ObRedoModuleReplayParam &param);
  int inner_replay_delete_ls(const ObRedoModuleReplayParam &param);
  int inner_replay_dup_table_ls_slog(const ObRedoModuleReplayParam &param);
  int inner_replay_put_tablet(const ObRedoModuleReplayParam &param);
  int inner_replay_delete_tablet(const ObRedoModuleReplayParam &param);
  int inner_replay_gts_record(const ObRedoModuleReplayParam &param);
  int inner_replay_gti_record(const ObRedoModuleReplayParam &param);
  int inner_replay_das_record(const ObRedoModuleReplayParam &param);
  int get_tablet_svr(const share::ObLSID &ls_id, ObLSTabletService *&ls_tablet_svr, ObLSHandle &ls_handle);
  int read_from_ckpt(const ObMetaDiskAddr &phy_addr, char *buf, const int64_t buf_len, int64_t &r_len);
  int read_from_slog(const ObMetaDiskAddr &phy_addr, char *buf, const int64_t buf_len, int64_t &pos);
  int remove_tablets_from_replay_map_(const share::ObLSID &ls_id);

private:
  typedef common::hash::ObHashMap<ObTabletMapKey, ObMetaDiskAddr> ReplayTabletDiskAddrMap;
  bool is_inited_;
  bool is_writing_checkpoint_;
  int64_t last_ckpt_time_;
  int64_t last_frozen_version_;
  common::TCRWLock lock_;  // protect block_handle
  ObMetaBlockListHandle ls_block_handle_;
  ObMetaBlockListHandle tablet_block_handle_;

  int tg_id_;
  ObWriteCheckpointTask write_ckpt_task_;
  ReplayTabletDiskAddrMap replay_tablet_disk_addr_map_;
};

}  // end namespace storage
}  // namespace oceanbase

#endif  // OB_STORAGE_CKPT_TENANT_CHECKPOINT_SLOG_HANDLER_H_
