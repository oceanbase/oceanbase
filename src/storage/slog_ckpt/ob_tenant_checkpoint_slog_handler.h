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
#include "observer/ob_server_struct.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_reader.h"
#include "storage/slog_ckpt/ob_tenant_slog_checkpoint_workflow.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_store/ob_startup_accelerate_info.h"
#include "storage/ob_super_block_struct.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/high_availability/ob_tablet_transfer_info.h"

namespace oceanbase
{

namespace share
{
class ObLSID;
class ObTenantSnapshotID;
}
namespace storage
{
class ObTenantSuperBlock;
struct ObMetaDiskAddr;
class ObTenantStorageSnapshotWriter;
class ObRedoModuleReplayParam;
class ObStorageLogger;

struct ObLSCkptMember final
{
public:
  static const int64_t LS_CKPT_MEM_VERSION = 1;
  ObLSCkptMember();
  ~ObLSCkptMember();
  DISALLOW_COPY_AND_ASSIGN(ObLSCkptMember);
  void reset();

  bool is_valid() const;
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  TO_STRING_KV(K_(version), K_(ls_meta), K_(tablet_meta_entry), K_(dup_ls_meta));

public:
  int32_t version_;
  int32_t length_;
  ObLSMeta ls_meta_;
  transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta dup_ls_meta_;
  blocksstable::MacroBlockId tablet_meta_entry_;
};

class ObReplayTabletValue final
{
public:
  ObReplayTabletValue(
      const ObMetaDiskAddr &addr,
      const int64_t ls_epoch)
    : addr_(addr),
      ls_epoch_(ls_epoch),
      tablet_attr_(),
      accelerate_info_()
  {}
  ObReplayTabletValue(
      const ObMetaDiskAddr &addr,
      const int64_t ls_epoch,
      const ObTabletAttr &tablet_attr,
      const ObStartupTabletAccelerateInfo &accelerate_info)
    : addr_(addr),
      ls_epoch_(ls_epoch),
      tablet_attr_(tablet_attr),
      accelerate_info_(accelerate_info)
  {}
  ObReplayTabletValue() = default;
  ~ObReplayTabletValue() = default;
  TO_STRING_KV(K_(addr),
               K_(ls_epoch),
               K_(tablet_attr),
               K_(accelerate_info));
public:
  ObMetaDiskAddr addr_;
  int64_t ls_epoch_;
  ObTabletAttr tablet_attr_;
  ObStartupTabletAccelerateInfo accelerate_info_;
};

class ObTenantCheckpointSlogHandler : public ObIRedoModule
{
public:
  typedef common::hash::ObHashSet<ObDeleteTabletLog> ReplayWaitGCTabletSet;
  typedef common::hash::ObHashSet<ObGCTabletLog> ReplayGCTabletSet;
public:
  ObTenantCheckpointSlogHandler();
  ~ObTenantCheckpointSlogHandler() = default;
  ObTenantCheckpointSlogHandler(const ObTenantCheckpointSlogHandler &) = delete;
  ObTenantCheckpointSlogHandler &operator=(const ObTenantCheckpointSlogHandler &) = delete;

  int init(ObStorageLogger &slogger);
  int start();
  void stop();
  void wait();
  void destroy();

  // for tenant clone & snapshot
  int add_snapshot(const ObTenantSnapshotMeta &tenant_snapshot);
  int delete_snapshot(const share::ObTenantSnapshotID &snapshot_id);
  int swap_snapshot(const ObTenantSnapshotMeta &tenant_snapshot);
  int clone_ls(
      observer::ObStartupAccelTaskHandler* startup_accel_handler,
      const blocksstable::MacroBlockId &tablet_meta_entry);

  // for slog & checkpoint
  int write_checkpoint(const ObTenantSlogCheckpointWorkflow::Type ckpt_type);

  virtual int replay(const ObRedoModuleReplayParam &param) override;
  virtual int replay_over() override;

  // for ls item
  int create_tenant_ls_item(const share::ObLSID ls_id, int64_t &ls_epoch);
  int update_tenant_ls_item(const share::ObLSID ls_id, const int64_t ls_epoch, const ObLSItemStatus status);
  int delete_tenant_ls_item(const share::ObLSID ls_id, const int64_t ls_epoch);

  int update_tenant_preallocated_seqs(const ObTenantMonotonicIncSeqs &preallocated_seqs);
  int get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId> &block_list);
  int read_empty_shell_file(const ObMetaDiskAddr &phy_addr, common::ObArenaAllocator &allocator, char *&buf, int64_t &buf_len);
  int update_hidden_sys_tenant_super_block_to_real(omt::ObTenant &sys_tenant);
  int update_real_sys_tenant_super_block_to_hidden(omt::ObTenant &sys_tenant);

private:
  friend class ObTenantSlogCheckpointWorkflow;

private:
  class ObWriteCheckpointTask : public common::ObTimerTask
  {
  public:
    static const int64_t FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL = 1000L * 1000L * 3600LL;  // 6h
    static const int64_t WRITE_CHECKPOINT_INTERVAL_US = 1000L * 1000L * 60L;             // 1min
    static const int64_t RETRY_WRITE_CHECKPOINT_MIN_INTERVAL = 1000L * 1000L * 300L;     // 5min
    static const int64_t MIN_WRITE_CHECKPOINT_LOG_CNT = 100000; // TODO(fenggu)

    explicit ObWriteCheckpointTask(ObTenantCheckpointSlogHandler *handler) : handler_(handler)
    {
      disable_timeout_check();
    }
    virtual ~ObWriteCheckpointTask() = default;
    virtual void runTimerTask() override;

  private:
    ObTenantCheckpointSlogHandler *handler_;
  };
private:
  int read_from_disk_addr(const ObMetaDiskAddr &phy_addr, char *buf, const int64_t buf_len, char *&r_buf, int64_t &r_len);
  int read_tablet_checkpoint_by_addr(
    const ObMetaDiskAddr &addr, char *item_buf, int64_t &item_buf_len);
  int clone_tablet(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len);
  int gc_checkpoint_file();
  int gc_min_checkpoint_file(const int64_t min_file_id);
  int gc_max_checkpoint_file(const int64_t max_file_id);
  virtual int parse(const int32_t cmd, const char *buf, const int64_t len, FILE *stream) override;
  int replay_checkpoint_and_slog(const ObTenantSuperBlock &super_block);
  int replay_checkpoint(const ObTenantSuperBlock &super_block);
  int replay_snapshot(const ObTenantSuperBlock &super_block);
  int do_replay_single_snapshot(const blocksstable::MacroBlockId &ls_meta_entry);
  int replay_new_checkpoint(const ObTenantSuperBlock &super_block);
  int replay_old_checkpoint(const ObTenantSuperBlock &super_block);
  int replay_ls_meta(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len);
  int replay_tablet(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len);
  int replay_snapshot_ls(
      const ObMetaDiskAddr &addr,
      const char *buf,
      const int64_t buf_len);
  int replay_new_ls(
      const ObMetaDiskAddr &addr,
      const char *buf,
      const int64_t buf_len,
      ObIArray<blocksstable::MacroBlockId> &tablet_block_list);
  int replay_new_tablet(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len);
  int replay_dup_table_ls_meta(const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta);
  int replay_tenant_slog(const common::ObLogCursor &start_point);
  int inner_replay_update_ls_slog(const ObRedoModuleReplayParam &param);
  int inner_replay_create_ls_slog(const ObRedoModuleReplayParam &param);
  int inner_replay_create_ls_commit_slog(const ObRedoModuleReplayParam &param);
  int inner_replay_delete_ls(const ObRedoModuleReplayParam &param);
  int inner_replay_put_old_tablet(const ObRedoModuleReplayParam &param);
  int inner_replay_update_tablet(const ObRedoModuleReplayParam &param);
  int inner_replay_dup_table_ls_slog(const ObRedoModuleReplayParam &param);
  int inner_replay_delete_tablet(const ObRedoModuleReplayParam &param);
  int inner_replay_gc_tablet(const ObRedoModuleReplayParam &param);
  int inner_replay_empty_shell_tablet(const ObRedoModuleReplayParam &param);
  int inner_replay_gts_record(const ObRedoModuleReplayParam &param);
  int inner_replay_gti_record(const ObRedoModuleReplayParam &param);
  int inner_replay_das_record(const ObRedoModuleReplayParam &param);
  int read_from_ckpt(const ObMetaDiskAddr &phy_addr, char *buf, const int64_t buf_len, int64_t &r_len);
  int read_from_slog(const ObMetaDiskAddr &phy_addr, char *buf, const int64_t buf_len, int64_t &pos);
  int remove_tablets_from_replay_map_(const share::ObLSID &ls_id);
  int inner_write_checkpoint(const ObTenantSlogCheckpointWorkflow::Type ckpt_type);

  int inner_replay_deserialize(
      const char *buf,
      const int64_t buf_len,
      bool allow_override /* allow to overwrite the map's element or not */);
  int inner_replay_old_deserialize(
      const ObMetaDiskAddr &addr,
      const char *buf,
      const int64_t buf_len,
      bool allow_override /* allow to overwrite the map's element or not */);

private:
  const static int64_t BUCKET_NUM = 109;
private:
  typedef common::hash::ObHashMap<ObTabletMapKey, ObReplayTabletValue> ReplayTabletDiskAddrMap;
  bool is_inited_;
  bool is_writing_checkpoint_;
  ObStorageLogger *slogger_;
  common::TCRWLock lock_;  // protect block_handle
  ObMetaBlockListHandle ls_block_handle_;
  ObMetaBlockListHandle tablet_block_handle_;
  ObMetaBlockListHandle wait_gc_tablet_block_handle_;

  int tg_id_;
  ObWriteCheckpointTask write_ckpt_task_;
  ReplayTabletDiskAddrMap replay_tablet_disk_addr_map_;
  ReplayWaitGCTabletSet replay_wait_gc_tablet_set_;
  ReplayGCTabletSet replay_gc_tablet_set_;
  lib::ObMutex super_block_mutex_;

  ObTenantSlogCheckpointInfo ckpt_info_;
};

}  // end namespace storage
}  // namespace oceanbase

#endif  // OB_STORAGE_CKPT_TENANT_CHECKPOINT_SLOG_HANDLER_H_
