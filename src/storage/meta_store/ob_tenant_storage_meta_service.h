/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_STORAGE_META_STORE_TENANT_STORAGE_META_SERVICE_
#define OCEANBASE_STORAGE_META_STORE_TENANT_STORAGE_META_SERVICE_

#include <stdint.h>
#include "storage/blockstore/ob_shared_object_reader_writer.h"
#include "storage/meta_store/ob_tenant_seq_generator.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/slog/ob_storage_logger.h"

namespace oceanbase
{
namespace storage
{
struct ObGCTabletMetaInfoList;
struct ObUpdateTabletPointerParam;
class ObTenantStorageMetaService final
{
public:
  ObTenantStorageMetaService();
  ~ObTenantStorageMetaService() = default;

  static int mtl_init(ObTenantStorageMetaService *&meta_service);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  bool is_started() { return is_started_; }

  ObTenantSeqGenerator &get_seq_generator() { return seq_generator_; }

  // for slog & checkpoint operation
  int get_meta_block_list(ObIArray<blocksstable::MacroBlockId> &meta_block_list);
  int write_checkpoint(const ObTenantSlogCheckpointWorkflow::Type ckpt_type);
  storage::ObStorageLogger &get_slogger() { return slogger_; }
  const ObTenantCheckpointSlogHandler& get_ckpt_slog_hdl() const { return ckpt_slog_handler_; };

  // for ls operation
  int prepare_create_ls(const ObLSMeta &meta, int64_t &ls_epoch);
  int commit_create_ls(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int abort_create_ls(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int delete_ls(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int delete_ls_item(const share::ObLSID ls_id, const int64_t ls_epoch);
  int update_ls_meta(const ObLSMeta &ls_meta);
  int update_dup_table_meta(const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_table_meta);
  int get_ls_items_by_status(const storage::ObLSItemStatus status, common::ObIArray<storage::ObLSItem> &ls_items);

  // for tablet operation
  int batch_update_tablet(const ObIArray<ObUpdateTabletLog> &slog_arr);
  int update_tablet(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const common::ObTabletID &tablet_id,
      const ObUpdateTabletPointerParam &update_tablet_pointer_param);
  /// @param last_gc_version: only used when SS mode(Just pass -1 if in SN mode)
  int remove_tablet(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletHandle &tablet_handle,
      const int64_t last_gc_version);
  /// @param last_gc_versions: only used when SS mode(Just pass empty array if in SN mode)
  int batch_remove_tablet(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObIArray<common::ObTabletID> &tablet_id_arr,
      const ObIArray<ObMetaDiskAddr> &tablet_addr_arr,
      const ObIArray<int64_t> &last_gc_versions);
  int write_empty_shell_tablet(
      const uint64_t data_version,
      const int64_t ls_epoch,
      ObTablet *tablet,
      ObMetaDiskAddr &tablet_addr);
  int replay_wait_gc_tablet_array(
      const ObMetaDiskAddr &addr,
      const char *buf,
      const int64_t buf_len);
  int get_wait_gc_tablet_items(
      const ObLSID &ls_id,
      const int64_t ls_epoch,
      common::ObIArray<ObPendingFreeTabletItem> &items);
  int delete_wait_gc_tablet_items(
      const ObLSID &ls_id,
      const int64_t ls_epoch,
      const common::ObIArray<ObPendingFreeTabletItem> &items);
  int replay_apply_wait_gc_tablet_items(
      ObTenantCheckpointSlogHandler::ReplayWaitGCTabletSet &wait_gc_set,
      ObTenantCheckpointSlogHandler::ReplayGCTabletSet &gc_set);

  // for tenant clone & snapshot operation
  int add_snapshot(const ObTenantSnapshotMeta &tenant_snapshot);
  int delete_snapshot(const share::ObTenantSnapshotID &snapshot_id);
  int swap_snapshot(const ObTenantSnapshotMeta &tenant_snapshot);
  int clone_ls(
      observer::ObStartupAccelTaskHandler* startup_accel_handler,
      const blocksstable::MacroBlockId &tablet_meta_entry);

  // for share meta object read/write operation
  int read_from_disk(
      const ObMetaDiskAddr &addr,
      const int64_t ls_epoch,
      common::ObArenaAllocator &allocator,
      char *&buf,
      int64_t &buf_len);
  ObSharedObjectReaderWriter &get_shared_object_reader_writer() { return shared_object_rwriter_; }
  ObSharedObjectReaderWriter &get_shared_object_raw_reader_writer() { return shared_object_raw_rwriter_; }
  int update_hidden_sys_tenant_super_block_to_real(omt::ObTenant &sys_tenant);
  int update_real_sys_tenant_super_block_to_hidden(omt::ObTenant &sys_tenant);
  int get_max_meta_version_from_wait_gc_map(
      const ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      /*out*/ int64_t &max_meta_version);

#ifdef OB_BUILD_SHARED_STORAGE
  // for shared storage gc operation
  int get_blocks_from_private_tablet_meta(
      const share::ObLSID &id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      const int64_t tablet_version,
      const int32_t tablet_private_transfer_epoch,
      const bool is_shared,
      ObIArray<blocksstable::MacroBlockId> &block_ids);
  int get_shared_blocks_for_tablet(
      const ObTabletID &tablet_id,
      const int64_t tablet_version,
      ObIArray<blocksstable::MacroBlockId> &block_ids);
  int get_next_major_shared_blocks_for_tablet(
      const ObTabletID &tablet_id,
      const int64_t last_tablet_version,// last major snapshot
      ObIArray<blocksstable::MacroBlockId> &block_ids);
  int get_gc_tablet_scn_arr(
      const ObTabletID &tablet_id,
      const blocksstable::ObStorageObjectType obj_type,
      ObGCTabletMetaInfoList &tablet_scn_arr);
  int write_gc_tablet_scn_arr(
    const ObTabletID &tablet_id,
    const blocksstable::ObStorageObjectType obj_type,
    const ObGCTabletMetaInfoList &tablet_scn_arr);
  static int ss_is_meta_list_exist(const ObTabletID tablet_id, bool &is_exist);
  int update_shared_tablet_meta_list(
    const ObTabletID &tablet_id,
    const int64_t tablet_meta_version);
#endif

private:
  class ObLSItemIterator final
  {
  public:
    explicit ObLSItemIterator(const storage::ObTenantSuperBlock &super_block):
      idx_(0),
      tenant_super_block_(super_block)
      {}
    ~ObLSItemIterator() = default;
    int get_next_ls_item(storage::ObLSItem &item);
    TO_STRING_KV(K_(idx), K_(tenant_super_block));
  private:
    int64_t idx_;
    const storage::ObTenantSuperBlock tenant_super_block_;
    DISALLOW_COPY_AND_ASSIGN(ObLSItemIterator);
  };
private:
  class WaitGCTabletArrayKey final
  {
  public:
    WaitGCTabletArrayKey() : ls_id_(), ls_epoch_(0) {}
    WaitGCTabletArrayKey(const share::ObLSID &ls_id, const int64_t ls_epoch)
      : ls_id_(ls_id), ls_epoch_(ls_epoch)
    {}
    uint64_t hash() const { return ls_id_.hash() ^ ls_epoch_;}
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    bool operator ==(const WaitGCTabletArrayKey &other) const
    {
      return other.ls_id_ == ls_id_ && other.ls_epoch_ == ls_epoch_;
    }
    bool operator !=(const WaitGCTabletArrayKey &other) const { return !(other == *this); }
    bool operator <(const WaitGCTabletArrayKey &other) const
    {
      bool bool_ret = false;
      if (ls_id_ < other.ls_id_) {
        bool_ret = true;
      } else if (ls_id_ == other.ls_id_) {
        bool_ret = (ls_epoch_ < other.ls_epoch_);
      }
      return bool_ret;
    }
    TO_STRING_KV(K_(ls_id), K_(ls_epoch));
  public:
    share::ObLSID ls_id_;
    int64_t ls_epoch_;
  };
private:
  class WaitGCTabletArray final
  {
  public:
    WaitGCTabletArray() : lock_(), wait_gc_tablet_arr_() {}
    ~WaitGCTabletArray() = default;
    TO_STRING_KV(K_(wait_gc_tablet_arr));
  public:
    lib::ObMutex lock_;
    ObLSPendingFreeTabletArray wait_gc_tablet_arr_;
  };

  typedef common::hash::ObHashMap<WaitGCTabletArrayKey, WaitGCTabletArray*, common::hash::NoPthreadDefendMode> WaitGCTabletArrayMap;

  struct TabletInfo
  {
  public:
    TabletInfo():
      tablet_id_(),
      tablet_addr_(),
      last_gc_version_(-1)
    {
    }
    TabletInfo(
      const ObTabletID &tablet_id,
      const ObMetaDiskAddr &tablet_addr,
      const int64_t last_gc_version):
      tablet_id_(tablet_id),
      tablet_addr_(tablet_addr),
      last_gc_version_(last_gc_version)
    {
    }
    bool is_valid() const
    {
      return tablet_id_.is_valid() &&
             tablet_addr_.is_valid() &&
             last_gc_version_ >= -1 &&
             last_gc_version_ < static_cast<int64_t>(tablet_addr_.block_id().meta_version_id());
    }
    void reset()
    {
      tablet_id_.reset();
      tablet_addr_.reset();
      last_gc_version_ = -1;
    }
    TO_STRING_KV(K_(tablet_id), K_(tablet_addr), K_(last_gc_version));

  public:
    ObTabletID tablet_id_;
    ObMetaDiskAddr tablet_addr_;
    int64_t last_gc_version_;
  };

private:
  int read_from_share_blk(
      const ObMetaDiskAddr &addr,
      const int64_t ls_epoch,
      common::ObArenaAllocator &allocator,
      char *&buf,
      int64_t &buf_len);
  int write_remove_tablet_slog_for_sn(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id);
  int write_remove_tablet_slog_for_ss(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      const ObTablet &tablet,
      const int64_t last_gc_version);
  /// @param last_gc_versions: only used when SS mode(Just pass empty array if in SN mode)
  int batch_write_remove_tablet_slog(
      const ObLSID &ls_id,
      const int64_t ls_epoch,
      const common::ObIArray<ObTabletID> &tablet_ids,
      const common::ObIArray<ObMetaDiskAddr> &tablet_addrs,
      const common::ObIArray<int64_t> &last_gc_versions);
  int safe_batch_write_remove_tablet_slog_for_sn(
      const ObLSID &ls_id,
      const common::ObIArray<TabletInfo> &tablet_infos);
  int safe_batch_write_remove_tablet_slog_for_ss(
      const ObLSID &ls_id,
      const int64_t ls_epoch,
      const common::ObIArray<TabletInfo> &tablet_infos,
      const int64_t item_arr_expand_cnt);
  int safe_batch_write_gc_tablet_slog(
      const ObLSID &ls_id,
      const int64_t ls_epoch,
      const common::ObIArray<ObPendingFreeTabletItem> &items,
      WaitGCTabletArray &wait_gc_tablet_array);
  int get_wait_gc_tablet_items_except_gc_set(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTenantCheckpointSlogHandler::ReplayGCTabletSet &gc_set,
      ObTenantCheckpointSlogHandler::ReplayWaitGCTabletSet &wait_gc_set,
      common::ObIArray<ObPendingFreeTabletItem> &items);
  int add_wait_gc_set_items_except_gc_set(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTenantCheckpointSlogHandler::ReplayGCTabletSet &gc_set,
      const ObTenantCheckpointSlogHandler::ReplayWaitGCTabletSet &wait_gc_set,
      common::ObIArray<ObPendingFreeTabletItem> &items);

private:
#ifdef OB_BUILD_SHARED_STORAGE
  int inner_get_blocks_for_tablet_(
    const ObMetaDiskAddr &tablet_addr,
    const int64_t ls_epoch,
    const bool is_shared,
    ObIArray<blocksstable::MacroBlockId> &block_ids/*OUT*/) const;
  int inner_get_gc_tablet_scn_arr_(
    const blocksstable::ObStorageObjectOpt &opt,
    ObGCTabletMetaInfoList &gc_tablet_scn_arr) const;
  int ss_write_gc_info_(
     const ObTabletID tablet_id, const ObGCTabletMetaInfoList &gc_info_scn_arr);
  int ss_write_meta_list_(
     const ObTabletID tablet_id, const ObGCTabletMetaInfoList &meta_list_scn_arr);
  int force_write_gc_tablet_scn_arr_(
    const ObTabletID &tablet_id,
    const blocksstable::ObStorageObjectType obj_type,
    const ObGCTabletMetaInfoList &tablet_scn_arr);
#endif

private:
  bool is_inited_;
  bool is_started_;
  bool is_shared_storage_;
  ObTenantCheckpointSlogHandler ckpt_slog_handler_;
  storage::ObStorageLogger slogger_;
  ObTenantSeqGenerator seq_generator_;
  ObSharedObjectReaderWriter shared_object_rwriter_;
  ObSharedObjectReaderWriter shared_object_raw_rwriter_;
  lib::ObMutex wait_gc_map_lock_; // wait_gc_tablet_arr_map_
  WaitGCTabletArrayMap wait_gc_tablet_arr_map_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantStorageMetaService);
};

#define TENANT_STORAGE_META_SERVICE (*MTL(ObTenantStorageMetaService *))
#define TENANT_SEQ_GENERATOR (MTL(ObTenantStorageMetaService*)->get_seq_generator())
#define TENANT_SLOGGER (MTL(ObTenantStorageMetaService*)->get_slogger())

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_META_STORE_TENANT_STORAGE_META_SERVICE_
