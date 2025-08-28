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
#include "storage/meta_store/ob_tenant_storage_meta_persister.h"
#include "storage/meta_store/ob_tenant_storage_meta_replayer.h"
#include "storage/blockstore/ob_shared_object_reader_writer.h"
#include "storage/meta_store/ob_tenant_seq_generator.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/slog/ob_storage_logger.h"

namespace oceanbase
{
namespace storage
{
struct ObGCTabletMetaInfoList;
class ObTenantStorageMetaService
{
public:
  ObTenantStorageMetaService();
  ~ObTenantStorageMetaService() = default;
  ObTenantStorageMetaService(const ObTenantStorageMetaService &) = delete;
  ObTenantStorageMetaService &operator=(const ObTenantStorageMetaService &) = delete;

  static int mtl_init(ObTenantStorageMetaService *&meta_service);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  bool is_started() { return is_started_; }
  ObTenantStorageMetaPersister &get_persister() { return persister_; }
  ObTenantStorageMetaReplayer &get_replayer() { return replayer_; }
  ObTenantSeqGenerator &get_seq_generator() { return seq_generator_; }
  int get_active_cursor(common::ObLogCursor &log_cursor);
  int get_meta_block_list(ObIArray<blocksstable::MacroBlockId> &meta_block_list);
  int write_checkpoint(bool is_force);
  int add_snapshot(const ObTenantSnapshotMeta &tenant_snapshot);
  int delete_snapshot(const share::ObTenantSnapshotID &snapshot_id);
  int swap_snapshot(const ObTenantSnapshotMeta &tenant_snapshot);
  int clone_ls(
      observer::ObStartupAccelTaskHandler* startup_accel_handler,
      const blocksstable::MacroBlockId &tablet_meta_entry);
  int read_from_disk(
      const ObMetaDiskAddr &addr,
      const int64_t ls_epoch,
      common::ObArenaAllocator &allocator,
      char *&buf,
      int64_t &buf_len);
  int read_from_share_blk(
      const ObMetaDiskAddr &addr,
      const int64_t ls_epoch,
      common::ObArenaAllocator &allocator,
      char *&buf,
      int64_t &buf_len);
  const ObTenantCheckpointSlogHandler& get_ckpt_slog_hdl() const { return ckpt_slog_handler_; };

#ifdef OB_BUILD_SHARED_STORAGE
  int get_private_blocks_for_tablet(
      const share::ObLSID &id, 
      const int64_t ls_epoch, 
      const ObTabletID &tablet_id, 
      const int64_t tablet_version, 
      const int64_t tablet_transfer_seq,
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
  ObSharedObjectReaderWriter &get_shared_object_reader_writer() { return shared_object_rwriter_; }
  ObSharedObjectReaderWriter &get_shared_object_raw_reader_writer() { return shared_object_raw_rwriter_; }
  storage::ObStorageLogger &get_slogger() { return slogger_; }

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
  int get_ls_items_by_status(
    const storage::ObLSItemStatus status,
    ObIArray<storage::ObLSItem> &ls_items);
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
  ObTenantStorageMetaPersister persister_;
  ObTenantStorageMetaReplayer replayer_;
  ObSharedObjectReaderWriter shared_object_rwriter_;
  ObSharedObjectReaderWriter shared_object_raw_rwriter_;
  
};

#define TENANT_STORAGE_META_PERSISTER (MTL(ObTenantStorageMetaService*)->get_persister())
#define TENANT_SEQ_GENERATOR (MTL(ObTenantStorageMetaService*)->get_seq_generator())


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_META_STORE_TENANT_STORAGE_META_SERVICE_
