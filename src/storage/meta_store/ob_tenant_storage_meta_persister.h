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
#ifndef OCEANBASE_STORAGE_META_STORE_OB_TENANT_STORAGE_META_PERSISTER_H_
#define OCEANBASE_STORAGE_META_STORE_OB_TENANT_STORAGE_META_PERSISTER_H_

#include "share/ob_unit_getter.h"
#include "storage/slog/ob_storage_log.h"
#include "lib/hash/ob_hashmap.h"
#include "storage/tx/ob_dup_table_base.h"

namespace oceanbase
{
namespace storage
{
class ObLSMeta;
class ObStorageLogger;
class ObTenantMonotonicIncSeqs;
class ObTenantCheckpointSlogHandler;


class ObTenantStorageMetaPersister
{
public:
  ObTenantStorageMetaPersister()
    : is_inited_(false), is_shared_storage_(false),
      slogger_() {}
  ObTenantStorageMetaPersister(const ObTenantStorageMetaPersister &) = delete;
  ObTenantStorageMetaPersister &operator=(const ObTenantStorageMetaPersister &) = delete;
      
  int init(const bool is_share_storage,
           ObStorageLogger &slogger,
           ObTenantCheckpointSlogHandler &ckpt_slog_handler);
  void destroy();


  int prepare_create_ls(const ObLSMeta &meta, int64_t &ls_epoch);
  int commit_create_ls(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int abort_create_ls(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int delete_ls(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int update_ls_meta(const int64_t ls_epoch, const ObLSMeta &ls_meta);
  int update_dup_table_meta(
      const int64_t ls_epoch,
      const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_table_meta);
  int update_tenant_preallocated_seqs(const ObTenantMonotonicIncSeqs &preallocated_seqs);
  int batch_update_tablet(const ObIArray<ObUpdateTabletLog> &slog_arr);
  int batch_update_tablet(const ObIArray<ObUpdateTabletLog> &slog_arr, ObIArray<ObStorageLogParam> &param_arr);
  int update_tablet(
    const share::ObLSID &ls_id, const int64_t ls_epoch,
    const common::ObTabletID &tablet_id, const ObMetaDiskAddr &disk_addr);
  int write_active_tablet_array(ObLS *ls);
  
  int write_empty_shell_tablet(ObTablet *tablet, ObMetaDiskAddr &tablet_addr);
  int remove_tablet(
      const share::ObLSID &ls_id, const int64_t ls_epoch,
      const ObTabletHandle &tablet_handle);
  int remove_tablets(
      const share::ObLSID &ls_id, const int64_t ls_epoch,
      const ObIArray<common::ObTabletID> &tablet_id_arr, const ObIArray<ObMetaDiskAddr> &tablet_addr_arr);

  int get_items_from_pending_free_tablet_array( 
      const ObLSID &ls_id, 
      const int64_t ls_epoch,
      ObIArray<ObPendingFreeTabletItem> &items);
  int delete_items_from_pending_free_tablet_array(
      const ObLSID &ls_id, 
      const int64_t ls_epoch, 
      const ObIArray<ObPendingFreeTabletItem> &items);
#ifdef OB_BUILD_SHARED_STORAGE
  // recover pending_free_tablet_arr_map during reboot
  int ss_replay_ls_pending_free_arr(
    ObArenaAllocator &allocator,
    const ObLSID &ls_id,
    const uint64_t ls_epoch);

  // for ls replica gc
  int ss_delete_tablet_current_version(
    const ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const uint64_t ls_epoch)
  { return ss_delete_tablet_current_version_(tablet_id, ls_id, ls_epoch); }
  int ss_batch_remove_ls_tablets(
    const share::ObLSID &ls_id, 
    const int64_t ls_epoch,
    const ObIArray<common::ObTabletID> &tablet_id_arr, 
    const ObIArray<ObMetaDiskAddr> &tablet_addr_arr,
    const bool delete_current_version);
  int ss_check_and_delete_tablet_current_version(
    const ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const uint64_t ls_epoch,
    const int64_t deleted_tablet_version,
    const int64_t deleted_tablet_transfer_seq,
    ObArenaAllocator &allocator);
  int ss_delete_tenant_ls_item(
    const share::ObLSID ls_id, const int64_t ls_epoch)
  {
    return delete_tenant_ls_item_(ls_id, ls_epoch);
  }
#endif
private:
  int write_prepare_create_ls_slog_(const ObLSMeta &ls_meta);
  int write_commit_create_ls_slog_(const share::ObLSID &ls_id);
  int write_abort_create_ls_slog_(const share::ObLSID &ls_id);
  int write_delete_ls_slog_(const share::ObLSID &ls_id);
  int write_update_ls_meta_slog_(const ObLSMeta &ls_meta);
  int write_update_dup_table_meta_slog_(
      const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_table_meta);
  int write_update_tablet_slog_(
      const share::ObLSID &ls_id, const common::ObTabletID &tablet_id, const ObMetaDiskAddr &tablet_addr);
  int write_remove_tablet_slog_(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id);
  int write_remove_tablets_slog_(
      const ObLSID &ls_id, const common::ObIArray<ObTabletID> &tablet_ids);
  int safe_batch_write_remove_tablets_slog_(
      const ObLSID &ls_id, const common::ObIArray<ObTabletID> &tablet_ids);

#ifdef OB_BUILD_SHARED_STORAGE
  int ss_prepare_create_ls_(const ObLSMeta &meta, int64_t &ls_epoch);
  int ss_commit_create_ls_(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int ss_abort_create_ls_(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int ss_delete_ls_(const share::ObLSID &ls_id, const int64_t ls_epoch);
  int ss_update_tablet_(
    const ObLSID ls_id, const int64_t ls_epoch,
    const common::ObTabletID &tablet_id, const ObMetaDiskAddr &tablet_addr);
  int ss_remove_tablet_(
    const ObLSID ls_id, const int64_t ls_epoch,
    const ObTabletHandle &tablet_handle);
  int ss_remove_tablets_();
  int create_tenant_ls_item_(const share::ObLSID ls_id, int64_t &ls_epoch);
  int update_tenant_ls_item_(
    const share::ObLSID ls_id, const int64_t ls_epoch, const ObLSItemStatus status);
  int delete_tenant_ls_item_(
    const share::ObLSID ls_id, const int64_t ls_epoch);
  int ss_write_tenant_super_block_(const ObTenantSuperBlock &tenant_super_block);
  int ss_write_ls_meta_(const int64_t ls_epoch, const ObLSMeta &meta);
  int ss_write_dup_table_meta_(
      const int64_t ls_epoch,
      const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &meta);
  int ss_write_active_tablet_array_(
    const ObLSID ls_id, const int64_t ls_epoch, const ObLSActiveTabletArray &active_tablet_array);
  int ss_write_pending_free_tablet_array_(
     const ObLSID ls_id, const int64_t ls_epoch, const ObLSPendingFreeTabletArray &pending_free_arry);
  int ss_delete_tablet_current_version_(
    const ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const uint64_t ls_epoch);
#endif 

private:
  struct PendingFreeTabletArrayKey
  {
    PendingFreeTabletArrayKey() : ls_id_(), ls_epoch_(0) {}
    PendingFreeTabletArrayKey(const share::ObLSID &ls_id, const int64_t ls_epoch)
      : ls_id_(ls_id), ls_epoch_(ls_epoch) {}
    uint64_t hash() const
    {
      return ls_id_.hash() ^ ls_epoch_;
    }

    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    bool operator ==(const PendingFreeTabletArrayKey &other) const
    {
      return other.ls_id_ == ls_id_ && other.ls_epoch_ == ls_epoch_;
    }
    bool operator !=(const PendingFreeTabletArrayKey &other) const { return !(other == *this); }
    bool operator <(const PendingFreeTabletArrayKey &other) const
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

    share::ObLSID ls_id_;
    int64_t ls_epoch_;
  };

  struct PendingFreeTabletArrayInfo
  {
    PendingFreeTabletArrayInfo()
      : lock_(), pending_free_tablet_arr_() {}

    lib::ObMutex lock_;
    ObLSPendingFreeTabletArray pending_free_tablet_arr_;
  };

  typedef common::hash::ObHashMap<
      PendingFreeTabletArrayKey,
      PendingFreeTabletArrayInfo*,
      common::hash::NoPthreadDefendMode> PendingFreeTabletArrayMap;

private:
  bool is_inited_;
  bool is_shared_storage_;
  storage::ObStorageLogger *slogger_;
  common::ObConcurrentFIFOAllocator allocator_;
  lib::ObMutex super_block_lock_; // protect tenant super block
  ObTenantCheckpointSlogHandler *ckpt_slog_handler_;
  
  lib::ObMutex peding_free_map_lock_; // pending_free_tablet_arr_map_

  PendingFreeTabletArrayMap pending_free_tablet_arr_map_;
  
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTALE_OB_STORAGE_META_PERSISTER_H_
