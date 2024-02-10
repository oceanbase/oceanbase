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

#ifndef OB_STORAGE_TENANT_META_SNAPSHOT_HANDLER_H_
#define OB_STORAGE_TENANT_META_SNAPSHOT_HANDLER_H_

#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/ob_super_block_struct.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/high_availability/ob_tablet_transfer_info.h"
#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_reader.h"
#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_writer.h"
#include "storage/slog/ob_storage_log_struct.h"
#include "storage/slog/ob_storage_log.h"

namespace oceanbase
{
namespace share
{
class ObTenantSnapshotID;
}
namespace observer
{
class ObStartupAccelTaskHandler;
}
namespace storage
{
class ObTenantStorageCheckpointWriter;
class ObTenantMetaSnapshotHandler
{
public:
  static const int64_t MAX_SLOG_BATCH_NUM = 30000; // almost 2MB
public:
  ObTenantMetaSnapshotHandler() {}
  ~ObTenantMetaSnapshotHandler() = default;
  DISALLOW_COPY_AND_ASSIGN(ObTenantMetaSnapshotHandler);

  // create snapshot
  static int create_tenant_snapshot(const ObTenantSnapshotID &snapshot_id);
  static int create_single_ls_snapshot(const ObTenantSnapshotID &snapshot_id,
                                       const ObLSID &ls_id,
                                       share::SCN &clog_max_scn);

  // delete snapshot
  static int delete_tenant_snapshot(const ObTenantSnapshotID &snapshot_id);
  static int delete_single_ls_snapshot(const ObTenantSnapshotID &snapshot_id, const ObLSID &ls_id);

  // clone snapshot
  static int get_ls_snapshot(
      const ObTenantSnapshotID &snapshot_id,
      const ObLSID &ls_id,
      blocksstable::MacroBlockId &tablet_meta_entry);
  static int create_all_tablet(observer::ObStartupAccelTaskHandler* startup_accel_handler,
                               const blocksstable::MacroBlockId &tablet_meta_entry);
  static int get_ls_meta_entry(const ObTenantSnapshotID &snapshot_id, blocksstable::MacroBlockId &ls_meta_entry);

  // recover snapshot for restart
  static int get_all_tenant_snapshot(ObIArray<ObTenantSnapshotID> &snapshot_ids);
  static int get_all_ls_snapshot(const ObTenantSnapshotID &snapshot_id, ObIArray<ObLSID> &ls_ids);

  // increase ref cnt for linked blocks
  static int inc_linked_block_ref(const ObIArray<blocksstable::MacroBlockId> &meta_block_list, bool &inc_success);

private:
  static int find_tablet_meta_entry(
      const blocksstable::MacroBlockId &ls_meta_entry,
      const ObLSID &ls_id,
      blocksstable::MacroBlockId &tablet_meta_entry);
  static int push_ls_snapshot(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len, ObIArray<ObLSID> &ls_ids);
  static int inc_all_linked_block_ref(
      ObTenantStorageCheckpointWriter &tenant_storage_meta_writer,
      bool &inc_ls_blocks_ref_succ,
      bool &inc_tablet_blocks_ref_succ);
  static void rollback_ref_cnt(
      const bool inc_ls_blocks_ref_succ,
      const bool inc_tablet_blocks_ref_succ,
      ObTenantStorageCheckpointWriter &tenant_storage_meta_writer);
  static void dec_meta_block_ref(const ObIArray<blocksstable::MacroBlockId> &meta_block_list);
  static int inner_delete_tablet_by_addrs(const ObIArray<ObMetaDiskAddr> &deleted_tablet_addrs);
  static int inner_delete_ls_snapshot(const blocksstable::MacroBlockId& tablet_meta_entry,
                                      ObIArray<ObMetaDiskAddr> &deleted_tablet_addrs,
                                      ObIArray<MacroBlockId> &tablet_meta_block_list);
  static int delete_ls_snapshot(
      const ObMetaDiskAddr &addr,
      const char *buf,
      const int64_t buf_len,
      ObIArray<ObMetaDiskAddr> &deleted_tablet_addrs,
      ObIArray<blocksstable::MacroBlockId> &tablet_meta_block_list);
  static int delete_tablet_snapshot(
      const ObMetaDiskAddr &addr,
      const char *buf,
      const int64_t buf_len,
      ObIArray<ObMetaDiskAddr> &deleted_tablet_addrs);
  static int batch_write_slog(
      const ObMetaDiskAddr &addr,
      const char *buf,
      const int64_t buf_len,
      ObIArray<ObUpdateTabletLog> &slog_array);
  static int batch_report_slog(const ObIArray<ObStorageLogParam> &param_arr);
  static int do_write_slog(ObIArray<ObUpdateTabletLog> &slog_arr);
};
}
}

#endif
