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

#ifndef OCEABASE_STORAGE_TABLET_COPY_FINISH_TASK_
#define OCEABASE_STORAGE_TABLET_COPY_FINISH_TASK_

#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"
#include "ob_storage_ha_struct.h"
#include "ob_storage_ha_macro_block_writer.h"
#include "ob_storage_ha_reader.h"
#include "storage/blocksstable/ob_sstable.h"
#include "ob_storage_restore_struct.h"
#include "ob_storage_ha_dag.h"
#include "ob_physical_copy_ctx.h"

namespace oceanbase
{
namespace storage
{


class ObTabletCopyFinishTask final : public share::ObITask
{
public:
  ObTabletCopyFinishTask();
  virtual ~ObTabletCopyFinishTask();
  int init(
      const common::ObTabletID &tablet_id,
      ObLS *ls,
      observer::ObIMetaReport *reporter,
      const ObTabletRestoreAction::ACTION &restore_action,
      const ObMigrationTabletParam *src_tablet_meta,
      ObICopyTabletCtx *copy_tablet_ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletCopyFinishTask"), KP(this));
  int add_sstable(ObTableHandleV2 &table_handle);
  int add_sstable(ObTableHandleV2 &table_handle, const int64_t last_meta_macro_seq);
  int add_shared_sstable(const ObITable::TableKey &table_key);
  int get_sstable(
      const ObITable::TableKey &table_key,
      ObTableHandleV2 &table_handle);
  common::ObArenaAllocator &get_allocator() { return arena_allocator_; }
  int set_tablet_status(const ObCopyTabletStatus::STATUS &status);
  int get_tablet_status(ObCopyTabletStatus::STATUS &status);
  int get_restore_action(ObTabletRestoreAction::ACTION &restore_action);

  const ObMigrationTabletParam *get_src_tablet_meta() const { return src_tablet_meta_; }
private:
  int create_new_table_store_with_major_();
  int create_new_table_store_with_minor_();
  int trim_tablet_();

  int check_finish_copy_tablet_data_valid_();
  int get_tables_handle_ptr_(
      const ObITable::TableKey &table_key,
      ObTablesHandleArray *&table_handle_ptr);
  int check_tablet_valid_();
  int check_shared_table_type_();
  int deal_with_shared_majors_();
  int deal_with_no_shared_majors_();
  int check_major_valid_();
  int check_restore_major_valid_(
      const common::ObIArray<ObITable::TableKey> &shared_table_key_array,
      const ObTablesHandleArray &major_tables_handle);
  int check_log_replay_to_mds_sstable_end_scn_();

private:
  bool is_inited_;
  common::SpinRWLock lock_;
  common::ObTabletID tablet_id_;
  ObLS *ls_;
  observer::ObIMetaReport *reporter_;
  ObStorageHADag *ha_dag_;
  common::ObArenaAllocator arena_allocator_;
  ObTablesHandleArray minor_tables_handle_;
  ObTablesHandleArray ddl_tables_handle_;
  ObTablesHandleArray major_tables_handle_;
  ObTablesHandleArray mds_tables_handle_;
  ObTabletRestoreAction::ACTION restore_action_;
  const ObMigrationTabletParam *src_tablet_meta_;
  ObICopyTabletCtx *copy_tablet_ctx_;
  common::ObArray<ObITable::TableKey> shared_table_key_array_;
  common::ObArray<std::pair<ObITable::TableKey, int64_t>> last_meta_seq_array_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletCopyFinishTask);
};

}
}

#endif
