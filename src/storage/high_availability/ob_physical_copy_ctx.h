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

#ifndef OCEABASE_STORAGE_PHYSICAL_COPY_CTX_
#define OCEABASE_STORAGE_PHYSICAL_COPY_CTX_

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
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace storage
{

struct ObICopyTabletCtx
{
public:
  virtual int set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status) = 0;
  virtual int get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const = 0;
};


struct ObPhysicalCopyCtx final
{
  ObPhysicalCopyCtx();
  ~ObPhysicalCopyCtx();
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id),
               K_(ls_id),
               K_(tablet_id),
               K_(src_info),
               KP_(bandwidth_throttle),
               KP_(svr_rpc_proxy),
               K_(is_leader_restore),
               K_(restore_action),
               KP_(restore_base_info),
               KP_(meta_index_store),
               KP_(second_meta_index_store),
               KP_(ha_dag),
               KP_(sstable_index_builder),
               KP_(restore_macro_block_id_mgr),
               K_(need_sort_macro_meta),
               K_(need_check_seq),
               K_(ls_rebuild_seq),
               K_(table_key));


  common::SpinRWLock lock_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObStorageHASrcInfo src_info_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  bool is_leader_restore_;
  ObTabletRestoreAction::ACTION restore_action_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  ObStorageHADag *ha_dag_;
  ObSSTableIndexBuilder *sstable_index_builder_;
  ObRestoreMacroBlockIdMgr *restore_macro_block_id_mgr_;
  bool need_sort_macro_meta_; // not use
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  ObITable::TableKey table_key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalCopyCtx);
};


}
}
#endif