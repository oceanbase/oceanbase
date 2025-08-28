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
#include "ob_storage_ha_reader.h"
#include "storage/blocksstable/ob_sstable.h"
#include "ob_storage_restore_struct.h"
#include "ob_storage_ha_dag.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace storage
{
class ObCopyTabletRecordExtraInfo final
{
public:
  ObCopyTabletRecordExtraInfo();
  ~ObCopyTabletRecordExtraInfo(); 
  void reset();
public:
  OB_INLINE void add_cost_time_ms(const int64_t &time_cost_ms) { ATOMIC_FAA(&cost_time_ms_, time_cost_ms); }
  OB_INLINE void add_total_data_size(const int64_t &total_data_size) { ATOMIC_FAA(&total_data_size_, total_data_size); }
  OB_INLINE void add_write_data_size(const int64_t &write_data_size) { ATOMIC_FAA(&write_data_size_, write_data_size); }
  OB_INLINE void inc_major_count() { ATOMIC_INC(&major_count_); }
  OB_INLINE void add_macro_count(const int64_t &macro_count) { ATOMIC_FAA(&macro_count_, macro_count); }
  OB_INLINE void add_major_macro_count(const int64_t &major_macro_count) { ATOMIC_FAA(&major_macro_count_, major_macro_count); }
  OB_INLINE void add_reuse_macro_count(const int64_t &reuse_macro_count) { ATOMIC_FAA(&reuse_macro_count_, reuse_macro_count); }
  OB_INLINE void set_restore_action(const ObTabletRestoreAction::ACTION &action) { ATOMIC_SET(&restore_action_, action); }
  OB_INLINE int get_major_count() const { return ATOMIC_LOAD(&major_count_); }
  // not atomic, but only called when major sstable copy finish, which is sequential
  int update_max_reuse_mgr_size(ObMacroBlockReuseMgr *reuse_mgr);

  TO_STRING_KV(K_(cost_time_ms), K_(total_data_size), K_(write_data_size), K_(major_count), 
      K_(macro_count), K_(major_macro_count), K_(reuse_macro_count), K_(max_reuse_mgr_size), 
      "restore_action", ObTabletRestoreAction::get_action_str(restore_action_)); 
private:
  // The following 3 member variables are updated when writer of physical copy task finish
  // time cost of tablet copy (fully migration / restore of a single tablet)
  int64_t cost_time_ms_; 
  // total data size reading from copy source (Byte) 
  int64_t total_data_size_; 
  // data size writing to dst (Byte) (only count the data size of new macro block)
  int64_t write_data_size_; 

  // The following 5 member variables are updated when sstable copy finish
  // number of major sstable
  int64_t major_count_;
  // number of all macro block
  int64_t macro_count_;
  // number of major macro block
  int64_t major_macro_count_;
  // number of the macro block that is reused
  int64_t reuse_macro_count_;
  // max reuse mgr size
  int64_t max_reuse_mgr_size_;

  // The following 1 member variable are updated when tablet copy finish 
  // restore action (when migration, it is RESTORE_NONE) 
  ObTabletRestoreAction::ACTION restore_action_;
};

struct ObICopyTabletCtx
{
public:
  virtual int set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status) = 0;
  virtual int get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const = 0;
  virtual int get_copy_tablet_record_extra_info(ObCopyTabletRecordExtraInfo *&extra_info) = 0;
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
               K_(table_key),
               KP_(macro_block_reuse_mgr), 
               K_(total_macro_count), 
               K_(reuse_macro_count), 
               KPC_(extra_info));


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
  ObMacroBlockReuseMgr *macro_block_reuse_mgr_;
  int total_macro_count_; // total macro block count of single sstable
  int reuse_macro_count_; // reuse macro block count of single sstable
  ObCopyTabletRecordExtraInfo *extra_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalCopyCtx);
};


}
}
#endif