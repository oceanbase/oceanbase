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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_COMPONENT_FACTORY_
#define OCEANBASE_STORAGE_OB_PARTITION_COMPONENT_FACTORY_

#include "lib/allocator/ob_malloc.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "storage/ob_i_partition_component_factory.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_table_mgr.h"

namespace oceanbase {
namespace storage {
class ObSSStore;

class ObPartitionComponentFactory : public ObIPartitionComponentFactory {
public:
  ObPartitionComponentFactory()
  {}
  virtual ~ObPartitionComponentFactory()
  {}

  virtual ObIPartitionGroup* get_partition(const uint64_t tenant_id);
  virtual memtable::ObMemtable* get_memtable(const uint64_t tenant_id);
  virtual ObSSTable* get_sstable(const uint64_t tenant_id);
  virtual transaction::ObTransService* get_trans_service();
  virtual clog::ObICLogMgr* get_clog_mgr();
  virtual ObPartitionService* get_partition_service();
  virtual ObPartitionStorage* get_partition_storage(const uint64_t tenant_id);
  virtual replayengine::ObILogReplayEngine* get_replay_engine();
  virtual election::ObIElectionMgr* get_election_mgr();
  virtual clog::ObIPartitionLogService* get_log_service(const uint64_t tenant_id);
  virtual clog::ObLogReplayEngineWrapper* get_replay_engine_wrapper();
  virtual ObWarmUpService* get_warm_up_service();
  virtual ObReplayStatus* get_replay_status(const uint64_t tenant_id);

  // virtual ObMinorFreeze *get_minor_freeze();
  virtual ObMigrateCtx* get_migrate_ctx();
  virtual ObPhysicalBaseMetaReader* get_base_data_meta_ob_reader();
  virtual ObPartitionMacroBlockObReader* get_macro_block_ob_reader();
  virtual ObPartGroupMigrationTask* get_part_group_migration_task();
  virtual ObPhysicalBaseMetaRestoreReader* get_base_data_meta_restore_reader();
  virtual ObPartitionMacroBlockRestoreReader* get_macro_block_restore_reader();
  virtual ObPartitionBaseDataMetaRestoreReader* get_meta_restore_reader();
  virtual ObPartitionBaseDataMetaObReader* get_old_rpc_base_data_meta_reader();
  virtual ObPGPartitionBaseDataMetaObReader* get_pg_info_reader();
  virtual ObPGPartitionBaseDataMetaRestorReader* get_pg_info_restore_reader();
  virtual ObPartitionGroupMetaRestoreReader* get_partition_group_meta_restore_reader();
  virtual ObPartGroupBackupTask* get_part_group_backup_task();

  virtual ObPhysicalBaseMetaBackupReader* get_base_data_meta_backup_reader();
  virtual ObPartitionMacroBlockBackupReader* get_macro_block_backup_reader();
  virtual ObPartitionBaseDataMetaBackupReader* get_meta_backup_reader();
  virtual ObPGPartitionBaseDataMetaBackupReader* get_pg_info_backup_reader();
  virtual ObPartitionGroupMetaBackupReader* get_partition_group_meta_backup_reader();

  // for new physical restore
  virtual ObPhysicalBaseMetaRestoreReaderV1* get_base_data_meta_restore_reader_v1();
  virtual ObPartitionMacroBlockRestoreReaderV1* get_macro_block_restore_reader_v1();
  virtual ObPartitionMacroBlockRestoreReaderV2* get_macro_block_restore_reader_v2();
  virtual ObPartitionBaseDataMetaRestoreReaderV1* get_meta_restore_reader_v1();
  virtual ObPGPartitionBaseDataMetaRestoreReaderV1* get_pg_info_restore_reader_v1();
  virtual ObPartitionGroupMetaRestoreReaderV1* get_partition_group_meta_restore_reader_v1();
  virtual ObPhyRestoreMacroIndexStore* get_phy_restore_macro_index();

  // for 3.1 and later version physical restore
  virtual ObPartitionGroupMetaRestoreReaderV2* get_partition_group_meta_restore_reader_v2();
  virtual ObPhysicalBaseMetaRestoreReaderV2* get_base_data_meta_restore_reader_v2();
  virtual ObPGPartitionBaseDataMetaRestoreReaderV2* get_pg_info_restore_reader_v2();
  virtual ObPhyRestoreMacroIndexStoreV2* get_phy_restore_macro_index_v2();

  virtual void free(ObIPartitionGroup* partition);
  virtual void free(ObSSTable* sstable);
  virtual void free(memtable::ObMemtable* memtable);
  virtual void free(transaction::ObTransService* txs);
  virtual void free(clog::ObICLogMgr* clog_mgr);
  virtual void free(ObPartitionService* ptt_service);
  virtual void free(ObPartitionStorage* ptt_storage);
  virtual void free(replayengine::ObILogReplayEngine* rp_eg);
  virtual void free(election::ObIElectionMgr* election_mgr);
  virtual void free(clog::ObIPartitionLogService* log_service);
  virtual void free(clog::ObLogReplayEngineWrapper* rp_eg);
  virtual void free(ObWarmUpService* warm_up_service);
  virtual void free(ObReplayStatus* replay_status);
  // virtual void free(ObIPSFreezeCb *minor_freeze);
  virtual void free(ObMigrateCtx* ctx);
  virtual void free(ObIPhysicalBaseMetaReader* reader);
  virtual void free(ObIPartitionMacroBlockReader* reader);
  virtual void free(ObPartGroupMigrationTask*& task);
  virtual void free(ObPartitionBaseDataMetaRestoreReader* reader);
  virtual void free(ObPartitionBaseDataMetaObReader* reader);
  virtual void free(ObPGPartitionBaseDataMetaObReader* reader);
  virtual void free(ObPGPartitionBaseDataMetaRestorReader* reader);
  virtual void free(ObIPGPartitionBaseDataMetaObReader* reader);
  virtual void free(ObPartitionGroupMetaRestoreReader* reader);
  virtual void free(ObPartGroupBackupTask*& task);
  virtual void free(ObPartGroupTask*& task);

  virtual void free(ObPhysicalBaseMetaBackupReader* reader);
  virtual void free(ObPartitionMacroBlockBackupReader* reader);
  virtual void free(ObPartitionBaseDataMetaBackupReader* reader);
  virtual void free(ObPGPartitionBaseDataMetaBackupReader* reader);
  virtual void free(ObPartitionGroupMetaBackupReader* reader);

  // for new physical restore
  virtual void free(ObPhysicalBaseMetaRestoreReaderV1* reader);
  virtual void free(ObPartitionMacroBlockRestoreReaderV2* reader);
  virtual void free(ObPartitionBaseDataMetaRestoreReaderV1* reader);
  virtual void free(ObPGPartitionBaseDataMetaRestoreReaderV1* reader);
  virtual void free(ObPartitionGroupMetaRestoreReaderV1* reader);
  virtual void free(ObIPartitionGroupMetaRestoreReader* reader);

  // for 3.1 and later version physical restore
  virtual void free(ObPartitionGroupMetaRestoreReaderV2* reader);
  virtual void free(ObPhysicalBaseMetaRestoreReaderV2* reader);
  virtual void free(ObPGPartitionBaseDataMetaRestoreReaderV2* reader);
  virtual void free(ObIPhyRestoreMacroIndexStore* macro_index);

protected:
  template <class IT>
  void component_free(IT* component)
  {
    if (OB_LIKELY(NULL != component)) {
      op_free(component);
      component = NULL;
    }
  }
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_PARTITION_COMPONENT_FACTORY_
