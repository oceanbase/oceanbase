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

#ifndef OCEANBASE_STORAGE_OB_I_PARTITION_COMPONENT_FACTORY_
#define OCEANBASE_STORAGE_OB_I_PARTITION_COMPONENT_FACTORY_

namespace oceanbase {
namespace clog {
class ObICLogMgr;
class ObIPartitionLogService;
class ObLogReplayEngineWrapper;
}  // namespace clog
namespace transaction {
class ObTransService;
}
namespace replayengine {
class ObILogReplayEngine;
}
namespace election {
class ObIElectionMgr;
}
namespace memtable {
class ObIMemtable;
}
namespace storage {
class ObIPartitionGroup;
class ObPartitionService;
class ObPartitionStorage;
class ObSSTable;
class ObMinorFreeze;
class ObIPSFreezeCb;
class ObWarmUpService;
class ObPartitionBaseDataInfo;
class ObMigrateCtx;
class ObPartitionMacroBlockObReader;
class ObPartitionMacroBlockOfsReader;
class ObPartitionBaseDataMetaRestoreReader;
class ObPartitionMacroBlockRestoreReader;
class ObIPhysicalBaseMetaReader;
class ObIPartitionMacroBlockReader;
class ObPartGroupMigrationTask;
class ObSSStore;
class ObITable;
class ObPhysicalBaseMetaReader;
class ObPhysicalBaseMetaRestoreReader;
class ObReplayStatus;
class ObPartitionBaseDataMetaObReader;
class ObIPGPartitionBaseDataMetaObReader;
class ObPGPartitionBaseDataMetaObReader;
class ObPGPartitionBaseDataMetaRestorReader;
class ObPartitionGroupMetaRestoreReader;
class ObPartitionGroupMetaBackupReader;
class ObPhysicalBaseMetaBackupReader;
class ObPartitionMacroBlockBackupReader;
class ObPartitionBaseDataMetaBackupReader;
class ObPGPartitionBaseDataMetaBackupReader;
class ObPartitionGroupMetaBackupReader;
class ObPhysicalBaseMetaRestoreReaderV1;
class ObPartitionMacroBlockRestoreReaderV1;
class ObPartitionMacroBlockRestoreReaderV2;
class ObPartitionBaseDataMetaRestoreReaderV1;
class ObPGPartitionBaseDataMetaRestoreReaderV1;
class ObPartitionGroupMetaRestoreReaderV1;
class ObIPartitionGroupMetaRestoreReader;
class ObPartGroupBackupTask;
class ObPartGroupTask;
class ObPartitionGroupMetaRestoreReaderV2;
class ObPhysicalBaseMetaRestoreReaderV2;
class ObPGPartitionBaseDataMetaRestoreReaderV2;
class ObIPhyRestoreMacroIndexStore;
class ObPhyRestoreMacroIndexStore;
class ObPhyRestoreMacroIndexStoreV2;

class ObIPartitionComponentFactory {
public:
  virtual ~ObIPartitionComponentFactory()
  {}
  virtual ObIPartitionGroup* get_partition(const uint64_t tenant_id) = 0;
  virtual ObSSTable* get_sstable(const uint64_t tenant_id) = 0;
  virtual transaction::ObTransService* get_trans_service() = 0;
  virtual clog::ObICLogMgr* get_clog_mgr() = 0;
  virtual ObPartitionService* get_partition_service() = 0;
  virtual ObPartitionStorage* get_partition_storage(const uint64_t tenant_id) = 0;
  virtual replayengine::ObILogReplayEngine* get_replay_engine() = 0;
  virtual election::ObIElectionMgr* get_election_mgr() = 0;
  virtual clog::ObIPartitionLogService* get_log_service(const uint64_t tenant_id) = 0;
  virtual clog::ObLogReplayEngineWrapper* get_replay_engine_wrapper() = 0;
  virtual ObWarmUpService* get_warm_up_service() = 0;
  virtual ObReplayStatus* get_replay_status(const uint64_t tenant_id) = 0;
  // virtual ObMinorFreeze *get_minor_freeze() = 0;
  virtual ObMigrateCtx* get_migrate_ctx() = 0;
  virtual ObPhysicalBaseMetaReader* get_base_data_meta_ob_reader() = 0;
  virtual ObPartitionMacroBlockObReader* get_macro_block_ob_reader() = 0;
  virtual ObPartGroupMigrationTask* get_part_group_migration_task() = 0;
  virtual ObPhysicalBaseMetaRestoreReader* get_base_data_meta_restore_reader() = 0;
  virtual ObPartitionMacroBlockRestoreReader* get_macro_block_restore_reader() = 0;
  virtual ObPartitionBaseDataMetaRestoreReader* get_meta_restore_reader() = 0;
  virtual ObPartitionBaseDataMetaObReader* get_old_rpc_base_data_meta_reader() = 0;
  virtual ObPGPartitionBaseDataMetaObReader* get_pg_info_reader() = 0;
  virtual ObPGPartitionBaseDataMetaRestorReader* get_pg_info_restore_reader() = 0;
  virtual ObPartitionGroupMetaRestoreReader* get_partition_group_meta_restore_reader() = 0;
  virtual ObPartGroupBackupTask* get_part_group_backup_task() = 0;

  virtual ObPhysicalBaseMetaBackupReader* get_base_data_meta_backup_reader() = 0;
  virtual ObPartitionMacroBlockBackupReader* get_macro_block_backup_reader() = 0;
  virtual ObPartitionBaseDataMetaBackupReader* get_meta_backup_reader() = 0;
  virtual ObPGPartitionBaseDataMetaBackupReader* get_pg_info_backup_reader() = 0;
  virtual ObPartitionGroupMetaBackupReader* get_partition_group_meta_backup_reader() = 0;
  // for new physical restore
  virtual ObPhysicalBaseMetaRestoreReaderV1* get_base_data_meta_restore_reader_v1() = 0;
  virtual ObPartitionMacroBlockRestoreReaderV1* get_macro_block_restore_reader_v1() = 0;
  virtual ObPartitionMacroBlockRestoreReaderV2* get_macro_block_restore_reader_v2() = 0;
  virtual ObPartitionBaseDataMetaRestoreReaderV1* get_meta_restore_reader_v1() = 0;
  virtual ObPGPartitionBaseDataMetaRestoreReaderV1* get_pg_info_restore_reader_v1() = 0;
  virtual ObPartitionGroupMetaRestoreReaderV1* get_partition_group_meta_restore_reader_v1() = 0;
  virtual ObPhyRestoreMacroIndexStore* get_phy_restore_macro_index() = 0;

  // for 3.1 and later version physical restore
  virtual ObPartitionGroupMetaRestoreReaderV2* get_partition_group_meta_restore_reader_v2() = 0;
  virtual ObPhysicalBaseMetaRestoreReaderV2* get_base_data_meta_restore_reader_v2() = 0;
  virtual ObPGPartitionBaseDataMetaRestoreReaderV2* get_pg_info_restore_reader_v2() = 0;
  virtual ObPhyRestoreMacroIndexStoreV2* get_phy_restore_macro_index_v2() = 0;

  virtual void free(ObIPartitionGroup* partition) = 0;
  virtual void free(ObSSTable* sstable) = 0;
  virtual void free(transaction::ObTransService* txs) = 0;
  virtual void free(clog::ObICLogMgr* clog_mgr) = 0;
  virtual void free(ObPartitionService* ptt_service) = 0;
  virtual void free(ObPartitionStorage* ptt_storage) = 0;
  virtual void free(replayengine::ObILogReplayEngine* rp_eg) = 0;
  virtual void free(election::ObIElectionMgr* election_mgr) = 0;
  virtual void free(clog::ObIPartitionLogService* log_service) = 0;
  virtual void free(clog::ObLogReplayEngineWrapper* rp_eg) = 0;
  virtual void free(ObWarmUpService* warm_up_service) = 0;
  virtual void free(ObReplayStatus* replay_status) = 0;
  // virtual void free(ObIPSFreezeCb *minor_freeze) = 0;
  virtual void free(ObMigrateCtx* migrate_ctx) = 0;
  virtual void free(ObIPhysicalBaseMetaReader* reader) = 0;
  virtual void free(ObIPartitionMacroBlockReader* reader) = 0;
  virtual void free(ObPartGroupMigrationTask*& task) = 0;
  virtual void free(ObPartitionBaseDataMetaRestoreReader* reader) = 0;
  virtual void free(ObPartitionBaseDataMetaObReader* reader) = 0;
  virtual void free(ObPGPartitionBaseDataMetaObReader* reader) = 0;
  virtual void free(ObIPGPartitionBaseDataMetaObReader* reader) = 0;
  virtual void free(ObPGPartitionBaseDataMetaRestorReader* reader) = 0;
  virtual void free(ObPartitionGroupMetaRestoreReader* reader) = 0;
  virtual void free(ObPartGroupBackupTask*& task) = 0;
  virtual void free(ObPartGroupTask*& task) = 0;

  virtual void free(ObPhysicalBaseMetaBackupReader* reader) = 0;
  virtual void free(ObPartitionMacroBlockBackupReader* reader) = 0;
  virtual void free(ObPartitionBaseDataMetaBackupReader* reader) = 0;
  virtual void free(ObPGPartitionBaseDataMetaBackupReader* reader) = 0;
  virtual void free(ObPartitionGroupMetaBackupReader* reader) = 0;
  // for new physical restore
  virtual void free(ObPhysicalBaseMetaRestoreReaderV1* reader) = 0;
  virtual void free(ObPartitionMacroBlockRestoreReaderV2* reader) = 0;
  virtual void free(ObPartitionBaseDataMetaRestoreReaderV1* reader) = 0;
  virtual void free(ObPGPartitionBaseDataMetaRestoreReaderV1* reader) = 0;
  virtual void free(ObPartitionGroupMetaRestoreReaderV1* reader) = 0;
  virtual void free(ObIPartitionGroupMetaRestoreReader* reader) = 0;
  // for 3.1 and later version physical restore
  virtual void free(ObPartitionGroupMetaRestoreReaderV2* reader) = 0;
  virtual void free(ObPGPartitionBaseDataMetaRestoreReaderV2* reader) = 0;
  virtual void free(ObIPhyRestoreMacroIndexStore* macro_index) = 0;
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_I_PARTITION_COMPONENT_FACTORY_
