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

#ifndef MOCK_OB_PARTITION_COMPONENT_FACTORY_H_
#define MOCK_OB_PARTITION_COMPONENT_FACTORY_H_

namespace oceanbase {
namespace storage {

class MockObIPartitionComponentFactory : public ObIPartitionComponentFactory {
public:
  MOCK_METHOD1(get_partition, ObIPartitionGroup*(const uint64_t tenant_id));
  MOCK_METHOD1(get_replay_status, ObReplayStatus*(const uint64_t tenant_id));
  MOCK_METHOD0(get_ssstore, ObSSStore*());
  MOCK_METHOD1(get_sstable, ObSSTable*(const uint64_t tenant_id));
  MOCK_METHOD0(get_trans_service, transaction::ObTransService*());
  MOCK_METHOD0(get_clog_mgr, clog::ObICLogMgr*());
  MOCK_METHOD0(get_partition_service, ObPartitionService*());
  MOCK_METHOD1(get_partition_storage, ObPartitionStorage*(const uint64_t tenant_id));
  MOCK_METHOD0(get_replay_engine, replayengine::ObILogReplayEngine*());
  MOCK_METHOD0(get_election_mgr, election::ObElectionMgr*());
  MOCK_METHOD1(get_log_service, clog::ObIPartitionLogService*(const uint64_t tenant_id));
  MOCK_METHOD0(get_replay_engine_wrapper, clog::ObLogReplayEngineWrapper*());
  MOCK_METHOD0(get_warm_up_service, ObWarmUpService*());
  MOCK_METHOD0(get_migrate_ctx, ObMigrateCtx*());
  MOCK_METHOD0(get_base_data_meta_ob_reader, ObPhysicalBaseMetaReader*());
  MOCK_METHOD0(get_macro_block_ob_reader, ObPartitionMacroBlockObReader*());
  MOCK_METHOD0(get_part_group_migration_task, ObPartGroupMigrationTask*());
  MOCK_METHOD0(get_base_data_meta_restore_reader, ObPhysicalBaseMetaRestoreReader*());
  MOCK_METHOD0(get_macro_block_restore_reader, ObPartitionMacroBlockRestoreReader*());
  MOCK_METHOD0(get_meta_restore_reader, ObPartitionBaseDataMetaRestoreReader*());
  MOCK_METHOD0(get_old_rpc_base_data_meta_reader, ObPartitionBaseDataMetaObReader*());
  MOCK_METHOD0(get_pg_info_reader, ObPGPartitionBaseDataMetaObReader*());
  MOCK_METHOD0(get_pg_info_restore_reader, ObPGPartitionBaseDataMetaRestorReader*());
  MOCK_METHOD0(get_partition_group_meta_restore_reader, ObPartitionGroupMetaRestoreReader*());
  MOCK_METHOD0(get_base_data_meta_backup_reader, ObPhysicalBaseMetaBackupReader*());
  MOCK_METHOD0(get_macro_block_backup_reader, ObPartitionMacroBlockBackupReader*());
  MOCK_METHOD0(get_meta_backup_reader, ObPartitionBaseDataMetaBackupReader*());
  MOCK_METHOD0(get_pg_info_backup_reader, ObPGPartitionBaseDataMetaBackupReader*());
  MOCK_METHOD0(get_partition_group_meta_backup_reader, ObPartitionGroupMetaBackupReader*());
  MOCK_METHOD0(get_base_data_meta_restore_reader_v1, ObPhysicalBaseMetaRestoreReaderV1*());
  MOCK_METHOD0(get_macro_block_restore_reader_v1, ObPartitionMacroBlockRestoreReaderV1*());
  MOCK_METHOD0(get_macro_block_restore_reader_v2, ObPartitionMacroBlockRestoreReaderV2*());
  MOCK_METHOD0(get_meta_restore_reader_v1, ObPartitionBaseDataMetaRestoreReaderV1*());
  MOCK_METHOD0(get_pg_info_restore_reader_v1, ObPGPartitionBaseDataMetaRestoreReaderV1*());
  MOCK_METHOD0(get_partition_group_meta_restore_reader_v1, ObPartitionGroupMetaRestoreReaderV1*());
  MOCK_METHOD0(get_part_group_backup_task, ObPartGroupBackupTask*());
  MOCK_METHOD0(get_phy_restore_macro_index, ObPhyRestoreMacroIndexStore*());
  MOCK_METHOD0(get_partition_group_meta_restore_reader_v2, ObPartitionGroupMetaRestoreReaderV2*());
  MOCK_METHOD0(get_base_data_meta_restore_reader_v2, ObPhysicalBaseMetaRestoreReaderV2*());
  MOCK_METHOD0(get_pg_info_restore_reader_v2, ObPGPartitionBaseDataMetaRestoreReaderV2*());
  MOCK_METHOD0(get_phy_restore_macro_index_v2, ObPhyRestoreMacroIndexStoreV2*());

  MOCK_METHOD1(free, void(ObIPartitionGroup* partition));
  MOCK_METHOD1(free, void(ObReplayStatus* status));
  MOCK_METHOD1(free, void(ObSSStore* store));
  MOCK_METHOD1(free, void(ObSSTable* sstable));
  MOCK_METHOD1(free, void(transaction::ObTransService* txs));
  MOCK_METHOD1(free, void(clog::ObICLogMgr* clog_mgr));
  MOCK_METHOD1(free, void(ObPartitionService* ptt_service));
  MOCK_METHOD1(free, void(ObPartitionStorage* ptt_storage));
  MOCK_METHOD1(free, void(replayengine::ObILogReplayEngine* rp_eg));
  MOCK_METHOD1(free, void(election::ObIElectionMgr* election_mgr));
  MOCK_METHOD1(free, void(clog::ObIPartitionLogService* log_service));
  MOCK_METHOD1(free, void(clog::ObLogReplayEngineWrapper* rp_eg));
  MOCK_METHOD1(free, void(ObWarmUpService* warm_up_service));
  MOCK_METHOD1(free, void(ObMigrateCtx* migrate_ctx));
  MOCK_METHOD1(free, void(ObIPhysicalBaseMetaReader* reader));
  MOCK_METHOD1(free, void(ObIPartitionMacroBlockReader* reader));
  MOCK_METHOD1(free, void(ObPartGroupMigrationTask*& task));
  MOCK_METHOD1(free, void(ObPartitionBaseDataMetaRestoreReader* reader));
  MOCK_METHOD1(free, void(ObPartitionBaseDataMetaObReader* reader));
  MOCK_METHOD1(free, void(ObPGPartitionBaseDataMetaObReader* reader));
  MOCK_METHOD1(free, void(ObIPGPartitionBaseDataMetaObReader* reader));
  MOCK_METHOD1(free, void(ObPGPartitionBaseDataMetaRestorReader* reader));
  MOCK_METHOD1(free, void(ObPartitionGroupMetaRestoreReader* reader));
  MOCK_METHOD1(free, void(ObPhysicalBaseMetaBackupReader* reader));
  MOCK_METHOD1(free, void(ObPartitionMacroBlockBackupReader* reader));
  MOCK_METHOD1(free, void(ObPartitionBaseDataMetaBackupReader* reader));
  MOCK_METHOD1(free, void(ObPGPartitionBaseDataMetaBackupReader* reader));
  MOCK_METHOD1(free, void(ObPartitionGroupMetaBackupReader* reader));
  MOCK_METHOD1(free, void(ObPhysicalBaseMetaRestoreReaderV1* reader));
  MOCK_METHOD1(free, void(ObPartitionMacroBlockRestoreReaderV2* reader));
  MOCK_METHOD1(free, void(ObPartitionBaseDataMetaRestoreReaderV1* reader));
  MOCK_METHOD1(free, void(ObPGPartitionBaseDataMetaRestoreReaderV1* reader));
  MOCK_METHOD1(free, void(ObPartitionGroupMetaRestoreReaderV1* reader));
  MOCK_METHOD1(free, void(ObIPartitionGroupMetaRestoreReader* reader));
  MOCK_METHOD1(free, void(ObPartGroupBackupTask*& task));
  MOCK_METHOD1(free, void(ObPartGroupTask*& task));
  MOCK_METHOD1(free, void(ObPartitionGroupMetaRestoreReaderV2* reader));
  MOCK_METHOD1(free, void(ObPGPartitionBaseDataMetaRestoreReaderV2* reader));
  MOCK_METHOD1(free, void(ObIPhyRestoreMacroIndexStore* macro_index));
};

}  // namespace storage
}  // namespace oceanbase

#endif
