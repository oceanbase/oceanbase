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

#include "storage/ob_partition_component_factory.h"
#include "storage/ob_partition_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/replayengine/ob_log_replay_engine.h"
#include "clog/ob_log_replay_engine_wrapper.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_partition_base_data_ob_reader.h"
#include "storage/ob_partition_base_data_restore_reader.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_replay_status.h"
#include "storage/ob_partition_group.h"
#include "storage/ob_partition_base_data_backup.h"
#include "storage/ob_migrate_macro_block_writer.h"
#include "storage/ob_partition_base_data_backup.h"
#include "storage/ob_partition_base_data_ob_reader.h"
#include "storage/ob_partition_backup.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/allocator/ob_pcounter.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::clog;
using namespace oceanbase::transaction;
using namespace oceanbase::memtable;
using namespace oceanbase::replayengine;
using namespace oceanbase::election;
namespace storage {
ObMemtable* ObPartitionComponentFactory::get_memtable(const uint64_t tenant_id)
{
  ObMemtable* obj = ObMemtableFactory::alloc(tenant_id);
  if (NULL != obj) {
    PC_ADD(MT, 1);
  }
  return obj;
}

ObSSTable* ObPartitionComponentFactory::get_sstable(const uint64_t tenant_id)
{
  ObMemAttr memattr(tenant_id, ObModIds::OB_SSTABLE, ObCtxIds::SSTABLE_CTX_ID);
  return OB_NEW_ALIGN32(ObSSTable, memattr);
}

ObTransService* ObPartitionComponentFactory::get_trans_service()
{
  return op_alloc(ObTransService);
}

ObICLogMgr* ObPartitionComponentFactory::get_clog_mgr()
{
  return op_alloc(ObCLogMgr);
}

ObPartitionService* ObPartitionComponentFactory::get_partition_service()
{
  return op_alloc(ObPartitionService);
}

ObPartitionStorage* ObPartitionComponentFactory::get_partition_storage(const uint64_t tenant_id)
{
  ObMemAttr memattr(tenant_id, ObModIds::OB_PARTITION_STORAGE, ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID);
  return OB_NEW_ALIGN32(ObPartitionStorage, memattr);
}

ObIPartitionGroup* ObPartitionComponentFactory::get_partition(const uint64_t tenant_id)
{
  ObMemAttr memattr(tenant_id, ObModIds::OB_PARTITION, ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID);
  ObIPartitionGroup* obj = OB_NEW_ALIGN32(ObPartitionGroup, memattr);
  if (!OB_ISNULL(obj)) {
    PC_ADD(PT, 1);
  }
  return obj;
}

ObReplayStatus* ObPartitionComponentFactory::get_replay_status(const uint64_t tenant_id)
{
  ObMemAttr memattr(tenant_id, ObModIds::OB_REPLAY_STATUS, ObCtxIds::REPLAY_STATUS_CTX_ID);
  ObReplayStatus* obj = OB_NEW_ALIGN32(ObReplayStatus, memattr);
  if (!OB_ISNULL(obj)) {
    PC_ADD(RT, 1);
  }
  return obj;
}
ObILogReplayEngine* ObPartitionComponentFactory::get_replay_engine()
{
  return op_alloc(ObLogReplayEngine);
}

ObIElectionMgr* ObPartitionComponentFactory::get_election_mgr()
{
  return op_alloc(ObElectionMgr);
}

ObIPartitionLogService* ObPartitionComponentFactory::get_log_service(const uint64_t tenant_id)
{
  common::ObILogAllocator* alloc_mgr = NULL;
  ObIPartitionLogService* ret_ptr = NULL;
  if (OB_SUCCESS == TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr)) {
    ret_ptr = alloc_mgr->alloc_partition_log_service();
  }
  return ret_ptr;
}

ObLogReplayEngineWrapper* ObPartitionComponentFactory::get_replay_engine_wrapper()
{
  return op_alloc(ObLogReplayEngineWrapper);
}

ObWarmUpService* ObPartitionComponentFactory::get_warm_up_service()
{
  return op_alloc(ObWarmUpService);
}

// ObMinorFreeze *ObPartitionComponentFactory::get_minor_freeze()
// {
//   return op_alloc(ObMinorFreeze);
// }

ObMigrateCtx* ObPartitionComponentFactory::get_migrate_ctx()
{
  return op_alloc(ObMigrateCtx);
}

ObPhysicalBaseMetaReader* ObPartitionComponentFactory::get_base_data_meta_ob_reader()
{
  return op_alloc(ObPhysicalBaseMetaReader);
}

ObPartitionMacroBlockObReader* ObPartitionComponentFactory::get_macro_block_ob_reader()
{
  return op_alloc(ObPartitionMacroBlockObReader);
}

ObPartGroupMigrationTask* ObPartitionComponentFactory::get_part_group_migration_task()
{
  return op_alloc(ObPartGroupMigrationTask);
}

ObPhysicalBaseMetaRestoreReader* ObPartitionComponentFactory::get_base_data_meta_restore_reader()
{
  return op_alloc(ObPhysicalBaseMetaRestoreReader);
}

ObPartitionMacroBlockRestoreReader* ObPartitionComponentFactory::get_macro_block_restore_reader()
{
  return op_alloc(ObPartitionMacroBlockRestoreReader);
}

ObPartitionBaseDataMetaRestoreReader* ObPartitionComponentFactory::get_meta_restore_reader()
{
  return op_alloc(ObPartitionBaseDataMetaRestoreReader);
}

ObPartitionBaseDataMetaObReader* ObPartitionComponentFactory::get_old_rpc_base_data_meta_reader()
{
  return op_alloc(ObPartitionBaseDataMetaObReader);
}

ObPGPartitionBaseDataMetaObReader* ObPartitionComponentFactory::get_pg_info_reader()
{
  return op_alloc(ObPGPartitionBaseDataMetaObReader);
}

ObPGPartitionBaseDataMetaRestorReader* ObPartitionComponentFactory::get_pg_info_restore_reader()
{
  return op_alloc(ObPGPartitionBaseDataMetaRestorReader);
}

ObPartitionGroupMetaRestoreReader* ObPartitionComponentFactory::get_partition_group_meta_restore_reader()
{
  return op_alloc(ObPartitionGroupMetaRestoreReader);
}

ObPartGroupBackupTask* ObPartitionComponentFactory::get_part_group_backup_task()
{
  return op_alloc(ObPartGroupBackupTask);
}

ObPhysicalBaseMetaBackupReader* ObPartitionComponentFactory::get_base_data_meta_backup_reader()
{
  return op_alloc(ObPhysicalBaseMetaBackupReader);
}

ObPartitionMacroBlockBackupReader* ObPartitionComponentFactory::get_macro_block_backup_reader()
{
  return op_alloc(ObPartitionMacroBlockBackupReader);
}

ObPartitionBaseDataMetaBackupReader* ObPartitionComponentFactory::get_meta_backup_reader()
{
  return op_alloc(ObPartitionBaseDataMetaBackupReader);
}

ObPGPartitionBaseDataMetaBackupReader* ObPartitionComponentFactory::get_pg_info_backup_reader()
{
  return op_alloc(ObPGPartitionBaseDataMetaBackupReader);
}

ObPartitionGroupMetaBackupReader* ObPartitionComponentFactory::get_partition_group_meta_backup_reader()
{
  return op_alloc(ObPartitionGroupMetaBackupReader);
}

ObPhysicalBaseMetaRestoreReaderV1* ObPartitionComponentFactory::get_base_data_meta_restore_reader_v1()
{
  return op_alloc(ObPhysicalBaseMetaRestoreReaderV1);
}

ObPartitionMacroBlockRestoreReaderV1* ObPartitionComponentFactory::get_macro_block_restore_reader_v1()
{
  return op_alloc(ObPartitionMacroBlockRestoreReaderV1);
}

ObPartitionMacroBlockRestoreReaderV2* ObPartitionComponentFactory::get_macro_block_restore_reader_v2()
{
  return op_alloc(ObPartitionMacroBlockRestoreReaderV2);
}

ObPartitionBaseDataMetaRestoreReaderV1* ObPartitionComponentFactory::get_meta_restore_reader_v1()
{
  return op_alloc(ObPartitionBaseDataMetaRestoreReaderV1);
}

ObPGPartitionBaseDataMetaRestoreReaderV1* ObPartitionComponentFactory::get_pg_info_restore_reader_v1()
{
  return op_alloc(ObPGPartitionBaseDataMetaRestoreReaderV1);
}

ObPartitionGroupMetaRestoreReaderV1* ObPartitionComponentFactory::get_partition_group_meta_restore_reader_v1()
{
  return op_alloc(ObPartitionGroupMetaRestoreReaderV1);
}

ObPartitionGroupMetaRestoreReaderV2* ObPartitionComponentFactory::get_partition_group_meta_restore_reader_v2()
{
  return op_alloc(ObPartitionGroupMetaRestoreReaderV2);
}

ObPhysicalBaseMetaRestoreReaderV2* ObPartitionComponentFactory::get_base_data_meta_restore_reader_v2()
{
  return op_alloc(ObPhysicalBaseMetaRestoreReaderV2);
}

ObPGPartitionBaseDataMetaRestoreReaderV2* ObPartitionComponentFactory::get_pg_info_restore_reader_v2()
{
  return op_alloc(ObPGPartitionBaseDataMetaRestoreReaderV2);
}

ObPhyRestoreMacroIndexStore* ObPartitionComponentFactory::get_phy_restore_macro_index()
{
  return op_alloc(ObPhyRestoreMacroIndexStore);
}

ObPhyRestoreMacroIndexStoreV2* ObPartitionComponentFactory::get_phy_restore_macro_index_v2()
{
  return op_alloc(ObPhyRestoreMacroIndexStoreV2);
}

void ObPartitionComponentFactory::free(ObIPartitionGroup* partition)
{
  if (NULL != partition) {
    OB_DELETE_ALIGN32(ObIPartitionGroup, unused, partition);
    partition = NULL;
    PC_ADD(PT, -1);
  }
}

void ObPartitionComponentFactory::free(ObReplayStatus* replay_status)
{
  if (NULL != replay_status) {
    OB_DELETE_ALIGN32(ObReplayStatus, unused, replay_status);
    replay_status = NULL;
    PC_ADD(RT, -1);
  }
}
void ObPartitionComponentFactory::free(ObSSTable* sstable)
{
  if (NULL != sstable) {
    OB_DELETE_ALIGN32(ObSSTable, unused, sstable);
    sstable = NULL;
  }
}

void ObPartitionComponentFactory::free(memtable::ObMemtable* memtable)
{
  if (NULL != memtable) {
    PC_ADD(MT, -1);
    ObMemtableFactory::free(memtable);
    memtable = NULL;
  }
}

void ObPartitionComponentFactory::free(ObTransService* txs)
{
  op_free(txs);
  txs = NULL;
}

void ObPartitionComponentFactory::free(ObICLogMgr* clog_mgr)
{
  op_free(static_cast<ObCLogMgr*>(clog_mgr));
  clog_mgr = NULL;
}

void ObPartitionComponentFactory::free(ObPartitionService* ptt_service)
{
  op_free(ptt_service);
  ptt_service = NULL;
}

void ObPartitionComponentFactory::free(ObPartitionStorage* ptt_storage)
{
  if (NULL != ptt_storage) {
    OB_DELETE_ALIGN32(ObPartitionStorage, unused, ptt_storage);
    ptt_storage = NULL;
  }
}

void ObPartitionComponentFactory::free(ObILogReplayEngine* rp_eg)
{
  op_free(static_cast<ObLogReplayEngine*>(rp_eg));
  rp_eg = NULL;
}

void ObPartitionComponentFactory::free(ObIElectionMgr* election_mgr)
{
  op_free(static_cast<ObElectionMgr*>(election_mgr));
  election_mgr = NULL;
}

void ObPartitionComponentFactory::free(ObIPartitionLogService* log_service)
{
  if (NULL != log_service) {
    common::ob_slice_free_partition_log_service(log_service);
    log_service = NULL;
  }
}

void ObPartitionComponentFactory::free(ObLogReplayEngineWrapper* rp_eg)
{
  op_free(static_cast<ObLogReplayEngineWrapper*>(rp_eg));
  rp_eg = NULL;
}

void ObPartitionComponentFactory::free(ObWarmUpService* warm_up_service)
{
  op_free(warm_up_service);
  warm_up_service = NULL;
}

// void ObPartitionComponentFactory::free(ObIPSFreezeCb *minor_freeze)
// {
//   op_free(static_cast<ObMinorFreeze *>(minor_freeze));
//   minor_freeze = NULL;
// }

void ObPartitionComponentFactory::free(ObMigrateCtx* ctx)
{
  component_free(ctx);
}

void ObPartitionComponentFactory::free(ObIPhysicalBaseMetaReader* reader)
{
  if (NULL != reader) {
    if (ObIPhysicalBaseMetaReader::BASE_DATA_META_OB_READER == reader->get_type()) {
      component_free(static_cast<ObPhysicalBaseMetaReader*>(reader));
    } else if (ObIPhysicalBaseMetaReader::BASE_DATA_META_RESTORE_READER == reader->get_type()) {
      component_free(static_cast<ObPhysicalBaseMetaRestoreReader*>(reader));
    } else if (ObIPhysicalBaseMetaReader::BASE_DATA_META_OB_COMPAT_READER == reader->get_type()) {
      component_free(static_cast<ObPartitionBaseDataMetaObReader*>(reader));
    } else if (ObIPhysicalBaseMetaReader::BASE_DATA_META_RESTORE_READER_V1 == reader->get_type()) {
      component_free(static_cast<ObPhysicalBaseMetaRestoreReaderV1*>(reader));
    } else if (ObIPhysicalBaseMetaReader::BASE_DATA_META_RESTORE_READER_V2 == reader->get_type()) {
      component_free(static_cast<ObPhysicalBaseMetaRestoreReaderV2*>(reader));
    } else {
      STORAGE_LOG(ERROR, "unknown reader type", "type", reader->get_type());
    }
  }
}

void ObPartitionComponentFactory::free(ObIPartitionMacroBlockReader* reader)
{
  if (NULL != reader) {
    if (ObIPartitionMacroBlockReader::MACRO_BLOCK_OB_READER == reader->get_type()) {
      component_free(static_cast<ObPartitionMacroBlockObReader*>(reader));
    } else if (ObIPartitionMacroBlockReader::MACRO_BLOCK_RESTORE_READER == reader->get_type()) {
      component_free(static_cast<ObPartitionMacroBlockRestoreReader*>(reader));
    } else if (ObIPartitionMacroBlockReader::MACRO_BLOCK_BACKUP_READER == reader->get_type()) {
      component_free(static_cast<ObPartitionMacroBlockBackupReader*>(reader));
    } else if (ObIPartitionMacroBlockReader::MACRO_BLOCK_RESTORE_READER_V2 == reader->get_type()) {
      component_free(static_cast<ObPartitionMacroBlockRestoreReaderV2*>(reader));
    } else if (ObIPartitionMacroBlockReader::MACRO_BLOCK_RESTORE_READER_V1 == reader->get_type()) {
      component_free(static_cast<ObPartitionMacroBlockRestoreReaderV1*>(reader));
    } else {
      STORAGE_LOG(ERROR, "unknown reader type", "type", reader->get_type());
    }
  }
}

void ObPartitionComponentFactory::free(ObPartGroupMigrationTask*& task)
{
  component_free(task);
  task = NULL;
}

void ObPartitionComponentFactory::free(ObPartitionBaseDataMetaRestoreReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPartitionBaseDataMetaObReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPGPartitionBaseDataMetaObReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPGPartitionBaseDataMetaRestorReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObIPGPartitionBaseDataMetaObReader* reader)
{
  if (NULL != reader) {
    if (ObIPGPartitionBaseDataMetaObReader::BASE_DATA_META_OB_READER == reader->get_type()) {
      free(static_cast<ObPGPartitionBaseDataMetaObReader*>(reader));
    } else if (ObIPGPartitionBaseDataMetaObReader::BASE_DATA_META_OB_RESTORE_READER == reader->get_type()) {
      free(static_cast<ObPGPartitionBaseDataMetaRestorReader*>(reader));
    } else if (ObIPGPartitionBaseDataMetaObReader::BASE_DATA_META_OB_BACKUP_READER == reader->get_type()) {
      free(static_cast<ObPGPartitionBaseDataMetaBackupReader*>(reader));
    } else if (ObIPGPartitionBaseDataMetaObReader::BASE_DATA_META_OB_RESTORE_READER_V1 == reader->get_type()) {
      free(static_cast<ObPGPartitionBaseDataMetaRestoreReaderV1*>(reader));
    } else if (ObIPGPartitionBaseDataMetaObReader::BASE_DATA_META_OB_RESTORE_READER_V2 == reader->get_type()) {
      free(static_cast<ObPGPartitionBaseDataMetaRestoreReaderV2*>(reader));
    } else {
      STORAGE_LOG(ERROR, "unknown reader type", "type", reader->get_type());
    }
  }
}

void ObPartitionComponentFactory::free(ObPartitionGroupMetaRestoreReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPartGroupBackupTask*& task)
{
  component_free(task);
  task = NULL;
}

void ObPartitionComponentFactory::free(ObPartGroupTask*& task)
{
  if (NULL != task) {
    if (ObPartGroupTask::PART_GROUP_MIGATION_TASK == task->get_task_type()) {
      ObPartGroupMigrationTask* tmp_task = static_cast<ObPartGroupMigrationTask*>(task);
      free(tmp_task);
      task = NULL;
    } else if (ObPartGroupTask::PART_GROUP_BACKUP_TASK == task->get_task_type()) {
      ObPartGroupBackupTask* tmp_task = static_cast<ObPartGroupBackupTask*>(task);
      free(tmp_task);
      task = NULL;
    } else {
      STORAGE_LOG(ERROR, "unknown task type", "type", task->get_task_type());
    }
  }
}

void ObPartitionComponentFactory::free(ObPhysicalBaseMetaBackupReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPartitionMacroBlockBackupReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPartitionBaseDataMetaBackupReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPGPartitionBaseDataMetaBackupReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPartitionGroupMetaBackupReader* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPhysicalBaseMetaRestoreReaderV1* reader)
{
  component_free(reader);
}
void ObPartitionComponentFactory::free(ObPartitionMacroBlockRestoreReaderV2* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPartitionBaseDataMetaRestoreReaderV1* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPGPartitionBaseDataMetaRestoreReaderV1* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPartitionGroupMetaRestoreReaderV1* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObIPartitionGroupMetaRestoreReader* reader)
{
  if (NULL != reader) {
    if (ObIPartitionGroupMetaRestoreReader::PG_META_RESTORE_READER == reader->get_type()) {
      component_free(static_cast<ObPartitionGroupMetaRestoreReader*>(reader));
    } else if (ObIPartitionGroupMetaRestoreReader::PG_META_RESTORE_READER_V1 == reader->get_type()) {
      component_free(static_cast<ObPartitionGroupMetaRestoreReaderV1*>(reader));
    } else if (ObIPartitionGroupMetaRestoreReader::PG_META_RESTORE_READER_V2 == reader->get_type()) {
      component_free(static_cast<ObPartitionGroupMetaRestoreReaderV2*>(reader));
    } else {
      STORAGE_LOG(ERROR, "unknown reader type", "type", reader->get_type());
    }
  }
}

void ObPartitionComponentFactory::free(ObPartitionGroupMetaRestoreReaderV2* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPhysicalBaseMetaRestoreReaderV2* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObPGPartitionBaseDataMetaRestoreReaderV2* reader)
{
  component_free(reader);
}

void ObPartitionComponentFactory::free(ObIPhyRestoreMacroIndexStore* macro_index)
{
  if (NULL != macro_index) {
    if (ObIPhyRestoreMacroIndexStore::PHY_RESTORE_MACRO_INDEX_STORE_V1 == macro_index->get_type()) {
      component_free(static_cast<ObPhyRestoreMacroIndexStore*>(macro_index));
    } else if (ObIPhyRestoreMacroIndexStore::PHY_RESTORE_MACRO_INDEX_STORE_V2 == macro_index->get_type()) {
      component_free(static_cast<ObPhyRestoreMacroIndexStoreV2*>(macro_index));
    } else {
      STORAGE_LOG(ERROR, "unknown macro index type", "type", macro_index->get_type());
    }
  }
}

}  // namespace storage
}  // namespace oceanbase
