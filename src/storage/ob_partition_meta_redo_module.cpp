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

#define USING_LOG_PREFIX STORAGE

#include "ob_partition_meta_redo_module.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_macro_meta_replay_map.h"
#include "storage/ob_server_log.h"
#include "storage/ob_tenant_file_pg_meta_checkpoint_reader.h"

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace share::schema;

namespace storage {
ObPartitionMetaRedoModule::ObPartitionMetaRedoModule()
    : cp_fty_(nullptr),
      txs_(nullptr),
      rp_eg_(nullptr),
      schema_service_(nullptr),
      file_mgr_(nullptr),
      iter_allocator_(),
      structure_change_mutex_(),
      partition_map_(1 << 10),
      is_replay_old_(false),
      is_inited_(false)
{}

ObPartitionMetaRedoModule::~ObPartitionMetaRedoModule()
{
  destroy();
}

int ObPartitionMetaRedoModule::destroy()
{
  int ret = OB_SUCCESS;
  LOG_INFO("destroy ObPartitionMetaRedoModule");
  if (IS_INIT) {
    // PG memory MUST be release before others. Otherwise may core
    partition_map_.destroy();
    pg_mgr_.destroy();
    pg_index_.destroy();
    iter_allocator_.destroy();
    if (nullptr != rp_eg_) {
      // rp_eg_->destroy();
      cp_fty_->free(rp_eg_);
      rp_eg_ = nullptr;
    }
    if (nullptr != txs_) {
      // rp_eg_->destroy();
      cp_fty_->free(txs_);
      rp_eg_ = nullptr;
    }
    cp_fty_ = nullptr;
    schema_service_ = nullptr;
    file_mgr_ = nullptr;
    is_inited_ = false;
  }
  return ret;
}

int ObPartitionMetaRedoModule::init(
    ObPartitionComponentFactory* cp_fty, ObMultiVersionSchemaService* schema_service, ObBaseFileMgr* file_mgr)
{
  int ret = OB_SUCCESS;
  const int64_t ITER_ALLOC_TOTAL_LIMIT = 1024 * 1024 * 1024;
  const int64_t ITER_ALLOC_HOLD_LIMIT = 512 * 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionMetaRedoModule is inited.", K(is_inited_), K(ret));
  } else if (OB_ISNULL(cp_fty) || OB_ISNULL(schema_service) || OB_ISNULL(file_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cp_fty), KP(schema_service), KP(file_mgr));
  } else if (OB_FAIL(iter_allocator_.init(
                 ITER_ALLOC_TOTAL_LIMIT, ITER_ALLOC_HOLD_LIMIT, common::OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_WARN("Fail to init iter allocator, ", K(ret));
  } else if (OB_FAIL(partition_map_.init(ObModIds::OB_PG_PARTITION_MAP))) {
    LOG_WARN("Fail to init pg_partition map", K(ret));
  } else if (OB_FAIL(pg_mgr_.init(cp_fty))) {
    LOG_WARN("Fail to init partition mgr, ", K(ret));
  } else if (OB_FAIL(pg_index_.init())) {
    LOG_WARN("Fail to init partition group index", K(ret));
  } else if (NULL == (txs_ = cp_fty->get_trans_service())) {
    // Does't init txs_ on purpose. Replay slog doesn't need.
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create transaction service failed", K(ret));
  } else if (NULL == (rp_eg_ = cp_fty->get_replay_engine())) {
    // Does't init rp_eg_ on purpose. Replay slog doesn't need.
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create replay engine failed", K(ret));
  } else {
    iter_allocator_.set_label(ObModIds::OB_PARTITION_SERVICE);
    cp_fty_ = cp_fty;
    schema_service_ = schema_service;
    file_mgr_ = file_mgr;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

// Used in server restart. Recover all stand alone partition and partition group.
// pg partition is recovered by slog
int ObPartitionMetaRedoModule::load_partition(
    const char* buf, int64_t buf_len, int64_t& pos, ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* ptt = NULL;
  bool add_pg_mgr = false;
  // partition key deserialized from buf, this key just make checker mute.
  ObPartitionKey key(OB_MIN_USER_TABLE_ID, 0, 0);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0 || pos >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len), K(pos));
  } else {
    // FIXME
    if (NULL == (ptt = cp_fty_->get_partition(OB_SERVER_TENANT_ID))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to create partition");
    } else if (OB_FAIL(init_partition_group(*ptt, key))) {
      LOG_WARN("fail to create partition");
    } else {
      if (nullptr == file_handle.get_storage_file()) {
      } else if (OB_FAIL(ptt->set_storage_file(file_handle))) {
        LOG_WARN("fail to set storage file on pg", K(ret), K(file_handle));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ptt->deserialize(buf, buf_len, pos))) {
          LOG_WARN("fail to create partition's store.", K(ret));
        } else if (OB_FAIL(inner_add_partition(*ptt,
                       false /*need_check_tenant*/,
                       true /*is_replay*/,
                       true /*allow multiple same partitions*/))) {
          LOG_WARN("add new partition to partition manager failed", K(ret));
          // record pkey to PG mapping in PG index
        } else {
          add_pg_mgr = true;
        }
      }
    }
  }
  // free component when creation failed or no need to add.
  if (OB_FAIL(ret)) {
    if (NULL != ptt) {
      cp_fty_->free(ptt);
      ptt = NULL;
    }
  }
  return ret;
}

int ObPartitionMetaRedoModule::handle_pg_conflict(
    const char* buf, const int64_t buf_len, int64_t& pos, ObStorageFileHandle& file_handle, ObIPartitionGroup*& ptt)
{
  int ret = OB_SUCCESS;
  ObPartitionKey key(OB_MIN_USER_TABLE_ID, 0, 0);
  ObPartitionGroupMeta recover_pg_meta;
  if (OB_ISNULL(buf) || buf_len <= 0 || pos >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(ptt->get_pg_storage().get_pg_meta(recover_pg_meta))) {
    LOG_WARN("fail to get recover pg meta", K(ret), K(*ptt));
  } else if (!recover_pg_meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recover pg meta is invalid", K(ret), K(recover_pg_meta));
  } else {
    ptt->clear();
    cp_fty_->free(ptt);
    ptt = NULL;
    if (NULL == (ptt = cp_fty_->get_partition(OB_SERVER_TENANT_ID))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to create partition");
    } else if (OB_FAIL(init_partition_group(*ptt, key))) {
      LOG_WARN("fail to create partition");
    } else if (nullptr == file_handle.get_storage_file()) {
    } else if (OB_FAIL(ptt->set_storage_file(file_handle))) {
      LOG_WARN("fail to set storage file on pg", K(ret), K(file_handle));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ptt->deserialize(buf, buf_len, pos))) {
        LOG_WARN("fail to deserialize partition", K(ret));
      }
    }
  }
  return ret;
}

// Recover PG partition struct when replay slog
int ObPartitionMetaRedoModule::replay(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
  int32_t sub_type = 0;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_REDO_LOG_PARTITION != main_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong redo log type.", K(ret), K(main_type), K(sub_type));
  } else {
    switch (sub_type) {
      case REDO_LOG_ADD_PARTITION:
      case REDO_LOG_ADD_PARTITION_GROUP:
        if (OB_FAIL(replay_add_partition_slog(param))) {
          LOG_ERROR("replay add partition failed.", K(ret), K(param));
        }
        break;
      case REDO_LOG_ADD_PARTITION_TO_PG:
        if (OB_FAIL(replay_add_partition_slog(param))) {
          LOG_ERROR("replay add partition failed.", K(ret), K(param));
        }
        break;
      case REDO_LOG_CREATE_PARTITION_GROUP:
        if (OB_FAIL(replay_create_partition_group(param))) {
          LOG_ERROR("replay create partition group command failed.", K(ret), K(param));
        }
        break;
      case REDO_LOG_REMOVE_PARTITION:
        if (OB_FAIL(replay_remove_partition(param))) {
          LOG_ERROR("replay remove partition command failed.", K(ret), K(param));
        }
        break;
      case REDO_LOG_REMOVE_PARTITION_FROM_PG:
        if (OB_FAIL(replay_remove_partition(param))) {
          LOG_ERROR("replay remove partition command failed.", K(ret), K(param));
        }
        break;
      case REDO_LOG_SET_REPLICA_TYPE:
        if (OB_FAIL(replay_replica_type(param))) {
          LOG_ERROR("replay replica type command failed.", K(ret), K(param));
        }
        break;
      case REDO_LOG_SET_PARTITION_SPLIT_STATE:
        if (OB_FAIL(replay_set_split_state(param))) {
          LOG_ERROR("replay set split state command failed.", K(ret), K(param));
        }
        break;
      case REDO_LOG_SET_PARTITION_SPLIT_INFO:
        if (OB_FAIL(replay_set_split_info(param))) {
          LOG_ERROR("replay set split info command failed.", K(ret), K(param));
        }
        break;
      case REDO_LOG_MODIFY_TABLE_STORE: {
        if (OB_FAIL(replay_modify_table_store(param))) {
          LOG_ERROR("failed to replay_modify_table_store", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_DROP_INDEX_SSTABLE_OF_STORE: {
        if (OB_FAIL(replay_drop_index_of_store(param))) {
          LOG_ERROR("failed to replay_drop_index_of_store", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_UPDATE_PARTITION_GROUP_META: {
        if (OB_FAIL(replay_update_partition_group_meta(param))) {
          LOG_ERROR("failed to replay_update_partition_group_meta", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_CREATE_PG_PARTITION_STORE: {
        if (OB_FAIL(replay_create_pg_partition_store(param))) {
          LOG_ERROR("replay create storage command failed.", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_UPDATE_PG_PARTITION_META: {
        if (OB_FAIL(replay_update_pg_partition_meta(param))) {
          LOG_ERROR("failed to replay_update_pg_partition_storage_info", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_ADD_SSTABLE: {
        if (OB_FAIL(replay_add_sstable(param))) {
          LOG_ERROR("fail to replay add sstable", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_REMOVE_SSTABLE: {
        if (OB_FAIL(replay_remove_sstable(param))) {
          LOG_ERROR("fail to replay remove sstable", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_CHANGE_MACRO_META: {
        if (OB_FAIL(replay_macro_meta(param))) {
          LOG_ERROR("fail to replay macro meta", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_WRITE_FILE_CHECKPOINT: {
        if (OB_FAIL(replay_update_tenant_file_info(param))) {
          LOG_ERROR("failed to replay update tenant file info", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_ADD_RECOVERY_POINT_DATA: {
        if (OB_FAIL(replay_add_recovery_point_data(param))) {
          LOG_ERROR("failed to add recovery point data", K(ret));
        }
        break;
      }
      case REDO_LOG_REMOVE_RECOVERY_POINT_DATA: {
        if (OB_FAIL(replay_remove_recovery_point_data(param))) {
          LOG_ERROR("failed to remove recovery point data", K(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown subtype, do not know how to replay.", K(ret), K(param));
        break;
    }
  }

  return ret;
}

int ObPartitionMetaRedoModule::parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObRedoLogMainType main_type = OB_REDO_LOG_PARTITION;
  int32_t sub_type = 0;

  ObIRedoModule::parse_subcmd(subcmd, main_type, sub_type);  // this func has no ret
  if (NULL == buf || len <= 0 || NULL == stream) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(buf), K(len), K(stream));
  } else if (OB_REDO_LOG_PARTITION != main_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong redo log type.", K(ret), K(main_type), K(sub_type));
  } else if (0 > fprintf(stream, "partition redo log, ")) {
    ret = OB_IO_ERROR;
    LOG_WARN("failed to fprintf log", K(ret));
  } else {
    switch (sub_type) {
      case REDO_LOG_ADD_PARTITION: {
        PrintSLogEntry(ObChangePartitionLogEntry);
        break;
      }
      case REDO_LOG_REMOVE_PARTITION: {
        PrintSLogEntry(ObChangePartitionLogEntry);
        break;
      }
      case REDO_LOG_SET_REPLICA_TYPE: {
        PrintSLogEntry(ObChangePartitionLogEntry);
        break;
      }
      case REDO_LOG_SET_PARTITION_SPLIT_STATE: {
        PrintSLogEntry(ObSplitPartitionStateLogEntry);
        break;
      }
      case REDO_LOG_SET_PARTITION_SPLIT_INFO: {
        PrintSLogEntry(ObSplitPartitionInfoLogEntry);
        break;
      }
      case REDO_LOG_MODIFY_TABLE_STORE: {
        ObTableStore table_store;
        ObModifyTableStoreLogEntry entry(table_store);
        if (OB_FAIL(entry.deserialize(buf, len, pos))) {
          LOG_WARN("Fail to deserialize ObModifyTableStoreLogEntry", K(ret));
        } else if (0 > fprintf(stream, "ObModifyTableStoreLogEntry\n%s\n", to_cstring(entry))) {
          ret = OB_IO_ERROR;
          LOG_WARN("failed to print ObModifyTableStoreLogEntry", K(ret), K(entry));
        }
        break;
      }
      case REDO_LOG_DROP_INDEX_SSTABLE_OF_STORE: {
        PrintSLogEntry(ObDropIndexSSTableLogEntry);
        break;
      }
      // create PG struct and persist meta
      case REDO_LOG_CREATE_PARTITION_GROUP: {
        PrintSLogEntry(ObCreatePartitionGroupLogEntry);
        break;
      }
      // add partition to PG
      case REDO_LOG_ADD_PARTITION_TO_PG: {
        PrintSLogEntry(ObChangePartitionLogEntry);
        break;
      }
      // remove partition from PG
      case REDO_LOG_REMOVE_PARTITION_FROM_PG: {
        PrintSLogEntry(ObChangePartitionLogEntry);
        break;
      }
      // create partition store in pg partition
      case REDO_LOG_CREATE_PG_PARTITION_STORE: {
        PrintSLogEntry(ObCreatePGPartitionStoreLogEntry);
        break;
      }
      // update the partition store meta in pg partition or stand alone partition
      case REDO_LOG_UPDATE_PG_PARTITION_META: {
        PrintSLogEntry(ObUpdatePGPartitionMetaLogEntry);
        break;
      }
      // create PG, similar to creating stand alone partition
      case REDO_LOG_ADD_PARTITION_GROUP: {
        PrintSLogEntry(ObChangePartitionLogEntry);
        break;
      }
      // PG meta update
      case REDO_LOG_UPDATE_PARTITION_GROUP_META: {
        PrintSLogEntry(ObUpdatePartitionGroupMetaLogEntry);
        break;
      }
      case REDO_LOG_ADD_SSTABLE: {
        // TODO (): skip get pg file in sstable deserialize
        //         ObPGKey pgkey;
        //         ObSSTable sstable;
        //         ObAddSSTableLogEntry entry(pgkey, sstable);
        //         if (OB_FAIL(entry.deserialize(buf, len, pos))) {
        //           LOG_WARN("Fail to deserialize ObAddSSTableLogEntry", K(ret));
        //         }
        if (0 > fprintf(stream, "ObAddSSTableLogEntry\nTBD\n")) {
          ret = OB_IO_ERROR;
          LOG_WARN("failed to print ObAddSSTableLogEntry", K(ret));
        }
        break;
      }
      case REDO_LOG_REMOVE_SSTABLE: {
        PrintSLogEntry(ObRemoveSSTableLogEntry);
        break;
      }
      case REDO_LOG_CHANGE_MACRO_META: {
        ObPGKey pgkey;
        ObITable::TableKey table_key;
        blocksstable::ObMacroBlockMetaV2 meta;
        MacroBlockId macro_block_id;
        ObPGMacroBlockMetaLogEntry entry(pgkey, table_key, 0, 0, macro_block_id, meta);
        if (OB_FAIL(entry.deserialize(buf, len, pos))) {
          LOG_WARN("Fail to deserialize ObPGMacroBlockMetaLogEntry", K(ret));
        } else if (0 > fprintf(stream, "ObPGMacroBlockMetaLogEntry\n%s\n", to_cstring(entry))) {
          ret = OB_IO_ERROR;
          LOG_WARN("failed to print ObPGMacroBlockMetaLogEntry", K(ret), K(entry));
        }
        break;
      }
      case REDO_LOG_UPDATE_TENANT_FILE_INFO: {
        PrintSLogEntry(ObUpdateTenantFileInfoLogEntry);
        break;
      }
      case REDO_LOG_ADD_RECOVERY_POINT_DATA: {
        ObRecoveryPointData point_data;
        ObAddRecoveryPointDataLogEntry entry(ObRecoveryPointType::UNKNOWN_TYPE, point_data);
        if (OB_FAIL(entry.deserialize(buf, len, pos))) {
          LOG_WARN("failed to deserialize ObAddRecoveryPointDataLogEntry", K(ret));
        } else if (0 > fprintf(stream, "ObAddRecoveryPointDataLogEntry\n\%s\n", to_cstring(entry))) {
          ret = OB_IO_ERROR;
          LOG_WARN("failed to print ObAddRecoveryPointDataLogEntry", K(ret), K(entry));
        }
        break;
      }
      case REDO_LOG_REMOVE_RECOVERY_POINT_DATA: {
        PrintSLogEntry(ObRemoveRecoveryPointDataLogEntry);
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown subtype, do not know how to replay.", K(ret), K(subcmd), K(main_type), K(sub_type));
        break;
    }
  }
  return ret;
}

int ObPartitionMetaRedoModule::enable_write_log()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* partition_iter = NULL;
  ObIPartitionGroup* partition = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(pg_mgr_.remove_duplicate_pgs())) {
    LOG_WARN("fail to remove duplicate pgs", K(ret));
  } else if (NULL == (partition_iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc partition iter, ", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(partition_iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to get next partition, ", K(ret));
        }
      } else if (NULL == partition) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The partition is NULL, ", K(ret));
      } else if (OB_FAIL(partition->enable_write_log(is_replay_old_))) {
        LOG_WARN("enable_write_log error", "pkey", partition->get_partition_key());
      } else {
        LOG_INFO("enable_write_log success", "pkey", partition->get_partition_key());
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObIRedoModule::enable_write_log())) {
        LOG_WARN("Fail to enable redo module write log, ", K(ret));
      }
    }
  }

  if (NULL != partition_iter) {
    revert_pg_iter(partition_iter);
  }
  return ret;
}

int ObPartitionMetaRedoModule::get_all_partitions(ObIPartitionArrayGuard& partitions)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  ObIPartitionGroup* partition = NULL;

  partitions.set_pg_mgr(pg_mgr_);
  partitions.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service not initialized, cannot get partitions.", K(ret));
  } else {
    lib::ObMutexGuard guard(structure_change_mutex_);
    if (NULL == (iter = alloc_pg_iter())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not alloc scan iterator", K(ret));
    } else if (OB_FAIL(partitions.reserve(pg_mgr_.get_total_partition_count()))) {
      LOG_WARN("failed to reserve partitions", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("get next partition from BTree failed.", K(partition), K(ret));
          }
        } else if (OB_FAIL(partitions.push_back(partition))) {
          LOG_WARN("push to partitions failed.", K(partition), K(ret));
        }
      }
    }
  }

  if (NULL != iter) {
    revert_pg_iter(iter);
  }
  return ret;
}

int ObPartitionMetaRedoModule::get_partitions_by_file_key(
    const ObTenantFileKey& file_key, ObIPartitionArrayGuard& partitions)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = nullptr;
  ObIPartitionGroup* partition = nullptr;

  partitions.set_pg_mgr(pg_mgr_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(file_key));
  } else {
    lib::ObMutexGuard guard(structure_change_mutex_);
    if (NULL == (iter = alloc_pg_iter())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not alloc scan iterator", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        partition = nullptr;
        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("get next partition from BTree failed.", K(ret));
          }
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("return partition is null", K(ret), KP(partition));
        } else if (nullptr == partition->get_storage_file()) {
          // skip
        } else if (partition->get_storage_file()->get_tenant_id() == file_key.tenant_id_ &&
                   partition->get_storage_file()->get_file_id() == file_key.file_id_) {
          if (OB_FAIL(partitions.push_back(partition))) {
            LOG_WARN("push to partitions failed.", K(ret), K(partition->get_partition_key()));
          }
        } else { /*do nothing*/
        }
      }
    }
  }

  if (nullptr != iter) {
    revert_pg_iter(iter);
  }
  return ret;
}

int ObPartitionMetaRedoModule::get_replay_partition(
    const common::ObPartitionKey& pkey, const int64_t file_id, ObIPartitionGroupGuard& guard) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMetaRedoModule has not been inited", K(ret));
  } else if (OB_FAIL(get_partition(pkey, OB_INVALID_DATA_FILE_ID == file_id ? nullptr : &file_id, guard))) {
    LOG_WARN("fail to get partition", K(ret), K(pkey), K(file_id));
  }
  return ret;
}

int ObPartitionMetaRedoModule::get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMetaRedoModule has not been inited", K(ret));
  } else if (OB_FAIL(get_partition(pkey, nullptr /*file_id*/, guard))) {
    if ((OB_PARTITION_NOT_EXIST == ret && REACH_TIME_INTERVAL(1 * 1000 * 1000)) || OB_PARTITION_NOT_EXIST != ret) {
      LOG_WARN("fail to get partition", K(ret), K(pkey), K(common::lbt()));
    }
  }
  return ret;
}

int ObPartitionMetaRedoModule::get_partition(
    const common::ObPartitionKey& pkey, const int64_t* file_id, ObIPartitionGroupGuard& guard) const
{
  int ret = OB_SUCCESS;
  ObPGKey pg_key;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The partition service has not been inited, ", K(ret));
  } else if (OB_SUCCESS == (ret = pg_mgr_.get_pg(pkey, file_id, guard))) {
    // do nothing
  } else if (OB_UNLIKELY(pkey.is_pg() && !pkey.is_trans_table())) {
    LOG_WARN("get partition group error", K(ret), K(pkey));
  } else if (OB_UNLIKELY(OB_PARTITION_NOT_EXIST != ret)) {
    LOG_WARN("get partition group error", K(ret), K(pkey));
  } else if (OB_FAIL(ObPartitionMetaRedoModule::get_pg_key(pkey, pg_key))) {
    if ((OB_PARTITION_NOT_EXIST == ret && REACH_TIME_INTERVAL(1 * 1000 * 1000)) || OB_PARTITION_NOT_EXIST != ret) {
      LOG_WARN("get pg key error", K(ret), K(pkey), K(pg_key));
    }
  } else if (OB_FAIL(pg_mgr_.get_pg(pg_key, file_id, guard))) {
    if ((OB_PARTITION_NOT_EXIST == ret && REACH_TIME_INTERVAL(1 * 1000 * 1000)) || OB_PARTITION_NOT_EXIST != ret) {
      LOG_WARN("get partition group error", K(ret), K(pkey), K(pg_key));
    }
  } else {
    // do nothing
  }
  return ret;
}

ObIPartitionGroupIterator* ObPartitionMetaRedoModule::alloc_pg_iter()
{
  ObPartitionGroupIterator* partition_iter = NULL;
  void* buf = NULL;
  if (NULL == (buf = iter_allocator_.alloc(sizeof(ObPartitionGroupIterator)))) {
    LOG_ERROR("Fail to allocate memory for partition iterator.");
  } else {
    partition_iter = new (buf) ObPartitionGroupIterator();
    partition_iter->set_pg_mgr(pg_mgr_);
  }
  return partition_iter;
}

void ObPartitionMetaRedoModule::revert_pg_iter(ObIPartitionGroupIterator* iter)
{
  if (NULL != iter) {
    iter->~ObIPartitionGroupIterator();
    iter_allocator_.free(iter);
    iter = NULL;
  }
}

int ObPartitionMetaRedoModule::inner_add_partition(
    ObIPartitionGroup& partition, const bool need_check_tenant, const bool is_replay, const bool allow_multi_value)
{
  int ret = OB_SUCCESS;
  UNUSEDx(need_check_tenant, is_replay);
  if (OB_FAIL(pg_mgr_.add_pg(partition, false /*need_check_tenant*/, allow_multi_value))) {
    LOG_WARN("add partition group error", K(ret));
  } else {
    LOG_INFO("partition service add partition", "pkey", partition.get_partition_key());
  }
  return ret;
}

int ObPartitionMetaRedoModule::inner_del_partition_impl(const ObPartitionKey& pkey, const int64_t* file_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = nullptr;

  if (OB_FAIL(get_partition(pkey, file_id, guard))) {
    LOG_WARN("get partition failed", K(ret), K(pkey));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition group is null, unexpected error", K(pkey));
  } else {
    ObIPartitionGroupGuard partition_guard;
    if (OB_FAIL(pg_mgr_.get_pg(pkey, file_id, partition_guard))) {
      LOG_WARN("pg mgr get partition group error", K(ret), K(pkey));
    } else if (OB_FAIL(partition_guard.get_partition_group()->remove_election_from_mgr())) {
      LOG_WARN("remove election from election mgr error", K(ret), K(pkey));
    } else if (OB_FAIL(pg_mgr_.del_pg(pkey, file_id))) {
      LOG_WARN("pg mgr remove partition group error", K(ret), K(pkey));
    } else if (OB_FAIL(pg->get_pg_storage().remove_all_pg_index())) {
      LOG_WARN("failed to remove all pg index", K(ret), K(pkey));
    }
  }
  LOG_INFO("partition service delete partition", K(ret), K(pkey));

  return ret;
}

int ObPartitionMetaRedoModule::inner_del_partition(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_del_partition_impl(pkey, nullptr /*file_id*/))) {
    LOG_WARN("fail to inner del partition", K(ret));
  }
  return ret;
}

int ObPartitionMetaRedoModule::inner_del_partition_for_replay(const ObPartitionKey& pkey, const int64_t file_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_FAIL(get_partition(pkey, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      LOG_INFO("pg not exist in partition image", K(pkey));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get partition", K(ret), K(pkey));
    }
  } else if (OB_FAIL(inner_del_partition_impl(pkey, OB_INVALID_DATA_FILE_ID == file_id ? nullptr : &file_id))) {
    LOG_WARN("fail to inner del partition", K(ret));
  }
  return ret;
}

int ObPartitionMetaRedoModule::init_partition_group(ObIPartitionGroup& pg, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg.init(pkey,
          cp_fty_,
          schema_service_,
          txs_,
          rp_eg_,
          nullptr,  // ObPartitionService *ps
          &pg_index_,
          &partition_map_))) {
    LOG_WARN("fail to create partition", K(ret), K(pkey));
  }
  return ret;
}

// 1. if pkey represents a stand alone partition, no need to call get_pg_key because pg_key == pkey
// 2. if pkey represents a pg, then need pg_index to get the correct pg_key
int ObPartitionMetaRedoModule::get_pg_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(const_cast<ObPartitionGroupIndex&>(pg_index_).get_pg_key(pkey, pg_key))) {
    // do nothing
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_PARTITION_NOT_EXIST;
  } else if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    LOG_WARN("partition group index get pg key error", K(ret), K(pkey), K(pg_key));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionMetaRedoModule::replay_add_partition_slog(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObIPartitionGroup* ptt = NULL;
  ObChangePartitionLogEntry log_entry;
  bool is_ptt_in_mgr = false;
  ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
  int32_t sub_type = 0;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
  } else if (REDO_LOG_ADD_PARTITION == sub_type || REDO_LOG_ADD_PARTITION_GROUP == sub_type) {
    // PG persistence checkpoint may concurrent with server checkpoint
    ObIPartitionGroupGuard guard;
    bool need_replay = false;
    if (OB_FAIL(pg_mgr_.get_pg(
            log_entry.pg_key_, OB_INVALID_DATA_FILE_ID == param.file_id_ ? nullptr : &param.file_id_, guard))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        need_replay = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get partition group error", K(ret), K(log_entry));
      }
    } else {
      need_replay = false;
    }
    if (OB_FAIL(ret)) {
    } else if (!need_replay) {
      LOG_INFO("current log do not need replay", K(log_entry));
    } else if (NULL == (ptt = cp_fty_->get_partition(log_entry.partition_key_.get_tenant_id()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to create partition");
    } else if (OB_FAIL(init_partition_group(*ptt, log_entry.partition_key_))) {
      LOG_WARN("fail to init partition", K(ret), K(log_entry));
    } else if (REDO_LOG_ADD_PARTITION == sub_type && !log_entry.partition_key_.is_pg() &&
               OB_FAIL(ptt->replay_pg_partition(log_entry.partition_key_, log_entry.log_id_))) {
      LOG_WARN("failed to replay pg partition for standalone pg", K(ret), K(log_entry));
    } else {
      ObStorageFileHandle file_handle;
      ObTenantFileKey file_key(log_entry.partition_key_.get_tenant_id(), param.file_id_);
      if (is_replay_old_) {
        // do nothing
      } else if (OB_INVALID_DATA_FILE_ID != param.file_id_) {
        if (OB_FAIL(file_mgr_->open_file(file_key, file_handle))) {
          LOG_WARN("fail to open file", K(ret));
        }
      } else {
        const bool sys_table = is_sys_table(log_entry.partition_key_.get_table_id());
        if (OB_FAIL(file_mgr_->alloc_file(
                log_entry.partition_key_.get_tenant_id(), sys_table, file_handle, false /*write slog*/))) {
          LOG_WARN("fail to open pg file", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ptt->set_storage_file(file_handle))) {
        LOG_WARN("fail to set storage file", K(ret));
      } else if (OB_FAIL(inner_add_partition(*ptt,
                     false /*need_check_tenant*/,
                     false /*is_replay*/,
                     true /*allow multiple same partitions*/))) {
        LOG_WARN("fail to inner add partition, ", K(ret));
      } else {
        is_ptt_in_mgr = true;
        file_handle.reset();
      }
    }
  } else if (REDO_LOG_ADD_PARTITION_TO_PG == sub_type) {
    // pg init success
    ObIPartitionGroupGuard guard;
    is_ptt_in_mgr = true;
    if (OB_FAIL(pg_mgr_.get_pg(
            log_entry.pg_key_, OB_INVALID_DATA_FILE_ID == param.file_id_ ? nullptr : &param.file_id_, guard))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("pg not exist in partition image", K(log_entry));
      } else {
        LOG_WARN("get partition group error", K(ret), K(log_entry));
      }
    } else if (OB_ISNULL(ptt = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("partition group is null, unexpected error", K(ret), K(log_entry), KP(ptt));
    } else if (OB_FAIL(ptt->replay_pg_partition(log_entry.partition_key_, log_entry.log_id_))) {
      LOG_WARN("replay add partition to pg error", K(ret), K(log_entry), K(param));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected sub type", K(ret), K(sub_type));
  }

  LOG_INFO("replay base storage log::add_partition", K(param), K(log_entry), K(sub_type), K(ret));

  if (OB_FAIL(ret) && NULL != ptt && REDO_LOG_ADD_PARTITION_TO_PG != sub_type) {
    // remove from pg_mgr
    if (is_ptt_in_mgr) {
      ObIPartitionGroupGuard partition_guard;
      common::ObPartitionKey pkey = ptt->get_partition_key();
      const int64_t* file_id = OB_INVALID_DATA_FILE_ID == param.file_id_ ? nullptr : &param.file_id_;
      if (OB_FAIL(pg_mgr_.get_pg(pkey, file_id, partition_guard))) {
        LOG_WARN("pg mgr get partition group error", K(ret), K(pkey));
      } else if (OB_FAIL(partition_guard.get_partition_group()->remove_election_from_mgr())) {
        LOG_WARN("remove election from election mgr error", K(ret), K(pkey));
      } else if (OB_FAIL(pg_mgr_.del_pg(pkey, file_id))) {
        LOG_WARN("pg mgr remove partition group error", K(ret), K(pkey));
      }
    } else {
      cp_fty_->free(ptt);
    }
    ptt = NULL;
  }
  return ret;
}

int ObPartitionMetaRedoModule::replay_create_partition_group(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObCreatePartitionGroupLogEntry log_entry;
  const bool write_slog = false;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize ObCreatePartitionGroupLogEntry", K(ret));
  } else {
    LOG_INFO("start to replay partition group log", K(log_entry), K(param));
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* pg = nullptr;
    bool need_replay = true;
    ObCreatePGParam create_pg_param;
    if (OB_FAIL(pg_mgr_.get_pg(
            log_entry.meta_.pg_key_, OB_INVALID_DATA_FILE_ID == param.file_id_ ? nullptr : &param.file_id_, guard))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("pg not exist in partition image", K(log_entry));
      } else {
        LOG_WARN("failed to get partition group", K(ret), K(log_entry));
      }
    } else if (OB_ISNULL(pg = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pg is null", K(ret));
    } else if (pg->get_pg_storage().is_pg_meta_valid()) {
      FLOG_INFO("pg is load from checkpoint, skip create pg group slog", K(param));
    } else {
      ObStorageFileHandle file_handle;
      ObTenantFileKey file_key(log_entry.meta_.pg_key_.get_tenant_id(), log_entry.meta_.storage_info_.get_pg_file_id());
      if (OB_INVALID_DATA_FILE_ID != log_entry.meta_.storage_info_.get_pg_file_id()) {
        if (OB_FAIL(file_mgr_->open_file(file_key, file_handle))) {
          LOG_WARN("fail to open file", K(ret));
        }
      } else {
        const bool sys_table = is_sys_table(log_entry.meta_.pg_key_.get_table_id());
        if (OB_FAIL(file_mgr_->alloc_file(
                log_entry.meta_.pg_key_.get_tenant_id(), sys_table, file_handle, false /*write slog*/))) {
          LOG_WARN("fail to open pg file", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_create_pg_param(log_entry, write_slog, &file_handle, file_mgr_, create_pg_param))) {
        LOG_WARN("failed to get create pg param", K(ret), K(log_entry));
      } else if (OB_FAIL(pg->get_pg_storage().create_partition_group(create_pg_param))) {
        LOG_WARN("failed to replay create partition group", K(ret), K(log_entry), K(create_pg_param));
      } else {
        file_handle.reset();
      }

      if (OB_FAIL(ret)) {
      } else if (FOLLOWER_WAIT_SPLIT == log_entry.meta_.saved_split_state_ && OB_FAIL(pg->set_wait_split())) {
        LOG_WARN("failed to set wait split", K(ret), K(log_entry));
      } else {
        LOG_INFO("succeed to replay create partition group", K(log_entry), K(param));
      }
    }
  }
  return ret;
}

int ObPartitionMetaRedoModule::replay_remove_partition(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObChangePartitionLogEntry log_entry;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
    // compatible 2.1 stand alone partition
  } else if (!log_entry.pg_key_.is_valid()) {
    if (OB_FAIL(inner_del_partition_for_replay(log_entry.partition_key_, param.file_id_))) {
      LOG_WARN("fail to delete partition, ", K(ret), K(log_entry));
    }
    // 2.2 stand alone partition
  } else if (!log_entry.pg_key_.is_pg()) {
    if (OB_FAIL(inner_del_partition_for_replay(log_entry.partition_key_, param.file_id_))) {
      LOG_WARN("fail to delete partition, ", K(ret), K(log_entry));
    }
    // 2.2 PG
  } else if (log_entry.partition_key_.is_pg()) {
    if (OB_FAIL(inner_del_partition_for_replay(log_entry.partition_key_, param.file_id_))) {
      LOG_WARN("fail to delete partition, ", K(ret), K(log_entry));
    }
  } else {
    // PG partition
    ObIPartitionGroupGuard guard;
    const bool write_slog_trans = false;
    if (OB_FAIL(get_replay_partition(log_entry.pg_key_, param.file_id_, guard))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        LOG_INFO("pg not exist in partition image", K(log_entry));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get partition failed", K(ret), K(log_entry));
      }
    } else if (OB_UNLIKELY(NULL == guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get partition", K(ret), K(log_entry));
    } else if (OB_FAIL(guard.get_partition_group()->remove_partition_from_pg(
                   true /* for_replay */, log_entry.partition_key_, write_slog_trans, log_entry.log_id_))) {
      LOG_WARN("remove partition from pg error", K(ret), K(log_entry));
    } else if (OB_FAIL(post_replay_remove_pg_partition(log_entry))) {
      LOG_WARN("post work after reply remove pg partition fail", K(log_entry), K(ret));
    }
  }
  LOG_INFO("replay base storage log::remove_partition", K(log_entry), K(ret), K(param));
  return ret;
}

int ObPartitionMetaRedoModule::replay_replica_type(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObChangePartitionLogEntry log_entry;
  int64_t pos = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_ERROR("deserialize log_entry failed.", K(ret));
  } else if (log_entry.partition_key_ != log_entry.pg_key_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pg key and partition key should be equal", K(ret), K(log_entry));
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* pg = nullptr;
    if (OB_FAIL(get_replay_partition(log_entry.partition_key_, param.file_id_, guard))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO(
            "partition is not exist in checkpoint of partition image.", K(ret), K(log_entry.partition_key_), K(param));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error when replay set replica status log.", K(ret), K(log_entry.partition_key_), K(param));
      }
    } else if (OB_ISNULL(pg = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get partition failed", K(log_entry), K(ret));
    } else if (OB_FAIL(pg->get_pg_storage().set_pg_replica_type(log_entry.replica_type_, false))) {
      LOG_ERROR("set_replica_type failed.", K(log_entry), K(ret));
    } else {
      FLOG_INFO("replay migrate_stat_log::set_replica_type", K(log_entry), K(ret), K(param));
    }
  }

  return ret;
}

int ObPartitionMetaRedoModule::replay_set_split_state(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObSplitPartitionStateLogEntry log_entry;
  ObPartitionStorage* partition_storage = NULL;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
  } else if (OB_FAIL(get_replay_partition(log_entry.get_pkey(), param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry), K(param));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error when replay replay_drop_index_of_store.", K(ret), K(log_entry), K(param));
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition failed", K(log_entry), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(log_entry.get_pkey(), pg_partition_guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get pg partition", K(ret), K(log_entry));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("pg_partition might already be deleted, skip replay", K(ret), K(log_entry));
    }
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_SYS;
    LOG_WARN("partition must not null", K(ret), K(log_entry));
  } else if (OB_ISNULL(partition_storage =
                           static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition storage must not null", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->replay_split_state_slog(log_entry))) {
    LOG_WARN("replay partition split slog failed", K(ret), K(log_entry));
  }

  LOG_INFO("replay set partition split state", K(log_entry), K(ret), K(param));
  return ret;
}

int ObPartitionMetaRedoModule::replay_set_split_info(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObSplitPartitionInfoLogEntry log_entry;
  ObPartitionStorage* partition_storage = NULL;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
  } else if (OB_FAIL(get_replay_partition(log_entry.get_pkey(), param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry), K(param));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error when replay replay_drop_index_of_store.", K(ret), K(log_entry), K(param));
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition failed", K(log_entry), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(log_entry.get_pkey(), pg_partition_guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get pg partition", K(ret), K(log_entry));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("pg_partition might already be deleted, skip replay", K(ret), K(log_entry));
    }
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_SYS;
    LOG_WARN("partition must not null", K(ret), K(log_entry));
  } else if (OB_ISNULL(partition_storage =
                           static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition storage must not null", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->replay_split_info_slog(log_entry))) {
    LOG_WARN("replay partition split info slog failed", K(ret), K(log_entry));
  }

  LOG_INFO("replay set partition split info", K(log_entry), K(ret), K(param));
  return ret;
}

int ObPartitionMetaRedoModule::replay_modify_table_store(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTableStore table_store;
  ObModifyTableStoreLogEntry log_entry(table_store);
  ObPartitionStorage* partition_storage = NULL;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
  } else if (OB_FAIL(get_replay_partition(
                 log_entry.pg_key_.is_valid() ? log_entry.pg_key_ : log_entry.table_store_.get_partition_key(),
                 param.file_id_,
                 guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry), K(param));
    } else {
      LOG_WARN("unexpected error when replay replay_modify_table_store.", K(ret), K(log_entry), K(param));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition failed", K(ret), K(log_entry.table_store_));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(
                 log_entry.table_store_.get_partition_key(), pg_partition_guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get pg partition", K(ret), K(log_entry));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("pg_partition might already be deleted, skip replay", K(ret), K(log_entry));
    }
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_SYS;
    LOG_WARN("partition must not null", K(ret), K(log_entry));
  } else if (OB_ISNULL(partition_storage =
                           static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition storage must not null", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_modify_table_store(log_entry))) {
    LOG_WARN("failed to modify table store", K(ret));
  }

  LOG_INFO("replay base storage log::modify table store", K(log_entry), K(ret), K(param));
  return ret;
}

int ObPartitionMetaRedoModule::replay_drop_index_of_store(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObDropIndexSSTableLogEntry log_entry;
  ObPartitionStorage* partition_storage = NULL;
  ObIPartitionGroupGuard guard;  // with write lock
  ObPGPartitionGuard pg_partition_guard;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
  } else if (OB_FAIL(get_replay_partition(
                 log_entry.pg_key_.is_valid() ? log_entry.pg_key_ : log_entry.pkey_, param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry), K(param));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error when replay replay_drop_index_of_store.", K(ret), K(log_entry), K(param));
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition failed", K(log_entry.pkey_), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(log_entry.pkey_, pg_partition_guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get pg partition", K(ret), K(log_entry));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("pg_partition might already be deleted, skip replay", K(ret), K(log_entry));
    }
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_SYS;
    LOG_WARN("partition must not null", K(ret), K(log_entry));
  } else if (OB_ISNULL(partition_storage =
                           static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition storage must not null", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_drop_index(
                 log_entry.pkey_, log_entry.index_id_))) {
    LOG_WARN("failed to drop partition index", K(ret), K(log_entry));
  }

  LOG_INFO("replay base storage log::drop partition index", K(log_entry), K(ret), K(param));
  return ret;
}

int ObPartitionMetaRedoModule::replay_update_partition_group_meta(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObUpdatePartitionGroupMetaLogEntry log_entry;
  ObIPartitionGroupGuard guard;  // with write lock
  ObIPartitionGroup* pg = nullptr;
  bool need_replay = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
  } else if (OB_FAIL(get_replay_partition(log_entry.meta_.pg_key_, param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry), K(param));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error when replay replay_update_partition_group_meta.", K(ret), K(log_entry), K(param));
    }
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition failed", K(ret), "pg key", log_entry.meta_.pg_key_);
  } else if (UNKNOWN_SPLIT_STATE != log_entry.meta_.saved_split_state_ &&
             OB_FAIL(pg->restore_split_state(static_cast<int>(log_entry.meta_.saved_split_state_)))) {
    LOG_WARN("restore split state failed", K(ret));
  } else if (OB_FAIL(pg->restore_split_info(log_entry.meta_.split_info_))) {
    LOG_WARN("restore split info failed", K(ret));
  } else if (OB_FAIL(pg->get_pg_storage().replay_partition_group_meta(log_entry.meta_))) {
    LOG_WARN("failed to update_storage_info", K(ret), K(log_entry));
  }

  LOG_INFO("replay base storage log::update partition meta", K(param), K(log_entry), K(ret));
  return ret;
}

int ObPartitionMetaRedoModule::replay_create_pg_partition_store(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObCreatePGPartitionStoreLogEntry log_entry;
  ObIPartitionGroupGuard guard;  // with write lock

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
  } else if (OB_FAIL(get_replay_partition(
                 log_entry.pg_key_.is_valid() ? log_entry.pg_key_ : log_entry.meta_.pkey_, param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      LOG_INFO("pg not exist in partition image", K(log_entry));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get partition failed", K(log_entry.meta_.pkey_), K(ret));
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition failed", K(log_entry.meta_.pkey_), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_pg_partition_store(log_entry.meta_))) {
    LOG_WARN("failed to init pg partition store", K(ret), K(log_entry), K(param));
  }

  FLOG_INFO("replay base storage log::create pg partition store", K(param), K(log_entry), K(ret));
  return ret;
}

int ObPartitionMetaRedoModule::replay_update_pg_partition_meta(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObUpdatePGPartitionMetaLogEntry log_entry;
  ObPartitionStorage* partition_storage = NULL;
  ObIPartitionGroupGuard guard;  // with write lock
  ObPGPartitionGuard pg_partition_guard;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize log entry failed.", K(ret));
  } else if (OB_FAIL(get_replay_partition(
                 log_entry.pg_key_.is_valid() ? log_entry.pg_key_ : log_entry.meta_.pkey_, param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry), K(param));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error when replay replay_drop_index_of_store.", K(ret), K(log_entry), K(param));
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition failed", K(ret), "pkey", log_entry.meta_.pkey_);
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(log_entry.meta_.pkey_, pg_partition_guard)) ||
             OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get pg partition", K(ret), K(log_entry));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("pg_partition might already be deleted, skip replay", K(ret), K(log_entry));
    }
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_SYS;
    LOG_WARN("partition must not null", K(ret), K(log_entry));
  } else if (OB_ISNULL(partition_storage =
                           static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition storage must not null", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_pg_partition_meta(log_entry.meta_))) {
    LOG_WARN("failed to replay_pg_partition_meta", K(ret), K(log_entry));
  }

  FLOG_INFO("replay base storage log::update pg partition meta", K(param), K(log_entry), K(ret));
  return ret;
}

int ObPartitionMetaRedoModule::replay_add_sstable(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObSSTable sstable;
  sstable.set_replay_module(this);
  ObAddSSTableLogEntry log_entry(sstable);
  ObIPartitionGroupGuard guard;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMetaRedoModule has not been inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      LOG_WARN("fail to deserialize log entry", K(ret));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(param));
    }
  } else if (OB_FAIL(get_replay_partition(log_entry.pg_key_, param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry), K(param));
    } else {
      LOG_WARN("unexpected error when replay add sstable.", K(ret), K(log_entry), K(param));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition failed", K(ret), K(log_entry.sstable_));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_add_sstable(log_entry.sstable_))) {
    LOG_WARN("fail to replay add sstable", K(ret), K(param));
  }

  FLOG_INFO("replay base storage log::replay add sstable", K(ret), K(log_entry), K(param));
  return ret;
}

int ObPartitionMetaRedoModule::replay_remove_sstable(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObRemoveSSTableLogEntry log_entry;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMetaRedoModule has not been inited", K(ret));
  } else if (!param.is_valid()) {
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (OB_FAIL(get_replay_partition(log_entry.pg_key_, param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry), K(param));
    } else {
      LOG_WARN("unexpected error when replay remove sstable.", K(ret), K(log_entry), K(param));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition group must not be null", K(ret), K(log_entry));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_remove_sstable(log_entry.table_key_))) {
    LOG_WARN("fail to replay remove sstable", K(ret), K(param));
  }
  FLOG_INFO("replay base storage log::replay remove sstable", K(ret), K(log_entry), K(param));
  return ret;
}

int ObPartitionMetaRedoModule::replay_macro_meta(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObMacroBlockMetaV2 meta;
  ObObj endkey[OB_MAX_ROWKEY_COLUMN_NUMBER];
  MEMSET(&meta, 0, sizeof(meta));
  meta.endkey_ = endkey;
  ObPGMacroBlockMetaLogEntry log_entry(meta);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMetaRedoModule has not been inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else {
    ObMacroBlockKey macro_key;
    macro_key.table_key_ = log_entry.table_key_;
    macro_key.macro_block_id_ = log_entry.macro_block_id_;
    ObMacroMetaReplayMap* replay_map = nullptr;
    const ObTenantFileKey tenant_file_key(log_entry.pg_key_.get_tenant_id(), param.file_id_);
    if (OB_FAIL(file_mgr_->get_macro_meta_replay_map(tenant_file_key, replay_map)) || OB_ISNULL(replay_map)) {
      LOG_WARN("fail to get replay map", K(ret), K(tenant_file_key), KP(replay_map));
    } else if (OB_FAIL(replay_map->set(macro_key, log_entry.meta_, true /*overwrite*/))) {
      LOG_WARN("fail to replay macro meta to map", K(ret), K(macro_key));
    } else {
      FLOG_INFO("replay base storage log::replay macro meta", K(ret), K(log_entry), K(param));
    }
  }
  return ret;
}

int ObPartitionMetaRedoModule::get_create_pg_param(const ObCreatePartitionGroupLogEntry& log_entry,
    const bool write_pg_slog, ObStorageFileHandle* file_handle, ObBaseFileMgr* file_mgr, ObCreatePGParam& param)
{
  int ret = OB_SUCCESS;
  if (!log_entry.is_valid() || OB_ISNULL(file_mgr) || OB_ISNULL(file_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get create pg param get invalid argument", K(ret), K(log_entry), KP(file_handle), KP(file_mgr));
  } else if (OB_FAIL(param.set_storage_info(log_entry.meta_.storage_info_))) {
    LOG_WARN("failed to set storage info", K(ret), K(log_entry));
  } else if (OB_FAIL(param.set_split_info(log_entry.meta_.split_info_))) {
    LOG_WARN("failed to set split info", K(ret), K(log_entry));
  } else {
    param.create_timestamp_ = log_entry.meta_.create_timestamp_;
    param.data_version_ = log_entry.meta_.report_status_.data_version_;
    param.is_restore_ = log_entry.meta_.is_restore_;
    param.replica_property_ = log_entry.meta_.replica_property_;
    param.replica_type_ = log_entry.meta_.replica_type_;
    param.split_state_ = log_entry.meta_.saved_split_state_;
    param.write_slog_ = write_pg_slog;
    param.file_handle_ = file_handle;
    param.file_mgr_ = file_mgr;
    param.create_frozen_version_ = log_entry.meta_.create_frozen_version_;
    param.last_restore_log_id_ = log_entry.meta_.last_restore_log_id_;
    param.restore_snapshot_version_ = log_entry.meta_.restore_snapshot_version_;
  }
  return ret;
}

int ObPartitionMetaRedoModule::replay_update_tenant_file_info(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObUpdateTenantFileInfoLogEntry log_entry;
  if (OB_ISNULL(file_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file mgr is null", K(ret), KP(file_mgr_));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (!log_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log_entry is not valid", K(ret), K(log_entry));
  } else {
    ObTenantFilePGMetaCheckpointReader reader;
    const ObTenantFileKey& file_key = log_entry.file_info_.tenant_key_;
    bool from_svr_ckpt = false;
    if (OB_FAIL(file_mgr_->is_from_svr_ckpt(file_key, from_svr_ckpt)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("check is from svr ckpt fail", K(ret), K(file_key));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file not exist, skip update file info log entry", K(ret), K(log_entry));
    } else if (from_svr_ckpt) {
      FLOG_INFO("file is in svr ckpt, skip load file ckpt", K(file_key), K(from_svr_ckpt));
    } else if (OB_FAIL(reader.read_checkpoint(
                   file_key, log_entry.file_info_.tenant_file_super_block_, *file_mgr_, *this))) {
      LOG_WARN("fail to read checkpoint", K(ret), K(log_entry));
    } else {
      FLOG_INFO("replay base storage log::update tenant file info", K(ret), K(log_entry), K(param));
    }
  }
  return ret;
}

int ObPartitionMetaRedoModule::replay_add_recovery_point_data(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObRecoveryPointData point_data;
  ObAddRecoveryPointDataLogEntry log_entry(ObRecoveryPointType::UNKNOWN_TYPE, point_data);
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMetaRedoModule has not been inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (!log_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log_entry is not valid", K(ret), K(log_entry));
  } else if (OB_FAIL(get_replay_partition(log_entry.point_data_.get_pg_key(), param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry));
    } else {
      LOG_WARN("unexpected error when replay remove sstable.", K(ret), K(log_entry));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition group must not be null", K(ret), K(log_entry));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_add_recovery_point_data(
                 log_entry.point_type_, point_data))) {
    LOG_WARN("failed to replay add recovery point data", K(ret), K(point_data));
  }
  return ret;
}

int ObPartitionMetaRedoModule::replay_remove_recovery_point_data(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* buf = param.buf_;
  const int64_t buf_len = param.buf_len_;
  ObRemoveRecoveryPointDataLogEntry log_entry;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMetaRedoModule has not been inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize log entry", K(ret));
  } else if (!log_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log_entry is not valid", K(ret), K(log_entry));
  } else if (OB_FAIL(get_replay_partition(log_entry.pg_key_, param.file_id_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("partition is not exist in checkpoint of partition image.", K(ret), K(log_entry));
    } else {
      LOG_WARN("unexpected error when replay remove sstable.", K(ret), K(log_entry));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition group must not be null", K(ret), K(log_entry));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_remove_recovery_point_data(
                 log_entry.point_type_, log_entry.snapshot_version_))) {
    LOG_WARN("failed to replay remove recovery point data", K(ret), K(log_entry));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
