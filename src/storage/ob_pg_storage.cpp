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
#include "storage/ob_pg_storage.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/ob_partition_key.h"
#include "share/ob_force_print_log.h"
#include "share/ob_index_build_stat.h"
#include "share/ob_index_checksum.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_service_rpc.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_saved_storage_info_v2.h"
#include "common/storage/ob_freeze_define.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/ob_partition_scheduler.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_partition_split_worker.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_partition_checkpoint.h"
#include "share/ob_partition_modify.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_interm_macro_mgr.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_table_store.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"
#include "storage/transaction/ob_trans_part_ctx.h"
#include "observer/ob_server.h"
#include "storage/ob_pg_all_meta_checkpoint_writer.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_partition_group.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase {
using namespace common;
using namespace rootserver;
using namespace blocksstable;
using namespace memtable;
using namespace transaction;
using namespace clog;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace storage {

bool ObPGCreateSSTableParam::is_valid() const
{
  return (nullptr != with_partition_param_) || (nullptr != with_table_param_) ||
         (nullptr != table_key_ && nullptr != meta_);
}

int ObPGCreateSSTableParam::assign(const ObPGCreateSSTableParam& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_blocks_.assign(other.data_blocks_))) {
    LOG_WARN("fail to assign data blocks", K(ret));
  } else if (OB_FAIL(lob_blocks_.assign(other.lob_blocks_))) {
    LOG_WARN("fail to assign lob blocks", K(ret));
  } else {
    with_partition_param_ = other.with_partition_param_;
    with_table_param_ = other.with_table_param_;
    table_key_ = other.table_key_;
    meta_ = other.meta_;
    bloomfilter_block_ = other.bloomfilter_block_;
    sstable_merge_info_ = other.sstable_merge_info_;
  }
  return ret;
}

ObPGStorage::ObPGStorage()
    : is_inited_(false),
      is_removed_(false),
      pkey_(),
      cp_fty_(NULL),
      txs_(NULL),
      pls_(NULL),
      pg_(NULL),
      pg_partition_(NULL),
      schema_service_(NULL),
      partition_list_(),
      lock_(ObLatchIds::PARTITION_GROUP_LOCK),
      meta_(nullptr),
      log_seq_num_(0),
      need_clear_pg_index_(true),
      file_handle_(),
      sstable_mgr_(),
      meta_block_handle_(),
      file_mgr_(nullptr),
      is_paused_(false),
      trans_table_seq_(0),
      last_freeze_ts_(0),
      cached_replica_type_(REPLICA_TYPE_MAX),
      recovery_point_data_mgr_()
{}

ObPGStorage::~ObPGStorage()
{
  destroy();
}

int ObPGStorage::init(const ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
    share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
    clog::ObIPartitionLogService* pls, ObIPartitionGroup* pg)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid() || NULL == cp_fty || NULL == schema_service || NULL == txs || NULL == pg) {
    STORAGE_LOG(WARN, "invalid arguments.", K(key), K(cp_fty), K(schema_service), K(txs));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "partition is already initialized", K(key), K(ret));
  } else if (OB_FAIL(bucket_lock_.init(BUCKET_LOCK_BUCKET_CNT, ObLatchIds::PG_STORAGE_BUCKET_LOCK))) {
    LOG_WARN("failed to init bucket lock", K(ret), K(key));
  } else if (OB_FAIL(alloc_meta_(meta_))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else if (OB_FAIL(sstable_mgr_.init(key))) {
    STORAGE_LOG(WARN, "fail to init pg sstable mgr", K(ret));
  } else if (OB_FAIL(recovery_point_data_mgr_.init(key))) {
    STORAGE_LOG(WARN, "failed to init recovery point data mgr", K(ret));
  } else {
    cp_fty_ = cp_fty;
    pkey_ = key;
    txs_ = txs;
    pls_ = pls;
    pg_ = pg;
    schema_service_ = schema_service;
    pg_memtable_mgr_.set_pkey(key);
    is_inited_ = true;
  }
  STORAGE_LOG(INFO, "partition init", K(ret), K(key));
  return ret;
}

void ObPGStorage::destroy()
{
  FLOG_INFO("destroy pg storage", K(*this), K(lbt()));
  clear();
  if (NULL != file_handle_.get_storage_file()) {
    file_handle_.reset();
  }
}

void ObPGStorage::clear()
{
  FLOG_INFO("clear pg storage", K(*this), K(lbt()));

  int tmp_ret = OB_SUCCESS;

  meta_block_handle_.reset();
  if (is_inited_) {
    if (OB_SUCCESS != (tmp_ret = remove_all_pg_index())) {
      LOG_ERROR("failed to remove all pg index", K(tmp_ret), K(pkey_));
      ob_abort();
    }
    need_clear_pg_index_ = false;
  }
  pls_ = NULL;
  pkey_.reset();
  cp_fty_ = NULL;
  txs_ = NULL;
  if (nullptr != pg_ && nullptr != pg_->get_pg_partition_map()) {
    RemovePGPartitionFunctor functor(*(pg_->get_pg_partition_map()));
    partition_list_.remove_all(functor);
  }
  pg_ = NULL;
  pg_partition_ = NULL;
  schema_service_ = NULL;
  log_seq_num_ = 0;

  is_inited_ = false;
  is_removed_ = false;
  if (nullptr != meta_) {
    free_meta_(meta_);
  }
  bucket_lock_.destroy();
}

int ObPGStorage::alloc_meta_(ObPartitionGroupMeta*& meta)
{
  int ret = OB_SUCCESS;
  void* buf = nullptr;

  if (meta != nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("meta is not null, no need alloc meta", K(ret), KP(meta));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObPartitionGroupMeta), "PgMeta"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else {
    meta = new (buf) ObPartitionGroupMeta();
    meta->reset();
  }
  return ret;
}

void ObPGStorage::free_meta_(ObPartitionGroupMeta*& meta)
{
  if (nullptr != meta) {
    meta->~ObPartitionGroupMeta();
    ob_free(meta);
    meta = nullptr;
  }
}

void ObPGStorage::switch_meta_(ObPartitionGroupMeta*& meta)
{
  OB_ASSERT(nullptr != meta);
  TCWLockGuard lock_guard(lock_);
  ObPartitionGroupMeta* tmp_meta = meta_;
  meta_ = meta;
  meta = tmp_meta;
}

int ObPGStorage::serialize_impl(
    ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size, ObTablesHandle* tables_handle)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = 0;
  new_buf = nullptr;
  serialize_size = 0;
  char* meta_buf = nullptr;
  int64_t meta_serialize_size = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (0 >= (meta_size = get_meta_serialize_size_())) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to get serialize size", K(ret), K_(pkey));
  } else if (OB_ISNULL(meta_buf = static_cast<char*>(allocator.alloc(meta_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc serialize buf", K(ret), K_(pkey));
  } else if (OB_FAIL(serialization::encode_i64(meta_buf, meta_size, meta_serialize_size, MAGIC_NUM))) {
    LOG_WARN("failed to serialize MAGIC_NUM", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(meta_buf, meta_size, meta_serialize_size, log_seq_num_))) {
    LOG_WARN("failed to serialize log_seq_num", K(ret));
  } else if (OB_FAIL(meta_->serialize(meta_buf, meta_size, meta_serialize_size))) {
    LOG_WARN("failed to serialize meta", K(ret));
  } else if (meta_size != meta_serialize_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta size is not equal to meta_serialize_size", K(ret), K(meta_size), K(meta_serialize_size));
  } else {
    const int64_t pg_partition_cnt = meta_->partitions_.count();
    char* tmp_buf = nullptr;
    int64_t tmp_size = 0;
    serialize_size += meta_serialize_size;
    SerializePair tmp_pair;
    ObArray<SerializePair> pairs;
    SerializePair sstable_pair;
    SerializePair recovery_point_pair;
    int64_t copy_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_partition_cnt; ++i) {
      ObPGPartition* pg_partition = nullptr;
      const ObPartitionKey& pkey = meta_->partitions_.at(i);

      ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
      if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pg partition info is null, unexpected error", K(ret), K(pkey));
      } else if (OB_FAIL(pg_partition->serialize(allocator, tmp_buf, tmp_size))) {
        LOG_WARN("Fail to serialize storage, ", K(ret));
      } else {
        tmp_pair.buf_ = tmp_buf;
        tmp_pair.size_ = tmp_size;
        if (OB_FAIL(pairs.push_back(tmp_pair))) {
          LOG_WARN("failed to push_back SerializePair", K(ret), K(i));
        } else {
          serialize_size += tmp_size;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_mgr_.serialize(allocator, sstable_pair.buf_, sstable_pair.size_, tables_handle))) {
        LOG_WARN("fail to serialize sstable mgr", K(ret));
      } else if (OB_FAIL(recovery_point_data_mgr_.serialize(
                     allocator, recovery_point_pair.buf_, recovery_point_pair.size_))) {
        LOG_WARN("failed serialize recovery point data mgr", K(ret), K_(pkey));
      }
    }
    if (OB_SUCC(ret)) {
      serialize_size += sstable_pair.size_;
      serialize_size += recovery_point_pair.size_;
      if (OB_ISNULL(new_buf = static_cast<char*>(allocator.alloc(serialize_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate serialize buf", K(ret), K_(pkey));
      } else {
        MEMCPY(new_buf, meta_buf, meta_serialize_size);
        copy_size += meta_serialize_size;
        for (int64_t i = 0; OB_SUCC(ret) && i < pg_partition_cnt; ++i) {
          const SerializePair& spair = pairs.at(i);
          MEMCPY(new_buf + copy_size, spair.buf_, spair.size_);
          copy_size += spair.size_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      MEMCPY(new_buf + copy_size, sstable_pair.buf_, sstable_pair.size_);
      copy_size += sstable_pair.size_;
      MEMCPY(new_buf + copy_size, recovery_point_pair.buf_, recovery_point_pair.size_);
      copy_size += recovery_point_pair.size_;
    }

    if (OB_SUCC(ret)) {
      if (copy_size != serialize_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy_size should be equal to serialize_size", K(ret), K(copy_size), K(serialize_size));
      } else {
        FLOG_INFO("succeed to serialize pg storage", K(ret), K(*meta_));
      }
    }
  }

  return ret;
}

// replay storage checkpoint log
int ObPGStorage::deserialize(
    const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  int64_t tmp_pos = pos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0 || pos >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, tmp_pos, &magic_num))) {
    LOG_WARN("failed to decode magic num", K(ret));
  } else {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);
    TCWLockGuard lock_guard(lock_);
    switch (magic_num) {
      case MAGIC_NUM: {
        if (OB_FAIL(deserialize_(buf, buf_len, pos, state, split_info))) {
          LOG_WARN("failed to deserialize pg storage", K(ret));
        }
        break;
      }
      case MAGIC_NUM_2_2_70: {
        if (OB_FAIL(deserialize_2270_(buf, buf_len, pos, state, split_info))) {
          LOG_WARN("failed to deserialize pg storage", K(ret));
        }
        break;
      }
      case MAGIC_NUM_BEFORE_2_2_70: {
        if (OB_FAIL(deserialize_before_2270_(buf, buf_len, pos, state, split_info))) {
          LOG_WARN("failed to deserialize pg storage", K(ret));
        }
        break;
      }
      default: {
        if (OB_FAIL(deserialize_old_partition_(buf, buf_len, pos, state, split_info))) {
          LOG_WARN("failed to deserialize old partition", K(ret));
        }
      }  // default
    }    // switch
  }
  // (Note yanyuan.cxf) why we need set the pkey again?
  if (OB_SUCC(ret)) {
    sstable_mgr_.set_pg_key(pkey_);
    recovery_point_data_mgr_.set_pg_key(pkey_);
    // no need lock
    cached_replica_type_ = meta_->replica_type_;
  }
  return ret;
}

int ObPGStorage::deserialize_old_partition_(
    const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  ObPGPartition* pg_partition = nullptr;
  ObPartitionStoreMeta store_meta;
  ObPartitionGroupMeta pg_meta;
  bool is_old_meta = false;
  const bool is_replay = true;

  if (OB_ISNULL(pg_partition = op_alloc(ObPGPartition))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc pg partition", K(ret));
  } else if (OB_FAIL(pg_partition->init(pkey_, cp_fty_, schema_service_, txs_, pg_, pg_memtable_mgr_))) {
    LOG_WARN("failed to init pg partition", K(ret));
  } else if (OB_FAIL(
                 pg_partition->deserialize(meta_->replica_type_, buf, buf_len, pg_, pos, is_old_meta, store_meta))) {
    LOG_WARN("failed to deserialize pg partition", K(ret));
  } else if (OB_FAIL(register_pg_partition_(pg_partition->get_partition_key(), pg_partition))) {
    LOG_WARN("failed to register pg partition", K(ret), "pkey", pg_partition->get_partition_key());
  } else if (OB_UNLIKELY(!is_old_meta || !store_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old partition should have valid old meta", K(ret), K(is_old_meta), K(store_meta));
  } else if (OB_FAIL(pg_meta.copy_from_store_meta(store_meta))) {
    LOG_WARN("failed to copy from store meta", K(ret), K(store_meta));
    // need set pg partitions of meta !!!!
  } else if (OB_FAIL(insert_pg_partition_arr_(&(pg_meta.partitions_), store_meta.pkey_))) {
    STORAGE_LOG(WARN, "failed to push back pkey to meta partitions", K(ret), K(store_meta), K(pg_meta), K(*meta_));
  } else if (OB_FAIL(set_pg_meta_(pg_meta, is_replay))) {
    LOG_WARN("failed to set pg meta", K(ret), K(pg_meta));
  } else if (OB_FAIL(split_info.assign(store_meta.split_info_))) {
    LOG_WARN("failed to assign split info", K(ret), K(store_meta));
  } else if (OB_FAIL(pg_->get_pg_index()->add_partition(meta_->pg_key_, meta_->pg_key_))) {
    LOG_WARN("failed to add pg index", K(ret), "pkey", meta_->pg_key_);
    if (OB_ENTRY_EXIST == ret) {
      need_clear_pg_index_ = false;
    }
  } else {
    state = store_meta.saved_split_state_;
    pkey_ = meta_->pg_key_;
    pg_memtable_mgr_.set_pkey(pkey_);
  }
  return ret;
}

int ObPGStorage::deserialize_(
    const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  ObPartitionGroupMeta meta;
  ObPartitionStoreMeta store_meta;
  bool is_old_meta = false;
  const bool is_replay = true;

  if (NULL == buf || buf_len <= 0 || pos < 0 || pos >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &magic_num))) {
    LOG_WARN("failed to decode magic num", K(ret));
  } else if (MAGIC_NUM != magic_num) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid magic num", K(ret), K(magic_num), LITERAL_K(MAGIC_NUM));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &log_seq_num_))) {
    LOG_WARN("failed to decode log_seq_num", K(ret));
  } else if (OB_FAIL(meta.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize meta", K(ret));
  } else if (OB_FAIL(set_pg_meta_(meta, is_replay))) {
    LOG_WARN("failed to replay partition group meta", K(ret));
  } else {
    pkey_ = meta.pg_key_;
    pg_memtable_mgr_.set_pkey(pkey_);
    recovery_point_data_mgr_.set_pg_key(pkey_);
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.partitions_.count(); ++i) {
      ObPGPartition* pg_partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      const ObPartitionKey& pkey = meta.partitions_.at(i);
      if (NULL == (pg_partition = op_alloc(ObPGPartition))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "alloc pg partition info error", K(ret), KP(pg_partition), K_(pkey));
      } else if (OB_FAIL(pg_partition->init(pkey, cp_fty_, schema_service_, txs_, pg_, pg_memtable_mgr_))) {
        STORAGE_LOG(
            WARN, "partition info init error", K(ret), K(pkey_), KP_(cp_fty), KP_(schema_service), KP_(txs), K(pkey));
      } else if (OB_FAIL(pg_partition->deserialize(
                     meta_->replica_type_, buf, buf_len, pg_, pos, is_old_meta, store_meta))) {
        STORAGE_LOG(WARN, "Fail to deserialize pg partition info, ", K(ret), K(pkey), "pg key", pkey_);
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage must not null", K(ret), K(pkey), "pg key", pkey_);
      } else if (OB_FAIL(register_pg_partition_(pg_partition->get_partition_key(), pg_partition))) {
        STORAGE_LOG(WARN, "create partition info error", K(ret), "pkey", pg_partition->get_partition_key());
      } else if (OB_FAIL(pg_->get_pg_index()->add_partition(pkey, pkey_))) {
        LOG_WARN("failed to add pg index", K(ret), K(pkey_), K(pkey));
        if (OB_ENTRY_EXIST == ret) {
          need_clear_pg_index_ = false;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(split_info.assign(meta.split_info_))) {
        LOG_WARN("failed to assign split info", K(ret), K(meta));
      } else {
        state = meta.saved_split_state_;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_mgr_.deserialize(buf, buf_len, pos))) {
        LOG_WARN("fail to deserialize buf", K(ret));
      }
    }
    if (OB_SUCC(ret) && pos < buf_len) {
      if (OB_FAIL(recovery_point_data_mgr_.deserialize(buf, buf_len, pos))) {
        LOG_WARN("failed to deserialize recovery point data mgr", K(ret), K_(pkey));
      }
    }
  }
  FLOG_INFO("deserialize pg storage", K(ret), K(*this));

  return ret;
}

int ObPGStorage::deserialize_2270_(
    const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  ObPartitionGroupMeta meta;
  ObPartitionStoreMeta store_meta;
  bool is_old_meta = false;
  const bool is_replay = true;

  if (NULL == buf || buf_len <= 0 || pos < 0 || pos >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &magic_num))) {
    LOG_WARN("failed to decode magic num", K(ret));
  } else if (MAGIC_NUM_2_2_70 != magic_num) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid magic num", K(ret), K(magic_num), LITERAL_K(MAGIC_NUM_2_2_70));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &log_seq_num_))) {
    LOG_WARN("failed to decode log_seq_num", K(ret));
  } else if (OB_FAIL(meta.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize meta", K(ret));
  } else if (OB_FAIL(set_pg_meta_(meta, is_replay))) {
    LOG_WARN("failed to replay partition group meta", K(ret));
  } else {
    pkey_ = meta.pg_key_;
    pg_memtable_mgr_.set_pkey(pkey_);
    recovery_point_data_mgr_.set_pg_key(pkey_);
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.partitions_.count(); ++i) {
      ObPGPartition* pg_partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      const ObPartitionKey& pkey = meta.partitions_.at(i);
      if (NULL == (pg_partition = op_alloc(ObPGPartition))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "alloc pg partition info error", K(ret), KP(pg_partition), K_(pkey));
      } else if (OB_FAIL(pg_partition->init(pkey, cp_fty_, schema_service_, txs_, pg_, pg_memtable_mgr_))) {
        STORAGE_LOG(
            WARN, "partition info init error", K(ret), K(pkey_), KP_(cp_fty), KP_(schema_service), KP_(txs), K(pkey));
      } else if (OB_FAIL(pg_partition->deserialize(
                     meta_->replica_type_, buf, buf_len, pg_, pos, is_old_meta, store_meta))) {
        STORAGE_LOG(WARN, "Fail to deserialize pg partition info, ", K(ret), K(pkey), "pg key", pkey_);
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage must not null", K(ret), K(pkey), "pg key", pkey_);
      } else if (OB_FAIL(register_pg_partition_(pg_partition->get_partition_key(), pg_partition))) {
        STORAGE_LOG(WARN, "create partition info error", K(ret), "pkey", pg_partition->get_partition_key());
      } else if (OB_FAIL(pg_->get_pg_index()->add_partition(pkey, pkey_))) {
        LOG_WARN("failed to add pg index", K(ret), K(pkey_), K(pkey));
        if (OB_ENTRY_EXIST == ret) {
          need_clear_pg_index_ = false;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(split_info.assign(meta.split_info_))) {
        LOG_WARN("failed to assign split info", K(ret), K(meta));
      } else {
        state = meta.saved_split_state_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(alloc_file_for_old_replay())) {
        LOG_WARN("alloc file for old replay fail", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(recovery_point_data_mgr_.deserialize(buf, buf_len, pos))) {
        LOG_WARN("failed to deserialize recovery point data mgr", K(ret), K_(pkey));
      }
    }
  }
  FLOG_INFO("deserialize pg storage", K(ret), K(*this));

  return ret;
}

int ObPGStorage::deserialize_before_2270_(
    const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  ObPartitionGroupMeta meta;
  ObPartitionStoreMeta store_meta;
  bool is_old_meta = false;
  const bool is_replay = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0 || pos >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &magic_num))) {
    LOG_WARN("failed to decode magic num", K(ret));
  } else if (MAGIC_NUM_BEFORE_2_2_70 != magic_num) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid magic num", K(ret), K(magic_num), LITERAL_K(MAGIC_NUM_BEFORE_2_2_70));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &log_seq_num_))) {
    LOG_WARN("failed to decode log_seq_num", K(ret));
  } else if (OB_FAIL(meta.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize meta", K(ret));
  } else if (OB_FAIL(set_pg_meta_(meta, is_replay))) {
    LOG_WARN("failed to replay partition group meta", K(ret));
  } else {
    pkey_ = meta.pg_key_;
    pg_memtable_mgr_.set_pkey(pkey_);
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.partitions_.count(); ++i) {
      ObPGPartition* pg_partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      const ObPartitionKey& pkey = meta.partitions_.at(i);
      if (NULL == (pg_partition = op_alloc(ObPGPartition))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "alloc pg partition info error", K(ret), KP(pg_partition), K_(pkey));
      } else if (OB_FAIL(pg_partition->init(pkey, cp_fty_, schema_service_, txs_, pg_, pg_memtable_mgr_))) {
        STORAGE_LOG(
            WARN, "partition info init error", K(ret), K(pkey_), KP_(cp_fty), KP_(schema_service), KP_(txs), K(pkey));
      } else if (OB_FAIL(pg_partition->deserialize(
                     meta_->replica_type_, buf, buf_len, pg_, pos, is_old_meta, store_meta))) {
        STORAGE_LOG(WARN, "Fail to deserialize pg partition info, ", K(ret), K(pkey), "pg key", pkey_);
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage must not null", K(ret), K(pkey), "pg key", pkey_);
      } else if (OB_FAIL(register_pg_partition_(pg_partition->get_partition_key(), pg_partition))) {
        STORAGE_LOG(WARN, "create partition info error", K(ret), K(pkey_), K(pkey));
      } else if (OB_FAIL(pg_->get_pg_index()->add_partition(pkey, pkey_))) {
        LOG_WARN("failed to add pg index", K(ret), K(pkey_), K(pkey));
        if (OB_ENTRY_EXIST == ret) {
          need_clear_pg_index_ = false;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(split_info.assign(meta.split_info_))) {
        LOG_WARN("failed to assign split info", K(ret), K(meta));
      } else {
        state = meta.saved_split_state_;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(alloc_file_for_old_replay())) {
        LOG_WARN("alloc file for old replay fail", K(ret));
      } else {
        sstable_mgr_.set_pg_key(pkey_);
        recovery_point_data_mgr_.set_pg_key(pkey_);
      }
    }
    FLOG_INFO("deserialize pg storage v1", K(ret), K(*this));
  }
  return ret;
}

int ObPGStorage::get_pg_partition(const common::ObPartitionKey& pkey, ObPGPartitionGuard& guard)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey));
  } else if (OB_FAIL(guard.set_pg_partition(pkey, *(pg_->get_pg_partition_map())))) {
    STORAGE_LOG(WARN, "get pg partition error", K(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPGStorage::check_pg_partition_exist(const ObPartitionKey& pkey, bool& exist)
{
  int ret = OB_SUCCESS;
  ObPGPartitionGuard pg_partition_guard;
  exist = false;
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey));
  } else if (OB_FAIL(pg_partition_guard.set_pg_partition(pkey, *(pg_->get_pg_partition_map())))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      exist = false;
    } else {
      LOG_WARN("fail to set pg partition", K(ret), K(pkey));
    }
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    exist = false;
  } else {
    exist = true;
  }
  return ret;
}

int ObPGStorage::register_pg_partition_(const common::ObPartitionKey& pkey, ObPGPartition* pg_info)
{
  int ret = OB_SUCCESS;

  if (!pkey.is_valid() || pkey.get_table_id() == OB_MIN_USER_TABLE_ID) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (NULL == pg_info) {
    if (NULL == (pg_info = op_alloc(ObPGPartition))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc pg partition info error", K(ret), KP(pg_info), K(pkey));
    } else if (OB_FAIL(pg_info->init(pkey, cp_fty_, schema_service_, txs_, pg_, pg_memtable_mgr_))) {
      STORAGE_LOG(WARN, "partition info init error", K(ret), K(pkey), KP_(cp_fty), KP_(schema_service), KP_(txs));
      op_free(pg_info);
      pg_info = nullptr;
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_->get_pg_partition_map()->insert_and_get(pkey, pg_info))) {
      STORAGE_LOG(WARN, "partition map insert and get error", K(ret), K(pkey));
      op_free(pg_info);
    } else {
      // insert into partition_
      if (OB_FAIL(partition_list_.add(pkey, ObModIds::OB_PG_PARTITION_MAP))) {
        STORAGE_LOG(WARN, "partition list insert error", K(ret), K(pkey));
      } else {
        TRANS_LOG(INFO, "create partition info success", K(pkey), KP(pg_info));
      }
      // need revert
      pg_->get_pg_partition_map()->revert(pg_info);

      if (OB_FAIL(ret)) {
        pg_->get_pg_partition_map()->del(pkey);
      }
    }
  }
  return ret;
}

bool ObPGStorage::is_empty_pg()
{
  bool is_empty = false;
  is_empty = (get_partition_cnt() > 0 ? false : true);
  return is_empty;
}

int ObPGStorage::get_all_pg_partition_keys_(ObPartitionArray& pkeys, const bool include_trans_table)
{
  int ret = OB_SUCCESS;
  pkeys.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (pkey_.is_pg()) {
    GetAllPGPartitionKeyFunctor functor(pkeys);
    if (OB_FAIL(partition_list_.for_each(functor))) {
      TRANS_LOG(WARN, "for each pg partition keys error", K(ret), K_(pkey), K(pkeys));
    }
  } else if (OB_FAIL(pkeys.push_back(pkey_))) {
    STORAGE_LOG(WARN, "pkeys push back error", K(ret), K_(pkey));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret) && include_trans_table) {
    ObPartitionKey trans_table_pkey;
    if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(pkey_))) {
      LOG_WARN("failed to generate trans_table_pkey", K(ret), K_(pkey));
    } else if (OB_ENTRY_EXIST == pg_->get_pg_partition_map()->contains_key(trans_table_pkey)) {
      if (OB_FAIL(pkeys.push_back(trans_table_pkey))) {
        STORAGE_LOG(WARN, "failed to push back pkey", K(ret), K_(pkey), K(trans_table_pkey));
      }
    }
  }

  return ret;
}

int ObPGStorage::check_can_replay_add_partition_to_pg_log(
    const common::ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts, bool& can_replay)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!pkey.is_valid() || pkey.is_pg()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(log_id), K(pkey_));
  } else if (is_removed_) {
    STORAGE_LOG(INFO, "partition group is garbaging, no need to replay", K(pkey), K(log_id));
    can_replay = false;
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = guard.get_pg_partition())) {
      can_replay = true;
      const int64_t publish_version = get_publish_version();
      if (log_ts <= publish_version) {
        TRANS_LOG(INFO, "no need to replay current log", K(pkey), K(log_id), K(log_ts), K(publish_version));
        can_replay = false;
      }
    } else {
      can_replay = false;
    }
  }

  return ret;
}

int ObPGStorage::check_can_replay_remove_partition_from_pg_log(
    const common::ObPartitionKey& pkey, const uint64_t log_id, bool& can_replay)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!pkey.is_valid() || pkey.is_pg()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(pkey_));
  } else if (is_removed_) {
    STORAGE_LOG(INFO, "partition group is garbaging, no need to replay", K(pkey), K(log_id));
    can_replay = false;
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = guard.get_pg_partition())) {
      can_replay = false;
    } else {
      can_replay = true;
    }
  }

  return ret;
}

int ObPGStorage::create_partition_group(const ObCreatePGParam& param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(param));
  } else {
    ObBucketWLockAllGuard guard(bucket_lock_);
    TCWLockGuard lock_guard(lock_);
    ObPartitionGroupMeta meta;
    if (is_removed_) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K_(pkey));
    } else if (OB_FAIL(set_storage_file(const_cast<ObStorageFileHandle&>(*param.file_handle_)))) {
      LOG_WARN("fail to assign file handle", K(ret));
    } else if (OB_FAIL(init_pg_meta_(param, meta))) {
      LOG_WARN("failed to init pg meta", K(ret), K(param));
    } else if (OB_FAIL(set_pg_meta_(meta, !param.write_slog_))) {
      LOG_WARN("failed to set pg meta", K(ret), K(meta));
    } else if (param.write_slog_) {
      ObCreatePartitionGroupLogEntry entry;
      const int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_CREATE_PARTITION_GROUP);
      const ObStorageLogAttribute log_attr(pkey_.get_tenant_id(), meta_->storage_info_.get_pg_file_id());
      if (OB_FAIL(recovery_point_data_mgr_.enable_write_slog(sstable_mgr_))) {
        LOG_WARN("failed to enable write slog for recovery point data mgr", K(ret), K_(pkey));
      } else if (OB_FAIL(sstable_mgr_.enable_write_log())) {
        LOG_WARN("fail to enable write log", K(ret));
      } else if (OB_FAIL(entry.meta_.deep_copy(*meta_))) {
        STORAGE_LOG(WARN, "meta deep copy error", K(ret), K(param.info_), K(meta), K(*this));
      } else if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, entry))) {
        STORAGE_LOG(WARN, "write create partition group meta slog error", K(ret), K(param.info_), K(meta));
      } else {
        ObTenantFileKey file_key(pkey_.get_tenant_id(), meta_->storage_info_.get_pg_file_id());
        file_mgr_ = const_cast<ObBaseFileMgr*>(param.file_mgr_);
        if (OB_FAIL(file_mgr_->write_add_pg_slog(file_key, pkey_))) {
          LOG_WARN("fail to write add pg slog to tenant file", K(ret));
        } else {
          LOG_INFO("succeed to write create partition group log", K(param));
        }
      }
    }

    if (OB_FAIL(ret)) {
      file_handle_.reset();
    } else {
      // save replica_type for the first time
      cached_replica_type_ = meta_->replica_type_;
    }
  }
  return ret;
}

int ObPGStorage::insert_pg_partition_arr_(ObPartitionArray* arr, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(arr) || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(*arr), K(pkey), K(meta_));
  } else {
    bool hit = false;
    for (int64_t i = 0; i < arr->count(); ++i) {
      if (pkey == arr->at(i)) {
        hit = true;
        break;
      }
    }
    if (!hit) {
      if (OB_FAIL(arr->push_back(pkey))) {
        STORAGE_LOG(WARN, "array push back error", K(ret), K(*arr), K(pkey));
      }
    }
  }

  return ret;
}

int ObPGStorage::del_pg_partition_arr_(ObPartitionArray* arr, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(arr) || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(*arr), K(pkey), K(meta_));
  } else {
    int64_t i = 0;
    for (i = 0; i < arr->count(); ++i) {
      if (pkey == arr->at(i)) {
        break;
      }
    }
    if (i >= arr->count()) {
      // no ret
      STORAGE_LOG(WARN, "partition array not contain curr pkey", K(pkey), K(*arr), K(*meta_));
    } else if (OB_FAIL(arr->remove(i))) {
      STORAGE_LOG(WARN, "array remove pkey error", K(ret), K(*arr), K(pkey));
    } else {
      STORAGE_LOG(INFO, "del pg partition from pg meta partition array success", K(pkey), "pg key", pkey_);
    }
  }

  return ret;
}

int ObPGStorage::init_pg_meta_(const ObCreatePGParam& param, ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  bool is_split = false;
  if (!param.is_valid() || (param.info_.get_pg_file_id() > 0 && param.file_handle_->is_valid() &&
                               param.info_.get_pg_file_id() != param.file_handle_->get_storage_file()->get_file_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else {
    pg_meta.reset();
    pg_meta.pg_key_ = pkey_;
    pg_meta.is_restore_ = static_cast<int16_t>(param.is_restore_);
    pg_meta.replica_type_ = param.replica_type_;
    pg_meta.replica_property_ = param.replica_property_;
    pg_meta.report_status_.reset();
    pg_meta.report_status_.data_version_ = param.data_version_;
    pg_meta.create_schema_version_ = param.info_.get_data_info().get_schema_version();
    pg_meta.saved_split_state_ = param.split_state_;
    pg_meta.create_timestamp_ = param.create_timestamp_;
    pg_meta.create_frozen_version_ = param.create_frozen_version_;
    pg_meta.last_restore_log_id_ = param.last_restore_log_id_;
    pg_meta.restore_snapshot_version_ = param.restore_snapshot_version_;
    pg_meta.migrate_status_ = param.migrate_status_;
    if (OB_FAIL(pg_meta.storage_info_.deep_copy(param.info_))) {
      LOG_WARN("failed to deep copy storage info", K(ret), K(param.info_));
    } else if (OB_FAIL(pg_meta.split_info_.assign(param.split_info_))) {
      LOG_WARN("failed to assign split info", K(ret), K(param.split_info_));
    } else {
      if (param.split_state_ >= ObPartitionSplitStateEnum::LEADER_WAIT_SPLIT) {
        is_split = true;
      }

      if (OB_INVALID_DATA_FILE_ID == pg_meta.storage_info_.get_pg_file_id()) {
        pg_meta.storage_info_.set_pg_file_id(file_handle_.get_storage_file()->get_file_id());
      }
    }
  }
  return ret;
}

int ObPGStorage::set_pg_meta_(const ObPartitionGroupMeta& pg_meta, const bool is_replay)
{
  int ret = OB_SUCCESS;
  if (!pg_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pg_meta));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (OB_FAIL(meta_->deep_copy(pg_meta))) {
    STORAGE_LOG(WARN, "failed to copy saved_storage_info", K(ret));
  } else {
    if (is_replay) {
      ObMigrateStatus reboot_status = OB_MIGRATE_STATUS_MAX;
      if (OB_FAIL(ObMigrateStatusHelper::trans_reboot_status(meta_->migrate_status_, reboot_status))) {
        LOG_WARN("failed to trans_migrate_reboot_status", K(ret), K_(*meta));
      } else if (meta_->migrate_status_ != reboot_status) {
        LOG_INFO("override migrate status after reboot", "old", meta_->migrate_status_, K(reboot_status));
        meta_->migrate_status_ = reboot_status;
      }
    }

    if (OB_SUCC(ret)) {
      if (!ObReplicaTypeCheck::is_replica_with_memstore(meta_->replica_type_)) {
        meta_->storage_info_.get_data_info().reset_for_no_memtable_replica();
      }
      FLOG_INFO("success to init_pg_meta", K(*this));
    }
  }
  return ret;
}

int ObPGStorage::replay_partition_schema_version_change_log(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  ObMemtable* mem_store = NULL;
  // no memtable ctx
  ObStoreCtx ctx;
  ObStorageWriterGuard guard(ctx, &pg_memtable_mgr_, true, true);

  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  const int64_t pkey_cnt = get_partition_cnt();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (pkey_cnt <= 0) {
    STORAGE_LOG(INFO, "empty pg, no need to replay clog, maybe restart scenario", K(schema_version));
  } else if (!need_create_memtable()) {
    STORAGE_LOG(INFO, "split source partition, no need to replay schema change log", K_(pkey));
  } else if (!pg_memtable_mgr_.has_memtable()) {
    STORAGE_LOG(INFO, "current pg has no memstores, maybe data replica, need skip", K(schema_version), K_(pkey));
  } else if (OB_FAIL(guard.refresh_and_protect_pg_memtable(*this, tables_handle))) {
    STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_), K(schema_version));
  } else if (OB_FAIL(tables_handle.get_last_memtable(mem_store))) {
    STORAGE_LOG(WARN, "failed to get memtable", K(ret), K(pkey_), K(schema_version));
  } else if (OB_ISNULL(mem_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "write_memtable must not null", K(ret));
  } else if (OB_FAIL(mem_store->replay_schema_version_change_log(schema_version))) {
    STORAGE_LOG(WARN, "fail to replay partition schema version change log ", K(ret), K(schema_version));
  } else {
    // do nothing
  }
  return ret;
}

// replay pg clog
int ObPGStorage::replay(const ObStoreCtx& ctx, const char* data, const int64_t data_len, bool& replayed)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  ObMemtable* mem_store = NULL;
  ObStorageWriterGuard guard(ctx, &pg_memtable_mgr_, true, true);
  const int64_t pkey_cnt = get_partition_cnt();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (NULL == ctx.mem_ctx_) {
    TRANS_LOG(ERROR, "unexpected memtable context", K(ctx));
    ret = OB_ERR_UNEXPECTED;
  } else if (pkey_cnt <= 0) {
    STORAGE_LOG(INFO, "empty pg, no need to replay clog, maybe restart scenario", K(ctx));
    replayed = false;
  } else {
    // big row log should ensure that all redo logs replay on the same memstore
    ObIMemtable* memtable = NULL;
    ObMemtableMutatorMeta meta;
    int64_t pos = 0;
    if (OB_FAIL(meta.deserialize(data, data_len, pos))) {
      STORAGE_LOG(ERROR, "meta deserialize error", K(ctx));
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (ObTransRowFlag::is_normal_row(meta.get_flags())) {
        if (OB_FAIL(guard.refresh_and_protect_pg_memtable(*this, tables_handle))) {
          STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
        } else if (OB_ISNULL(mem_store) && OB_FAIL(tables_handle.get_last_memtable(mem_store))) {
          STORAGE_LOG(WARN, "failed to get memtable", K(ret), K(pkey_));
        } else if (OB_ISNULL(mem_store)) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "write_memtable must not null", K(ret));
        }
      } else if (ObTransRowFlag::is_big_row_start(meta.get_flags())) {
        if (NULL != (memtable = ctx.mem_ctx_->get_memtable_for_cur_log())) {
          mem_store = static_cast<ObMemtable*>(memtable);
          TRANS_LOG(INFO, "may be retry replay the first big row redo log", KP(memtable), K(ctx));
        } else if (OB_FAIL(guard.refresh_and_protect_pg_memtable(*this, tables_handle))) {
          STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
        } else if (OB_FAIL(tables_handle.get_last_memtable(mem_store))) {
          STORAGE_LOG(WARN, "failed to get memtable", K(ret), K(pkey_));
        } else if (OB_ISNULL(mem_store)) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "write_memtable must not null", K(ret));
        } else if (OB_FAIL(ctx.mem_ctx_->set_memtable_for_cur_log(static_cast<ObIMemtable*>(mem_store)))) {
          TRANS_LOG(WARN, "set memtable for current log error", K(ret), K(ctx));
        } else {
          // do nothing
        }
      } else if (ObTransRowFlag::is_big_row_mid(meta.get_flags()) || ObTransRowFlag::is_big_row_end(meta.get_flags())) {
        if (NULL == (memtable = ctx.mem_ctx_->get_memtable_for_cur_log())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "memtable is null, unexpected error", K(ret), KP(memtable), K(ctx));
        } else {
          // record the memstore when first lob redo log replay
          mem_store = static_cast<ObMemtable*>(memtable);
        }
      } else {
        TRANS_LOG(ERROR, "invalid row flag, unexpected error", K(meta), K(log));
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == mem_store) {
        TRANS_LOG(ERROR, "memstore is null, unexpected error", KP(mem_store), K(ctx));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(mem_store->replay(ctx, data, data_len))) {
        STORAGE_LOG(WARN, "Fail to replay mem store, ", K(ret));
        // clear memstore in memtable ctx when the last lob redo log replay
      } else if (ObTransRowFlag::is_big_row_end(meta.get_flags())) {
        if (mem_store != ctx.mem_ctx_->get_memtable_for_cur_log()) {
          TRANS_LOG(WARN, "unexpected memstore", KP(mem_store), KP(ctx.mem_ctx_->get_memtable_for_cur_log()));
          ret = OB_ERR_UNEXPECTED;
        } else {
          ctx.mem_ctx_->clear_memtable_for_cur_log();
          TRANS_LOG(INFO, "clear memtable for cur log success", "mem_ctx", *(ctx.mem_ctx_), KP(mem_store));
        }
      } else {
        // do nothing
      }
    } else if (OB_EMPTY_PG == ret) {
      ret = OB_SUCCESS;
      replayed = false;
      STORAGE_LOG(INFO, "empty pg, no need to replay clog, maybe restart scenario", K_(pkey), K(ctx));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPGStorage::replay_pg_partition(const common::ObPartitionKey& pkey, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_rollback_partition_map = false;
  bool need_rollback_pg_index = false;
  bool exist = false;
  // wrlock_

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey));
  } else if (OB_FAIL(check_pg_partition_exist(pkey, exist))) {
    STORAGE_LOG(WARN, "fail to check pg partition exist", K(ret));
  } else if (exist) {
    STORAGE_LOG(INFO, "pg partition already exist, do not replay", K(ret));
  } else if (OB_FAIL(register_pg_partition_(pkey, NULL))) {
    STORAGE_LOG(WARN, "create pg partition error", K(ret), "pg_key", pkey_, K(pkey));
  } else {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);
    TCWLockGuard lock_guard(lock_);
    need_rollback_partition_map = true;
    if (OB_FAIL(insert_pg_partition_arr_(&(meta_->partitions_), pkey))) {
      STORAGE_LOG(WARN, "failed to push back pkey to meta.partitions", K(ret), "pg key", pkey_, K(pkey));
    } else if (OB_FAIL(pg_->get_pg_index()->add_partition(pkey, pkey_))) {
      LOG_WARN("failed to add pg index", K(ret), K(pkey_), K(pkey));
      if (OB_ENTRY_EXIST == ret) {
        need_clear_pg_index_ = false;
      }
    } else {
      need_rollback_pg_index = true;
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("replay create pg partition success", "pg_key", pkey_, K(pkey), K(log_id), K(*meta_));
  } else {
    if (need_rollback_partition_map) {
      if (OB_SUCCESS != (tmp_ret = pg_->get_pg_partition_map()->del(pkey))) {
        LOG_WARN("failed to del pg partition from partition map", K(tmp_ret), K(pkey));
        ob_abort();
      }
      partition_list_.remove_latest(pkey);
    }

    if (need_rollback_pg_index) {
      if (OB_SUCCESS != (tmp_ret = pg_->get_pg_index()->remove_partition(pkey))) {
        LOG_ERROR("failed to remove pg index", K(tmp_ret), K(pkey_), K(pkey));
        ob_abort();
      }
    }
  }

  return ret;
}

int ObPGStorage::write_create_partition_slog(const common::ObPartitionKey& pkey, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(pkey));
  } else {
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_ADD_PARTITION_TO_PG);
    ObChangePartitionLogEntry entry;
    entry.partition_key_ = pkey;
    entry.replica_type_ = meta_->replica_type_;
    entry.pg_key_ = pkey_;
    entry.log_id_ = log_id;
    const ObStorageLogAttribute log_attr(pkey_.get_tenant_id(), meta_->storage_info_.get_pg_file_id());
    if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, entry))) {
      STORAGE_LOG(WARN, "fail to write add partition log.", K(ret), K(pkey_), K(meta_->storage_info_));
    } else {
      LOG_INFO("succeed to write create partition slog", K(pkey_), K(pkey));
    }
  }
  return ret;
}

int ObPGStorage::write_remove_pg_partition_trans_(
    const common::ObPartitionKey& pkey, const uint64_t log_id, const ObPartitionGroupMeta& next_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(pkey), K(log_id));
  } else {
    ObChangePartitionLogEntry entry;
    entry.replica_type_ = meta_->replica_type_;
    entry.partition_key_ = pkey;
    entry.pg_key_ = pkey_;
    entry.log_id_ = log_id;
    ObUpdatePartitionGroupMetaLogEntry update_meta_log_entry;

    if (OB_FAIL(update_meta_log_entry.meta_.deep_copy(next_meta))) {
      STORAGE_LOG(WARN, "failed to deep copy meta", K(ret));
    } else if (OB_FAIL(SLOGGER.begin(OB_LOG_CS_DEL_PARTITION))) {
      STORAGE_LOG(WARN, "fail to begin commit log.", K(ret), K(pkey));
    } else {
      const ObStorageLogAttribute log_attr(pkey_.get_tenant_id(), meta_->storage_info_.get_pg_file_id());

      if (OB_FAIL(SLOGGER.write_log(
              ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_REMOVE_PARTITION_FROM_PG), log_attr, entry))) {
        STORAGE_LOG(WARN, "fail to write remove partition from pg log.", K(ret), K(pkey));
      } else if (OB_FAIL(SLOGGER.write_log(
                     ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_UPDATE_PARTITION_GROUP_META),
                     log_attr,
                     update_meta_log_entry))) {
        STORAGE_LOG(WARN, "failed to write update partition meta slog", K(ret));
      }

      if (OB_SUCC(ret)) {
        int64_t lsn = 0;
        if (OB_FAIL((SLOGGER.commit(lsn)))) {
          STORAGE_LOG(WARN, "fail to commit log.", K(ret), K(lsn));
        } else {
          STORAGE_LOG(INFO, "write_remove_pg_partition_trans success", K(pkey), K(log_id), K(lsn), K(*meta_));
        }
      } else {
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          STORAGE_LOG(WARN, "failed to rollback slog", K(ret), K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObPGStorage::create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
    const uint64_t data_table_id, const ObCreatePartitionArg& arg, const bool in_slog_trans, const bool is_replay,
    const uint64_t log_id, ObTablesHandle& sstables_handle)
{
  int ret = OB_SUCCESS;
  ObCreatePartitionParam create_param;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(pkey));
  } else if (pkey.get_tenant_id() != pkey_.get_tenant_id()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tenant id not match", K(ret), K(pkey), K(pkey_));
  } else if (OB_FAIL(create_param.extract_from(arg))) {
    STORAGE_LOG(WARN, "failed to extract create_param", KR(ret), K(pkey), K(arg));
  } else {
    ret = create_pg_partition(
        pkey, multi_version_start, data_table_id, create_param, in_slog_trans, is_replay, log_id, sstables_handle);
  }
  return ret;
}

int ObPGStorage::create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
    const uint64_t data_table_id, const ObCreatePartitionParam& arg, const bool in_slog_trans, const bool is_replay,
    const uint64_t log_id, ObTablesHandle& sstables_handle)
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  ret = create_pg_partition_(
      pkey, multi_version_start, data_table_id, arg, in_slog_trans, is_replay, log_id, sstables_handle);

  return ret;
}

int ObPGStorage::get_create_schema_version(int64_t& create_schema_verison)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized.", K(ret));
  } else {
    ObBucketTryRLockAllGuard guard(bucket_lock_);
    // try all lock guard fail
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
      LOG_WARN("get max pg schema version rdlock error", K(ret), K(pkey_));
    } else {
      TCRLockGuard lock_guard(lock_);
      create_schema_verison = meta_->create_schema_version_;
    }
  }

  return ret;
}

int ObPGStorage::get_create_schema_version(const ObPartitionKey& pkey, int64_t& create_schema_verison)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_idx = get_bucket_idx_(pkey);
  ObBucketRLockGuard bucket_guard(bucket_lock_, bucket_idx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().get_create_schema_version(create_schema_verison))) {
      LOG_WARN("failed to add ssstable", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPGStorage::get_replica_state(const bool disable_replay_log, ObPartitionReplicaState& state)
{
  int ret = OB_SUCCESS;
  state = OB_UNKNOWN_REPLICA;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized.", K(ret));
  } else {
    TCRLockGuard lock_guard(lock_);
    if (0 != meta_->is_restore_) {
      state = OB_RESTORE_REPLICA;
    } else if (disable_replay_log && !pg_memtable_mgr_.has_memtable()) {
      state = OB_PERMANENT_OFFLINE_REPLICA;
    } else {
      state = OB_NORMAL_REPLICA;
    }
  }

  return ret;
}

int ObPGStorage::check_need_minor_freeze(const uint64_t log_id, bool& need_freeze)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObIArray<ObITable*>& memtables = handle.get_tables();
  ObSavedStorageInfoV2 info;
  ObBaseStorageInfo clog_info;
  need_freeze = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_memtables(handle))) {
    STORAGE_LOG(WARN, "get memtables failed", K_(pkey), K(ret));
  } else if (memtables.count() >= 2) {
    // has frozen memstore
    ObMemtable* frozen_memtable = static_cast<ObMemtable*>(memtables.at(memtables.count() - 2));
    if (OB_FAIL(frozen_memtable->get_base_storage_info(info))) {
      STORAGE_LOG(WARN, "get base storage info failed", K_(pkey), K(ret));
    } else if (info.get_data_info().get_last_replay_log_id() < log_id) {
      need_freeze = true;
    }
    // for Log replica, check last_replay_log_id and current log_id
  } else if (OB_FAIL(get_saved_clog_info(clog_info))) {
    STORAGE_LOG(WARN, "fail to get saved clog info", K(ret), K(pkey_));
  } else if (clog_info.get_last_replay_log_id() < log_id) {
    need_freeze = true;
  } else {
    // do nothing
  }

  return ret;
}

int ObPGStorage::set_emergency_release()
{
  int ret = OB_SUCCESS;

  ObTablesHandle handle;
  ObIArray<ObITable*>& memtables = handle.get_tables();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_memtables(handle))) {
    STORAGE_LOG(WARN, "get memtables failed", K_(pkey), K(ret));
  } else if (memtables.count() >= 2) {
    for (int64_t i = 0; i < memtables.count() - 1; ++i) {
      ObMemtable* frozen_memtable = static_cast<ObMemtable*>(memtables.at(i));
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(frozen_memtable)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "memtable is NULL", K(ret), K(pkey_));
      } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = frozen_memtable->set_emergency(true)))) {
        STORAGE_LOG(WARN, "fail to set memtable emergency release", K(tmp_ret), K(pkey_), K(*frozen_memtable));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObPGStorage::check_log_or_data_changed(
    const ObBaseStorageInfo& clog_info, bool& log_changed, bool& need_freeze_data)
{
  int ret = OB_SUCCESS;
  ObTableHandle table_handle;

  log_changed = false;
  need_freeze_data = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(check_need_minor_freeze(clog_info.get_last_replay_log_id(), log_changed))) {
    STORAGE_LOG(WARN, "fail to check log changed", K(ret), K(pkey_));
  } else {
    memtable::ObMemtable* memtable = NULL;
    if (OB_FAIL(pg_memtable_mgr_.get_active_memtable(table_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "fail to get active memtable", K(ret), K(pkey_));
      }
    } else if (OB_FAIL(table_handle.get_memtable(memtable))) {
      STORAGE_LOG(WARN, "fail to get memtable", K(ret), K(pkey_));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, " memtable is NULL", K(ret), K(pkey_));
    } else {
      need_freeze_data = memtable->not_empty();
    }
  }

  if (OB_SUCC(ret) && log_changed && !need_freeze_data) {
    ObDataStorageInfo data_info;
    if (OB_FAIL(get_saved_data_info(data_info))) {
      STORAGE_LOG(WARN, "fail to get saved data info", K(ret), K(pkey_));
    } else if (clog_info.get_last_replay_log_id() > data_info.get_last_replay_log_id()) {
      need_freeze_data = true;
    }
  }

  return ret;
}

int ObPGStorage::create_memtable(const bool in_slog_trans, const bool is_replay, const bool ignore_memstore_percent)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to create memtable", K(ret), K_(pkey));
  } else {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);
    ObTableHandle active_memtable_handle;
    if (is_removed_) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K_(pkey));
    } else if (!ObReplicaTypeCheck::is_replica_with_memstore(meta_->replica_type_)) {
      LOG_INFO("replica type is not with memstore, no need create memtable", K(meta_));
    } else if (OB_FAIL(create_trans_table_partition_(in_slog_trans, is_replay))) {
      STORAGE_LOG(WARN, "failed to create trans table partition", K(ret), K_(meta));
    } else if (!ignore_memstore_percent && 0 == meta_->replica_property_.get_memstore_percent()) {
      LOG_INFO("replica has no memtable, skip", K(meta_));
    } else if (OB_FAIL(pg_memtable_mgr_.get_active_memtable(active_memtable_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "get active memtable error", K(ret), K(*meta_));
        // create memtable for the frist time
      } else if (OB_FAIL(pg_memtable_mgr_.create_memtable(meta_->storage_info_.get_data_info(), meta_->pg_key_))) {
        STORAGE_LOG(WARN, "create memtable error", K(ret), K(active_memtable_handle), K_(meta));
      } else {
        // do nothing
      }
    } else {
      // active memstore already exist
    }
  }

  return ret;
}

int ObPGStorage::remove_pg_partition_from_pg(
    const ObPartitionKey& pkey, const bool write_slog_trans, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
#ifdef ERRSIM
  ret = E(EventTable::EN_SKIP_DROP_PG_PARTITION) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    return OB_SUCCESS;
  }
#endif

  // for compatibility with 2.2.0, log_id maybe 0
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(log_id), K(*meta_));
  } else if (!pkey_.is_pg()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected pg key", K(ret), K(*meta_), K(pkey));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(pkey_));
  } else if (OB_FAIL(check_pg_partition_exist(pkey, is_exist))) {
    LOG_WARN("failed to check pg partition exist", K(ret), K(pkey), K(*meta_));
  } else if (!is_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition not exist, cannot remove", K(ret), K(pkey), K(*meta_));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      STORAGE_LOG(WARN, "failed to copy meta", K(ret), K_(pkey));
    } else if (OB_FAIL(del_pg_partition_arr_(&(next_meta.partitions_), pkey))) {
      STORAGE_LOG(WARN, "failed to push back pkey to meta.partitions", K(ret), "pg key", pkey_, K(pkey));
    } else {  // should not fail
      if (next_meta.partitions_.empty()) {
        next_meta.saved_split_state_ = ObPartitionSplitStateEnum::FOLLOWER_INIT;
      }
      next_meta.ddl_seq_num_++;

      if (write_slog_trans && OB_FAIL(write_remove_pg_partition_trans_(pkey, log_id, next_meta))) {
        LOG_WARN("failed to write remove pg partition trans", K(ret), K(pkey), K(next_meta));
      } else {
        if (OB_FAIL(pg_->get_pg_index()->remove_partition(pkey))) {
          STORAGE_LOG(ERROR, "remove pg index partition error", K(ret), K(pkey));
          ob_abort();
        }

        if (OB_FAIL(pg_->get_pg_partition_map()->del(pkey))) {
          STORAGE_LOG(ERROR, "partition map delete cur partition key error", K(ret), K(pkey), K(*meta_));
          ob_abort();
        }

        partition_list_.remove_latest(pkey);

        switch_meta_(next_meta_ptr);
      }
    }
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "remove pg partition from pg success", K(pkey), K(log_id), K(*meta_));
  }

  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::clear_non_reused_stores(const ObPartitionKey& pkey, bool& cleared_memstore)
{
  int ret = OB_SUCCESS;
  bool can_clear_non_reused_stores = false;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);

  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(pkey_));
  } else if (pkey_.is_pg() && pkey_ != pkey) {
    if (OB_FAIL(set_partition_removed_(pkey))) {
      STORAGE_LOG(WARN, "failed to set_partition_removed", K(ret), K(pkey), K(*this));
    }
  } else {
    can_clear_non_reused_stores = true;
  }
#ifdef ERRSIM
  ret = E(EventTable::EN_SKIP_DROP_MEMTABLE) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    return OB_SUCCESS;
  }
#endif
  if (OB_SUCC(ret) && can_clear_non_reused_stores) {
    if (OB_FAIL(clear_all_complement_minor_sstable_())) {
      LOG_WARN("failed to clear_all_complement_minor_sstable_", K(ret));
    } else if (OB_FAIL(clear_all_memtables())) {
      LOG_WARN("failed to clear all memtables", K(ret));
    } else {
      cleared_memstore = true;
      STORAGE_LOG(INFO, "clear non-reused memstore(s) success", K(pkey), K(pkey_));
    }
  }

  return ret;
}

int ObPGStorage::get_merge_priority_info(ObMergePriorityInfo& merge_priority_info)
{
  int ret = OB_SUCCESS;
  ObTableHandle handle;
  ObMemtable* memtable = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage has not been inited", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_first_frozen_memtable(handle))) {
    STORAGE_LOG(WARN, "failed to get first frozen memtable", K(ret), K_(pkey));
  } else if (OB_ISNULL(handle.get_table())) {
    TCRLockGuard guard(lock_);
    merge_priority_info.handle_id_ = meta_->pg_key_.get_partition_group_id();
    merge_priority_info.tenant_id_ = meta_->pg_key_.get_tenant_id();
    merge_priority_info.emergency_ = false;
  } else if (OB_FAIL(handle.get_memtable(memtable))) {
    STORAGE_LOG(WARN, "failed to get memtable", K(ret), K_(pkey));
  } else if (OB_FAIL(memtable->get_merge_priority_info(merge_priority_info))) {
    STORAGE_LOG(WARN, "failed to get merge priority info", K(ret), K_(pkey));
  }

  return ret;
}

int ObPGStorage::check_and_new_active_memstore(
    const ObBaseStorageInfo& clog_info, const bool force, bool& changed, int64_t& active_protection_clock)
{
  int ret = OB_SUCCESS;
  changed = force;
  bool log_changed = false;
  bool need_freeze_data = false;

  if (!force && OB_FAIL(check_log_or_data_changed(clog_info, log_changed, need_freeze_data))) {
    STORAGE_LOG(WARN, "check log or data changed error", K(ret), K(clog_info));
  } else if (FALSE_IT(changed = (log_changed || need_freeze_data))) {
  } else if (!changed && !force) {
    // do nothing
  } else if (OB_FAIL(new_active_memstore(active_protection_clock))) {
    STORAGE_LOG(WARN, "new active memstore error", K(ret), K(clog_info));
  } else {
    // do nothing
  }

  return ret;
}

bool ObPGStorage::has_active_memtable()
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else {
    found = pg_memtable_mgr_.has_active_memtable();
    if (!found && EXECUTE_COUNT_PER_SEC(1)) {
      STORAGE_LOG(INFO, "the partition group has no active memtable", K(*this));
    }
  }
  return found;
}

int ObPGStorage::check_pg_partition_offline(const ObPartitionKey& pkey, bool& offline)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObPartition is not inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
    // offline pg
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(pkey_), K(pkey));
    } else if (OB_FAIL(pg_partition->set_gc_starting())) {
      STORAGE_LOG(WARN, "set pg gc starting error", K(ret), K(pkey));
    } else {
      offline = static_cast<ObPartitionStorage*>(pg_partition->get_storage())->get_partition_store().is_removed();
      STORAGE_LOG(INFO, "check pg partition offline", K(pkey), K(offline));
    }
  }

  return ret;
}

bool ObPGStorage::has_memstore()
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else {
    bool_ret = pg_memtable_mgr_.has_memtable();
  }

  return bool_ret;
}

bool ObPGStorage::is_replica_with_remote_memstore() const
{
  bool bool_ret = false;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(WARN, "partition is not initialized", K(pkey_));
  } else {
    bool_ret = 0 == meta_->replica_property_.get_memstore_percent() &&
               ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_);
  }

  return bool_ret;
}

bool ObPGStorage::is_share_major_in_zone() const
{
  bool bool_ret = false;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(WARN, "partition is not initialized", K(pkey_));
  }
  return bool_ret;
}

int ObPGStorage::new_active_memstore(int64_t& active_protection_clock)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (OB_FAIL(pg_memtable_mgr_.new_active_memstore(pkey_, active_protection_clock))) {
    STORAGE_LOG(WARN, "new active memstore error", K(ret), K_(pkey));
  }
  return ret;
}

int ObPGStorage::effect_new_active_memstore(const ObSavedStorageInfoV2& info, const bool emergency)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else {
    ObReplicaType replica_type;
    {
      TCRLockGuard lock_guard(lock_);
      replica_type = meta_->replica_type_;
    }

    if (OB_FAIL(pg_memtable_mgr_.effect_new_active_memstore(info, pkey_, replica_type, emergency))) {
      STORAGE_LOG(WARN, "fail to effective new active memstore", K(ret), K(pkey_), K(info), K(emergency));
    }
  }

  return ret;
}

int ObPGStorage::complete_active_memstore(const ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (OB_FAIL(pg_memtable_mgr_.complete_active_memstore(info))) {
    STORAGE_LOG(WARN, "complete active memstore error", K(ret), K(info), K_(pkey));
  } else {
    STORAGE_LOG(INFO, "complete active memstore success", K(info), K(pkey_));
  }

  return ret;
}

int ObPGStorage::clean_new_active_memstore()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (OB_FAIL(pg_memtable_mgr_.clean_new_active_memstore())) {
    STORAGE_LOG(WARN, "fail to clean new active memstore", K(ret), K(pkey_));
  }
  return ret;
}

int ObPGStorage::get_active_memtable_base_version(int64_t& base_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else {
    ObTableHandle handle;
    memtable::ObMemtable* memtable = NULL;
    if (OB_FAIL(pg_memtable_mgr_.get_active_memtable(handle))) {
      STORAGE_LOG(WARN, "get active memtable error", K(ret), K_(pkey));
    } else if (OB_FAIL(handle.get_memtable(memtable))) {
      STORAGE_LOG(WARN, "get memtable error", K(ret), K(handle));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, " memtable is NULL", K(ret), K(pkey_));
    } else {
      base_version = memtable->get_base_version();
    }
  }

  return ret;
}

int ObPGStorage::get_active_memtable_start_log_ts(int64_t& start_log_ts)
{
  int ret = OB_SUCCESS;
  start_log_ts = INT64_MAX;
  TCRLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (!ObReplicaTypeCheck::is_replica_with_memstore(meta_->replica_type_) ||
             0 == meta_->replica_property_.get_memstore_percent()) {
    // do nothing
  } else {
    ObTableHandle handle;
    memtable::ObMemtable* memtable = NULL;
    if (OB_FAIL(pg_memtable_mgr_.get_active_memtable(handle))) {
      STORAGE_LOG(WARN, "get active memtable error", K(ret), K_(pkey));
    } else if (OB_FAIL(handle.get_memtable(memtable))) {
      STORAGE_LOG(WARN, "get memtable error", K(ret), K(handle));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, " memtable is NULL", K(ret), K(pkey_));
    } else {
      start_log_ts = memtable->get_start_log_ts();
    }
  }

  return ret;
}

int ObPGStorage::check_active_mt_hotspot_row_exist(bool& has_hotspot_row, const int64_t fast_freeze_interval)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), K(pkey_), K(pkeys));
  } else if (pkeys.count() <= 0 || ObTimeUtility::current_time() < last_freeze_ts_ + fast_freeze_interval) {
    // do nothing
  } else {
    ObTableHandle handle;
    memtable::ObMemtable* memtable = NULL;
    if (OB_FAIL(pg_memtable_mgr_.get_active_memtable(handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        has_hotspot_row = false;
      }
      STORAGE_LOG(WARN, "get active memtable error", K(ret), K_(pkey));
    } else if (OB_FAIL(handle.get_memtable(memtable))) {
      STORAGE_LOG(WARN, "get memtable error", K(ret), K(handle));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, " memtable is NULL", K(ret), K(pkey_));
    } else {
      has_hotspot_row = memtable->has_hotspot_row();
    }
  }

  return ret;
}

int ObPGStorage::get_active_protection_clock(int64_t& active_protection_clock)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* memtable = NULL;
  ObTableHandle handle;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (OB_FAIL(pg_memtable_mgr_.get_active_memtable(handle))) {
    STORAGE_LOG(WARN, "failed to get active memtable", K(ret));
  } else if (OB_FAIL(handle.get_memtable(memtable))) {
    STORAGE_LOG(WARN, "get active memtable error", K(ret), K(handle), K_(pkey));
  } else {
    int64_t retire_clock = memtable->get_retire_clock();
    if (memtable->not_empty()) {
      active_protection_clock = memtable->get_protection_clock();
    } else {
      active_protection_clock = retire_clock;
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
int ObPGStorage::retire_warmup_store(const bool is_disk_full)
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(is_removed_)) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(*meta_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_->partitions_.count(); ++i) {
      const ObPartitionKey& pkey = meta_->partitions_.at(i);
      ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
      ObPGPartition* partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
      } else if (OB_FAIL(storage->get_partition_store().retire_prewarm_store(is_disk_full))) {
        LOG_WARN("failed to retire_preware_store", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPGStorage::set_replay_sstables(const bool is_replay_old, ObPartitionStore& store)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> index_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_FAIL(store.get_all_table_ids(index_ids))) {
    LOG_WARN("fail to get all table ids", K(ret));
  } else {
    ObArray<ObITable::TableKey> replay_tables;
    ObArray<ObSSTable*> sstables;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
      const uint64_t table_id = index_ids.at(i);
      replay_tables.reuse();
      sstables.reuse();
      if (OB_FAIL(store.get_replay_tables(table_id, replay_tables))) {
        LOG_WARN("fail to get replay tables", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < replay_tables.count(); ++i) {
          ObSSTable* sstable = nullptr;
          ObTableHandle table_handle;
          ObITable::TableKey& table_key = replay_tables.at(i);
          if (OB_FAIL(sstable_mgr_.acquire_sstable(table_key, table_handle))) {
            LOG_WARN("fail to acquire sstable", K(ret), K(table_key));
          } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
            LOG_WARN("fail to get sstable", K(ret));
          } else if (OB_FAIL(sstables.push_back(sstable))) {
            LOG_WARN("fail to push back sstable", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(is_replay_old && compat_fill_log_ts(sstables))) {
            LOG_WARN("fail to fill log ts for compat", K(ret), K(sstables));
          } else if (OB_FAIL(store.set_replay_sstables(table_id, is_replay_old, sstables))) {
            LOG_WARN("fail to set replay sstables", K(ret), K(table_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObPGStorage::compat_fill_log_ts(ObArray<ObSSTable*>& replay_tables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (replay_tables.empty() ||
             common::ObLogTsRange::MIN_TS != replay_tables.at(replay_tables.count() - 1)->get_end_log_ts()) {
    LOG_INFO("no need to compat fill", K(ret), K(replay_tables));
  } else {
    ObTableCompater table_compater;
    if (OB_FAIL(table_compater.add_tables(replay_tables))) {
      STORAGE_LOG(WARN, "Failed to add replay tables for compat", K(ret), K(replay_tables));
    } else if (OB_FAIL(table_compater.fill_log_ts())) {
      STORAGE_LOG(WARN, "Failed to fill new log ts for compat", K(ret), K(table_compater));
    } else {
      const int64_t fill_log_ts = ObTableCompater::OB_MAX_COMPAT_LOG_TS;
      meta_->storage_info_.get_data_info().set_last_replay_log_ts(fill_log_ts);
      LOG_INFO("Succ to update replay log ts for compat", K(ret), K(fill_log_ts));
    }
  }
  return ret;
}

int ObPGStorage::enable_write_log(const bool is_replay_old)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);
    if (is_removed_) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K_(pkey));
    } else if (!meta_->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid meta", K(ret), K(*meta_), K_(pkey));
    } else if (is_replay_old && OB_FAIL(transform_and_add_old_sstable())) {
      LOG_WARN("fail to transform and add old sstable", K(ret));
    } else {
      ObPGReportStatus pg_report_status;
      ObPartitionArray pkeys;
      if (OB_FAIL(get_all_pg_partition_keys_(pkeys, true /*include_trans_table*/))) {
        STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), K(pkey_), K(pkeys));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
          const ObPartitionKey& pkey = pkeys.at(i);
          ObPGPartition* pg_partition = nullptr;
          ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
          ObPartitionStorage* storage = nullptr;
          if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey), K(pkey));
          } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "partition storage is null", K(ret), K(pkey));
          } else if (OB_FAIL(set_replay_sstables(is_replay_old, storage->get_partition_store()))) {
            LOG_WARN("fail to set replay sstables", K(ret));
          } else if (OB_FAIL(storage->get_partition_store().finish_replay())) {
            LOG_WARN("failed to finish_replay", K(ret), K(pkey));
          }
        }
        if (OB_SUCC(ret)) {
          bool write_slog = false;
          if (pkeys.count() > 0 && OB_FAIL(update_report_status_(write_slog))) {
            LOG_WARN("failed to update report status", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(recovery_point_data_mgr_.enable_write_slog(sstable_mgr_))) {
        LOG_WARN("failed to enable write slog for recovery point data mgr", K(ret), K_(pkey));
      } else if (OB_FAIL(sstable_mgr_.enable_write_log())) {
        LOG_WARN("fail to enable write log", K(ret));
      }
    }
  }
  return ret;
}

int ObPGStorage::check_can_migrate(bool& can_migrate)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  ObPGPartition* pg_partition = NULL;
  can_migrate = true;
  bool is_in_dest_split = false;
  if (is_inited_) {
    TCRLockGuard lock_guard(lock_);
    is_in_dest_split = is_dest_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_));
  }

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage is not inited", K(ret));
  } else if (!is_in_dest_split) {
    // do nothing
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    STORAGE_LOG(WARN, "get all pg partition keys failed", K(ret), K_(pkey));
  } else {
    can_migrate = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); i++) {
      ObPGPartitionGuard guard(pkeys.at(i), *(pg_->get_pg_partition_map()));
      if (NULL == (pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(pkeys.at(i)));
      } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                             ->get_partition_store()
                             .check_can_migrate(can_migrate))) {
        STORAGE_LOG(WARN, "failed to get merged version", K(ret), K(pkeys.at(i)));
      } else if (!can_migrate) {
        break;
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObPGStorage::do_warm_up_request(const ObIWarmUpRequest* request)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    STORAGE_LOG(WARN, "fail to get pg partition keys", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      ObPGPartition* pg_partition = NULL;
      ObPGPartitionGuard guard(pkeys.at(i), *(pg_->get_pg_partition_map()));
      if (NULL == (pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
      } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())->do_warm_up_request(request))) {
        STORAGE_LOG(WARN, "failed to do warm up request", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

// get cached replica type
ObReplicaType ObPGStorage::get_replica_type_() const
{
  int tmp_ret = OB_SUCCESS;
  ObReplicaType replica_type = ATOMIC_LOAD(&cached_replica_type_);

  if (REPLICA_TYPE_MAX != replica_type) {
    // do nothing
  } else if (OB_SUCCESS != (tmp_ret = get_replica_type(replica_type))) {
    STORAGE_LOG(WARN, "get replica_type error", K(tmp_ret), K_(pkey), K(replica_type));
  } else {
    // do nothing
  }

  return replica_type;
}

int ObPGStorage::get_replica_type(common::ObReplicaType& replica_type) const
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  replica_type = meta_->replica_type_;
  return ret;
}

int ObPGStorage::get_replica_property(ObReplicaProperty& replica_property) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else {
    TCRLockGuard lock_guard(lock_);
    replica_property = meta_->replica_property_;
  }
  return ret;
}

int ObPGStorage::set_pg_replica_type(const ObReplicaType& replica_type, const bool write_redo_log)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  const bool need_clean_memtable = !ObReplicaTypeCheck::is_replica_with_memstore(replica_type);
  const bool need_clean_sstable = !ObReplicaTypeCheck::is_replica_with_ssstore(replica_type);

  ObBucketWLockAllGuard bucket_guard(bucket_lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    STORAGE_LOG(WARN, "pg is removed", K(ret), K_(pkey));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else if (replica_type == meta_->replica_type_) {
    STORAGE_LOG(INFO, "replica type is same, no need to change", K(replica_type), K(*this));
  } else if (!ObReplicaTypeCheck::is_replica_type_valid(replica_type) ||
             !ObReplicaTypeCheck::change_replica_op_allow(meta_->replica_type_, replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(replica_type), K(*meta_));
  } else if (OB_FAIL(next_meta_ptr->deep_copy(*meta_))) {
    STORAGE_LOG(WARN, "failed to copy pg meta", K(ret), K(*meta_));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    next_meta.replica_type_ = replica_type;
    if (need_clean_memtable) {
      next_meta.storage_info_.get_data_info().reset_for_no_memtable_replica();
      LOG_INFO("has no memtable, clear data info", K(next_meta));
    }

    if (need_clean_sstable) {
      next_meta.report_status_.reset();
    }

    ObPartitionArray pkeys;
    if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
      STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), K(pkey_), K(pkeys));
    } else {
      if (OB_SUCC(ret) && write_redo_log) {
        if (OB_FAIL(write_set_replica_type_log_(replica_type))) {
          LOG_WARN("failed to write_set_replica_type_log", K(ret), K(next_meta));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < pkeys.count(); ++i) {
        ObPGPartition* pg_partition = nullptr;
        ObPGPartitionGuard guard(pkeys.at(i), *(pg_->get_pg_partition_map()));
        ObPartitionStorage* storage = nullptr;
        if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("pg partition info is null, unexpected error", K(tmp_ret), K_(pkey));
          ob_abort();
        } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("partition storage is null", K(tmp_ret), "pkey", pkeys.at(i));
          ob_abort();
        } else {
          storage->get_partition_store().set_replica_type(replica_type);
        }
      }
      switch_meta_(next_meta_ptr);
      LOG_INFO("succeed to set replica type", K(*meta_));

      if (need_clean_memtable) {
        pg_memtable_mgr_.clean_memtables();
        pg_memtable_mgr_.clean_new_active_memstore();
        LOG_INFO("clean all memtables", K_(pkey));
      }
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::write_set_replica_type_log_(const ObReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t lsn = 0;
  ObChangePartitionLogEntry log_entry;
  log_entry.pg_key_ = pkey_;
  log_entry.partition_key_ = pkey_;
  log_entry.replica_type_ = replica_type;
  if (OB_FAIL(SLOGGER.begin(OB_LOG_SET_REPLICA_TYPE))) {
    LOG_WARN("failed to begin set replica slog trans", K(ret), K_(pkey), K(replica_type));
  } else {
    const int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_SET_REPLICA_TYPE);
    const ObStorageLogAttribute log_attr(pkey_.get_tenant_id(), meta_->storage_info_.get_pg_file_id());
    if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
      LOG_WARN("failed to write set replica type log", K(ret), K(log_entry));
    } else if (OB_FAIL(SLOGGER.commit(lsn))) {
      LOG_ERROR("failed to commit set replica type trans", K(ret), K(log_entry));
    } else {
      FLOG_INFO("succeed to set replica type", K(log_entry), K(lsn));
    }
    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        LOG_ERROR("failed to abort set replica type trans", K(tmp_ret), K(log_entry));
      }
    }
  }
  return ret;
}

// get cached slave read timestamp
int ObPGStorage::get_weak_read_timestamp(int64_t& timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    timestamp = pg_memtable_mgr_.get_readable_ts();
    if (INT64_MAX == timestamp) {
      STORAGE_LOG(ERROR, "unexpected weak read timestamp", K(timestamp), K_(pkey));
      // slave read timestamp not init
    } else if (OB_INVALID_TIMESTAMP == timestamp) {
      if (EXECUTE_COUNT_PER_SEC(1)) {
        STORAGE_LOG(INFO, "weak read timestamp not changed", K_(pkey), K(timestamp), K(*this));
      }
      timestamp = 0;
    } else if (!is_replica_with_remote_memstore() && ObTimeUtility::current_time() - timestamp > 120000000) {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(WARN, "weak read timestamp is too old", K_(pkey), K(timestamp));
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPGStorage::get_all_saved_info(ObSavedStorageInfoV2& info) const
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (OB_FAIL(info.deep_copy(meta_->storage_info_))) {
    STORAGE_LOG(WARN, "failed to copy info", K(ret), K_(meta));
  }
  return ret;
}

int ObPGStorage::get_saved_clog_info(common::ObBaseStorageInfo& clog_info) const
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg is not inited", K(ret));
  } else if (OB_FAIL(clog_info.deep_copy(meta_->storage_info_.get_clog_info()))) {
    STORAGE_LOG(WARN, "failed to copy clog info", K(ret));
  }
  return ret;
}

int ObPGStorage::get_saved_data_info(ObDataStorageInfo& data_info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg is not inited", K(ret));
  } else {
    TCRLockGuard lock_guard(lock_);
    data_info = meta_->storage_info_.get_data_info();
  }

  return ret;
}

int ObPGStorage::append_local_sort_data(
    const ObPartitionKey& pkey, const share::ObBuildIndexAppendLocalDataParam& param, ObNewRowIterator& iter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg is not inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", K(ret), K(pkey), K(param));
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(pkey), K(pkey_));
    } else if (OB_FAIL(pg_partition->get_storage()->append_local_sort_data(param, pkey_, file_handle_, iter))) {
      STORAGE_LOG(WARN, "fail to append local sort data", K(ret), K(param));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPGStorage::append_sstable(
    const ObPartitionKey& pkey, const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg is not inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", K(ret), K(param));
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
    ObTableHandle sstable_handle;
    ObSSTable* sstable = nullptr;
    const int64_t max_kept_major_version_number = 1;
    const bool in_slog_trans = false;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* index_schema = nullptr;
    const uint64_t tenant_id = is_inner_table(param.index_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(pkey.table_id_);
    bool has_major = false;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage must not null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().has_major_sstable(param.index_id_, has_major))) {
      LOG_WARN("failed to check index table has major sstable", K(ret), K(param));
    } else if (has_major) {
      // already has major, do not append
    } else if (OB_FAIL(append_sstable(param, iter, sstable_handle))) {
      STORAGE_LOG(WARN, "fail to append sstable", K(ret), K(param));
    } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get sstable", K(ret), K(param));
    } else if (OB_FAIL(add_sstable(pkey, sstable, max_kept_major_version_number, in_slog_trans))) {
      LOG_WARN("failed to add sstable", K(ret), K(pkey));
    } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get_tenant_full_schema_guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(param.index_id_, index_schema))) {
      LOG_WARN("failed to get_table_schema", K(ret), K(pkey), K(param));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("schema error, index schema not exist", K(ret), K(pkey));
    } else {
      const int64_t bucket_idx = get_bucket_idx_(pkey);
      ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
      if (OB_UNLIKELY(is_removed_)) {
        ret = OB_PG_IS_REMOVED;
        LOG_WARN("pg is removed", K(ret), K_(pkey));
      } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                             ->get_partition_store()
                             .try_update_report_status(index_schema->get_data_table_id()))) {
        LOG_WARN("failed to try_update_report_status", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPGStorage::check_single_replica_major_sstable_exist(const ObPartitionKey& pkey, const uint64_t index_table_id)
{
  int ret = OB_SUCCESS;
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  bool has_major = false;

  if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                         ->get_partition_store()
                         .has_major_sstable(index_table_id, has_major))) {
    STORAGE_LOG(WARN, "fail to append sstable", K(ret));
  } else {
    ret = has_major ? OB_SUCCESS : OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObPGStorage::get_table_stat(const common::ObPartitionKey& pkey, ObTableStat& stat)
{
  int ret = OB_SUCCESS;

  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K_(pkey));
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = guard.get_pg_partition())) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "pg partition info not exist", K(ret), K_(pkey));
    } else if (OB_FAIL(pg_partition->get_storage()->get_table_stat(pkey.get_table_id(), stat))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to get table stat", K(ret), K(pkey), K_(pkey));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPGStorage::set_pg_clog_info(const ObBaseStorageInfo& clog_info, const bool replica_with_data)
{
  int ret = OB_SUCCESS;

  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    STORAGE_LOG(WARN, "partition is removed", K(ret), K(meta_));
  } else if (!clog_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(clog_info));
  } else if (!replica_with_data && pg_memtable_mgr_.get_memtable_count() > 0) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(ERROR, "cannot set clog if memtable exist", K(ret), K(*this));
  } else if (meta_->storage_info_.get_clog_info().get_last_replay_log_id() > clog_info.get_last_replay_log_id()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "last_replay_log_id should not decrease", K(ret), K(*meta_), K(clog_info));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    if (FALSE_IT(next_meta.reset())) {
      // can no reach here
    } else if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      STORAGE_LOG(WARN, "failed to get next meta", K(ret));
    } else if (OB_FAIL(next_meta.storage_info_.get_clog_info().deep_copy(clog_info))) {
      STORAGE_LOG(WARN, "Failed to deep copy clog info", K(ret));
    } else if (OB_FAIL(next_meta.storage_info_.update_and_fetch_log_info(pkey_,
                   replica_with_data,
                   meta_->storage_info_.get_clog_info(),
                   PG_LOG_INFO_QUERY_TIMEOUT,
                   false /*log info usable*/))) {
      STORAGE_LOG(WARN, "failed to update_last_replay_log_info", K(ret), K_(pkey), K(clog_info));
    } else {
      if (OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_CLOG_INFO))) {
        STORAGE_LOG(WARN, "failed to set_storage_info", K(ret));
      } else {
        switch_meta_(next_meta_ptr);
      }
    }
  }
  free_meta_(next_meta_ptr);

  return ret;
}

int ObPGStorage::try_update_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
    const ObMemberList& mlist, const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;

  ObBaseStorageInfo clog_info;

  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    STORAGE_LOG(WARN, "partition is removed", K(ret), K(meta_));
  } else if (OB_INVALID_ID == ms_log_id || mc_timestamp <= 0 || replica_num <= 0 || !mlist.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey_), K(ms_log_id), K(mc_timestamp), K(replica_num), K(mlist));
  } else if (OB_FAIL(clog_info.deep_copy(meta_->storage_info_.get_clog_info()))) {
    STORAGE_LOG(WARN, "failed to copy clog info", K(ret));
  } else if (OB_FAIL(clog_info.try_update_member_list(ms_log_id, mc_timestamp, replica_num, mlist, ms_proposal_id))) {
    if (OB_ENTRY_EXIST == ret) {
      STORAGE_LOG(
          INFO, "skip update member list", K(ret), K(pkey_), K(ms_log_id), K(mc_timestamp), K(replica_num), K(mlist));
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(INFO,
          "fail to update member list",
          K(ret),
          K(pkey_),
          K(ms_log_id),
          K(mc_timestamp),
          K(replica_num),
          K(mlist));
    }
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      STORAGE_LOG(WARN, "failed to get next meta", K(ret));
    } else if (OB_FAIL(next_meta.storage_info_.get_clog_info().deep_copy(clog_info))) {
      STORAGE_LOG(WARN, "Failed to deep copy clog info", K(ret), K(pkey_));
    } else {
      if (OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_MEMBER_LIST))) {
        STORAGE_LOG(WARN, "failed to set_storage_info", K(ret));
      } else {
        switch_meta_(next_meta_ptr);
      }
    }
  }

  free_meta_(next_meta_ptr);

  return ret;
}

int ObPGStorage::set_publish_version_after_create(const int64_t publish_version)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* memtable = NULL;
  ObTableHandle memtable_handle;
  bool is_empty = true;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);

    if (is_removed_) {
      ret = OB_PARTITION_IS_REMOVED;
      LOG_WARN("partition is removed", K(ret), K(meta_));
    } else if (OB_FAIL(pg_memtable_mgr_.get_active_memtable(memtable_handle)) ||
               OB_FAIL(memtable_handle.get_memtable(memtable))) {
      STORAGE_LOG(WARN, "get active memtable error", K(ret), K(memtable_handle), K_(meta));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ACTIVE_MEMTBALE_NOT_EXSIT;
      LOG_ERROR("active memtable must not null", K(ret));
    } else if (OB_FAIL(check_table_store_empty(is_empty))) {
      LOG_WARN("failed to check table store empty", K(ret));
    } else if (pg_memtable_mgr_.get_memtable_count() > 1 || !is_empty) {
      ret = OB_ERR_SYS;
      int64_t memtable_count = pg_memtable_mgr_.get_memtable_count();
      LOG_ERROR("could only set publish version for new create pg storage",
          K(ret),
          K(*meta_),
          K(is_empty),
          K(memtable_count));
    } else if (!memtable->is_active_memtable() || memtable->get_snapshot_version() != ObVersionRange::MAX_VERSION) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("cannot set memtable base version now", K(ret), K(*memtable));
    } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
      LOG_WARN("failed to alloc meta", K(ret));
    } else {
      ObPartitionGroupMeta& next_meta = *next_meta_ptr;
      if (OB_FAIL(next_meta.deep_copy(*meta_))) {
        LOG_WARN("failed to get next meta", K(ret));
      } else {
        next_meta.storage_info_.get_data_info().set_publish_version(publish_version);

        if (OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_PUBLISH_VERSION_AFTER_CREATE))) {
          LOG_WARN("failed to write update partition meta log", K(ret));
        } else if (OB_FAIL(memtable->set_base_version(publish_version))) {
          LOG_WARN("failed to set memtable base version, abort now", K(ret));
          ob_abort();
        } else {
          switch_meta_(next_meta_ptr);
        }
      }
    }
    FLOG_INFO("set publish version after creat", K(ret), K(publish_version));
  }

  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::save_split_state(const int64_t state, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (is_removed_) {
    LOG_INFO("pg is removed", K(ret), K_(pkey));
  } else if (!is_valid_split_state(static_cast<ObPartitionSplitStateEnum>(state))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split state", K(ret), K(state), K_(pkey));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      LOG_WARN("failed to get next meta", K(ret));
    } else {
      next_meta.saved_split_state_ = state;
      if (write_slog && OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_PARTITION_SPLIT_INFO))) {
        LOG_WARN("failed to write update partition meta log", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      switch_meta_(next_meta_ptr);
      LOG_INFO("save split state", K_(pkey), K(state));
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::save_split_info(const ObPartitionSplitInfo& split_info, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (is_removed_) {
    LOG_INFO("pg is removed", K(ret), K_(pkey));
  } else if (!split_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split info", K(ret), K(split_info), K_(pkey));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      LOG_WARN("failed to get next meta", K(ret));
    } else if (OB_FAIL(next_meta.split_info_.assign(split_info))) {
      LOG_WARN("failed to assign split info", K(ret));
    } else {
      if (write_slog && OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_PARTITION_SPLIT_INFO))) {
        LOG_WARN("failed to write update partition meta log", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      switch_meta_(next_meta_ptr);
      LOG_INFO("save split info", K_(pkey), K(split_info));
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::clear_split_info()
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K_(pkey));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    next_meta.reset();
    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      LOG_WARN("failed to get next meta", K(ret));
    } else {
      next_meta.split_info_.reset();
      if (OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_PARTITION_SPLIT_INFO))) {
        LOG_WARN("failed to write update partition meta log", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      switch_meta_(next_meta_ptr);
      LOG_INFO("clear split info", K_(pkey));
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::get_all_table_ids(const ObPartitionKey& pkey, ObIArray<uint64_t>& index_tables)
{
  int ret = OB_SUCCESS;
  index_tables.reset();
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(pkey));
  } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                         ->get_partition_store()
                         .get_all_table_ids(index_tables))) {
    STORAGE_LOG(WARN, "get all table ids failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::get_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                         ->get_partition_store()
                         .get_reference_tables(index_id, handle))) {
    STORAGE_LOG(WARN, "get reference tables failed", K(ret), K(index_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::set_reference_memtables(const ObTablesHandle& memtables)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.push_reference_tables(memtables))) {
    LOG_WARN("failed to push_reference_tables", K(ret), K(pkey_), K(memtables));
  } else {
    LOG_INFO("succ to set reference memtables", K(memtables));
  }
  return ret;
}

int ObPGStorage::set_pg_storage_info(const ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    STORAGE_LOG(WARN, "partition is removed", K(ret), K(*meta_));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(info));
  } else if (pg_memtable_mgr_.get_memtable_count() > 0) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(ERROR, "cannot set storage info when memtable count not zero", K(ret), K(*this));
  } else if (meta_->storage_info_.get_data_info().get_publish_version() > info.get_data_info().get_publish_version() &&
             ObReplicaRestoreStatus::REPLICA_RESTORE_DATA != meta_->is_restore_ &&
             ObReplicaRestoreStatus::REPLICA_RESTORE_CUT_DATA != meta_->is_restore_) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "new storage info's publish version should not smaller than local", K(ret), K(*meta_), K(info));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      STORAGE_LOG(WARN, "failed to get next meta", K(ret));
    } else if (OB_FAIL(next_meta.storage_info_.deep_copy(info))) {
      STORAGE_LOG(WARN, "Failed to deep copy storage info", K(ret));
    } else {
      if (OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_STORAGE_INFO))) {
        STORAGE_LOG(WARN, "failed to set_storage_info", K(ret));
      } else {
        switch_meta_(next_meta_ptr);
      }
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::fill_pg_partition_replica(const ObPartitionKey& pkey, ObReportStatus& report_status)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || pkey.is_pg()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(INFO, "invalid argument", K(ret), K(pkey));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* pg_partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pg_partition is null", K(ret), K(pkey));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage is null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().get_report_status(report_status))) {
      LOG_WARN("failed to get report status", K(ret), K(pkey));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPGStorage::fill_replica(share::ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else {
    TCRLockGuard guard(lock_);
    replica.is_restore_ = meta_->is_restore_;
    replica.replica_type_ = meta_->replica_type_;
    replica.data_version_ = meta_->report_status_.data_version_;
    replica.data_size_ = meta_->report_status_.data_size_;
    replica.required_size_ = meta_->report_status_.required_size_;
    replica.property_ = meta_->replica_property_;
    replica.data_file_id_ =
        nullptr == file_handle_.get_storage_file() ? 0 : file_handle_.get_storage_file()->get_file_id();
  }
  // for compatibility
  if (OB_SUCC(ret) && !pkey_.is_pg()) {
    ObPGPartitionGuard pg_partition_guard(pkey_, *(pg_->get_pg_partition_map()));
    ObPGPartition* pg_partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    ObReportStatus report_status;
    if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pg_partition is null", K(ret), K(pkey_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage is null", K(ret), K(pkey_));
    } else if (OB_FAIL(storage->get_partition_store().get_report_status(report_status))) {
      LOG_WARN("failed to get report status", K(ret), K(pkey_));
    } else {
      replica.data_version_ = report_status.data_version_;
      replica.row_count_ = report_status.row_count_;
      replica.row_checksum_.checksum_ = report_status.row_checksum_;
      replica.data_checksum_ = report_status.data_checksum_;
      replica.data_size_ = report_status.data_size_;
      replica.required_size_ = report_status.required_size_;
    }
  }
  return ret;
}

int ObPGStorage::replay_schema_log(const char* buf, const int64_t size, const int64_t log_id)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  ObPartitionKey pkey;
  if (OB_FAIL(pkey.deserialize(buf, size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize pkey", K(pkey_));
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));

    if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "pg partition not found", K(ret), K(pkey_), K(pkey));
    } else if (OB_FAIL(pg_partition->replay_schema_log(buf + pos, size - pos, log_id))) {
      STORAGE_LOG(WARN, "fail to replay schema log", K(ret), K(pkey_), K(pkey));
    }
  }

  return ret;
}

int ObPGStorage::check_physical_split(bool& finished)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not intied", K(ret));
  } else {
    TCRLockGuard lock_guard(lock_);

    finished = is_physical_split_finished(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_));
  }
  return ret;
}

int ObPGStorage::table_scan(ObTableScanParam& param, ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;
  int64_t data_max_schema_version = 0;
  bool is_bounded_staleness_read = (NULL == param.trans_desc_) ? false : param.trans_desc_->is_bounded_staleness_read();
  const bool is_pg = pkey_.is_pg();
  ObPGPartition* pg_partition = NULL;

  if (is_bounded_staleness_read && OB_FAIL(get_based_schema_version(data_max_schema_version))) {
    STORAGE_LOG(WARN, "get based schema version fail", K(ret), K_(pkey), K(param));
  } else if (!ObReplicaTypeCheck::is_readable_replica(get_replica_type_())) {
    ret = OB_REPLICA_NOT_READABLE;
    STORAGE_LOG(WARN, "replica is not readable", K(ret), "this", *this);
  } else if (is_pg || NULL == pg_partition_) {
    const common::ObPartitionKey& pkey = (is_pg ? param.pkey_ : pkey_);
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
    } else {
      // cache pg_partition pointer for standalone partition
      if (!is_pg) {
        pg_partition_ = pg_partition;
      }
    }
  } else {
    // standalone partition
    pg_partition = pg_partition_;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_partition->table_scan(param, data_max_schema_version, result))) {
      if (NULL != param.trans_desc_) {
        STORAGE_LOG(WARN,
            "failed to do table scan",
            K(ret),
            "this",
            *this,
            "trans_id",
            param.trans_desc_->get_trans_id(),
            K(data_max_schema_version));
      } else {
        STORAGE_LOG(WARN, "failed to do table scan", K(ret), "this", *this, K(data_max_schema_version));
      }
    }
  }

  return ret;
}

int ObPGStorage::table_scan(ObTableScanParam& param, ObNewIterIterator*& result)
{
  int ret = OB_SUCCESS;
  int64_t data_max_schema_version = 0;
  bool is_bounded_staleness_read = (NULL == param.trans_desc_) ? false : param.trans_desc_->is_bounded_staleness_read();
  const bool is_pg = pkey_.is_pg();
  ObPGPartition* pg_partition = NULL;

  if (is_bounded_staleness_read && OB_FAIL(get_based_schema_version(data_max_schema_version))) {
    STORAGE_LOG(WARN, "get based schema version fail", K(ret), K_(pkey), K(param));
  } else if (!ObReplicaTypeCheck::is_readable_replica(get_replica_type_())) {
    ret = OB_REPLICA_NOT_READABLE;
    STORAGE_LOG(WARN, "replica is not readable", K(ret), "this", *this);
  } else if (is_pg || NULL == pg_partition_) {
    const common::ObPartitionKey& pkey = (is_pg ? param.pkey_ : pkey_);
    ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
    } else {
      if (!is_pg) {
        pg_partition_ = pg_partition;
      }
    }
  } else {
    // standalone partition
    pg_partition = pg_partition_;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_partition->table_scan(param, data_max_schema_version, result))) {
      if (NULL != param.trans_desc_) {
        STORAGE_LOG(WARN,
            "failed to do table scan",
            K(ret),
            "this",
            *this,
            "trans_id",
            param.trans_desc_->get_trans_id(),
            K(data_max_schema_version));
      } else {
        STORAGE_LOG(WARN, "failed to do table scan", K(ret), "this", *this, K(data_max_schema_version));
      }
    }
  }

  return ret;
}

int ObPGStorage::join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
    ObIPartitionGroup& right_partition, common::ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;
  int64_t left_data_max_schema_version = 0;
  int64_t right_data_max_schema_version = 0;
  bool is_bounded_staleness_read =
      (NULL == left_param.trans_desc_) ? false : left_param.trans_desc_->is_bounded_staleness_read();

  ObPartitionGroup& rp = static_cast<ObPartitionGroup&>(right_partition);
  const common::ObPartitionKey& lpkey = (pkey_.is_pg() ? left_param.pkey_ : pkey_);
  const common::ObPartitionKey& rpkey = (rp.is_pg() ? right_param.pkey_ : rp.get_partition_key());

  ObPGPartitionGuard lguard(lpkey, *(pg_->get_pg_partition_map()));
  ObPGPartitionGuard rguard;

  if (!ObReplicaTypeCheck::is_readable_replica(get_replica_type_())) {
    ret = OB_REPLICA_NOT_READABLE;
    STORAGE_LOG(WARN, "replica is not readable", K(ret), "this", *this);
  } else if (OB_FAIL(rp.get_pg_partition(rpkey, rguard))) {
    STORAGE_LOG(WARN, "get pg partition error", K(ret), K(right_param));
  } else if (OB_ISNULL(lguard.get_pg_partition()) || OB_ISNULL(rguard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(lpkey), K(rpkey), K_(pkey));
  } else if (is_bounded_staleness_read && OB_FAIL(get_based_schema_version(left_data_max_schema_version))) {
    STORAGE_LOG(
        WARN, "get based schema version for left param fail", K(ret), K(lpkey), K(rpkey), K_(pkey), K(left_param));
  } else if (is_bounded_staleness_read &&
             OB_FAIL(rp.get_pg_storage().get_based_schema_version(right_data_max_schema_version))) {
    STORAGE_LOG(
        WARN, "get based schema version for right param fail", K(ret), K(lpkey), K(rpkey), K_(pkey), K(right_param));
  } else if (OB_FAIL(lguard.get_pg_partition()->join_mv_scan(left_param,
                 right_param,
                 left_data_max_schema_version,
                 right_data_max_schema_version,
                 *rguard.get_pg_partition(),
                 result))) {
    STORAGE_LOG(WARN,
        "join mv scan failed",
        K(ret),
        K(left_param),
        K(right_param),
        K(left_data_max_schema_version),
        K(right_data_max_schema_version));
  } else {
    // do nothing
  }

  return ret;
}

int ObPGStorage::delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->delete_rows(ctx, dml_param, column_ids, row_iter, affected_rows))) {
    STORAGE_LOG(WARN, "failed to delete row", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }

  return ret;
}

int ObPGStorage::delete_row(
    const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->delete_row(ctx, dml_param, column_ids, row))) {
    STORAGE_LOG(WARN, "failed to delete row", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
    ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->put_rows(ctx, dml_param, column_ids, row_iter, affected_rows))) {
    STORAGE_LOG(WARN, "failed to put rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->insert_rows(ctx, dml_param, column_ids, row_iter, affected_rows))) {
    STORAGE_LOG(WARN, "failed to insert rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::insert_row(
    const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->insert_row(ctx, dml_param, column_ids, row))) {
    STORAGE_LOG(WARN, "failed to insert rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& duplicated_column_ids,
    const common::ObNewRow& row, const ObInsertFlag flag, int64_t& affected_rows,
    common::ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->insert_row(
                 ctx, dml_param, column_ids, duplicated_column_ids, row, flag, affected_rows, duplicated_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE != ret) {
      STORAGE_LOG(WARN, "failed to insert rows", K(ret), "this", *this, K(ctx));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& in_column_ids, const ObIArray<uint64_t>& out_column_ids, ObNewRowIterator& check_row_iter,
    ObIArray<ObNewRowIterator*>& dup_row_iters)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->fetch_conflict_rows(
                 ctx, dml_param, in_column_ids, out_column_ids, check_row_iter, dup_row_iters))) {
    STORAGE_LOG(WARN, "failed to fetch conflict rows", K(ret), KPC(this), K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::revert_insert_iter(const common::ObPartitionKey& pkey, ObNewRowIterator* iter)
{
  int ret = OB_SUCCESS;

  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), KP(iter));
  } else {
    const common::ObPartitionKey& key = (pkey_.is_pg() ? pkey : pkey_);
    ObPGPartition* pg_partition = NULL;
    ObPGPartitionGuard guard(key, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(pkey), K_(pkey));
    } else {
      ret = pg_partition->get_storage()->revert_insert_iter(iter);
    }
  }
  return ret;
}

int ObPGStorage::update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& updated_column_ids, ObNewRowIterator* row_iter,
    int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(
                 pg_partition->update_rows(ctx, dml_param, column_ids, updated_column_ids, row_iter, affected_rows))) {
    STORAGE_LOG(WARN, "failed to update rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& updated_column_ids, const ObNewRow& old_row,
    const ObNewRow& new_row)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->update_row(ctx, dml_param, column_ids, updated_column_ids, old_row, new_row))) {
    STORAGE_LOG(WARN, "failed to update row", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
    ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->lock_rows(ctx, dml_param, abs_lock_timeout, row_iter, lock_flag, affected_rows))) {
    STORAGE_LOG(WARN, "failed to lock rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
    const ObNewRow& row, const ObLockFlag lock_flag)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = (pkey_.is_pg() ? ctx.cur_pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (!ObReplicaTypeCheck::is_writable_replica(get_replica_type_())) {
    ret = OB_ERR_READ_ONLY;
    STORAGE_LOG(ERROR, "replica is not writable", K(ret), "this", *this);
  } else if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->lock_rows(ctx, dml_param, abs_lock_timeout, row, lock_flag))) {
    STORAGE_LOG(WARN, "failed to lock rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::get_based_schema_version(int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = -1;
  ObTablesHandle tables_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_memtables(tables_handle))) {
    LOG_WARN("failed to get memtables", K(ret), K_(pkey));
  } else {
    const int64_t memtable_count = tables_handle.get_count();
    ObITable* memtable = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < memtable_count; ++i) {
      if (OB_ISNULL(memtable = tables_handle.get_table(i))) {
        ret = OB_ERR_SYS;
        LOG_ERROR("memtable must not null", K(ret));
      } else if (OB_UNLIKELY(!memtable->is_memtable())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("get_memtables returns non-memtable", K(ret), K(pkey_), K(*memtable));
      } else {
        schema_version = std::max(schema_version, static_cast<ObMemtable*>(memtable)->get_max_schema_version());
      }
    }
    if (OB_SUCC(ret)) {
      TCRLockGuard lock_guard(lock_);
      schema_version = std::max(schema_version, meta_->storage_info_.get_data_info().get_schema_version());
      schema_version = std::max(schema_version, meta_->create_schema_version_);
    }
  }
  return ret;
}

int ObPGStorage::check_can_release_pg_memtable_(ObTablesHandle& memtable_merged, ObTablesHandle& memtable_to_release)
{
  int ret = OB_SUCCESS;
  ObPartitionArray partitions;
  ObTablesHandle memtables_handle;
  int64_t schema_version =
      std::max(meta_->storage_info_.get_data_info().get_schema_version(), meta_->create_schema_version_);
  uint64_t replay_log_id = meta_->storage_info_.get_data_info().get_last_replay_log_id();
  int64_t trans_table_end_log_ts = 0;
  int64_t timestamp = 0;
  memtable_merged.reset();
  memtable_to_release.reset();

  if (OB_FAIL(get_all_pg_partition_keys_(partitions))) {
    STORAGE_LOG(WARN, "failed to get all pg partition keys", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_memtables(memtables_handle))) {
    STORAGE_LOG(WARN, "failed to get all memtables", K(ret));
  } else if (OB_FAIL(get_trans_table_end_log_ts_and_timestamp_(trans_table_end_log_ts, timestamp))) {
    LOG_WARN("failed to get_trans_table_end_log_ts", K(ret), K(pkey_));
  } else {
    ObMemtable* memtable = nullptr;
    for (int64_t midx = 0; OB_SUCC(ret) && midx < memtables_handle.get_count(); ++midx) {
      if (OB_ISNULL(memtable = static_cast<ObMemtable*>(memtables_handle.get_table(midx)))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "memtable is null", K(ret), K(midx));
      } else if (!memtable->can_be_minor_merged()) {
        break;
      } else if (trans_table_end_log_ts < memtable->get_end_log_ts() || timestamp < memtable->get_timestamp()) {
        // trans table is not merged yet
        break;
      } else {
        bool pg_all_merged = true;
        bool pg_can_release = true;
        bool part_all_merged = false;
        bool part_can_release = false;
        schema_version = std::max(schema_version, memtable->get_max_schema_version());
        for (int64_t i = 0; OB_SUCC(ret) && (pg_all_merged || pg_can_release) && i < partitions.count(); ++i) {
          ObPGPartition* pg_partition = nullptr;
          ObPartitionStorage* storage = nullptr;
          const ObPartitionKey& pkey = partitions.at(i);
          ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
          if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "pg partition is null", K(ret), K(pkey));
          } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "partition storage is null", K(ret), K(pkey));
          } else if (OB_FAIL(storage->get_partition_store().check_all_merged(
                         *memtable, schema_version, part_all_merged, part_can_release))) {
            STORAGE_LOG(WARN, "failed to check if partition merged", K(ret), K(pkey));
          } else {
            pg_all_merged = pg_all_merged && part_all_merged;
            pg_can_release = pg_can_release && part_can_release;
          }
        }
        if (OB_SUCC(ret)) {
          if (!pg_all_merged && !pg_can_release) {
            break;
          } else if (pg_all_merged) {
            if (OB_FAIL(memtable_merged.add_table(memtable))) {
              LOG_WARN("failed to add table", K(ret), K(*memtable));
            } else if (pg_can_release) {
              if (OB_FAIL(memtable_to_release.add_table(memtable))) {
                LOG_WARN("failed to add table", K(ret), K(*memtable));
              } else {
                FLOG_INFO("add memtable to release", K(*memtable), K(schema_version));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPGStorage::check_release_memtable(ObIPartitionReport& report)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_merged = true;
  ObTablesHandle memtable_merged;
  ObTablesHandle memtable_to_release;

  if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
    STORAGE_LOG(INFO, "check_release_memtable", K(pkey_));
  }
#ifdef ERRSIM
  const bool skip_update = GCONF.skip_update_storage_info;
  if (skip_update) {
    return OB_SUCCESS;
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage is not inited", K(ret));
  } else {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);
    if (OB_FAIL(check_split_dest_merged_(is_merged))) {
      LOG_WARN("failed to check split dest merged", K(ret), K(pkey_));
    } else if (!is_merged) {
      // do nothing
    } else if (OB_FAIL(check_can_release_pg_memtable_(memtable_merged, memtable_to_release))) {
      LOG_WARN("failed to check_can_release_pg_memtable_", K(ret), K(pkey_));
    } else {
      for (int64_t midx = 0; OB_SUCC(ret) && midx < memtable_merged.get_count(); ++midx) {
        ObMemtable* memtable = nullptr;
        ObSavedStorageInfoV2 info;
        if (OB_ISNULL(memtable = static_cast<ObMemtable*>(memtable_merged.get_table(midx)))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "memtable is null", K(ret), K(midx));
        } else if (memtable->get_partition_key() != pkey_) {
          // the memtable is from split source partition, do nothing
        } else if (OB_FAIL(memtable->get_base_storage_info(info))) {
          STORAGE_LOG(WARN, "failed to get base storage info", K(ret));
        } else {
          ObPartitionGroupMeta* next_meta_ptr = nullptr;
          if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
            LOG_WARN("failed to alloc meta", K(ret));
          } else if (info.get_data_info().get_publish_version() >
                     meta_->storage_info_.get_data_info().get_publish_version()) {
            ObPartitionGroupMeta& meta = *next_meta_ptr;
            const bool replica_with_data = true;
            const ObBaseStorageInfo& old_clog_info = meta_->storage_info_.get_clog_info();

            STORAGE_LOG(INFO, "will update storage info", K_(pkey), K(*memtable));
            info.get_data_info().inc_update_schema_version(meta_->storage_info_.get_data_info().get_schema_version());

            if (OB_FAIL(info.update_and_fetch_log_info(
                    pkey_, replica_with_data, old_clog_info, PG_LOG_INFO_QUERY_TIMEOUT, false /*log_info_usable*/))) {
              STORAGE_LOG(WARN, "failed to update_info_and_fetch_checksum", K(ret), K_(pkey), K(info), K(meta_));
            } else if (OB_FAIL(meta.deep_copy(*meta_))) {
              STORAGE_LOG(WARN, "failed to deep_copy meta", K(ret), K_(pkey));
            } else {
              if (OB_FAIL(meta.storage_info_.deep_copy(info))) {
                STORAGE_LOG(WARN, "failed to deep copy storage info", K(ret), K_(pkey));
              } else if (OB_FAIL(write_update_pg_meta_trans(meta, OB_LOG_UPDATE_STORAGE_INFO_AFTER_RELEASE_MEMTABLE))) {
                STORAGE_LOG(WARN, "failed to write_update_pg_meta_trans", K(ret), K_(pkey));
              } else {
                switch_meta_(next_meta_ptr);
                memtable->set_minor_merged();
              }
            }
          }
          free_meta_(next_meta_ptr);
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t midx = 0; OB_SUCC(ret) && midx < memtable_to_release.get_count(); ++midx) {
          ObMemtable* memtable = nullptr;
          if (OB_ISNULL(memtable = static_cast<ObMemtable*>(memtable_to_release.get_table(midx)))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "memtable is null", K(ret), K(midx));
          } else {
            ObITable::TableKey memtable_key = memtable->get_key();
            const int64_t ref = memtable->get_ref();
            ObTableHandle handle;
            if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = pg_memtable_mgr_.get_first_frozen_memtable(handle)))) {
              STORAGE_LOG(WARN, "fail to get frozen memtable", K(tmp_ret), K(memtable_key));
            } else if (!handle.is_valid()) {
              STORAGE_LOG(ERROR, "frozen memtable is not valid", K(handle), K(memtable_key));
            } else if (memtable != handle.get_table()) {
            } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = remove_mem_ctx_for_trans_ctx_(memtable)))) {
              STORAGE_LOG(WARN, "failed to remove ctx for txn", K(tmp_ret), KP(memtable));
            } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = pg_memtable_mgr_.release_head_memtable(memtable)))) {
              STORAGE_LOG(WARN, "failed to release head memtable", K(tmp_ret), K(memtable_key));
            } else {
              FLOG_INFO("succeed to release memtable", K(tmp_ret), K(memtable_key), K(ref));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (tmp_ret = check_for_restore_(report))) {
      LOG_WARN("failed to check set membset list for restore", K(ret), K(pkey_));
    }
  }
  return ret;
}

int ObPGStorage::check_for_restore_(ObIPartitionReport& report)
{
  int ret = OB_SUCCESS;
  int16_t status = get_restore_state();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (REPLICA_RESTORE_DUMP_MEMTABLE == status || REPLICA_RESTORE_WAIT_ALL_DUMPED == status ||
             REPLICA_RESTORE_MEMBER_LIST == status) {
    int64_t new_next_replay_log_ts = 0;
    bool need_dump_memtable = false;
    ObTablesHandle memtables_handle;
    if (OB_FAIL(pg_memtable_mgr_.get_all_memtables(memtables_handle))) {
      LOG_WARN("failed to get all memtables for restore", K(ret));
    }
    const int64_t memtable_count = memtables_handle.get_count();
    for (int64_t i = memtable_count - 1; OB_SUCC(ret) && i >= 0; --i) {
      ObITable* table = memtables_handle.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got table is NULL", K(pkey_), K(i), K(ret));
      } else if (table->is_frozen_memtable()) {
        need_dump_memtable = true;
        new_next_replay_log_ts = table->get_snapshot_version() + 1;
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (need_dump_memtable) {
        share::ObPhysicalRestoreInfo restore_info;
        if (OB_FAIL(ObBackupInfoMgr::get_instance().get_restore_info(pkey_.get_tenant_id(), restore_info))) {
          ARCHIVE_LOG(WARN, "failed to get_restore_info", KR(ret), K(pkey_));
        } else if (new_next_replay_log_ts >= restore_info.restore_start_ts_) {
          ret = OB_EAGAIN;
          LOG_WARN("can not update next_replay_log_ts to the value",
              KR(ret),
              K(new_next_replay_log_ts),
              K(restore_info),
              K_(pkey));
        } else if (OB_FAIL(pls_->try_update_next_replay_log_ts_in_restore(new_next_replay_log_ts))) {
          LOG_WARN("Failed to try_update_next_replay_log_ts_in_resotre", K(ret), K_(pkey), K(new_next_replay_log_ts));
        } else { /*do nothing*/
        }
      } else if (REPLICA_RESTORE_DUMP_MEMTABLE == status) {
        // minor merge finish, change restore state REPLICA_RESTORE_WAIT_ALL_DUMPED
        if (OB_FAIL(set_restore_flag(
                REPLICA_RESTORE_WAIT_ALL_DUMPED, OB_INVALID_TIMESTAMP /*not update restore_snapshot_version */))) {
          LOG_WARN("failed to set restore flag REPLICA_RESTORE_WAIT_ALL_DUMPED", K(ret), K_(pkey));
        } else if (OB_FAIL(report.submit_pt_update_task(pkey_))) {
          LOG_WARN("Failed to submit pg pt update task", K(ret), K_(pkey));
        } else { /*do nothing*/
        }
      }
    }
  } else { /*do nothing*/
  }

  return ret;
}

int ObPGStorage::check_restore_flag(const int16_t old_flag, const int16_t new_flag) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(new_flag < REPLICA_NOT_RESTORE || new_flag > REPLICA_RESTORE_MEMBER_LIST)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid restore flag", K(ret), K(old_flag), K(new_flag));
  } else {
    switch (old_flag) {
      case REPLICA_NOT_RESTORE:
        ret = OB_STATE_NOT_MATCH;
        break;
      case REPLICA_LOGICAL_RESTORE_DATA:
        if (REPLICA_NOT_RESTORE != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      case REPLICA_RESTORE_DATA:
        if (REPLICA_RESTORE_LOG != new_flag && REPLICA_RESTORE_CUT_DATA != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      case REPLICA_RESTORE_ARCHIVE_DATA:
        if (REPLICA_RESTORE_LOG != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      case REPLICA_RESTORE_LOG:
        if (REPLICA_RESTORE_DUMP_MEMTABLE != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      case REPLICA_RESTORE_DUMP_MEMTABLE:
        if (REPLICA_RESTORE_WAIT_ALL_DUMPED != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      case REPLICA_RESTORE_WAIT_ALL_DUMPED:
        if (REPLICA_RESTORE_MEMBER_LIST != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      case REPLICA_RESTORE_MEMBER_LIST:
        if (REPLICA_NOT_RESTORE != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      case REPLICA_RESTORE_CUT_DATA:
        if (REPLICA_RESTORE_LOG != new_flag && REPLICA_NOT_RESTORE != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      case REPLICA_RESTORE_STANDBY:
        if (REPLICA_NOT_RESTORE != new_flag) {
          ret = OB_STATE_NOT_MATCH;
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }

    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "restore flag is not match", K(ret), K(old_flag), K(new_flag));
    }
  }
  return ret;
}

bool ObPGStorage::is_restore()
{
  bool bool_ret = false;

  if (IS_NOT_INIT) {
    LOG_WARN("not inited", K_(pkey));
  } else {
    TCRLockGuard guard(lock_);
    bool_ret = 0 != meta_->is_restore_;
  }
  return bool_ret;
}

bool ObPGStorage::is_pg_meta_valid()
{
  bool is_valid = false;
  if (is_inited_) {
    TCRLockGuard guard(lock_);
    is_valid = meta_->is_valid();
  }
  return is_valid;
}

ObReplicaRestoreStatus ObPGStorage::get_restore_status()
{
  ObReplicaRestoreStatus restore_status = REPLICA_RESTORE_STANDBY_MAX;

  if (IS_NOT_INIT) {
    LOG_WARN("not inited", K_(pkey));
  } else {
    TCRLockGuard guard(lock_);
    restore_status = static_cast<oceanbase::share::ObReplicaRestoreStatus>(meta_->is_restore_);
  }
  return restore_status;
}

int64_t ObPGStorage::get_create_frozen_version()
{
  int64_t frozen_version = 0;

  if (is_inited_) {
    TCRLockGuard guard(lock_);
    frozen_version = meta_->create_frozen_version_;
  }
  return frozen_version;
}

bool ObPGStorage::is_restoring_base_data()
{
  bool bool_ret = false;

  if (IS_NOT_INIT) {
    LOG_WARN("pg not inited", K_(pkey));
  } else {
    TCRLockGuard guard(lock_);
    bool_ret = REPLICA_RESTORE_DATA == meta_->is_restore_ || REPLICA_RESTORE_ARCHIVE_DATA == meta_->is_restore_;
  }
  return bool_ret;
}

bool ObPGStorage::is_restoring_standby()
{
  bool bool_ret = false;

  if (IS_NOT_INIT) {
    LOG_WARN("pg not inited", K_(pkey));
  } else {
    TCRLockGuard guard(lock_);
    bool_ret = REPLICA_RESTORE_STANDBY == meta_->is_restore_;
  }
  return bool_ret;
}

int16_t ObPGStorage::get_restore_state() const
{
  int16_t resotre = 0;
  if (IS_NOT_INIT) {
    LOG_WARN("pg not intied", K_(pkey));
  } else {
    TCRLockGuard guard(lock_);
    resotre = meta_->is_restore_;
  }
  return resotre;
}

int ObPGStorage::get_all_pg_partitions(ObPGPartitionArrayGuard& guard)
{
  int ret = OB_SUCCESS;
  GetAllPGPartitionFunctor functor(guard);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage is not inited", K(ret));
  } else if (OB_FAIL(partition_list_.for_each(functor))) {
    STORAGE_LOG(WARN, "failed to get all pg partitions", K(ret), K(pkey_));
  }
  return ret;
}

int ObPGStorage::set_pg_removed()
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  TCWLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage has not been inited", K(ret));
  } else if (!ObMigrateStatusHelper::check_allow_gc(meta_->migrate_status_)) {
    ret = OB_NOT_ALLOW_TO_REMOVE;
    LOG_WARN("migrate in is set, cannot set removed", K(ret), KP(this), K(*meta_));
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys, true /*include_trans_table*/))) {
    STORAGE_LOG(WARN, "get all pg partition keys", K(ret), K(pkeys));
  } else {
    ObPartitionStorage* partition_storage = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      ObPGPartitionGuard pg_partition_guard;
      if (OB_FAIL(get_pg_partition(pkeys.at(i), pg_partition_guard))) {
        STORAGE_LOG(WARN, "get pg partition guard error", K(ret), "pkey", pkeys.at(i));
      } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "get pg partition guard error", K(ret), "pkey", pkeys.at(i));
      } else if (OB_ISNULL(partition_storage = static_cast<ObPartitionStorage*>(
                               pg_partition_guard.get_pg_partition()->get_storage()))) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "partition storage mut nost null", K(ret), "pkey", pkeys.at(i));
      } else if (OB_FAIL(partition_storage->get_partition_store().set_removed())) {
        STORAGE_LOG(WARN, "failed to set removed", K(ret), "key", pkeys.at(i));
      } else {
        // do nothing
      }
    }
    is_removed_ = true;
    LOG_INFO("partition storage set removed", KP(this), K(ret), K(*meta_), K(lbt()));
  }

  return ret;
}

int ObPGStorage::write_update_pg_meta_trans(const ObPartitionGroupMeta& meta, const LogCommand& cmd)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_UPDATE_PARTITION_GROUP_META);
  int64_t lsn = 0;
  ObUpdatePartitionGroupMetaLogEntry log_entry;

  if (!meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(meta));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(pkey));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    STORAGE_LOG(WARN, "partition is removed", K(ret), K(meta));
  } else if (OB_FAIL(log_entry.meta_.deep_copy(meta))) {
    STORAGE_LOG(WARN, "failed to deep copy meta", K(ret));
  } else if (OB_FAIL(SLOGGER.begin(cmd))) {
    STORAGE_LOG(WARN, "Fail to begin update pg meta log, ", K(ret));
  } else {
    const ObStorageLogAttribute log_attr(meta.pg_key_.get_tenant_id(), meta_->storage_info_.get_pg_file_id());
    if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
      STORAGE_LOG(WARN, "Failed to write_update_partition_meta_trans", K(ret), K(log_attr));
    } else if (OB_FAIL(SLOGGER.commit(lsn))) {
      STORAGE_LOG(ERROR, "Fail to commit logger, ", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "succeed to write update partition meta trans", K(meta), K(lsn), K(*this));
    }

    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        STORAGE_LOG(ERROR, "write_update_pg_meta_trans logger abort error", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObPGStorage::allow_gc(bool& allow_gc)
{
  int ret = OB_SUCCESS;
  allow_gc = true;
  TCRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    allow_gc = ObMigrateStatusHelper::check_allow_gc(meta_->migrate_status_);
  }
  return ret;
}

int ObPGStorage::get_pg_migrate_status(ObMigrateStatus& migrate_status)
{
  int ret = OB_SUCCESS;
  int64_t ts = 0;
  if (OB_FAIL(get_migrate_status(migrate_status, ts))) {
    STORAGE_LOG(WARN, "fail to get migrate status", K(ret), K(pkey_));
  }
  return ret;
}

int ObPGStorage::get_migrate_status(ObMigrateStatus& migrate_status, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg stroage is not initialized", K(ret), K(pkey_));
  } else {
    migrate_status = meta_->migrate_status_;
    timestamp = meta_->migrate_timestamp_;
  }
  return ret;
}

int ObPGStorage::set_pg_migrate_status(const ObMigrateStatus status, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage is not initialized", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    STORAGE_LOG(WARN, "partition is removed", K(ret), K(*meta_));
  } else if (status < 0 || status >= OB_MIGRATE_STATUS_MAX) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "invalid migrate_status", K(ret), K(status), K(*meta_));
  } else if (status == meta_->migrate_status_) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "cannot set same migrate status", K(ret), K(status), K(*meta_));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    STORAGE_LOG(INFO, "set migrate status", K(status), K(timestamp), K(*this));
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    next_meta.reset();
    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      STORAGE_LOG(WARN, "failed to deep copy pg meta", K(ret), K(pkey_), K(*meta_));
    } else {
      next_meta.migrate_status_ = status;
      next_meta.migrate_timestamp_ = timestamp;
      if (OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_MIGRATE_STATUS))) {
        STORAGE_LOG(WARN, "failed to write update partition group meta log", K(ret));
      } else {
        switch_meta_(next_meta_ptr);
      }
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::try_update_report_status(
    ObIPartitionReport& report, const common::ObVersion& version, bool& is_finished, bool& need_report)
{
  int ret = OB_SUCCESS;
  ObPGPartition* pg_partition = nullptr;
  ObPartitionStorage* storage = nullptr;
  is_finished = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage is not inited", K(ret));
  } else if (OB_FAIL(check_need_report_(version, need_report))) {
    STORAGE_LOG(WARN, "failed to check_need_report", K(ret), K(pkey_), K(version));
  } else if (need_report) {
    const bool write_slog = true;
    ObPartitionArray pkeys;
    {
      ObBucketWLockAllGuard bucket_guard(bucket_lock_);
      if (OB_UNLIKELY(is_removed_)) {
        ret = OB_PG_IS_REMOVED;
        LOG_WARN("pg is removed", K(ret), K_(pkey));
      } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
        STORAGE_LOG(WARN, "failed to get all pg partition keys", K(ret), K(pkey_));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      ObFreezeInfoSnapshotMgr::FreezeInfo freeze_info;
      if (OB_FAIL(get_freeze_info_(version, freeze_info))) {
        LOG_WARN("failed to get_freeze_info_", K(ret), K(version));
      } else {
        // no need to lock
        for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
          bool is_part_finish = false;
          bool part_need_report = false;
          const ObPartitionKey& pkey = pkeys.at(i);
          ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
          if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "pg partition is null", K(ret), K(pkey));
          } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "partition storage is null", K(ret), K(pkey));
          } else {
            const int64_t bucket_idx = get_bucket_idx_(pkey);
            ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
            if (OB_FAIL(storage->try_update_report_status(version, is_part_finish, part_need_report))) {
              STORAGE_LOG(WARN, "failed to try_update_report_status_", K(ret), K_(pkey), K(version));
            }
          }
          if (OB_SUCC(ret)) {
            if (is_part_finish && part_need_report) {
              if (OB_FAIL(report.submit_checksum_update_task(pkey, OB_INVALID_ID, -1 /*report all sstables*/))) {
                STORAGE_LOG(ERROR, "failed to submit pt update task", K(ret));
              }
            } else if (!is_part_finish) {
              is_finished = false;
              break;
            }
          }
        }

        if (OB_SUCC(ret) && is_finished) {
          ObPGReportStatus status;
          int64_t default_data_version = version.major_;
          int64_t default_snapshot_version = 0;
          if (freeze_info.freeze_ts <= pg_memtable_mgr_.get_readable_ts()) {
            default_snapshot_version = freeze_info.freeze_ts;
          }
          ObBucketWLockAllGuard bucket_guard(bucket_lock_);
          if (OB_FAIL(update_report_status_and_schema_version_(
                  status, freeze_info.schema_version, default_data_version, default_snapshot_version, write_slog))) {
            STORAGE_LOG(WARN, "failed to update report status", K(ret), K_(pkey));
          }
        }
      }
    }
  }
  return ret;
}

int ObPGStorage::get_pg_meta(ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;

  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg store do not inited", K(ret));
  } else if (OB_FAIL(pg_meta.deep_copy(*meta_))) {
    STORAGE_LOG(WARN, "failed to copy pg meta", K(ret), K(*meta_));
  }
  return ret;
}

int ObPGStorage::get_pg_create_ts(int64_t& create_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg store do not inited", K(ret));
  } else {
    TCRLockGuard guard(lock_);
    create_ts = meta_->create_timestamp_;
  }
  return ret;
}

int ObPGStorage::get_major_version(ObVersion& major_version)
{
  int ret = OB_SUCCESS;
  major_version = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg store do not inited", K(ret));
  } else {
    TCRLockGuard guard(lock_);
    major_version.version_ = meta_->report_status_.data_version_;
  }
  return ret;
}

int ObPGStorage::get_pg_partition_store_meta(const ObPartitionKey& pkey, ObPGPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;
  meta.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage do not inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get partition store meta get invalid argument", K(ret), K(pkey));
  } else {
    TCRLockGuard lock_guard(lock_);
    ObPGPartition* pg_partition = NULL;
    ObPartitionStorage* partition_storage = NULL;
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
    } else if (OB_ISNULL(partition_storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get partition storage", K(ret), KP(partition_storage));
    } else if (OB_FAIL(partition_storage->get_partition_store().get_meta(meta))) {
      STORAGE_LOG(WARN, "fail to get partition store meta", K(ret));
    }
  }
  return ret;
}

int ObPGStorage::check_need_report_(const ObVersion& version, bool& need_report)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  need_report = ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_) &&
                version > meta_->report_status_.data_version_;
  return ret;
}

int ObPGStorage::update_report_status_(
    ObPGReportStatus& pg_report_status, const int64_t default_data_version, const int64_t default_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;

  pg_report_status.reset();
  pg_report_status.data_version_ = default_data_version;
  pg_report_status.snapshot_version_ = default_snapshot_version;
  if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    STORAGE_LOG(WARN, "failed to get_all_pg_partition_keys", K(ret), K_(pkey));
  } else {
    ObReportStatus report_status;
    int64_t data_version = INT64_MAX;
    int64_t snapshot_version = INT64_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      ObPGPartition* pg_partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      const ObPartitionKey& pkey = pkeys.at(i);
      ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
      if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition is null", K(ret), K(pkey));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition storage is null", K(ret), K(pkey));
      } else if (OB_FAIL(storage->get_partition_store().get_report_status(report_status))) {
        STORAGE_LOG(WARN, "failed to get report status", K(ret), K(pkey));
      } else {
        pg_report_status.data_size_ += report_status.data_size_;
        pg_report_status.required_size_ += report_status.required_size_;
        if (report_status.data_version_ < data_version) {
          data_version = report_status.data_version_;
        }
        if (report_status.snapshot_version_ < snapshot_version) {
          snapshot_version = report_status.snapshot_version_;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (INT64_MAX != data_version) {
        pg_report_status.data_version_ = data_version;
      }

      if (INT64_MAX != snapshot_version) {
        pg_report_status.snapshot_version_ = snapshot_version;
      }
    }
  }
  return ret;
}

int ObPGStorage::update_report_status_(const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObPGReportStatus pg_report_status;

  if (OB_FAIL(update_report_status_(pg_report_status))) {
    LOG_WARN("failed to update report status", K(ret), K_(pkey));
  } else if (OB_FAIL(write_report_status_(pg_report_status, write_slog))) {
    STORAGE_LOG(WARN, "failed to write report status", K(ret), K_(pkey));
  }
  return ret;
}

int ObPGStorage::update_report_status_and_schema_version_(ObPGReportStatus& status, const int64_t schema_version,
    const int64_t default_data_version, const int64_t default_snapshot_version, const bool write_slog)
{
  int ret = OB_SUCCESS;
  bool need_reset = false;

  if (OB_FAIL(update_report_status_(status, default_data_version, default_snapshot_version))) {
    LOG_WARN("failed to update report status", K(ret), K_(pkey));
  } else if (OB_FAIL(write_report_status_and_schema_version_(status, schema_version, write_slog))) {
    LOG_WARN("failed to write_report_status_and_schema_version_", K(ret), K_(pkey), K(schema_version), K(status));
  }
  return ret;
}

int ObPGStorage::get_migrate_table_ids(const ObPartitionKey& pkey, ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  table_ids.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage do not inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get partition store meta get invalid argument", K(ret), K(pkey));
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPartitionStorage* partition_storage = NULL;
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
    } else if (NULL == (partition_storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      STORAGE_LOG(WARN, "fail to get partition storage", K(ret), KP(partition_storage));
    } else if (OB_FAIL(partition_storage->get_partition_store().get_migrate_table_ids(table_ids))) {
      STORAGE_LOG(WARN, "fail to get partition store meta", K(ret));
    }
  }
  return ret;
}

int ObPGStorage::get_partition_tables(
    const ObPartitionKey& pkey, const uint64_t table_id, ObTablesHandle& tables_handle, bool& is_ready_for_read)
{
  int ret = OB_SUCCESS;
  tables_handle.reset();
  is_ready_for_read = false;
  int64_t kept_max_snapshot = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage do not inited", K(ret));
  } else if (!pkey.is_valid() || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get partition store meta get invalid argument", K(ret), K(pkey), K(table_id));
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPartitionStorage* partition_storage = NULL;
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
    } else if (NULL == (partition_storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "fail to get partition storage", K(ret), KP(partition_storage));
    } else if (OB_FAIL(partition_storage->get_partition_store().get_effective_tables(
                   table_id, tables_handle, is_ready_for_read))) {
      STORAGE_LOG(WARN, "fail to get_effective_tables", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObPGStorage::get_partition_gc_tables(
    const ObPartitionKey& pkey, const uint64_t table_id, ObTablesHandle& gc_tables_handle)
{

  int ret = OB_SUCCESS;
  gc_tables_handle.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage do not inited", K(ret));
  } else if (!pkey.is_valid() || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get partition store meta get invalid argument", K(ret), K(pkey), K(table_id));
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPartitionStorage* partition_storage = NULL;
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
    } else if (NULL == (partition_storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "fail to get partition storage", K(ret), KP(partition_storage));
    } else if (OB_FAIL(partition_storage->get_partition_store().get_gc_sstables(table_id, gc_tables_handle))) {
      STORAGE_LOG(WARN, "fail to gc_tables_handle", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObPGStorage::write_report_status_(const ObPGReportStatus& status, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    next_meta.reset();

    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      STORAGE_LOG(WARN, "failed to deep copy pg meta", K(ret));
    } else {
      next_meta.report_status_ = status;
      if (write_slog && OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_WRITE_REPORT_STATUS))) {
        STORAGE_LOG(WARN, "failed to write_update_pg_meta_trans", K(ret));
      } else {
        ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(INFO, "succeed to update pg report status", "status", next_meta.report_status_, K(*this));
        switch_meta_(next_meta_ptr);
      }
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::write_report_status_and_schema_version_(
    const ObPGReportStatus& status, const int64_t schema_version, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    next_meta.reset();

    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      STORAGE_LOG(WARN, "failed to deep copy pg meta", K(ret));
    } else {
      next_meta.report_status_ = status;
      if (schema_version > 0) {
        next_meta.storage_info_.get_data_info().inc_update_schema_version(schema_version);
      }
      if (write_slog && OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_WRITE_REPORT_STATUS))) {
        STORAGE_LOG(WARN, "failed to write_update_pg_meta_trans", K(ret));
      } else {
        ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(INFO, "succeed to update pg report status", "status", next_meta.report_status_, K(*this));
        switch_meta_(next_meta_ptr);
      }
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::replay_partition_group_meta(const ObPartitionGroupMeta& meta)
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  TCWLockGuard lock_guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret), K(meta));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(meta));
  } else {
    const int64_t cur_pg_file_id = meta_->storage_info_.get_pg_file_id();
    meta_->reset();

    if (OB_FAIL(meta_->deep_copy(meta))) {
      LOG_WARN("failed to copy pg meta", K(ret), K(meta));
    } else {
      ObMigrateStatus reboot_status = OB_MIGRATE_STATUS_MAX;
      if (OB_FAIL(ObMigrateStatusHelper::trans_reboot_status(meta_->migrate_status_, reboot_status))) {
        LOG_WARN("failed to trans_migrate_reboot_status", K(ret), K_(*meta));
      } else if (meta_->migrate_status_ != reboot_status) {
        LOG_INFO("override migrate status after reboot",
            "pkey",
            meta_->pg_key_,
            "old",
            meta_->migrate_status_,
            K(reboot_status));
        meta_->migrate_status_ = reboot_status;
      }

      if (OB_SUCC(ret) && OB_INVALID_DATA_FILE_ID == meta_->storage_info_.get_pg_file_id()) {
        // the old format meta hasn't pg_file_id_
        meta_->storage_info_.set_pg_file_id(cur_pg_file_id);
      }
      LOG_INFO("succeed to replay partition group meta", K(*meta_));
    }
  }
  return ret;
}

int64_t ObPGStorage::get_meta_serialize_size_()
{
  int64_t serialize_size = 0;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    serialize_size += serialization::encoded_length_i64(MAGIC_NUM);
    serialize_size += serialization::encoded_length_i64(log_seq_num_);
    serialize_size += meta_->get_serialize_size();
  }
  if (OB_FAIL(ret)) {
    serialize_size = 0;
  }
  return serialize_size;
}

int ObPGStorage::replay_pg_partition_store(ObPGPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    const int64_t bucket_idx = get_bucket_idx_(meta.pkey_);
    const bool write_slog = false;
    ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
    if (is_removed_) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K_(pkey));
    } else if (!meta.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(meta));
    } else {
      ObPGPartitionGuard pg_partition_guard(meta.pkey_, *(pg_->get_pg_partition_map()));
      ObPGPartition* pg_partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      meta.replica_type_ = meta_->replica_type_;
      if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
        FLOG_INFO("pg partition not exist when replay_pg_partition_store", K(meta));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition storage is null", K(ret));
      } else if (storage->get_partition_store().is_inited()) {
        FLOG_INFO("pg partition is load from checkpoint, skip replay create", K(meta));
      } else if (OB_FAIL(storage->get_partition_store().create_partition_store(
                     meta, write_slog, pg_, ObFreezeInfoMgrWrapper::get_instance(), &pg_memtable_mgr_))) {
        LOG_WARN("failed to create partition store", K(ret), K(meta));
      } else {
        // do nothing
      }
    }
  }

  {
    TCRLockGuard guard(lock_);
    FLOG_INFO("pg storage replay create pg partition store", K(ret), K(pkey_), K(*this), K(meta));
  }

  return ret;
}

int ObPGStorage::replay_pg_partition_meta(ObPGPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    const int64_t bucket_idx = get_bucket_idx_(meta.pkey_);
    ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
    if (is_removed_) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K_(pkey));
    } else if (!meta.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(meta));
    } else {
      ObPGPartitionGuard pg_partition_guard(meta.pkey_, *(pg_->get_pg_partition_map()));
      ObPGPartition* pg_partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      meta.replica_type_ = meta_->replica_type_;
      if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
        FLOG_INFO("pg partition not exist when replay pg partition meta", K(ret), K(meta));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition storage is null", K(ret));
      } else if (OB_FAIL(storage->get_partition_store().replay_partition_meta(meta))) {
        LOG_WARN("failed to replay pg partition meta", K(ret), K(meta));
      } else {
        cached_replica_type_ = meta.replica_type_;
      }
    }
  }
  FLOG_INFO("pg storage replay pg partition store meta", K(ret), K(pkey_), K(*meta_), K(meta));
  return ret;
}

int ObPGStorage::update_multi_version_start(const ObPartitionKey& pkey, const int64_t multi_version_start)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage do not inited", K(ret));
  } else if (!pkey.is_valid() || multi_version_start <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "update multi version start get invalid argument", K(ret), K(pkey), K(multi_version_start));
  } else {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);
    TCWLockGuard guard(lock_);
    ObPGPartition* pg_partition = NULL;
    ObPartitionStorage* partition_storage = NULL;
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
      STORAGE_LOG(INFO, "pg partition not exist, no need update multi version start", K(ret), K(pkey_), K(pkey));
    } else if (OB_UNLIKELY(is_removed_)) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K_(pkey));
    } else if (NULL == (partition_storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      STORAGE_LOG(WARN, "fail to get partition storage", K(ret), KP(partition_storage));
    } else if (OB_FAIL(partition_storage->update_multi_version_start(multi_version_start))) {
      STORAGE_LOG(WARN, "fail to update multi version start", K(ret), K(multi_version_start));
    }
  }
  return ret;
}

int ObPGStorage::get_last_all_major_sstable(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  ObPartitionArray pkeys;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage has not been inited", K(ret));
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    STORAGE_LOG(WARN, "failed to get all pg partition keys", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      ObPGPartition* pg_partition = NULL;
      const ObPartitionKey& pkey = pkeys.at(i);
      ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
      if (NULL == (pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition is null, unexpected error", K(ret), K(pkey));
      } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                             ->get_partition_store()
                             .get_last_all_major_sstable(handle))) {
        STORAGE_LOG(WARN, "push frozen memstore per table", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

OB_INLINE int64_t ObPGStorage::get_bucket_idx_(const ObPartitionKey& pkey) const
{
  return pkey.hash() % BUCKET_LOCK_BUCKET_CNT;
}

int ObPGStorage::set_restore_flag(const int16_t restore_flag, const int64_t restore_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("pg storage is not inited", K(ret));
    } else if (is_removed_) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K(pkey_));
    } else if (meta_->restore_snapshot_version_ != OB_INVALID_TIMESTAMP &&
               restore_snapshot_version != OB_INVALID_TIMESTAMP &&
               meta_->restore_snapshot_version_ != restore_snapshot_version) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid restore_snapshot_version", K(ret), K(restore_snapshot_version), K(*meta_));
    } else if (restore_flag == meta_->is_restore_) {
      LOG_INFO("set same restore, ignore", K(ret), K(pkey_), K(restore_flag), K(*meta_));
    } else if (OB_FAIL(check_restore_flag(meta_->is_restore_, restore_flag))) {
      LOG_WARN("check restore falg fail", K(ret), K(pkey_));
    } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
      LOG_WARN("failed to alloc meta", K(ret));
    } else {
      ObPartitionGroupMeta& next_meta = *next_meta_ptr;
      if (REPLICA_RESTORE_LOG == restore_flag) {
        if (!is_sys_table(pkey_.get_table_id())) {
          DEBUG_SYNC(BEFORE_UPDATE_RESTORE_FLAG_RESTORE_LOG);
        }
      }

      next_meta.reset();
      if (OB_FAIL(next_meta.deep_copy(*meta_))) {
        LOG_WARN("failed to get next meta", K(ret));
      } else {
        next_meta.is_restore_ = restore_flag;
        if (OB_INVALID_TIMESTAMP != restore_snapshot_version) {
          next_meta.restore_snapshot_version_ = restore_snapshot_version;
        }
        if (OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_RESTORE_FLAG))) {
          LOG_WARN("failed to write_update_pg_meta_trans", K(ret), K(*meta_), K(next_meta));
        } else {
          {
            switch_meta_(next_meta_ptr);
          }
          // TODO(haofan): this func cannot rollback, should not return ret.
          if (OB_FAIL(pls_->set_archive_restore_state(restore_flag))) {
            LOG_WARN("set_archive_restore_state failed", K(ret), K_(pkey));
          }
          FLOG_INFO("set is_restore",
              K(restore_flag),
              K(restore_snapshot_version),
              K_(pkey),
              "tenant_id",
              pkey_.get_tenant_id(),
              "is_inner_table",
              is_inner_table(pkey_.get_table_id()));
        }
      }
    }
  }

  free_meta_(next_meta_ptr);
  SERVER_EVENT_ADD("physical_restore",
      "set_restore_flag",
      "partition",
      pkey_,
      "restore_flag",
      restore_flag,
      "restore_snapshot_version",
      restore_snapshot_version,
      "ret",
      ret);
  return ret;
}

int ObPGStorage::set_last_restore_log_id(const int64_t last_restore_log_id)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(pkey_));
  } else if (last_restore_log_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(last_restore_log_id));
  } else if (meta_->last_restore_log_id_ != OB_INVALID_ID) {
    if (last_restore_log_id != meta_->last_restore_log_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot set last_restore_log_id twice", K(ret), K(last_restore_log_id), K(*meta_));
    } else {
      FLOG_INFO("last_restore_log_id has already been assigned",
          K_(pkey),
          "tenant_id",
          pkey_.get_tenant_id(),
          K(last_restore_log_id));
    }
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    ObPartitionGroupMeta& next_meta = *next_meta_ptr;
    next_meta.reset();
    if (OB_FAIL(next_meta.deep_copy(*meta_))) {
      LOG_WARN("failed to get next meta", K(ret));
    } else {
      next_meta.last_restore_log_id_ = last_restore_log_id;
      if (OB_FAIL(write_update_pg_meta_trans(next_meta, OB_LOG_SET_PG_LAST_RESTORE_LOG_ID))) {
        LOG_WARN("failed to write_update_pg_meta_trans", K(ret), K(*meta_), K(next_meta));
      } else {
        switch_meta_(next_meta_ptr);
        FLOG_INFO("last_restore_log_id", K_(pkey), "tenant_id", pkey_.get_tenant_id(), K(last_restore_log_id));
      }
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::get_restore_replay_info(uint64_t& last_restore_log_id, int64_t& restore_snapshot_version)
{
  int ret = OB_SUCCESS;

  last_restore_log_id = OB_INVALID_ID;
  restore_snapshot_version = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not init", K(ret));
  } else {
    TCRLockGuard guard(lock_);
    last_restore_log_id = meta_->last_restore_log_id_;
    restore_snapshot_version = meta_->restore_snapshot_version_;
  }
  return ret;
}

int ObPGStorage::set_partition_removed(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not init", K(ret));
  } else {
    const int64_t bucket_idx = get_bucket_idx_(pkey);
    ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
    if (OB_FAIL(set_partition_removed_(pkey))) {
      LOG_WARN("failed to set_partition_removed_nolock", K(ret));
    }
  }
  return ret;
}

int ObPGStorage::physical_flashback(const int64_t flashback_scn)
{
  int ret = OB_SUCCESS;
  ObPartitionArray partitions;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (flashback_scn <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical flashback get invalid argument", K(ret), K(flashback_scn));
  } else if (OB_FAIL(get_all_pg_partition_keys_(partitions))) {
    STORAGE_LOG(WARN, "failed to get all pg partition keys", K(ret));
  } else {
    ObPGPartition* pg_partition = nullptr;
    int64_t smallest_publish_version = INT64_MAX;
    int64_t schema_version = 0;
    int64_t tmp_publish_version = 0;
    int64_t tmp_schema_version = 0;
    ObVersion data_version;
    ObRecoverPoint recover_point;

    if (ObReplicaTypeCheck::is_log_replica(meta_->replica_type_)) {
      // just modified meta data info or clog info
      smallest_publish_version = 0;
      schema_version = 0;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); i++) {
        bool need_remove = false;
        ObPGPartitionGuard pg_partition_guard(partitions.at(i), *(pg_->get_pg_partition_map()));
        if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
          // may pg has create but partition do not create
          // do nothing
        } else if (OB_FAIL(check_flashback_partition_need_remove(flashback_scn, pg_partition, need_remove))) {
          LOG_WARN("failed to check flashback partition need remove", K(ret), K(flashback_scn));
        } else if (need_remove) {
          const ObPartitionKey& pkey = pg_partition->get_partition_key();
          const bool write_slog = false;
          const int64_t log_id = 0;
          if (OB_FAIL(remove_pg_partition_from_pg(pkey, write_slog, log_id))) {
            LOG_WARN("failed to remove pg partition from pg", K(ret), K(pkey));
          }
        } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                               ->get_partition_store()
                               .physical_flashback(flashback_scn, data_version))) {
          STORAGE_LOG(WARN, "failed to do physical flashback", K(ret), K(pg_partition->get_partition_key()));
        } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                               ->get_partition_store()
                               .get_major_frozen_versions(data_version, tmp_publish_version, tmp_schema_version))) {
          STORAGE_LOG(WARN, "failed to do physical flashback", K(ret), K(pg_partition->get_partition_key()));
        } else {
          if (tmp_publish_version < smallest_publish_version) {
            smallest_publish_version = tmp_publish_version;
            schema_version = tmp_schema_version;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (meta_->partitions_.empty()) {
        smallest_publish_version = 0;
        schema_version = 0;
      }

      if (INT64_MAX == smallest_publish_version) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(
            WARN, "[PHY_FLASHBACK]unpected smallest_publish_version", K(ret), K_(pkey), K(smallest_publish_version));
      } else if (OB_FAIL(meta_->get_recover_info_for_flashback(smallest_publish_version, recover_point))) {
        STORAGE_LOG(WARN,
            "[PHY_FLASHBACK]get_recover_info_for_flashback failed",
            K(ret),
            K_(pkey),
            K(smallest_publish_version));
      } else if (recover_point.submit_timestamp_ <= 0) {
        ret = OB_OP_NOT_ALLOW;
        STORAGE_LOG(ERROR,
            "[PHY_FLASHBACK]recover point is invalid, not allow do physical flashback",
            K(ret),
            K_(pkey),
            K(smallest_publish_version),
            K(recover_point));
      } else {
        STORAGE_LOG(INFO,
            "[PHY_FLASHBACK]recover point is valid",
            K(ret),
            K(smallest_publish_version),
            K(recover_point),
            K_(pkey));
      }
    }

    if (OB_SUCC(ret)) {
      // update pg meta
      const bool need_write_slog = false;
      meta_->storage_info_.get_data_info().set_publish_version(smallest_publish_version);
      meta_->storage_info_.get_data_info().set_schema_version(schema_version);
      meta_->storage_info_.get_data_info().set_last_replay_log_id(0);
      if (ObMultiClusterUtil::is_cluster_private_table(pkey_.get_table_id())) {
        // last_replay_log_id is 0 for private table
        meta_->storage_info_.get_clog_info().set_last_replay_log_id(0);
        meta_->storage_info_.get_clog_info().set_accumulate_checksum(0);
      } else {
        meta_->storage_info_.get_clog_info().set_last_replay_log_id(recover_point.recover_log_id_);
        meta_->storage_info_.get_clog_info().set_accumulate_checksum(recover_point.checksum_);
      }
      meta_->storage_info_.get_clog_info().set_submit_timestamp(recover_point.submit_timestamp_);
      meta_->storage_info_.get_data_info().set_created_by_new_minor_freeze();

      meta_->report_status_.reset();
      if (OB_FAIL(meta_->clear_recover_points_for_physical_flashback(smallest_publish_version))) {
        STORAGE_LOG(WARN, "failed to clear recover points", K(smallest_publish_version));
      } else if (meta_->partitions_.empty()) {
        // do nothing
      } else if (OB_FAIL(update_report_status_(need_write_slog))) {
        STORAGE_LOG(WARN, "failed to update report status", K(ret));
      }
    }
  }
  return ret;
}

int ObPGStorage::get_max_major_sstable_snapshot(int64_t& sstable_ts)
{
  int ret = OB_SUCCESS;
  sstable_ts = 0;
  int64_t temp_max_sstable_ts = 0;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage has not been inited", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_->partitions_.count(); ++i) {
    const ObPartitionKey& pkey = meta_->partitions_.at(i);
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get pg partition", K(ret), K(pkey));
    } else if (OB_ISNULL(
                   storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage can not be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().get_max_major_sstable_snapshot(temp_max_sstable_ts))) {
      LOG_WARN("failed to get_max_major_sstable_snapshot", K(ret), K(pkey));
    } else {
      sstable_ts = max(sstable_ts, temp_max_sstable_ts);
    }
  }
  return ret;
}

int ObPGStorage::get_min_max_major_version(int64_t& min_version, int64_t& max_version)
{
  int ret = OB_SUCCESS;
  min_version = INT64_MAX;
  max_version = INT64_MIN;
  int64_t tmp_min_version = 0;
  int64_t tmp_max_version = 0;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage has not been inited", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_->partitions_.count(); ++i) {
    const ObPartitionKey& pkey = meta_->partitions_.at(i);
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get pg partition", K(ret), K(pkey));
    } else if (OB_ISNULL(
                   storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage can not be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().get_min_max_major_version(tmp_min_version, tmp_max_version))) {
      LOG_WARN("failed to get_min_max_major_version", K(ret), K(pkey));
    } else {
      min_version = min(min_version, tmp_min_version);
      max_version = max(max_version, tmp_max_version);
    }
  }
  return ret;
}

int ObPGStorage::set_partition_removed_(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not init", K(ret));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().set_removed())) {
      LOG_WARN("failed to set partition removed", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPGStorage::create_index_table_store(
    const ObPartitionKey& pkey, const uint64_t table_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_idx = get_bucket_idx_(pkey);
  ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(is_removed_)) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(*meta_));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_)) {
    // not data replica, do nothing;
  } else if (pkey.get_tenant_id() != pkey_.get_tenant_id()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tenant id not match", K(ret), K(pkey), K(pkey_));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().create_index_table_store(table_id, schema_version))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("failed to create index table store", K(ret), K(pkey), K(table_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPGStorage::add_sstable(const ObPartitionKey& pkey, storage::ObSSTable* table,
    const int64_t max_kept_major_version_number, const bool in_slog_trans)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_idx = get_bucket_idx_(pkey);
  ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(is_removed_)) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(*meta_));
  } else if (pkey.get_tenant_id() != pkey_.get_tenant_id()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tenant id not match", K(ret), K(pkey), K(pkey_));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    const bool is_in_dest_split = is_dest_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_));
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().add_sstable(
                   table, max_kept_major_version_number, in_slog_trans, meta_->migrate_status_, is_in_dest_split))) {
      LOG_WARN("failed to add ssstable", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPGStorage::add_sstable_for_merge(const ObPartitionKey& pkey, storage::ObSSTable* table,
    const int64_t max_kept_major_version_number, ObSSTable* complement_minor_sstable)
{
  int ret = OB_SUCCESS;
  {
    const int64_t bucket_idx = get_bucket_idx_(pkey);
    ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("pg storage is not inited", K(ret));
    } else if (OB_UNLIKELY(is_removed_)) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K(*meta_));
    } else if (pkey.get_tenant_id() != pkey_.get_tenant_id()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("tenant id not match", K(ret), K(pkey), K(pkey_));
    } else {
      ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
      ObPGPartition* partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
      } else if (OB_FAIL(storage->get_partition_store().add_sstable_for_merge(table,
                     max_kept_major_version_number,
                     meta_->migrate_status_,
                     is_restore(),
                     is_source_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_)),
                     is_dest_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_)),
                     complement_minor_sstable))) {
        LOG_WARN("failed to add_sstable_for_merge", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPGStorage::halt_prewarm()
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(is_removed_)) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(*meta_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_->partitions_.count(); ++i) {
      const ObPartitionKey& pkey = meta_->partitions_.at(i);
      ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
      ObPGPartition* partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
      } else if (OB_FAIL(storage->get_partition_store().halt_prewarm())) {
        LOG_WARN("failed to halt_prewarm", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPGStorage::get_reference_memtables(ObTablesHandle& memtables)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_all_memtables(memtables))) {
    LOG_WARN("failed to get all memtables", K(ret));
  }
  return ret;
}

int ObPGStorage::remove_uncontinues_inc_tables(const ObPartitionKey& pkey, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObPGPartition* pg_partition = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage has not been inited", K(ret));
  } else if (!pkey.is_valid() || 0 == table_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "partition key is invalid", K(ret), K(pkey), K(table_id));
  } else {
    const int64_t bucket_idx = pkey.hash() % BUCKET_LOCK_BUCKET_CNT;
    ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);

    ObPGPartitionGuard pg_guard(pkey, *(pg_->get_pg_partition_map()));
    if (is_removed_) {
      ret = OB_PG_IS_REMOVED;
      LOG_WARN("pg is removed", K(ret), K_(pkey));
    } else if (OB_MIGRATE_STATUS_ADD != meta_->migrate_status_ && OB_MIGRATE_STATUS_MIGRATE != meta_->migrate_status_) {
      STORAGE_LOG(INFO,
          "pg is not migrate status is not add or migrate, "
          "no need remove uncontinues inc sstables",
          K(pkey),
          K(table_id),
          K(*this));
    } else if (NULL == (pg_partition = pg_guard.get_pg_partition())) {
      // may pg has create but partition do not create
      // do nothing
    } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                           ->get_partition_store()
                           .remove_uncontinues_inc_tables(table_id))) {
      STORAGE_LOG(WARN, "push frozen memstore per table", K(ret), K(pkey), K(table_id));
    }
  }
  return ret;
}

int ObPGStorage::replay_modify_table_store(const ObModifyTableStoreLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = log_entry.table_store_.get_partition_key();
  const int64_t bucket_idx = get_bucket_idx_(pkey);
  ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(is_removed_)) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(*meta_));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      FLOG_INFO("pg partition not exist when replay modify table store", K(ret), K(pkey), K(*meta_));
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().replay_modify_table_store(log_entry))) {
      LOG_WARN("failed to replay_modify_table_store", K(ret), K(pkey), K(log_entry));
    }
  }
  return ret;
}

int ObPGStorage::set_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(is_removed_)) {
    LOG_INFO("pg is removed", K(ret), K(*meta_));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().set_reference_tables(index_id, handle))) {
      LOG_WARN("failed to set_reference_tables", K(ret), K(pkey), K(index_id));
    }
  }
  return ret;
}

int ObPGStorage::get_partition_store_info(const ObPartitionKey& pkey, ObPartitionStoreInfo& info)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get pg partition", K(ret), K(pkey));
    } else if (OB_ISNULL(
                   storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage can not be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().get_partition_store_info(info))) {
      LOG_WARN("failed to get partition store info", K(ret), K(pkey));
    } else {
      info.is_restore_ = meta_->is_restore_;
      info.migrate_status_ = meta_->migrate_status_;
      info.migrate_timestamp_ = meta_->migrate_timestamp_;
      info.replica_type_ = meta_->replica_type_;
    }
  }
  return ret;
}

int ObPGStorage::replay_drop_index(const ObPartitionKey& pkey, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const bool write_slog = false;
  const int64_t bucket_idx = get_bucket_idx_(pkey);
  ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(is_removed_)) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(*meta_));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      FLOG_INFO("pg partition not exist when replay drop index", K(pkey), K(*meta_));
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().drop_index(table_id, write_slog))) {
      LOG_WARN("failed to replay drop index", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPGStorage::try_drop_unneed_index(const int64_t latest_schema_version /*OB_INVALID_VERSION*/)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObPartitionArray pkeys;
  int64_t local_version = OB_INVALID_VERSION;
  uint64_t tenant_id = pkey_.get_tenant_id();
  uint64_t fetch_tenant_id = is_inner_table(pkey_.get_table_id()) ? OB_SYS_TENANT_ID : tenant_id;
  int64_t refreshed_schema_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_UNLIKELY(is_removed_)) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K_(pkey));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("failed to get_tenant_full_schema_guard", K(ret), K_(pkey));
  } else if (OB_FAIL(schema_guard.get_schema_version(fetch_tenant_id, local_version))) {
    LOG_WARN("failed to get schema version", K(ret), K_(pkey));
  } else if (!share::schema::ObSchemaService::is_formal_version(local_version)) {
    // not formal schema version, just skip
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_refreshed_schema_version(
                 tenant_id, refreshed_schema_version))) {
    LOG_WARN("fail to get tenant refreshed schema version", K(ret));
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    LOG_WARN("failed to get all pg partition keys", K(ret));
  } else {
    ObSEArray<TableStoreStat, OB_MAX_INDEX_PER_TABLE> index_tables;
    const bool write_slog = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
      ObPGPartition* partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("pg partition not exist", K(ret), K(pkey));
      } else if (OB_UNLIKELY(is_removed_)) {
        ret = OB_PG_IS_REMOVED;
        LOG_WARN("pg is removed", K(ret), K_(pkey));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
      } else if (OB_FAIL(storage->get_partition_store().get_all_table_stats(index_tables))) {
        LOG_WARN("failed to get_all_table_ids", K(ret), K(pkey));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < index_tables.count(); ++j) {
          bool need_drop = false;
          const TableStoreStat& index_stat = index_tables.at(j);
          const ObTableSchema* index_schema = nullptr;
          if (index_stat.index_id_ == pkey.get_table_id()) {
            // main table or global index should skip drop index
            continue;
          } else if (OB_FAIL(schema_guard.get_table_schema(index_stat.index_id_, index_schema))) {
            LOG_WARN("failed to get_table_schema", K(ret), K(index_stat), K(pkey));
          } else if (OB_ISNULL(index_schema) ||
                     ObIndexStatus::INDEX_STATUS_INDEX_ERROR == index_schema->get_index_status()) {
            ObTablesHandle sstable_handle;
            transaction::ObTransService* trans_service = NULL;
            int64_t schema_version = 0;
            int64_t refreshed_schema_ts = 0;
            int64_t max_commit_version = 0;
            int64_t sstable_schema_version = INT64_MAX;

            if (OB_FAIL(storage->get_partition_store().get_table_schema_version(
                    index_stat.index_id_, sstable_schema_version))) {
              LOG_WARN("failed to get_table_schema_version", K(ret), K(index_stat));
            } else if (sstable_schema_version <= local_version) {
              if (OB_FAIL(storage->get_partition_store().get_drop_schema_info(
                      index_stat.index_id_, schema_version, refreshed_schema_ts))) {
                LOG_WARN("fail to get drop schema info", K(ret));
              } else if (0 == schema_version) {
                schema_version = refreshed_schema_version;
                if (OB_FAIL(
                        storage->get_partition_store().set_drop_schema_info(index_stat.index_id_, schema_version))) {
                  LOG_WARN("fail to set drop schema version", K(ret));
                } else if (OB_FAIL(storage->get_partition_store().get_drop_schema_info(
                               index_stat.index_id_, schema_version, refreshed_schema_ts))) {
                  LOG_WARN("fail to get drop schema info", K(ret));
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_ISNULL(trans_service = ObPartitionService::get_instance().get_trans_service())) {
                ret = OB_ERR_UNEXPECTED;
                TRANS_LOG(WARN, "can't get trans service", K(ret));
              } else if (OB_FAIL(trans_service->check_schema_version_elapsed(
                             pkey_, schema_version, refreshed_schema_ts, max_commit_version))) {
                if (OB_EAGAIN == ret) {
                  LOG_INFO("have active trans", K(pkey_), K(schema_version), K(refreshed_schema_ts));
                } else if (OB_NOT_MASTER == ret) {
                  ret = OB_SUCCESS;
                  need_drop = true;
                } else {
                  LOG_WARN("fail to check schema version elapsed", K(ret));
                }
              } else {
                need_drop = true;
                LOG_INFO("do not have active trans", K(pkey_), K(schema_version), K(refreshed_schema_ts));
              }
            } else {
              LOG_INFO("local schema version is smaller than schema version stored on index "
                       "table store, can not drop index",
                  K(sstable_schema_version),
                  K(local_version));
            }

            if (OB_SUCC(ret) && !need_drop) {
              if (OB_INVALID_VERSION == latest_schema_version) {
              } else if (sstable_schema_version > latest_schema_version) {
                need_drop = true;
                LOG_INFO("schema has been flashbacked, need drop unused index schema",
                    K(latest_schema_version),
                    K(sstable_schema_version),
                    K(index_stat));
              } else {
                // nothing todo
              }
            }
          } else if (index_schema->is_dropped_schema() && !index_stat.has_dropped_flag_) {
            FLOG_INFO("index schema has been marked dropped schema, need to set dropped flag", K(index_stat), K(pkey));
            const int64_t bucket_idx = get_bucket_idx_(pkey);
            ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
            if (OB_FAIL(storage->get_partition_store().set_dropped_flag(index_stat.index_id_))) {
              LOG_WARN("failed to set_dropped_flag", K(ret), K(index_stat), K(pkey));
            }
          }
          if (OB_SUCC(ret) && need_drop) {
            FLOG_INFO("index schema has been error or deleted, need to drop index", K(index_stat), K(pkey));
            const int64_t bucket_idx = get_bucket_idx_(pkey);
            ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
            if (OB_FAIL(storage->get_partition_store().drop_index(index_stat.index_id_, write_slog))) {
              LOG_WARN("failed to drop index", K(ret), K(index_stat), K(pkey));
            }
          }
        }
      }
    }
  }
  return ret;
}

int64_t ObPGStorage::get_publish_version() const
{
  int64_t publish_version = 0;
  if (!is_inited_) {
    LOG_WARN("pg is not inited", K_(pkey));
  } else {
    TCRLockGuard guard(lock_);
    publish_version = meta_->storage_info_.get_data_info().get_publish_version();
  }
  return publish_version;
}

int ObPGStorage::update_readable_info(const ObPartitionReadableInfo& readable_info)
{
  return pg_memtable_mgr_.update_readable_info(readable_info);
}

int ObPGStorage::update_split_state_after_merge(int64_t& split_state)
{
  int ret = OB_SUCCESS;
  if (is_dest_split(static_cast<ObPartitionSplitStateEnum>(split_state))) {
    if (OB_FAIL(update_dest_split_state_after_merge_(split_state))) {
      LOG_WARN("failed to update dest split state after merge", K_(pkey));
    }
  }
  return ret;
}

int ObPGStorage::get_pkey_for_table(const int64_t table_id, ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    LOG_WARN("failed to get all pg partition keys");
  } else {
    for (int i = 0; i < pkeys.count(); i++) {
      if (pkeys.at(i).get_table_id() == table_id) {
        pkey = pkeys.at(i);
        break;
      }
    }
  }
  return ret;
}

int ObPGStorage::check_table_store_empty(bool& is_empty)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObPGPartition* pg_partition = nullptr;
  ObPartitionStorage* storage = nullptr;
  ObMultiVersionTableStore* data_table_store = nullptr;
  ObPartitionArray partitions;
  if (OB_FAIL(get_all_pg_partition_keys_(partitions))) {
    STORAGE_LOG(WARN, "failed to get all pg partition keys", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); i++) {
      ObPGPartitionGuard pg_partition_guard(partitions.at(i), *(pg_->get_pg_partition_map()));
      if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get pg partition", K(ret), K(pkey_));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage is null", K(ret), K(pkey_));
      } else if (OB_ISNULL(data_table_store = storage->get_partition_store().get_data_table_store())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("data table store must not null", K(ret));
      } else if (OB_FAIL(data_table_store->get_effective_tables(false /*include_active_memtable*/, handle))) {
        LOG_WARN("failed to get_latest_effective_tables", K(ret));
      } else if (handle.get_count() > 0) {
        is_empty = false;
        break;
      }
    }
  }
  return ret;
}

int ObPGStorage::update_dest_split_state_after_merge_(int64_t& split_state)
{
  int ret = OB_SUCCESS;
  bool is_physical_split_finish = true;
  bool is_temp_physical_split_finish = true;
  ObPartitionArray pkeys;
  ObPGPartition* partition = nullptr;
  ObPartitionStorage* storage = nullptr;
  if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    LOG_WARN("failed to get all pg partition keys", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
      if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("pg partition not exist", K(ret), K(pkey));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
        ret = OB_ERR_SYS;
        LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
      } else if (OB_FAIL(storage->get_partition_store().is_physical_split_finished(is_temp_physical_split_finish))) {
        LOG_WARN("failed to get_all_table_ids", K(ret), K(pkey));
      } else if (!is_temp_physical_split_finish) {
        is_physical_split_finish = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (is_physical_split_finish) {
        split_state = ObPartitionSplitStateEnum::FOLLOWER_INIT;
      }
    }
  }
  return ret;
}

int ObPGStorage::remove_all_pg_index()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (need_clear_pg_index_) {
    RemovePGIndexFunctor functor(*(pg_->get_pg_index()));
    if (OB_FAIL(partition_list_.for_each(functor))) {
      LOG_ERROR("failed to remove pg index", K(ret), K(is_inited_), K(pkey_));
    } else {
      need_clear_pg_index_ = false;
    }
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "remove all pg index success", K(pkey_), K(*this));
  }

  return ret;
}

int ObPGStorage::check_can_create_pg_partition(bool& can_create)
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K_(pkey));
  } else if (is_empty_pg()) {
    // if (meta_->ddl_seq_num_ > 0) {
    //  ret = OB_OP_NOT_ALLOW;
    //  LOG_WARN("create partition on empty pg with non-zero ddl_seq_num_ is not allowed", K(ret), K_(pkey));
    //}
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    // overwrite retcode
    ret = OB_SUCCESS;
    can_create = false;
  } else {
    can_create = true;
  }
  return ret;
}

int ObPGStorage::update_split_table_store(
    const common::ObPartitionKey& pkey, int64_t table_id, bool is_major_split, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_idx = get_bucket_idx_(pkey);
  ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K_(pkey));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("pg partition not exist", K(ret), K(pkey), K(*meta_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().update_split_table_store(table_id, is_major_split, handle))) {
      LOG_WARN("failed to update split table store", K(ret), "pg key", pkey_, K(pkey), K(table_id), K(is_major_split));
    } else {
      FLOG_INFO("succeed to split table store", K(ret), K(pkey), K(table_id), K(is_major_split), "pg key", pkey_);
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = check_update_split_state_()))) {
      LOG_WARN("failed to check update split state", K(tmp_ret), K(pkey), "pg key", pkey_);
    }
  }

  return ret;
}

int ObPGStorage::get_min_sstable_version_(int64_t& min_sstable_snapshot_version)
{
  int ret = OB_SUCCESS;
  min_sstable_snapshot_version = INT64_MAX;
  int64_t temp_min_sstable_version;
  ObPartitionArray pkeys;
  if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    LOG_WARN("failed to get all pg partition keys", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
    const ObPartitionKey& pkey = pkeys.at(i);
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get pg partition", K(ret), K(pkey));
    } else if (OB_ISNULL(
                   storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage can not be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().get_min_sstable_version(temp_min_sstable_version))) {
      LOG_WARN("failed to get_min_sstable_version", K(ret), K(pkey));
    } else if (temp_min_sstable_version < min_sstable_snapshot_version) {
      min_sstable_snapshot_version = temp_min_sstable_version;
    }
  }
  return ret;
}

int ObPGStorage::get_min_frozen_memtable_base_version(int64_t& min_base_version)
{
  int ret = OB_SUCCESS;
  min_base_version = INT64_MAX;
  ObTableHandle handle;
  ObMemtable* memtable = nullptr;
  if (OB_FAIL(pg_memtable_mgr_.get_first_frozen_memtable(handle))) {
    LOG_WARN("failed to get first frozen memtable", K(ret), K_(pkey));
  } else if (OB_NOT_NULL(memtable = static_cast<ObMemtable*>(handle.get_table()))) {
    min_base_version = memtable->get_base_version();
  }
  return ret;
}

int ObPGStorage::get_first_frozen_memtable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage is not initialized", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_first_frozen_memtable(handle))) {
    STORAGE_LOG(WARN, "get first frozen memtable error", K(ret), K(handle));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::get_table_store_cnt(int64_t& table_store_cnt) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg is not inited", K(ret), K_(pkey));
  }
  TCRLockGuard lock_guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_->partitions_.count(); ++i) {
    const ObPartitionKey& pkey = meta_->partitions_.at(i);
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get pg partition", K(ret), K(pkey));
    } else if (OB_ISNULL(
                   storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage can not be null", K(ret), K(pkey));
    } else {
      table_store_cnt += storage->get_partition_store().get_table_store_cnt();
    }
  }
  return ret;
}

int ObPGStorage::get_partition_access_stat(const ObPartitionKey& pkey, ObPartitionPrefixAccessStat& stat)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get pg partition", K(ret), K(pkey));
    } else if (OB_ISNULL(
                   storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage can not be null", K(ret), K(pkey));
    } else {
      stat = storage->get_prefix_access_stat();
    }
  }
  return ret;
}

int ObPGStorage::feedback_scan_access_stat(const ObTableScanParam& param)
{
  int ret = OB_SUCCESS;
  const common::ObPartitionKey& pkey = (pkey_.is_pg() ? param.pkey_ : pkey_);
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_partition->feedback_scan_access_stat(param))) {
    STORAGE_LOG(INFO, "feedback scan access stat failed in ObPGPartition,", K(ret));
  }
  return ret;
}

int ObPGStorage::get_all_pg_partition_keys_with_lock(ObPartitionArray& pkeys, const bool include_trans_table)
{
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  return get_all_pg_partition_keys_(pkeys, include_trans_table);
}

int ObPGStorage::create_sstable_impl(const common::ObIArray<ObMacroBlocksWriteCtx*>& data_blocks,
    const common::ObIArray<ObMacroBlocksWriteCtx*>& lob_blocks, ObMacroBlocksWriteCtx* bloomfilter_block,
    const ObSSTableMergeInfo* sstable_merge_info, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObSSTable* sstable = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionGroup has not been inited", K(ret));
  } else if (OB_FAIL(handle.get_sstable(sstable))) {
    LOG_WARN("fail to get sstable", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, sstable must not be null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_blocks.count(); ++i) {
      if (OB_ISNULL(data_blocks.at(i))) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, data block must not be null", K(ret));
      } else if (OB_FAIL(sstable->append_macro_blocks(*data_blocks.at(i)))) {
        STORAGE_LOG(WARN, "fail to append macro blocks", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < lob_blocks.count(); ++i) {
      if (OB_ISNULL(lob_blocks.at(i))) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, lob block must not be null", K(ret));
      } else if (OB_FAIL(sstable->append_lob_macro_blocks(*lob_blocks.at(i)))) {
        STORAGE_LOG(WARN, "fail to append lob macro blocks", K(ret));
      }
    }

    if (OB_SUCC(ret) && nullptr != bloomfilter_block) {
      if (OB_FAIL(sstable->append_bf_macro_blocks(*bloomfilter_block))) {
        STORAGE_LOG(WARN, "fail to append bloomfilter macro block", K(ret));
      }
    }

    if (OB_SUCC(ret) && nullptr != sstable_merge_info) {
      if (OB_FAIL(sstable->add_sstable_merge_info(*sstable_merge_info))) {
        LOG_WARN("fail to add sstable merge info", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstable->close())) {
        LOG_WARN("fail to close sstable", K(ret));
      }
    }
  }

  return ret;
}

int ObPGStorage::create_sstables(const common::ObIArray<ObPGCreateSSTableParam>& create_sstable_params,
    const bool in_slog_trans, ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_UNLIKELY(create_sstable_params.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(create_sstable_params.count()));
  } else {
    ObTablesHandle tables_handle_for_slog;
    ObTableHandle table_handle;
    bool need_create_sstable = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < create_sstable_params.count(); ++i) {
      const ObPGCreateSSTableParam& param = create_sstable_params.at(i);
      if (!param.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), K(param));
      } else {
        ObCreateSSTableParam* sstable_param = nullptr;
        if (nullptr != param.with_partition_param_) {
          sstable_param = param.with_partition_param_;
        } else {
          sstable_param = param.with_table_param_;
        }
        ObITable::TableKey table_key = nullptr != sstable_param ? sstable_param->table_key_ : *param.table_key_;
        if (OB_FAIL(sstable_mgr_.acquire_sstable(table_key, table_handle))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            need_create_sstable = true;
          } else {
            LOG_WARN("fail to acquire sstable", K(ret));
          }
        } else {
          need_create_sstable = false;
          if (OB_FAIL(tables_handle.add_table(table_handle))) {
            LOG_WARN("fail to add table", K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && need_create_sstable) {
        if (nullptr != param.with_partition_param_) {
          if (OB_FAIL(sstable_mgr_.create_sstable(*param.with_partition_param_, table_handle))) {
            LOG_WARN("fail to create sstable", K(ret));
          }
        } else if (nullptr != param.with_table_param_) {
          if (OB_FAIL(sstable_mgr_.create_sstable(*param.with_table_param_, table_handle))) {
            LOG_WARN("fail to create sstable", K(ret));
          }
        } else {
          if (OB_FAIL(sstable_mgr_.create_sstable(*param.table_key_, *param.meta_, table_handle))) {
            LOG_WARN("fail to create sstable", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(create_sstable_impl(param.data_blocks_,
                  param.lob_blocks_,
                  param.bloomfilter_block_,
                  param.sstable_merge_info_,
                  table_handle))) {
            LOG_WARN("fail to create sstable impl", K(ret));
          } else if (OB_FAIL(tables_handle_for_slog.add_table(table_handle))) {
            LOG_WARN("fail to add table", K(ret));
          } else if (OB_FAIL(tables_handle.add_table(table_handle))) {
            LOG_WARN("fail to add table", K(ret));
          } else if (nullptr != param.meta_) {
            ObSSTable* tmp_sstable = nullptr;
            if (OB_FAIL(table_handle.get_sstable(tmp_sstable))) {
              LOG_WARN("fail to get sstable", K(ret));
            } else if (OB_FAIL(param.meta_->check_data(tmp_sstable->get_meta()))) {
              LOG_WARN("fail to check sstable meta data",
                  K(ret),
                  K(*param.meta_),
                  K(*tmp_sstable),
                  K(tmp_sstable->get_meta()));
            }
          }

          if (OB_FAIL(ret)) {
            int tmp_ret = OB_SUCCESS;
            ObSSTable* sstable = nullptr;
            if (OB_SUCCESS != (tmp_ret = table_handle.get_sstable(sstable))) {
              LOG_WARN("fail to get sstable", K(tmp_ret));
            } else if (nullptr != sstable) {
              table_handle.reset();
              if (0 == sstable->get_ref()) {
                sstable_mgr_.free_sstable(sstable);
              }
            }
          }
        }
      }
      table_handle.reset();
    }

    if (OB_SUCC(ret) && tables_handle_for_slog.get_count() > 0) {
      if (OB_FAIL(sstable_mgr_.add_sstables(in_slog_trans, tables_handle_for_slog))) {
        LOG_WARN("fail to complete sstables", K(ret));
      } else {
        LOG_DEBUG("success to add sstables", K(tables_handle));
      }
    }
    if (OB_FAIL(ret)) {
      const common::ObIArray<storage::ObITable*>& tables = tables_handle_for_slog.get_tables();
      tables_handle.reset();
      for (int64_t i = 0; i < tables.count(); ++i) {
        ObSSTable* sstable = static_cast<ObSSTable*>(tables.at(i));
        sstable_mgr_.free_sstable(sstable);
        sstable = nullptr;
      }
      tables_handle_for_slog.get_tables().reset();
    }
  }
  return ret;
}

int ObPGStorage::create_sstable(
    const ObPGCreateSSTableParam& param, const bool in_slog_trans, ObTableHandle& table_handle)
{
  int ret = OB_SUCCESS;
  table_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else {
    ObTablesHandle tables_handle;
    ObArray<ObPGCreateSSTableParam> create_sstable_params;
    if (OB_FAIL(create_sstable_params.push_back(param))) {
      LOG_WARN("fail to push back create sstable param", K(ret));
    } else if (OB_FAIL(create_sstables(create_sstable_params, in_slog_trans, tables_handle))) {
      LOG_WARN("fail to create sstables", K(ret));
    } else if (1 != tables_handle.get_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table count", K(ret), K(tables_handle));
    } else if (OB_FAIL(table_handle.set_table(tables_handle.get_table(0)))) {
      LOG_WARN("fail to add table", K(ret));
    }
  }
  return ret;
}

int ObPGStorage::serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size)
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  TCRLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_FAIL(serialize_impl(allocator, new_buf, serialize_size, nullptr /*unused table handle*/))) {
    LOG_WARN("fail to serialize impl", K(ret));
  }
  return ret;
}

int ObPGStorage::get_meta_block_list(ObIArray<MacroBlockId>& macro_block_list) const
{
  int ret = OB_SUCCESS;
  macro_block_list.reuse();
  TCRLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_FAIL(macro_block_list.assign(meta_block_handle_.get_meta_block_list()))) {
    LOG_WARN("fail to assign macro block list", K(ret));
  }
  return ret;
}

int ObPGStorage::get_checkpoint_info(ObArenaAllocator& allocator, ObPGCheckpointInfo& pg_checkpoint_info)
{
  int ret = OB_SUCCESS;
  pg_checkpoint_info.reset();
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  TCRLockGuard lock_guard(lock_);
  char* tmp_buf = nullptr;
  int64_t tmp_buf_len = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_FAIL(serialize_impl(allocator, tmp_buf, tmp_buf_len, &pg_checkpoint_info.tables_handle_))) {
    LOG_WARN("fail to serialize pg meta", K(ret));
  } else {
    pg_checkpoint_info.pg_meta_buf_ = tmp_buf;
    pg_checkpoint_info.pg_meta_buf_len_ = tmp_buf_len;
  }
  return ret;
}

int64_t ObPGStorage::get_partition_cnt()
{
  int64_t part_cnt = 0;
  const bool include_trans_table = false;
  GetPGPartitionCountFunctor functor(include_trans_table, part_cnt);
  (void)partition_list_.for_each(functor);

  return part_cnt;
}

int ObPGStorage::get_active_memtable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (OB_FAIL(pg_memtable_mgr_.get_active_memtable(handle))) {
    LOG_WARN("get active memtable fail", K(ret), K_(pkey));
  }

  return ret;
}

int ObPGStorage::check_need_merge_trans_table(
    bool& need_merge, int64_t& merge_log_ts, int64_t& trans_table_seq, int64_t& end_log_ts, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObPartitionKey trans_table_pkey;
  end_log_ts = 0;
  timestamp = 0;
  need_merge = false;
  merge_log_ts = 0;
  trans_table_seq = -1;
  bool is_paused = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else {
    {
      ObBucketWLockAllGuard bucket_guard(bucket_lock_);
      trans_table_seq = trans_table_seq_;
      is_paused = is_paused_;
    }
    if (is_paused) {
      // pg is paused, can not do trans table merge now
    } else if (OB_FAIL(get_trans_table_end_log_ts_and_timestamp_(end_log_ts, timestamp))) {
      LOG_WARN("failed to get_trans_table_end_log_ts", K(ret), K_(pkey));
    } else if (OB_FAIL(pg_memtable_mgr_.get_memtables(handle, false, -1, false /*include_active_memtable*/))) {
      LOG_WARN("failed to get memtable", K(ret), K_(pkey));
    } else if (handle.empty()) {
      // skip if no frozen memtables
    } else {
      ObMemtable* table = nullptr;
      for (int64_t i = handle.get_count() - 1; OB_SUCC(ret) && !need_merge && i >= 0; i--) {
        if (OB_ISNULL(table = static_cast<ObMemtable*>(handle.get_table(i)))) {
          ret = OB_ERR_SYS;
          LOG_WARN("table should not be null", K(ret), K(i), K(handle));
        } else if (table->get_end_log_ts() > end_log_ts || table->get_timestamp() > timestamp) {
          if (table->can_be_minor_merged()) {
            need_merge = true;
            merge_log_ts = table->get_end_log_ts();
          }
        }
      }
    }
  }
  if (!need_merge && REACH_TIME_INTERVAL(30 * 1000 * 1000L)) {
    FLOG_INFO("check_need_merge_trans_table",
        K(need_merge),
        K_(pkey),
        K(handle),
        K(end_log_ts),
        K(timestamp),
        K(trans_table_seq));
  }
  return ret;
}

int ObPGStorage::replay_add_sstable(ObSSTable& replay_sstable)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = replay_sstable.get_partition_key();
  const int64_t bucket_idx = get_bucket_idx_(pkey);
  ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_UNLIKELY(!replay_sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(replay_sstable));
  } else if (OB_FAIL(sstable_mgr_.replay_add_sstable(replay_sstable))) {
    LOG_WARN("fail to replay add sstable log", K(ret), K(replay_sstable));
  }
  return ret;
}

int ObPGStorage::replay_remove_sstable(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = table_key.get_partition_key();
  const int64_t bucket_idx = get_bucket_idx_(pkey);
  ObBucketWLockGuard bucket_guard(bucket_lock_, bucket_idx);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key));
  } else if (OB_FAIL(sstable_mgr_.replay_remove_sstable(table_key))) {
    LOG_WARN("fail to replay remove sstable", K(ret), K(table_key));
  }
  return ret;
}

int ObPGStorage::acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(sstable_mgr_.acquire_sstable(table_key, table_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to acquire sstable", K(ret));
    }
  }
  return ret;
}

int ObPGStorage::append_sstable(
    const ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter, ObTableHandle& sstable_handle)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = 0;
  ObFreezeInfoProxy freeze_info_proxy;
  const int64_t progressive_merge_start_version = 0;
  const int64_t progressive_merge_end_version = 0;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* index_schema = NULL;
  const uint64_t index_table_id = param.index_id_;
  const int64_t schema_version = param.schema_version_;
  ObITable::TableKey table_key;
  ObPGCreateSSTableParam pg_create_sstable_param;
  ObCreateSSTableParamWithTable sstable_param;
  int64_t checksum_method = 0;
  ObSimpleFrozenStatus frozen_status;
  const uint64_t tenant_id = is_inner_table(index_table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(index_table_id);
  const bool check_formal = extract_pure_id(index_table_id) > OB_MAX_CORE_TABLE_ID;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
                 tenant_id, schema_guard, schema_version, OB_INVALID_VERSION))) {
    STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(pkey_), K(index_table_id), K(schema_version));
  } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_table_id, index_schema))) {
    STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(index_table_id));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_SCHEMA_ERROR;
    STORAGE_LOG(WARN, "schema error, index schema not exist", K(ret), K(index_table_id), K(schema_version));
  } else if (OB_FAIL(ObIndexBuildStatOperator::get_snapshot_version(
                 index_schema->get_data_table_id(), index_table_id, *GCTX.sql_proxy_, snapshot_version))) {
    STORAGE_LOG(WARN, "fail to get snapshot version", K(ret), K(index_table_id));
  } else if (snapshot_version <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, snapshot version is invalid", K(ret), K(snapshot_version));
  } else if (OB_FAIL(freeze_info_proxy.get_frozen_info_less_than(*GCTX.sql_proxy_, snapshot_version, frozen_status))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get next major freeze", K(ret), K(pkey_));
    } else {
      frozen_status.frozen_version_ = 1L;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(ObIndexChecksumOperator::get_checksum_method(
                 param.execution_id_, index_schema->get_data_table_id(), checksum_method, *GCTX.sql_proxy_))) {
    STORAGE_LOG(WARN, "fail to get checksum method", K(ret));
  }

  if (OB_SUCC(ret)) {
    table_key.table_type_ = ObITable::MAJOR_SSTABLE;
    table_key.pkey_ = pkey_;
    table_key.table_id_ = index_table_id;
    table_key.trans_version_range_.multi_version_start_ = snapshot_version;
    table_key.trans_version_range_.base_version_ = ObVersionRange::MIN_VERSION;
    table_key.trans_version_range_.snapshot_version_ = snapshot_version;
    table_key.version_ = ObVersion(frozen_status.frozen_version_);

    sstable_param.table_key_ = table_key;
    sstable_param.schema_ = index_schema;
    sstable_param.schema_version_ = schema_version;
    sstable_param.progressive_merge_start_version_ = progressive_merge_start_version;
    sstable_param.progressive_merge_end_version_ = progressive_merge_end_version;
    sstable_param.create_snapshot_version_ = snapshot_version;
    sstable_param.create_index_base_version_ = snapshot_version;
    sstable_param.checksum_method_ = checksum_method;
    sstable_param.logical_data_version_ = table_key.version_;

    pg_create_sstable_param.with_table_param_ = &sstable_param;
  }

  if (OB_FAIL(ret)) {
    // pass
  } else {
    ObNewRow* row_val = NULL;
    ObIntermMacroKey key;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        STORAGE_LOG(WARN, "failed to check status", K(ret));
      } else if (OB_FAIL(iter.get_next_row(row_val))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "fail to get next row", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        const char* buf = row_val->cells_[0].get_string_ptr();
        const int64_t data_len = row_val->cells_[0].get_string_len();
        int64_t pos = 0;
        ObIntermMacroHandle handle;
        if (OB_FAIL(key.deserialize(buf, data_len, pos))) {
          STORAGE_LOG(WARN, "fail to deserialize interm macro key", K(ret));
        } else if (OB_FAIL(INTERM_MACRO_MGR.get(key, handle))) {
          STORAGE_LOG(WARN, "fail to get macro list", K(ret));
        } else if (OB_FAIL(pg_create_sstable_param.data_blocks_.push_back(
                       &handle.get_resource_ptr()->get_macro_blocks()))) {
          STORAGE_LOG(WARN, "fail to push back data block ctx", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_sstable(pg_create_sstable_param, false /*in slog trans*/, sstable_handle))) {
        STORAGE_LOG(WARN, "fail to create sstable", K(ret));
      }
    }
  }
  STORAGE_LOG(INFO, "global index append sstable", K(ret), "index_id", param.index_id_, K(pkey_), K(sstable_handle));
  return ret;
}

int ObPGStorage::check_update_split_state_()
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage is not inited", K(ret));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else {
    int64_t split_state = meta_->saved_split_state_;
    ObPartitionGroupMeta& meta = *next_meta_ptr;
    meta.reset();
    if (OB_FAIL(update_split_state_after_merge(split_state))) {
      LOG_WARN("failed to update split state", K(ret), K_(pkey));
    } else if (meta_->saved_split_state_ == split_state) {
      // do nothing
    } else if (OB_FAIL(meta.deep_copy(*meta_))) {
      STORAGE_LOG(WARN, "failed to deep_copy meta", K(ret), K_(pkey));
    } else {
      meta.saved_split_state_ = split_state;
      if (OB_FAIL(write_update_pg_meta_trans(meta, OB_LOG_SET_PARTITION_SPLIT_INFO))) {
        STORAGE_LOG(WARN, "failed to write_update_pg_meta_trans", K(ret), K_(pkey));
      } else {
        switch_meta_(next_meta_ptr);
      }
    }
  }
  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::check_complete(bool& is_complete)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  ObPGPartition* pg_partition = NULL;
  is_complete = true;

  bool is_in_dest_split = false;
  if (is_inited_) {
    TCRLockGuard lock_guard(lock_);
    is_in_dest_split = is_dest_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_));
  }

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage is not inited", K(ret));
  } else if (!is_in_dest_split) {
    // do nothing
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    STORAGE_LOG(WARN, "get all pg partition keys failed", K(ret), K_(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); i++) {
      ObPGPartitionGuard guard(pkeys.at(i), *(pg_->get_pg_partition_map()));
      if (NULL == (pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(pkeys.at(i)));
      } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                             ->get_partition_store()
                             .check_store_complete(is_complete))) {
        STORAGE_LOG(WARN, "failed to get merged version", K(ret), K(pkeys.at(i)));
      } else if (!is_complete) {
        break;
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObPGStorage::recycle_unused_sstables(const int64_t max_recycle_cnt, int64_t& recycled_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sstable_mgr_.recycle_unused_sstables(max_recycle_cnt, recycled_cnt))) {
    STORAGE_LOG(WARN, "fail to recycle unused sstables", K(ret));
  }
  return ret;
}

int ObPGStorage::alloc_file_for_old_replay()
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = pkey_.get_tenant_id();
  const bool sys_table = is_sys_table(pkey_.get_table_id());
  if (OB_ISNULL(meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("meta is null", K(ret), KP(meta_));
  } else if (OB_UNLIKELY(OB_INVALID_DATA_FILE_ID != meta_->storage_info_.get_pg_file_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("already have pg file id in meta", K(ret), K(meta_->storage_info_));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.alloc_file(tenant_id, sys_table, file_handle_, false /*write slog*/))) {
    LOG_WARN("fail to open pg file", K(ret));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.add_pg(ObTenantFileKey(file_handle_.get_storage_file()->get_tenant_id(),
                                                   file_handle_.get_storage_file()->get_file_id()),
                 pkey_))) {
    LOG_WARN("fail to add pg to tenant file", K(ret));
  } else if (OB_FAIL(sstable_mgr_.set_storage_file_handle(file_handle_))) {
    LOG_WARN("fail to set storage file handle", K(ret), K(file_handle_));
  } else if (OB_FAIL(recovery_point_data_mgr_.set_storage_file_handle(file_handle_))) {
    LOG_WARN("failed to set storage file handle for recovery point data", K(ret), K_(file_handle));
  } else {
    file_mgr_ = &OB_SERVER_FILE_MGR;
    meta_->storage_info_.set_pg_file_id(file_handle_.get_storage_file()->get_file_id());
  }
  return ret;
}

int ObPGStorage::transform_and_add_old_sstable()
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), K(pkey_), K(pkeys));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      ObPGPartition* pg_partition = nullptr;
      ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
      ObPartitionStorage* storage = nullptr;
      if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition storage is null", K(ret), K(pkey));
      } else if (OB_FAIL(transform_and_add_old_sstable_for_partition(storage->get_partition_store()))) {
        LOG_WARN("fail to set replay sstables", K(ret));
      }
    }
  }
  return ret;
}

int ObPGStorage::check_can_free(bool& can_free)
{
  int ret = OB_SUCCESS;
  can_free = false;
  if (OB_FAIL(sstable_mgr_.check_all_sstable_unused(can_free))) {
    LOG_WARN("fail to check can free", K(ret));
  }
  return ret;
}

int ObPGStorage::transform_and_add_old_sstable_for_partition(ObPartitionStore& store)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> index_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (OB_FAIL(store.get_all_table_ids(index_ids))) {
    LOG_WARN("fail to get all table ids", K(ret));
  } else {
    ObArray<ObITable::TableKey> replay_tables;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
      const uint64_t table_id = index_ids.at(i);
      replay_tables.reuse();
      if (OB_FAIL(store.get_replay_tables(table_id, replay_tables))) {
        LOG_WARN("fail to get replay tables", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < replay_tables.count(); ++i) {
          ObOldSSTable* sstable = nullptr;
          ObTableHandle table_handle;
          ObITable::TableKey& table_key = replay_tables.at(i);
          if (OB_UNLIKELY(table_key.is_new_table_from_3x())) {
            ret = OB_ERR_SYS;
            LOG_ERROR("Unexpected new table type from 22x upgrade cluster", K(ret), K(table_key), K(replay_tables));
          } else if (OB_FAIL(ObTableMgr::get_instance().acquire_old_table(table_key, table_handle))) {
            LOG_WARN("fail to acquire table", K(ret), K(table_key));
          } else if (OB_FAIL(table_handle.get_old_sstable(sstable))) {
            LOG_WARN("fail to get sstable", K(ret));
          } else if (OB_FAIL(sstable_mgr_.replay_add_old_sstable(*sstable))) {
            LOG_WARN("fail to replay add sstable log", K(ret), K(*sstable));
          }
        }
      }
    }
  }
  return ret;
}

bool ObPGStorage::need_create_memtable()
{
  bool bool_ret = false;

  if (IS_NOT_INIT) {
    LOG_WARN("not inited", K_(pkey));
  } else {
    TCRLockGuard lock_guard(lock_);
    const int64_t publish_version = meta_->storage_info_.get_data_info().get_publish_version();
    const int64_t split_version = meta_->split_info_.get_split_version();
    const ObPartitionKey& src_pkey = meta_->split_info_.get_src_partition();
    bool_ret = !(publish_version == split_version && src_pkey == pkey_);
  }
  return bool_ret;
}

int ObPGStorage::update_upper_trans_version_and_gc_sstable(ObTransService& trans_service, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  ObTablesHandle updated_tables;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg_storage is not inited", K(ret));
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    LOG_WARN("failed to get_all_pg_partitions", K(ret));
  } else {
    int64_t cur_last_replay_log_ts = 0;
    {
      TCRLockGuard lock_guard(lock_);
      cur_last_replay_log_ts = meta_->storage_info_.get_data_info().get_last_replay_log_ts();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      if (OB_FAIL(update_upper_trans_version_(pkey, trans_service, cur_last_replay_log_ts, updated_tables))) {
        LOG_WARN("failed to update_upper_trans_version_", K(ret), K(pkey));
      } else {
        ObBucketWLockGuard bucket_guard(bucket_lock_, get_bucket_idx_(pkey));
        if (OB_FAIL(remove_old_table_(pkey, frozen_version))) {
          LOG_WARN("failed to remove old table", K(ret), K(pkey));
        }
      }
    }
  }
  return ret;
}

int ObPGStorage::check_flashback_partition_need_remove(
    const int64_t flashback_scn, ObPGPartition* pg_partition, bool& need_remove)
{
  int ret = OB_SUCCESS;
  ObPGPartitionStoreMeta meta;
  need_remove = false;

  if (OB_ISNULL(pg_partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg partition should not be NULL", K(ret), KP(pg_partition));
  } else if (OB_FAIL(
                 static_cast<ObPartitionStorage*>(pg_partition->get_storage())->get_partition_store().get_meta(meta))) {
    STORAGE_LOG(WARN, "failed to do physical flashback", K(ret), K(pg_partition->get_partition_key()));
  } else if (meta.create_timestamp_ > flashback_scn) {
    need_remove = true;
    FLOG_INFO("partition create after flashback scn, need remove", K(meta), K(flashback_scn));
  }
  return ret;
}

int ObPGStorage::update_upper_trans_version_(const ObPartitionKey& pkey, ObTransService& trans_service,
    const int64_t last_replay_log_ts, ObTablesHandle& updated_tables)
{
  int ret = OB_SUCCESS;
  ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
  ObPGPartition* pg_partition = nullptr;
  ObPartitionStorage* storage = nullptr;
  ObTablesHandle handle;
  if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
    LOG_INFO("pg partition may be deleted", K(ret), K(pkey));
  } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_storage should not be null", K(ret), K(pkey));
  } else if (OB_FAIL(storage->get_partition_store().get_all_latest_minor_sstables(handle))) {
    LOG_WARN("failed to get_all_latest_minor_sstables", K(ret), K(pkey));
  } else {
    bool is_paused = false;
    int64_t old_trans_table_seq = 0;
    int64_t new_trans_table_seq = 0;
    {
      ObBucketWLockAllGuard bucket_guard(bucket_lock_);
      old_trans_table_seq = trans_table_seq_;
      is_paused = is_paused_;
    }

    if (is_paused) {
      LOG_INFO("pg is paused, cannot update upper trans version", K_(pkey));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < handle.get_count(); ++j) {
        ObSSTable* sstable = static_cast<ObSSTable*>(handle.get_table(j));
        if (OB_ISNULL(sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table should not be null", K(ret), K(pkey));
        } else if (sstable->get_end_log_ts() > last_replay_log_ts) {
          // skip sstable whose end log id is greater than last_replay_log_id
          // it might be minor sstables left by a failed rebuild
        } else if (INT64_MAX == sstable->get_upper_trans_version()) {
          int64_t max_trans_version = INT64_MAX;
          bool is_all_rollback_trans = false;
          // get_all_latest_minor_sstables return all minor except complement
          // all normal minor sstable consist of data between [start_log_ts, end_log_ts]
          if (OB_FAIL(trans_service.get_max_trans_version_before_given_log_ts(
                  pkey_, sstable->get_end_log_ts(), max_trans_version, is_all_rollback_trans))) {
            LOG_WARN("failed to get_max_trans_version_before_given_log_id", K(ret), K(pkey_), KPC(sstable));
          } else if (0 == max_trans_version && !is_all_rollback_trans) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("max trans version should not be 0", K_(pkey), "end_log_ts", sstable->get_end_log_ts());
          } else if (INT64_MAX != max_trans_version) {
            {
              ObBucketWLockAllGuard bucket_guard(bucket_lock_);
              new_trans_table_seq = trans_table_seq_;
            }
            if (old_trans_table_seq == new_trans_table_seq) {
              if (OB_UNLIKELY(0 == max_trans_version)) {
                FLOG_INFO("Get empty max_trans_version , maybe all the trans have been rollbacked",
                    K(max_trans_version),
                    K(is_all_rollback_trans),
                    "end_log_ts",
                    sstable->get_end_log_ts());
              }
              if (OB_FAIL(sstable->set_upper_trans_version(max_trans_version))) {
                LOG_WARN("failed to set_upper_trans_version", K(ret), K(*sstable));
              } else if (OB_FAIL(updated_tables.add_table(sstable))) {
                LOG_WARN("failed to add table", K(ret), K(sstable->get_key()));
              } else {
                FLOG_INFO("update sstable upper_trans_version",
                    "sstable_key",
                    sstable->get_key(),
                    K(max_trans_version),
                    K(sstable->get_upper_trans_version()));
              }
            } else {
              FLOG_INFO("trans_table_seq changed, cannot update upper_trans_version", K_(pkey));
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPGStorage::set_meta_block_list(const ObIArray<MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGStorage has not been inited", K(ret));
  } else if (FALSE_IT(meta_block_handle_.set_storage_file(file_handle_.get_storage_file()))) {
  } else if (OB_FAIL(meta_block_handle_.add_macro_blocks(meta_block_list, true /*switch handle*/))) {
    LOG_WARN("fail to add macro blocks", K(ret));
  }
  return ret;
}

int ObPGStorage::restore_mem_trans_table()
{
  int ret = OB_SUCCESS;
  ObPartitionKey trans_table_pkey;
  ObTableHandle handle;
  ObSSTable* sstable = nullptr;
  int16_t restore_state = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (FALSE_IT(restore_state = get_restore_state())) {
  } else if (!has_memstore() && REPLICA_NOT_RESTORE == restore_state) {
    // do nothing
    FLOG_INFO("no need to restore mem trans table", K_(pkey), K(restore_state));
  } else if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(pkey_))) {
    LOG_WARN("failed to generate trans_table_pkey", K(ret), K_(pkey));
  } else {
    ObPGPartitionGuard pg_partition_guard(trans_table_pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      if (0 != get_partition_cnt()) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("pg partition not exist", K(ret), K(trans_table_pkey), K_(pkey));
      } else {
        FLOG_INFO("no need to restore mem trans table", K_(pkey));
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(trans_table_pkey));
    } else if (OB_FAIL(storage->get_partition_store().get_last_major_sstable(trans_table_pkey.table_id_, handle))) {
      LOG_WARN("failed to get last trans sstable", K(ret), K_(pkey), K(trans_table_pkey));
    } else if (OB_FAIL(handle.get_sstable(sstable))) {
      LOG_WARN("failed to get last trans sstable", K(ret), K_(pkey), K(trans_table_pkey));
    } else if (OB_FAIL(restore_mem_trans_table(*sstable))) {
      LOG_WARN("failed to restore mem trans table", K(ret), K_(pkey), K(trans_table_pkey));
    }
  }
  return ret;
}

int ObPGStorage::restore_mem_trans_table(ObSSTable& trans_sstable)
{
  int ret = OB_SUCCESS;
  ObPartitionKey trans_table_pkey;
  ObStoreRowIterator* row_iter = NULL;
  ObPartitionTransCtxMgr* trans_ctx_mgr = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(pkey_))) {
    LOG_WARN("failed to generate trans_table_pkey", K(ret), K_(pkey));
  } else if (OB_ISNULL(trans_ctx_mgr = txs_->get_part_trans_ctx_mgr().get_partition_trans_ctx_mgr(pkey_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get partition trans ctx mgr", K(ret), K_(pkey));
  } else {
    ObArenaAllocator allocator;
    ObExtStoreRange whole_range;
    whole_range.get_range().set_whole_range();

    ObStoreCtx ctx;
    ObTableIterParam iter_param;
    ObTableAccessContext access_context;
    common::ObVersionRange trans_version_range;
    common::ObQueryFlag query_flag;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;

    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = common::ObVersionRange::MAX_VERSION - 2;

    common::ObSEArray<share::schema::ObColDesc, 2> columns;
    share::schema::ObColDesc key;
    key.col_id_ = OB_APP_MIN_COLUMN_ID;
    key.col_type_.set_binary();
    key.col_order_ = ObOrderType::ASC;

    share::schema::ObColDesc value;
    value.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
    value.col_type_.set_binary();

    if (OB_FAIL(access_context.init(query_flag, ctx, allocator, trans_version_range))) {
      STORAGE_LOG(WARN, "failed to init access context", K(ret));
    } else if (OB_FAIL(columns.push_back(key))) {
      STORAGE_LOG(WARN, "failed to push back key", K(ret), K(key), K_(pkey));
    } else if (OB_FAIL(columns.push_back(value))) {
      STORAGE_LOG(WARN, "failed to push back value", K(ret), K(value), K_(pkey));
    } else if (OB_FAIL(whole_range.to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
      STORAGE_LOG(WARN, "failed to transform range to collation free and range cutoff", K(whole_range), K(ret));
    } else {
      iter_param.table_id_ = trans_table_pkey.get_table_id();
      iter_param.schema_version_ = 0;
      iter_param.rowkey_cnt_ = 1;
      iter_param.out_cols_ = &columns;

      ObPGPartitionGuard guard(trans_table_pkey, *(pg_->get_pg_partition_map()));
      ObPGPartition* partition = nullptr;
      if (OB_ISNULL(partition = guard.get_pg_partition())) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(trans_sstable.scan(iter_param, access_context, whole_range, row_iter))) {
        STORAGE_LOG(WARN, "failed to scan trans table", K(ret));
      } else if (NULL == row_iter) {
        FLOG_INFO("row_iter is NULL, no need to restore mem trans table", K_(pkey));
        // do nothing
      } else {
        const ObStoreRow* row = NULL;
        ObTransCtx* ctx = NULL;
        ObPartTransCtx* part_ctx = NULL;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(row_iter->get_next_row(row))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "failed to get next row", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
            break;
          } else {
            bool alloc = true;
            transaction::ObTransID trans_id;
            transaction::ObTransSSTableDurableCtxInfo ctx_info;
            int64_t pos = 0;
            int64_t pos1 = 0;

            if (OB_FAIL(trans_id.deserialize(
                    row->row_val_.cells_[0].get_string_ptr(), row->row_val_.cells_[0].val_len_, pos))) {
              STORAGE_LOG(WARN, "failed to deserialize trans_id", K(ret), K(trans_id));
            } else if (OB_FAIL(ctx_info.deserialize(
                           row->row_val_.cells_[1].v_.string_, row->row_val_.cells_[1].val_len_, pos1))) {
              STORAGE_LOG(WARN, "failed to deserialize status_info", K(ret), K(ctx_info));
            } else if (OB_FAIL(trans_ctx_mgr->get_trans_ctx(trans_id,
                           true,  /*for_replay*/
                           false, /*is_readonly*/
                           false, /*is_staleness_read*/
                           false, /*need_completed_dirty_txn*/
                           alloc,
                           ctx))) {
              STORAGE_LOG(WARN, "failed to get trans ctx", K(ret), K_(pkey), K(trans_id));
            } else if (OB_ISNULL(part_ctx = dynamic_cast<ObPartTransCtx*>(ctx))) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "part ctx should not be NULL", K(ret), K_(pkey));
            } else {
              if (OB_FAIL(part_ctx->init(ctx_info.tenant_id_,
                      trans_id,
                      ctx_info.trans_expired_time_,
                      pkey_,
                      &txs_->get_part_trans_ctx_mgr(),
                      ctx_info.trans_param_,
                      GET_MIN_CLUSTER_VERSION(),
                      txs_,
                      ctx_info.cluster_id_,
                      -1,
                      ctx_info.can_elr_))) {
                TRANS_LOG(WARN, "part ctx init error", K(ret), K(*part_ctx));
              } else if (OB_FAIL(part_ctx->recover_from_trans_sstable_durable_ctx_info(ctx_info))) {
                TRANS_LOG(WARN, "recover from trans sstable durable ctx info failed", K(ret), K(*part_ctx));
              } else {
                FLOG_INFO("restore trans state in memory", K(trans_id), K(ctx_info));
              }
            }
            if (NULL != ctx) {
              int tmp_ret = 0;
              if (OB_SUCCESS != (tmp_ret = trans_ctx_mgr->revert_trans_ctx(ctx))) {
                TRANS_LOG(WARN, "failed to revert trans ctx", K(ret), K_(pkey));
              }
              ctx = NULL;
            }
          }
        }
        row_iter->~ObStoreRowIterator();
      }
    }
  }
  FLOG_INFO("restore trans table in memory", K(ret), K(pkey_), K(trans_sstable));
  return ret;
}

int ObPGStorage::remove_old_table_(const ObPartitionKey& pkey, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(pkey));
  } else {
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* pg_partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
      LOG_INFO("pg partition may be deleted", K(ret), K(pkey));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition_storage should not be null", K(ret), K(pkey));
    } else if (OB_FAIL(storage->get_partition_store().remove_old_table(frozen_version))) {
      LOG_WARN("failed to remove old table", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPGStorage::remove_mem_ctx_for_trans_ctx_(memtable::ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService* trans_service = NULL;

  if (OB_ISNULL(trans_service = ObPartitionService::get_instance().get_trans_service())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "can't get trans service", K(ret));
  } else if (OB_ISNULL(mt)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "can't get memtable", K(ret));
  } else if (OB_FAIL(trans_service->remove_mem_ctx_for_trans_ctx(mt))) {
    TRANS_LOG(WARN, "remove mem ctx for trans ctx failed", K(ret), KP(mt));
  }

  return ret;
}

int ObPGStorage::get_max_cleanout_log_ts(int64_t& max_cleanout_log_ts)
{
  int ret = OB_SUCCESS;
  int64_t sstable_clean_out_log_ts = 0;
  max_cleanout_log_ts = 0;
  ObTablesHandle handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(sstable_mgr_.get_clean_out_log_ts(sstable_clean_out_log_ts))) {
    LOG_WARN("failed to get clean out log id", K(ret), K(pkey_));
  } else {
    {
      TCRLockGuard lock_guard(lock_);
      max_cleanout_log_ts =
          std::min(sstable_clean_out_log_ts, meta_->storage_info_.get_data_info().get_last_replay_log_ts());
    }
  }
  return ret;
}

int ObPGStorage::get_min_schema_version(int64_t& min_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = 0;
  min_schema_version = INT64_MAX;
  ObPartitionArray pkeys;
  ObPGPartition* pg_partition = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage has not been inited", K(ret));
  } else if (OB_FAIL(get_all_pg_partition_keys_(pkeys))) {
    STORAGE_LOG(WARN, "get all pg partition keys failed", K(ret), K_(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); i++) {
      ObPGPartitionGuard guard(pkeys.at(i), *(pg_->get_pg_partition_map()));
      if (NULL == (pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(pkeys.at(i)));
      } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                             ->get_partition_store()
                             .get_min_schema_version(schema_version))) {
        STORAGE_LOG(WARN, "failed to get merged version", K(ret), K(pkeys.at(i)));
      } else if (schema_version < min_schema_version) {
        min_schema_version = schema_version;
      }
    }
    if (OB_SUCC(ret)) {
      TCRLockGuard lock_guard(lock_);
      if (OB_ISNULL(meta_)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("meta must not null", K(ret));
      } else {
        min_schema_version = min(min_schema_version, meta_->create_schema_version_);
        min_schema_version = min(min_schema_version, meta_->storage_info_.get_data_info().get_schema_version());
      }
    }
  }
  return ret;
}

int ObPGStorage::clear_unused_trans_status()
{
  int ret = OB_SUCCESS;
  int64_t max_cleanout_log_ts = 0;
  if (OB_FAIL(get_max_cleanout_log_ts(max_cleanout_log_ts))) {
    LOG_WARN("failed to get max cleanout log id", K(ret), K_(pkey));
  } else if (OB_FAIL(txs_->clear_unused_trans_status(pkey_, max_cleanout_log_ts))) {
    LOG_WARN("failed to clear trans table", K(ret), K_(pkey));
  }
  return ret;
}

int ObPGStorage::set_storage_file(ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  if (file_handle_.is_valid()) {
    // do nothing
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    LOG_WARN("fail to assign file handle", K(ret));
  } else if (OB_FAIL(sstable_mgr_.set_storage_file_handle(file_handle_))) {
    LOG_WARN("fail to set storage file handle", K(ret), K(file_handle_));
  } else if (OB_FAIL(recovery_point_data_mgr_.set_storage_file_handle(file_handle_))) {
    LOG_WARN("failed to set storage file handle for recovery point data mgr", K(ret), K_(pkey));
  }
  return ret;
}

int ObPGStorage::get_trans_table_end_log_ts_and_timestamp(int64_t& end_log_ts, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg is not inited", K(ret));
  } else if (OB_FAIL(get_trans_table_end_log_ts_and_timestamp_(end_log_ts, timestamp))) {
    LOG_WARN("failed to get_trans_table_end_log_ts_and_timestamp_", K(ret));
  }
  return ret;
}

int ObPGStorage::get_trans_table_end_log_ts_and_timestamp_(int64_t& end_log_ts, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  end_log_ts = 0;
  timestamp = -1;
  bool data_replica = false;
  ObPartitionKey trans_table_pkey;
  {
    TCRLockGuard lock_guard(lock_);
    data_replica = ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_);
  }
  if (!data_replica) {
    // not replica with sstable, no need to retrieve trans sstable
  } else if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(pkey_))) {
    LOG_WARN("failed to generate trans_table_pkey", K(ret), K_(pkey));
  } else {
    ObPGPartitionGuard pg_partition_guard(trans_table_pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      if (0 != get_partition_cnt()) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("trans table partition not exist", K(ret), K(trans_table_pkey), K(pkey_));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(trans_table_pkey));
    } else if (OB_FAIL(
                   storage->get_partition_store().get_trans_table_end_log_ts_and_timestamp(end_log_ts, timestamp))) {
      LOG_WARN("failed to get last snapshot version", K(ret), K_(pkey));
    }
  }
  return ret;
}

int ObPGStorage::get_latest_frozen_memtable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTablesHandle memtables;
  const int64_t start_point = -1;
  const bool include_active_memtable = false;
  const bool reset_handle = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg_storage is not inited", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_memtables(memtables, reset_handle, start_point, include_active_memtable))) {
    LOG_WARN("failed to get_memtables", K(ret), K_(pkey));
  } else if (memtables.get_count() <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ObITable* memtable = memtables.get_table(memtables.get_count() - 1);
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      LOG_WARN("memtable is null", K(ret));
    } else if (OB_FAIL(handle.set_table(memtable))) {
      LOG_WARN("failed to set table", K(ret), K(memtables));
    }
  }
  return ret;
}

// bucket lock is held by caller
int ObPGStorage::clear_all_complement_minor_sstable_()
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  const bool include_trans_table = false;
  if (OB_FAIL(get_all_pg_partition_keys_(pkeys, include_trans_table))) {
    LOG_WARN("failed to get_all_pg_partition_keys", K(ret), K_(pkey));
  } else {
    ObPGPartition* pg_partition = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      ObPGPartitionGuard guard(pkeys.at(i), *(pg_->get_pg_partition_map()));
      if (NULL == (pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K(pkeys.at(i)));
      } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                             ->get_partition_store()
                             .clear_complement_minor_sstable())) {
        STORAGE_LOG(WARN, "failed to get merged version", K(ret), K(pkeys.at(i)));
      }
    }
  }
  return ret;
}

int ObPGStorage::get_all_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(sstable_mgr_.get_all_sstables(handle))) {
    LOG_WARN("failed to get sstables", K(ret), K_(pkey));
  }
  return ret;
}

// only restart and physical flashbackup use this interface
// not lock it
int ObPGStorage::check_can_physical_flashback(const int64_t flashback_scn)
{
  int ret = OB_SUCCESS;
  ObPartitionArray partitions;
  TCRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (flashback_scn <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical flashback get invalid argument", K(ret), K(flashback_scn));
  } else if (OB_FAIL(get_all_pg_partition_keys_(partitions))) {
    STORAGE_LOG(WARN, "failed to get all pg partition keys", K(ret));
  } else {
    ObPGPartition* pg_partition = nullptr;
    int64_t smallest_publish_version = INT64_MAX;
    bool need_skip_check = false;

    if (ObReplicaTypeCheck::is_log_replica(meta_->replica_type_)) {
      // just modified meta data info or clog info
      need_skip_check = true;
    } else {
      need_skip_check = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); i++) {
        bool need_remove = false;
        ObPGPartitionGuard pg_partition_guard(partitions.at(i), *(pg_->get_pg_partition_map()));
        int64_t publish_version = 0;
        if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
          // may pg has create but partition do not create
        } else if (OB_FAIL(check_flashback_partition_need_remove(flashback_scn, pg_partition, need_remove))) {
          LOG_WARN("[PHY_FLASHBACK]failed to check flashback partition need remove", K(ret), K(flashback_scn));
        } else if (need_remove) {
          // do nothing
        } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                               ->get_partition_store()
                               .get_physical_flashback_publish_version(flashback_scn, publish_version))) {
          STORAGE_LOG(WARN,
              "[PHY_FLASHBACK]failed to get physical flashback publish version",
              K(ret),
              K(pg_partition->get_partition_key()));
        } else {
          smallest_publish_version =
              smallest_publish_version > publish_version ? publish_version : smallest_publish_version;
          need_skip_check = false;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObRecoverPoint recover_point;
      if (need_skip_check) {
        STORAGE_LOG(
            INFO, "[PHY_FLASHBACK]need skip check this parttiion", K(ret), K_(pkey), K(smallest_publish_version));
      } else if (OB_FAIL(meta_->get_recover_info_for_flashback(smallest_publish_version, recover_point))) {
        STORAGE_LOG(WARN,
            "[PHY_FLASHBACK]get_recover_info_for_flashback failed",
            K(ret),
            K_(pkey),
            K(smallest_publish_version));
      } else if (recover_point.submit_timestamp_ <= 0) {
        ret = OB_OP_NOT_ALLOW;
        STORAGE_LOG(ERROR,
            "[PHY_FLASHBACK]recover point is invalid, not allow do physical flashback",
            K(ret),
            K_(pkey),
            K(smallest_publish_version),
            K(recover_point));
      } else {
        STORAGE_LOG(INFO,
            "[PHY_FLASHBACK]this partition can do physical flashback",
            K(ret),
            K_(pkey),
            K(smallest_publish_version),
            K(recover_point));
      }
    } else {
      STORAGE_LOG(WARN,
          "[PHY_FLASHBACK]this partition cannot do physical flashback",
          K(ret),
          K_(pkey),
          K(smallest_publish_version));
    }
  }

  return ret;
}

int ObPGStorage::get_latest_table_count(const ObPartitionKey& pkey, const int64_t index_id, int64_t& table_count)
{
  int ret = OB_SUCCESS;
  ObPGPartition* pg_partition = NULL;
  ObPGPartitionGuard guard(pkey, *(pg_->get_pg_partition_map()));
  if (NULL == (pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
  } else if (OB_FAIL(static_cast<ObPartitionStorage*>(pg_partition->get_storage())
                         ->get_partition_store()
                         .get_latest_table_count(index_id, table_count))) {
    STORAGE_LOG(WARN, "get latest table count failed", K(ret), K(index_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObPGStorage::create_trans_table_partition_(const bool in_slog_trans, const bool is_replay)
{
  int ret = OB_SUCCESS;
  const int64_t multi_version_start = 1;
  const int64_t log_id = 0;
  bool is_exist = false;
  ObTablesHandle trans_sstables_handle;
  ObPartitionKey trans_table_pkey;
  ObCreatePartitionParam arg;
  arg.pg_key_ = pkey_;
  arg.frozen_timestamp_ = ObTimeUtility::current_time();
  arg.schema_version_ = 1;
  arg.lease_start_ = ObTimeUtility::current_time();

  if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(pkey_))) {
    STORAGE_LOG(WARN, "failed to transform table pkey", K(ret), K_(pkey));
  } else if (OB_FAIL(check_pg_partition_exist(trans_table_pkey, is_exist))) {
    STORAGE_LOG(WARN, "failed to check pg partition exist", K(ret), K(trans_table_pkey));
  } else if (is_exist) {
    // do nothing
  } else if (OB_FAIL(create_trans_sstable(arg, in_slog_trans, trans_sstables_handle))) {
    STORAGE_LOG(WARN, "failed to create trans sstable", K(ret), K_(pkey));
  } else if (OB_FAIL(create_pg_partition_(trans_table_pkey,
                 multi_version_start,
                 trans_table_pkey.get_table_id(),
                 arg,
                 in_slog_trans,
                 is_replay,
                 log_id,
                 trans_sstables_handle))) {
    STORAGE_LOG(WARN, "failed to create trans table pg partition", K(ret), K(trans_table_pkey));
  } else {
    STORAGE_LOG(INFO, "succ to create trans table gp partition", K(trans_table_pkey));
  }

  return ret;
}

int ObPGStorage::create_trans_sstable(
    const ObCreatePartitionParam& create_partition_param, const bool in_slog_trans, ObTablesHandle& sstables_handle)
{
  int ret = OB_SUCCESS;
  ObITable::TableKey table_key;
  ObPGCreateSSTableParam create_sstable_param;
  ObTableSchema table_schema;
  ObCreateSSTableParamWithPartition param;
  uint64_t table_id = 0;
  ObCreatePartitionMeta meta;
  ObPartitionKey trans_table_pkey;

  if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(create_partition_param.pg_key_))) {
    LOG_WARN("failed to generate trans table pkey", K(ret), K(create_partition_param.pg_key_));
  } else if (FALSE_IT(table_id = trans_table_pkey.get_table_id())) {
    // never reach
  } else if (OB_FAIL(table_schema.generate_kv_schema(trans_table_pkey.get_tenant_id(), table_id))) {
    LOG_WARN("failed to generate trans table pkey", K(ret), K(create_partition_param.pg_key_));
  } else if (OB_FAIL(meta.extract_from(table_schema))) {
    LOG_WARN("failed to extract from table schema", K(ret));
  } else {
    param.progressive_merge_round_ = 0;
    table_key.pkey_ = trans_table_pkey;
    table_key.table_type_ = ObITable::TRANS_SSTABLE;
    param.schema_ = &meta;
  }

  if (OB_SUCC(ret)) {
    ObTableHandle sstable_handle;
    ObSSTable* sstable = NULL;
    table_key.table_id_ = table_id;
    table_key.trans_version_range_.multi_version_start_ = create_partition_param.frozen_timestamp_;
    table_key.trans_version_range_.base_version_ = ObVersionRange::MIN_VERSION;
    table_key.trans_version_range_.snapshot_version_ = create_partition_param.frozen_timestamp_;
    table_key.version_ = ObVersion(create_partition_param.memstore_version_ - 1, 0);

    param.table_key_ = table_key;
    param.logical_data_version_ = table_key.version_;
    param.schema_version_ = create_partition_param.schema_version_;
    param.create_snapshot_version_ = create_partition_param.frozen_timestamp_;
    param.checksum_method_ = CCM_VALUE_ONLY;
    param.progressive_merge_step_ = 0;
    param.set_is_inited(true);
    create_sstable_param.with_partition_param_ = &param;

    if (OB_FAIL(create_sstable(create_sstable_param, in_slog_trans, sstable_handle))) {
      STORAGE_LOG(WARN, "failed to create major sstable", K(ret), K(table_key));
    } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
      STORAGE_LOG(WARN, "failed to get sstable", K(ret));
    } else if (OB_FAIL(sstables_handle.add_table(sstable_handle))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }
  return ret;
}

int ObPGStorage::get_trans_table_status(ObTransTableStatus& status)
{
  int ret = OB_SUCCESS;
  ObPartitionKey trans_table_pkey;

  if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(pkey_))) {
    LOG_WARN("failed to generate trans_table_pkey", K(ret), K_(pkey));
  } else {
    ObPGPartitionGuard guard(trans_table_pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    if (OB_ISNULL(partition = guard.get_pg_partition())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(static_cast<ObPartitionStorage*>(partition->get_storage())
                           ->get_partition_store()
                           .get_trans_table_status(status))) {
      STORAGE_LOG(WARN, "failed to scan trans table", K(ret));
    }
  }
  return ret;
}

int ObPGStorage::batch_replace_store_map(const ObIArray<ObPartitionMigrateCtx>& part_ctx_array,
    const int64_t schema_version, const bool is_restore, const int64_t old_trans_table_seq)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionStore::TableStoreMap** store_maps = nullptr;
  ObTablesHandle empty_handle;
  const int64_t unused = 0;
  const bool in_slog_trans = true;
  const bool is_replay = false;
  const int64_t alloc_size = sizeof(ObPartitionStore::TableStoreMap*) * part_ctx_array.count();
  ObMemAttr mem_attr(pkey_.get_tenant_id(), ObModIds::OB_PARTITION_MIGRATOR);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (part_ctx_array.count() == 0) {
    LOG_INFO("empty pg, no need to replace", K(ret), K_(pkey));
  } else if (OB_ISNULL(store_maps = static_cast<ObPartitionStore::TableStoreMap**>(ob_malloc(alloc_size, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate store maps", K(ret));
  } else {
    MEMSET(store_maps, 0, alloc_size);
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);
    if (ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_) && old_trans_table_seq != trans_table_seq_) {
      ret = OB_EAGAIN;
      LOG_WARN("trans table seq has changed", K(ret), K(old_trans_table_seq), K_(trans_table_seq));
    } else if (OB_FAIL(SLOGGER.begin(OB_LOG_BATCH_REPLACE_STORE_MAP))) {
      LOG_WARN("failed to begin slog trans", K(ret));
    } else if (OB_FAIL(create_pg_partition_if_need_(part_ctx_array, schema_version, is_restore))) {
      LOG_WARN("failed to create pg partition", K(ret), K(part_ctx_array));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < part_ctx_array.count(); ++i) {
        const ObPartitionMigrateCtx& part_ctx = part_ctx_array.at(i);
        ObPartitionStore::TableStoreMap* store_map = nullptr;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(prepare_partition_store_map_(part_ctx, store_map))) {
            LOG_WARN("failed to replace partition store map", K(ret), K(part_ctx));
          } else {
            store_maps[i] = store_map;
          }
        }
      }
      if (OB_SUCC(ret)) {
        int64_t lsn = 0;
        if (OB_FAIL(SLOGGER.commit(lsn))) {
          LOG_WARN("failed to commit slog trans", K(ret));
        } else if (OB_FAIL(do_replace_store_map_(part_ctx_array, store_maps))) {
          LOG_ERROR("failed to do replace store maps", K(ret));
          ob_abort();
        } else {
          ++trans_table_seq_;
          FLOG_INFO("succeed to batch replace store map", K(ret), K_(pkey), K(lsn));
        }
      } else {
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          STORAGE_LOG(WARN, "failed to rollback slog", K(ret), K(tmp_ret));
        }
      }
      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i < part_ctx_array.count(); ++i) {
          ObPartitionStore::destroy_store_map(store_maps[i]);
        }
      }
    }
  }
  if (OB_NOT_NULL(store_maps)) {
    ob_free(store_maps);
  }
  return ret;
}

int ObPGStorage::prepare_partition_store_map_(
    const ObPartitionMigrateCtx& ctx, ObPartitionStore::TableStoreMap*& store_map)
{
  int ret = OB_SUCCESS;
  ObPGPartitionGuard part_guard(ctx.copy_info_.meta_.pkey_, *(pg_->get_pg_partition_map()));
  ObPGPartition* partition = nullptr;
  ObPartitionStorage* storage = nullptr;
  const int64_t max_kept_major_version_number = GCONF.max_kept_major_version_number;
  store_map = nullptr;
  if (ctx.handle_.empty()) {
  } else if (OB_ISNULL(partition = part_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get pg partition failed", K(ret), K(ctx));
  } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get partition storage failed", K(ret), K(ctx));
  } else if (OB_FAIL(storage->get_partition_store().prepare_new_store_map(
                 ctx.handle_, max_kept_major_version_number, ctx.need_reuse_local_minor_, store_map))) {
    LOG_WARN("failed to prepare new store map", K(ret), K(ctx));
  }
  return ret;
}

int ObPGStorage::do_replace_store_map_(
    const common::ObIArray<ObPartitionMigrateCtx>& part_ctx_array, ObPartitionStore::TableStoreMap** store_maps)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_ctx_array.count(); ++i) {
    const ObPartitionMigrateCtx& ctx = part_ctx_array.at(i);
    ObPartitionStore::TableStoreMap* store_map = store_maps[i];
    if (store_map != nullptr) {
      ObPGPartitionGuard part_guard(ctx.copy_info_.meta_.pkey_, *(pg_->get_pg_partition_map()));
      ObPGPartition* partition = nullptr;
      ObPartitionStorage* storage = nullptr;
      if (OB_ISNULL(partition = part_guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get pg partition failed", K(ret), K(ctx));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get partition storage failed", K(ret), K(ctx));
      } else {
        storage->get_partition_store().replace_store_map(*store_map);
        LOG_INFO("Succ to replace store map", KP(store_map));
      }
    }
  }
  return ret;
}

void ObPGStorage::pause()
{
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  is_paused_ = true;
  ++trans_table_seq_;
  FLOG_INFO("pg storage is paused", K_(pkey), K_(is_paused), K_(trans_table_seq));
}

void ObPGStorage::online()
{
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  ++trans_table_seq_;
  is_paused_ = false;
  FLOG_INFO("pg storage is onlined", K_(pkey), K_(is_paused), K_(trans_table_seq));
}

int64_t ObPGStorage::get_trans_table_seq()
{
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  return trans_table_seq_;
}

bool ObPGStorage::is_paused()
{
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);
  return is_paused_;
}

int ObPGStorage::add_trans_sstable(const int64_t old_trans_table_seq, ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  ObPartitionKey trans_table_pkey;
  const int64_t max_kept_major_version_number = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable should not be null", K(ret));
  } else if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(pkey_))) {
    LOG_WARN("failed to generate trans_table_pkey", K(ret), K_(pkey));
  } else {
    ObBucketWLockGuard bucket_guard(bucket_lock_, get_bucket_idx_(trans_table_pkey));
    ObPGPartitionGuard pg_partition_guard(trans_table_pkey, *(pg_->get_pg_partition_map()));
    ObPGPartition* partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (is_paused_) {
      ret = OB_EAGAIN;
      LOG_WARN("pg is paused, can not add trans table now", K(ret), K_(pkey), K(trans_table_pkey));
    } else if (old_trans_table_seq != trans_table_seq_) {
      ret = OB_EAGAIN;
      LOG_WARN("trans table seq is not equal, can not add trans table now",
          K(ret),
          K_(pkey),
          K(trans_table_pkey),
          K(old_trans_table_seq),
          K_(trans_table_seq));
    } else if (OB_ISNULL(partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("pg partition not exist", K(ret), K(pkey_), K(*meta_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage cannot be null", K(ret), K(pkey_));
    } else if (OB_FAIL(storage->get_partition_store().add_sstable_for_merge(sstable,
                   max_kept_major_version_number,
                   meta_->migrate_status_,
                   0 != meta_->is_restore_,
                   is_source_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_)),
                   is_dest_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_)),
                   nullptr /*complement_minor_sstable*/))) {
      LOG_WARN("failed to add_sstable_for_merge", K(ret), K(pkey_));
    } else {
      ++trans_table_seq_;
      FLOG_INFO("succeed to add trans sstable", K_(pkey), K(sstable->get_key()), K(trans_table_seq_));
    }
  }
  return ret;
}

int ObPGStorage::inc_pending_batch_commit_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObMemtable* memtable = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_memtable_mgr_.get_all_memtables(handle))) {
    STORAGE_LOG(WARN, "failed to get active memtable", K(ret), K_(pkey));
  } else {
    for (int64_t i = 0; i < handle.get_count(); i++) {
      if (handle.get_table(i)->get_start_log_ts() < log_ts && handle.get_table(i)->get_end_log_ts() >= log_ts) {
        memtable = static_cast<ObMemtable*>(handle.get_table(i));
        break;
      }
    }
    if (NULL == memtable) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "memtable should exist", K(ret), K(log_ts), K(handle));
    } else {
      if (OB_FAIL(mt_ctx.set_memtable_for_batch_commit(memtable))) {
        STORAGE_LOG(WARN, "set memtable for batch commit", K(ret), K_(pkey), K(log_ts));
      } else {
        memtable->inc_pending_batch_commit_count();
      }
    }
  }

  return ret;
}

int ObPGStorage::inc_pending_elr_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObMemtable* memtable = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_memtable_mgr_.get_all_memtables(handle))) {
    STORAGE_LOG(WARN, "failed to get active memtable", K(ret), K_(pkey));
  } else {
    for (int64_t i = 0; i < handle.get_count(); i++) {
      if (handle.get_table(i)->get_start_log_ts() < log_ts && handle.get_table(i)->get_end_log_ts() >= log_ts) {
        memtable = static_cast<ObMemtable*>(handle.get_table(i));
        break;
      }
    }
    if (NULL == memtable) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "memtable should exist", K(ret), K(log_ts), K(handle));
    } else {
      if (OB_FAIL(mt_ctx.set_memtable_for_elr(memtable))) {
        STORAGE_LOG(WARN, "set memtable for elr", K(ret), K_(pkey), K(log_ts));
      } else {
        memtable->inc_pending_elr_count();
      }
    }
  }

  return ret;
}

int ObPGStorage::get_freeze_info_(const common::ObVersion& version, ObFreezeInfoSnapshotMgr::FreezeInfo& freeze_info)
{
  int ret = OB_SUCCESS;
  freeze_info.reset();
  if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_freeze_info_by_major_version(
          pkey_.get_table_id(), version.major_, freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret ||
        (OB_EAGAIN == ret && OB_ALL_CORE_TABLE_TID == extract_pure_id(pkey_.get_table_id()))) {
      LOG_WARN("failed to get freeze info", K(ret), K(pkey_));
      ObFreezeInfoSnapshotMgr::FreezeInfoLite freeze_info_lite;
      if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_freeze_info_by_major_version(
              version.major_, freeze_info_lite))) {
        LOG_WARN("failed to get freeze info by major version", K(ret), K_(pkey), K(version));
      } else {
        freeze_info = freeze_info_lite;
        freeze_info.schema_version = 0;
      }
    } else {
      LOG_WARN("failed to get freeze info by major version", K(ret), K_(pkey), K(version));
    }
  }
  return ret;
}

int ObPGStorage::create_pg_partition_(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
    const uint64_t data_table_id, const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans,
    const bool is_replay, const uint64_t log_id, ObTablesHandle& sstables_handle)
{
  int ret = OB_SUCCESS;
  ObCreatePartitionParam create_param;
  if (pkey.get_tenant_id() != pkey_.get_tenant_id()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tenant id not match", K(ret), K(pkey), K(pkey_));
  } else if (OB_FAIL(create_param.extract_from(arg))) {
    STORAGE_LOG(WARN, "failed to extract create_param", KR(ret), K(pkey), K(arg));
  } else {
    ret = create_pg_partition_(
        pkey, multi_version_start, data_table_id, create_param, in_slog_trans, is_replay, log_id, sstables_handle);
  }
  return ret;
}

int ObPGStorage::create_pg_partition_(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
    const uint64_t data_table_id, const ObCreatePartitionParam& arg, const bool in_slog_trans, const bool is_replay,
    const uint64_t log_id, ObTablesHandle& sstables_handle)
{
  ObTimeGuard tg(__func__, 100L * 1000L);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  tg.click();
  bool empty_pg = is_empty_pg();
  ObPartitionGroupMeta* next_meta_ptr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_table_id));
  } else if (pkey.get_tenant_id() != pkey_.get_tenant_id()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tenant id not match", K(ret), K(pkey), K(pkey_));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K_(pkey));
  } else if (!is_replay && get_partition_cnt() + 1 > OB_MAX_PG_PARTITION_COUNT_PER_PG) {
    ret = OB_TOO_MANY_PARTITIONS_ERROR;
    LOG_WARN("too many pg partitions per pg", "pg_partition_cnt", get_partition_cnt(), K(pkey));
  } else if (OB_FAIL(alloc_meta_(next_meta_ptr))) {
    LOG_WARN("failed to alloc meta", K(ret));
  } else if (!in_slog_trans && OB_FAIL(SLOGGER.begin(OB_LOG_CS_CREATE_PARTITION))) {
    STORAGE_LOG(WARN, "fail to begin commit partition log.", K(ret));
  } else {
    tg.click();
    ObPGPartition* pg_partition = NULL;
    ObPartitionStorage* storage = nullptr;
    bool need_rollback_pg_index = false;
    bool need_rollback_partition_map = false;

    if (NULL == (pg_partition = op_alloc(ObPGPartition))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc pg partition info error", K(ret), KP(pg_partition), K(pkey));
    } else if (OB_FAIL(pg_partition->init(pkey, cp_fty_, schema_service_, txs_, pg_, pg_memtable_mgr_))) {
      STORAGE_LOG(WARN, "partition info init error", K(ret), K(pkey), KP_(cp_fty), KP_(schema_service), KP_(txs));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition storage is null", K(ret), K(pkey));
    } else if (OB_FAIL(write_create_partition_slog(pkey, log_id))) {
      LOG_WARN("failed to write create partition slog", K(ret), K(pkey_), K(pkey));
    } else if (OB_FAIL(storage->create_partition_store(meta_->replica_type_,
                   multi_version_start,
                   data_table_id,
                   arg.schema_version_,
                   arg.lease_start_,
                   pg_,
                   sstables_handle))) {
      if (OB_INIT_FAIL == ret) {
        STORAGE_LOG(WARN, "failed to create partition store", K(ret), K(pkey));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    } else if (ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_) && sstables_handle.get_count() > 0) {
      const int64_t kept_major_num = 1;
      const bool in_slog_trans = true;
      const bool is_in_dest_split = is_dest_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_));
      for (int64_t i = 0; OB_SUCC(ret) && i < sstables_handle.get_count(); ++i) {
        ObITable* table = sstables_handle.get_table(i);
        if (OB_FAIL(storage->get_partition_store().add_sstable(static_cast<ObSSTable*>(table),
                kept_major_num,
                in_slog_trans,
                meta_->migrate_status_,
                is_in_dest_split,
                arg.schema_version_))) {
          LOG_WARN("failed to add sstable", K(ret), K(pkey));
        }
      }
    }
    tg.click();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(register_pg_partition_(pkey, pg_partition))) {
        STORAGE_LOG(WARN, "create pg partition error", K(ret), K(pkey));
      } else {
        need_rollback_partition_map = true;
      }
    } else {
      if (NULL != pg_partition) {
        op_free(pg_partition);
        pg_partition = nullptr;
      }
    }
    tg.click();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPartitionService::get_instance().get_pg_index().add_partition(pkey, pkey_))) {
        LOG_WARN("failed to add pg index", K(ret), K(pkey_), K(pkey));
        if (OB_ENTRY_EXIST == ret) {
          need_clear_pg_index_ = false;
        }
      } else {
        need_rollback_pg_index = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (ObReplicaTypeCheck::is_replica_with_memstore(meta_->replica_type_) &&
          OB_FAIL(pg_memtable_mgr_.update_memtable_schema_version(arg.schema_version_))) {
        LOG_ERROR("failed to add pg index", K(ret), K(pkey_), K(pkey));
      }
    }

    tg.click();
    if (OB_SUCC(ret)) {
      ObPartitionGroupMeta& next_meta = *next_meta_ptr;
      next_meta.reset();
      if (OB_FAIL(next_meta.deep_copy(*meta_))) {
        STORAGE_LOG(WARN, "failed to copy meta", K(ret), K_(pkey));
        // restore publish version from pg meta
      } else if (pkey_.is_pg() && empty_pg && next_meta.ddl_seq_num_ > 1) {
        const int64_t curr_publish_version = next_meta.storage_info_.get_data_info().get_publish_version();
        if (multi_version_start > curr_publish_version) {
          next_meta.storage_info_.get_data_info().set_publish_version(multi_version_start);
        }
      } else {
        // do nothing
      }
      tg.click();
      if (OB_SUCC(ret)) {
        if (OB_FAIL(insert_pg_partition_arr_(&(next_meta.partitions_), pkey))) {
          STORAGE_LOG(WARN, "failed to push back pkey to meta.partitions", K(ret), "pg key", pkey_, K(pkey));
        } else {
          next_meta.ddl_seq_num_++;
          ObUpdatePartitionGroupMetaLogEntry log_entry;
          const int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_UPDATE_PARTITION_GROUP_META);
          const ObStorageLogAttribute log_attr(pkey_.get_tenant_id(), meta_->storage_info_.get_pg_file_id());
          if (OB_FAIL(log_entry.meta_.deep_copy(next_meta))) {
            STORAGE_LOG(WARN, "failed to deep copy meta", K(ret));
          } else if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
            STORAGE_LOG(WARN, "failed to write update partition meta slog", K(ret));
          } else {
            // do nothing
          }
        }
      }
      tg.click();
    }

    if (!in_slog_trans) {
      int64_t lsn = 0;
      if (OB_FAIL(ret)) {
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          STORAGE_LOG(WARN, "commit logger failed to abort", K(tmp_ret));
        }
      } else if (OB_FAIL((SLOGGER.commit(lsn)))) {
        STORAGE_LOG(WARN, "fail to commit log.", K(ret), K(lsn));
      }
    }
    tg.click();

    if (OB_SUCC(ret)) {
      switch_meta_(next_meta_ptr);
    } else {
      if (need_rollback_partition_map) {
        if (OB_SUCCESS != (tmp_ret = pg_->get_pg_partition_map()->del(pkey))) {
          LOG_WARN("failed to del pg partition from partition map", K(tmp_ret), K(pkey));
          ob_abort();
        }
        partition_list_.remove_latest(pkey);
      }

      if (need_rollback_pg_index) {
        if (OB_SUCCESS != (tmp_ret = pg_->get_pg_index()->remove_partition(pkey))) {
          LOG_ERROR("failed to remove pg index", K(tmp_ret), K(pkey_), K(pkey));
          ob_abort();
        }
      }
    }
    tg.click();
  }
  FLOG_INFO("create pg partition", K(ret), K(pkey), K(*meta_));

  free_meta_(next_meta_ptr);
  return ret;
}

int ObPGStorage::create_pg_partition_if_need_(
    const ObIArray<ObPartitionMigrateCtx>& part_ctx_array, const int64_t schema_version, const bool is_restore)
{
  int ret = OB_SUCCESS;
  ObTablesHandle empty_handle;
  const int64_t unused = 0;
  const bool in_slog_trans = true;
  const bool is_replay = false;
  for (int i = 0; OB_SUCC(ret) && i < part_ctx_array.count(); ++i) {
    const ObPartitionMigrateCtx& part_ctx = part_ctx_array.at(i);
    bool is_exist = false;
    if (OB_FAIL(check_pg_partition_exist(part_ctx.copy_info_.meta_.pkey_, is_exist))) {
      LOG_WARN("failed to check pg partition exist", K(ret), K(part_ctx));
    } else if (!is_exist) {
      const ObPGPartitionStoreMeta& meta = part_ctx.copy_info_.meta_;
      ObCreatePartitionArg arg;
      arg.schema_version_ = schema_version;
      arg.lease_start_ = meta.create_timestamp_;
      arg.restore_ = is_restore;
      if (OB_FAIL(create_pg_partition_(meta.pkey_,
              meta.multi_version_start_,
              meta.data_table_id_,
              arg,
              in_slog_trans,
              is_replay,
              unused,
              empty_handle))) {
        LOG_WARN("failed to create pg partition", K(ret), K(part_ctx));
      }
    }
  }
  return ret;
}

int ObPGStorage::get_partition_migrate_tables(
    const ObPartitionKey& pkey, const uint64_t table_id, ObTablesHandle& tables_handle, bool& is_ready_for_read)
{
  int ret = OB_SUCCESS;
  tables_handle.reset();
  is_ready_for_read = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg storage do not inited", K(ret));
  } else if (!pkey.is_valid() || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get partition store meta get invalid argument", K(ret), K(pkey), K(table_id));
  } else {
    ObPGPartition* pg_partition = NULL;
    ObPartitionStorage* partition_storage = NULL;
    ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
    if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
    } else if (NULL == (partition_storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "fail to get partition storage", K(ret), KP(partition_storage));
    } else if (OB_FAIL(partition_storage->get_partition_store().get_migrate_tables(
                   table_id, tables_handle, is_ready_for_read))) {
      STORAGE_LOG(WARN, "fail to get_effective_tables", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObPGStorage::check_split_dest_merged_(bool& is_merged)
{
  int ret = OB_SUCCESS;
  const bool is_tmp_source_split = is_source_split(static_cast<ObPartitionSplitStateEnum>(meta_->saved_split_state_));
  if (is_tmp_source_split) {
    bool is_tmp_merged = true;
    is_merged = true;
    ObIPartitionGroupGuard dest_guard;
    storage::ObIPartitionGroup* dest_partition = NULL;
    ObPartitionSplitInfo& split_info = meta_->split_info_;
    const common::ObIArray<common::ObPartitionKey>& dest_pgkeys = split_info.get_dest_partitions();

    for (int64_t i = 0; OB_SUCC(ret) && i < dest_pgkeys.count(); i++) {
      const ObPartitionKey dest_pgkey = dest_pgkeys.at(i);
      if (OB_FAIL(ObPartitionService::get_instance().get_partition(dest_pgkey, dest_guard))) {
        if (OB_PARTITION_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          // do nothing
        } else {
          STORAGE_LOG(WARN, "Fail to get partition", K(ret));
        }
      } else if (OB_ISNULL(dest_partition = dest_guard.get_partition_group())) {
        ret = OB_ERR_SYS;
        LOG_WARN("partition must not null", K(ret), K(dest_pgkey));
      } else if (OB_FAIL(dest_partition->get_pg_storage().check_trans_table_merged(is_tmp_merged))) {
        LOG_WARN("failed to check complete", K(ret), K(dest_pgkey));
      } else if (!is_tmp_merged) {
        is_merged = false;
        break;
      }
    }
  }

  return ret;
}

int ObPGStorage::check_trans_table_merged(bool& is_merged)
{
  int ret = OB_SUCCESS;
  is_merged = true;
  ObTablesHandle handle;
  int64_t trans_table_end_log_ts = 0;
  int64_t timestamp = 0;
  ObMemtable* memtable = NULL;
  ObBucketWLockAllGuard bucket_guard(bucket_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg is not inited", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_.get_all_memtables(handle))) {
    LOG_WARN("failed to get all memtables", K(ret), K_(pkey));
  } else if (OB_FAIL(get_trans_table_end_log_ts_and_timestamp_(trans_table_end_log_ts, timestamp))) {
    LOG_WARN("failed to get_trans_table_end_log_ts_and_timestamp", K(ret), K_(pkey));
  } else {
    for (int64_t i = 0; i < handle.get_count(); i++) {
      memtable = static_cast<memtable::ObMemtable*>(handle.get_table(i));
      if (trans_table_end_log_ts < memtable->get_end_log_ts() || timestamp < memtable->get_timestamp()) {
        is_merged = false;
        break;
      }
    }
  }
  return ret;
}

int ObPGStorage::replay_add_recovery_point_data(
    const ObRecoveryPointType point_type, const ObRecoveryPointData& point_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K(pkey_));
  } else if (OB_FAIL(recovery_point_data_mgr_.replay_add_recovery_point(point_type, point_data))) {
    LOG_WARN("failed to replay add recovery point data", K(ret), K(point_type), K(point_data));
  }
  return ret;
}

int ObPGStorage::replay_remove_recovery_point_data(const ObRecoveryPointType point_type, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(recovery_point_data_mgr_.replay_remove_recovery_point(point_type, snapshot_version))) {
    LOG_WARN("failed to replay remove recovery point data", K(ret), K(point_type), K(snapshot_version));
  }
  return ret;
}

int ObPGStorage::get_backup_meta_data(
    const int64_t snapshot_version, ObPartitionGroupMeta& pg_meta, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(recovery_point_data_mgr_.get_backup_point_data(snapshot_version, pg_meta, handle))) {
    LOG_WARN("failed to get backup meta data", K(ret), K(pkey_));
  }
  return ret;
}

int ObPGStorage::get_backup_pg_meta_data(const int64_t snapshot_version, ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (OB_FAIL(recovery_point_data_mgr_.get_backup_pg_meta_data(snapshot_version, pg_meta))) {
    LOG_WARN("failed to get backup meta data", K(ret), K(pkey_));
  }
  return ret;
}

int ObPGStorage::update_restore_points(
    const ObIArray<int64_t>& restore_points, const ObIArray<int64_t>& schema_versions, const int64_t snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> snapshot_versions;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("pg is removed", K(ret), K_(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < restore_points.count(); i++) {
      const int64_t snapshot_ts = restore_points.at(i);
      const int64_t schema_version = schema_versions.at(i);
      ObPartitionGroupMeta pg_meta;
      ObSEArray<ObPGPartitionStoreMeta, 1> metas;
      ObTablesHandle handle;
      bool is_exist = true;
      bool is_ready = false;
      bool is_need = true;
      if (OB_FAIL(recovery_point_data_mgr_.check_restore_point_exist(snapshot_ts, is_exist))) {
        LOG_WARN("failed to check restore point exist", K(ret), K_(pkey), K(restore_points));
      } else if (!is_exist) {
        if (OB_FAIL(
                get_restore_point_tables_(snapshot_ts, schema_version, pg_meta, metas, handle, is_ready, is_need))) {
          LOG_WARN("failed to get restore point sstables", K(ret), K_(pkey), K(snapshot_ts));
        } else if (!is_need) {
          // do nothing
        } else if (!is_ready) {
          if (EXECUTE_COUNT_PER_SEC(1)) {
            LOG_INFO("restore point is not ready", K_(pkey), K(snapshot_ts));
          }
          // do nothing
        } else {
          ObBucketWLockAllGuard bucket_guard(bucket_lock_);
          if (OB_FAIL(recovery_point_data_mgr_.add_restore_point(snapshot_ts, pg_meta, metas, handle))) {
            LOG_WARN("failed to add restore point", K(ret), K(snapshot_ts), K(pg_meta), K(handle));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObBucketWLockAllGuard bucket_guard(bucket_lock_);
      if (OB_FAIL(recovery_point_data_mgr_.remove_unneed_restore_point(restore_points, snapshot_gc_ts))) {
        LOG_WARN("failed to remove unneed restore point", K(ret), K(restore_points));
      }
    }
  }
  return ret;
}

int ObPGStorage::update_backup_points(
    const ObIArray<int64_t>& backup_points, const ObIArray<int64_t>& schema_versions, const int64_t snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> snapshot_versions;
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_points.count(); i++) {
    const int64_t snapshot_ts = backup_points.at(i);
    const int64_t schema_version = schema_versions.at(i);
    ObPartitionGroupMeta pg_meta;
    ObSEArray<ObPGPartitionStoreMeta, 1> metas;
    ObTablesHandle handle;
    bool is_exist = true;
    bool is_ready = false;
    bool is_need = true;
    if (OB_FAIL(recovery_point_data_mgr_.check_backup_point_exist(snapshot_ts, is_exist))) {
      LOG_WARN("failed to check restore point exist", K(ret), K_(pkey), K(backup_points));
    } else if (!is_exist) {
      if (OB_FAIL(get_restore_point_tables_(snapshot_ts, schema_version, pg_meta, metas, handle, is_ready, is_need))) {
        LOG_WARN("failed to get restore point sstables", K(ret), K_(pkey), K(snapshot_ts));
      } else if (!is_need) {
        // do nothing
      } else if (!is_ready) {
        if (EXECUTE_COUNT_PER_SEC(1)) {
          LOG_INFO("restore point is not ready", K_(pkey), K(snapshot_ts));
        }
        // do nothing
      } else {
        ObBucketWLockAllGuard bucket_guard(bucket_lock_);
        if (OB_FAIL(recovery_point_data_mgr_.add_backup_point(snapshot_ts, pg_meta, metas, handle))) {
          LOG_WARN("failed to add restore point", K(ret), K(snapshot_ts), K(pg_meta), K(handle));
        } else {
          LOG_INFO("succeed add backup point", K(snapshot_gc_ts), K(pg_meta.pg_key_));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObBucketWLockAllGuard bucket_guard(bucket_lock_);
    if (OB_FAIL(recovery_point_data_mgr_.remove_unneed_backup_point(backup_points, snapshot_gc_ts))) {
      LOG_WARN("failed to remove unneed restore point", K(ret), K(backup_points));
    }
  }

  return ret;
}

int ObPGStorage::get_backup_partition_meta_data(const ObPartitionKey& pkey, const int64_t snapshot_version,
    ObPGPartitionStoreMeta& partition_store_meta, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg storage is not inited", K(ret));
  } else if (!pkey.is_valid() || snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup partition meta data get invalid argument", K(ret), K(pkey), K(snapshot_version));
  } else if (OB_FAIL(recovery_point_data_mgr_.get_backup_partition_meta_data(
                 pkey, snapshot_version, partition_store_meta, handle))) {
    LOG_WARN("failed to get backup meta data", K(ret), K(pkey_), K(pkey));
  }
  return ret;
}

int ObPGStorage::get_restore_point_tables_(const int64_t snapshot_version, const int64_t schema_version,
    ObPartitionGroupMeta& pg_meta, ObIArray<ObPGPartitionStoreMeta>& partition_metas, ObTablesHandle& handle,
    bool& is_ready, bool& is_need)
{
  int ret = OB_SUCCESS;
  bool is_tmp_ready = true;
  int64_t create_schema_version = 0;
  ObPGPartitionStoreMeta meta;
  is_need = false;
  const bool include_trans_table = true;
  ObRecoveryPointSchemaFilter schema_filter;
  const int64_t MAX_BUCKET_SIZE = 1024;
  int64_t real_schema_version = 0;
  uint64_t tenant_id = 0;
  bool is_sys_table = false;
  TCRLockGuard guard(lock_);
  // pg has not done minor merge
  int64_t max_sstable_schema_version = 0;
  int64_t minor_schema_version = meta_->storage_info_.get_data_info().get_schema_version();
  const int64_t publish_version = meta_->storage_info_.get_data_info().get_publish_version();
  if (is_inner_table(pkey_.get_table_id())) {
    tenant_id = OB_SYS_TENANT_ID;
    real_schema_version = minor_schema_version;
    is_sys_table = true;
  } else {
    tenant_id = pkey_.get_tenant_id();
    real_schema_version = schema_version;
    is_sys_table = false;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_meta.deep_copy(*meta_))) {
    STORAGE_LOG(WARN, "failed to copy pg meta", K(ret), K(*meta_));
  } else if (meta_->create_schema_version_ > schema_version && !is_sys_table) {
    is_need = false;
  } else if (publish_version < snapshot_version) {
    is_ready = false;
    STORAGE_LOG(INFO, "restore point is not ready", K(publish_version), K(snapshot_version), K_(pkey));
  } else if (OB_FAIL(get_restore_point_max_schema_version_(
                 publish_version, meta_->partitions_, max_sstable_schema_version))) {
    STORAGE_LOG(WARN, "failed to get restore point max schema version", K(ret), K(publish_version));
  } else if (FALSE_IT(minor_schema_version = minor_schema_version < max_sstable_schema_version
                                                 ? max_sstable_schema_version
                                                 : minor_schema_version)) {
  } else if (OB_FAIL(schema_filter.init(tenant_id, real_schema_version, minor_schema_version))) {
    LOG_WARN("failed to init backup schema checker",
        K(ret),
        K(pkey_),
        K(schema_version),
        K(minor_schema_version),
        K(real_schema_version));
  } else if (OB_FAIL(schema_filter.do_filter_pg_partitions(pg_meta.pg_key_, pg_meta.partitions_))) {
    STORAGE_LOG(WARN, "filter partition in pg meta failed", K(ret), K(pg_meta));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_tmp_ready && i < pg_meta.partitions_.count(); ++i) {
      meta.reset();
      const ObPartitionKey& pkey = pg_meta.partitions_.at(i);
      ObPGPartition* pg_partition = NULL;
      ObPartitionStorage* partition_storage = NULL;
      ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
      if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
      } else if (NULL == (partition_storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "fail to get partition storage", K(ret), KP(partition_storage));
      } else if (OB_FAIL(partition_storage->get_partition_store().get_meta(meta))) {
        STORAGE_LOG(WARN, "failed to get restore point tables", K(ret), K(pkey));
      } else if (OB_FAIL(partition_metas.push_back(meta))) {
        STORAGE_LOG(WARN, "fail to get partition store meta", K(ret), K(pkey), K(meta));
      } else if (OB_FAIL(partition_storage->get_partition_store().get_restore_point_tables(
                     snapshot_version, publish_version, schema_filter, handle, is_tmp_ready))) {
        STORAGE_LOG(WARN, "failed to get restore point tables", K(ret), K(pkey));
      } else {
        is_need = true;
      }
    }

    if (OB_SUCC(ret)) {
      is_ready = is_tmp_ready;
    }

    if (OB_SUCC(ret) && is_ready) {
      ObHashSet<uint64_t> table_id_set;
      if (OB_FAIL(table_id_set.create(MAX_BUCKET_SIZE))) {
        LOG_WARN("failed to create table id set", K(ret), K(MAX_BUCKET_SIZE));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
        const ObITable* table = handle.get_table(i);
        if (OB_FAIL(table_id_set.set_refactored(table->get_key().table_id_))) {
          STORAGE_LOG(WARN, "failed to set table id into set", K(ret), KPC(table));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_filter.check_if_table_miss_by_schema(pg_meta.pg_key_, table_id_set))) {
          STORAGE_LOG(WARN, "failed to check if table miss by schema", K(ret), K(pg_meta));
        }
      }

      if (OB_SUCC(ret)) {
        pg_meta.storage_info_.get_data_info().set_schema_version(minor_schema_version);
      }
    }
  }
  return ret;
}

int ObPGStorage::get_restore_point_max_schema_version_(
    const int64_t publish_version, const ObPartitionArray& partitions, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  ObTablesHandle tables_handle;
  bool pg_only_has_trans_partition = true;

  if (publish_version <= 0 || partitions.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get restore point max schema version get invalid argument", K(ret), K(publish_version), K(partitions));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      tables_handle.reset();
      const ObPartitionKey& pkey = partitions.at(i);
      ObPGPartition* pg_partition = NULL;
      ObPartitionStorage* partition_storage = NULL;
      ObPGPartitionGuard pg_partition_guard(pkey, *(pg_->get_pg_partition_map()));
      if (pkey.is_trans_table()) {
        // do nothing
      } else if (NULL == (pg_partition = pg_partition_guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pg partition info is null, unexpected error", K(ret), K_(pkey));
      } else if (NULL == (partition_storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "fail to get partition storage", K(ret), KP(partition_storage));
      } else if (OB_FAIL(partition_storage->get_partition_store().get_all_tables(tables_handle))) {
        STORAGE_LOG(WARN, "fail to get_effective_tables", K(ret), K(pkey));
      } else {
        pg_only_has_trans_partition = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < tables_handle.get_count(); ++j) {
          ObITable* table = tables_handle.get_table(j);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table is unexpected", K(ret), KPC(table));
          } else if (table->is_memtable() || table->is_trans_sstable()) {
            // do nothing
          } else if (table->get_key().get_snapshot_version() < publish_version) {
            // do nothing
          } else {
            ObSSTable* sstable = static_cast<ObSSTable*>(table);
            schema_version = std::max(schema_version, sstable->get_meta().schema_version_);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (pg_only_has_trans_partition) {
        // do nothing
      } else if (OB_INVALID_VERSION == schema_version) {
        ret = OB_EAGAIN;
        LOG_WARN("failed to find sstable witch snapshot version bigger than publish version",
            K(ret),
            K(publish_version),
            K(partitions));
      } else {
        LOG_INFO("succeed get max schema version", K(publish_version), K(schema_version), K(pkey_));
      }
    }
  }
  return ret;
}

int ObPGStorage::clear_all_memtables()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_memtable_mgr_.clean_new_active_memstore())) {
    STORAGE_LOG(WARN, "clean new active memstore failed", K(pkey_), K(ret));
  } else {
    pg_memtable_mgr_.clean_memtables();
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
