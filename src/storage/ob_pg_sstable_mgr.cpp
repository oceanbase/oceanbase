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

#include "storage/ob_pg_sstable_mgr.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/ob_partition_log.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

int ObPGSSTableMgr::TableGetFunctor::operator()(ObITable& table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_handle_.set_table(&table))) {
    LOG_WARN("fail to set table to handle", K(ret));
  }
  return ret;
}

int ObPGSSTableMgr::UnusedTableFunctor::init(const int64_t max_recycle_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObUnusedTableFunctor has not been inited", K(ret));
  } else {
    max_recycle_cnt_ = max_recycle_cnt;
    is_inited_ = true;
  }
  return ret;
}

int ObPGSSTableMgr::UnusedTableFunctor::operator()(ObITable& table, bool& is_full)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (0 == table.get_ref()) {
    if (OB_FAIL(tables_.push_back(&table))) {
      LOG_WARN("fail to push back table", K(ret));
    } else {
      is_full = tables_.count() >= max_recycle_cnt_;
    }
  }
  return ret;
}

int ObPGSSTableMgr::UnusedTableFunctor::get_tables(common::ObIArray<ObITable*>& tables)
{
  int ret = OB_SUCCESS;
  tables.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
      ObITable* table = tables_.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, table must not be null", K(ret));
      } else if (0 == table->get_ref()) {
        if (OB_FAIL(tables.push_back(table))) {
          LOG_WARN("fail to push back table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPGSSTableMgr::CheckAllTableUnusedFunctor::operator()(ObITable& table, bool& is_full)
{
  int ret = OB_SUCCESS;
  if (is_all_unused_) {
    is_all_unused_ = 0 == table.get_ref();
  }
  is_full = is_all_unused_;
  return ret;
}

ObPGSSTableMgr::ObPGSSTableMgr()
    : is_inited_(false), pg_key_(), bucket_lock_(), table_map_(), file_handle_(), enable_write_log_(false)
{}

ObPGSSTableMgr::~ObPGSSTableMgr()
{
  destroy();
}

void ObPGSSTableMgr::destroy()
{
  if (is_inited_) {
    {
      ObBucketWLockAllGuard guard(bucket_lock_);
      int tmp_ret = OB_SUCCESS;
      UnusedTableFunctor finder;
      ObArray<ObITable*> tables;
      if (OB_SUCCESS != (tmp_ret = finder.init(INT64_MAX))) {
        LOG_WARN("fail to init table map", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = table_map_.foreach (finder))) {
        LOG_WARN("failed to get all tables", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = finder.get_tables(tables))) {
        LOG_WARN("failed to get tables", K(tmp_ret));
      } else {
        for (int64_t i = 0; i < tables.count(); ++i) {
          ObSSTable* sstable = static_cast<ObSSTable*>(tables.at(i));
          free_sstable(sstable);
        }
      }
      table_map_.destroy();
    }
    bucket_lock_.destroy();
    pg_key_.reset();
    file_handle_.reset();
    enable_write_log_ = false;
    is_inited_ = false;
  }
}

int ObPGSSTableMgr::init(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGSSTableMgr has been inited twice", K(ret));
  } else if (OB_FAIL(
                 bucket_lock_.init(DEFAULT_HASH_BUCKET_COUNT, ObLatchIds::TABLE_MGR_MAP, ObModIds::OB_TABLE_MGR_MAP))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else if (OB_FAIL(
                 table_map_.init(DEFAULT_HASH_BUCKET_COUNT, ObLatchIds::TABLE_MGR_MAP, ObModIds::OB_TABLE_MGR_MAP))) {
    LOG_WARN("fail to init table map", K(ret));
  } else {
    pg_key_ = pg_key;
    is_inited_ = true;
  }
  return ret;
}

int ObPGSSTableMgr::create_sstable(storage::ObCreateSSTableParamWithTable& param, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    ObCreatePartitionMeta schema;
    ObCreateSSTableParamWithPartition p_param;
    if (OB_FAIL(p_param.extract_from(param, schema))) {
      STORAGE_LOG(WARN, "failed to extract partition param", K(param), K(ret));
    } else {
      ret = create_sstable(p_param, handle);
    }
  }
  return ret;
}

int ObPGSSTableMgr::create_sstable(storage::ObCreateSSTableParamWithPartition& param, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObSSTable* sstable = nullptr;
  handle.reset();
  ObBucketHashRLockGuard lock_guard(bucket_lock_, param.table_key_.hash());
  param.pg_key_ = pg_key_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !ObITable::is_sstable(param.table_key_.table_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_HASH_NOT_EXIST != (hash_ret = table_map_.exist(param.table_key_))) {
    if (OB_HASH_EXIST == hash_ret) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("sstable already exist", K(ret), K(param.table_key_));
    } else {
      ret = hash_ret;
      LOG_WARN("fail to check sstable exist", K(ret), K(param.table_key_));
    }
  } else if (OB_FAIL(alloc_sstable(param.table_key_.get_tenant_id(), sstable))) {
    LOG_WARN("fail to alloc table", K(ret), K(param));
  } else if (OB_FAIL(sstable->init(param.table_key_))) {
    LOG_WARN("fail to init sstable", K(ret), K(param));
  } else if (OB_FAIL(sstable->open(param))) {
    LOG_WARN("fail to open sstable", K(ret), K(param));
  } else if (OB_FAIL(sstable->set_storage_file_handle(file_handle_))) {
    LOG_WARN("fail to set storage file handle", K(ret));
  } else if (OB_FAIL(handle.set_table(sstable))) {
    LOG_WARN("fail to set table", K(ret));
  }

  if (OB_FAIL(ret) && nullptr != sstable) {
    free_sstable(sstable);
    sstable = nullptr;
    handle.reset();
  }
  return ret;
}

int ObPGSSTableMgr::create_sstable(
    const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& meta, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  handle.reset();
  ObSSTable* sstable = nullptr;
  meta.pg_key_ = pg_key_;
  ObBucketHashRLockGuard lock_guard(bucket_lock_, table_key.hash());
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || !meta.is_valid() || !ObITable::is_sstable(table_key.table_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key), K(meta));
  } else if (OB_HASH_NOT_EXIST != (hash_ret = table_map_.exist(table_key))) {
    if (OB_HASH_EXIST == hash_ret) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("sstable already exist", K(ret), K(table_key));
    } else {
      ret = hash_ret;
      LOG_WARN("fail to check sstable exist", K(ret), K(table_key));
    }
  } else if (OB_FAIL(alloc_sstable(table_key.get_tenant_id(), sstable))) {
    LOG_WARN("fail to alloc table", K(ret), K(table_key));
  } else if (OB_FAIL(sstable->init(table_key))) {
    LOG_WARN("fail to init sstable", K(ret), K(table_key));
  } else if (OB_FAIL(sstable->open(meta))) {
    LOG_WARN("fail to open sstable", K(ret), K(meta));
  } else if (OB_FAIL(sstable->set_storage_file_handle(file_handle_))) {
    LOG_WARN("fail to set storage file handle", K(ret));
  } else if (OB_FAIL(handle.set_table(sstable))) {
    LOG_WARN("fail to set table", K(ret));
  }

  if (OB_FAIL(ret) && nullptr != sstable) {
    free_sstable(sstable);
    sstable = nullptr;
    handle.reset();
  }
  return ret;
}

void ObPGSSTableMgr::free_sstable(ObSSTable* sstable)
{
  if (nullptr != sstable) {
    OB_DELETE_ALIGN32(ObSSTable, unused, sstable);
  }
}

int ObPGSSTableMgr::acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& table_handle)
{
  int ret = OB_SUCCESS;
  TableGetFunctor get_functor(table_handle);
  ObBucketHashRLockGuard lock_guard(bucket_lock_, table_key.hash());
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || !ObITable::is_sstable(table_key.table_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key));
  } else if (OB_FAIL(table_map_.get(table_key, get_functor))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get table", K(ret), K(table_key));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObPGSSTableMgr::get_all_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObITable* table = nullptr;
  const int64_t lock_start_time = ObTimeUtility::current_time();
  TableMap::Iterator iter(table_map_);
  const int64_t lock_end_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_FAIL(handle.reserve(table_map_.get_count()))) {
    LOG_WARN("fail to reserve tables", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next table", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, table must not be null", K(ret));
      } else if (OB_FAIL(handle.add_table(table))) {
        LOG_WARN("fail to add table", K(ret));
      }
    }
  }
  const int64_t end_time = ObTimeUtility::current_time();
  const int64_t lock_time = lock_end_time - lock_start_time;
  const int64_t get_time = end_time - lock_end_time;
  FLOG_INFO("get_all_sstables cost time",
      K_(pg_key),
      K(lock_time),
      K(get_time),
      "total_cost_time",
      lock_time + get_time,
      "total_cnt",
      handle.get_count());
  return ret;
}

int ObPGSSTableMgr::write_add_sstable_log(ObSSTable& sstable)
{
  int ret = OB_SUCCESS;
  ObStorageFile* file = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (!enable_write_log_) {
    ret = OB_ERR_SYS;
    LOG_WARN("can not write slog during replay", K(ret));
  } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("data file ptr is null", K(ret), KP(file));
  } else {
    ObFullMacroBlockMeta full_meta;
    const ObSSTable::ObSSTableGroupMacroBlocks& macro_blocks = sstable.get_total_macro_blocks();
    int64_t subcmd = 0;
    ObStorageLogAttribute log_attr(pg_key_.get_tenant_id(), file->get_file_id());
    // write sstable data macro blocks and lob macro blocks
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_blocks.count(); ++i) {
      const MacroBlockId& block_id = macro_blocks.at(i);
      if (OB_FAIL(sstable.get_meta(block_id, full_meta))) {
        LOG_WARN("fail to get macro block meta", K(ret), K(block_id));
      } else if (!full_meta.is_valid()) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, macro block meta must not be null", K(ret), K(block_id));
      } else if (ObMacroBlockCommonHeader::SSTableData != full_meta.meta_->attr_ &&
                 ObMacroBlockCommonHeader::LobIndex != full_meta.meta_->attr_ &&
                 ObMacroBlockCommonHeader::LobData != full_meta.meta_->attr_) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, macro block type is invalid", K(ret), K(full_meta));
      } else {
        ObPGMacroBlockMetaLogEntry meta_log_entry(pg_key_,
            sstable.get_key(),
            0 /*data file id*/,
            block_id.disk_no(),
            block_id,
            const_cast<ObMacroBlockMetaV2&>(*full_meta.meta_));
        subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_CHANGE_MACRO_META);
        if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, meta_log_entry))) {
          LOG_WARN("fail to write macro block meta slog", K(ret));
        }
      }
    }

    // write bloomfilter macro block meta log
    if (OB_SUCC(ret) && sstable.has_bloom_filter_macro_block()) {
      const MacroBlockId& block_id = sstable.get_bloom_filter_block_id();
      if (OB_FAIL(sstable.get_meta(block_id, full_meta))) {
        LOG_WARN("fail to get macro block meta", K(ret), K(block_id));
      } else if (!full_meta.is_valid()) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, macro block meta must not be null", K(ret), K(block_id));
      } else if (ObMacroBlockCommonHeader::BloomFilterData != full_meta.meta_->attr_) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, macro block type is invalid", K(ret), K(full_meta));
      } else {
        ObPGMacroBlockMetaLogEntry meta_log_entry(pg_key_,
            sstable.get_key(),
            0 /*data file id*/,
            block_id.disk_no(),
            block_id,
            const_cast<ObMacroBlockMetaV2&>(*full_meta.meta_));
        subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_CHANGE_MACRO_META);
        if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, meta_log_entry))) {
          LOG_WARN("fail to write log", K(ret));
        }
      }
    }

    // write add sstable log
    if (OB_SUCC(ret)) {
      ObAddSSTableLogEntry sstable_log_entry(pg_key_, sstable);
      int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_ADD_SSTABLE);
      if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, sstable_log_entry))) {
        LOG_WARN("fail to write sstable log", K(ret));
      } else {
        FLOG_INFO("write add sstable log", K(pg_key_), K(sstable));
      }
    }
  }
  return ret;
}

int ObPGSSTableMgr::write_remove_sstable_log(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  ObStorageFile* file = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (!enable_write_log_) {
    ret = OB_ERR_SYS;
    LOG_WARN("can not write slog during replay", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || !ObITable::is_sstable(table_key.table_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key));
  } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("data file ptr is null", K(ret), KP(file));
  } else {
    ObRemoveSSTableLogEntry sstable_log_entry(pg_key_, table_key);
    ObStorageLogAttribute log_attr(pg_key_.get_tenant_id(), file->get_file_id());
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_REMOVE_SSTABLE);
    if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, sstable_log_entry))) {
      LOG_WARN("fail to write sstable log", K(ret));
    } else {
      FLOG_INFO("write remove sstable log", K(pg_key_), K(table_key));
    }
  }
  return ret;
}

int ObPGSSTableMgr::check_can_add_sstables(ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else {
    int hash_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      ObITable* table = tables_handle.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table must not be null", K(ret));
      } else if (!table->is_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), K(*table));
      } else if (OB_HASH_NOT_EXIST != (hash_ret = table_map_.exist(table->get_key()))) {
        if (OB_HASH_EXIST == hash_ret) {
          ret = OB_ENTRY_EXIST;
          LOG_WARN("sstable already exist in table map", K(ret), K(table->get_key()));
        } else {
          ret = hash_ret;
          LOG_WARN("fail to get from table map", K(ret), K(table->get_key()));
        }
      }
    }
  }
  return ret;
}

int ObPGSSTableMgr::add_sstables(const bool in_slog_trans, ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  ObArray<TableNode*> node_array;
  TableNode* table_node = nullptr;
  const bool is_write_lock = true;
  int64_t lsn = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage file handle is invalid", K(ret), K(file_handle_));
  } else if (OB_FAIL(file_handle_.get_storage_file()->fsync())) {
    LOG_WARN("fail to fsync", K(ret), "file", *(file_handle_.get_storage_file()));
  }

  ObMultiBucketLockGuard lock_guard(bucket_lock_, is_write_lock);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lock_multi_bucket(lock_guard, tables_handle.get_tables()))) {
    LOG_WARN("fail to lock multi bucket", K(ret));
  } else if (OB_FAIL(check_can_add_sstables(tables_handle))) {
    LOG_WARN("fail to check can add sstables", K(ret));
  } else if (OB_FAIL(node_array.reserve(tables_handle.get_count()))) {
    LOG_WARN("fail to reverse node array", K(ret));
  } else if (!in_slog_trans && OB_FAIL(SLOGGER.begin(OB_LOG_TABLE_MGR))) {
    LOG_WARN("fail to begin slog trans", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      ObSSTable* sstable = static_cast<ObSSTable*>(tables_handle.get_table(i));
      if (OB_ISNULL(sstable)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret));
      } else if (OB_FAIL(write_add_sstable_log(*sstable))) {
        LOG_WARN("fail to write sstable log", K(ret));
      } else if (OB_ISNULL(table_node = table_map_.alloc_node(*sstable))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for table node", K(ret));
      } else if (OB_FAIL(node_array.push_back(table_node))) {
        LOG_WARN("fail to push back table node", K(ret));
        table_map_.free_node(table_node);
      }
    }

    if (!in_slog_trans) {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(SLOGGER.commit(lsn))) {
          LOG_WARN("fail to commit slog", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          LOG_WARN("fail to abort slog", K(tmp_ret));
        } else {
          LOG_WARN("fail to add sstables", K(ret), K(lsn), K(tables_handle));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < node_array.count(); ++i) {
        if (OB_FAIL(table_map_.put(*node_array.at(i)))) {
          LOG_WARN("fail to put table map", K(ret));
          ob_abort();
        }
      }
      FLOG_INFO("finish add sstables", K(ret), K(lsn), K(tables_handle));
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < node_array.count(); ++i) {
      table_map_.free_node(node_array.at(i));
    }
  }
  return ret;
}

int ObPGSSTableMgr::add_sstable(const bool in_slog_trans, ObTableHandle& table_handle)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_FAIL(tables_handle.add_table(table_handle))) {
    LOG_WARN("fail to add table", K(ret), K(table_handle));
  } else if (OB_FAIL(add_sstables(in_slog_trans, tables_handle))) {
    LOG_WARN("fail to add sstables", K(ret));
  }
  return ret;
}

int ObPGSSTableMgr::lock_multi_bucket(common::ObMultiBucketLockGuard& guard, common::ObIArray<ObITable*>& tables)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_hash_array;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (0 == tables.count()) {
    // do nothing
  } else if (OB_FAIL(table_hash_array.reserve(tables.count()))) {
    LOG_WARN("fail to reserve table hash array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      ObITable* table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (OB_FAIL(table_hash_array.push_back(table->get_key().hash()))) {
        LOG_WARN("fail to push back table hash array", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(guard.lock_multi_buckets(table_hash_array))) {
        LOG_WARN("fail to lock multi buckets", K(ret));
      }
    }
  }
  return ret;
}

int ObPGSSTableMgr::recycle_unused_sstables(const int64_t max_recycle_cnt, int64_t& recycled_cnt)
{
  int ret = OB_SUCCESS;
  UnusedTableFunctor table_functor;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (!enable_write_log_) {
    LOG_INFO("do not recycle unused sstable now");
  } else if (OB_UNLIKELY(max_recycle_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(max_recycle_cnt));
  } else if (OB_FAIL(table_functor.init(max_recycle_cnt))) {
    LOG_WARN("fail to init table functor", K(ret));
  } else if (OB_FAIL(table_map_.foreach (table_functor))) {
    LOG_WARN("fail to foreach table map", K(ret));
  } else {
    ObArray<ObITable*> unused_table_array;
    const bool is_write_lock = true;
    ObMultiBucketLockGuard lock_guard(bucket_lock_, is_write_lock);
    if (OB_FAIL(lock_multi_bucket(lock_guard, table_functor.get_all_tables()))) {
      LOG_WARN("fail to lock multi bucket", K(ret));
    } else if (OB_FAIL(table_functor.get_tables(unused_table_array))) {
      LOG_WARN("fail to get unused table array", K(ret));
    } else if (0 == unused_table_array.count()) {
      // no unused table, skip
    } else if (OB_FAIL(SLOGGER.begin(OB_LOG_TABLE_MGR))) {
      LOG_WARN("fail to begin transaction", K(ret));
    } else {
      recycled_cnt = unused_table_array.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < unused_table_array.count(); ++i) {
        ObITable* table = unused_table_array.at(i);
        if (OB_ISNULL(table)) {
          ret = OB_ERR_SYS;
          LOG_WARN("error sys, table must not be null", K(ret));
        } else if (OB_FAIL(write_remove_sstable_log(table->get_key()))) {
          LOG_WARN("fail to write remove sstable log", K(ret), K(table->get_key()));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t lsn = 0;
        if (OB_FAIL(SLOGGER.commit(lsn))) {
          LOG_WARN("fail to commit slog", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < unused_table_array.count(); ++i) {
          ObITable* table = unused_table_array.at(i);
          if (OB_FAIL(table_map_.erase(table->get_key()))) {
            LOG_WARN("fail to erase from table map", K(ret), K(table->get_key()));
            ob_abort();
          } else {
            free_sstable(static_cast<ObSSTable*>(table));
          }
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          LOG_WARN("fail to abort slog", K(ret));
        }
      }
    }
  }
  return ret;
}

int64_t ObPGSSTableMgr::get_serialize_size()
{
  int ret = OB_SUCCESS;
  int64_t serialize_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else {
    TableMap::Iterator iter(table_map_);
    const int64_t table_cnt = table_map_.get_count();
    serialize_size += serialization::encoded_length_i64(table_cnt);
    ObITable* table = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next table", K(ret));
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, table must not be null", K(ret));
      } else {
        ObSSTable* sstable = static_cast<ObSSTable*>(table);
        serialize_size += sstable->get_serialize_size();
      }
    }
  }
  if (OB_FAIL(ret)) {
    serialize_size = 0;
  }
  return serialize_size;
}

int ObPGSSTableMgr::serialize(
    common::ObIAllocator& allocator, char*& buf, int64_t& serialize_size, ObTablesHandle* tables_handle)
{
  int ret = OB_SUCCESS;
  ObBucketTryRLockAllGuard guard(bucket_lock_);
  int64_t request_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("fail to get read lock", K(ret));
  } else if (FALSE_IT(request_size = get_serialize_size())) {
  } else if (request_size <= 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, serialize size must not be zero", K(request_size));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(request_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(request_size));
  } else if (OB_FAIL(serialization::encode_i64(buf, request_size, serialize_size, table_map_.get_count()))) {
    LOG_WARN("fail to encode table map size", K(ret));
  } else {
    TableMap::Iterator iter(table_map_);
    ObITable* table = nullptr;
    ObSSTable* sstable = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next table", K(ret));
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, table must not be null", K(ret));
      } else {
        sstable = static_cast<ObSSTable*>(table);
        if (OB_FAIL(sstable->serialize(buf, request_size, serialize_size))) {
          LOG_WARN("fail to serialize sstable", K(ret));
        } else if (nullptr != tables_handle) {
          if (OB_FAIL(tables_handle->add_table(sstable))) {
            LOG_WARN("fail to add sstable", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to serialize sstable mgr", K(pg_key_));
  }
  return ret;
}

int ObPGSSTableMgr::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t table_cnt = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &table_cnt))) {
    LOG_WARN("fail to decode table cnt", K(ret));
  } else if (OB_UNLIKELY(!file_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage file handle is invalid", K(ret), K(file_handle_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_cnt; ++i) {
      ObSSTable sstable;
      if (OB_FAIL(sstable.set_storage_file_handle(file_handle_))) {
        LOG_WARN("fail to set storage file handle", K(ret), K(file_handle_));
      } else if (OB_FAIL(sstable.deserialize(buf, buf_len, pos))) {
        LOG_WARN("fail to deserialize sstable", K(ret));
      } else if (OB_FAIL(add_sstable_to_map(sstable, false /*overwrite*/))) {
        LOG_WARN("fail to add replay sstable", K(ret));
      } else {
        LOG_INFO("replay add sstable to map", K(pg_key_), K(sstable));
      }
    }
  }

  FLOG_INFO("deserialize pg sstable mgr", K(ret), K(table_cnt), K(pg_key_));
  return ret;
}

int ObPGSSTableMgr::add_sstable_to_map(ObSSTable& sstable, const bool overwrite)
{
  int ret = OB_SUCCESS;
  ObSSTable* new_sstable = nullptr;
  TableNode* table_node = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sstable));
  } else if (OB_FAIL(alloc_sstable(sstable.get_key().get_tenant_id(), new_sstable))) {
    LOG_WARN("fail to alloc sstable", K(ret));
  } else if (OB_FAIL(new_sstable->set_sstable(sstable))) {
    LOG_WARN("fail to set sstable", K(ret));
  } else if (OB_UNLIKELY(!new_sstable->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, sstable must not be null", K(ret));
  } else if (OB_ISNULL(table_node = table_map_.alloc_node(*new_sstable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(table_map_.put(*table_node))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("fail to put node", K(ret), K(overwrite));
    } else if (overwrite) {
      const ObITable::TableKey& delete_key = table_node->get_key();
      ObITable* delete_table = nullptr;
      if (OB_FAIL(table_map_.erase(delete_key, delete_table))) {
        LOG_WARN("erase table fail", K(ret), K(delete_key));
      } else if (OB_FAIL(table_map_.put(*table_node))) {
        LOG_WARN("put table fail", K(ret), K(delete_key), K(overwrite));
      } else {
        free_sstable(static_cast<ObSSTable*>(delete_table));
        delete_table = nullptr;
        table_node = nullptr;
        new_sstable = nullptr;
      }
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    table_node = nullptr;
    new_sstable = nullptr;
  }

  if (nullptr != table_node) {
    table_map_.free_node(table_node);
    table_node = nullptr;
  }
  if (nullptr != new_sstable) {
    free_sstable(new_sstable);
    new_sstable = nullptr;
  }
  return ret;
}

int ObPGSSTableMgr::replay_add_sstable(ObSSTable& sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(sstable));
  } else if (OB_FAIL(add_sstable_to_map(sstable, true /*overwrite*/))) {
    LOG_WARN("fail to add sstable to map", K(ret));
  }
  return ret;
}

int ObPGSSTableMgr::replay_remove_sstable(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  ObITable* table = nullptr;
  ObSSTable* sstable = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key));
  } else if (OB_FAIL(table_map_.erase(table_key, table))) {
    if (OB_HASH_NOT_EXIST == ret) {
      FLOG_INFO("erase not exist sstable when replay remove sstable", K(table_key));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase from map", K(ret), K(table_key));
    }
  } else {
    sstable = static_cast<ObSSTable*>(table);
    free_sstable(sstable);
    sstable = nullptr;
  }
  return ret;
}

int ObPGSSTableMgr::alloc_sstable(const int64_t tenant_id, ObSSTable*& sstable)
{
  int ret = OB_SUCCESS;
  sstable = NULL;
  ObMemAttr memattr(tenant_id);
  memattr.label_ = ObModIds::OB_SSTABLE;
  memattr.ctx_id_ = ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID;

  if (OB_ISNULL(sstable = OB_NEW_ALIGN32(ObSSTable, memattr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc table", K(ret), K(memattr), K(tenant_id));
  }
  return ret;
}

int ObPGSSTableMgr::replay_add_old_sstable(ObOldSSTable& sstable)
{
  int ret = OB_SUCCESS;
  ObSSTable* new_sstable = nullptr;
  TableNode* table_node = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sstable));
  } else if (OB_FAIL(alloc_sstable(sstable.get_key().get_tenant_id(), new_sstable))) {
    LOG_WARN("fail to alloc sstable", K(ret));
  } else if (OB_FAIL(new_sstable->set_storage_file_handle(file_handle_))) {
    LOG_WARN("fail to set pg file", K(ret));
  } else if (OB_FAIL(new_sstable->convert_from_old_sstable(sstable))) {
    LOG_WARN("fail to set sstable", K(ret));
  } else if (OB_UNLIKELY(!new_sstable->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, sstable must not be null", K(ret));
  } else if (OB_ISNULL(table_node = table_map_.alloc_node(*new_sstable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(table_map_.put(*table_node))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("fail to put node", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != table_node) {
      table_map_.free_node(table_node);
      table_node = nullptr;
    }
    if (nullptr != new_sstable) {
      free_sstable(new_sstable);
      new_sstable = nullptr;
    }
  }
  return ret;
}

int ObPGSSTableMgr::check_all_sstable_unused(bool& all_unused)
{
  int ret = OB_SUCCESS;
  CheckAllTableUnusedFunctor functor;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else if (OB_FAIL(table_map_.foreach (functor))) {
    LOG_WARN("fail to foreach table map", K(ret));
  } else {
    all_unused = functor.is_all_unused();
  }
  return ret;
}

int ObPGSSTableMgr::enable_write_log()
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard guard(bucket_lock_);
  LOG_INFO("enable write pg sstable table mgr log");
  enable_write_log_ = true;
  return ret;
}

int ObPGSSTableMgr::GetCleanOutLogIdFunctor::operator()(ObITable& table, bool& is_full)
{
  int ret = OB_SUCCESS;
  is_full = false;
  if (table.get_ref() > 0 && table.is_multi_version_minor_sstable() && table.get_end_log_ts() < clean_out_log_ts_) {
    clean_out_log_ts_ = table.get_end_log_ts();
  }
  return ret;
}

int ObPGSSTableMgr::get_clean_out_log_ts(int64_t& clean_out_log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableMgr has not been inited", K(ret));
  } else {
    GetCleanOutLogIdFunctor functor;
    if (OB_FAIL(table_map_.foreach (functor))) {
      LOG_WARN("fail to foreach table map", K(ret));
    } else {
      clean_out_log_ts = functor.get_clean_out_log_ts();
    }
  }
  return ret;
}
