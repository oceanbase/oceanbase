/**
 * Copyright (c) 2025 OceanBase
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
#include "storage/direct_load/ob_inc_major_ddl_aggregate_sstable.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace compaction;
using namespace transaction;

namespace storage
{
/*------ ObIncMajorDDLAggregateSSTableEmptyRowIterator ------*/
ObIncMajorDDLAggregateSSTableEmptyRowIterator::ObIncMajorDDLAggregateSSTableEmptyRowIterator()
{
}

ObIncMajorDDLAggregateSSTableEmptyRowIterator::~ObIncMajorDDLAggregateSSTableEmptyRowIterator()
{
}

int ObIncMajorDDLAggregateSSTableEmptyRowIterator::get_next_row(const ObDatumRow *&row)
{
  return OB_ITER_END;
}


/*------ ObIncMajorDDLAggregateCGSSTable ------*/
ObIncMajorDDLAggregateCGSSTable::ObIncMajorDDLAggregateCGSSTable()
 : cg_idx_(OB_INVALID_ID),
   rowkey_cg_sstable_(nullptr),
   is_inited_(false)
{
}

ObIncMajorDDLAggregateCGSSTable::~ObIncMajorDDLAggregateCGSSTable()
{
  reset();
}

void ObIncMajorDDLAggregateCGSSTable::reset()
{
  is_inited_ = false;
  index_tree_root_.reset();
  meta_handle_.reset();
  sstable_wrappers_.reset();
  rowkey_cg_sstable_ = nullptr;
  cg_idx_ = OB_INVALID_ID;
}

int ObIncMajorDDLAggregateCGSSTable::init(
    ObArenaAllocator &allocator,
    const ObITable::TableKey &table_key,
    ObSSTable &base_table,
    ObSSTableMetaHandle &meta_handle,
    const int64_t column_group_cnt,
    const int64_t rowkey_column_cnt,
    const int64_t column_cnt,
    const int64_t row_cnt,
    const int64_t cg_idx)
{
  int ret = OB_SUCCESS;
  ObTabletCreateSSTableParam sstable_param;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || !meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_key),
                                 K(meta_handle));
  } else if (OB_FAIL(sstable_param.init_for_inc_major_ddl_aggregate(
                                                      table_key,
                                                      base_table,
                                                      meta_handle,
                                                      column_group_cnt,
                                                      rowkey_column_cnt,
                                                      column_cnt,
                                                      row_cnt))) {
    LOG_WARN("fail to init sstable param", KR(ret), K(table_key),
                                           K(base_table), K(meta_handle),
                                           K(column_group_cnt),
                                           K(column_cnt), K(row_cnt));
  } else if (OB_FAIL(ObSSTable::init(sstable_param, &allocator))) {
    LOG_WARN("fail to init sstable", KR(ret), K(sstable_param));
  } else {
    cg_idx_ = cg_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObIncMajorDDLAggregateCGSSTable::add_table(
    const int64_t cg_idx,
    ObITable *table)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(cg_idx != cg_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cg idx", KR(ret), K(cg_idx_), K(cg_idx));
  } else if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is nullptr", KR(ret));
  } else {
    if (table->is_inc_major_ddl_dump_sstable()) {
      ObSSTable *sstable = static_cast<ObSSTable *>(table);
      if (OB_FAIL(ddl_sstables_.push_back(sstable))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    } else if (table->is_inc_major_ddl_mem_sstable()) {
      ObDDLMemtable *ddl_memtable = static_cast<ObDDLMemtable *>(table);
      if (OB_FAIL(ddl_memtables_.push_back(ddl_memtable))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sstable type", KR(ret), KPC(table));
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCGSSTable::add_sstable_wrapper(ObSSTableWrapper &sstable_wrapper)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!sstable_wrapper.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable wrapper is invalid", KR(ret));
  } else if (OB_FAIL(sstable_wrappers_.push_back(sstable_wrapper))) {
    LOG_WARN("fail to push back sstable wrapper", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCGSSTable::init_index_tree_root()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(index_tree_root_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index tree root should invalid before init", KR(ret), K(index_tree_root_));
  } else if (OB_UNLIKELY(ddl_sstables_.count() <= 0 && ddl_memtables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ddl sstables count or ddl memtables count",
             KR(ret), K(ddl_sstables_), K(ddl_memtables_));
  } else if (ddl_sstables_.count() > 0) {
    ObSSTableMetaHandle meta_handle;
    if (OB_FAIL(find_sstable_meta_with_valid_root_block(ddl_sstables_, meta_handle))) {
      LOG_WARN("failed to find sstable meta with valid root block", KR(ret), K_(ddl_sstables));
    } else if (meta_handle.is_valid()) {
      meta_handle_ = meta_handle;
      index_tree_root_ = meta_handle.get_sstable_meta().get_root_info().get_block_data();
    }
  }

  if (OB_SUCC(ret) && !index_tree_root_.is_valid() && (ddl_memtables_.count() > 0)) {
    ObSSTableMetaHandle meta_handle;
    if (OB_FAIL(find_sstable_meta_with_valid_root_block(ddl_memtables_, meta_handle))) {
      LOG_WARN("failed to find sstable meta with valid root block", KR(ret), K_(ddl_memtables));
    } else if (meta_handle.is_valid()) {
      meta_handle_ = meta_handle;
      index_tree_root_ = meta_handle.get_sstable_meta().get_root_info().get_block_data();
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!meta_handle_.is_valid() || !index_tree_root_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid meta_handle or index_tree_root",
        KR(ret), K_(meta_handle), K_(index_tree_root));
  }
  return ret;
}

template<typename T>
int ObIncMajorDDLAggregateCGSSTable::find_sstable_meta_with_valid_root_block(
    const ObIArray<T *> &sstables,
    ObSSTableMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < sstables.count()); ++i) {
    ObSSTableMetaHandle tmp_meta_handle;
    ObSSTable *sstable = sstables.at(i);
    if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is nullptr", KR(ret));
    } else if (OB_FAIL(sstable->get_meta(tmp_meta_handle))) {
      LOG_WARN("fail to get sstable meta", KR(ret));
    } else if (OB_UNLIKELY(!tmp_meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle is invalid", KR(ret), K(tmp_meta_handle));
    } else {
      const ObMicroBlockData &micro_block_data = tmp_meta_handle.get_sstable_meta().get_root_info().get_block_data();
      if (micro_block_data.is_valid()) {
        meta_handle = tmp_meta_handle;
        break;
      }
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCGSSTable::get_sstable(const ObSSTable *&sstable) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(ddl_sstables_.count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable count should be 0 or 1 when column store need rescan", KR(ret), K(ddl_sstables_));
  } else if (1 == ddl_sstables_.count()) {
    sstable = ddl_sstables_.at(0);
  }
  return ret;
}

int ObIncMajorDDLAggregateCGSSTable::get_sstables(ObIArray<ObSSTable *> &ddl_sstables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (FALSE_IT(ddl_sstables.reset())) {
  } else if (OB_FAIL(ddl_sstables.assign(ddl_sstables_))) {
    LOG_WARN("fail to assign", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCGSSTable::get_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (FALSE_IT(ddl_memtables.reset())) {
  } else if (OB_FAIL(ddl_memtables.assign(ddl_memtables_))) {
    LOG_WARN("fail to assign ddl memtables", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCGSSTable::get_rowkey_sstables(ObIArray<ObSSTable *> &ddl_sstables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(rowkey_cg_sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(rowkey_cg_sstable_->get_sstables(ddl_sstables))) {
    LOG_WARN("fail to get rowkey sstables", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCGSSTable::get_rowkey_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(rowkey_cg_sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(rowkey_cg_sstable_->get_memtables(ddl_memtables))) {
    LOG_WARN("fail to get rowkey memtables", KR(ret));
  }
  return ret;
}
int ObIncMajorDDLAggregateCGSSTable::set_rowkey_cg_sstable(
    const ObIncMajorDDLAggregateCGSSTable *rowkey_cg_sstable)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_ISNULL(rowkey_cg_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg sstable is nullptr", KR(ret));
  } else if (OB_UNLIKELY(0 != rowkey_cg_sstable->get_cg_idx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cg idx, rowkey cg idx should be 0",
             KR(ret), K(rowkey_cg_sstable->get_cg_idx()));
  } else {
    rowkey_cg_sstable_ = rowkey_cg_sstable;
  }
  return ret;
}

int64_t ObIncMajorDDLAggregateCGSSTable::get_cg_idx() const
{
  return cg_idx_;
}

int ObIncMajorDDLAggregateCGSSTable::get_index_tree_root(
    blocksstable::ObMicroBlockData &index_data,
    const bool need_transform)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    index_data = index_tree_root_;
    index_data.type_ = ObMicroBlockData::DDL_MERGE_INDEX_BLOCK;
  }
  return ret;
}

bool ObIncMajorDDLAggregateCGSSTable::is_empty() const
{
  return false;
}

/*------ ObIncMajorDDLAggregateCOSSTable ------*/
ObIncMajorDDLAggregateCOSSTable::ObIncMajorDDLAggregateCOSSTable()
 : allocator_("INC_MAJOR"),
   tablet_id_(),
   base_sstable_(nullptr),
   column_group_cnt_(0),
   rowkey_column_cnt_(0),
   column_cnt_(0),
   row_cnt_(0),
   tx_info_(),
   rowkey_cg_sstable_(nullptr),
   is_inited_(false)
{
}

ObIncMajorDDLAggregateCOSSTable::~ObIncMajorDDLAggregateCOSSTable()
{
  destroy();
}

void ObIncMajorDDLAggregateCOSSTable::destroy()
{
  ObCOSSTableV2::reset();
  is_inited_ = false;
  co_meta_handles_.reset();
  rowkey_cg_sstable_ = nullptr;
  FOREACH(it, cg_sstables_) {
    ObIncMajorDDLAggregateCGSSTable *cg_sstable = it->second;
    if (OB_NOT_NULL(cg_sstable)) {
      cg_sstable->~ObIncMajorDDLAggregateCGSSTable();
      allocator_.free(cg_sstable);
      cg_sstable = nullptr;
    }
  }
  tx_info_.reset();
  row_cnt_ = 0;
  column_cnt_ = 0;
  rowkey_column_cnt_ = 0;
  column_group_cnt_ = 0;
  base_sstable_meta_.reset();
  base_sstable_ = nullptr;
  tablet_id_.reset();
  allocator_.reset();
}

void ObIncMajorDDLAggregateCOSSTable::TxInfo::reset()
{
  trans_id_.reset();
  seq_no_.reset();
}

int ObIncMajorDDLAggregateCOSSTable::init(
    ObSSTable &base_sstable,
    const ObITable::TableKey &table_key,
    const int64_t column_group_cnt,
    const int64_t column_cnt,
    const ObCOSSTableBaseType co_base_type,
    ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  ObTabletCreateSSTableParam sstable_param;
  ObMemAttr attr(MTL_ID(), "INC_MAJOR");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid()
                         || !base_sstable.get_start_scn().is_valid_and_not_min()
                         || OB_UNLIKELY(column_group_cnt <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_key), K(column_group_cnt));
  } else if (OB_FAIL(calculate_rowkey_column_cnt_and_row_cnt(tables))) {
    LOG_WARN("fail to calculate rowkey column cnt", KR(ret));
  } else if (OB_FAIL(base_sstable.get_meta(base_sstable_meta_))) {
    LOG_WARN("fail to get meta handle");
  } else if (OB_UNLIKELY(!base_sstable_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base sstable meta is invalid", KR(ret), K(base_sstable_meta_));
  } else if (OB_FAIL(sstable_param.init_for_inc_major_ddl_aggregate(
                                                      table_key,
                                                      base_sstable,
                                                      base_sstable_meta_,
                                                      column_group_cnt,
                                                      rowkey_column_cnt_,
                                                      column_cnt,
                                                      row_cnt_))) {
    LOG_WARN("fail to init sstable param", KR(ret), K(table_key), K(base_sstable),
                                           K(column_group_cnt), K(column_cnt), K(row_cnt_));
  } else if (OB_FAIL(ObSSTable::init(sstable_param, &allocator_))) {
    LOG_WARN("fail to init sstable", KR(ret), K(sstable_param));
  } else if (OB_FAIL(cg_sstables_.create(CG_SSTABLE_COUNT, attr, attr))) {
    LOG_WARN("fail to create cg sstable map", KR(ret), K(attr));
  } else if (OB_FAIL(init_tx_info(base_sstable))) {
    LOG_WARN("fail to init tx info", KR(ret), K(base_sstable), K(tx_info_));
  } else {
    tablet_id_ = table_key.tablet_id_;
    base_sstable_ = &base_sstable;
    column_group_cnt_ = column_group_cnt;
    column_cnt_ = column_cnt;

    // derived from ObCOSSTableV2
    valid_for_cs_reading_ = true;
    base_type_ = co_base_type;
    cs_meta_.column_group_cnt_ = column_group_cnt;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    if (OB_FAIL(add_table(tables.at(i)))) {
      LOG_WARN("fail to add table", KR(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_before_query())) {
    LOG_WARN("fail to prepare before query", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("init aggregate co sstable success", KR(ret), K(table_key), K(*this), K(common::lbt())); // tmp log
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::init_tx_info(const ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("fail to get meta", KR(ret));
  } else {
    const ObMetaUncommitTxInfo &uncommit_info
          = meta_handle.get_sstable_meta().get_uncommit_tx_info();
    if (OB_UNLIKELY(!uncommit_info.is_valid() || OB_ISNULL(uncommit_info.tx_infos_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid uncommit info", KR(ret), K(uncommit_info));
    } else {
      ObTransID trans_id(uncommit_info.tx_infos_[0].tx_id_);
      ObTxSEQ sql_seq;
      sql_seq.cast_from_int(uncommit_info.tx_infos_[0].sql_seq_);
      tx_info_.trans_id_ = trans_id;
      tx_info_.seq_no_ = sql_seq;
    }
  }
  return ret;
}
int ObIncMajorDDLAggregateCOSSTable::check_can_access(
    const ObTableAccessContext &context,
    bool &can_access) const
{
  int ret = OB_SUCCESS;
  can_access = true;
  SCN max_scn = SCN::max_scn();
  SCN trans_version;
  memtable::ObMvccAccessCtx &mvcc_acc_ctx = context.store_ctx_->mvcc_acc_ctx_;
  storage::ObTxTableGuards &tx_table_guards = mvcc_acc_ctx.get_tx_table_guards();
  transaction::ObLockForReadArg lock_for_read_arg(mvcc_acc_ctx,
                                                  tx_info_.trans_id_,
                                                  tx_info_.seq_no_,
                                                  context.query_flag_.read_latest_,
                                                  context.query_flag_.iter_uncommitted_row_,
                                                  max_scn);
  if (OB_FAIL(tx_table_guards.lock_for_read(lock_for_read_arg, can_access, trans_version))) {
    LOG_WARN("fail to lock for read", KR(ret), K(lock_for_read_arg));
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::calculate_rowkey_column_cnt_and_row_cnt(ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tables.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables is empty", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      ObSSTable *sstable = static_cast<ObSSTable *>(tables.at(i));
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable is nullptr", KR(ret));
      } else if (sstable->is_rowkey_cg_sstable()) {
        ObSSTableMetaHandle meta_handle;
        if (OB_FAIL(sstable->get_meta(meta_handle))) {
          LOG_WARN("fail to get meta", KR(ret));
        } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("meta handle is invalid", KR(ret));
        } else {
          if (sstable->is_ddl_mem_sstable()) {
            ObDDLMemtable *ddl_mem = static_cast<ObDDLMemtable *>(sstable);
            row_cnt_ += ddl_mem->get_row_count();
          } else {
            row_cnt_ += meta_handle.get_sstable_meta().get_row_count();
          }
          if (OB_UNLIKELY(0 == rowkey_column_cnt_)) {
            rowkey_column_cnt_ = meta_handle.get_sstable_meta().get_rowkey_column_count();
          }
        }
      }
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::add_table(ObITable *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is nullptr", KR(ret));
  } else if (OB_UNLIKELY(!table->is_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table type", KR(ret), KPC(table));
  } else {
    ObSSTable *sstable = static_cast<ObSSTable *>(table);
    if (sstable->is_inc_major_ddl_dump_sstable()) {
      if (OB_FAIL(add_ddl_sstable(sstable))) {
        LOG_WARN("fail to add sstable", KR(ret));
      }
    } else if (sstable->is_inc_major_ddl_mem_sstable()) {
      if (OB_FAIL(add_ddl_memtable(sstable))) {
        LOG_WARN("fail to add ddl memetable", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sstable type", KR(ret), KPC(sstable));
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::set_rowkey_cg_sstable()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey_cg_sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg sstable is nullptr", KR(ret));
  } else if (0 != rowkey_cg_sstable_->get_cg_idx()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg idx should be zero", KR(ret), K(rowkey_cg_sstable_->get_cg_idx()));
  } else {
    FOREACH_X(it, cg_sstables_, (OB_SUCC(ret))) {
      if (OB_FAIL(it->second->set_rowkey_cg_sstable(rowkey_cg_sstable_))) {
        LOG_WARN("fail to set rowkey cg sstable", KR(ret));
      }
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::get_sstable(const ObSSTable *&sstable) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(rowkey_cg_sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(rowkey_cg_sstable_->get_sstable(sstable))) {
    LOG_WARN("fail to get sstable", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::get_sstables(ObIArray<ObSSTable *> &ddl_sstables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(rowkey_cg_sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(rowkey_cg_sstable_->get_sstables(ddl_sstables))) {
    LOG_WARN("fail to get sstables", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::get_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(rowkey_cg_sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(rowkey_cg_sstable_->get_memtables(ddl_memtables))) {
    LOG_WARN("fail to get memtables", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::get_rowkey_sstables(ObIArray<ObSSTable *> &ddl_sstables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(get_sstables(ddl_sstables))) {
    LOG_WARN("fail to get sstables", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::get_rowkey_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(get_memtables(ddl_memtables))) {
    LOG_WARN("fail to get memtables", KR(ret));
  }
  return ret;
}

int64_t ObIncMajorDDLAggregateCOSSTable::get_cg_idx() const
{
  return 0;
}

int ObIncMajorDDLAggregateCOSSTable::get_index_tree_root(
    blocksstable::ObMicroBlockData &index_data,
    const bool need_transform)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(rowkey_cg_sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(rowkey_cg_sstable_->get_index_tree_root(index_data, need_transform))) {
    LOG_WARN("fail to get index tree root", KR(ret));
  }
  return ret;
}

bool ObIncMajorDDLAggregateCOSSTable::is_empty() const
{
  return false;
}

int ObIncMajorDDLAggregateCOSSTable::prepare_before_query()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_all_cg())) {
    LOG_WARN("fail to check all cg", KR(ret));
  } else if (OB_FAIL(init_index_tree_root())) {
    LOG_WARN("fail to init index tree root", KR(ret));
  } else if (OB_FAIL(set_rowkey_cg_sstable())) {
    LOG_WARN("fail to set rowkey cg sstable", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::check_all_cg()
{
  int ret = OB_SUCCESS;
  ObIncMajorDDLAggregateCGSSTable *cg_sstable = nullptr;
  if (OB_UNLIKELY(cg_sstables_.size() != column_group_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg cnt has error", KR(ret), K(column_group_cnt_), K(cg_sstables_.size()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_group_cnt_; ++i) {
      if (OB_FAIL(cg_sstables_.get_refactored(i, cg_sstable))) {
        LOG_WARN("fail to get cg sstable", KR(ret), K(i), KPC(cg_sstable));
      } else if (OB_ISNULL(cg_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg sstable is nullptr", KR(ret), K(i));
      } else {
        cg_sstable = nullptr;
      }
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::init_index_tree_root()
{
  int ret = OB_SUCCESS;
  FOREACH_X(it, cg_sstables_, (OB_SUCC(ret))) {
    if (OB_FAIL(it->second->init_index_tree_root())) {
      LOG_WARN("fail to init index tree root", KR(ret));
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::add_ddl_sstable(ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is nullptr", KR(ret));
  } else if (OB_UNLIKELY(!sstable->is_co_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sstable type", KR(ret), KPC(sstable));
  } else {
    const ObCOSSTableV2 *co_sstable = static_cast<const ObCOSSTableV2 *>(sstable);
    ObSSTableMetaHandle co_meta_handle;
    ObArray<ObSSTableWrapper> cg_tables;
    if (OB_FAIL(co_sstable->get_meta(co_meta_handle))) {
      LOG_WARN("fail to get co meta", KR(ret));
    } else if (OB_UNLIKELY(!co_meta_handle.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("co meta handle is invalid", KR(ret));
    } else if (OB_FAIL(co_meta_handles_.push_back(co_meta_handle))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(co_sstable->get_all_tables(cg_tables))) {
      LOG_WARN("fail to get all tables", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_tables.count(); ++i) {
      ObSSTableWrapper &cg_sstable_wrapper = cg_tables.at(i);
      ObSSTable *cg_sstable = nullptr;
      if (0 == i) {
        cg_sstable_wrapper.meta_handle_ = co_meta_handle.get_storage_handle();
      }
      if (OB_FAIL(cg_sstable_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
        LOG_WARN("fail to get loaded column store sstable", KR(ret));
      } else if (OB_FAIL(add_cg_sstable(i, cg_sstable))) {
        LOG_WARN("fail to inner add table", KR(ret), K(i), KPC(cg_sstable));
      } else if (OB_FAIL(add_cg_sstable_wrapper(i, cg_sstable_wrapper))) {
        LOG_WARN("fail to add_cg_sstable_wrapper", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::add_ddl_memtable(ObSSTable *memtable)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("memtable is nullptr", KR(ret));
  } else {
    int64_t cg_idx = memtable->get_column_group_id();
    if (OB_FAIL(add_cg_sstable(cg_idx, memtable))) {
      LOG_WARN("fail to inner add table", KR(cg_idx));
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::add_cg_sstable(
    const int64_t cg_idx,
    ObITable *table)
{
  int ret = OB_SUCCESS;
  ObIncMajorDDLAggregateCGSSTable *cg_sstable = nullptr;
  if (OB_UNLIKELY(cg_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cg_idx));
  } else if (OB_FAIL(cg_sstables_.get_refactored(cg_idx, cg_sstable))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
      ret = OB_SUCCESS;
      ObITable::TableKey table_key;
      table_key.table_type_ = ObITable::INC_MAJOR_DDL_AGGREGATE_CG_SSTABLE;
      table_key.tablet_id_ = tablet_id_;
      table_key.scn_range_ = base_sstable_->get_key().scn_range_;
      table_key.column_group_idx_ = cg_idx;
      if (OB_ISNULL(cg_sstable = OB_NEWx(ObIncMajorDDLAggregateCGSSTable,
                                         &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret));
      } else if (OB_FAIL(cg_sstable->init(allocator_,
                                          table_key,
                                          *base_sstable_,
                                          base_sstable_meta_,
                                          column_group_cnt_,
                                          (0 == cg_idx) ? rowkey_column_cnt_ : 0,
                                          column_cnt_,
                                          row_cnt_,
                                          cg_idx))) {
        LOG_WARN("fail to init cg sstable", KR(ret), K(table_key), KPC(base_sstable_),
                                            K(base_sstable_meta_), K(column_group_cnt_),
                                            K(column_cnt_), K(row_cnt_), K(cg_idx));
      } else if (OB_FAIL(cg_sstables_.set_refactored(cg_idx, cg_sstable))) {
        LOG_WARN("fail to insert cgsstable to map", KR(ret), K(cg_idx));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(cg_sstable)) {
        cg_sstable->~ObIncMajorDDLAggregateCGSSTable();
        allocator_.free(cg_sstable);
        cg_sstable = nullptr;
      }
    } else {
      LOG_WARN("fail to get from cg sstable map", KR(ret), K(cg_idx));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cg_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(cg_sstable->add_table(cg_idx, table))) {
    LOG_WARN("fail to add table", KR(ret), K(cg_idx));
  } else if (0 == cg_idx) {
    // save rowkey cg sstable(ObIncMajorDDLAggregateCGSSTable)
    if (OB_ISNULL(rowkey_cg_sstable_)) {
      rowkey_cg_sstable_ = cg_sstable;
    } else if (rowkey_cg_sstable_ != cg_sstable) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey cg sstable", KR(ret));
    }
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::add_cg_sstable_wrapper(
    const int64_t cg_idx,
    ObSSTableWrapper &cg_wrapper)
{
  int ret = OB_SUCCESS;
  ObIncMajorDDLAggregateCGSSTable *cg_sstable = nullptr;
  if (OB_UNLIKELY(cg_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cg idx", KR(ret), K(cg_idx));
  } else if (OB_FAIL(cg_sstables_.get_refactored(cg_idx, cg_sstable))) {
    LOG_WARN("fail to get cg sstable", KR(ret), K(cg_idx));
  } else if (OB_ISNULL(cg_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(cg_sstable->add_sstable_wrapper(cg_wrapper))) {
    LOG_WARN("fail ot add sstable wrapper", KR(ret));
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::fetch_cg_sstable(
    const uint32_t cg_idx,
    ObSSTableWrapper &cg_wrapper) const
{
  int ret = OB_SUCCESS;
  cg_wrapper.reset();
  uint32_t real_cg_idx = UINT32_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (FALSE_IT(real_cg_idx = cg_idx < column_group_cnt_ ? cg_idx : key_.column_group_idx_)) {
  } else if (OB_FAIL(get_cg_sstable(real_cg_idx, cg_wrapper))) {
    LOG_WARN("fail to get cg sstable", KR(ret), K(cg_idx), K(real_cg_idx));
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::get_cg_sstable(
    const uint32_t cg_idx,
    ObSSTableWrapper &cg_wrapper) const
{
  int ret = OB_SUCCESS;
  ObIncMajorDDLAggregateCGSSTable *cg_sstable = nullptr;
  ObSSTableMetaHandle cg_meta_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(cg_idx >= column_group_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg idx is invalid", KR(ret), K(cg_idx), K(column_group_cnt_));
  } else if (OB_FAIL(cg_sstables_.get_refactored(cg_idx, cg_sstable))) {
    LOG_WARN("fail to get inc major aggregate cg sstable", KR(ret), K(cg_idx));
  } else if (OB_ISNULL(cg_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inc major aggregate cg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(cg_sstable->get_meta(cg_meta_handle))) {
    LOG_WARN("fail to get cg meta handle", KR(ret));
  } else if (OB_UNLIKELY(!cg_meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg meta handle is invalid", KR(ret), K(cg_meta_handle));
  } else {
    cg_wrapper.sstable_ = cg_sstable;
    cg_wrapper.meta_handle_ = cg_meta_handle.get_storage_handle();
  }
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::get_all_tables(
    common::ObIArray<ObSSTableWrapper> &table_wrappers) const
{
  int ret = OB_SUCCESS;
  ObSSTableWrapper cg_wrapper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  }
  FOREACH_X(it, cg_sstables_, (OB_SUCC(ret))) {
    if (OB_FAIL(fetch_cg_sstable(it->first, cg_wrapper))) {
      LOG_WARN("fail to fetch cg sstable", KR(ret), K(it->first));
    } else if (OB_FAIL(table_wrappers.push_back(cg_wrapper))) {
      LOG_WARN("fail to push back cg wrapper", KR(ret), K(cg_wrapper));
    }
  }
  return ret;
}

#define QUERY_INTERFACE_IMPLEMENTATION(query_type, query_key)                         \
  do {                                                                               \
    bool can_access = false;                                                         \
    if (IS_NOT_INIT) {                                                               \
      ret = OB_NOT_INIT;                                                             \
      LOG_WARN("not init", KR(ret));                                                 \
    } else if (OB_FAIL(check_can_access(context, can_access))) {                     \
      LOG_WARN("fail to check can access", KR(ret));                                 \
    } else if (can_access) {                                                         \
      if (OB_FAIL(ObCOSSTableV2::query_type(param, context,                          \
                                            query_key, row_iter))) {                 \
        LOG_WARN("fail to query", KR(ret));                                          \
      } else {                                                                       \
        LOG_INFO("query aggregate co sstable",                                       \
                 KR(ret), K(param), K(*this), K(common::lbt()));                     \
      }                                                                              \
    } else {                                                                         \
      ObStoreRowIterator *empty_iter = nullptr;                                      \
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,                                     \
                                        ObIncMajorDDLAggregateSSTableEmptyRowIterator, \
                                        empty_iter);                                 \
      if (OB_FAIL(ret)) {                                                            \
        if (OB_NOT_NULL(empty_iter)) {                                               \
          empty_iter->~ObStoreRowIterator();                                         \
          FREE_TABLE_STORE_ROW_IETRATOR(context, empty_iter);                        \
          empty_iter = nullptr;                                                      \
        }                                                                            \
      } else {                                                                       \
        row_iter = empty_iter;                                                       \
      }                                                                              \
    }                                                                                \
  } while (false)

int ObIncMajorDDLAggregateCOSSTable::scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRange &key_range,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  QUERY_INTERFACE_IMPLEMENTATION(scan, key_range);
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::multi_scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const common::ObIArray<blocksstable::ObDatumRange> &ranges,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  QUERY_INTERFACE_IMPLEMENTATION(multi_scan, ranges);
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::get(
    const storage::ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  QUERY_INTERFACE_IMPLEMENTATION(get, rowkey);
  return ret;
}

int ObIncMajorDDLAggregateCOSSTable::multi_get(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  QUERY_INTERFACE_IMPLEMENTATION(multi_get, rowkeys);
  return ret;
}

#undef QUERY_INTERFACE_IMPLEMENTATION

} // namespace storage
} // namespace oceanbase
