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
#include "share/schema/ob_table_dml_param.h"
#include "storage/ob_relative_table.h"
#include "storage/access/ob_single_merge.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ob_value_row_iterator.h"
#include "storage/memtable/ob_memtable_context.h"
namespace oceanbase
{
using namespace oceanbase::common;
using namespace blocksstable;
namespace storage
{
ObValueRowIterator::ObValueRowIterator()
    : ObDatumRowIterator(),
      is_inited_(false),
      unique_(false),
      allocator_("ObValueRowAlloc"),
      rows_(),
      cur_idx_(0)
{
}

ObValueRowIterator::~ObValueRowIterator()
{
}

int ObValueRowIterator::init(bool unique)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObValueRowIterator is already initialized", K(ret));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    is_inited_ = true;
    unique_ = unique;
    cur_idx_ = 0;
  }
  return ret;
}

int ObValueRowIterator::add_row(ObDatumRow &row, const ObStorageDatumUtils &rowkey_datum_utils)
{
  int ret = OB_SUCCESS;
  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row", K(ret), K(row));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObValueRowIterator is not initialized", K(ret));
  } else {
    bool exist = false;
    // check whether exists
    if (unique_ && rows_.count() > 0) {
      //we consider that in general, the probability that a row produces different conflicting rows
      //on multiple unique index is small, so there is usually only one row in the value row iterator
      //so using list traversal to deduplicate unique index is more efficiently
      //and also saves the CPU overhead that constructs the hash map
      ObDatumRowkey rowkey;
      ObDatumRowkey tmp_rowkey;
      if (OB_FAIL(rowkey.assign(row.storage_datums_, row.count_))) {
        STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), K(row));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < rows_.count(); ++i) {
        if (OB_FAIL(tmp_rowkey.assign(rows_.at(i)->storage_datums_, rows_.at(i)->count_))) {
          STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), K(i), KPC(rows_.at(i)));
        } else if (OB_FAIL(tmp_rowkey.equal(rowkey, rowkey_datum_utils, exist))) {
          STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(i), K(tmp_rowkey), K(rowkey));
        }
      }
    }
    // store non-exist row
    if (OB_SUCC(ret)) {
      if (!exist) {
        ObDatumRow *cur_row = nullptr;
        void *buff = nullptr;
        if (OB_ISNULL(buff = allocator_.alloc(sizeof(ObDatumRow)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "alloc memory for datum row error", K(ret));
        } else if (FALSE_IT(cur_row = new (buff) ObDatumRow())) {
        } else if (OB_FAIL(cur_row->init(allocator_, row.count_))) {
          STORAGE_LOG(WARN, "init datum row error", K(ret), K(row), KPC(cur_row));
        } else if (OB_FAIL(cur_row->deep_copy(row, allocator_))) {
          STORAGE_LOG(WARN, "copy row error", K(ret), K(row));
        } else if (OB_FAIL(rows_.push_back(cur_row))) {
          STORAGE_LOG(WARN, "fail to push datum row to iterator array", K(ret), K(cur_row));
        }
      }
    }
  }
  return ret;
}

int ObValueRowIterator::get_next_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObValueRowIterator is not initialized", K(ret));
  } else if (cur_idx_ < rows_.count()) {
    row = rows_.at(cur_idx_++);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

void ObValueRowIterator::reset()
{
  rows_.reset();
  allocator_.reset();
  is_inited_ = false;
  unique_ = false;
  cur_idx_ = 0;
}

ObSingleRowGetter::ObSingleRowGetter(ObIAllocator &allocator, ObTablet &tablet)
  : tablet_(&tablet),
    single_merge_(nullptr),
    store_ctx_(nullptr),
    output_projector_(allocator),
    relative_table_(nullptr),
    allocator_(allocator),
    cached_iter_node_(nullptr)
{
}

ObSingleRowGetter::~ObSingleRowGetter()
{
  if (nullptr != single_merge_) {
    if (nullptr == cached_iter_node_) {
      single_merge_->~ObSingleMerge();
      allocator_.free(single_merge_);
    }
    single_merge_ = nullptr;
  }
  if (nullptr != cached_iter_node_) {
    ObGlobalIteratorPool *iter_pool = MTL(ObGlobalIteratorPool*);
    iter_pool->release(cached_iter_node_);
  }
}

int ObSingleRowGetter::init_dml_access_ctx(
    ObStoreCtx &store_ctx,
    bool skip_read_lob)
{
  int ret = OB_SUCCESS;
  common::ObQueryFlag query_flag;
  common::ObVersionRange trans_version_range;
  query_flag.set_not_use_bloomfilter_cache();
  query_flag.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;
  if (skip_read_lob) {
    query_flag.skip_read_lob_ = ObQueryFlag::OBSF_MASK_SKIP_READ_LOB;
  }
  trans_version_range.snapshot_version_ = store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  store_ctx_ = &store_ctx;

  if (OB_FAIL(access_ctx_.init(query_flag, store_ctx, allocator_, trans_version_range, cached_iter_node_))) {
    LOG_WARN("failed to init table access ctx", K(ret));
  }
  return ret;
}

int ObSingleRowGetter::init_dml_access_param(ObRelativeTable &relative_table,
                                             const ObIArray<uint64_t> &out_col_ids,
                                             const bool skip_read_lob)
{
  int ret = OB_SUCCESS;
  relative_table_ = &relative_table;

  const share::schema::ObTableSchemaParam *schema_param = relative_table.get_schema_param();
  output_projector_.set_capacity(out_col_ids.count());
  if (OB_FAIL(get_table_param_.tablet_iter_.assign(relative_table.tablet_iter_))) {
    LOG_WARN("assign tablet iterator fail", K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < out_col_ids.count(); ++i) {
    int idx = OB_INVALID_INDEX;
    if (OB_FAIL(schema_param->get_col_map().get(out_col_ids.at(i), idx))) {
      LOG_WARN("get column index from column map failed", K(ret), K(out_col_ids.at(i)));
    } else if (OB_FAIL(output_projector_.push_back(idx))) {
      LOG_WARN("store output projector failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(access_param_.init_dml_access_param(relative_table,
                                                    tablet_->get_rowkey_read_info(),
                                                    *schema_param,
                                                    &output_projector_))) {
      LOG_WARN("init dml access param failed", K(ret));
    } else if (skip_read_lob) {
      access_param_.iter_param_.has_lob_column_out_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    const ObTabletMeta &tablet_meta = tablet_->get_tablet_meta();
    if (OB_UNLIKELY(!tablet_meta.is_valid())) {
      LOG_WARN("tablet meta is invalid", K(ret), K(tablet_meta));
    } else {
      access_param_.iter_param_.tablet_id_ = tablet_meta.tablet_id_;
    }
  }

  LOG_DEBUG("init dml access param", K(ret), K(out_col_ids), K(relative_table), K_(access_param));
  return ret;
}

int ObSingleRowGetter::prepare_cached_iter_node(const ObDMLBaseParam &dml_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != cached_iter_node_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected not null cached iter node", K(ret), KP(cached_iter_node_));
  } else if (can_use_global_iter_pool(dml_param)) {
    ObGlobalIteratorPool *iter_pool = MTL(ObGlobalIteratorPool*);
    if (OB_FAIL(iter_pool->get(ITER_TYPE, cached_iter_node_))) {
      STORAGE_LOG(WARN, "Failed to get from iter pool", K(ret));
    } else if (nullptr != cached_iter_node_) {
      access_param_.set_use_global_iter_pool();
      access_param_.iter_param_.set_use_stmt_iter_pool();
      STORAGE_LOG(TRACE, "use global iter pool", K(access_param_));
    }
  }
  return ret;
}

int ObSingleRowGetter::open(const ObDatumRowkey &rowkey, bool use_fuse_row_cache)
{
  int ret = OB_SUCCESS;
  {
    ObStorageTableGuard guard(tablet_, *store_ctx_, false);
    if (OB_FAIL(guard.refresh_and_protect_memtable_for_write(*relative_table_))) {
      STORAGE_LOG(WARN, "fail to protect table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ACTIVE_GLOBAL_ITERATOR_GUARD(ret, cached_iter_node_);
    if (OB_FAIL(init_single_merge())) {
      STORAGE_LOG(WARN, "Fail to init ObSingleMerge", K(ret));
    } else if (OB_FAIL(single_merge_->open(rowkey))) {
      STORAGE_LOG(WARN, "Fail to open iter", K(ret));
    }
    if (use_fuse_row_cache) {
      access_ctx_.use_fuse_row_cache_ = true;
    }
  }
  return ret;
}

int ObSingleRowGetter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  ACTIVE_GLOBAL_ITERATOR_GUARD(ret, cached_iter_node_);
  row = nullptr;
  while (OB_SUCC(ret)) {
    blocksstable::ObDatumRow *store_row = NULL;
    if (OB_FAIL(single_merge_->get_next_row(store_row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to get next row", K(ret));
      }
    } else if (store_row->row_flag_.is_exist_without_delete()) {
      row = store_row;
      break;
    }
  }
  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    // check txn status not aborted, which cause readout incorrect result
    memtable::ObMvccAccessCtx &acc_ctx = store_ctx_->mvcc_acc_ctx_;
    if (acc_ctx.snapshot_.tx_id_.is_valid() &&
        acc_ctx.mem_ctx_ &&
        acc_ctx.mem_ctx_->is_tx_rollbacked()) {
      if (acc_ctx.mem_ctx_->is_for_replay()) {
        // goes here means the txn was killed due to LS's GC etc,
        // return NOT_MASTER
        ret = OB_NOT_MASTER;
      } else {
        // The txn has been killed during normal processing. So we return
        // OB_TRANS_KILLED to prompt this abnormal state.
        ret = OB_TRANS_KILLED;
        STORAGE_LOG(WARN, "txn has terminated", K(ret), "tx_id", acc_ctx.tx_id_);
      }
    }
  }
  return ret;
}

bool ObSingleRowGetter::can_use_global_iter_pool(const ObDMLBaseParam &dml_param) const
{
  bool use_pool = false;
  if (access_param_.iter_param_.tablet_id_.is_inner_tablet()) {
  } else if (access_param_.iter_param_.has_lob_column_out_) {
  } else {
    const int64_t table_cnt = get_table_param_.tablet_iter_.table_iter()->count();
    const int64_t col_cnt = dml_param.table_param_->get_data_table().get_read_info().get_schema_column_count();
    ObGlobalIteratorPool *iter_pool = MTL(ObGlobalIteratorPool*);
    if (OB_NOT_NULL(iter_pool)) {
       use_pool = iter_pool->can_use_iter_pool(table_cnt, col_cnt, ITER_TYPE);
    }
  }
  return use_pool;
}

int ObSingleRowGetter::init_single_merge()
{
  int ret = OB_SUCCESS;
  ObQueryRowIterator *cached_iter = nullptr == cached_iter_node_ ? nullptr : cached_iter_node_->get_iter();
  if (OB_NOT_NULL(cached_iter)) {
    if (OB_UNLIKELY(cached_iter->get_type() != ITER_TYPE)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected cached iter type", K(ret), K(cached_iter->get_type()));
    } else {
      single_merge_ = static_cast<ObSingleMerge*>(cached_iter);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (nullptr == single_merge_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = access_ctx_.get_long_life_allocator()->alloc(sizeof(ObSingleMerge)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory", K(ret));
    } else {
      single_merge_ = new (buf) ObSingleMerge();
      if (OB_FAIL(single_merge_->init(access_param_, access_ctx_, get_table_param_))) {
        STORAGE_LOG(WARN, "Failed to init multiple merge", K(ret));
      }
      if (OB_FAIL(ret)) {
        single_merge_->~ObSingleMerge();
        access_ctx_.get_long_life_allocator()->free(single_merge_);
        single_merge_ = nullptr;
      } else if (nullptr != cached_iter_node_) {
        cached_iter_node_->set_iter(single_merge_);
      }
    }
  } else if (OB_FAIL(single_merge_->switch_table(access_param_, access_ctx_, get_table_param_))) {
    STORAGE_LOG(WARN, "Failed to switch table", K(ret), K(access_param_));
  }
  return ret;
}
} // end namespace storage
} // end namespace oceanbase
