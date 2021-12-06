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
#include "ob_value_row_iterator.h"
#include "ob_partition_storage.h"
#include "ob_single_merge.h"

namespace oceanbase {
using namespace oceanbase::common;
namespace storage {
ObValueRowIterator::ObValueRowIterator()
    : ObNewRowIterator(),
      is_inited_(false),
      unique_(false),
      allocator_(ObModIds::OB_VALUE_ROW_ITER),
      rows_(),
      cur_idx_(0),
      data_table_rowkey_cnt_(0)
{}

ObValueRowIterator::~ObValueRowIterator()
{}

int ObValueRowIterator::init(bool unique, int64_t data_table_rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObValueRowIterator is already initialized", K(ret));
  } else {
    is_inited_ = true;
    unique_ = unique;
    cur_idx_ = 0;
    data_table_rowkey_cnt_ = data_table_rowkey_cnt;
  }
  return ret;
}

int ObValueRowIterator::add_row(common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObNewRow* cur_row = NULL;
  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row", K(ret), K(row));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObValueRowIterator is not initialized", K(ret));
  } else if (data_table_rowkey_cnt_ > row.count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row", K(ret), K(row), K(data_table_rowkey_cnt_));
  } else {
    bool exist = false;
    // check whether exists
    if (unique_ && rows_.count() > 0) {
      // we consider that in general, the probability that a row produces different conflicting rows
      // on multiple unique index is small, so there is usually only one row in the value row iterator
      // so using list traversal to deduplicate unique index is more efficiently
      // and also saves the CPU overhead that constructs the hash map
      ObStoreRowkey rowkey(row.cells_, data_table_rowkey_cnt_);
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < rows_.count(); ++i) {
        if (data_table_rowkey_cnt_ > rows_.at(i).count_) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid row", K(ret), K(row), K(data_table_rowkey_cnt_));
        } else {
          ObStoreRowkey tmp_rowkey(rows_.at(i).cells_, data_table_rowkey_cnt_);
          STORAGE_LOG(DEBUG, "print rowkey info", K(rowkey), K(tmp_rowkey), K(data_table_rowkey_cnt_));
          if (OB_UNLIKELY(tmp_rowkey == rowkey)) {
            exist = true;
          }
        }
      }
    }
    // store non-exist row
    if (OB_SUCC(ret)) {
      if (!exist) {
        if (NULL == (cur_row = rows_.alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(ERROR, "add row error", K(ret));
        } else if (OB_SUCCESS != (ret = ob_write_row(allocator_, row, *cur_row))) {
          STORAGE_LOG(WARN, "copy row error", K(ret), K(row));
        }
      }
    }
  }
  return ret;
}

int ObValueRowIterator::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObValueRowIterator is not initialized", K(ret));
  } else if (cur_idx_ < rows_.count()) {
    row = &rows_.at(cur_idx_++);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObValueRowIterator::get_next_rows(ObNewRow*& rows, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  if (cur_idx_ < rows_.count()) {
    rows = &(rows_.at(cur_idx_));
    row_count = rows_.count() - cur_idx_;
    cur_idx_ = rows_.count();
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

ObSingleRowGetter::ObSingleRowGetter(ObIAllocator &allocator, ObPartitionStore &store)
    : store_(store),
      single_merge_(nullptr),
      store_ctx_(nullptr),
      output_projector_(allocator),
      relative_table_(nullptr),
      table_param_(nullptr),
      allocator_(allocator)
{}

ObSingleRowGetter::~ObSingleRowGetter()
{
  if (single_merge_ != nullptr) {
    single_merge_->~ObSingleMerge();
    allocator_.free(single_merge_);
    single_merge_ = nullptr;
  }
  if (table_param_ != nullptr) {
    table_param_->~ObTableParam();
    allocator_.free(table_param_);
    table_param_ = nullptr;
  }
}

int ObSingleRowGetter::init_dml_access_ctx(const ObStoreCtx &store_ctx, const ObDMLBaseParam &dml_param)
{
  int ret = OB_SUCCESS;
  common::ObVersionRange trans_version_range;
  // TODO (muwei) trans_version_range值后续由上层传入
  trans_version_range.snapshot_version_ = store_ctx.mem_ctx_->get_read_snapshot();
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  store_ctx_ = &store_ctx;

  if (OB_FAIL(access_ctx_.init(dml_param.query_flag_, store_ctx, allocator_, trans_version_range))) {
    LOG_WARN("failed to init table access ctx", K(ret));
  } else {
    access_ctx_.expr_ctx_ = const_cast<ObExprCtx *>(&dml_param.expr_ctx_);
  }
  return ret;
}

int ObSingleRowGetter::init_dml_access_param(
    ObRelativeTable &relative_table, const ObDMLBaseParam &dml_param, const ObIArray<uint64_t> &out_col_ids)
{
  int ret = OB_SUCCESS;
  relative_table_ = &relative_table;
  get_table_param_.tables_handle_ = &(relative_table.tables_handle_);
  if (!dml_param.virtual_columns_.empty() && !relative_table.is_index_table()) {
    //The index table does not contain virtual columns, no need to set virtual_columns
    access_param_.virtual_column_exprs_ = &(dml_param.virtual_columns_);
  }
  if (OB_UNLIKELY(!relative_table.use_schema_param())) {
    const share::schema::ObTableSchema *schema = relative_table.get_schema();
    if (OB_FAIL(create_table_param())) {
      LOG_WARN("create table param failed", K(ret));
    } else if (OB_FAIL(table_param_->convert(*schema, *schema, out_col_ids, false))) {
      LOG_WARN("build table param from schema fail", K(ret), KPC(schema));
    } else if (OB_FAIL(access_param_.init_dml_access_param(relative_table.get_table_id(),
                   relative_table.get_schema_version(),
                   relative_table.get_rowkey_column_num(),
                   *table_param_))) {
      LOG_WARN("init dml access param failed", K(ret));
    }
  } else {
    const share::schema::ObTableSchemaParam *schema_param = relative_table.get_schema_param();
    output_projector_.set_capacity(out_col_ids.count());
    for (int32_t i = 0; OB_SUCC(ret) && i < out_col_ids.count(); ++i) {
      int idx = OB_INVALID_INDEX;
      if (OB_FAIL(schema_param->get_col_map().get(out_col_ids.at(i), idx))) {
        LOG_WARN("get column index from column map failed", K(ret), K(out_col_ids.at(i)));
      } else if (OB_FAIL(output_projector_.push_back(idx))) {
        LOG_WARN("store output projector failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(access_param_.init_dml_access_param(relative_table.get_table_id(),
              relative_table.get_schema_version(),
              relative_table.get_rowkey_column_num(),
              *schema_param,
              &output_projector_))) {
        LOG_WARN("init dml access param failed", K(ret));
      }
    }
  }
  LOG_DEBUG("init dml access param", K(ret), K(out_col_ids), K(relative_table), K(dml_param), K_(access_param));
  return ret;
}

int ObSingleRowGetter::create_table_param()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (table_param_ != nullptr) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init table param twice", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(share::schema::ObTableParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate table param failed", K(ret), K(sizeof(share::schema::ObTableParam)));
  } else {
    table_param_ = new (buf) share::schema::ObTableParam(allocator_);
  }
  return ret;
}

int ObSingleRowGetter::open(const ObStoreRowkey &rowkey, bool use_fuse_row_cache)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  new (&ext_rowkey_) ObExtStoreRowkey(rowkey);
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSingleMerge)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for multi get merge ", K(ret));
  } else {
    {
      ObStorageWriterGuard guard(store_, *store_ctx_, false);
      if (OB_FAIL(guard.refresh_and_protect_table(*relative_table_))) {
        STORAGE_LOG(WARN, "fail to protect table", K(ret));
      }
    }
    single_merge_ = new (buf) ObSingleMerge();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(single_merge_->init(access_param_, access_ctx_, get_table_param_))) {
      STORAGE_LOG(WARN, "Fail to init ObSingleMerge, ", K(ret));
    } else if (OB_FAIL(single_merge_->open(ext_rowkey_))) {
      STORAGE_LOG(WARN, "Fail to open iter, ", K(ret));
    }
    if (use_fuse_row_cache) {
      access_ctx_.use_fuse_row_cache_ = true;
      access_ctx_.fuse_row_cache_hit_rate_ = 100L;
    }
  }
  return ret;
}

int ObSingleRowGetter::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  while (OB_SUCC(ret)) {
    ObStoreRow *store_row = NULL;
    if (OB_FAIL(single_merge_->get_next_row(store_row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to get next row", K(ret));
      }
    } else if (ObActionFlag::OP_ROW_EXIST == store_row->flag_) {
      row = &store_row->row_val_;
      break;
    }
  }
  return ret;
}
}  // end namespace storage
}  // end namespace oceanbase
