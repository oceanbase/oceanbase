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

#include "ob_ms_row_iterator.h"

namespace oceanbase {
using namespace memtable;
using namespace common;
using namespace share::schema;
using namespace compaction;
using namespace blocksstable;
namespace storage {

ObMSRowIterator::ObMSRowIterator()
    : is_inited_(false),
      allocator_(ObModIds::OB_MS_ROW_ITERATOR),
      macro_row_iters_(),
      minimum_iters_(),
      partition_fuser_(NULL),
      merge_param_(),
      cur_first_dml_(T_DML_UNKNOWN)
{}

ObMSRowIterator::~ObMSRowIterator()
{
  reuse();
}

int ObMSRowIterator::init(ObTablesHandle& tables_handle, const ObTableSchema& table_schema, ObExtStoreRange& range,
    const ObVersionRange& version_range, memtable::ObIMemtableCtxFactory* memctx_factory, const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (tables_handle.get_count() <= 0 || !table_schema.is_valid() || !version_range.is_valid() ||
      OB_ISNULL(memctx_factory) || !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "migrate split row iterator get invalid argument", K(ret));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObMSRowIterator has been inited, ", K(ret));
  } else if (OB_FAIL(init_merge_param(tables_handle, table_schema, version_range))) {
    STORAGE_LOG(WARN, "fail to init merge param", K(ret));
  } else {
    if (GCONF._enable_sparse_row) {
      partition_fuser_ = &sparse_partition_fuser_;
    } else {
      partition_fuser_ = &flat_partition_fuser_;
    }
    if (OB_FAIL(partition_fuser_->init(merge_param_))) {
      STORAGE_LOG(WARN, "Failed to init partition merge fuser", K(ret));
    } else if (OB_FAIL(prepare_iterators(memctx_factory, range, pg_key))) {  // prepare macro_row_iter
      STORAGE_LOG(WARN, "fail to prepare iterators", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMSRowIterator::prepare_iterators(
    memtable::ObIMemtableCtxFactory* memctx_factory, common::ObExtStoreRange& range, const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObITable* table = NULL;
  void* buf = NULL;
  ObMacroRowIterator* macro_row_iter = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObMSRowIterator has been inited, ", K(ret));
  } else if (OB_ISNULL(memctx_factory)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "prepare iterators get invalid argument", K(ret), KP(memctx_factory));
  } else {
    const ObIArray<ObITable*>& tables = merge_param_.tables_handle_->get_tables();
    // int64_t pre_snapshot_version = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      // prepare macro_rwo_iter
      table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The store is NULL", K(i), K(ret));
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMinorMergeMacroRowIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        macro_row_iter = new (buf) ObMinorMergeMacroRowIterator();
        if (OB_SUCC(ret)) {
          ObMacroRowIterator::Param param;
          param.memctx_factory_ = memctx_factory;
          param.schema_ = merge_param_.table_schema_;
          param.column_ids_ = &partition_fuser_->get_schema_column_ids();
          param.table_ = table;
          param.is_base_iter_ = (0 == i);
          param.is_full_merge_ = merge_param_.is_full_merge_;
          param.merge_level_ = merge_param_.merge_level_;
          // Logical migration does not rely on ObRowStoreType when reading row, because it does not write disk, the
          // default is FLAT_ROW_STORE here
          param.row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
          param.range_ = &range;
          param.multi_version_row_info_ = partition_fuser_->get_multi_version_row_info();
          param.merge_type_ = merge_param_.merge_type_;
          param.version_range_ = merge_param_.version_range_;
          param.is_sstable_cut_ = true;
          // TODO () enable range split after upgrade
          // pre_snapshot_version = (0 == i) ? merge_param_->version_range_.base_version_ : pre_snapshot_version;
          // if (OB_FAIL(param.split_version_range(merge_param_->version_range_, table->get_version_range(),
          // pre_snapshot_version))) {
          //  STORAGE_LOG(WARN, "failed to split verison range", K(ret));
          //}
          // pre_snapshot_version = param.version_range_.snapshot_version_;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(macro_row_iter->init(param, pg_key))) {
            macro_row_iter->~ObMacroRowIterator();
            allocator_.free(macro_row_iter);
            STORAGE_LOG(WARN, "Fail to init macro_row_iter", K(ret));
          } else if (OB_FAIL(macro_row_iters_.push_back(macro_row_iter))) {
            macro_row_iter->~ObMacroRowIterator();
            allocator_.free(macro_row_iter);
            STORAGE_LOG(WARN, "Fail to add macro_row_iter, ", K(ret));
          } else if (OB_FAIL(macro_row_iter->next())) {
            if (OB_LIKELY(OB_ITER_END == ret)) {
              ret = OB_SUCCESS;
            } else {
              STORAGE_LOG(WARN, "Fail to get next", K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO,
          "succeed to init ms row iter",
          "macro row iters count",
          macro_row_iters_.count(),
          "table count",
          tables.count());
    }
  }
  return ret;
}

int ObMSRowIterator::get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMSRowIterator has not been inited, ", K(ret));
  } else {
    // werid place since we need keep the row value buffer
    // The row with the smallest rowkey that handles the multi-way merge get_next
    for (int64_t i = 0; OB_SUCC(ret) && i < minimum_iters_.count(); ++i) {
      if (OB_FAIL(minimum_iters_.at(i)->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN,
              "fail to get next macro row iter",
              K(ret),
              K(i),
              K(minimum_iters_.count()),
              K(macro_row_iters_.count()));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_get_next())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to run inner get next row", K(ret));
    }
  } else if (OB_ISNULL(store_row = partition_fuser_->get_result_row())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null store row", KP(store_row), K(ret));
  } else if (!store_row->is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "Invalid store row", K(ret), K(*store_row));
  } else if (common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == store_row->flag_) {
    STORAGE_LOG(ERROR, "Unexpected row flag in logical migrate iterator");
    for (int64_t i = 0; i < minimum_iters_.count(); ++i) {
      ObMacroRowIterator* cur_iter = minimum_iters_.at(i);
      STORAGE_LOG(
          WARN, "macro row iter content: ", K(i), K(cur_iter->get_table()->get_key()), KPC(cur_iter->get_curr_row()));
    }
  }

  return ret;
}

int ObMSRowIterator::inner_get_next()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(partition_fuser_->find_minimum_iters(macro_row_iters_, minimum_iters_))) {
    STORAGE_LOG(WARN, "Failed to find minimum iters", K(ret));
  } else if (0 == minimum_iters_.count()) {
    ret = OB_ITER_END;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < minimum_iters_.count(); ++i) {
      if (NULL == minimum_iters_.at(i)->get_curr_row()) {
        // There is macro_row_iter in minimum_iters, but the row in row_iter is NULL
        // Explain that the data of the current macro block has not changed, but the basic unit of behavior is used in
        // the logic migration (taking into account the lob) So you must open the macro block
        if (OB_FAIL(minimum_iters_.at(i)->open_curr_macro_block())) {
          STORAGE_LOG(WARN, "Fail to open the curr macro block, ", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_fuser_->fuse_row(minimum_iters_))) {
        STORAGE_LOG(WARN, "fail to fuse store row", K(ret));
      }
    }
  }
  return ret;
}

void ObMSRowIterator::reuse()
{
  for (int64_t i = 0; i < macro_row_iters_.count(); ++i) {
    if (NULL != macro_row_iters_.at(i)) {
      STORAGE_LOG(INFO,
          "iter row count",
          K(macro_row_iters_.at(i)->get_iter_row_count()),
          K(i),
          K(macro_row_iters_.at(i)->get_table()->get_key()));
      macro_row_iters_.at(i)->~ObMacroRowIterator();
    }
  }
  macro_row_iters_.reset();
  minimum_iters_.reset();
  if (nullptr != partition_fuser_) {
    partition_fuser_->reset();
    partition_fuser_ = nullptr;
  }
  merge_param_.reset();
  // memctx_factory_
  allocator_.reuse();
  cur_first_dml_ = T_DML_UNKNOWN;
  is_inited_ = false;
}

int ObMSRowIterator::init_merge_param(
    ObTablesHandle& tables_handle, const ObTableSchema& table_schema, const ObVersionRange& version_range)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY((tables_handle.empty() || !table_schema.is_valid() || !version_range.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to init merge param", K(ret));
  } else {
    merge_param_.tables_handle_ = &tables_handle;
    merge_param_.table_schema_ = &table_schema;
    merge_param_.mv_dep_table_schema_ = NULL;
    merge_param_.is_full_merge_ = true;
    merge_param_.merge_type_ = MINOR_MERGE;
    merge_param_.checksum_method_ = blocksstable::CCM_UNKOWN;
    merge_param_.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
    merge_param_.version_range_ = version_range;
    merge_param_.checksum_calculator_ = NULL;
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
