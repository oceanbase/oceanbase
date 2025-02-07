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
#define USING_LOG_PREFIX SERVER
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_merger.h"
#include "observer/table_load/ob_table_load_data_row_handler.h"
#include "observer/table_load/ob_table_load_index_row_handler.h"
#include "observer/table_load/ob_table_load_index_table_projector.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_sstable_scan_merge.h"
#include "src/observer/table_load/ob_table_load_error_row_handler.h"

namespace oceanbase
{
namespace observer
{
using namespace table;

ObTableLoadStoreTableCtx::ObTableLoadStoreTableCtx()
  : allocator_("TLD_STCtx"),
    table_id_(OB_INVALID_ID),
    is_index_table_(false),
    store_ctx_(NULL),
    need_sort_(false),
    schema_(NULL),
    project_(NULL),
    is_fast_heap_table_(false),
    is_multiple_mode_(false),
    insert_table_ctx_(NULL),
    row_handler_(NULL),
    merger_(NULL),
    is_inited_(false)
{
}

ObTableLoadStoreTableCtx::~ObTableLoadStoreTableCtx()
{
  if (OB_NOT_NULL(schema_)) {
    schema_->~ObTableLoadSchema();
    allocator_.free(schema_);
    schema_ = NULL;
  }
  if (OB_NOT_NULL(project_)) {
    project_->~ObTableLoadIndexTableProjector();
    allocator_.free(project_);
    project_ = NULL;
  }
  if (OB_NOT_NULL(insert_table_ctx_)) {
    insert_table_ctx_->~ObDirectLoadInsertTableContext();
    allocator_.free(insert_table_ctx_);
    insert_table_ctx_ = NULL;
  }
  if (OB_NOT_NULL(row_handler_)) {
    row_handler_->~ObDirectLoadDMLRowHandler();
    allocator_.free(row_handler_);
    row_handler_ = NULL;
  }
  if (OB_NOT_NULL(merger_)) {
    merger_->~ObTableLoadMerger();
    allocator_.free(merger_);
    merger_ = NULL;
  }
  FOREACH(iter, index_table_builder_map_) {
    ObIDirectLoadPartitionTableBuilder *table_builder = iter->second;
    table_builder->~ObIDirectLoadPartitionTableBuilder();
    allocator_.free(table_builder);
  }
  index_table_builder_map_.destroy();
}

int ObTableLoadStoreTableCtx::init_table_load_schema()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_ = OB_NEWx(ObTableLoadSchema, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadSchema", KR(ret));
  } else if (OB_FAIL(schema_->init(store_ctx_->ctx_->param_.tenant_id_, table_id_))) {
    LOG_WARN("fail to init schema", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(schema_)) {
      schema_->~ObTableLoadSchema();
      allocator_.free(schema_);
      schema_ = NULL;
    }
  }
  return ret;
}

int ObTableLoadStoreTableCtx::init_index_projector()
{
  int ret = OB_SUCCESS;
  if (is_index_table_) {
    ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema *data_table_schema = nullptr;
    const share::schema::ObTableSchema *index_table_schema = nullptr;
    if (OB_FAIL(
          ObTableLoadSchema::get_schema_guard(store_ctx_->ctx_->param_.tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret));
    } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(
                 schema_guard, store_ctx_->ctx_->param_.tenant_id_,
                 store_ctx_->ctx_->ddl_param_.dest_table_id_, data_table_schema))) {
      LOG_WARN("fail to get table shema of main table", KR(ret));
    } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(schema_guard,
                       store_ctx_->ctx_->param_.tenant_id_, table_id_, index_table_schema))) {
      LOG_WARN("fail to get table shema of index table", KR(ret));
    }
    else if (OB_ISNULL(project_ = OB_NEWx(ObTableLoadIndexTableProjector, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadIndexTableProjector", KR(ret));
    } else if (OB_FAIL(project_->init(data_table_schema, index_table_schema))) {
      LOG_WARN("fail to init ObTableLoadIndexTableProjector", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(project_)) {
        project_->~ObTableLoadIndexTableProjector();
        allocator_.free(project_);
        project_ = NULL;
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTableCtx::init_ls_partition_ids(
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array)
{
  int ret = OB_SUCCESS;
  if (is_index_table_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_id_array.count(); ++i) {
      ObTabletID index_tablet_id;
      ObObjectID index_part_id;
      const ObTabletID &data_tablet_id = partition_id_array[i].part_tablet_id_.tablet_id_;
      if (OB_FAIL(project_->get_index_tablet_id_and_part_id_by_data_tablet_id(
            data_tablet_id, index_tablet_id, index_part_id))) {
        LOG_WARN("fail to get index_tablet_id", KR(ret), K(data_tablet_id));
      } else if (OB_FAIL(ls_partition_ids_.push_back(table::ObTableLoadLSIdAndPartitionId(
                   partition_id_array[i].ls_id_,
                   table::ObTableLoadPartitionId(index_part_id, index_tablet_id))))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_partition_id_array.count(); ++i) {
      ObTabletID index_tablet_id;
      ObObjectID index_part_id;
      const ObTabletID &data_tablet_id = target_partition_id_array[i].part_tablet_id_.tablet_id_;
      if (OB_FAIL(project_->get_index_tablet_id_and_part_id_by_data_tablet_id(
            data_tablet_id, index_tablet_id, index_part_id))) {
        LOG_WARN("fail to get index_tablet_id", KR(ret), K(data_tablet_id));
      } else if (OB_FAIL(target_ls_partition_ids_.push_back(table::ObTableLoadLSIdAndPartitionId(
                   target_partition_id_array[i].ls_id_,
                   table::ObTableLoadPartitionId(index_part_id, index_tablet_id))))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_id_array.count(); ++i) {
      const ObLSID &ls_id = partition_id_array[i].ls_id_;
      const table::ObTableLoadPartitionId &part_tablet_id = partition_id_array[i].part_tablet_id_;
      if (OB_FAIL(ls_partition_ids_.push_back(table::ObTableLoadLSIdAndPartitionId(ls_id, part_tablet_id)))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_partition_id_array.count(); ++i) {
      const ObLSID &ls_id = target_partition_id_array[i].ls_id_;
      const table::ObTableLoadPartitionId &tablet_id = target_partition_id_array[i].part_tablet_id_;
      if (OB_FAIL(target_ls_partition_ids_.push_back(table::ObTableLoadLSIdAndPartitionId(ls_id, tablet_id)))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }

  }
  return ret;
}

int ObTableLoadStoreTableCtx::init_table_data_desc()
{
  int ret = OB_SUCCESS;
  table_data_desc_.rowkey_column_num_ =
    (!schema_->is_heap_table_ ? schema_->rowkey_column_count_ : 0);
  table_data_desc_.column_count_ =
    (!schema_->is_heap_table_ ? schema_->store_column_count_
                                   : schema_->store_column_count_ - 1);

  table_data_desc_.sstable_index_block_size_ =
    ObDirectLoadSSTableIndexBlock::DEFAULT_INDEX_BLOCK_SIZE;
  table_data_desc_.sstable_data_block_size_ = ObDirectLoadSSTableDataBlock::DEFAULT_DATA_BLOCK_SIZE;
  if (!GCTX.is_shared_storage_mode()) {
    table_data_desc_.external_data_block_size_ = ObDirectLoadDataBlock::SN_DEFAULT_DATA_BLOCK_SIZE;
  } else {
    table_data_desc_.external_data_block_size_ = ObDirectLoadDataBlock::SS_DEFAULT_DATA_BLOCK_SIZE;
  }

  table_data_desc_.extra_buf_size_ = ObDirectLoadTableDataDesc::DEFAULT_EXTRA_BUF_SIZE;
  table_data_desc_.compressor_type_ = store_ctx_->ctx_->param_.compressor_type_;
  table_data_desc_.is_heap_table_ = schema_->is_heap_table_;
  table_data_desc_.is_shared_storage_ = GCTX.is_shared_storage_mode();
  table_data_desc_.session_count_ = store_ctx_->ctx_->param_.session_count_;
  table_data_desc_.exe_mode_ = store_ctx_->ctx_->param_.exe_mode_;

  int64_t wa_mem_limit = 0;
  if (table_data_desc_.exe_mode_ == ObTableLoadExeMode::MAX_TYPE) {
    if (OB_FAIL(ObTableLoadService::get_memory_limit(wa_mem_limit))) {
      LOG_WARN("failed to get work area memory limit", KR(ret), K(store_ctx_->ctx_->param_.tenant_id_));
    } else if (wa_mem_limit < ObDirectLoadMemContext::MIN_MEM_LIMIT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wa_mem_limit is too small", KR(ret), K(wa_mem_limit));
    } else {
      table_data_desc_.merge_count_per_round_ =
        min(wa_mem_limit / table_data_desc_.sstable_data_block_size_ / store_ctx_->ctx_->param_.session_count_,
            ObDirectLoadSSTableScanMerge::MAX_SSTABLE_COUNT);
      table_data_desc_.max_mem_chunk_count_ = 128;
      int64_t mem_chunk_size = wa_mem_limit / table_data_desc_.max_mem_chunk_count_;
      if (mem_chunk_size <= ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT) {
        mem_chunk_size = ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT;
        table_data_desc_.max_mem_chunk_count_ = wa_mem_limit / mem_chunk_size;
      }
      table_data_desc_.mem_chunk_size_ = mem_chunk_size;
      table_data_desc_.heap_table_mem_chunk_size_ = wa_mem_limit / store_ctx_->ctx_->param_.session_count_;
    }
    if (OB_SUCC(ret)) {
      if (table_data_desc_.is_heap_table_) {
        int64_t bucket_cnt =
          wa_mem_limit / (store_ctx_->ctx_->param_.session_count_ * MACRO_BLOCK_WRITER_MEM_SIZE);
        if ((ls_partition_ids_.count() <= bucket_cnt) || !need_sort_) {
          is_fast_heap_table_ = true;
        } else {
          is_multiple_mode_ = true;
        }
      } else {
        int64_t bucket_cnt =
          wa_mem_limit / store_ctx_->ctx_->param_.session_count_ /
          (table_data_desc_.sstable_index_block_size_ + table_data_desc_.sstable_data_block_size_);
        is_multiple_mode_ = need_sort_ || ls_partition_ids_.count() > bucket_cnt || is_index_table_;
      }
    }
  } else {
    wa_mem_limit = store_ctx_->ctx_->param_.avail_memory_;
    if (!is_index_table_ && (store_ctx_->ctx_->param_.exe_mode_ == ObTableLoadExeMode::FAST_HEAP_TABLE ||
        store_ctx_->ctx_->param_.exe_mode_ == ObTableLoadExeMode::GENERAL_TABLE_COMPACT)) {
      is_fast_heap_table_ = (store_ctx_->ctx_->param_.exe_mode_ == ObTableLoadExeMode::FAST_HEAP_TABLE);
    } else {
      is_multiple_mode_ = true;
    }
    table_data_desc_.merge_count_per_round_ =
      min(wa_mem_limit / table_data_desc_.sstable_data_block_size_ / store_ctx_->ctx_->param_.session_count_,
          ObDirectLoadSSTableScanMerge::MAX_SSTABLE_COUNT);
    table_data_desc_.max_mem_chunk_count_ =
      wa_mem_limit / ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT;
  }
  if (OB_SUCC(ret)) {
    lob_id_table_data_desc_ = table_data_desc_;
    lob_id_table_data_desc_.rowkey_column_num_ = 1;
    lob_id_table_data_desc_.column_count_ = 1;
    lob_id_table_data_desc_.is_heap_table_ = false;
  }
  return ret;
}

int ObTableLoadStoreTableCtx::init_insert_table_ctx()
{
  int ret = OB_SUCCESS;
  ObDirectLoadInsertTableParam insert_table_param;
  insert_table_param.table_id_ = table_id_;
  insert_table_param.schema_version_ = store_ctx_->ctx_->ddl_param_.schema_version_;
  insert_table_param.snapshot_version_ = store_ctx_->ctx_->ddl_param_.snapshot_version_;
  insert_table_param.ddl_task_id_ = store_ctx_->ctx_->ddl_param_.task_id_;
  insert_table_param.data_version_ = store_ctx_->ctx_->ddl_param_.data_version_;
  insert_table_param.parallel_ = store_ctx_->ctx_->param_.session_count_;
  insert_table_param.reserved_parallel_ = is_fast_heap_table_ ? store_ctx_->ctx_->param_.session_count_ : 0;
  insert_table_param.rowkey_column_count_ = schema_->rowkey_column_count_;
  insert_table_param.column_count_ = schema_->store_column_count_;
  insert_table_param.lob_inrow_threshold_ = schema_->lob_inrow_threshold_;
  insert_table_param.is_partitioned_table_ = schema_->is_partitioned_table_;
  insert_table_param.is_heap_table_ = schema_->is_heap_table_;
  insert_table_param.is_column_store_ = schema_->is_column_store_;
  insert_table_param.online_opt_stat_gather_ = store_ctx_->ctx_->param_.online_opt_stat_gather_ && !is_index_table_;
  insert_table_param.reuse_pk_ = !ObDirectLoadInsertMode::is_overwrite_mode(store_ctx_->ctx_->param_.insert_mode_);
  insert_table_param.is_incremental_ = ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_);
  insert_table_param.trans_param_ = store_ctx_->trans_param_;
  insert_table_param.datum_utils_ = &(schema_->datum_utils_);
  insert_table_param.col_descs_ = &(schema_->column_descs_);
  insert_table_param.cmp_funcs_ = &(schema_->cmp_funcs_);
  insert_table_param.lob_column_idxs_ = &(schema_->lob_column_idxs_);
  insert_table_param.online_sample_percent_ = store_ctx_->ctx_->param_.online_sample_percent_;
  insert_table_param.is_no_logging_ = store_ctx_->ctx_->ddl_param_.is_no_logging_;
  insert_table_param.max_batch_size_ = store_ctx_->ctx_->param_.batch_size_;
  if (OB_ISNULL(insert_table_ctx_ =
                      OB_NEWx(ObDirectLoadInsertTableContext, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadInsertTableContext", KR(ret));
  } else if (OB_FAIL(insert_table_ctx_->init(insert_table_param, ls_partition_ids_, target_ls_partition_ids_))) {
    LOG_WARN("fail to init insert table ctx", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(insert_table_ctx_)) {
      insert_table_ctx_->~ObDirectLoadInsertTableContext();
      allocator_.free(insert_table_ctx_);
      insert_table_ctx_ = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStoreTableCtx::init_row_handler()
{
  int ret = OB_SUCCESS;
  if (!is_index_table_) {
    ObTableLoadDataRowHandler *main_row_handler = nullptr;
    if (OB_ISNULL(main_row_handler = OB_NEWx(ObTableLoadDataRowHandler, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadDataRowHandler", KR(ret));
    } else if (OB_FAIL(main_row_handler->init(store_ctx_->ctx_->param_, store_ctx_->result_info_,
                                              store_ctx_->error_row_handler_,
                                              &(store_ctx_->index_store_table_ctxs_)))) {
      LOG_WARN("fail to init row handler", KR(ret), KPC(store_ctx_), K(is_index_table),
               K(schema_->index_table_count_), KP(project_));
    } else {
      row_handler_ = main_row_handler;
    }
  } else {
    ObTableLoadIndexRowHandler *index_row_handler = nullptr;
    if (OB_ISNULL(index_row_handler = OB_NEWx(ObTableLoadIndexRowHandler, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadIndexRowHandler", KR(ret));
    } else {
      row_handler_ = index_row_handler;
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(row_handler_)) {
      row_handler_->~ObDirectLoadDMLRowHandler();
      allocator_.free(row_handler_);
      row_handler_ = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStoreTableCtx::init_merger()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merger_ = OB_NEWx(ObTableLoadMerger, (&allocator_), store_ctx_, this))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadMerger", KR(ret));
  } else if (OB_FAIL(merger_->init())) {
    LOG_WARN("fail to init ObTableLoadMerger", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(merger_)) {
      merger_->~ObTableLoadMerger();
      allocator_.free(merger_);
      merger_ = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStoreTableCtx::init(
  ObTableID table_id, bool is_index_table, ObTableLoadStoreCtx *store_ctx,
  const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
  const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadStoreTableCtx has been inited", KR(ret));
  } else if (OB_ISNULL(store_ctx) || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(store_ctx), K(table_id));
  } else if (OB_FAIL(index_table_builder_map_.create(1024, "TLD_IdxBd", "TLD_IdxBd", MTL_ID()))) {
    LOG_WARN("fail to create index_table_builder_map_", KR(ret));
  } else {
    table_id_ = table_id;
    is_index_table_ = is_index_table;
    store_ctx_ = store_ctx;
    if (is_index_table) {
      need_sort_ = true;
    } else {
      need_sort_ = (ObTableLoadExeMode::MEM_COMPACT == store_ctx_->ctx_->param_.exe_mode_) ||
                   (ObTableLoadExeMode::MULTIPLE_HEAP_TABLE_COMPACT == store_ctx_->ctx_->param_.exe_mode_);
    }
    if (OB_FAIL(init_table_load_schema())) {
      LOG_WARN("fail to init table load schema", KR(ret));
    } else if (OB_FAIL(init_index_projector())) {
      LOG_WARN("fail to init index projector", KR(ret));
    } else if (OB_FAIL(init_ls_partition_ids(partition_id_array, target_partition_id_array))) {
      LOG_WARN("fail to init ls_partition_ids", KR(ret));
    } else if (OB_FAIL(init_table_data_desc())) {
      LOG_WARN("fail to init table data desc", KR(ret));
    } else if (OB_FAIL(init_insert_table_ctx())) {
      LOG_WARN("fail to init insert table ctx", KR(ret));
    } else if (OB_FAIL(init_row_handler())) {
      LOG_WARN("fail to init row handler", KR(ret));
    } else if (OB_FAIL(init_merger())) {
      LOG_WARN("fail to init merger", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadStoreTableCtx::close_index_table_builder()
{
  int ret = OB_SUCCESS;
  FOREACH_X(item, index_table_builder_map_, OB_SUCC(ret)) {
    if (item->second != nullptr) {
      if (OB_FAIL(item->second->close())) {
        LOG_WARN("fail to close table", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTableCtx::get_index_table_builder(ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    const int64_t part_id = get_tid_cache();
    if (OB_FAIL(index_table_builder_map_.get_refactored(get_tid_cache(), table_builder))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get table builder", KR(ret), K(get_tid_cache()));
      } else {
        ret = OB_SUCCESS;
        ObDirectLoadExternalMultiPartitionTableBuildParam builder_param;
        builder_param.table_data_desc_ = table_data_desc_;
        builder_param.datum_utils_ = &(schema_->datum_utils_);
        builder_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
        builder_param.extra_buf_ = reinterpret_cast<char *>(1);     // unuse, delete in future
        builder_param.extra_buf_size_ = 4096;                       // unuse, delete in future
        {
          ObMutexGuard guard(mutex_);
          // use mutext to avoid thread unsafe allocator;
          ObDirectLoadExternalMultiPartitionTableBuilder *new_builder = nullptr;
          if (OB_ISNULL(new_builder = OB_NEWx(
              ObDirectLoadExternalMultiPartitionTableBuilder, &allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to new ObDirectLoadExternalMultiPartitionTableBuilder", KR(ret));
          } else if (OB_FAIL(new_builder->init(builder_param))) {
            LOG_WARN("fail to init new_builder", KR(ret));
          } else if (OB_FAIL(index_table_builder_map_.set_refactored(part_id, new_builder))) {
            LOG_WARN("fail to set table_builder_map_", KR(ret), K(part_id));
          } else {
            table_builder = new_builder;
          }
          if (OB_FAIL(ret)) {
            if (nullptr != new_builder) {
              new_builder->~ObDirectLoadExternalMultiPartitionTableBuilder();
              allocator_.free(new_builder);
              new_builder = nullptr;
            }
          }
        }
      }
    }
  }
  return ret;
}

}
}