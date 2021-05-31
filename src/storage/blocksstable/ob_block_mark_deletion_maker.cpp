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

#include "storage/blocksstable/ob_block_mark_deletion_maker.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_partition.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace storage;
using namespace memtable;
namespace blocksstable {

ObBlockMarkDeletionMaker::ObBlockMarkDeletionMaker()
    : is_inited_(false),
      access_param_(),
      access_context_(),
      allocator_(ObModIds::OB_BLOCK_MARK_DELETION_MAKER),
      stmt_allocator_(ObModIds::OB_BLOCK_MARK_DELETION_MAKER),
      ctx_(),
      block_cache_ws_(),
      scan_merge_(),
      out_cols_project_(),
      tables_handle_(),
      ctx_factory_(NULL),
      mem_ctx_(NULL),
      col_params_()
{}

ObBlockMarkDeletionMaker::~ObBlockMarkDeletionMaker()
{
  reset();
}

// get information by partitions
int ObBlockMarkDeletionMaker::init(const ObTableSchema& table_schema, const ObPartitionKey& pkey,
    const uint64_t index_id, const int64_t snapshot_version, const uint64_t end_log_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_tables_(pkey, index_id, snapshot_version, end_log_id))) {
    STORAGE_LOG(WARN, "failed to prepare tables", K(ret), K(pkey), K(index_id), K(snapshot_version));
  } else if (OB_FAIL(init_(table_schema,
                 pkey,
                 index_id,
                 snapshot_version,
                 ObPartitionService::get_instance().get_trans_service()->get_mem_ctx_factory()))) {
    STORAGE_LOG(WARN, "fail to init block mark deletion", K(ret));
  }
  return ret;
}

int ObBlockMarkDeletionMaker::init_(const ObTableSchema& table_schema, const ObPartitionKey& pkey,
    const uint64_t index_id, const int64_t snapshot_version, memtable::ObIMemtableCtxFactory* ctx_factory)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "micro block delete tag init twice", K(ret));
  } else if (!table_schema.is_valid() || !pkey.is_valid() || OB_INVALID_ID == index_id || snapshot_version <= 0 ||
             INT64_MAX == snapshot_version || NULL == ctx_factory) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "column ids should not empty",
        K(ret),
        K(table_schema),
        K(pkey),
        K(index_id),
        K(snapshot_version),
        KP(ctx_factory));
  } else {
    int64_t read_snapshot = snapshot_version;
    ObSEArray<ObColDesc, OB_DEFAULT_SE_ARRAY_COUNT> column_ids;
    if (OB_FAIL(table_schema.get_rowkey_column_ids(column_ids))) {
      STORAGE_LOG(WARN, "fail to get column ids. ", K(ret));
    } else {
      for (int32_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        if (OB_FAIL(out_cols_project_.push_back(i))) {
          STORAGE_LOG(WARN, "fail to push column index into out cols project", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // Build out_cols_param
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        const ObColumnSchemaV2* col = table_schema.get_column_schema(column_ids.at(i).col_id_);
        if (NULL == col) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "fail get col schema", K(ret), KP(col));
        } else {
          void* buf = stmt_allocator_.alloc(sizeof(ObColumnParam));
          if (NULL == buf) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
          } else {
            ObColumnParam* col_param = new (buf) ObColumnParam(stmt_allocator_);
            if (OB_FAIL(ObTableParam::convert_column_schema_to_param(*col, *col_param))) {
              STORAGE_LOG(WARN, "fail to convert to schema column to column parameter", K(ret));
            } else if (OB_FAIL(col_params_.push_back(col_param))) {
              STORAGE_LOG(WARN, "push array failed", K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObQueryFlag query_flag(ObQueryFlag::Forward,  // scan_order
          false,                                    // daily_merge
          false,                                    // optimize
          false,                                    // sys scan
          false,                                    // full_row
          false,                                    // index_back
          false,                                    // query_stat
          ObQueryFlag::MysqlMode,                   // sql_mode
          true                                      // read_latest
      );
      query_flag.set_not_use_row_cache();
      query_flag.set_not_use_block_cache();
      query_flag.set_iter_uncommitted_row();
      common::ObVersionRange trans_version_range;
      // init access_param

      if (OB_FAIL(access_param_.out_col_desc_param_.init(nullptr))) {
        STORAGE_LOG(WARN, "init out cols fail", K(ret));
      } else if (OB_FAIL(access_param_.out_col_desc_param_.assign(column_ids))) {
        STORAGE_LOG(WARN, "assign out cols fail", K(ret));
      } else {
        access_param_.iter_param_.table_id_ = table_schema.get_table_id();
        access_param_.iter_param_.rowkey_cnt_ = table_schema.get_rowkey_column_num();
        access_param_.iter_param_.schema_version_ = table_schema.get_schema_version();
        access_param_.iter_param_.out_cols_project_ = &out_cols_project_;
        access_param_.iter_param_.out_cols_ = &access_param_.out_col_desc_param_.get_col_descs();
        access_param_.out_cols_param_ = &col_params_;
        // init access_context
        trans_version_range.snapshot_version_ = read_snapshot;
        trans_version_range.multi_version_start_ = read_snapshot;
        trans_version_range.base_version_ = 0;
        ctx_factory_ = ctx_factory;
        mem_ctx_ = ctx_factory_->alloc();
        if (OB_ISNULL(mem_ctx_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc mem ctx", K(ret), KP(mem_ctx_));
        } else {
          ctx_.mem_ctx_ = mem_ctx_;
        }
      }
      if (OB_SUCC(ret)) {
        ObPartitionKey pg_key;
        if (OB_FAIL(ctx_.mem_ctx_->trans_begin())) {
          STORAGE_LOG(WARN, "fail to begin transaction", K(ret));
        } else if (OB_FAIL(ctx_.mem_ctx_->sub_trans_begin(
                       read_snapshot, MERGE_READ_SNAPSHOT_VERSION, true /*safe read*/))) {
          STORAGE_LOG(WARN, "fail to begin sub transaction", K(ret));
        } else if (OB_FAIL(block_cache_ws_.init(extract_tenant_id(table_schema.get_table_id())))) {
          STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(extract_tenant_id(table_schema.get_table_id())));
        } else if (OB_FAIL(ObPartitionService::get_instance().get_pg_key(pkey, pg_key))) {
          STORAGE_LOG(WARN, "failed to get_pg_key", K(ret), K(pkey));
        } else if (OB_FAIL(ctx_.init_trans_ctx_mgr(pg_key))) {
          STORAGE_LOG(WARN, "failed to init_ctx_mgr", K(ret), K(pkey));
        } else if (OB_FAIL(access_context_.init(
                       query_flag, ctx_, allocator_, stmt_allocator_, block_cache_ws_, trans_version_range))) {
          STORAGE_LOG(WARN, "failed to init access context", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObGetTableParam get_table_param;
      get_table_param.tables_handle_ = &tables_handle_;
      if (!access_param_.is_valid() || !access_context_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "access param or access context is not valid", K(ret), K(access_param_), K(access_context_));
      } else if (OB_FAIL(scan_merge_.init(access_param_, access_context_, get_table_param))) {
        STORAGE_LOG(WARN, "fail to init multiple scan merge", K(ret));
      } else {
        pkey_ = pkey;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObBlockMarkDeletionMaker::prepare_tables_(const common::ObPartitionKey& pkey, const uint64_t index_id,
    const int64_t snapshot_version, const uint64_t end_log_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObPartitionStorage* partition_storage = NULL;

  tables_handle_.reset();
  if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(pkey, pg_partition_guard)) ||
             OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    STORAGE_LOG(WARN, "fail to get pg partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition_storage = reinterpret_cast<ObPartitionStorage*>(
                           pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition storage should not be null", K(ret));
  } else if (OB_FAIL(partition_storage->get_partition_store().get_mark_deletion_tables(
                 index_id, end_log_id, snapshot_version, tables_handle_))) {
    STORAGE_LOG(WARN, "fail to get all tables", K(ret));
  } else if (tables_handle_.get_count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tables handle count should not be zero", K(ret), K(tables_handle_.get_count()));
  }
  return ret;
}

int ObBlockMarkDeletionMaker::can_mark_delete(bool& can_mark_deletion, ObExtStoreRange& range)
{
  int ret = OB_SUCCESS;
  ObStoreRow* store_row = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "micro block mark delete maker do not init", K(ret));
  } else if (!range.get_range().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ext range is invalid", K(ret), K(range));
  } else if (OB_FAIL(scan_merge_.open(range))) {
    STORAGE_LOG(WARN, "fail to open multi scan merge", K(ret), K(range));
  } else if (OB_FAIL(scan_merge_.get_next_row(store_row))) {
    if (OB_ITER_END == ret) {
      can_mark_deletion = true;
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to get next row", K(ret));
    }
  } else {  // Explain that there is data, you can not mark the mark for deletion
    can_mark_deletion = false;
  }
  scan_merge_.reuse();
  allocator_.reuse();
  return ret;
}

void ObBlockMarkDeletionMaker::reset()
{
  is_inited_ = false;
  scan_merge_.reset();
  access_param_.reset();
  access_context_.reset();
  allocator_.reset();
  stmt_allocator_.reset();
  ctx_.reset();
  block_cache_ws_.reset();
  out_cols_project_.reset();
  if (NULL != mem_ctx_) {
    ctx_factory_->free(mem_ctx_);
    mem_ctx_ = NULL;
  }
  col_params_.reset();
  tables_handle_.reset();
}

}  // namespace blocksstable
}  // namespace oceanbase
