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

#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_merger.h"
#include "observer/table_load/ob_table_load_store_trans.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "share/ob_autoincrement_service.h"
#include "share/sequence/ob_sequence_cache.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "storage/direct_load/ob_direct_load_data_block.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block.h"
#include "storage/direct_load/ob_direct_load_sstable_index_block.h"
#include "storage/direct_load/ob_direct_load_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace lib;
using namespace table;
using namespace storage;
using namespace share;

ObTableLoadStoreCtx::ObTableLoadStoreCtx(ObTableLoadTableCtx *ctx)
  : ctx_(ctx),
    allocator_("TLD_StoreCtx"),
    task_scheduler_(nullptr),
    merger_(nullptr),
    insert_table_ctx_(nullptr),
    next_tablet_idx_(0),
    opened_insert_tablet_count_(0),
    is_multiple_mode_(false),
    is_fast_heap_table_(false),
    px_writer_count_(0),
    tmp_file_mgr_(nullptr),
    error_row_handler_(nullptr),
    sequence_schema_(&allocator_),
    next_session_id_(0),
    status_(ObTableLoadStatusType::NONE),
    error_code_(OB_SUCCESS),
    last_heart_beat_ts_(0),
    enable_heart_beat_check_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  ls_partition_ids_.set_tenant_id(MTL_ID());
  target_ls_partition_ids_.set_tenant_id(MTL_ID());
  committed_trans_store_array_.set_tenant_id(MTL_ID());
}

ObTableLoadStoreCtx::~ObTableLoadStoreCtx()
{
  destroy();
}

int ObTableLoadStoreCtx::init(
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &partition_id_array,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_partition_id_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadStoreCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(partition_id_array.empty() || target_partition_id_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(partition_id_array), K(target_partition_id_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_id_array.count(); ++i) {
      const ObLSID &ls_id = partition_id_array[i].ls_id_;
      const ObTableLoadPartitionId &part_tablet_id = partition_id_array[i].part_tablet_id_;
      if (OB_FAIL(ls_partition_ids_.push_back(ObTableLoadLSIdAndPartitionId(ls_id, part_tablet_id)))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_partition_id_array.count(); ++i) {
      const ObLSID &ls_id = target_partition_id_array[i].ls_id_;
      const ObTableLoadPartitionId &tablet_id = target_partition_id_array[i].part_tablet_id_;
      if (OB_FAIL(target_ls_partition_ids_.push_back(ObTableLoadLSIdAndPartitionId(ls_id, tablet_id)))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      table_data_desc_.rowkey_column_num_ =
        (!ctx_->schema_.is_heap_table_ ? ctx_->schema_.rowkey_column_count_ : 0);
      table_data_desc_.column_count_ = ctx_->param_.column_count_;
      table_data_desc_.external_data_block_size_ = ObDirectLoadDataBlock::DEFAULT_DATA_BLOCK_SIZE;
      table_data_desc_.sstable_index_block_size_ =
        ObDirectLoadSSTableIndexBlock::DEFAULT_INDEX_BLOCK_SIZE;
      table_data_desc_.sstable_data_block_size_ =
        ObDirectLoadSSTableDataBlock::DEFAULT_DATA_BLOCK_SIZE;
      table_data_desc_.extra_buf_size_ = ObDirectLoadTableDataDesc::DEFAULT_EXTRA_BUF_SIZE;
      table_data_desc_.compressor_type_ = ctx_->param_.compressor_type_;
      table_data_desc_.is_heap_table_ = ctx_->schema_.is_heap_table_;
      table_data_desc_.session_count_ = ctx_->param_.session_count_;
      table_data_desc_.exe_mode_ = ctx_->param_.exe_mode_;

      int64_t wa_mem_limit = 0;
      if (table_data_desc_.exe_mode_ == ObTableLoadExeMode::MAX_TYPE) {
        if (OB_FAIL(ObTableLoadService::get_memory_limit(wa_mem_limit))) {
          LOG_WARN("failed to get work area memory limit", KR(ret), K(ctx_->param_.tenant_id_));
        } else if (wa_mem_limit < ObDirectLoadMemContext::MIN_MEM_LIMIT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("wa_mem_limit is too small", KR(ret), K(wa_mem_limit));
        } else {
          table_data_desc_.merge_count_per_round_ = min(wa_mem_limit / table_data_desc_.sstable_data_block_size_ / ctx_->param_.session_count_,
                                                        ObDirectLoadSSTableScanMerge::MAX_SSTABLE_COUNT);
          table_data_desc_.max_mem_chunk_count_ = 128;
          int64_t mem_chunk_size = wa_mem_limit / table_data_desc_.max_mem_chunk_count_;
          if (mem_chunk_size <= ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT) {
            mem_chunk_size = ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT;
            table_data_desc_.max_mem_chunk_count_ = wa_mem_limit / mem_chunk_size;
          }
          table_data_desc_.mem_chunk_size_ = mem_chunk_size;
          table_data_desc_.heap_table_mem_chunk_size_ = wa_mem_limit / ctx_->param_.session_count_;
        }
        if (OB_SUCC(ret)) {
          if (table_data_desc_.is_heap_table_) {
            int64_t bucket_cnt = wa_mem_limit / (ctx_->param_.session_count_ * MACRO_BLOCK_WRITER_MEM_SIZE);
            if ((ls_partition_ids_.count() <= bucket_cnt) || !ctx_->param_.need_sort_) {
              is_fast_heap_table_ = true;
            } else {
              is_multiple_mode_ = true;
            }
          } else {
            int64_t bucket_cnt = wa_mem_limit / ctx_->param_.session_count_ /
                                 (table_data_desc_.sstable_index_block_size_ + table_data_desc_.sstable_data_block_size_);
            is_multiple_mode_ = ctx_->param_.need_sort_ || ls_partition_ids_.count() > bucket_cnt;
          }
        }
      } else {
        wa_mem_limit = ctx_->param_.avail_memory_;
        if (ctx_->param_.exe_mode_ == ObTableLoadExeMode::FAST_HEAP_TABLE ||
            ctx_->param_.exe_mode_ == ObTableLoadExeMode::GENERAL_TABLE_COMPACT) {
          is_fast_heap_table_ = (ctx_->param_.exe_mode_ == ObTableLoadExeMode::FAST_HEAP_TABLE);
        } else {
          is_multiple_mode_ = true;
        }
        table_data_desc_.merge_count_per_round_ = min(wa_mem_limit / table_data_desc_.sstable_data_block_size_ / ctx_->param_.session_count_,
                                                      ObDirectLoadSSTableScanMerge::MAX_SSTABLE_COUNT);
        table_data_desc_.max_mem_chunk_count_ = wa_mem_limit / ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT;
      }
      if (OB_SUCC(ret)) {
        lob_id_table_data_desc_ = table_data_desc_;
        lob_id_table_data_desc_.rowkey_column_num_ = 1;
        lob_id_table_data_desc_.column_count_ = 1;
        lob_id_table_data_desc_.is_heap_table_ = false;
      }
    }
    if (OB_FAIL(ret)) {
    }
    // init trans_param_
    else if (ObDirectLoadMethod::is_incremental(ctx_->param_.method_) && OB_FAIL(init_trans_param())) {
      LOG_WARN("fail to init trans param", KR(ret));
    }
    // init trans_allocator_
    else if (OB_FAIL(trans_allocator_.init("TLD_STransPool", ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to init trans allocator", KR(ret));
    }
    // init trans_map_
    else if (OB_FAIL(trans_map_.create(1024, "TLD_STransMap", "TLD_STransMap",
                                       ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to create trans map", KR(ret));
    }
    // init trans_ctx_map_
    else if (OB_FAIL(trans_ctx_map_.create(1024, "TLD_TCtxMap", "TLD_TCtxMap",
                                           ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to create trans ctx map", KR(ret));
    }
    // init segment_trans_ctx_map_
    else if (OB_FAIL(segment_ctx_map_.init("TLD_SegCtxMap", ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to init segment ctx map", KR(ret));
    }
    // 初始化task_scheduler_
    else if (OB_ISNULL(task_scheduler_ = OB_NEWx(ObTableLoadTaskThreadPoolScheduler, (&allocator_),
                                                 ctx_->param_.session_count_, ctx_->param_.table_id_, "Store"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadTaskThreadPoolScheduler", KR(ret));
    } else if (OB_FAIL(task_scheduler_->init())) {
      LOG_WARN("fail to init task scheduler", KR(ret));
    } else if (OB_FAIL(task_scheduler_->start())) {
      LOG_WARN("fail to start task scheduler", KR(ret));
    }
    // 初始化merger_
    else if (OB_ISNULL(merger_ = OB_NEWx(ObTableLoadMerger, (&allocator_), this))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadMerger", KR(ret));
    } else if (OB_FAIL(merger_->init())) {
      LOG_WARN("fail to init merger", KR(ret));
    }
    // init insert_table_ctx_
    if (OB_SUCC(ret)) {
      ObDirectLoadInsertTableParam insert_table_param;
      insert_table_param.table_id_ = ctx_->ddl_param_.dest_table_id_;
      insert_table_param.schema_version_ = ctx_->ddl_param_.schema_version_;
      insert_table_param.snapshot_version_ = ctx_->ddl_param_.snapshot_version_;
      insert_table_param.ddl_task_id_ = ctx_->ddl_param_.task_id_;
      insert_table_param.data_version_ = ctx_->ddl_param_.data_version_;
      insert_table_param.parallel_ = ctx_->param_.session_count_;
      insert_table_param.reserved_parallel_ = is_fast_heap_table_ ? ctx_->param_.session_count_ : 0;
      insert_table_param.rowkey_column_count_ = ctx_->schema_.rowkey_column_count_;
      insert_table_param.column_count_ = ctx_->schema_.store_column_count_;
      insert_table_param.lob_column_count_ = ctx_->schema_.lob_column_idxs_.count();
      insert_table_param.is_partitioned_table_ = ctx_->schema_.is_partitioned_table_;
      insert_table_param.is_heap_table_ = ctx_->schema_.is_heap_table_;
      insert_table_param.is_column_store_ = ctx_->schema_.is_column_store_;
      insert_table_param.online_opt_stat_gather_ = ctx_->param_.online_opt_stat_gather_;
      insert_table_param.is_incremental_ = ObDirectLoadMethod::is_incremental(ctx_->param_.method_);
      insert_table_param.trans_param_ = trans_param_;
      insert_table_param.datum_utils_ = &(ctx_->schema_.datum_utils_);
      insert_table_param.col_descs_ = &(ctx_->schema_.column_descs_);
      insert_table_param.cmp_funcs_ = &(ctx_->schema_.cmp_funcs_);
      insert_table_param.online_sample_percent_ = ctx_->param_.online_sample_percent_;
      if (OB_ISNULL(insert_table_ctx_ =
                         OB_NEWx(ObDirectLoadInsertTableContext, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadInsertTableContext", KR(ret));
      } else if (OB_FAIL(insert_table_ctx_->init(insert_table_param, ls_partition_ids_, target_ls_partition_ids_))) {
        LOG_WARN("fail to init insert table ctx", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    }
    // init tmp_file_mgr_
    else if (OB_ISNULL(tmp_file_mgr_ = OB_NEWx(ObDirectLoadTmpFileManager, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadTmpFileManager", KR(ret));
    } else if (OB_FAIL(tmp_file_mgr_->init(ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to init tmp file manager", KR(ret));
    }
    // init error_row_handler_
    else if (OB_ISNULL(error_row_handler_ =
                         OB_NEWx(ObTableLoadErrorRowHandler, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadErrorRowHandler", KR(ret));
    } else if (OB_FAIL(error_row_handler_->init(ctx_->param_, result_info_, ctx_->job_stat_))) {
      LOG_WARN("fail to init error row handler", KR(ret));
    }
    // init session_ctx_array_
    else if (OB_FAIL(init_session_ctx_array())) {
      LOG_WARN("fail to init session ctx array", KR(ret));
    }
    // init sequence_cache_ and sequence_schema_
    else if (ctx_->schema_.has_identity_column_ && OB_FAIL(init_sequence())) {
      LOG_WARN("fail to init sequence", KR(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    } else {
      destroy();
    }
  }
  return ret;
}

void ObTableLoadStoreCtx::stop()
{
  if (nullptr != merger_) {
    merger_->stop();
  }
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
  }
  LOG_INFO("store ctx stop succ");
}

void ObTableLoadStoreCtx::destroy()
{
  if (nullptr != merger_) {
    merger_->~ObTableLoadMerger();
    allocator_.free(merger_);
    merger_ = nullptr;
  }
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
    task_scheduler_->~ObITableLoadTaskScheduler();
    allocator_.free(task_scheduler_);
    task_scheduler_ = nullptr;
  }
  for (TransMap::const_iterator iter = trans_map_.begin(); iter != trans_map_.end(); ++iter) {
    ObTableLoadStoreTrans *trans = iter->second;
    abort_unless(0 == trans->get_ref_count());
    trans_allocator_.free(trans);
  }
  trans_map_.reuse();
  for (int64_t i = 0; i < committed_trans_store_array_.count(); ++i) {
    ObTableLoadTransStore *trans_store = committed_trans_store_array_.at(i);
    ObTableLoadTransCtx *trans_ctx = trans_store->trans_ctx_;
    trans_store->~ObTableLoadTransStore();
    trans_ctx->allocator_.free(trans_store);
  }
  committed_trans_store_array_.reset();
  for (TransCtxMap::const_iterator iter = trans_ctx_map_.begin(); iter != trans_ctx_map_.end();
       ++iter) {
    ObTableLoadTransCtx *trans_ctx = iter->second;
    ctx_->free_trans_ctx(trans_ctx);
  }
  trans_ctx_map_.reuse();
  if (nullptr != insert_table_ctx_) {
    insert_table_ctx_->~ObDirectLoadInsertTableContext();
    allocator_.free(insert_table_ctx_);
    insert_table_ctx_ = nullptr;
  }
  if (nullptr != tmp_file_mgr_) {
    tmp_file_mgr_->~ObDirectLoadTmpFileManager();
    allocator_.free(tmp_file_mgr_);
    tmp_file_mgr_ = nullptr;
  }
  if (nullptr != error_row_handler_) {
    error_row_handler_->~ObTableLoadErrorRowHandler();
    allocator_.free(error_row_handler_);
    error_row_handler_ = nullptr;
  }
}

int ObTableLoadStoreCtx::advance_status(ObTableLoadStatusType status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLoadStatusType::NONE == status || ObTableLoadStatusType::ERROR == status ||
                  ObTableLoadStatusType::ABORT == status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(status));
  } else {
    obsys::ObWLockGuard guard(status_lock_);
    if (OB_UNLIKELY(ObTableLoadStatusType::ERROR == status_)) {
      ret = error_code_;
      LOG_WARN("store has error", KR(ret));
    } else if (OB_UNLIKELY(ObTableLoadStatusType::ABORT == status_)) {
      ret = OB_CANCELED;
      LOG_WARN("store is abort", KR(ret));
    }
    // normally, the state is advanced step by step
    else if (OB_UNLIKELY(static_cast<int64_t>(status) != static_cast<int64_t>(status_) + 1)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("unexpected status", KR(ret), K(status), K(status_));
    }
    // advance status
    else {
      status_ = status;
      table_load_status_to_string(status_, ctx_->job_stat_->store_.status_);
      LOG_INFO("LOAD DATA STORE advance status", K(status));
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::set_status_error(int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS == error_code)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(error_code));
  } else {
    obsys::ObWLockGuard guard(status_lock_);
    if (static_cast<int64_t>(status_) >= static_cast<int64_t>(ObTableLoadStatusType::ERROR)) {
      // ignore
    } else {
      status_ = ObTableLoadStatusType::ERROR;
      error_code_ = error_code;
      table_load_status_to_string(status_, ctx_->job_stat_->store_.status_);
      LOG_INFO("LOAD DATA STORE status error", KR(error_code));
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::set_status_abort()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(status_lock_);
  if (ObTableLoadStatusType::ABORT == status_) {
    LOG_INFO("LOAD DATA STORE already abort");
  } else {
    status_ = ObTableLoadStatusType::ABORT;
    table_load_status_to_string(status_, ctx_->job_stat_->store_.status_);
    LOG_INFO("LOAD DATA STORE status abort");
  }
  return ret;
}

int ObTableLoadStoreCtx::check_status(ObTableLoadStatusType status) const
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard guard(status_lock_);
  if (OB_UNLIKELY(status != status_)) {
    if (ObTableLoadStatusType::ERROR == status_) {
      ret = error_code_;
    } else if (ObTableLoadStatusType::ABORT == status_) {
      ret = OB_SUCCESS != error_code_ ? error_code_ : OB_CANCELED;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
  }
  return ret;
}

void ObTableLoadStoreCtx::heart_beat()
{
  last_heart_beat_ts_ = ObTimeUtil::current_time();
}

bool ObTableLoadStoreCtx::check_heart_beat_expired(const uint64_t expired_time_us)
{
  return ObTimeUtil::current_time() > (last_heart_beat_ts_ + expired_time_us);
}

int ObTableLoadStoreCtx::init_trans_param()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = nullptr;
  transaction::ObTxDesc *tx_desc = nullptr;
  trans_param_.reset();
  if (OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret));
  } else if (OB_UNLIKELY(!session_info->is_in_transaction())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not in transaction", KR(ret));
  } else if (OB_ISNULL(tx_desc = session_info->get_tx_desc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tx desc", KR(ret));
  } else {
    trans_param_.tx_desc_ = tx_desc;
    trans_param_.tx_id_ = tx_desc->get_tx_id();
    trans_param_.tx_seq_ = tx_desc->inc_and_get_tx_seq(0);
    LOG_INFO("init trans param", K(trans_param_));
  }
  return ret;
}

int ObTableLoadStoreCtx::generate_autoinc_params(AutoincParam &autoinc_param)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(ObTableLoadSchema::get_table_schema(ctx_->param_.tenant_id_,
                                                  ctx_->param_.table_id_,
                                                  schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(ctx_->param_.tenant_id_),
                                         K(ctx_->param_.table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(ctx_->param_.tenant_id_), K(ctx_->param_.table_id_));
  } else {
    //ddl对于auto increment是最后进行自增值同步，对于autoinc_param参数初始化得使用原表table id的table schema
    ObColumnSchemaV2 *autoinc_column_schema = nullptr;
    uint64_t column_id = 0;
    for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
         OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
      ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid column schema", KR(ret), KP(column_schema));
      } else {
        column_id = column_schema->get_column_id();
        if (column_schema->is_autoincrement() && column_id != OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
          autoinc_column_schema = column_schema;
          break;
        }
      }
    }//end for
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(autoinc_column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null autoinc column schema", KR(ret), KP(autoinc_column_schema));
      } else {
        autoinc_param.tenant_id_ = ctx_->param_.tenant_id_;
        autoinc_param.autoinc_table_id_ = ctx_->param_.table_id_;
        autoinc_param.autoinc_first_part_num_ = table_schema->get_first_part_num();
        autoinc_param.autoinc_table_part_num_ = table_schema->get_all_part_num();
        autoinc_param.autoinc_col_id_ = column_id;
        autoinc_param.auto_increment_cache_size_ = MAX_INCREMENT_CACHE_SIZE;
        autoinc_param.part_level_ = table_schema->get_part_level();
        autoinc_param.autoinc_col_type_ = autoinc_column_schema->get_data_type();
        autoinc_param.total_value_count_ = 1;
        autoinc_param.autoinc_desired_count_ = 0;
        autoinc_param.autoinc_mode_is_order_ = table_schema->is_order_auto_increment_mode();
        autoinc_param.autoinc_auto_increment_ = table_schema->get_auto_increment();
        autoinc_param.autoinc_increment_ = 1;
        autoinc_param.autoinc_offset_ = 1;
        autoinc_param.part_value_no_order_ = true;
        if (autoinc_column_schema->is_tbl_part_key_column()) {
          // don't keep intra-partition value asc order when partkey column is auto inc
          autoinc_param.part_value_no_order_ = true;
        }
        autoinc_param.autoinc_version_ = table_schema->get_truncate_version();
      }
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::init_sequence()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx_->param_.tenant_id_;
  const uint64_t table_id = ctx_->ddl_param_.dest_table_id_;
  share::schema::ObSchemaGetterGuard table_schema_guard;
  share::schema::ObSchemaGetterGuard sequence_schema_guard;
  const ObSequenceSchema *sequence_schema = nullptr;
  const ObTableSchema *target_table_schema = nullptr;
  uint64_t sequence_id = OB_INVALID_ID;
  if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id, table_id, table_schema_guard,
                                                  target_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else {
    //ddl对于identity是建表的时候进行自增值同步，对于sequence参数初始化得用隐藏表table id的table schema
    for (ObTableSchema::const_column_iterator iter = target_table_schema->column_begin();
          OB_SUCC(ret) && iter != target_table_schema->column_end(); ++iter) {
      ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid column schema", K(column_schema));
      } else {
        uint64_t column_id = column_schema->get_column_id();
        if (column_schema->is_identity_column() && column_id != OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
          sequence_id = column_schema->get_sequence_id();
          break;
        }
      }
    }//end for
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                     tenant_id,
                     sequence_schema_guard))) {
    LOG_WARN("get schema guard failed", KR(ret));
  } else if (OB_FAIL(sequence_schema_guard.get_sequence_schema(
                     tenant_id,
                     sequence_id,
                     sequence_schema))) {
    LOG_WARN("fail get sequence schema", K(sequence_id), KR(ret));
  } else if (OB_ISNULL(sequence_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", KR(ret));
  } else if (OB_FAIL(sequence_schema_.assign(*sequence_schema))) {
    LOG_WARN("cache sequence_schema fail", K(tenant_id), K(sequence_id), KR(ret));
  }
  return ret;
}

int ObTableLoadStoreCtx::commit_autoinc_value()
{
  int ret = OB_SUCCESS;
  ObAutoincrementService &auto_service = ObAutoincrementService::get_instance();
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->param_.write_session_count_; ++i) {
    SessionContext *session_ctx = session_ctx_array_ + i;
    if (OB_FAIL(auto_service.sync_insert_value_local(session_ctx->autoinc_param_))) {
      LOG_WARN("fail to sync insert auto increment value local", KR(ret), K(session_ctx->autoinc_param_));
    } else if (OB_FAIL(auto_service.sync_insert_value_global(session_ctx->autoinc_param_))) {
      LOG_WARN("fail to sync insert auto increment value global", KR(ret), K(session_ctx->autoinc_param_));
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::init_session_ctx_array()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  AutoincParam autoinc_param;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(SessionContext) * ctx_->param_.write_session_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else if (ctx_->schema_.has_autoinc_column_ && OB_FAIL(generate_autoinc_params(autoinc_param))) {
    LOG_WARN("fail to init auto increment param", KR(ret));
  } else {
    session_ctx_array_ = new (buf) SessionContext[ctx_->param_.write_session_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->param_.write_session_count_; ++i) {
      SessionContext *session_ctx = session_ctx_array_ + i;
      session_ctx->autoinc_param_ = autoinc_param;
      if (!is_fast_heap_table_) {
        session_ctx->extra_buf_size_ = table_data_desc_.extra_buf_size_;
        if (OB_ISNULL(session_ctx->extra_buf_ =
                        static_cast<char *>(allocator_.alloc(session_ctx->extra_buf_size_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::alloc_trans_ctx(const ObTableLoadTransId &trans_id,
                                         ObTableLoadTransCtx *&trans_ctx)
{
  int ret = OB_SUCCESS;
  trans_ctx = nullptr;
  // 分配trans_ctx
  if (OB_ISNULL(trans_ctx = ctx_->alloc_trans_ctx(trans_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc trans ctx", KR(ret), K(trans_id));
  }
  // 把trans_ctx插入map
  else if (OB_FAIL(trans_ctx_map_.set_refactored(trans_ctx->trans_id_, trans_ctx))) {
    LOG_WARN("fail to set trans ctx", KR(ret), K(trans_ctx->trans_id_));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != trans_ctx) {
      ctx_->free_trans_ctx(trans_ctx);
      trans_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::alloc_trans(const ObTableLoadTransId &trans_id,
                                     ObTableLoadStoreTrans *&trans)
{
  int ret = OB_SUCCESS;
  trans = nullptr;
  ObTableLoadTransCtx *trans_ctx = nullptr;
  // 分配trans_ctx
  if (OB_FAIL(alloc_trans_ctx(trans_id, trans_ctx))) {
    LOG_WARN("fail to alloc trans ctx", KR(ret), K(trans_id));
  }
  // 构造trans
  else if (OB_ISNULL(trans = trans_allocator_.alloc(trans_ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTableLoadStoreTrans", KR(ret));
  } else if (OB_FAIL(trans->init())) {
    LOG_WARN("fail to init trans", KR(ret), K(trans_id));
  } else if (OB_FAIL(trans_map_.set_refactored(trans_id, trans))) {
    LOG_WARN("fail to set_refactored", KR(ret), K(trans_id));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != trans) {
      trans_allocator_.free(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::start_trans(const ObTableLoadTransId &trans_id,
                                     ObTableLoadStoreTrans *&trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else if (OB_FAIL(check_status(ObTableLoadStatusType::LOADING))) {
    LOG_WARN("fail to check status", KR(ret), K_(status));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    const ObTableLoadSegmentID &segment_id = trans_id.segment_id_;
    SegmentCtx *segment_ctx = nullptr;
    if (OB_FAIL(segment_ctx_map_.get(segment_id, segment_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment ctx", KR(ret));
      } else {
        if (OB_FAIL(segment_ctx_map_.create(segment_id, segment_ctx))) {
          LOG_WARN("fail to create", KR(ret));
        } else {
          segment_ctx->segment_id_ = segment_id;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(nullptr != segment_ctx->current_trans_ ||
                      nullptr != segment_ctx->committed_trans_store_)) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("trans already exist", KR(ret), KPC(segment_ctx));
      } else {
        if (OB_FAIL(alloc_trans(trans_id, trans))) {
          LOG_WARN("fail to alloc trans", KR(ret));
        } else {
          segment_ctx->current_trans_ = trans;
          trans->inc_ref_count();
        }
      }
    }
    if (OB_NOT_NULL(segment_ctx)) {
      segment_ctx_map_.revert(segment_ctx);
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::commit_trans(ObTableLoadStoreTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else if (OB_ISNULL(trans)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(trans));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    const ObTableLoadSegmentID &segment_id = trans->get_trans_id().segment_id_;
    SegmentCtx *segment_ctx = nullptr;
    ObTableLoadTransStore *trans_store = nullptr;
    if (OB_FAIL(segment_ctx_map_.get(segment_id, segment_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment ctx", KR(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected trans", KR(ret));
      }
    } else if (OB_UNLIKELY(segment_ctx->current_trans_ != trans)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans", KR(ret));
    } else if (OB_FAIL(trans->check_trans_status(ObTableLoadTransStatusType::COMMIT))) {
      LOG_WARN("fail to check trans status commit", KR(ret));
    } else if (OB_FAIL(trans->output_store(trans_store))) {
      LOG_WARN("fail to output store", KR(ret));
    } else if (OB_FAIL(committed_trans_store_array_.push_back(trans_store))) {
      LOG_WARN("fail to push back trans store", KR(ret));
    } else {
      segment_ctx->current_trans_ = nullptr;
      segment_ctx->committed_trans_store_ = trans_store;
      trans->set_dirty();
    }
    if (OB_NOT_NULL(segment_ctx)) {
      segment_ctx_map_.revert(segment_ctx);
    }
    if (OB_FAIL(ret)) {
      if (nullptr != trans_store) {
        trans_store->~ObTableLoadTransStore();
        trans_store = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::abort_trans(ObTableLoadStoreTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else if (OB_ISNULL(trans)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(trans));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    const ObTableLoadSegmentID &segment_id = trans->get_trans_id().segment_id_;
    SegmentCtx *segment_ctx = nullptr;
    if (OB_FAIL(segment_ctx_map_.get(segment_id, segment_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment ctx", KR(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected trans", KR(ret));
      }
    } else if (OB_UNLIKELY(segment_ctx->current_trans_ != trans)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans", KR(ret));
    } else if (OB_FAIL(trans->check_trans_status(ObTableLoadTransStatusType::ABORT))) {
      LOG_WARN("fail to check trans status abort", KR(ret));
    } else {
      segment_ctx->current_trans_ = nullptr;
      trans->set_dirty();
    }
    if (OB_NOT_NULL(segment_ctx)) {
      segment_ctx_map_.revert(segment_ctx);
    }
  }
  return ret;
}

void ObTableLoadStoreCtx::put_trans(ObTableLoadStoreTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else if (OB_ISNULL(trans)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(trans));
  } else {
    ObTableLoadTransCtx *trans_ctx = trans->get_trans_ctx();
    if (0 == trans->dec_ref_count() && trans->is_dirty()) {
      ObTableLoadTransStatusType trans_status = trans_ctx->get_trans_status();
      OB_ASSERT(ObTableLoadTransStatusType::COMMIT == trans_status ||
                ObTableLoadTransStatusType::ABORT == trans_status);
      obsys::ObWLockGuard guard(rwlock_);
      if (OB_FAIL(trans_map_.erase_refactored(trans->get_trans_id()))) {
        LOG_WARN("fail to erase_refactored", KR(ret));
      } else {
        trans_allocator_.free(trans);
        trans = nullptr;
      }
    }
  }
  if (OB_FAIL(ret)) {
    set_status_error(ret);
  }
}

int ObTableLoadStoreCtx::get_trans(const ObTableLoadTransId &trans_id,
                                   ObTableLoadStoreTrans *&trans)
{
  int ret = OB_SUCCESS;
  trans = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(trans_map_.get_refactored(trans_id, trans))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get_refactored", KR(ret), K(trans_id));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      trans->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::get_trans_ctx(const ObTableLoadTransId &trans_id,
                                       ObTableLoadTransCtx *&trans_ctx) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(trans_ctx_map_.get_refactored(trans_id, trans_ctx))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get trans ctx", KR(ret), K(trans_id));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::get_segment_trans(const ObTableLoadSegmentID &segment_id,
                                           ObTableLoadStoreTrans *&trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    SegmentCtx *segment_ctx = nullptr;
    if (OB_FAIL(segment_ctx_map_.get(segment_id, segment_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment ctx", KR(ret));
      }
    } else if (OB_ISNULL(segment_ctx->current_trans_)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("active segment trans not exist", KR(ret), KPC(segment_ctx));
    } else {
      trans = segment_ctx->current_trans_;
      trans->inc_ref_count();
    }
    if (OB_NOT_NULL(segment_ctx)) {
      segment_ctx_map_.revert(segment_ctx);
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::get_active_trans_ids(ObIArray<ObTableLoadTransId> &trans_id_array) const
{
  int ret = OB_SUCCESS;
  trans_id_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    for (TransMap::const_iterator trans_iter = trans_map_.begin();
         OB_SUCC(ret) && trans_iter != trans_map_.end(); ++trans_iter) {
      if (OB_FAIL(trans_id_array.push_back(trans_iter->first))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::get_committed_trans_ids(
  ObTableLoadArray<ObTableLoadTransId> &trans_id_array, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(trans_id_array.create(committed_trans_store_array_.count(), allocator))) {
      LOG_WARN("fail to create trans id array", KR(ret));
    } else {
      for (int64_t i = 0; i < committed_trans_store_array_.count(); ++i) {
        ObTableLoadTransStore *trans_store = committed_trans_store_array_.at(i);
        trans_id_array[i] = trans_store->trans_ctx_->trans_id_;
      }
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::get_committed_trans_stores(
  ObIArray<ObTableLoadTransStore *> &trans_store_array) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(trans_store_array.assign(committed_trans_store_array_))) {
      LOG_WARN("fail to assign trans store array", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::check_exist_trans(bool &exist) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    exist = !trans_map_.empty();
  }
  return ret;
}

void ObTableLoadStoreCtx::clear_committed_trans_stores()
{
  obsys::ObWLockGuard guard(rwlock_);
  for (int64_t i = 0; i < committed_trans_store_array_.count(); ++i) {
    ObTableLoadTransStore *trans_store = committed_trans_store_array_.at(i);
    ObTableLoadTransCtx *trans_ctx = trans_store->trans_ctx_;
    trans_store->~ObTableLoadTransStore();
    trans_ctx->allocator_.free(trans_store);
  }
  committed_trans_store_array_.reset();
}

int ObTableLoadStoreCtx::get_next_insert_tablet_ctx(ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(op_lock_);
  if (next_tablet_idx_ < ls_partition_ids_.count()) {
    tablet_id = ls_partition_ids_[next_tablet_idx_++].part_tablet_id_.tablet_id_;
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

void ObTableLoadStoreCtx::handle_open_insert_tablet_ctx_finish(bool &is_finish)
{
  is_finish = false;
  ObMutexGuard guard(op_lock_);
  if (++opened_insert_tablet_count_ == ls_partition_ids_.count()) {
    is_finish = true;
  }
}

} // namespace observer
} // namespace oceanbase
