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

#include "ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_data_row_insert_handler.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_merge_op.h"
#include "observer/table_load/ob_table_load_pre_sorter.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
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
#include "storage/direct_load/ob_direct_load_table_store.h"
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
    thread_cnt_(0),
    task_scheduler_(nullptr),
    error_row_handler_(nullptr),
    data_store_table_ctx_(nullptr),
    tmp_file_mgr_(nullptr),
    table_mgr_(nullptr),
    sequence_schema_(&allocator_),
    next_session_id_(0),
    basic_table_data_desc_(),
    merge_count_per_round_(0),
    max_mem_chunk_count_(0),
    mem_chunk_size_(0),
    heap_table_mem_chunk_size_(0),
    write_ctx_(),
    merge_op_allocator_("TLD_MergeOp"),
    merge_root_op_(nullptr),
    status_(ObTableLoadStatusType::NONE),
    error_code_(OB_SUCCESS),
    last_heart_beat_ts_(ObTimeUtil::current_time()),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  index_store_table_ctxs_.set_block_allocator(ModulePageAllocator(allocator_));
  merge_op_allocator_.set_tenant_id(MTL_ID());
  committed_trans_store_array_.set_tenant_id(MTL_ID());
}

ObTableLoadStoreCtx::~ObTableLoadStoreCtx()
{
  destroy();
}

void ObTableLoadStoreCtx::destroy()
{
  // 先让工作线程停下来
  if (nullptr != write_ctx_.pre_sorter_) {
    write_ctx_.pre_sorter_->stop();
  }
  if (OB_NOT_NULL(task_scheduler_)) {
    task_scheduler_->stop();
  }
  if (nullptr != write_ctx_.pre_sorter_) {
    write_ctx_.pre_sorter_->wait();
  }
  if (OB_NOT_NULL(task_scheduler_)) {
    task_scheduler_->wait();
  }
  // 按顺序析构对象, 被依赖的最后析构
  if (nullptr != write_ctx_.pre_sorter_) {
    write_ctx_.pre_sorter_->~ObTableLoadPreSorter();
    allocator_.free(write_ctx_.pre_sorter_);
    write_ctx_.pre_sorter_ = nullptr;
  }
  if (OB_NOT_NULL(write_ctx_.dml_row_handler_)) {
    write_ctx_.dml_row_handler_->~ObDirectLoadDMLRowHandler();
    allocator_.free(write_ctx_.dml_row_handler_);
    write_ctx_.dml_row_handler_ = nullptr;
  }
  if (OB_NOT_NULL(merge_root_op_)) {
    merge_root_op_->~ObTableLoadMergeRootOp();
    allocator_.free(merge_root_op_);
    merge_root_op_ = nullptr;
  }
  merge_op_allocator_.reset();
  if (OB_NOT_NULL(task_scheduler_)) {
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
  if (OB_NOT_NULL(data_store_table_ctx_)) {
    data_store_table_ctx_->~ObTableLoadStoreDataTableCtx();
    allocator_.free(data_store_table_ctx_);
    data_store_table_ctx_ = nullptr;
  }
  for (int64_t i = 0; i < index_store_table_ctxs_.count(); ++i) {
    ObTableLoadStoreIndexTableCtx* index_store_table_ctx = index_store_table_ctxs_.at(i);
    index_store_table_ctx->~ObTableLoadStoreIndexTableCtx();
    allocator_.free(index_store_table_ctx);
  }
  index_store_table_ctxs_.reset();
  if (OB_NOT_NULL(error_row_handler_)) {
    error_row_handler_->~ObTableLoadErrorRowHandler();
    allocator_.free(error_row_handler_);
    error_row_handler_ = nullptr;
  }
  if (OB_NOT_NULL(tmp_file_mgr_)) {
    tmp_file_mgr_->~ObDirectLoadTmpFileManager();
    allocator_.free(tmp_file_mgr_);
    tmp_file_mgr_ = nullptr;
  }
  if (OB_NOT_NULL(table_mgr_)) {
    table_mgr_->~ObDirectLoadTableManager();
    allocator_.free(table_mgr_);
    table_mgr_ = nullptr;
  }
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
  }
  // init trans_allocator_
  else if (OB_FAIL(trans_allocator_.init("TLD_STransPool", ctx_->param_.tenant_id_))) {
    LOG_WARN("fail to init trans allocator", KR(ret));
  }
  // init trans_map_
  else if (OB_FAIL(
             trans_map_.create(1024, "TLD_STransMap", "TLD_STransMap", ctx_->param_.tenant_id_))) {
    LOG_WARN("fail to create trans map", KR(ret));
  }
  // init trans_ctx_map_
  else if (OB_FAIL(
             trans_ctx_map_.create(1024, "TLD_TCtxMap", "TLD_TCtxMap", ctx_->param_.tenant_id_))) {
    LOG_WARN("fail to create trans ctx map", KR(ret));
  }
  // init segment_trans_ctx_map_
  else if (OB_FAIL(segment_ctx_map_.init("TLD_SegCtxMap", ctx_->param_.tenant_id_))) {
    LOG_WARN("fail to init segment ctx map", KR(ret));
  }
  // init task_scheduler_
  else if (FALSE_IT(thread_cnt_ = ctx_->param_.session_count_)) {
  } else if (OB_ISNULL(task_scheduler_ =
                         OB_NEWx(ObTableLoadTaskThreadPoolScheduler, (&allocator_), thread_cnt_,
                                 ctx_->param_.table_id_, "Store", ctx_->session_info_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadTaskThreadPoolScheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->init())) {
    LOG_WARN("fail to init task scheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->start())) {
    LOG_WARN("fail to start task scheduler", KR(ret));
  }
  // init error_row_handler_
  else if (OB_ISNULL(error_row_handler_ = OB_NEWx(ObTableLoadErrorRowHandler, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadErrorRowHandler", KR(ret));
  } else if (OB_FAIL(error_row_handler_->init(ctx_->param_, result_info_, ctx_->job_stat_))) {
    LOG_WARN("fail to init error row handler", KR(ret));
  }
  // init tmp_file_mgr_
  else if (OB_ISNULL(tmp_file_mgr_ = OB_NEWx(ObDirectLoadTmpFileManager, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadTmpFileManager", KR(ret));
  } else if (OB_FAIL(tmp_file_mgr_->init(ctx_->param_.tenant_id_))) {
    LOG_WARN("fail to init tmp file manager", KR(ret));
  }
  // init table_mgr_
  else if (OB_ISNULL(table_mgr_ = OB_NEWx(ObDirectLoadTableManager, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadTableManager", KR(ret));
  } else if (OB_FAIL(table_mgr_->init())) {
    LOG_WARN("fail to init table mgr", KR(ret));
  }
  // init data_store_table_ctx_, index_store_table_ctxs_
  else if (OB_FAIL(init_store_table_ctxs(partition_id_array, target_partition_id_array))) {
    LOG_WARN("fail to init store table ctxs", KR(ret));
  }
  // init session_ctx_array_
  else if (OB_FAIL(init_session_ctx_array())) {
    LOG_WARN("fail to init session ctx array", KR(ret));
  }
  // init sequence_cache_ and sequence_schema_
  else if (data_store_table_ctx_->schema_->has_identity_column_ && OB_FAIL(init_sequence())) {
    LOG_WARN("fail to init sequence", KR(ret));
  }
  // init sort param
  else if (OB_FAIL(init_sort_param())) {
    LOG_WARN("fail to init sort param", KR(ret));
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

void ObTableLoadStoreCtx::stop()
{
  LOG_INFO("store ctx stop");
  if (nullptr != write_ctx_.pre_sorter_) {
    write_ctx_.pre_sorter_->stop();
  }
  if (nullptr != merge_root_op_) {
    merge_root_op_->stop();
  }
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
  }
}

bool ObTableLoadStoreCtx::is_stopped() const
{
  return (nullptr == task_scheduler_ || task_scheduler_->is_stopped()) &&
         (nullptr == write_ctx_.pre_sorter_ || write_ctx_.pre_sorter_->is_stopped()) &&
         (0 == ATOMIC_LOAD(&write_ctx_.px_writer_cnt_));
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
    error_code_ = (error_code_ == OB_SUCCESS ? OB_CANCELED : error_code_);
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
    LOG_WARN("unexpected status", KR(ret), K(status), K(status_));
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

int ObTableLoadStoreCtx::init_trans_param(storage::ObDirectLoadTransParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = nullptr;
  transaction::ObTxDesc *tx_desc = nullptr;
  trans_param.reset();
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
    trans_param.tx_desc_ = tx_desc;
    trans_param.tx_id_ = tx_desc->get_tx_id();
    trans_param.tx_seq_ = tx_desc->inc_and_get_tx_seq(0);
    LOG_INFO("init trans param", K(trans_param));
  }
  return ret;
}

int ObTableLoadStoreCtx::init_store_table_ctxs(
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &partition_id_array,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_partition_id_array)
{
  int ret = OB_SUCCESS;
  // init data store_table_ctx_
  if (OB_ISNULL(data_store_table_ctx_ =
                  OB_NEWx(ObTableLoadStoreDataTableCtx, (&allocator_), this))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadStoreDataTableCtx", KR(ret));
  } else if (OB_FAIL(data_store_table_ctx_->init(ctx_->param_.table_id_, partition_id_array,
                                                 target_partition_id_array))) {
    LOG_WARN("fail to init data table", KR(ret));
  }
  // init indexs store_table_ctx_
  if (OB_SUCC(ret) && ObDirectLoadMethod::is_incremental(ctx_->param_.method_)) {
    const ObIArray<uint64_t> &index_table_ids = data_store_table_ctx_->schema_->index_table_ids_;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_table_ids.count(); ++i) {
      const uint64_t index_table_id = index_table_ids.at(i);
      ObTableLoadStoreIndexTableCtx *index_table_ctx = nullptr;
      if (OB_ISNULL(index_table_ctx =
                      OB_NEWx(ObTableLoadStoreIndexTableCtx, (&allocator_), this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObTableLoadStoreIndexTableCtx", KR(ret));
      } else if (OB_FAIL(index_table_ctx->init(index_table_id, partition_id_array,
                                               target_partition_id_array))) {
        LOG_WARN("fail to init index table", KR(ret));
      } else if (OB_FAIL(index_store_table_ctxs_.push_back(index_table_ctx))) {
        LOG_WARN("fail to push back", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != index_table_ctx) {
          index_table_ctx->~ObTableLoadStoreIndexTableCtx();
          allocator_.free(index_table_ctx);
          index_table_ctx = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadStoreCtx::init_sort_param()
{
  int ret = OB_SUCCESS;
  // table_data_desc_
  basic_table_data_desc_.reset();
  basic_table_data_desc_.rowkey_column_num_ = 0;
  basic_table_data_desc_.column_count_ = 0;
  if (!GCTX.is_shared_storage_mode()) {
    basic_table_data_desc_.external_data_block_size_ = ObDirectLoadDataBlock::SN_DEFAULT_DATA_BLOCK_SIZE;
  } else {
    basic_table_data_desc_.external_data_block_size_ = ObDirectLoadDataBlock::SS_DEFAULT_DATA_BLOCK_SIZE;
  }
  basic_table_data_desc_.sstable_index_block_size_ =
    ObDirectLoadSSTableIndexBlock::DEFAULT_INDEX_BLOCK_SIZE;
  basic_table_data_desc_.sstable_data_block_size_ = ObDirectLoadSSTableDataBlock::DEFAULT_DATA_BLOCK_SIZE;
  basic_table_data_desc_.extra_buf_size_ = ObDirectLoadTableDataDesc::DEFAULT_EXTRA_BUF_SIZE;
  basic_table_data_desc_.compressor_type_ = ctx_->param_.compressor_type_;
  basic_table_data_desc_.is_shared_storage_ = GCTX.is_shared_storage_mode();
  // sort params
  int64_t wa_mem_limit = 0;
  if (ctx_->param_.exe_mode_ == ObTableLoadExeMode::MAX_TYPE) {
    // 无资源控制模式
    if (OB_FAIL(ObTableLoadService::get_memory_limit(wa_mem_limit))) {
      LOG_WARN("fail to get memory limit", KR(ret));
    } else if (wa_mem_limit < ObDirectLoadMemContext::MIN_MEM_LIMIT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wa_mem_limit is too small", KR(ret), K(wa_mem_limit));
    } else {
      merge_count_per_round_ =
        min(wa_mem_limit / basic_table_data_desc_.sstable_data_block_size_ / thread_cnt_,
            ObDirectLoadSSTableScanMerge::MAX_SSTABLE_COUNT);
      max_mem_chunk_count_ = 128;
      int64_t mem_chunk_size = wa_mem_limit / max_mem_chunk_count_;
      if (mem_chunk_size <= ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT) {
        mem_chunk_size = ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT;
        max_mem_chunk_count_ = wa_mem_limit / mem_chunk_size;
      }
      mem_chunk_size_ = mem_chunk_size;
      heap_table_mem_chunk_size_ = wa_mem_limit / thread_cnt_;
    }
  } else {
    // 资源控制模式
    wa_mem_limit = ctx_->param_.avail_memory_;
    merge_count_per_round_ =
      min(wa_mem_limit / basic_table_data_desc_.sstable_data_block_size_ / thread_cnt_,
          ObDirectLoadSSTableScanMerge::MAX_SSTABLE_COUNT);
    max_mem_chunk_count_ =
      wa_mem_limit / ObDirectLoadExternalMultiPartitionRowChunk::MIN_MEMORY_LIMIT;
  }
  return ret;
}

int ObTableLoadStoreCtx::init_write_ctx()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else {
    static const int64_t MACRO_BLOCK_WRITER_MEM_SIZE = 10 * 1024LL * 1024LL;
    // table_data_desc_
    write_ctx_.table_data_desc_ = basic_table_data_desc_;
    write_ctx_.table_data_desc_.rowkey_column_num_ =
      (!data_store_table_ctx_->schema_->is_table_without_pk_
         ? data_store_table_ctx_->schema_->rowkey_column_count_
         : 0);
    write_ctx_.table_data_desc_.column_count_ =
      (!data_store_table_ctx_->schema_->is_table_without_pk_
         ? data_store_table_ctx_->schema_->store_column_count_
         : data_store_table_ctx_->schema_->store_column_count_ - 1);
    write_ctx_.table_data_desc_.row_flag_.uncontain_hidden_pk_ =
      data_store_table_ctx_->schema_->is_table_without_pk_;
    // dml_row_handler_
    if (OB_SUCC(ret)) {
      ObTableLoadDataRowInsertHandler *data_row_handler = nullptr;
      if (OB_ISNULL(write_ctx_.dml_row_handler_ = data_row_handler =
                      OB_NEWx(ObTableLoadDataRowInsertHandler, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObTableLoadDataRowInsertHandler", KR(ret));
      } else if (OB_FAIL(data_row_handler->init(this))) {
        LOG_WARN("fail to init data row handler", KR(ret));
      }
    }
    // is_fast_heap_table_, is_multiple_mode_
    switch (ctx_->param_.exe_mode_) {
      // 堆表快速写入
      case ObTableLoadExeMode::FAST_HEAP_TABLE:
        write_ctx_.is_fast_heap_table_ = true;
        break;
      // 有主键表不排序
      case ObTableLoadExeMode::GENERAL_TABLE_COMPACT:
        break;
      // 堆表排序
      case ObTableLoadExeMode::MULTIPLE_HEAP_TABLE_COMPACT:
      // 有主键表排序
      case ObTableLoadExeMode::MEM_COMPACT:
        write_ctx_.is_multiple_mode_ = true;
        break;
      // 无资源控制
      case ObTableLoadExeMode::MAX_TYPE: {
        const int64_t part_cnt = data_store_table_ctx_->ls_partition_ids_.count();
        int64_t wa_mem_limit = 0;
        if (OB_FAIL(ObTableLoadService::get_memory_limit(wa_mem_limit))) {
          LOG_WARN("fail to get memory limit", KR(ret));
        } else if (wa_mem_limit < ObDirectLoadMemContext::MIN_MEM_LIMIT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("wa_mem_limit is too small", KR(ret), K(wa_mem_limit));
        } else if (data_store_table_ctx_->schema_->is_table_without_pk_) {
          const int64_t bucket_cnt = wa_mem_limit / (thread_cnt_ * MACRO_BLOCK_WRITER_MEM_SIZE);
          if (part_cnt <= bucket_cnt || !ctx_->param_.need_sort_) {
            // 堆表快速写入
            write_ctx_.is_fast_heap_table_ = true;
          } else {
            // 堆表排序
            write_ctx_.is_multiple_mode_ = true;
          }
        } else {
          int64_t bucket_cnt = wa_mem_limit / thread_cnt_ /
                               (write_ctx_.table_data_desc_.sstable_index_block_size_ +
                                write_ctx_.table_data_desc_.sstable_data_block_size_);
          write_ctx_.is_multiple_mode_ = (ctx_->param_.need_sort_ || part_cnt > bucket_cnt);
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected exe mode", KR(ret), K(ctx_->param_.exe_mode_));
        break;
    }
    // pre_sort
    if (OB_SUCC(ret)) {
      write_ctx_.enable_pre_sort_ =
        (write_ctx_.is_multiple_mode_ && !data_store_table_ctx_->schema_->is_table_without_pk_);
      if (write_ctx_.enable_pre_sort_) {
        if (OB_ISNULL(write_ctx_.pre_sorter_ =
                        OB_NEWx(ObTableLoadPreSorter, (&allocator_), this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new TableLoadPreSorter", KR(ret));
        } else if (OB_FAIL(write_ctx_.pre_sorter_->init())) {
          LOG_WARN("fail to init pre sorter", KR(ret));
        } else if (OB_FAIL(write_ctx_.pre_sorter_->start())) {
          LOG_WARN("fail to start pre_sorter", KR(ret));
        }
      }
    }
    // 堆表快速写入需要提前创建insert_table_ctx
    if (OB_SUCC(ret) && write_ctx_.is_fast_heap_table_) {
      if (ObDirectLoadMethod::is_incremental(ctx_->param_.method_) &&
          OB_FAIL(init_trans_param(write_ctx_.trans_param_))) {
        LOG_WARN("fail to init trans param", KR(ret));
      } else if (OB_FAIL(data_store_table_ctx_->init_insert_table_ctx(
                   write_ctx_.trans_param_, true /*online_opt_stat_gather*/,
                   true /*is_insert_lob*/))) {
        LOG_WARN("fail to init insert table ctx", KR(ret));
      } else if (ObDirectLoadMethod::is_incremental(ctx_->param_.method_)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < index_store_table_ctxs_.count(); ++i) {
          ObTableLoadStoreIndexTableCtx *index_table_ctx = index_store_table_ctxs_.at(i);
          if (OB_FAIL(index_table_ctx->init_build_insert_table())) {
            LOG_WARN("fail to init build index table", KR(ret));
          }
        }
      }
    }
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
  ObSchemaGetterGuard table_schema_guard;
  ObSchemaGetterGuard sequence_schema_guard;
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
  } else if (data_store_table_ctx_->schema_->has_autoinc_column_ && OB_FAIL(generate_autoinc_params(autoinc_param))) {
    LOG_WARN("fail to init auto increment param", KR(ret));
  } else {
    session_ctx_array_ = new (buf) SessionContext[ctx_->param_.write_session_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->param_.write_session_count_; ++i) {
      SessionContext *session_ctx = session_ctx_array_ + i;
      session_ctx->autoinc_param_ = autoinc_param;
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

int ObTableLoadStoreCtx::get_table_store_from_committed_trans_stores(ObDirectLoadTableStore &table_store)
{
  int ret = OB_SUCCESS;
  table_store.clear();
  table_store.set_table_data_desc(write_ctx_.table_data_desc_);
  if (write_ctx_.is_fast_heap_table_) {
    // 堆表快速路径, 空数据, 随便填一个类型
    table_store.set_external_table();
  } else if (ctx_->param_.need_sort_) {
    // 排序路径, 有主键表现在走pre_sort了
    table_store.set_external_table();
  } else {
    // 有主键表不排序路径
    table_store.set_multiple_sstable();
  }
  obsys::ObRLockGuard guard(rwlock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < committed_trans_store_array_.count(); ++i) {
    ObTableLoadTransStore *trans_store = committed_trans_store_array_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < trans_store->session_store_array_.count(); ++j) {
      const ObTableLoadTransStore::SessionStore *session_store =
        trans_store->session_store_array_.at(j);
      if (OB_FAIL(table_store.add_tables(session_store->tables_handle_))) {
        LOG_WARN("fail to add tables", KR(ret));
      }
    }
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

int ObTableLoadStoreCtx::start_merge()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else if (OB_UNLIKELY(write_ctx_.enable_pre_sort_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpectedpre sort is enabled", KR(ret));
  } else if (OB_FAIL(get_table_store_from_committed_trans_stores(data_store_table_ctx_->insert_table_store_))) {
    LOG_WARN("fail to get table store from committed trans stores", KR(ret));
  } else if (OB_FAIL(start_merge_op())) {
    LOG_WARN("fail to start merge op", KR(ret));
  } else {
    // 清空trans store, 以便在合并过程中释放磁盘空间
    clear_committed_trans_stores();
  }
  return ret;
}

int ObTableLoadStoreCtx::handle_pre_sort_success()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreCtx not init", KR(ret));
  } else if (OB_UNLIKELY(!write_ctx_.enable_pre_sort_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pre sort not enabled", KR(ret));
  } else if (OB_FAIL(write_ctx_.pre_sorter_->get_table_store(data_store_table_ctx_->insert_table_store_))) {
    LOG_WARN("fail to get table store", KR(ret));
  } else if (OB_FAIL(start_merge_op())) {
    LOG_WARN("fail to start merge op", KR(ret));
  } else {
    write_ctx_.pre_sorter_->reset();
    // 清空trans store, 释放内存, 里面也没啥东西
    clear_committed_trans_stores();
  }
  return ret;
}

int ObTableLoadStoreCtx::start_merge_op()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != merge_root_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge root op not null", KR(ret));
  } else if (OB_ISNULL(merge_root_op_ = OB_NEWx(ObTableLoadMergeRootOp, &allocator_, this))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadMergeRootOp", KR(ret));
  } else if (OB_FAIL(merge_root_op_->start())) {
    LOG_WARN("fail to start merge op", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
