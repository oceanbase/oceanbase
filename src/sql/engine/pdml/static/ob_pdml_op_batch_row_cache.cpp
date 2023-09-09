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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_pdml_op_batch_row_cache.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/dml/ob_table_modify_op.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

int ObPDMLOpRowIterator::init_data_source(ObChunkDatumStore &row_datum_store, ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  eval_ctx_ = eval_ctx;
  ret = row_datum_store.begin(row_store_it_);
  return ret;
}

int ObPDMLOpRowIterator::get_next_row(const ObExprPtrIArray &row)
{
  int ret = OB_SUCCESS;
  bool is_distinct = true;
  do {
    if (OB_ISNULL(eval_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not init the eval_ctx", K(ret));
    } else if (OB_SUCC(row_store_it_.get_next_row(row, *eval_ctx_))) {
      // we should do uniq row checking after data being stored in row_store
      // rather than before this. because we need to return all rows unfiltered to upper ops.
      if (uniq_row_checker_) {
        ret = uniq_row_checker_->check_rowkey_distinct(row, is_distinct);
      }
    }
  } while (OB_SUCC(ret) && !is_distinct);
  if (OB_SUCC(ret)) {
    LOG_TRACE("get next row from pdml iterator", "row", ROWEXPR2STR(*eval_ctx_, row));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////

ObPDMLOpBatchRowCache::ObPDMLOpBatchRowCache(ObEvalCtx *eval_ctx, ObMonitorNode &op_monitor_info) :
      row_allocator_("PDMLRowCache"),
      eval_ctx_(eval_ctx),
      iterator_(),
      cached_rows_num_(0),
      cached_rows_size_(0),
      cached_in_mem_rows_num_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      with_barrier_(false),
      mem_context_(nullptr),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_, op_monitor_info),
      io_event_observer_(op_monitor_info)
{
}

ObPDMLOpBatchRowCache::~ObPDMLOpBatchRowCache()
{
}

bool ObPDMLOpBatchRowCache::empty() const
{
  return cached_rows_num_ == 0;
}

int ObPDMLOpBatchRowCache::init(uint64_t tenant_id, int64_t part_cnt, bool with_barrier, const ObTableModifySpec &spec)
{
  int ret = OB_SUCCESS;
  row_allocator_.set_tenant_id(tenant_id);
  tenant_id_ = tenant_id;
  with_barrier_ = with_barrier;

  if (OB_ISNULL(mem_context_)) {
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, "PdmlCacheRows", ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObExecContext &ctx = eval_ctx_->exec_ctx_;
    int64_t row_count = spec.rows_;
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
                &ctx, spec.px_est_size_factor_, row_count, row_count))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(
                &mem_context_->get_malloc_allocator(),
                tenant_id,
                row_count * spec.width_, spec.type_, spec.id_, &ctx))) {
      LOG_WARN("failed to init sql memory manager processor", K(ret));
    }
    LOG_DEBUG("init opeartor params for batch row cache",
             K(row_count),
             K(spec.rows_),
             K(spec.width_),
             K(spec.px_est_size_factor_),
             K(spec.type_));
  }

  if (OB_SUCC(ret)) {
    ObMemAttr bucket_attr(tenant_id, "PDMLRowBucket");
    ObMemAttr node_attr(tenant_id, "PDMLRowNode");
    if (OB_FAIL(pstore_map_.create(part_cnt * 2, bucket_attr, node_attr))) {
      LOG_WARN("fail create part store map", K(ret), K(part_cnt));
    }
  }
  return ret;
}

int ObPDMLOpBatchRowCache::init_row_store(ObChunkDatumStore *&chunk_row_store)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = mem_context_->get_malloc_allocator();
  void *buf = allocator.alloc(sizeof(ObChunkDatumStore));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc mem", K(ret));
  } else {
    // 进行一个优化：
    // 1. 如果没有barrier，就不进行的dump
    // 2. 如果有barrier，就需要进行dump
    chunk_row_store = new(buf) ObChunkDatumStore("PDML_ROW_CACHE", &allocator);
    if (OB_FAIL(chunk_row_store->init(INT64_MAX, // let auto mem mgr take care of mem limit
                                      tenant_id_,
                                      ObCtxIds::WORK_AREA,
                                      "PDML_ROW_CACHE", // 模块lable，不超过15字符
                                      with_barrier_))) { // barrier情况下，需要支持dump能力；
                                                         // 非barrier情况下，不支持dump
      LOG_WARN("failed to init chunk row store in batch row cache", K(ret));
    } else {
      chunk_row_store->set_callback(&sql_mem_processor_);
      chunk_row_store->set_io_event_observer(&io_event_observer_);
      if (with_barrier_) {
        // barrier的情况下，如果数据量较大，需要对数据进行dump
        if (OB_FAIL(chunk_row_store->alloc_dir_id())) {
          LOG_WARN("failed to alloc dir id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPDMLOpBatchRowCache::create_new_bucket(ObTabletID tablet_id, ObChunkDatumStore *&chunk_row_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_row_store(chunk_row_store))) {
    LOG_WARN("fail init row store", K(ret), K(tablet_id));
  } else if (OB_FAIL(pstore_map_.set_refactored(tablet_id, chunk_row_store))) {
    LOG_WARN("fail set part id to map", K(ret), K(tablet_id));
  }
  return ret;
}

int ObPDMLOpBatchRowCache::add_row(const ObExprPtrIArray &row, ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *stored_row = NULL;
  ObChunkDatumStore *row_store = NULL;
  // to address storage write throttle, we limit the max buffer size of PDML write.
  // the 2MB config is tested optimal under PDML concurrency=4 and concurrency=8 cases
  // TODO: maybe we can introduce a dynamic control policy
  //       concidering the tenant overall access behavior to storage
  constexpr int64_t max_pdml_cache_size_per_thread = 2 * 1024 * 1024;
  if (!with_barrier_ && cached_rows_size_ > max_pdml_cache_size_per_thread) {
    ret = OB_EXCEED_MEM_LIMIT;
  } else if (OB_FAIL(process_dump())) {
    if (OB_EXCEED_MEM_LIMIT != ret) {
      LOG_WARN("fail process dump for PDML row cache", K(ret));
    }
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST == (ret = pstore_map_.get_refactored(tablet_id, row_store)))) {
    // new part id
    if (OB_FAIL(create_new_bucket(tablet_id, row_store))) {
      LOG_WARN("fail create new bucket", K(tablet_id), K(ret));
    }
  } else if (OB_FAIL(ret)) {
    LOG_WARN("fail get row store from map", K(tablet_id), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(nullptr == row_store)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(row_store->add_row(row, eval_ctx_, &stored_row))) {
      if (OB_EXCEED_MEM_LIMIT != ret) {
        LOG_WARN("fail add row to store", K(ret));
      } else {
        LOG_INFO("pdml row cache needs write out rows", K_(cached_rows_num), K(tablet_id), K(ret));
      }
    } else if (OB_ISNULL(stored_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the store row is null", K(ret));
    } else {
      cached_rows_num_++;
      cached_in_mem_rows_num_++;
      cached_rows_size_ += stored_row->row_size_;
      LOG_DEBUG("add one row to batch row cache",
                K_(cached_rows_num), K_(cached_rows_size), K(tablet_id), "row", ROWEXPR2STR(*eval_ctx_, row));
    }
  }
  return ret;
}


int ObPDMLOpBatchRowCache::get_part_id_array(ObTabletIDArray &arr)
{
  int ret = OB_SUCCESS;
  arr.reset();
  PartitionStoreMap::const_iterator iter = pstore_map_.begin();
  for (; OB_SUCC(ret) && iter != pstore_map_.end(); ++iter) {
    if (OB_FAIL(arr.push_back(iter->first))) {
      LOG_WARN("fail fill idx to arr", K(ret));
    }
  }
  return ret;
}


int ObPDMLOpBatchRowCache::get_row_iterator(ObTabletID tablet_id, ObPDMLOpRowIterator *&iterator)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore *row_store = nullptr;
  if (OB_FAIL(pstore_map_.get_refactored(tablet_id, row_store))) {
    LOG_WARN("expect cached part id same as stored",K(ret), K(tablet_id));
  } else if (OB_FAIL(iterator_.init_data_source(*row_store, eval_ctx_))) {
    LOG_WARN("fail init data source", K(ret));
  } else {
    iterator = &iterator_;
  }
  return ret;
}

// int ObPDMLOpBatchRowCache::free_rows(int64_t part_id)
// {
//   int ret = OB_NOT_SUPPORTED;
//   LOG_USER_ERROR(OB_NOT_SUPPORTED, "free rows in pdml");
//   UNUSED(part_id);
//   LOG_INFO("TODO:free batch row cache row", K(ret), K(part_id));
//   return ret;
// }

void ObPDMLOpBatchRowCache::destroy()
{
  if (cached_rows_num_ !=0) {
    LOG_TRACE("destroy the batch row cache, but the cache_rows_num_ is not zero",
              K(cached_rows_num_));
  }
  (void)free_datum_store_memory();
  pstore_map_.destroy();
  sql_mem_processor_.unregister_profile();
  sql_mem_processor_.destroy();
  cached_rows_num_ = 0;
  cached_rows_size_ = 0;
  cached_in_mem_rows_num_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  with_barrier_ = false;

  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
}

// call this after rows returned to upper OP
int ObPDMLOpBatchRowCache::reuse_after_rows_processed()
{
  int ret = OB_SUCCESS;
  if (cached_rows_num_ != 0) {
    sql_mem_processor_.reset();
    sql_mem_processor_.set_number_pass(0);
    LOG_DEBUG("trace material dump",
              K(sql_mem_processor_.get_data_size()),
              K(sql_mem_processor_.get_mem_bound()));
  }
  ret = free_datum_store_memory();
  pstore_map_.reuse();
  cached_rows_num_ = 0;
  cached_rows_size_ = 0;
  cached_in_mem_rows_num_ = 0;
  return ret;
}

int ObPDMLOpBatchRowCache::free_datum_store_memory()
{
  int ret = OB_SUCCESS;
  PartitionStoreMap::iterator iter = pstore_map_.begin();
  ObChunkDatumStore *store = nullptr;
  for (; OB_SUCC(ret) && iter != pstore_map_.end(); ++iter) {
    store = iter->second;
    if (OB_ISNULL(store)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("store should not be null", K(ret));
    } else {
      store->reset();
      mem_context_->get_malloc_allocator().free(store);
    }
  }
  return ret;
}

int ObPDMLOpBatchRowCache::process_dump()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool should_dumped = false;
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
              &mem_context_->get_malloc_allocator(),
              [&](int64_t cur_cnt) { return cached_in_mem_rows_num_ > cur_cnt; },
              updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump() &&
             OB_FAIL(sql_mem_processor_.extend_max_memory_size(
                     &mem_context_->get_malloc_allocator(),
                     [&](int64_t max_memory_size) { return sql_mem_processor_.get_data_size() > max_memory_size; },
                     should_dumped,
                     sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size",
             K(ret), "data_size", sql_mem_processor_.get_data_size());
  } else if (should_dumped) {
    LOG_DEBUG("should dump or flush cache to storage",
             K(updated),
             K(with_barrier_),
             K(cached_in_mem_rows_num_),
             K(cached_rows_num_),
             K(cached_rows_size_),
             K(sql_mem_processor_.get_data_size()),
             K(sql_mem_processor_.get_mem_bound()),
             K(profile_));
    if (with_barrier_) {
      if (OB_FAIL(dump_all_datum_store())) {
        LOG_WARN("fail dump all datum store", K(ret));
      }
    } else {
      ret = OB_EXCEED_MEM_LIMIT;
    }
  }
  return ret;
}

bool ObPDMLOpBatchRowCache::need_dump() const
{
  return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound();
}

int ObPDMLOpBatchRowCache::dump_all_datum_store()
{
  int ret = OB_SUCCESS;
  PartitionStoreMap::iterator iter = pstore_map_.begin();
  ObChunkDatumStore *store = nullptr;
  for (; OB_SUCC(ret) && iter != pstore_map_.end(); ++iter) {
    store = iter->second;
    if (OB_ISNULL(store)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("store should not be null", K(ret));
    } else if (OB_FAIL(store->dump(false, true))) {
      LOG_WARN("fail to dump and reuse store memory", K(ret));
    }
  }
  cached_in_mem_rows_num_ = 0;
  sql_mem_processor_.reset();
  sql_mem_processor_.set_number_pass(1);
  return ret;
}

