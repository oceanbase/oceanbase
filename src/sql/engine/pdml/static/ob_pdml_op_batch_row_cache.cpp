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

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

int ObPDMLOpRowIterator::init_data_source(ObChunkDatumStore& row_datum_store, ObEvalCtx* eval_ctx)
{
  int ret = OB_SUCCESS;
  eval_ctx_ = eval_ctx;
  ret = row_datum_store.begin(row_store_it_);
  return ret;
}

int ObPDMLOpRowIterator::get_next_row(const ObExprPtrIArray& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init the eval_ctx", K(ret));
  } else {
    ret = row_store_it_.get_next_row(row, *eval_ctx_);
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////

ObPDMLOpBatchRowCache::ObPDMLOpBatchRowCache(ObEvalCtx* eval_ctx)
    : row_allocator_("PDMLRowCache"),
      eval_ctx_(eval_ctx),
      iterator_(),
      cached_rows_num_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      with_barrier_(false)
{}

ObPDMLOpBatchRowCache::~ObPDMLOpBatchRowCache()
{
  destroy();
}

bool ObPDMLOpBatchRowCache::empty() const
{
  return cached_rows_num_ == 0;
}

int ObPDMLOpBatchRowCache::init(uint64_t tenant_id, int64_t part_cnt, bool with_barrier)
{
  int ret = OB_SUCCESS;
  row_allocator_.set_tenant_id(tenant_id);
  tenant_id_ = tenant_id;
  with_barrier_ = with_barrier;
  ObMemAttr bucket_attr(tenant_id, "PDMLRowBucket");
  ObMemAttr node_attr(tenant_id, "PDMLRowNode");
  ret = pstore_map_.create(part_cnt * 2, bucket_attr, node_attr);
  return ret;
}

int ObPDMLOpBatchRowCache::init_row_store(ObIAllocator& allocator, ObChunkDatumStore*& chunk_row_store)
{
  int ret = OB_SUCCESS;
  void* buf = allocator.alloc(sizeof(ObChunkDatumStore));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc mem", K(ret));
  } else {
    chunk_row_store = new (buf) ObChunkDatumStore(&allocator);
    if (OB_FAIL(chunk_row_store->init(100 * 1024 * 1024,
            tenant_id_,
            ObCtxIds::DEFAULT_CTX_ID,
            "PDML_ROW_CACHE",
            with_barrier_))) {  // barrier,enable dump
                                // not barrier,disable dump
      LOG_WARN("failed to init chunk row store in batch row cache", K(ret));
    } else if (with_barrier_) {
      // In the case of barriers, if the amount of data is large, data needs to be dumped
      if (OB_FAIL(chunk_row_store->alloc_dir_id())) {
        LOG_WARN("failed to alloc dir id", K(ret));
      }
    }
  }
  return ret;
}

int ObPDMLOpBatchRowCache::create_new_bucket(int64_t part_id, ObChunkDatumStore*& chunk_row_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_row_store(row_allocator_, chunk_row_store))) {
    LOG_WARN("fail init row store", K(ret), K(part_id));
  } else if (OB_FAIL(pstore_map_.set_refactored(part_id, chunk_row_store))) {
    LOG_WARN("fail set part id to map", K(ret), K(part_id));
  }
  return ret;
}

int ObPDMLOpBatchRowCache::add_row(const ObExprPtrIArray& row, int64_t part_id)
{
  int ret = OB_SUCCESS;

  ObChunkDatumStore::StoredRow* stored_row = NULL;
  ObChunkDatumStore* row_store = NULL;
  if (OB_UNLIKELY(OB_HASH_NOT_EXIST == (ret = pstore_map_.get_refactored(part_id, row_store)))) {
    // new part id
    if (OB_FAIL(create_new_bucket(part_id, row_store))) {
      LOG_WARN("fail create new bucket", K(part_id), K(ret));
    }
  } else if (OB_FAIL(ret)) {
    LOG_WARN("fail get row store from map", K(part_id), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(nullptr == row_store)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(row_store->add_row(row, eval_ctx_, &stored_row))) {
      if (OB_EXCEED_MEM_LIMIT != ret) {
        LOG_WARN("fail add row to store", K(ret));
      } else {
        LOG_INFO("pdml row cache needs write out rows", K_(cached_rows_num), K(part_id), K(ret));
      }
    } else if (OB_ISNULL(stored_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the store row is null", K(ret));
    } else {
      cached_rows_num_++;
      LOG_DEBUG("add one row to batch row cache", K_(cached_rows_num), K(part_id), "row", ROWEXPR2STR(*eval_ctx_, row));
    }
  }
  return ret;
}

int ObPDMLOpBatchRowCache::get_part_id_array(PartitionIdArray& arr)
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

int ObPDMLOpBatchRowCache::get_row_iterator(int64_t part_id, ObPDMLOpRowIterator*& iterator)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore* row_store = nullptr;
  if (OB_FAIL(pstore_map_.get_refactored(part_id, row_store))) {
    LOG_WARN("expect cached part id same as stored", K(ret), K(part_id));
  } else if (OB_FAIL(iterator_.init_data_source(*row_store, eval_ctx_))) {
    LOG_WARN("fail init data source", K(ret));
  } else {
    iterator = &iterator_;
  }
  return ret;
}

int ObPDMLOpBatchRowCache::free_rows(int64_t part_id)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(part_id);
  LOG_INFO("TODO:free batch row cache row", K(ret), K(part_id));
  return ret;
}

void ObPDMLOpBatchRowCache::destroy()
{
  if (cached_rows_num_ != 0) {
    LOG_TRACE("destroy the batch row cache, but the cache_rows_num_ is not zero", K(cached_rows_num_));
  }
  pstore_map_.destroy();
  row_allocator_.reset();
  cached_rows_num_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  with_barrier_ = false;
}

void ObPDMLOpBatchRowCache::reuse()
{
  if (cached_rows_num_ != 0) {
    LOG_TRACE("reuse the batch row cache, but the cache_rows_num_ is not zero", K(cached_rows_num_));
  }
  pstore_map_.reuse();
  row_allocator_.reuse();
  cached_rows_num_ = 0;
}
