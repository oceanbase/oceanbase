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
#include "ob_batch_row_cache.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

int ObPDMLRowIterator::init_data_source(sql::ObChunkRowStore& row_store)
{
  int ret = OB_SUCCESS;
  ret = row_store.begin(row_store_it_);
  return ret;
}

int ObPDMLRowIterator::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ret = row_store_it_.get_next_row(row);
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////

ObBatchRowCache::ObBatchRowCache()
    : row_allocator_("PDMLRowCache"),
      iterator_(),
      cached_rows_num_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      with_barrier_(false)
{}

ObBatchRowCache::~ObBatchRowCache()
{
  destroy();
}

bool ObBatchRowCache::empty() const
{
  return cached_rows_num_ == 0;
}

int ObBatchRowCache::init(uint64_t tenant_id, int64_t part_cnt, bool with_barrier)
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

int ObBatchRowCache::init_row_store(ObIAllocator& allocator, sql::ObChunkRowStore*& chunk_row_store)
{
  int ret = OB_SUCCESS;
  void* buf = allocator.alloc(sizeof(ObChunkRowStore));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc mem", K(ret));
  } else {
    // performs an optimization:
    // 1. has no barrier, not dump
    // 2. has barrier, dump
    chunk_row_store = new (buf) ObChunkRowStore(&allocator);
    if (OB_FAIL(chunk_row_store->init(100 * 1024 * 1024,  // default: 100M
            tenant_id_,
            ObCtxIds::DEFAULT_CTX_ID,
            "PDML_ROW_CACHE",
            with_barrier_,
            ObChunkRowStore::FULL))) {
      LOG_WARN("failed to init chunk row store in batch row cache", K(ret));
    } else if (with_barrier_) {
      // In barrier, if data is large, needs to do dump for those data
      if (OB_FAIL(chunk_row_store->alloc_dir_id())) {
        LOG_WARN("failed to alloc dir id", K(ret));
      }
    }
  }
  return ret;
}

int ObBatchRowCache::create_new_bucket(int64_t part_id, sql::ObChunkRowStore*& chunk_row_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_row_store(row_allocator_, chunk_row_store))) {
    LOG_WARN("fail init row store", K(ret), K(part_id));
  } else if (OB_FAIL(pstore_map_.set_refactored(part_id, chunk_row_store))) {
    LOG_WARN("fail set part id to map", K(ret), K(part_id));
  }
  return ret;
}

int ObBatchRowCache::add_row(const common::ObNewRow& row, int64_t part_id)
{
  int ret = OB_SUCCESS;

  ObChunkRowStore::StoredRow* stored_row = nullptr;
  sql::ObChunkRowStore* row_store = nullptr;
  if (OB_HASH_NOT_EXIST == (ret = pstore_map_.get_refactored(part_id, row_store))) {
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
    } else if (OB_FAIL(row_store->add_row(row, &stored_row))) {
      LOG_WARN("fail add row to store", K(ret));
    } else if (OB_ISNULL(stored_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the store row is null", K(ret));
    } else {
      cached_rows_num_++;
      LOG_DEBUG("add one row to batch row cache", K_(cached_rows_num), K(row));
    }
  }
  return ret;
}

int ObBatchRowCache::get_part_id_array(PartitionIdArray& arr)
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

int ObBatchRowCache::get_row_iterator(int64_t part_id, ObPDMLRowIterator*& iterator)
{
  int ret = OB_SUCCESS;
  sql::ObChunkRowStore* row_store = nullptr;
  if (OB_FAIL(pstore_map_.get_refactored(part_id, row_store))) {
    LOG_WARN("expect cached part id same as stored", K(ret), K(part_id));
  } else if (OB_FAIL(iterator_.init_data_source(*row_store))) {  // use chunk_row_store to initialize iterator
    LOG_WARN("fail init data source", K(ret));
  } else {
    iterator = &iterator_;
  }
  return ret;
}

int ObBatchRowCache::free_rows(int64_t part_id)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(part_id);
  LOG_INFO("TODO:free batch row cache row", K(ret), K(part_id));
  return ret;
}

void ObBatchRowCache::destroy()
{
  if (cached_rows_num_ != 0) {
    LOG_TRACE("destory the batch row cache, but the cache_rows_num_ is not zero", K(cached_rows_num_));
  }
  pstore_map_.destroy();
  row_allocator_.reset();
  cached_rows_num_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  with_barrier_ = false;
}

void ObBatchRowCache::reuse()
{
  if (cached_rows_num_ != 0) {
    LOG_TRACE("reuse the batch row cache, but the cache_rows_num_ is not zero", K(cached_rows_num_));
  }
  pstore_map_.reuse();
  row_allocator_.reuse();
  cached_rows_num_ = 0;
}
