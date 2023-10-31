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

#include "ob_empty_read_bucket.h"
#include "src/observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace storage
{
ObEmptyReadBucket::ObEmptyReadBucket()
  : allocator_(ObModIds::OB_BLOOM_FILTER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    buckets_(NULL),
    bucket_size_(0)
{
}

ObEmptyReadBucket::~ObEmptyReadBucket()
{
}

int ObEmptyReadBucket::init(const int64_t lower_bound)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  //size must be 2^n, for fast mod
  int64_t size = 1;
  while (size <= lower_bound) {
    size <<= 1;
  }
  STORAGE_LOG(DEBUG, "bucket number, ", K(size));
  if (OB_UNLIKELY(size <= 0 || (size & (size - 1)))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObBloomFilterCache bucket size should be > 0 and 2^n ", K(size), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObEmptyReadCell) * size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    buckets_ = new (buf) ObEmptyReadCell[size];
    bucket_size_ = size;
  }
  return ret;
}

int ObEmptyReadBucket::mtl_init(ObEmptyReadBucket *&bucket)
{
  int ret = OB_SUCCESS;
  int64_t global_mem_limit = GMEMCONF.get_server_memory_avail();
  int64_t tenant_mem_limit = lib::get_tenant_memory_limit(MTL_ID());
  if (global_mem_limit <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Global memory should be greater than 0, ", K(global_mem_limit));
  } else {
    int64_t bucket_num_lower_bound = static_cast<double>(tenant_mem_limit) / global_mem_limit * BUCKET_SIZE_LIMIT;
    if(OB_FAIL(bucket->init(bucket_num_lower_bound))) {
      STORAGE_LOG(WARN, "failed to init EmptyReadBucket, ", K(ret));
    }
  }
  return ret;
}

void ObEmptyReadBucket::destroy()
{
  if (NULL != buckets_) {
    for (int64_t i = 0; i < bucket_size_; ++i) {
      buckets_[i].~ObEmptyReadCell();
    }
    allocator_.free(buckets_);
    allocator_.reset();
    buckets_ = NULL;
    bucket_size_ = 0;
  }
}

void ObEmptyReadBucket::mtl_destroy(ObEmptyReadBucket *&bucket)
{
  if (OB_NOT_NULL(bucket)) {
    bucket->destroy();
    common::ob_delete(bucket);
  }
}

int ObEmptyReadBucket::get_cell(const uint64_t hashcode, ObEmptyReadCell *&cell)
{
  int ret = OB_SUCCESS;
  uint64_t idx = hashcode & (bucket_size_ - 1);
  cell = NULL;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterCache bucket not init ", K(ret));
  } else {
    cell = &buckets_[idx];
  }
  return ret;
}

void ObEmptyReadBucket::reset()
{
  for (int64_t i = 0; i < bucket_size_; ++i) {
    buckets_[i].reset();
  }
}

} // namespace storage
} // namespace oceanbase