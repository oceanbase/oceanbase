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

#include "ob_storage_cache_suite.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace blocksstable {
ObStorageCacheSuite::ObStorageCacheSuite()
    : block_index_cache_(), user_block_cache_(), user_row_cache_(), bf_cache_(), fuse_row_cache_(), is_inited_(false)
{}

ObStorageCacheSuite::~ObStorageCacheSuite()
{
  destroy();
}

ObStorageCacheSuite& ObStorageCacheSuite::get_instance()
{
  static ObStorageCacheSuite instance_;
  return instance_;
}

int ObStorageCacheSuite::init(const int64_t index_cache_priority, const int64_t user_block_cache_priority,
    const int64_t user_row_cache_priority, const int64_t fuse_row_cache_priority, const int64_t bf_cache_priority,
    const int64_t bf_cache_miss_count_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The cache suite has been inited, ", K(ret));
  } else if (OB_FAIL(block_index_cache_.init("block_index_cache", index_cache_priority))) {
    STORAGE_LOG(ERROR, "init block index cache failed, ", K(ret));
  } else if (OB_FAIL(user_block_cache_.init("user_block_cache", user_block_cache_priority))) {
    STORAGE_LOG(ERROR, "init user block cache failed, ", K(ret));
  } else if (OB_FAIL(user_row_cache_.init("user_row_cache", user_row_cache_priority))) {
    STORAGE_LOG(ERROR, "init user sstable row cache failed, ", K(ret));
  } else if (OB_FAIL(bf_cache_.init("bf_cache", bf_cache_priority))) {
    STORAGE_LOG(ERROR, "init bloom filter cache failed, ", K(ret));
  } else if (OB_FAIL(bf_cache_.set_bf_cache_miss_count_threshold(bf_cache_miss_count_threshold))) {
    STORAGE_LOG(ERROR, "failed to set bf_cache_miss_count_threshold", K(ret));
  } else if (OB_FAIL(fuse_row_cache_.init("fuse_row_cache", fuse_row_cache_priority))) {
    STORAGE_LOG(ERROR, "fail to init fuse row cache", K(ret));
  } else {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(OB_SUCCESS != ret && !is_inited_)) {
    destroy();
  }
  return ret;
}

int ObStorageCacheSuite::reset_priority(const int64_t index_cache_priority, const int64_t user_block_cache_priority,
    const int64_t user_row_cache_priority, const int64_t fuse_row_cache_priority, const int64_t bf_cache_priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The cache suite has not been inited, ", K(ret));
  } else if (OB_FAIL(block_index_cache_.set_priority(index_cache_priority))) {
    STORAGE_LOG(ERROR, "set priority for block index cache failed, ", K(ret));
  } else if (OB_FAIL(user_block_cache_.set_priority(user_block_cache_priority))) {
    STORAGE_LOG(ERROR, "set priority for user block cache failed, ", K(ret));
  } else if (OB_FAIL(user_row_cache_.set_priority(user_row_cache_priority))) {
    STORAGE_LOG(ERROR, "set priority for user sstable row cache failed, ", K(ret));
  } else if (OB_FAIL(bf_cache_.set_priority(bf_cache_priority))) {
    STORAGE_LOG(ERROR, "set priority for bloom filter cache failed, ", K(ret));
  } else if (OB_FAIL(fuse_row_cache_.set_priority(fuse_row_cache_priority))) {
    STORAGE_LOG(ERROR, "fail to set priority for fuse row cache", K(ret));
  }
  return ret;
}

int ObStorageCacheSuite::set_bf_cache_miss_count_threshold(const int64_t bf_cache_miss_count_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(bf_cache_.set_bf_cache_miss_count_threshold(bf_cache_miss_count_threshold))) {
    STORAGE_LOG(WARN, "failed to set bf_cache_miss_count_threshold", K(ret), K(bf_cache_miss_count_threshold));
  }
  return ret;
}

void ObStorageCacheSuite::destroy()
{
  block_index_cache_.destroy();
  user_block_cache_.destroy();
  user_row_cache_.destroy();
  bf_cache_.destroy();
  fuse_row_cache_.destroy();
  is_inited_ = false;
}

void ObStorageCacheContext::set(ObBlockCacheWorkingSet& block_cache_ws)
{
  block_index_cache_ = &(OB_STORE_CACHE.block_index_cache_);
  block_cache_ = &(OB_STORE_CACHE.user_block_cache_);
  block_cache_ws_ = &block_cache_ws;
  bf_cache_ = &(OB_STORE_CACHE.bf_cache_);
  row_cache_ = &(OB_STORE_CACHE.user_row_cache_);
}

bool ObStorageCacheContext::is_valid() const
{
  return NULL != block_index_cache_ && NULL != block_cache_ && NULL != block_cache_ws_ && NULL != row_cache_ &&
         NULL != bf_cache_;
}

void ObStorageCacheContext::reset()
{
  block_index_cache_ = NULL;
  block_cache_ = NULL;
  block_cache_ws_ = NULL;
  row_cache_ = NULL;
  bf_cache_ = NULL;
}
}  // namespace blocksstable
}  // namespace oceanbase
