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

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_persistent_reader.h"
#include "lib/time/ob_time_utility.h"
#include "storage/lob/ob_lob_persistent_iterator.h"

namespace oceanbase
{
namespace storage
{

ObPersistLobReaderCache::~ObPersistLobReaderCache()
{
  reset_and_clear();
}

void ObPersistLobReaderCache::reset_and_clear()
{
  if (OB_NOT_NULL(cached_reader_)) {
    cached_reader_->reset();
    cached_reader_->~ObLobMetaIterator();
    allocator_.free(cached_reader_);
    cached_reader_ = nullptr;
  }
  cached_key_.tablet_id_.reset();
  last_cached_time_us_ = 0;
}

int ObPersistLobReaderCache::get(ObPersistLobReaderCacheKey key, ObLobMetaIterator *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  if (OB_NOT_NULL(cached_reader_) && cached_key_ == key) {
    reader = cached_reader_;
    last_cached_time_us_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObPersistLobReaderCache::put(ObPersistLobReaderCacheKey key, ObLobMetaIterator *reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null", K(ret), K(key));
  } else {
    reset_and_clear();
    cached_key_ = key;
    cached_reader_ = reader;
    last_cached_time_us_ = ObTimeUtility::current_time();
  }
  return ret;
}

void ObPersistLobReaderCache::check_and_release_if_timeout(const int64_t timeout_us)
{
  if (OB_ISNULL(cached_reader_)) {
  } else if (OB_UNLIKELY((++timeout_check_call_count_ == TIMEOUT_CHECK_THROTTLE_INTERVAL))) {
    const int64_t now_us = ObTimeUtility::current_time();
    timeout_check_call_count_ = 0;
    if (now_us - last_cached_time_us_ > timeout_us) {
      reset_and_clear();
    }
  }
}

ObLobMetaIterator* ObPersistLobReaderCache::alloc_reader(const ObLobAccessCtx *access_ctx)
{
  return OB_NEWx(ObLobMetaIterator, &allocator_, access_ctx);
}

} // storage
} // oceanbase