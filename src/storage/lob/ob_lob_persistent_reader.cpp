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
#include "storage/lob/ob_lob_persistent_iterator.h"

namespace oceanbase
{
namespace storage
{

ObPersistLobReaderCache::~ObPersistLobReaderCache()
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH(curr, list_) {
    curr->reader_->reset();
    curr->reader_->~ObLobMetaIterator();
    curr->reader_ = nullptr;
  }
  list_.clear();
}

int ObPersistLobReaderCache::get(ObPersistLobReaderCacheKey key, ObLobMetaIterator *&reader)
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH_X(curr, list_, OB_SUCC(ret) && nullptr == reader) {
    if (OB_ISNULL(curr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("curr is null", K(ret));
    } else if (! (curr->key_ == key)) { // next
    } else if (false  == list_.move_to_last(curr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("move_to_last fail", K(ret), K(key));
    } else if (OB_ISNULL(reader = curr->reader_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reader is null", K(ret), K(key));
    }
  }
  return ret;
}

int ObPersistLobReaderCache::put(ObPersistLobReaderCacheKey key, ObLobMetaIterator *reader)
{
  int ret = OB_SUCCESS;
  ObPersistLobReaderCacheNode *node = nullptr;
  if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null", K(ret), K(key));
  } else if (OB_ISNULL(node = OB_NEWx(ObPersistLobReaderCacheNode, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret));
  } else {
    node->key_ = key;
    node->reader_ = reader;
    if (list_.get_size() >= cap_ && OB_FAIL(remove_first())) {
      LOG_WARN("remove_first fail", K(ret), K(list_), K(cap_));
    } else if (false == list_.add_last(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add_last fail", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(node)) {
      allocator_.free(node);
    }
  }
  return ret;
}

int ObPersistLobReaderCache::remove_first()
{
  int ret = OB_SUCCESS;
  ObPersistLobReaderCacheNode *node = list_.remove_first();
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret), K(list_));
  } else {
    node->reader_->~ObLobMetaIterator();
    allocator_.free(node->reader_);
    node->reader_ = nullptr;
    allocator_.free(node);
  }
  return ret;
}

ObLobMetaIterator* ObPersistLobReaderCache::alloc_reader(const ObLobAccessCtx *access_ctx)
{
  return OB_NEWx(ObLobMetaIterator, &allocator_, access_ctx);
}

} // storage
} // oceanbase