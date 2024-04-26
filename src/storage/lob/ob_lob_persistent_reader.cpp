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

namespace oceanbase
{
namespace storage
{

ObPersistLobReaderCache::~ObPersistLobReaderCache()
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH(curr, list_) {
    curr->reader_->~ObPersistLobReader();
    curr->reader_ = nullptr;
  }
  list_.clear();
}

int ObPersistLobReaderCache::get(ObPersistLobReaderCacheKey key, ObPersistLobReader *&reader)
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

int ObPersistLobReaderCache::put(ObPersistLobReaderCacheKey key, ObPersistLobReader *reader)
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
    node->reader_->~ObPersistLobReader();
    allocator_.free(node->reader_);
    node->reader_ = nullptr;
    allocator_.free(node);
  }
  return ret;
}

ObPersistLobReader::~ObPersistLobReader()
{
  if (OB_NOT_NULL(adaptor_)) {
    if (OB_NOT_NULL(row_iter_)) {
      adaptor_->revert_scan_iter(row_iter_);
      row_iter_ = nullptr;
    }
  }
}

ObPersistLobReader* ObPersistLobReaderCache::alloc_reader()
{
  return OB_NEWx(ObPersistLobReader, &allocator_);
}

int ObPersistLobReader::open(ObPersistentLobApator* adaptor, ObLobAccessParam &param, ObNewRowIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  // scan may be fail, must be sure adaptor not null
  // then deconstrcutor can release correctly
  adaptor_ = adaptor;

  ObNewRange range;
  if (OB_ISNULL(adaptor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("adaptor is null", KR(ret), K(param));
  } else if (! param.lob_meta_tablet_id_.is_valid() || ! param.lob_piece_tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet_id invalid", KR(ret), K(param));
  } else if (OB_FAIL(param.get_rowkey_range(rowkey_objs_, range))) {
    LOG_WARN("get_rowkey_range fail", K(ret));
  } else if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
    LOG_WARN("failed to push key range.", K(ret), K(scan_param_), K(range));
  } else if (OB_FAIL(adaptor->do_scan_lob_meta(param, scan_param_, row_iter_))) {
    LOG_WARN("do_scan_lob_meta fail", K(ret));
  } else {
    meta_iter = row_iter_;
    lob_meta_tablet_id_ = param.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = param.lob_piece_tablet_id_;
  }
  return ret;
}

int ObPersistLobReader::rescan(ObLobAccessParam &param, ObNewRowIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  ObAccessService *oas = MTL(ObAccessService*);

  param.lob_meta_tablet_id_ =  lob_meta_tablet_id_;
  param.lob_piece_tablet_id_ =  lob_piece_tablet_id_;

  if (OB_FAIL(oas->reuse_scan_iter(false/*tablet id same*/, row_iter_))) {
    LOG_WARN("fail to reuse scan iter", K(ret));
  } else if (OB_FAIL(param.get_rowkey_range(rowkey_objs_, range))) {
    LOG_WARN("get_rowkey_range fail", K(ret));
  } else if (OB_FAIL(adaptor_->build_common_scan_param(param, param.has_single_chunk(), ObLobMetaUtil::LOB_META_COLUMN_CNT, scan_param_))) {
    LOG_WARN("build common scan param failed.", K(ret));
  } else {
    scan_param_.key_ranges_.reset();
    if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
      LOG_WARN("failed to push key range.", K(ret), K(scan_param_), K(range));
    } else if (OB_FAIL(oas->table_rescan(scan_param_, row_iter_))) {
      LOG_WARN("fail to do table rescan", K(ret), K(scan_param_));
    } else {
      meta_iter = row_iter_;
    }
  }
  return ret;
}

} // storage
} // oceanbase