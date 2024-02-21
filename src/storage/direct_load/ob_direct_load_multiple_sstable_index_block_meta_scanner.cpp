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

#include "storage/direct_load/ob_direct_load_multiple_sstable_index_block_meta_scanner.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_index_block_compare.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

ObDirectLoadMultipleSSTableIndexBlockMeta::ObDirectLoadMultipleSSTableIndexBlockMeta()
  : count_(0)
{
}

ObDirectLoadMultipleSSTableIndexBlockMeta::~ObDirectLoadMultipleSSTableIndexBlockMeta()
{
}

/**
 * ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner
 */

ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner::
  ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner()
  : sstable_(nullptr), is_inited_(false)
{
}

ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner::
  ~ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner()
{
}

int ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner::init(
  ObDirectLoadMultipleSSTable *sstable, const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid() ||
                         !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable), K(table_data_desc));
  } else {
    if (OB_FAIL(index_block_reader_.init(table_data_desc.sstable_index_block_size_,
                                         table_data_desc.compressor_type_))) {
      LOG_WARN("fail to index block reader", KR(ret));
    } else if (OB_FAIL(data_block_reader_.init(table_data_desc.sstable_data_block_size_,
                                               table_data_desc.compressor_type_))) {
      LOG_WARN("fail to data block reader", KR(ret));
    } else {
      sstable_ = sstable;
      current_index_block_iter_ = sstable->index_block_begin();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner::get_next_meta(
  ObDirectLoadMultipleSSTableIndexBlockMeta &index_block_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner not init", KR(ret), KP(this));
  } else if (current_index_block_iter_ == sstable_->index_block_end()) {
    ret = OB_ITER_END;
  } else {
    const ObDirectLoadMultipleSSTableFragment &fragment =
      sstable_->get_fragments().at(current_index_block_iter_.fragment_idx_);
    int64_t index_block_offset =
      sstable_->get_meta().index_block_size_ * current_index_block_iter_.index_block_idx_;
    const ObDirectLoadSSTableIndexEntry *last_index_entry = nullptr;
    const RowType *row = nullptr;
    index_block_reader_.reuse();
    data_block_reader_.reuse();
    if (OB_FAIL(index_block_reader_.open(fragment.index_file_handle_, index_block_offset,
                                         sstable_->get_meta().index_block_size_))) {
      LOG_WARN("fail to open index file", KR(ret), K(fragment), K(index_block_offset));
    } else if (OB_FAIL(index_block_reader_.get_last_entry(last_index_entry))) {
      LOG_WARN("fail to get last entry", KR(ret));
    } else if (OB_FAIL(data_block_reader_.open(
                 fragment.data_file_handle_, last_index_entry->offset_, last_index_entry->size_))) {
      LOG_WARN("fail to open data file", KR(ret), K(fragment), KPC(last_index_entry));
    } else if (OB_FAIL(data_block_reader_.get_last_row(row))) {
      LOG_WARN("fail to get last row", KR(ret));
    } else {
      index_block_meta.count_ = index_block_reader_.get_header().count_;
      index_block_meta.end_key_ = row->rowkey_;
      ++current_index_block_iter_;
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner
 */

ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner::
  ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner()
  : sstable_(nullptr), is_iter_end_(false), is_inited_(false)
{
}

ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner::
  ~ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner()
{
}

int ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner::init(
  ObDirectLoadMultipleSSTable *sstable, const ObTabletID &tablet_id,
  const ObDirectLoadTableDataDesc &table_data_desc, const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner init twice", KR(ret),
             KP(this));
  } else if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid() || !tablet_id.is_valid() ||
                         !table_data_desc.is_valid() || nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable), K(table_data_desc), K(tablet_id),
             KP(datum_utils));
  } else {
    sstable_ = sstable;
    tablet_id_ = tablet_id;
    datum_utils_ = datum_utils;
    if (OB_FAIL(index_block_reader_.init(table_data_desc.sstable_index_block_size_,
                                         table_data_desc.compressor_type_))) {
      LOG_WARN("fail to index block reader", KR(ret));
    } else if (OB_FAIL(data_block_reader_.init(table_data_desc.sstable_data_block_size_,
                                               table_data_desc.compressor_type_))) {
      LOG_WARN("fail to data block reader", KR(ret));
    } else if (OB_FAIL(locate_left_border(index_block_reader_, data_block_reader_))) {
      LOG_WARN("fail to locate left border", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner::locate_left_border(
  IndexBlockReader &index_block_reader, DataBlockReader &data_block_reader)
{
  int ret = OB_SUCCESS;
  if (tablet_id_.compare(sstable_->get_start_key().tablet_id_) < 0) { // tablet_id < sstable
    current_index_block_iter_ = sstable_->index_block_end();
  } else if (tablet_id_.compare(sstable_->get_end_key().tablet_id_) > 0) { // tablet_id > sstable
    current_index_block_iter_ = sstable_->index_block_end();
  } else {
    ObDirectLoadMultipleDatumRowkey tablet_min_rowkey;
    if (OB_FAIL(tablet_min_rowkey.set_tablet_min_rowkey(tablet_id_))) {
      LOG_WARN("fail to set tablet min rowkey", KR(ret));
    } else {
      ObDirectLoadMultipleSSTableIndexBlockEndKeyCompare compare(
        ret, sstable_, datum_utils_, index_block_reader, data_block_reader);
      // tablet_min_rowkey < block_last_key
      current_index_block_iter_ = std::upper_bound(
        sstable_->index_block_begin(), sstable_->index_block_end(), tablet_min_rowkey, compare);
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner::get_next_meta(
  ObDirectLoadMultipleSSTableIndexBlockMeta &index_block_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner not init", KR(ret),
             KP(this));
  } else if (is_iter_end_ || current_index_block_iter_ == sstable_->index_block_end()) {
    ret = OB_ITER_END;
  } else {
    const ObDirectLoadMultipleSSTableFragment &fragment =
      sstable_->get_fragments().at(current_index_block_iter_.fragment_idx_);
    int64_t index_block_offset =
      sstable_->get_meta().index_block_size_ * current_index_block_iter_.index_block_idx_;
    const ObDirectLoadSSTableIndexEntry *last_index_entry = nullptr;
    const RowType *row = nullptr;
    index_block_reader_.reuse();
    data_block_reader_.reuse();
    if (OB_FAIL(index_block_reader_.open(fragment.index_file_handle_, index_block_offset,
                                         sstable_->get_meta().index_block_size_))) {
      LOG_WARN("fail to open index file", KR(ret), K(fragment), K(index_block_offset));
    } else if (OB_FAIL(index_block_reader_.get_last_entry(last_index_entry))) {
      LOG_WARN("fail to get last entry", KR(ret));
    } else if (OB_FAIL(data_block_reader_.open(
                 fragment.data_file_handle_, last_index_entry->offset_, last_index_entry->size_))) {
      LOG_WARN("fail to open data file", KR(ret), K(fragment), KPC(last_index_entry));
    } else if (OB_FAIL(data_block_reader_.get_last_row(row))) {
      LOG_WARN("fail to get last row", KR(ret));
    } else {
      int cmp_ret = tablet_id_.compare(row->rowkey_.tablet_id_);
      if (OB_UNLIKELY(cmp_ret > 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rowkey", KR(ret), K(tablet_id_), K(row->rowkey_));
      } else if (cmp_ret < 0) {
        ret = OB_ITER_END;
        is_iter_end_ = true;
      } else {
        index_block_meta.count_ = index_block_reader_.get_header().count_;
        index_block_meta.end_key_ = row->rowkey_;
        ++current_index_block_iter_;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator
 */

ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator::
  ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator()
  : index_block_meta_iter_(nullptr), is_inited_(false)
{
}

ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator::
  ~ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator()
{
  if (nullptr != index_block_meta_iter_) {
    index_block_meta_iter_->~ObDirectLoadMultipleSSTableIndexBlockMetaIterator();
    index_block_meta_iter_ = nullptr;
  }
}

int ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator::init(
  ObDirectLoadMultipleSSTableIndexBlockMetaIterator *index_block_meta_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(index_block_meta_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(index_block_meta_iter));
  } else {
    index_block_meta_iter_ = index_block_meta_iter;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator::get_next_rowkey(
  const ObDirectLoadMultipleDatumRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMacroBlockEndKeyIterator not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(index_block_meta_iter_->get_next_meta(index_block_meta_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next index block meta", KR(ret));
      }
    } else {
      rowkey = &index_block_meta_.end_key_;
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator
 */

ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator::
  ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator()
  : index_block_meta_iter_(nullptr), is_inited_(false)
{
}

ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator::
  ~ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator()
{
  if (nullptr != index_block_meta_iter_) {
    index_block_meta_iter_->~ObDirectLoadMultipleSSTableIndexBlockMetaIterator();
    index_block_meta_iter_ = nullptr;
  }
}

int ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator::init(
  ObDirectLoadMultipleSSTableIndexBlockMetaIterator *index_block_meta_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator init twice", KR(ret),
             KP(this));
  } else if (OB_ISNULL(index_block_meta_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(index_block_meta_iter));
  } else {
    index_block_meta_iter_ = index_block_meta_iter;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator::get_next_rowkey(
  const ObDatumRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMacroBlockEndKeyIterator not init", KR(ret), KP(this));
  } else {
    ObDirectLoadMultipleSSTableIndexBlockMeta index_block_meta;
    if (OB_FAIL(index_block_meta_iter_->get_next_meta(index_block_meta))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next index block meta", KR(ret));
      }
    } else if (OB_FAIL(index_block_meta.end_key_.get_rowkey(rowkey_))) {
      LOG_WARN("fail to get rowkey", KR(ret));
    } else {
      rowkey = &rowkey_;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
