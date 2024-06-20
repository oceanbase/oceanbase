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

#include "storage/direct_load/ob_direct_load_sstable_scanner.h"
#include "storage/direct_load/ob_direct_load_sstable.h"
#include "observer/table_load/ob_table_load_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace observer;
using namespace share;
using namespace share::schema;

/**
 * ObDirectLoadSSTableFragmentMeta
 */

ObDirectLoadSSTableFragmentMeta::ObDirectLoadSSTableFragmentMeta()
  : index_item_count_(0), index_block_count_(0), row_count_(0), occupy_size_(0)
{
}

ObDirectLoadSSTableFragmentMeta::~ObDirectLoadSSTableFragmentMeta() {}

void ObDirectLoadSSTableFragmentMeta::reset()
{
  index_item_count_ = 0;
  index_block_count_ = 0;
  row_count_ = 0;
  occupy_size_ = 0;
}

bool ObDirectLoadSSTableFragmentMeta::is_valid() const
{
  return index_item_count_ > 0 && index_block_count_ > 0 && row_count_ > 0 && occupy_size_ > 0;
}

/**
 * ObDirectLoadSSTableFragment
 */

ObDirectLoadSSTableFragment::ObDirectLoadSSTableFragment() {}

ObDirectLoadSSTableFragment::~ObDirectLoadSSTableFragment() { reset(); }

void ObDirectLoadSSTableFragment::reset()
{
  meta_.reset();
  index_file_handle_.reset();
  data_file_handle_.reset();
}

bool ObDirectLoadSSTableFragment::is_valid() const
{
  return meta_.is_valid() && index_file_handle_.is_valid() && data_file_handle_.is_valid();
}

int ObDirectLoadSSTableFragment::assign(const ObDirectLoadSSTableFragment &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other));
  } else {
    reset();
    meta_ = other.meta_;
    if (OB_FAIL(index_file_handle_.assign(other.index_file_handle_))) {
      LOG_WARN("fail to assign index file", KR(ret));
    } else if (OB_FAIL(data_file_handle_.assign(other.data_file_handle_))) {
      LOG_WARN("fail to assign data file", KR(ret));
    }
  }
  return ret;
}

/**
 * ObDirectLoadSSTableMeta
 */

void ObDirectLoadSSTableMeta::reset()
{
  tablet_id_.reset();
  rowkey_column_count_ = 0;
  column_count_ = 0;
  index_block_size_ = 0;
  data_block_size_ = 0;
  index_item_count_ = 0;
  index_block_count_ = 0;
  row_count_ = 0;
}

/**
 * ObDirectLoadSSTable
 */

ObDirectLoadSSTable::ObDirectLoadSSTable()
  : allocator_("TLD_SSTable"), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  fragments_.set_tenant_id(MTL_ID());
}

ObDirectLoadSSTable::~ObDirectLoadSSTable() {}

void ObDirectLoadSSTable::reset()
{
  meta_.reset();
  start_key_.reset();
  end_key_.reset();
  fragments_.reset();
  allocator_.reset();
  is_inited_ = false;
}

int ObDirectLoadSSTable::init(ObDirectLoadSSTableCreateParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadSSTable init twice", KR(ret), KP(this));
  } else {
    meta_.tablet_id_ = param.tablet_id_;
    meta_.rowkey_column_count_ = param.rowkey_column_count_;
    meta_.column_count_ = param.column_count_;
    meta_.index_block_size_ = param.index_block_size_;
    meta_.data_block_size_ = param.data_block_size_;
    meta_.index_item_count_ = param.index_item_count_;
    meta_.index_block_count_ = param.index_block_count_;
    meta_.row_count_ = param.row_count_;
    if (param.row_count_ > 0) {
      if (OB_UNLIKELY(param.fragments_.empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", KR(ret), K(param));
      } else if (OB_FAIL(param.start_key_.deep_copy(start_key_, allocator_))) {
        LOG_WARN("fail to deep copy start key", KR(ret));
      } else if (OB_FAIL(param.end_key_.deep_copy(end_key_, allocator_))) {
        LOG_WARN("fail to deep copy start key", KR(ret));
      } else if (OB_FAIL(fragments_.assign(param.fragments_))) {
        LOG_WARN("fail to assign fragments", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObDirectLoadSSTable::release_data()
{
  fragments_.reset();
}

int ObDirectLoadSSTable::copy(const ObDirectLoadSSTable &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other));
  } else {
    reset();
    meta_ = other.meta_;
    if (meta_.row_count_ > 0) {
      if (OB_FAIL(other.start_key_.deep_copy(start_key_, allocator_))) {
        LOG_WARN("fail to deep copy start key", KR(ret));
      } else if (OB_FAIL(other.end_key_.deep_copy(end_key_, allocator_))) {
        LOG_WARN("fail to deep copy start key", KR(ret));
      } else if (OB_FAIL(fragments_.assign(other.fragments_))) {
        LOG_WARN("fail to assign fragments", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadSSTable::scan_index_block_meta(ObIAllocator &allocator,
                                                     ObDirectLoadIndexBlockMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTable not init", KR(ret), KP(this));
  } else {
    void *buf = nullptr;
    ObDirectLoadIndexBlockMetaIterator *iter = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObDirectLoadIndexBlockMetaIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory", K(ret));
    } else if (OB_ISNULL(iter = new (buf) ObDirectLoadIndexBlockMetaIterator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null pointer of secondary meta iterator", K(ret));
    } else if (OB_FAIL(iter->init(this))) {
      LOG_WARN("Fail to open index block meta iterator", K(ret));
    } else {
      meta_iter = iter;
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(iter)) {
        iter->~ObDirectLoadIndexBlockMetaIterator();
      }
      if (OB_NOT_NULL(buf)) {
        allocator.free(buf);
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTable::scan(const ObDirectLoadTableDataDesc &table_data_desc,
                                    const ObDatumRange &key_range,
                                    const ObStorageDatumUtils *datum_utils, ObIAllocator &allocator,
                                    ObDirectLoadSSTableScanner *&scanner)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTable not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!table_data_desc.is_valid() || !key_range.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_data_desc), K(key_range), KP(datum_utils));
  } else {
    scanner = nullptr;
    // check param
    if (OB_UNLIKELY(meta_.rowkey_column_count_ != table_data_desc.rowkey_column_num_ ||
                    meta_.column_count_ != table_data_desc.column_count_ ||
                    meta_.index_block_size_ != table_data_desc.sstable_index_block_size_ ||
                    meta_.data_block_size_ != table_data_desc.sstable_data_block_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sstable", KR(ret), K(meta_), K(table_data_desc));
    } else if (OB_ISNULL(scanner = OB_NEWx(ObDirectLoadSSTableScanner, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadSSTableScanner", KR(ret));
    } else if (OB_FAIL(scanner->init(this, table_data_desc, key_range, datum_utils))) {
      LOG_WARN("fail to init sstable scanner", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != scanner) {
        scanner->~ObDirectLoadSSTableScanner();
        allocator.free(scanner);
        scanner = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadSSTableFragmentOperator
 */

int ObDirectLoadSSTableFragmentOperator::init(ObDirectLoadSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObDirectLoadIndexBlockMetaIterator has been inited", K(ret));
  } else if (OB_ISNULL(sstable) || !sstable->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(sstable));
  } else {
    sstable_ = sstable;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadSSTableFragmentOperator::get_fragment(
  int64_t idx, ObDirectLoadSSTableFragment &fragment)
{
  int ret = OB_SUCCESS;
  if (idx >= sstable_->get_fragment_array().count()) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_WARN("no fragment", K(ret), K(idx), K(sstable_->get_fragment_array().count()));
  } else if (OB_FAIL(fragment.assign(sstable_->get_fragment_array().at(idx)))) {
    LOG_WARN("fail to assign fragment", K(ret));
  }
  return ret;
}

int ObDirectLoadSSTableFragmentOperator::get_next_fragment(
  int64_t &curr_fragment_idx, ObDirectLoadSSTableFragment &fragment)
{
  int ret = OB_SUCCESS;
  if (curr_fragment_idx >= sstable_->get_fragment_array().count() - 1) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_WARN("no next fragment", K(ret), K(curr_fragment_idx),
             K(sstable_->get_fragment_array().count()));
  } else if (OB_FAIL(fragment.assign(sstable_->get_fragment_array().at(curr_fragment_idx + 1)))) {
    LOG_WARN("fail to assign fragment", K(ret));
  } else {
    curr_fragment_idx++;
  }
  return ret;
}

//索引项重新映射到对应文件的对应索引项
int ObDirectLoadSSTableFragmentOperator::get_fragment_item_idx(int64_t idx,
                                                                     int64_t &locate_fragment_idx,
                                                                     int64_t &new_idx)
{
  int ret = OB_SUCCESS;
  if (idx >= sstable_->get_meta().index_item_count_) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_WARN("no other fragment", K(ret), K(idx), K(sstable_->get_meta().index_item_count_));
  } else {
    for (int64_t i = 0; i < sstable_->get_fragment_array().count(); ++i) {
      if (idx <= (sstable_->get_fragment_array().at(i).meta_.index_item_count_ - 1)) {
        new_idx = idx;
        locate_fragment_idx = i;
        break;
      } else {
        idx = idx - sstable_->get_fragment_array().at(i).meta_.index_item_count_;
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableFragmentOperator::get_fragment_block_idx(int64_t idx,
                                                                      int64_t &locate_fragment_idx,
                                                                      int64_t &new_idx)
{
  int ret = OB_SUCCESS;
  if (idx >= sstable_->get_meta().index_block_count_) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_WARN("no other fragment", K(ret), K(idx), K(sstable_->get_meta().index_block_count_));
  } else {
    for (int64_t i = 0; i < sstable_->get_fragment_array().count(); ++i) {
      if (idx <= (sstable_->get_fragment_array().at(i).meta_.index_block_count_ - 1)) {
        new_idx = idx;
        locate_fragment_idx = i;
        break;
      } else {
        idx = idx - sstable_->get_fragment_array().at(i).meta_.index_block_count_;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
