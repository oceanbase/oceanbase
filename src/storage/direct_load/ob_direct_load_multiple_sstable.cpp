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

#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_index_block_meta_scanner.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scanner.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadMultipleSSTableFragment
 */

ObDirectLoadMultipleSSTableFragment::ObDirectLoadMultipleSSTableFragment()
  : index_block_count_(0),
    data_block_count_(0),
    index_file_size_(0),
    data_file_size_(0),
    row_count_(0),
    max_data_block_size_(0)
{
}

ObDirectLoadMultipleSSTableFragment::~ObDirectLoadMultipleSSTableFragment()
{
}

int ObDirectLoadMultipleSSTableFragment::assign(const ObDirectLoadMultipleSSTableFragment &other)
{
  int ret = OB_SUCCESS;
  index_block_count_ = other.index_block_count_;
  data_block_count_ = other.data_block_count_;
  index_file_size_ = other.index_file_size_;
  data_file_size_ = other.data_file_size_;
  row_count_ = other.row_count_;
  max_data_block_size_ = other.max_data_block_size_;
  if (OB_FAIL(index_file_handle_.assign(other.index_file_handle_))) {
    LOG_WARN("fail to assign file handle", KR(ret));
  } else if (OB_FAIL(data_file_handle_.assign(other.data_file_handle_))) {
    LOG_WARN("fail to assign file handle", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableCreateParam
 */

ObDirectLoadMultipleSSTableCreateParam::ObDirectLoadMultipleSSTableCreateParam()
  : rowkey_column_num_(0),
    column_count_(0),
    index_block_size_(0),
    data_block_size_(0),
    index_block_count_(0),
    data_block_count_(0),
    row_count_(0),
    max_data_block_size_(0)
{
  fragments_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleSSTableCreateParam::~ObDirectLoadMultipleSSTableCreateParam()
{
}

bool ObDirectLoadMultipleSSTableCreateParam::is_valid() const
{
  return column_count_ > 0 && index_block_size_ > 0 && index_block_size_ % DIO_ALIGN_SIZE == 0 &&
         data_block_size_ > 0 && data_block_size_ % DIO_ALIGN_SIZE == 0 &&
         max_data_block_size_ > 0 && max_data_block_size_ % DIO_ALIGN_SIZE == 0 && row_count_ > 0 &&
         !start_key_.is_min_rowkey() && start_key_.is_valid() && !end_key_.is_min_rowkey() &&
         end_key_.is_valid();
}

/**
 * ObDirectLoadMultipleSSTableMeta
 */

ObDirectLoadMultipleSSTableMeta::ObDirectLoadMultipleSSTableMeta()
  : rowkey_column_num_(0),
    column_count_(0),
    index_block_size_(0),
    data_block_size_(0),
    index_block_count_(0),
    data_block_count_(0),
    row_count_(0),
    max_data_block_size_(0)
{
}

ObDirectLoadMultipleSSTableMeta::~ObDirectLoadMultipleSSTableMeta()
{
}

void ObDirectLoadMultipleSSTableMeta::reset()
{
  rowkey_column_num_ = 0;
  column_count_ = 0;
  index_block_size_ = 0;
  data_block_size_ = 0;
  index_block_count_ = 0;
  data_block_count_ = 0;
  row_count_ = 0;
  max_data_block_size_ = 0;
}

/**
 * ObDirectLoadMultipleSSTable
 */

ObDirectLoadMultipleSSTable::ObDirectLoadMultipleSSTable()
  : allocator_("TLD_MSSTable"), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  fragments_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleSSTable::~ObDirectLoadMultipleSSTable()
{
}

void ObDirectLoadMultipleSSTable::reset()
{
  tablet_id_.reset();
  meta_.reset();
  start_key_.reset();
  end_key_.reset();
  fragments_.reset();
  is_inited_ = false;
}

int ObDirectLoadMultipleSSTable::init(const ObDirectLoadMultipleSSTableCreateParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTable init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    tablet_id_ = param.tablet_id_;
    meta_.rowkey_column_num_ = param.rowkey_column_num_;
    meta_.column_count_ = param.column_count_;
    meta_.index_block_size_ = param.index_block_size_;
    meta_.data_block_size_ = param.data_block_size_;
    meta_.index_block_count_ = param.index_block_count_;
    meta_.data_block_count_ = param.data_block_count_;
    meta_.row_count_ = param.row_count_;
    meta_.max_data_block_size_ = param.max_data_block_size_;
    if (OB_FAIL(start_key_.deep_copy(param.start_key_, allocator_))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
    } else if (OB_FAIL(end_key_.deep_copy(param.end_key_, allocator_))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
    } else if (OB_FAIL(fragments_.assign(param.fragments_))) {
      LOG_WARN("fail to assign fragments", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObDirectLoadMultipleSSTable::release_data()
{
  fragments_.reset();
}

int ObDirectLoadMultipleSSTable::copy(const ObDirectLoadMultipleSSTable &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other));
  } else {
    reset();
    tablet_id_ = other.tablet_id_;
    meta_ = other.meta_;
    if (OB_FAIL(start_key_.deep_copy(other.start_key_, allocator_))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
    } else if (OB_FAIL(end_key_.deep_copy(other.end_key_, allocator_))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
    } else if (OB_FAIL(fragments_.assign(other.fragments_))) {
      LOG_WARN("fail to assign fragments", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

ObDirectLoadMultipleSSTable::IndexBlockIterator ObDirectLoadMultipleSSTable::index_block_begin()
{
  IndexBlockIterator iter;
  iter.sstable_ = this;
  iter.fragment_idx_ = 0;
  iter.index_block_idx_ = 0;
  return iter;
}

ObDirectLoadMultipleSSTable::IndexBlockIterator ObDirectLoadMultipleSSTable::index_block_end()
{
  IndexBlockIterator iter;
  iter.sstable_ = this;
  iter.fragment_idx_ = fragments_.count();
  iter.index_block_idx_ = 0;
  return iter;
}

ObDirectLoadMultipleSSTable::IndexEntryIterator ObDirectLoadMultipleSSTable::index_entry_begin()
{
  IndexEntryIterator iter;
  iter.sstable_ = this;
  iter.fragment_idx_ = 0;
  iter.index_entry_idx_ = 0;
  return iter;
}

ObDirectLoadMultipleSSTable::IndexEntryIterator ObDirectLoadMultipleSSTable::index_entry_end()
{
  IndexEntryIterator iter;
  iter.sstable_ = this;
  iter.fragment_idx_ = fragments_.count();
  iter.index_entry_idx_ = 0;
  return iter;
}

int ObDirectLoadMultipleSSTable::scan(const ObDirectLoadTableDataDesc &table_data_desc,
                                      const ObDirectLoadMultipleDatumRange &range,
                                      const ObStorageDatumUtils *datum_utils,
                                      ObIAllocator &allocator,
                                      ObDirectLoadMultipleSSTableScanner *&scanner)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTable not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!table_data_desc.is_valid() || !range.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_data_desc), K(range), KP(datum_utils));
  } else {
    scanner = nullptr;
    if (OB_ISNULL(scanner = OB_NEWx(ObDirectLoadMultipleSSTableScanner, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTableScanner", KR(ret));
    } else if (OB_FAIL(scanner->init(this, table_data_desc, range, datum_utils))) {
      LOG_WARN("fail to init multiple sstable scanner", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != scanner) {
        scanner->~ObDirectLoadMultipleSSTableScanner();
        allocator.free(scanner);
        scanner = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTable::scan_whole_index_block_meta(
  const ObDirectLoadTableDataDesc &table_data_desc,
  ObIAllocator &allocator,
  ObDirectLoadMultipleSSTableIndexBlockMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  meta_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTable not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_data_desc));
  } else {
    ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner *whole_scanner = nullptr;
    if (OB_ISNULL(whole_scanner =
                    OB_NEWx(ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner", KR(ret));
    } else if (OB_FAIL(whole_scanner->init(this, table_data_desc))) {
      LOG_WARN("fail to init multiple sstable index block meta scanner", KR(ret));
    } else {
      meta_iter = whole_scanner;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != whole_scanner) {
        whole_scanner->~ObDirectLoadMultipleSSTableIndexBlockMetaWholeScanner();
        allocator.free(whole_scanner);
        whole_scanner = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTable::scan_tablet_whole_index_block_meta(
  const ObTabletID &tablet_id,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils,
  ObIAllocator &allocator,
  ObDirectLoadMultipleSSTableIndexBlockMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  meta_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTable not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !table_data_desc.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(table_data_desc), KP(datum_utils));
  } else {
    ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner *tablet_whole_scanner = nullptr;
    if (OB_ISNULL(tablet_whole_scanner = OB_NEWx(
                    ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner", KR(ret));
    } else if (OB_FAIL(tablet_whole_scanner->init(this, tablet_id, table_data_desc, datum_utils))) {
      LOG_WARN("fail to init multiple sstable index block meta scanner", KR(ret));
    } else {
      meta_iter = tablet_whole_scanner;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != tablet_whole_scanner) {
        tablet_whole_scanner->~ObDirectLoadMultipleSSTableIndexBlockMetaTabletWholeScanner();
        allocator.free(tablet_whole_scanner);
        tablet_whole_scanner = nullptr;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
