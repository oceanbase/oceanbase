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

#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scanner.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadMultipleHeapTableDataFragment
 */

ObDirectLoadMultipleHeapTableDataFragment::ObDirectLoadMultipleHeapTableDataFragment()
  : block_count_(0), file_size_(0), row_count_(0), max_block_size_(0)
{
}

ObDirectLoadMultipleHeapTableDataFragment::~ObDirectLoadMultipleHeapTableDataFragment()
{
}

int ObDirectLoadMultipleHeapTableDataFragment::assign(
  const ObDirectLoadMultipleHeapTableDataFragment &other)
{
  int ret = OB_SUCCESS;
  block_count_ = other.block_count_;
  file_size_ = other.file_size_;
  row_count_ = other.row_count_;
  max_block_size_ = other.max_block_size_;
  if (OB_FAIL(file_handle_.assign(other.file_handle_))) {
    LOG_WARN("fail to assign file handle", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadMultipleHeapTableCreateParam
 */

ObDirectLoadMultipleHeapTableCreateParam::ObDirectLoadMultipleHeapTableCreateParam()
  : column_count_(0),
    index_block_size_(0),
    data_block_size_(0),
    index_block_count_(0),
    data_block_count_(0),
    index_file_size_(0),
    data_file_size_(0),
    index_entry_count_(0),
    row_count_(0),
    max_data_block_size_(0)
{
}

ObDirectLoadMultipleHeapTableCreateParam::~ObDirectLoadMultipleHeapTableCreateParam()
{
}

bool ObDirectLoadMultipleHeapTableCreateParam::is_valid() const
{
  return column_count_ > 0 && index_block_size_ > 0 && index_block_size_ % DIO_ALIGN_SIZE == 0 &&
         data_block_size_ > 0 && data_block_size_ % DIO_ALIGN_SIZE == 0 && index_block_count_ > 0 &&
         data_block_count_ > 0 && index_file_size_ > 0 && data_file_size_ > 0 &&
         index_entry_count_ > 0 && row_count_ > 0 && max_data_block_size_ > 0 &&
         max_data_block_size_ % DIO_ALIGN_SIZE == 0 && index_file_handle_.is_valid() &&
         !data_fragments_.empty();
}

/**
 * ObDirectLoadMultipleHeapTableMeta
 */

ObDirectLoadMultipleHeapTableMeta::ObDirectLoadMultipleHeapTableMeta()
  : column_count_(0),
    index_block_size_(0),
    data_block_size_(0),
    index_block_count_(0),
    data_block_count_(0),
    index_file_size_(0),
    data_file_size_(0),
    index_entry_count_(0),
    row_count_(0),
    max_data_block_size_(0)
{
}

ObDirectLoadMultipleHeapTableMeta::~ObDirectLoadMultipleHeapTableMeta()
{
}

void ObDirectLoadMultipleHeapTableMeta::reset()
{
  column_count_ = 0;
  index_block_size_ = 0;
  data_block_size_ = 0;
  index_block_count_ = 0;
  data_block_count_ = 0;
  index_file_size_ = 0;
  data_file_size_ = 0;
  index_entry_count_ = 0;
  row_count_ = 0;
  max_data_block_size_ = 0;
}

/**
 * ObDirectLoadMultipleSSTable
 */

ObDirectLoadMultipleHeapTable::ObDirectLoadMultipleHeapTable()
  : is_inited_(false)
{
}

ObDirectLoadMultipleHeapTable::~ObDirectLoadMultipleHeapTable()
{
}

void ObDirectLoadMultipleHeapTable::reset()
{
  meta_.reset();
  index_file_handle_.reset();
  data_fragments_.reset();
  is_inited_ = false;
}

int ObDirectLoadMultipleHeapTable::init(const ObDirectLoadMultipleHeapTableCreateParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleHeapTable init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    meta_.column_count_ = param.column_count_;
    meta_.index_block_size_ = param.index_block_size_;
    meta_.data_block_size_ = param.data_block_size_;
    meta_.index_block_count_ = param.index_block_count_;
    meta_.data_block_count_ = param.data_block_count_;
    meta_.index_file_size_ = param.index_file_size_;
    meta_.data_file_size_ = param.data_file_size_;
    meta_.index_entry_count_ = param.index_entry_count_;
    meta_.row_count_ = param.row_count_;
    meta_.max_data_block_size_ = param.max_data_block_size_;
    if (OB_FAIL(index_file_handle_.assign(param.index_file_handle_))) {
      LOG_WARN("fail to assign file handle", KR(ret));
    } else if (OB_FAIL(data_fragments_.assign(param.data_fragments_))) {
      LOG_WARN("fail to assign data fragments", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTable::copy(const ObDirectLoadMultipleHeapTable &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other));
  } else {
    reset();
    meta_ = other.meta_;
    if (OB_FAIL(index_file_handle_.assign(other.index_file_handle_))) {
      LOG_WARN("fail to assign file handle", KR(ret));
    } else if (OB_FAIL(data_fragments_.assign(other.data_fragments_))) {
      LOG_WARN("fail to assign data fragments", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

ObDirectLoadMultipleHeapTable::IndexEntryIterator ObDirectLoadMultipleHeapTable::index_entry_begin()
{
  IndexEntryIterator iter;
  iter.table_ = this;
  iter.index_entry_idx_ = 0;
  return iter;
}

ObDirectLoadMultipleHeapTable::IndexEntryIterator ObDirectLoadMultipleHeapTable::index_entry_end()
{
  IndexEntryIterator iter;
  iter.table_ = this;
  iter.index_entry_idx_ = meta_.index_entry_count_;
  return iter;
}

int ObDirectLoadMultipleHeapTable::get_tablet_row_count(
  const ObTabletID &tablet_id,
  const ObDirectLoadTableDataDesc &table_data_desc,
  int64_t &row_count)
{
  int ret = OB_SUCCESS;
  row_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTable not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadMultipleHeapTableTabletIndex *tablet_index = nullptr;
    ObDirectLoadMultipleHeapTableTabletIndexWholeScanner index_scanner;
    if (OB_FAIL(index_scanner.init(this, tablet_id, table_data_desc))) {
      LOG_WARN("fail to init index scanner", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(index_scanner.get_next_index(tablet_index))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next index", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        row_count += tablet_index->row_count_;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
