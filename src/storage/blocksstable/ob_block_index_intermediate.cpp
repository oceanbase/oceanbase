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

#include "ob_block_index_intermediate.h"
#include "common/row/ob_row.h"
#include "ob_block_sstable_struct.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {

ObBlockIntermediateHeader::ObBlockIntermediateHeader()
    : version_(INTERMEDIATE_HEADER_VERSION),
      offset_(0),
      size_(0),
      is_leaf_node_(0),
      reserved_(0),
      row_count_(0),
      macro_version_(0),
      macro_logic_id_(0)
{}

ObBlockIntermediateHeader::ObBlockIntermediateHeader(const int64_t macro_version, const int64_t macro_logic_id,
    const int32_t offset, const int32_t size, const int64_t row_count, const bool is_leaf_node)
    : version_(INTERMEDIATE_HEADER_VERSION),
      offset_(offset),
      size_(size),
      is_leaf_node_(is_leaf_node),
      reserved_(0),
      row_count_(row_count),
      macro_version_(macro_version),
      macro_logic_id_(macro_logic_id)
{}

ObBlockIntermediateHeader::~ObBlockIntermediateHeader()
{}

ObBlockIntermediateBuilder::ObBlockIntermediateBuilder()
    : header_(),
      intermediate_row_(),
      allocator_(ObModIds::OB_BLOCK_INDEX_INTERMEDIATE),
      row_reader_(),
      rowkey_column_count_(0),
      is_inited_(false)
{}

ObBlockIntermediateBuilder::~ObBlockIntermediateBuilder()
{
  reset();
}

void ObBlockIntermediateBuilder::reset()
{
  intermediate_row_.reset();
  header_.reset();
  allocator_.reset();
  rowkey_column_count_ = 0;
  is_inited_ = false;
}

void ObBlockIntermediateBuilder::reuse()
{
  header_.reset();
  allocator_.reuse();
}

int ObBlockIntermediateBuilder::init(const int64_t rowkey_column_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBlockIntermediateBuilder init twice", K(ret));
  } else {
    rowkey_column_count_ = rowkey_column_count;
    intermediate_row_.flag_ = common::ObActionFlag::OP_ROW_EXIST;
    intermediate_row_.row_val_.cells_ = new (obj_buf_) ObObj[rowkey_column_count_ + 1];
    intermediate_row_.row_val_.count_ = rowkey_column_count_ + 1;
    intermediate_row_.row_val_.projector_ = nullptr;
    intermediate_row_.row_val_.projector_size_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObBlockIntermediateBuilder::build_intermediate_row(
    const ObBlockIntermediateHeader& header, const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBlockIntermediateBuilder has not been inited", K(ret));
  } else if (OB_UNLIKELY(!header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "BlockIntermediate head is invalid", K(ret), K(header));
  } else {
    header_ = header;
    common::ObObj& obj = intermediate_row_.row_val_.cells_[rowkey_column_count_];
    ObString str(sizeof(header_), reinterpret_cast<char*>(&header_));
    obj.set_binary(str);
    row = &intermediate_row_;
  }
  return ret;
}

int ObBlockIntermediateBuilder::set_rowkey(const ObStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBlockIntermediateBuilder not inited,", K(ret));
  } else if (OB_UNLIKELY(rowkey_column_count_ != rowkey.get_obj_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Rowkey column count is not match", K(rowkey_column_count_), K(rowkey.get_obj_cnt()), K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < rowkey_column_count_; ++i) {
      if (OB_FAIL(deep_copy_obj(allocator_, rowkey.get_obj_ptr()[i], intermediate_row_.row_val_.cells_[i]))) {
        STORAGE_LOG(WARN, "failed to deep copy obj", K(ret));
      }
    }
  }
  return ret;
}

int ObBlockIntermediateBuilder::set_rowkey(const ObDataStoreDesc& desc, const ObString& s_rowkey)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBlockIntermediateBuilder not inited,", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid() || rowkey_column_count_ != desc.rowkey_column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "ObDataStoreDesc is invalid or Rowkey column count is not match",
        K(rowkey_column_count_),
        K(desc),
        K(ret));
  } else if (OB_FAIL(row_reader_.read_compact_rowkey(desc.column_types_,
                 desc.rowkey_column_count_,
                 s_rowkey.ptr(),
                 s_rowkey.length(),
                 pos,
                 intermediate_row_.row_val_))) {
    STORAGE_LOG(WARN, "row reader fail to read row.", K(ret));
  } else {
    common::ObNewRow& row = intermediate_row_.row_val_;
    row.count_ = rowkey_column_count_ + 1;
    for (int i = 0; OB_SUCC(ret) && i < rowkey_column_count_; ++i) {
      if (row.cells_[i].need_deep_copy() && OB_FAIL(deep_copy_obj(allocator_, row.cells_[i], row.cells_[i]))) {
        STORAGE_LOG(WARN, "failed to deep copy obj", K(i), K(row.cells_[i]), K(ret));
      }
    }
  }
  return ret;
}

ObBlockIntermediateRowParser::ObBlockIntermediateRowParser()
    : intermediate_row_(nullptr), header_(nullptr), rowkey_column_count_(0), is_inited_(false)
{}

ObBlockIntermediateRowParser::~ObBlockIntermediateRowParser()
{}

int ObBlockIntermediateRowParser::init(const int64_t rowkey_column_count, const storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBlockIntermediateRowParser init twice", K(ret));
  } else if (OB_UNLIKELY(rowkey_column_count + 1 != row.row_val_.count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "rowkey column count mismatch", K(ret), K(rowkey_column_count), K(row));
  } else {
    ObString obj_value;
    const ObObj& obj = row.row_val_.cells_[rowkey_column_count];
    if (OB_FAIL(obj.get_binary(obj_value))) {
      STORAGE_LOG(WARN, "get binary obj failed, ", K(ret), K(obj));
    } else if (OB_UNLIKELY(obj_value.length() < sizeof(ObBlockIntermediateHeader))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get binary obj is invalid", K(ret), K(obj));
    } else {
      header_ = reinterpret_cast<ObBlockIntermediateHeader*>(obj_value.ptr());
      if (OB_UNLIKELY(!header_->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "ObBlockIntermediateHeader is not valid", K(*header_), K(ret));
        header_ = nullptr;
      } else {
        rowkey_column_count_ = rowkey_column_count;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObBlockIntermediateRowParser::get_intermediate_header(const ObBlockIntermediateHeader*& header)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBlockIntermediateBuilder has not been inited", K(ret));
  } else {
    header = header_;
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
