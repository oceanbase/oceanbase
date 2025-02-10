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

#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace observer;

ObDirectLoadTableDataDesc::ObDirectLoadTableDataDesc()
  : rowkey_column_num_(0),
    column_count_(0),
    external_data_block_size_(0),
    sstable_index_block_size_(0),
    sstable_data_block_size_(0),
    extra_buf_size_(0),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    is_shared_storage_(false),
    row_flag_()
{
}

ObDirectLoadTableDataDesc::~ObDirectLoadTableDataDesc()
{
}

void ObDirectLoadTableDataDesc::reset()
{
  rowkey_column_num_ = 0;
  column_count_ = 0;
  external_data_block_size_ = 0;
  sstable_index_block_size_ = 0;
  sstable_data_block_size_ = 0;
  extra_buf_size_ = 0;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  is_shared_storage_ = false;
  row_flag_.reset();
}

bool ObDirectLoadTableDataDesc::is_valid() const
{
  return rowkey_column_num_ >= 0 &&
         rowkey_column_num_ <= column_count_ && column_count_ > 0 &&
         external_data_block_size_ > 0 && external_data_block_size_ % DIO_ALIGN_SIZE == 0 &&
         sstable_index_block_size_ > 0 && sstable_index_block_size_ % DIO_ALIGN_SIZE == 0 &&
         sstable_data_block_size_ > 0 && sstable_data_block_size_ % DIO_ALIGN_SIZE == 0 &&
         extra_buf_size_ > 0 && extra_buf_size_ % DIO_ALIGN_SIZE == 0 &&
         compressor_type_ > ObCompressorType::INVALID_COMPRESSOR;
}

} // namespace storage
} // namespace oceanbase
