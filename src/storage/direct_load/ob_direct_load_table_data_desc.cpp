// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadTableDataDesc::ObDirectLoadTableDataDesc()
  : rowkey_column_num_(0),
    column_count_(0),
    external_data_block_size_(0),
    sstable_index_block_size_(0),
    sstable_data_block_size_(0),
    extra_buf_size_(0),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    is_heap_table_(false),
    mem_chunk_size_(0),
    max_mem_chunk_count_(0),
    merge_count_per_round_(0),
    heap_table_mem_chunk_size_(0)
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
  is_heap_table_ = false;
  mem_chunk_size_ = 0;
  max_mem_chunk_count_ = 0;
  merge_count_per_round_ = 0;
  heap_table_mem_chunk_size_ = 0;
}

bool ObDirectLoadTableDataDesc::is_valid() const
{
  return (is_heap_table_ || rowkey_column_num_ > 0) &&
         rowkey_column_num_ <= column_count_ && column_count_ > 0 &&
         external_data_block_size_ > 0 && external_data_block_size_ % DIO_ALIGN_SIZE == 0 &&
         sstable_index_block_size_ > 0 && sstable_index_block_size_ % DIO_ALIGN_SIZE == 0 &&
         sstable_data_block_size_ > 0 && sstable_data_block_size_ % DIO_ALIGN_SIZE == 0 &&
         extra_buf_size_ > 0 && extra_buf_size_ % DIO_ALIGN_SIZE == 0 &&
         compressor_type_ > ObCompressorType::INVALID_COMPRESSOR && mem_chunk_size_ > 0 &&
         max_mem_chunk_count_ > 0 && merge_count_per_round_ > 0 && heap_table_mem_chunk_size_ > 0;
}

} // namespace storage
} // namespace oceanbase
