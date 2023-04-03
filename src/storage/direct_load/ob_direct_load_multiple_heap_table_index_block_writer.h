// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_data_block_writer.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleHeapTableIndexBlockWriter
  : public ObDirectLoadDataBlockWriter<ObDirectLoadMultipleHeapTableIndexBlock::Header,
                                       ObDirectLoadMultipleHeapTableIndexBlock::Entry>
{
public:
  ObDirectLoadMultipleHeapTableIndexBlockWriter();
  virtual ~ObDirectLoadMultipleHeapTableIndexBlockWriter();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type);
  int append_index(const ObDirectLoadMultipleHeapTableTabletIndex &tablet_index);
private:
  int pre_write_item() override;
  int pre_flush_buffer() override;
private:
  int64_t entry_count_;
  int32_t last_entry_pos_;
  int32_t cur_entry_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadMultipleHeapTableIndexBlockWriter);
};

} // namespace storage
} // namespace oceanbase
