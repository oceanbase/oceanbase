// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_data_block_writer.h"
#include "storage/direct_load/ob_direct_load_sstable_index_block.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadSSTableIndexBlockWriter
  : public ObDirectLoadDataBlockWriter<ObDirectLoadSSTableIndexBlock::Header,
                                       ObDirectLoadSSTableIndexBlock::Entry>
{
public:
  ObDirectLoadSSTableIndexBlockWriter();
  virtual ~ObDirectLoadSSTableIndexBlockWriter();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type);
  int append_entry(const ObDirectLoadSSTableIndexEntry &entry);
private:
  int pre_write_item() override;
  int pre_flush_buffer() override;
private:
  int64_t start_offset_;
  int32_t entry_count_;
  int32_t last_entry_pos_;
  int32_t cur_entry_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadSSTableIndexBlockWriter);
};

} // namespace storage
} // namespace oceanbase
