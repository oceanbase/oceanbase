// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_data_block_reader.h"
#include "storage/direct_load/ob_direct_load_sstable_index_block.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadSSTableIndexBlockReader
  : public ObDirectLoadDataBlockReader<ObDirectLoadSSTableIndexBlock::Header,
                                       ObDirectLoadSSTableIndexBlock::Entry>
{
  typedef ObDirectLoadDataBlockReader<ObDirectLoadSSTableIndexBlock::Header,
                                      ObDirectLoadSSTableIndexBlock::Entry>
    ParentType;
public:
  ObDirectLoadSSTableIndexBlockReader();
  virtual ~ObDirectLoadSSTableIndexBlockReader();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type);
  int get_next_entry(const ObDirectLoadSSTableIndexEntry *&entry);
  int get_last_entry(const ObDirectLoadSSTableIndexEntry *&entry);
  int get_entry(int64_t idx, const ObDirectLoadSSTableIndexEntry *&entry);
  const ObDirectLoadSSTableIndexBlock::Header &get_header() const
  {
    return this->data_block_reader_.get_header();
  }
protected:
  int prepare_read_block() override;
private:
  int64_t last_offset_;
  ObDirectLoadSSTableIndexEntry entry_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadSSTableIndexBlockReader);
};

} // namespace storage
} // namespace oceanbase
