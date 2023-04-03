// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_data_block_reader.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleHeapTableIndexBlockReader
  : public ObDirectLoadDataBlockReader<ObDirectLoadMultipleHeapTableIndexBlock::Header,
                                       ObDirectLoadMultipleHeapTableIndexBlock::Entry>
{
  typedef ObDirectLoadMultipleHeapTableIndexBlock::Header Header;
  typedef ObDirectLoadMultipleHeapTableIndexBlock::Entry Entry;
  typedef ObDirectLoadDataBlockReader<Header, Entry> ParentType;
public:
  ObDirectLoadMultipleHeapTableIndexBlockReader();
  virtual ~ObDirectLoadMultipleHeapTableIndexBlockReader();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type);
  int get_next_index(const ObDirectLoadMultipleHeapTableTabletIndex *&index);
  int get_last_index(const ObDirectLoadMultipleHeapTableTabletIndex *&entry);
  int get_index(int64_t idx, const ObDirectLoadMultipleHeapTableTabletIndex *&entry);
  int seek_index(int64_t idx);
  const Header &get_header() const { return this->data_block_reader_.get_header(); }
private:
  ObDirectLoadMultipleHeapTableTabletIndex index_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadMultipleHeapTableIndexBlockReader);
};

} // namespace storage
} // namespace oceanbase
