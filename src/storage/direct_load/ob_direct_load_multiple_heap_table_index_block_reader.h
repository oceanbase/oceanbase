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
#pragma once

#include "storage/direct_load/ob_direct_load_data_block_reader.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleHeapTableIndexBlockReader
  : public ObDirectLoadDataBlockReader<ObDirectLoadMultipleHeapTableIndexBlock::Header,
                                       ObDirectLoadMultipleHeapTableIndexBlock::Entry,
                                       true/*align*/>
{
  typedef ObDirectLoadMultipleHeapTableIndexBlock::Header Header;
  typedef ObDirectLoadMultipleHeapTableIndexBlock::Entry Entry;
  typedef ObDirectLoadDataBlockReader<Header, Entry, true> ParentType;
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
