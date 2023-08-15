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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block_writer.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadMultipleHeapTableIndexBlockWriter::ObDirectLoadMultipleHeapTableIndexBlockWriter()
  : entry_count_(0), last_entry_pos_(-1), cur_entry_pos_(-1)
{
}

ObDirectLoadMultipleHeapTableIndexBlockWriter::~ObDirectLoadMultipleHeapTableIndexBlockWriter()
{
}

int ObDirectLoadMultipleHeapTableIndexBlockWriter::init(int64_t data_block_size,
                                                        ObCompressorType compressor_type)
{
  return ObDirectLoadDataBlockWriter::init(data_block_size, compressor_type, nullptr, 0, nullptr);
}

int ObDirectLoadMultipleHeapTableIndexBlockWriter::append_index(
  const ObDirectLoadMultipleHeapTableTabletIndex &tablet_index)
{
  int ret = OB_SUCCESS;
  ObDirectLoadMultipleHeapTableIndexBlock::Entry item;
  item.tablet_id_ = tablet_index.tablet_id_.id();
  item.row_count_ = tablet_index.row_count_;
  item.fragment_idx_ = tablet_index.fragment_idx_;
  item.offset_ = tablet_index.offset_;
  if (OB_FAIL(this->write_item(item))) {
    STORAGE_LOG(WARN, "fail to write item", KR(ret));
  } else {
    ++entry_count_;
    last_entry_pos_ = cur_entry_pos_;
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexBlockWriter::pre_write_item()
{
  cur_entry_pos_ = this->data_block_writer_.get_pos();
  return OB_SUCCESS;
}

int ObDirectLoadMultipleHeapTableIndexBlockWriter::pre_flush_buffer()
{
  ObDirectLoadMultipleHeapTableIndexBlock::Header &header = this->data_block_writer_.get_header();
  header.count_ = entry_count_;
  header.last_entry_pos_ = (last_entry_pos_ != -1 ? last_entry_pos_ : cur_entry_pos_);
  entry_count_ = 0;
  last_entry_pos_ = -1;
  cur_entry_pos_ = -1;
  return OB_SUCCESS;
}

} // namespace storage
} // namespace oceanbase
