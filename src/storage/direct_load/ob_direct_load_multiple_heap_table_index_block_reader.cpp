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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block_reader.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadMultipleHeapTableIndexBlockReader::ObDirectLoadMultipleHeapTableIndexBlockReader()
{
}

ObDirectLoadMultipleHeapTableIndexBlockReader::~ObDirectLoadMultipleHeapTableIndexBlockReader()
{
}

int ObDirectLoadMultipleHeapTableIndexBlockReader::init(int64_t data_block_size,
                                                        ObCompressorType compressor_type)
{
  return ParentType::init(data_block_size, data_block_size, compressor_type);
}

int ObDirectLoadMultipleHeapTableIndexBlockReader::get_next_index(
  const ObDirectLoadMultipleHeapTableTabletIndex *&index)
{
  int ret = OB_SUCCESS;
  index = nullptr;
  const Entry *item = nullptr;
  if (OB_FAIL(this->get_next_item(item))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "fail to get next item", KR(ret));
    }
  } else {
    index_.tablet_id_ = item->tablet_id_;
    index_.row_count_ = item->row_count_;
    index_.fragment_idx_ = item->fragment_idx_;
    index_.offset_ = item->offset_;
    index = &index_;
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexBlockReader::get_last_index(
  const ObDirectLoadMultipleHeapTableTabletIndex *&index)
{
  int ret = OB_SUCCESS;
  index = nullptr;
  const Header &header = this->data_block_reader_.get_header();
  Entry item;
  if (OB_FAIL(this->data_block_reader_.read_item(header.last_entry_pos_, item))) {
    STORAGE_LOG(WARN, "fail to read item", KR(ret));
  } else {
    index_.tablet_id_ = item.tablet_id_;
    index_.row_count_ = item.row_count_;
    index_.fragment_idx_ = item.fragment_idx_;
    index_.offset_ = item.offset_;
    index = &index_;
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexBlockReader::get_index(
  int64_t idx, const ObDirectLoadMultipleHeapTableTabletIndex *&index)
{
  int ret = OB_SUCCESS;
  const Header &header = this->data_block_reader_.get_header();
  if (OB_UNLIKELY(idx >= header.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(header), K(idx));
  } else {
    const int64_t pos = ObDirectLoadMultipleHeapTableIndexBlock::get_header_size() +
                        ObDirectLoadMultipleHeapTableIndexBlock::get_entry_size() * idx;
    Entry item;
    if (OB_FAIL(this->data_block_reader_.read_item(pos, item))) {
      STORAGE_LOG(WARN, "fail to read item", KR(ret));
    } else {
      index_.tablet_id_ = item.tablet_id_;
      index_.row_count_ = item.row_count_;
      index_.fragment_idx_ = item.fragment_idx_;
      index_.offset_ = item.offset_;
      index = &index_;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableIndexBlockReader::seek_index(int64_t idx)
{
  int ret = OB_SUCCESS;
  const Header &header = this->data_block_reader_.get_header();
  if (OB_UNLIKELY(idx >= header.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(header), K(idx));
  } else {
    const int64_t pos = ObDirectLoadMultipleHeapTableIndexBlock::get_header_size() +
                        ObDirectLoadMultipleHeapTableIndexBlock::get_entry_size() * idx;
    if (OB_FAIL(this->data_block_reader_.set_pos(pos))) {
      STORAGE_LOG(WARN, "fail to set pos", KR(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
