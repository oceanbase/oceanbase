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

#include "storage/direct_load/ob_direct_load_sstable_index_block_reader.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadSSTableIndexBlockReader::ObDirectLoadSSTableIndexBlockReader()
  : last_offset_(0)
{
}

ObDirectLoadSSTableIndexBlockReader::~ObDirectLoadSSTableIndexBlockReader()
{
}

int ObDirectLoadSSTableIndexBlockReader::init(int64_t data_block_size,
                                              ObCompressorType compressor_type)
{
  return ParentType::init(data_block_size, data_block_size, compressor_type);
}

int ObDirectLoadSSTableIndexBlockReader::get_next_entry(const ObDirectLoadSSTableIndexEntry *&entry)
{
  int ret = OB_SUCCESS;
  const ObDirectLoadSSTableIndexBlock::Entry *item = nullptr;
  if (OB_FAIL(this->get_next_item(item))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "fail to get next item", KR(ret));
    }
  } else {
    entry_.offset_ = last_offset_;
    entry_.size_ = item->offset_ - last_offset_;
    last_offset_ = item->offset_;
  }
  return ret;
}

int ObDirectLoadSSTableIndexBlockReader::get_last_entry(const ObDirectLoadSSTableIndexEntry *&entry)
{
  int ret = OB_SUCCESS;
  const ObDirectLoadSSTableIndexBlock::Header &header = this->data_block_reader_.get_header();
  if (header.count_ == 1) {
    entry_.offset_ = header.offset_;
  } else {
    // get penultimate entry
    ObDirectLoadSSTableIndexBlock::Entry item;
    const int64_t pos = header.last_entry_pos_ - ObDirectLoadSSTableIndexBlock::get_entry_size();
    if (OB_FAIL(this->data_block_reader_.read_item(pos, item))) {
      STORAGE_LOG(WARN, "fail to read item", KR(ret));
    } else {
      entry_.offset_ = item.offset_;
    }
  }
  if (OB_SUCC(ret)) {
    // get last entry
    ObDirectLoadSSTableIndexBlock::Entry item;
    if (OB_FAIL(this->data_block_reader_.read_item(header.last_entry_pos_, item))) {
      STORAGE_LOG(WARN, "fail to read item", KR(ret));
    } else {
      entry_.size_ = item.offset_ - entry_.offset_;
      entry = &entry_;
    }
  }
  return ret;
}

int ObDirectLoadSSTableIndexBlockReader::get_entry(int64_t idx,
                                                   const ObDirectLoadSSTableIndexEntry *&entry)
{
  int ret = OB_SUCCESS;
  const ObDirectLoadSSTableIndexBlock::Header &header = this->data_block_reader_.get_header();
  if (OB_UNLIKELY(idx >= header.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(header), K(idx));
  } else {
    const int64_t pos = ObDirectLoadSSTableIndexBlock::get_header_size() +
                        ObDirectLoadSSTableIndexBlock::get_entry_size() * idx;
    if (0 == idx) {
      entry_.offset_ = header.offset_;
    } else {
      // get prev entry
      ObDirectLoadSSTableIndexBlock::Entry item;
      const int64_t prev_pos = pos - ObDirectLoadSSTableIndexBlock::get_entry_size();
      if (OB_FAIL(this->data_block_reader_.read_item(prev_pos, item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret));
      } else {
        entry_.offset_ = item.offset_;
      }
    }
    if (OB_SUCC(ret)) {
      // get current entry
      ObDirectLoadSSTableIndexBlock::Entry item;
      if (OB_FAIL(this->data_block_reader_.read_item(pos, item))) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret));
      } else {
        entry_.size_ = item.offset_ - entry_.offset_;
        entry = &entry_;
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableIndexBlockReader::prepare_read_block()
{
  const ObDirectLoadSSTableIndexBlock::Header &header = this->data_block_reader_.get_header();
  last_offset_ = header.offset_;
  return OB_SUCCESS;
}

} // namespace storage
} // namespace oceanbase
