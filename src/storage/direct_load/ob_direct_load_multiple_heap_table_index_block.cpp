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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

/**
 * Header
 */

ObDirectLoadMultipleHeapTableIndexBlock::Header::Header()
  : count_(0), last_entry_pos_(0)
{
}

ObDirectLoadMultipleHeapTableIndexBlock::Header::~Header()
{
}

void ObDirectLoadMultipleHeapTableIndexBlock::Header::reset()
{
  ObDirectLoadDataBlock::Header::reset();
  count_ = 0;
  last_entry_pos_ = 0;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadMultipleHeapTableIndexBlock::Header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDirectLoadDataBlock::Header::serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to encode header", KR(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, count_))) {
    LOG_WARN("fail to encode i32", KR(ret), K(buf_len), K(pos), K(count_));
  } else if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, last_entry_pos_))) {
    LOG_WARN("fail to encode i32", KR(ret), K(buf_len), K(pos), K(last_entry_pos_));
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadMultipleHeapTableIndexBlock::Header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDirectLoadDataBlock::Header::deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to decode header", KR(ret), K(data_len), K(pos));
  } else if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &count_))) {
    LOG_WARN("fail to decode i32", KR(ret), K(data_len), K(pos), K(count_));
  } else if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &last_entry_pos_))) {
    LOG_WARN("fail to decode i32", KR(ret), K(data_len), K(pos), K(last_entry_pos_));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadMultipleHeapTableIndexBlock::Header)
{
  int64_t len = 0;
  len += ObDirectLoadDataBlock::Header::get_serialize_size();
  len += NS_::encoded_length_i32(count_);
  len += NS_::encoded_length_i32(last_entry_pos_);
  return len;
}

/*
 * Entry
 */

ObDirectLoadMultipleHeapTableIndexBlock::Entry::Entry()
  : tablet_id_(0), row_count_(0), offset_val_(0)
{
}

ObDirectLoadMultipleHeapTableIndexBlock::Entry::~Entry()
{
}

void ObDirectLoadMultipleHeapTableIndexBlock::Entry::reset()
{
  tablet_id_ = 0;
  row_count_ = 0;
  offset_val_ = 0;
}

void ObDirectLoadMultipleHeapTableIndexBlock::Entry::reuse()
{
  reset();
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadMultipleHeapTableIndexBlock::Entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(NS_::encode_i64(buf, buf_len, pos, static_cast<int64_t>(tablet_id_)))) {
    LOG_WARN("fail to encode i64", KR(ret), K(buf_len), K(pos), K(tablet_id_));
  } else if (OB_FAIL(NS_::encode_i64(buf, buf_len, pos, row_count_))) {
    LOG_WARN("fail to encode i64", KR(ret), K(buf_len), K(pos), K(row_count_));
  } else if (OB_FAIL(NS_::encode_i64(buf, buf_len, pos, offset_val_))) {
    LOG_WARN("fail to encode i64", KR(ret), K(buf_len), K(pos), K(offset_val_));
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadMultipleHeapTableIndexBlock::Entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(NS_::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&tablet_id_)))) {
    LOG_WARN("fail to decode i64", KR(ret), K(data_len), K(pos), K(tablet_id_));
  } else if (OB_FAIL(NS_::decode_i64(buf, data_len, pos, &row_count_))) {
    LOG_WARN("fail to decode i64", KR(ret), K(data_len), K(pos), K(row_count_));
  } else if (OB_FAIL(NS_::decode_i64(buf, data_len, pos, &offset_val_))) {
    LOG_WARN("fail to decode i64", KR(ret), K(data_len), K(pos), K(offset_val_));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadMultipleHeapTableIndexBlock::Entry)
{
  int64_t len = 0;
  len += NS_::encoded_length_i64(static_cast<int64_t>(tablet_id_));
  len += NS_::encoded_length_i64(row_count_);
  len += NS_::encoded_length_i64(offset_val_);
  return len;
}

/**
 * ObDirectLoadMultipleHeapTableIndexBlock
 */

int64_t ObDirectLoadMultipleHeapTableIndexBlock::get_header_size()
{
  static int64_t size = Header().get_serialize_size();
  return size;
}

int64_t ObDirectLoadMultipleHeapTableIndexBlock::get_entry_size()
{
  static int64_t size = Entry().get_serialize_size();
  return size;
}

int64_t ObDirectLoadMultipleHeapTableIndexBlock::get_entries_per_block(int64_t block_size)
{
  return (block_size - get_header_size()) / get_entry_size();
}

/**
 * ObDirectLoadMultipleHeapTableTabletIndex
 */

ObDirectLoadMultipleHeapTableTabletIndex::ObDirectLoadMultipleHeapTableTabletIndex()
  : row_count_(0), fragment_idx_(0), offset_(0)
{
}

ObDirectLoadMultipleHeapTableTabletIndex::~ObDirectLoadMultipleHeapTableTabletIndex()
{
}

} // namespace storage
} // namespace oceanbase
