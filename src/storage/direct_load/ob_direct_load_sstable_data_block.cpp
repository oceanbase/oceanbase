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

#include "storage/direct_load/ob_direct_load_sstable_data_block.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

/**
 * ObDirectLoadSSTableDataBlock
 */

ObDirectLoadSSTableDataBlock::Header::Header()
  : last_row_pos_(0), reserved_(0)
{
}

ObDirectLoadSSTableDataBlock::Header::~Header()
{
}

void ObDirectLoadSSTableDataBlock::Header::reset()
{
  ObDirectLoadDataBlock::Header::reset();
  last_row_pos_ = 0;
  reserved_ = 0;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadSSTableDataBlock::Header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDirectLoadDataBlock::Header::serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to encode header", KR(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, last_row_pos_))) {
    LOG_WARN("fail to encode i32", KR(ret), K(buf_len), K(pos), K(last_row_pos_));
  } else if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, reserved_))) {
    LOG_WARN("fail to encode i32", KR(ret), K(buf_len), K(pos), K(reserved_));
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadSSTableDataBlock::Header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDirectLoadDataBlock::Header::deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to decode header", KR(ret), K(data_len), K(pos));
  } else if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &last_row_pos_))) {
    LOG_WARN("fail to decode i32", KR(ret), K(data_len), K(pos), K(last_row_pos_));
  } else if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &reserved_))) {
    LOG_WARN("fail to decode i32", KR(ret), K(data_len), K(pos), K(reserved_));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadSSTableDataBlock::Header)
{
  int64_t len = 0;
  len += ObDirectLoadDataBlock::Header::get_serialize_size();
  len += NS_::encoded_length_i32(last_row_pos_);
  len += NS_::encoded_length_i32(reserved_);
  return len;
}

/**
 * ObDirectLoadSSTableDataBlock
 */

int64_t ObDirectLoadSSTableDataBlock::get_header_size()
{
  static int64_t size = Header().get_serialize_size();
  return size;
}

/**
 * ObDirectLoadSSTableDataBlockDesc
 */

ObDirectLoadSSTableDataBlockDesc::ObDirectLoadSSTableDataBlockDesc()
  : fragment_idx_(0),
    offset_(0),
    size_(0),
    block_count_(0),
    is_left_border_(false),
    is_right_border_(false)
{
}

ObDirectLoadSSTableDataBlockDesc::~ObDirectLoadSSTableDataBlockDesc()
{
}

void ObDirectLoadSSTableDataBlockDesc::reset()
{
  fragment_idx_ = 0;
  offset_ = 0;
  size_ = 0;
  block_count_ = 0;
  is_left_border_ = false;
  is_right_border_ = false;
}

} // namespace storage
} // namespace oceanbase
