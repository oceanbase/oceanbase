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

#include "storage/direct_load/ob_direct_load_data_block.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadDataBlock::Header::Header()
  : occupy_size_(0), data_size_(0), checksum_(0)
{
}

ObDirectLoadDataBlock::Header::~Header()
{
}

void ObDirectLoadDataBlock::Header::reset()
{
  occupy_size_ = 0;
  data_size_ = 0;
  checksum_ = 0;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadDataBlock::Header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, occupy_size_))) {
    LOG_WARN("fail to encode i32", KR(ret), K(buf_len), K(pos), K(occupy_size_));
  } else if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, data_size_))) {
    LOG_WARN("fail to encode i32", KR(ret), K(buf_len), K(pos), K(data_size_));
  } else if (OB_FAIL(NS_::encode_i64(buf, buf_len, pos, checksum_))) {
    LOG_WARN("fail to encode i64", KR(ret), K(buf_len), K(pos), K(checksum_));
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadDataBlock::Header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &occupy_size_))) {
    LOG_WARN("fail to decode i32", KR(ret), K(data_len), K(pos), K(occupy_size_));
  } else if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &data_size_))) {
    LOG_WARN("fail to decode i32", KR(ret), K(data_len), K(pos), K(data_size_));
  } else if (OB_FAIL(NS_::decode_i64(buf, data_len, pos, &checksum_))) {
    LOG_WARN("fail to decode i64", KR(ret), K(data_len), K(pos), K(checksum_));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadDataBlock::Header)
{
  int64_t len = 0;
  len += NS_::encoded_length_i32(occupy_size_);
  len += NS_::encoded_length_i32(data_size_);
  len += NS_::encoded_length_i64(checksum_);
  return len;
}

int64_t ObDirectLoadDataBlock::get_header_size()
{
  Header header;
  return header.get_serialize_size();
}

} // namespace storage
} // namespace oceanbase
