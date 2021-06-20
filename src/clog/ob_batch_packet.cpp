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

#include "ob_batch_packet.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
using namespace common;
namespace clog {
int64_t ObBatchPacketCodec::get_batch_header_size()
{
  return sizeof(int64_t) + sizeof(int32_t) + sizeof(int64_t) + sizeof(int64_t);
}

int64_t ObBatchPacketCodec::get_packet_header_size()
{
  common::ObPartitionKey key;
  return key.get_serialize_size() + sizeof(int32_t) + sizeof(int32_t) + sizeof(int64_t);
}

int ObBatchPacketCodec::encode_batch_header(char* buf, const int64_t len, int64_t& pos, const int64_t timestamp,
    const ObAddr& server, const int32_t count, const int64_t checksum)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || len <= 0 || pos < 0 || OB_INVALID_TIMESTAMP == timestamp || !server.is_valid() || count < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(buf), K(len), K(pos), K(timestamp), K(server), K(count));
  } else if (OB_FAIL(serialization::encode_i64(buf, len, new_pos, timestamp))) {
  } else if (OB_FAIL(serialization::encode_i32(buf, len, new_pos, count))) {
  } else if (OB_FAIL(server.serialize(buf, len, new_pos))) {
  } else if (OB_FAIL(serialization::encode_i64(buf, len, new_pos, checksum))) {
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObBatchPacketCodec::decode_batch_header(const char* buf, const int64_t len, int64_t& pos, int64_t* timestamp,
    ObAddr& server, int32_t* count, int64_t* checksum)
{
  int ret = OB_SUCCESS;
  int64_t server_id = 0;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_ISNULL(timestamp) || OB_ISNULL(count) || OB_ISNULL(checksum) || len < 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    *timestamp = OB_INVALID_TIMESTAMP;
    server.reset();
    *count = OB_INVALID_COUNT;
    if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, timestamp))) {
    } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, count))) {
    } else if (OB_FAIL(server.deserialize(buf, len, new_pos))) {
    } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, checksum))) {
    } else {
      pos = new_pos;
    }
  }

  return ret;
}

int ObBatchPacketCodec::encode_packet_header(char* buf, const int64_t limit, int64_t& pos,
    const common::ObPartitionKey& partition_id, const int32_t pcode, const int64_t req_id, const int32_t size)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || limit < 0 || pos < 0 || !partition_id.is_valid() || OB_INVALID_SIZE == size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partition_id.serialize(buf, limit, new_pos)) ||
             OB_FAIL(serialization::encode_i32(buf, limit, new_pos, pcode)) ||
             OB_FAIL(serialization::encode_i64(buf, limit, new_pos, req_id)) ||
             OB_FAIL(serialization::encode_i32(buf, limit, new_pos, size))) {
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObBatchPacketCodec::decode_packet(const char* buf, const int64_t limit, int64_t& pos,
    common::ObPartitionKey& partition_id, int32_t& pcode, int64_t& req_id, int32_t& size, const char*& pkt_content)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  partition_id.reset();
  req_id = OB_INVALID_ID;
  size = OB_INVALID_SIZE;
  pkt_content = NULL;
  if (OB_ISNULL(buf) || limit < 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partition_id.deserialize(buf, limit, new_pos)) ||
             OB_FAIL(serialization::decode_i32(buf, limit, new_pos, &pcode)) ||
             OB_FAIL(serialization::decode_i64(buf, limit, new_pos, &req_id)) ||
             OB_FAIL(serialization::decode_i32(buf, limit, new_pos, &size))) {
  } else if (new_pos + size > limit) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    pkt_content = buf + new_pos;
    pos = new_pos + size;
  }
  return ret;
}
};  // end namespace clog
};  // end namespace oceanbase
