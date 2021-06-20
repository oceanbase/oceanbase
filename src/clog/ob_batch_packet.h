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

#ifndef OCEANBASE_CLOG_OB_BATCH_PACKET_
#define OCEANBASE_CLOG_OB_BATCH_PACKET_

#include "share/ob_define.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace common {
class ObAddr;
}  // namespace common
namespace clog {
// count | partition_id pcode len content | partition_id pcode len content | ...
class ObBatchPacketCodec {
public:
  static int64_t get_batch_header_size();
  static int64_t get_packet_header_size();
  static int encode_batch_header(char* buf, const int64_t len, int64_t& pos, const int64_t timestamp,
      const common::ObAddr& server, const int32_t count, const int64_t checksum);
  static int decode_batch_header(const char* buf, const int64_t len, int64_t& pos, int64_t* timestamp,
      common::ObAddr& server, int32_t* count, int64_t* checksum);
  static int encode_packet_header(char* buf, const int64_t limit, int64_t& pos,
      const common::ObPartitionKey& partition_id, const int32_t pcode, const int64_t req_id, const int32_t size);
  static int decode_packet(const char* buf, const int64_t limit, int64_t& pos, common::ObPartitionKey& partition_id,
      int32_t& pcode, int64_t& req_id, int32_t& size, const char*& pkt_content);
};      // end class ObBatchPacketCodec
};      // end namespace clog
};      // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_BATCH_PACKET_
