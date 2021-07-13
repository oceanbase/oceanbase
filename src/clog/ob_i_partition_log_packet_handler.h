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

#ifndef OCEANBASE_CLOG_OB_I_PARTITION_LOG_PACKET_HANDLER_
#define OCEANBASE_CLOG_OB_I_PARTITION_LOG_PACKET_HANDLER_

namespace oceanbase {
namespace clog {
struct ObLogReqContext;
class ObIPartitionLogPacketHandler {
public:
  ObIPartitionLogPacketHandler()
  {}
  virtual ~ObIPartitionLogPacketHandler()
  {}
  virtual int handle_request(ObLogReqContext& req) = 0;
};

}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_I_PARTITION_LOG_PACKET_HANDLER_
