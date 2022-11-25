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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_TIME_
#define OCEANBASE_RPC_OBRPC_OB_RPC_TIME_

namespace oceanbase
{
namespace obrpc
{
struct ObRpcCostTime
{
public:
  static const uint8_t RPC_COST_TIME_SIZE = 40;
public:
  ObRpcCostTime() { memset(this, 0, sizeof(*this)); }
  ~ObRpcCostTime() {}
  static inline int64_t get_encoded_size() { return RPC_COST_TIME_SIZE; }

  int32_t len_;
  int32_t arrival_push_diff_;
  int32_t push_pop_diff_;
  int32_t pop_process_start_diff_;
  int32_t process_start_end_diff_;
  int32_t process_end_response_diff_;
  uint64_t packet_id_;
  int64_t request_arrival_time_;

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(K_(len), K_(arrival_push_diff),
      K_(push_pop_diff), K_(pop_process_start_diff), K_(process_start_end_diff),
      K_(process_end_response_diff), K_(packet_id), K_(request_arrival_time));
};

} // end of namespace rpc
} // end of namespace oceanbase
#endif
