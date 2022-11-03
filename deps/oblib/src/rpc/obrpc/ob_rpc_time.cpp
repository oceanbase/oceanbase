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

#define USING_LOG_PREFIX RPC_OBRPC
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "ob_rpc_time.h"

namespace oceanbase
{
namespace obrpc
{

using namespace common;
using namespace common::serialization;

DEFINE_SERIALIZE(ObRpcCostTime)
{
  int ret = OB_SUCCESS;

  if (buf_len - pos >= get_encoded_size()) {
    if (OB_FAIL(encode_i32(buf, buf_len, pos, static_cast<int32_t> (get_encoded_size())))) {
      LOG_WARN("Encode error", K(ret));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, arrival_push_diff_))) {
      LOG_WARN("Encode error", K(ret));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, push_pop_diff_))) {
      LOG_WARN("Encode error", K(ret));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, pop_process_start_diff_))) {
      LOG_WARN("Encode error", K(ret));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, process_start_end_diff_))) {
      LOG_WARN("Encode error", K(ret));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, process_end_response_diff_))) {
      LOG_WARN("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, packet_id_))) {
      LOG_WARN("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, request_arrival_time_))) {
      LOG_WARN("Encode error", K(ret));
    } else {
      // do nothing
    }
  } else {
    ret = OB_BUF_NOT_ENOUGH;
  }

  return ret;
}

DEFINE_DESERIALIZE(ObRpcCostTime)
{
  int ret = OB_SUCCESS;
  
  if (data_len - pos >= get_encoded_size()) {
    if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&len_)))) {
      LOG_WARN("Decode error", K(ret));
    } else if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&arrival_push_diff_)))) {
      LOG_WARN("Decode error", K(ret));
    } else if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&push_pop_diff_)))) {
      LOG_WARN("Decode error", K(ret));
    } else if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&pop_process_start_diff_)))) {
      LOG_WARN("Decode error", K(ret));
    } else if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&process_start_end_diff_)))) {
      LOG_WARN("Decode error", K(ret));
    } else if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&process_end_response_diff_)))) {
      LOG_WARN("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&packet_id_)))) {
      LOG_WARN("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&request_arrival_time_)))) {
      LOG_WARN("Decode error", K(ret));
    } else {
      // do nothing
    }
  } else {
    ret = OB_INVALID_DATA;
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRpcCostTime)
{
  return get_encoded_size();
}

} // end of namespace rpc
} // end of namespace oceanbase
