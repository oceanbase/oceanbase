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

#include "ob_clog_sync_msg.h"

namespace oceanbase {
namespace clog {
int64_t McCtx::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(partition_key_), K(mc_timestamp_), K(max_confirmed_log_id_), K(is_normal_partition_));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(McCtx, partition_key_, mc_timestamp_, max_confirmed_log_id_, is_normal_partition_);
}
namespace obrpc {
int64_t ObLogGetMCTsRequest::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(partition_key));
  J_OBJ_END();
  return pos;
}
int64_t ObLogGetMCTsResponse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(partition_key), K_(membership_timestamp), K_(max_confirmed_log_id), K_(is_normal_partition));
  J_OBJ_END();
  return pos;
}
int64_t ObLogGetPriorityArrayRequest::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(partition_array_));
  J_OBJ_END();
  return pos;
}
int64_t ObLogGetPriorityArrayResponse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(priority_array_));
  J_OBJ_END();
  return pos;
}
int64_t ObLogGetMcCtxArrayRequest::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(partition_array_));
  J_OBJ_END();
  return pos;
}
int64_t ObLogGetMcCtxArrayResponse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(mc_ctx_array_));
  J_OBJ_END();
  return pos;
}
int64_t ObLogGetRemoteLogRequest::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(partition_key_), K(log_id_));
  J_OBJ_END();
  return pos;
}
int64_t ObLogGetRemoteLogResponse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(partition_key_), K(log_id_), K(trans_id_), K(submit_timestamp_), K(ret_value_));
  J_OBJ_END();
  return pos;
}
// ObLogGetMCTs
OB_SERIALIZE_MEMBER(ObLogGetMCTsRequest, partition_key_);
OB_SERIALIZE_MEMBER(
    ObLogGetMCTsResponse, partition_key_, membership_timestamp_, max_confirmed_log_id_, is_normal_partition_);

OB_SERIALIZE_MEMBER(ObLogGetMcCtxArrayRequest, partition_array_);
OB_SERIALIZE_MEMBER(ObLogGetMcCtxArrayResponse, mc_ctx_array_);

OB_SERIALIZE_MEMBER(ObLogGetPriorityArrayRequest, partition_array_);
OB_SERIALIZE_MEMBER(ObLogGetPriorityArrayResponse, priority_array_);

OB_SERIALIZE_MEMBER(ObLogGetRemoteLogRequest, partition_key_, log_id_);
OB_SERIALIZE_MEMBER(ObLogGetRemoteLogResponse, partition_key_, log_id_, trans_id_, submit_timestamp_, ret_value_);
}  // namespace obrpc
}  // namespace oceanbase
