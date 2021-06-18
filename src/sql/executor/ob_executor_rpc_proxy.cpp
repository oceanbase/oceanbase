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

#include "sql/executor/ob_executor_rpc_proxy.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {
int64_t ObBKGDDistExecuteArg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(task_id), K_(scheduler_id), K_(return_addr), K(serialized_task_.length()));
  J_OBJ_END();
  return pos;
}
int64_t ObBKGDTaskCompleteArg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(task_id), K_(scheduler_id), K_(return_code), K_(event));
  J_OBJ_END();
  return pos;
}
int64_t ObFetchIntermResultItemArg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(slice_id), K_(index));
  J_OBJ_END();
  return pos;
}
int64_t ObFetchIntermResultItemRes::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(result_item), K_(total_item_cnt));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObBKGDDistExecuteArg, return_addr_, tenant_id_, task_id_, scheduler_id_, serialized_task_);

OB_SERIALIZE_MEMBER(ObBKGDTaskCompleteArg, task_id_, scheduler_id_, return_code_, event_);

OB_SERIALIZE_MEMBER(ObFetchIntermResultItemArg, slice_id_, index_);

OB_SERIALIZE_MEMBER(ObFetchIntermResultItemRes, result_item_, total_item_cnt_);

}  // namespace sql
namespace obrpc {}  // namespace obrpc
}  // namespace oceanbase
