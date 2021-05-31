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

OB_SERIALIZE_MEMBER(ObBKGDDistExecuteArg, return_addr_, tenant_id_, task_id_, scheduler_id_, serialized_task_);

OB_SERIALIZE_MEMBER(ObBKGDTaskCompleteArg, task_id_, scheduler_id_, return_code_, event_);

OB_SERIALIZE_MEMBER(ObFetchIntermResultItemArg, slice_id_, index_);

OB_SERIALIZE_MEMBER(ObFetchIntermResultItemRes, result_item_, total_item_cnt_);

}  // namespace sql
namespace obrpc {}  // namespace obrpc
}  // namespace oceanbase
