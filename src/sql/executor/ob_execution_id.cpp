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

#include "sql/executor/ob_execution_id.h"
#include "lib/json/ob_yson.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

const common::ObAddr &ObExecutionID::global_id_addr()
{
  static ObAddr global_addr(1, 1); // 1.0.0.0:1
  return global_addr;
}

DEFINE_TO_YSON_KV(ObExecutionID, OB_ID(addr), server_,
                                 OB_ID(execution_id), execution_id_);

OB_SERIALIZE_MEMBER(ObExecutionID, server_, execution_id_, execution_flag_);
int ObExecutionID::compare(const ObExecutionID &other) const
{
  int cmp_ret = 0;
  if (execution_id_ > other.execution_id_) {
    cmp_ret = 1;
  } else if (execution_id_ < other.execution_id_) {
    cmp_ret = -1;
  }
  return cmp_ret;
}

}/* ns sql*/
}/* ns oceanbase */


