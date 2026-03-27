/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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


