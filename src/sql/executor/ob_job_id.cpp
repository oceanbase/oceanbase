/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ob_define.h"
#include "sql/executor/ob_job_id.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER(ObJobID, ob_execution_id_, job_id_, root_op_id_);
DEFINE_TO_YSON_KV(ObJobID, OB_ID(execution_id), ob_execution_id_,
                           OB_ID(job_id), job_id_);

}/* ns */
}/* ns oceanbase */
