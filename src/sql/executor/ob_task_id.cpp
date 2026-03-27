/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/executor/ob_task_id.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER(ObTaskID, ob_job_id_, task_id_, task_cnt_);

}/* ns */
}/* ns oceanbase */
