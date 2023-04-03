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

#include "share/ob_define.h"
#include "lib/utility/serialization.h"
#include "sql/executor/ob_job_id.h"
#include "lib/json/ob_yson.h"
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
