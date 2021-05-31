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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_temp_table_transformation_op.h"
#include "sql/engine/ob_operator_reg.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {
#define USE_MULTI_GET_ARRAY_BINDING 1

DEF_TO_STRING(ObTempTableTransformationOpSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER_INHERIT(ObTempTableTransformationOpSpec, ObOpSpec);

int ObTempTableTransformationOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("failed to rescan the operator.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableTransformationOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (OB_FAIL(children_[0]->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row.", K(ret));
    } else {
      LOG_DEBUG("all rows are fetched");
      ret = OB_SUCCESS;
    }
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableTransformationOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_ISNULL(children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (OB_FAIL(children_[1]->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row.", K(ret));
    } else { /*do nothing.*/
    }
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableTransformationOp::inner_close()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObTempTableTransformationOp::destroy()
{
  ObOperator::destroy();
}

}  // end namespace sql
}  // end namespace oceanbase
