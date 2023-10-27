/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_link_dml.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase
{
namespace sql
{

ObLogLinkDml::ObLogLinkDml(ObLogPlan &plan)
  : ObLogLink(plan),
    dml_type_(stmt::StmtType::T_SELECT)
{}

int ObLogLinkDml::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  get_op_ordering().reset();
  is_local_order_ = false;
  return ret;
}

int ObLogLinkDml::get_explain_name_internal(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  switch (dml_type_) {
      case stmt::StmtType::T_INSERT:
        ret = BUF_PRINTF("%s", "LINK INSERT");
        break;
      case stmt::StmtType::T_UPDATE:
        ret = BUF_PRINTF("%s", "LINK UPDATE");
        break;
      case stmt::StmtType::T_DELETE:
        ret = BUF_PRINTF("%s", "LINK DELETE");
        break;
      case stmt::StmtType::T_MERGE:
        ret = BUF_PRINTF("%s", "LINK MERGE INTO");
        break;
      default:
        ret = BUF_PRINTF("%s", get_name());
  }
  return ret;
}

int ObLogLinkDml::get_plan_item_info(PlanText &plan_text,
                                     ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogLink::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(get_explain_name_internal(buf, buf_len, pos))) {
      LOG_WARN("failed to get explain name", K(ret));
    }
    END_BUF_PRINT(plan_item.operation_, plan_item.operation_len_);
  }
  return ret;
}


} // namespace sql
} // namespace oceanbase
