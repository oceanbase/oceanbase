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

#define USING_LOG_PREFIX SQL_OPT

#include "sql/table_format/jdbc/ob_jdbc_filter_utils.h"

#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{
namespace jdbc
{
using namespace oceanbase::common;

bool ObJdbcFilterUtils::is_local_filter_type(const ObObjType type)
{
  return ob_is_string_or_lob_type(type)
         || ob_is_raw(type)
         || (lib::is_mysql_mode()
             && (ObTimeType == type || ObDateTimeType == type || ObTimestampType == type));
}

int ObJdbcFilterUtils::check_filter_requires_local(const ObRawExpr *filter,
                                                   const ParamStore &params,
                                                   bool &requires_local)
{
  int ret = OB_SUCCESS;
  requires_local = false;
  ObSEArray<const ObRawExpr *, 16> pending;
  OZ(pending.push_back(filter));
  while (OB_SUCC(ret) && !requires_local && !pending.empty()) {
    const ObRawExpr *expr = pending.at(pending.count() - 1);
    pending.pop_back();
    if (OB_NOT_NULL(expr)) {
      requires_local = is_local_filter_type(expr->get_result_type().get_type());
      if (!requires_local && expr->is_const_raw_expr()) {
        const ObConstRawExpr *constant = static_cast<const ObConstRawExpr *>(expr);
        const ObObj &value = constant->get_value();
        if (T_QUESTIONMARK == expr->get_expr_type() && value.is_unknown()) {
          const int64_t index = value.get_unknown();
          if (index < 0 || index >= params.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid jdbc filter parameter index", K(ret), K(index), K(params.count()));
          } else {
            requires_local = is_local_filter_type(params.at(index).get_type());
          }
        } else {
          requires_local = is_local_filter_type(value.get_type());
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && !requires_local && i < expr->get_param_count(); ++i) {
        OZ(pending.push_back(expr->get_param_expr(i)));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("jdbc filter expression is null", K(ret));
    }
  }
  return ret;
}

} // namespace jdbc
} // namespace sql
} // namespace oceanbase
