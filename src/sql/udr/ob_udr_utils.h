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


#ifndef OB_SQL_UDR_OB_UDR_UTILS_H_
#define OB_SQL_UDR_OB_UDR_UTILS_H_

#include "sql/udr/ob_udr_struct.h"

namespace oceanbase
{
namespace sql
{
class ObSqlCtx;
class ObResultSet;
class ObPlanCacheCtx;

class ObUDRUtils
{
public:
  static int match_udr_and_refill_ctx(const ObString &pattern,
                                      ObSqlCtx &sql_ctx,
                                      ObResultSet &result,
                                      ObPlanCacheCtx &pc_ctx,
                                      bool &is_match_udr,
                                      ObUDRItemMgr::UDRItemRefGuard &item_guard);
  static int match_udr_item(const ObString &pattern,
                            const ObSQLSessionInfo &session_info,
                            ObExecContext &ectx,
                            ObIAllocator &allocator,
                            ObUDRItemMgr::UDRItemRefGuard &guard,
                            PatternConstConsList *cst_cons_list = nullptr);
  static int cons_udr_param_store(const DynamicParamInfoArray &dynamic_param_list,
                                  ObPlanCacheCtx &pc_ctx,
                                  ParamStore &param_store);

private:
  static int cons_udr_const_cons_list(const PatternConstConsList &cst_const_list,
                                      ObPlanCacheCtx &pc_ctx);
  static int refill_udr_exec_ctx(const ObUDRItemMgr::UDRItemRefGuard &item_guard,
                                 ObSqlCtx &context,
                                 ObResultSet &result,
                                 ObPlanCacheCtx &pc_ctx);
  static int clac_dynamic_param_store(const DynamicParamInfoArray& dynamic_param_list,
                                      ObPlanCacheCtx &pc_ctx,
                                      ParamStore &param_store);
  static int add_param_to_param_store(const ObObjParam &param,
                                      ParamStore &param_store);
};

} // namespace sql end
} // namespace oceanbase end

#endif