// Copyright 2015-2016 Alibaba Inc. All Rights Reserved.
// Author:
//     LuoFan 
// Normalizer:
//     LuoFan 


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