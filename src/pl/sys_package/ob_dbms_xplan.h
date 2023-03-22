// (C) Copyright 2020 Alibaba Inc. All Rights Reserved.
//  Authors:
//    zhenling.zzg <>
//  Normalizer:
//
//
#ifndef OB_DBMS_XPLAN_H
#define OB_DBMS_XPLAN_H

#include "share/stat/ob_stat_define.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_type.h"
#include "lib/string/ob_sql_string.h"
#include "share/stat/ob_dbms_stats_preferences.h"
#include "sql/monitor/ob_sql_plan.h"
#include "sql/monitor/ob_plan_info_manager.h"

namespace oceanbase
{
namespace sql {
  struct ObExplainDisplayOpt;
}
using namespace sql;
using namespace common;
namespace pl
{

class ObDbmsXplan
{
public:
  static int enable_opt_trace(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);

  static int disable_opt_trace(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);

  static int set_opt_trace_parameter(sql::ObExecContext &ctx,
                                    sql::ParamStore &params,
                                    common::ObObj &result);

  static int display(sql::ObExecContext &ctx,
                    sql::ParamStore &params,
                    common::ObObj &result);

  static int display_cursor(sql::ObExecContext &ctx,
                            sql::ParamStore &params,
                            common::ObObj &result);

  static int display_sql_plan_baseline(sql::ObExecContext &ctx,
                                      sql::ParamStore &params,
                                      common::ObObj &result);

private:
  static int set_display_result(sql::ObExecContext &ctx,
                                PlanText &plan_text,
                                common::ObObj &result);

  static int set_display_result_for_oracle(sql::ObExecContext &ctx,
                                           PlanText &plan_text,
                                           common::ObObj &result);

  static int set_display_result_for_mysql(sql::ObExecContext &ctx,
                                          PlanText &plan_text,
                                          common::ObObj &result);

  static int get_plan_format(const ObString &format,
                             ExplainType &type,
                             ObExplainDisplayOpt& option);

  static int get_plan_real_info_by_id(sql::ObExecContext &ctx,
                                      const ObString& sql_id,
                                      uint64_t plan_id,
                                      ObIArray<ObPlanRealInfo> &plan_infos);

  static int get_plan_real_info_by_hash(sql::ObExecContext &ctx,
                                        const ObString& sql_id,
                                        uint64_t plan_hash,
                                        ObIArray<ObPlanRealInfo> &plan_infos);

  static int inner_get_plan_real_info(sql::ObExecContext &ctx,
                                      const ObSqlString& sql,
                                      ObIArray<ObPlanRealInfo> &plan_infos);

  static int read_plan_real_info_from_result(sqlclient::ObMySQLResult& mysql_result,
                                             ObPlanRealInfo &plan_info);

};

}
}

#endif // OB_DBMS_XPLAN_H
