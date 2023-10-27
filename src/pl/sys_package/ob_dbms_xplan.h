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

#ifndef OB_DBMS_XPLAN_H
#define OB_DBMS_XPLAN_H

#include "share/stat/ob_dbms_stats_preferences.h"
#include "sql/monitor/ob_plan_info_manager.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_define.h"
#include "lib/string/ob_sql_string.h"
#include "sql/monitor/ob_sql_plan.h"
#include "pl/ob_pl_type.h"

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

  static int display_active_session_plan(sql::ObExecContext &ctx,
                                        sql::ParamStore &params,
                                        common::ObObj &result);

private:

  static int get_server_ip_port(sql::ObExecContext &ctx,
                                ObString &svr_ip,
                                int64_t &svr_port);

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

  static int get_plan_info_by_plan_table(sql::ObExecContext &ctx,
                                         ObString table_name,
                                         ObString statement_id,
                                         ObString filter_preds,
                                         ObIArray<ObSqlPlanItem*> &plan_infos);

  static int get_plan_info_by_id(sql::ObExecContext &ctx,
                                  int64_t tenant_id,
                                  const ObString &svr_ip,
                                  int64_t svr_port,
                                  uint64_t plan_id,
                                  ObIArray<ObSqlPlanItem*> &plan_infos);

  static int get_baseline_plan_info(sql::ObExecContext &ctx,
                                    int64_t tenant_id,
                                    const ObString &svr_ip,
                                    int64_t svr_port,
                                    const ObString &sql_handle,
                                    uint64_t plan_hash,
                                    ObIArray<ObSqlPlanItem*> &plan_infos);

  static int get_baseline_plan_detail(sql::ObExecContext &ctx,
                                      const ObString& sql_handle,
                                      const ObString& plan_name,
                                      int64_t tenant_id,
                                      PlanText &plan_text,
                                      bool from_plan_cache);

  static int inner_get_baseline_plan_detail(sql::ObExecContext &ctx,
                                            const ObSqlString& sql,
                                            PlanText &plan_text,
                                            bool from_plan_cache);

  static int format_baseline_plan_detail(sql::ObExecContext &ctx,
                                        sqlclient::ObMySQLResult& mysql_result,
                                        PlanText &plan_text,
                                        bool from_plan_cache);

  static int get_plan_info_by_session_id(sql::ObExecContext &ctx,
                                        int64_t session_id,
                                        const ObString &svr_ip,
                                        int64_t svr_port,
                                        int64_t tenant_id,
                                        ObIArray<ObSqlPlanItem*> &plan_infos);

  static int inner_get_plan_info(sql::ObExecContext &ctx,
                                const ObSqlString& sql,
                                ObIArray<ObSqlPlanItem*> &plan_infos);

  static int inner_get_plan_info_use_current_session(sql::ObExecContext &ctx,
                                                    const ObSqlString& sql,
                                                    ObIArray<ObSqlPlanItem*> &plan_infos);

  static int read_plan_info_from_result(sql::ObExecContext &ctx,
                                        sqlclient::ObMySQLResult& mysql_result,
                                        ObSqlPlanItem &plan_info);

  enum WAIT_COLUMN
  {
    OPERATOR = 0,
    OPTIONS,
    OBJECT_NODE,
    OBJECT_ID,
    OBJECT_OWNER,
    OBJECT_NAME,
    OBJECT_ALIAS,
    OBJECT_TYPE,
    OPTIMIZER,
    ID,
    PARENT_ID,
    DEPTH,
    POSITION,
    SEARCH_COLUMNS,
    IS_LAST_CHILD,
    COST,
    REAL_COST,
    CARDINALITY,
    REAL_CARDINALITY,
    BYTES,
    ROWSET,
    OTHER_TAG,
    PARTITION_START,
    PARTITION_STOP,
    PARTITION_ID,
    OTHER,
    DISTRIBUTION,
    CPU_COST,
    IO_COST,
    TEMP_SPACE,
    ACCESS_PREDICATES,
    FILTER_PREDICATES,
    STARTUP_PREDICATES,
    PROJECTION,
    SPECIAL_PREDICATES,
    TIME,
    QBLOCK_NAME,
    REMARKS,
    OTHER_XML
  };
};

}
}

#endif // OB_DBMS_XPLAN_H
