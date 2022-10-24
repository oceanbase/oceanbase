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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_MONITOR_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_MONITOR_H_

#include "sql/engine/ob_exec_context.h"
#include "lib/number/ob_number_v2.h"

using namespace oceanbase::common::number;

namespace oceanbase
{
namespace pl
{
class ObDBMSMonitor
{
public:
  ObDBMSMonitor() {}
  virtual ~ObDBMSMonitor() {}
public:
  // DBMS_MONITOR.OB_SESSION_TRACE_ENABLE(session_id   IN  BINARY_INTEGER DEFAULT NULL, 
  //                                      level        IN  INT,
  //                                      sample_pct   IN  NUMBER,
  //                                      record_policy IN VARCHAR2);
  static int session_trace_enable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  //DBMS_MONITOR.OB_SESSION_TRACE_DISABLE(session_id   IN  BINARY_INTEGER);
  static int session_trace_disable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  // DBMS_MONITOR.OB_CLIENT_ID_TRACE_ENABLE(client_id    IN  VARCHAR2,
  //                                        level        IN  INT,
  //                                        sample_pct   IN  NUMBER,
  //                                        record_policy IN VARCHAR2);
  static int client_id_trace_enable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  // DBMS_MONITOR.OB_CLIENT_ID_TRACE_DISABLE(client_id IN  VARCHAR2);
  static int client_id_trace_disable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  // DBMS_MONITOR.OB_MOD_ACT_TRACE_ENABLE(module_name     IN VARCHAR2 DEFAULT ANY_MODULE,
  //                                      action_name     IN VARCHAR2 DEFAULT ANY_ACTION,
  //                                      level        IN  INT,
  //                                      sample_pct   IN  NUMBER,
  //                                      record_policy IN VARCHAR2);
  static int mod_act_trace_enable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  // DBMS_MONITOR.OB_MOD_ACT_TRACE_DISABLE(module_name     IN  VARCHAR2,
  //                                       action_name     IN  VARCHAR2 DEFAULT ALL_ACTIONS)
  static int mod_act_trace_disable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  // DBMS_MONITOR.OB_TRACE_ENABLE(level        IN  INT,
  //                              sample_pct   IN  NUMBER,
  //                              record_policy IN VARCHAR2);
  static int tenant_trace_enable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  // DBMS_MONITOR.OB_TENANT_TRACE_DISABLE(tenant_name  IN VARCHAR2 DEFAULT NULL);
  static int tenant_trace_disable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  static int resolve_control_info(FLTControlInfo &coninfo, ObNumber level, ObNumber sample_pct, ObString record_policy);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_MONITOR_H_ */
