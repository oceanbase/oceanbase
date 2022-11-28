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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DEBUG_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DEBUG_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSDebug
{
public:
  ObDBMSDebug() {}
  virtual ~ObDBMSDebug() {}
public:
#define DECLARE_PDB_FUNC(name) \
  static int name(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

DECLARE_PDB_FUNC(get_values);
DECLARE_PDB_FUNC(target_program_running);
DECLARE_PDB_FUNC(ping);
DECLARE_PDB_FUNC(print_backtrace);
DECLARE_PDB_FUNC(get_runtime_info);
DECLARE_PDB_FUNC(on);
DECLARE_PDB_FUNC(off);
DECLARE_PDB_FUNC(initialize);
DECLARE_PDB_FUNC(detach_session);
DECLARE_PDB_FUNC(show_breakpoints);
DECLARE_PDB_FUNC(get_timeout_behaviour);
DECLARE_PDB_FUNC(attach_session);
DECLARE_PDB_FUNC(debug_continue);
DECLARE_PDB_FUNC(del_breakpoint);
DECLARE_PDB_FUNC(enable_breakpoint);
DECLARE_PDB_FUNC(disable_breakpoint);
DECLARE_PDB_FUNC(print_varchar_backtrace);
DECLARE_PDB_FUNC(set_timeout);
DECLARE_PDB_FUNC(set_timeout_behaviour);
DECLARE_PDB_FUNC(set_breakpoint);
DECLARE_PDB_FUNC(get_value);

#undef DECLARE_PDB_FUNC

  static int check_debug_sys_priv(sql::ObExecContext &ctx, const share::ObRawPriv &priv_id);
  static int check_debug_sys_priv_impl(ObSchemaGetterGuard &guard,
                                           sql::ObSQLSessionInfo &sess_info,
                                           const share::ObRawPriv &priv_id);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DEBUG_H_ */
