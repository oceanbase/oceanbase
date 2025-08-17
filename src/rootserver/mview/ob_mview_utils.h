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

#ifndef OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_UTILS_H_
#define OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_UTILS_H_

#include "lib/ob_define.h"
#include "share/schema/ob_table_schema.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewUtils
{
public:
  static int create_inner_session(const uint64_t tenant_id,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  sql::ObFreeSessionCtx &free_session_ctx,
                                  sql::ObSQLSessionInfo *&session);
  static void release_inner_session(sql::ObFreeSessionCtx &free_session_ctx,
                                    sql::ObSQLSessionInfo *&session);
  static int submit_build_mlog_task(const uint64_t tenant_id,
                                    ObIArray<ObString> &mlog_columns,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    const share::schema::ObTableSchema *base_table_schema,
                                    int64_t &task_id);
};

} // namespace rootserver
} // namespace oceanbase
#endif
