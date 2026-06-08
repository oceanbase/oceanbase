/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
namespace sql
{
class ObFreeSessionCtx;
class ObSQLSessionInfo;
} // namespace sql
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
class ObUserInfo;
} // namespace schema
} // namespace share
namespace obrpc
{
struct ObRunMViewPendingTaskArg;
}
namespace rootserver
{

class ObMViewPendingTaskExecutor
{
public:
  ObMViewPendingTaskExecutor();
  ~ObMViewPendingTaskExecutor();
  DISALLOW_COPY_AND_ASSIGN(ObMViewPendingTaskExecutor);

  int init(share::schema::ObMultiVersionSchemaService *schema_service);
  int run_pending_task(const obrpc::ObRunMViewPendingTaskArg &arg);

private:
  int create_session(uint64_t tenant_id,
                     sql::ObFreeSessionCtx &free_session_ctx,
                     sql::ObSQLSessionInfo *&session);
  int destroy_session(sql::ObFreeSessionCtx &free_session_ctx,
                      sql::ObSQLSessionInfo *session);
  int get_exec_user_info(uint64_t tenant_id,
                         uint64_t mview_id,
                         bool is_oracle_mode,
                         share::schema::ObSchemaGetterGuard &schema_guard,
                         const share::schema::ObTableSchema *mview_schema,
                         const share::schema::ObUserInfo *&user_info);
  int init_env(uint64_t tenant_id,
               uint64_t mview_id,
               bool is_oracle_mode,
               int64_t expire_ts,
               share::schema::ObSchemaGetterGuard &schema_guard,
               sql::ObSQLSessionInfo &session);
  static int get_session_timeout_us(int64_t expire_ts, int64_t &timeout_us);

private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
};

} // namespace rootserver
} // namespace oceanbase
