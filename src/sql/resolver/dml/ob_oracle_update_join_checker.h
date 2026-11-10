/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_ORACLE_UPDATE_JOIN_CHECKER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_ORACLE_UPDATE_JOIN_CHECKER_H_
namespace oceanbase
{
namespace sql
{
class ObUpdateStmt;
class ObDMLStmt;
class ObSchemaChecker;
class ObSQLSessionInfo;
// Deliberately separate from the existing Oracle updatable-view and MySQL rules.
class ObOracleUpdateJoinChecker
{
public:
  // Recognize only the direct Oracle JOIN syntax; check() validates its supported subset.
  static bool is_update_join(const ObDMLStmt &stmt);
  static int check(const ObUpdateStmt &stmt, const ObSchemaChecker &schema_checker,
                   const ObSQLSessionInfo &session, bool is_batch_stmt);
};
}
}
#endif
