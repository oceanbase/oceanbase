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

#ifndef OCEANBASE_SQL_TRANS_HOOK_
#define OCEANBASE_SQL_TRANS_HOOK_
#include "share/ob_define.h"
#include "common/ob_partition_key.h"
#include "storage/transaction/ob_trans_define.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
namespace oceanbase {
namespace transaction {
class ObStartTransParam;
class ObTransDesc;
}  // namespace transaction

namespace sql {
class ObStmt;
class ObSQLSessionInfo;

class ObSqlTransHook {
public:
  static int before_end_trans(ObSQLSessionInfo* session);
  static int after_end_trans(ObSQLSessionInfo* session, ObExclusiveEndTransCallback& callback, int64_t timeout);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlTransHook);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_TRANS_HOOK_ */
//// end of header file
