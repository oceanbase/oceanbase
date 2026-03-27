/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_OB_DES_EXEC_CONTEXT_
#define OCEANBASE_SQL_ENGINE_OB_DES_EXEC_CONTEXT_

#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace sql
{
class ObDesExecContext : public ObExecContext
{
public:
  ObDesExecContext(common::ObIAllocator &allocator, ObSQLSessionMgr *session_mgr);
  virtual ~ObDesExecContext();
  int create_my_session(uint64_t tenant_id);
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  void cleanup_session();
  void show_session();
  void hide_session();
protected:
  ObFreeSessionCtx free_session_ctx_;
  ObSqlCtx sql_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDesExecContext);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_OB_DES_EXEC_CONTEXT_ */
