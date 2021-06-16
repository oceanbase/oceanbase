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

#ifndef OCEANBASE_SQL_ENGINE_OB_DES_EXEC_CONTEXT_
#define OCEANBASE_SQL_ENGINE_OB_DES_EXEC_CONTEXT_

#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase {
namespace sql {
class ObDesExecContext : public ObExecContext {
public:
  explicit ObDesExecContext(ObSQLSessionMgr* session_mgr);
  ObDesExecContext(common::ObIAllocator& allocator, ObSQLSessionMgr* session_mgr);
  virtual ~ObDesExecContext();
  int create_my_session(uint64_t tenant_id);
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  void cleanup_session();
  void show_session();
  void hide_session();

protected:
  ObFreeSessionCtx free_session_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDesExecContext);
};

class ObDistributedExecContext : public ObDesExecContext {
public:
  explicit ObDistributedExecContext(ObSQLSessionMgr* session_mgr);
  ObDistributedExecContext(common::ObIAllocator& allocator, ObSQLSessionMgr* session_mgr);
  virtual ~ObDistributedExecContext();
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;

private:
  const char* phy_plan_ctx_buf_;
  int64_t phy_plan_ctx_len_;
  const char* my_session_buf_;
  int64_t my_session_len_;
  const char* task_executor_ctx_buf_;
  int64_t task_executor_ctx_len_;
  DISALLOW_COPY_AND_ASSIGN(ObDistributedExecContext);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_OB_DES_EXEC_CONTEXT_ */
