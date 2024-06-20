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

#ifndef OCEANBASE_SQL_RESOLVER_OB_DDL_STMT_H_
#define OCEANBASE_SQL_RESOLVER_OB_DDL_STMT_H_ 1
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/ob_cmd.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace sql
{
class ObDDLStmt : public ObStmt, public ObICmd
{
  const static int OB_DEFAULT_ARRAY_SIZE = 16;
public:
  ObDDLStmt(common::ObIAllocator *name_pool, stmt::StmtType type)
    : ObStmt(name_pool, type), parallelism_(1L)
  {
  }
  explicit ObDDLStmt(stmt::StmtType type): ObStmt(type)
  {
  }
  virtual ~ObDDLStmt() {}
  virtual int get_cmd_type() const { return get_stmt_type(); }
  virtual obrpc::ObDDLArg &get_ddl_arg() = 0;
  typedef common::ObSEArray<ObRawExpr *,
                            OB_DEFAULT_ARRAY_SIZE,
                            common::ModulePageAllocator,
                            true> array_t;
  typedef common::ObSEArray<array_t,
                            OB_DEFAULT_ARRAY_SIZE,
                            common::ModulePageAllocator,
                            true> array_array_t;
  virtual bool cause_implicit_commit() const { return true; }
  virtual int get_first_stmt(common::ObString &first_stmt);
  void set_parallelism(const int64_t parallelism) { parallelism_ = parallelism; }
  int64_t &get_parallelism() { return parallelism_; }
protected:
  ObArenaAllocator allocator_;
private:
  int64_t parallelism_;

  DISALLOW_COPY_AND_ASSIGN(ObDDLStmt);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_OB_DDL_STMT_H_ */
