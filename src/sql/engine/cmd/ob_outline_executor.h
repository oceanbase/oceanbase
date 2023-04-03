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

#ifndef OCEANBASE_SQL_OB_CREATE_OUTLINE_EXECUTOR_H_
#define OCEANBASE_SQL_OB_CREATE_OUTLINE_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{
namespace schema
{
class ObOutlineInfo;
}
}
namespace sql
{
class ObExecContext;
class ObCreateOutlineStmt;
class ObAlterOutlineStmt;
class ObDropOutlineStmt;
class ObLogPlan;
class ObDMLStmt;
class ObOptimizerContext;

class ObOutlineExecutor
{
public:
  ObOutlineExecutor() {}
  virtual ~ObOutlineExecutor() {}
protected:
  int get_outline(ObExecContext &ctx, ObDMLStmt *outline_stmt, common::ObString &outline);
  int generate_outline_info(ObExecContext &ctx, ObCreateOutlineStmt *outline_stmt,
                            share::schema::ObOutlineInfo &outline_info);
  int generate_outline_info1(ObExecContext &ctx, ObDMLStmt *outline_stmt,
                            share::schema::ObOutlineInfo &outline_info);
  int generate_outline_info2(ObExecContext &ctx, ObCreateOutlineStmt *outline_stmt,
                            share::schema::ObOutlineInfo &outline_info);
  int generate_logical_plan(ObExecContext &ctx,
                            ObOptimizerContext &opt_ctx,
                            ObDMLStmt *outline_stmt,
                            ObLogPlan *&logical_plan);
  bool is_valid_outline_stmt_type(stmt::StmtType type);
  int print_outline(ObExecContext &ctx, ObLogPlan *log_plan, common::ObString &outline);
private:
  DISALLOW_COPY_AND_ASSIGN(ObOutlineExecutor);
};

class ObCreateOutlineExecutor : public ObOutlineExecutor
{
public:
  ObCreateOutlineExecutor() {}
  virtual ~ObCreateOutlineExecutor() {}
  int execute(ObExecContext &ctx, ObCreateOutlineStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateOutlineExecutor);
};

class ObAlterOutlineExecutor : public ObOutlineExecutor
{
public:
  ObAlterOutlineExecutor() {}
  virtual ~ObAlterOutlineExecutor() {}
  int execute(ObExecContext &ctx, ObAlterOutlineStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterOutlineExecutor);
};

class ObDropOutlineExecutor
{
public:
  ObDropOutlineExecutor() {}
  virtual ~ObDropOutlineExecutor() {}
  int execute(ObExecContext &ctx, ObDropOutlineStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropOutlineExecutor);
};

}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_CREATE_OUTLINE_EXECUTOR_H_
