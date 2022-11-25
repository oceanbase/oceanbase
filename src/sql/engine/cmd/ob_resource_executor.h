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

#ifndef __OB_SQL_RESOURCE_EXECUTOR_H__
#define __OB_SQL_RESOURCE_EXECUTOR_H__
namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateResourcePoolStmt;
class ObDropResourcePoolStmt;
class ObSplitResourcePoolStmt;
class ObMergeResourcePoolStmt;
class ObAlterResourceTenantStmt;
class ObAlterResourcePoolStmt;
class ObCreateResourceUnitStmt;
class ObAlterResourceUnitStmt;
class ObDropResourceUnitStmt;

class ObCreateResourcePoolExecutor
{
public:
  ObCreateResourcePoolExecutor();
  virtual ~ObCreateResourcePoolExecutor();
  int execute(ObExecContext &ctx, ObCreateResourcePoolStmt &stmt);
private:
};

class ObDropResourcePoolExecutor
{
public:
  ObDropResourcePoolExecutor();
  virtual ~ObDropResourcePoolExecutor();
  int execute(ObExecContext &ctx, ObDropResourcePoolStmt &stmt);
private:
};

class ObSplitResourcePoolExecutor
{
public:
  ObSplitResourcePoolExecutor();
  virtual ~ObSplitResourcePoolExecutor();
  int execute(ObExecContext &ctx, ObSplitResourcePoolStmt &stmt);
private:
};

class ObMergeResourcePoolExecutor
{
public:
  ObMergeResourcePoolExecutor();
  virtual ~ObMergeResourcePoolExecutor();
  int execute(ObExecContext &ctx, ObMergeResourcePoolStmt &stmt);
private:
};

class ObAlterResourceTenantExecutor
{
public:
  ObAlterResourceTenantExecutor();
  virtual ~ObAlterResourceTenantExecutor();
  int execute(ObExecContext &ctx, ObAlterResourceTenantStmt &stmt);
private:
};

class ObAlterResourcePoolExecutor
{
public:
  ObAlterResourcePoolExecutor();
  virtual ~ObAlterResourcePoolExecutor();
  int execute(ObExecContext &ctx, ObAlterResourcePoolStmt &stmt);
private:
};

class ObCreateResourceUnitExecutor
{
public:
  ObCreateResourceUnitExecutor();
  virtual ~ObCreateResourceUnitExecutor();
  int execute(ObExecContext &ctx, ObCreateResourceUnitStmt &stmt);
private:
};

class ObAlterResourceUnitExecutor
{
public:
  ObAlterResourceUnitExecutor();
  virtual ~ObAlterResourceUnitExecutor();
  int execute(ObExecContext &ctx, ObAlterResourceUnitStmt &stmt);
private:
};

class ObDropResourceUnitExecutor
{
public:
  ObDropResourceUnitExecutor();
  virtual ~ObDropResourceUnitExecutor();
  int execute(ObExecContext &ctx, ObDropResourceUnitStmt &stmt);
private:
};

}
}
#endif /* __OB_SQL_RESOURCE_EXECUTOR_H__ */
//// end of header file

