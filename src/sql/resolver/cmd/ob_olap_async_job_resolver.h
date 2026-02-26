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

#ifndef _OB_OLAP_ASYNC_JOB_RESOLVER_H
#define _OB_OLAP_ASYNC_JOB_RESOLVER_H 1

#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/cmd/ob_olap_async_job_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObOLAPAsyncJobResolver: public ObSelectResolver
{
public:
  explicit ObOLAPAsyncJobResolver(ObResolverParams &params);
  virtual ~ObOLAPAsyncJobResolver() = default;

  virtual int resolve(const ParseNode &parse_tree);
private:
  static const int OB_JOB_NAME_MAX_LENGTH = 128;
  static const int OB_JOB_SQL_MAX_LENGTH = 4096;
  int resolve_submit_job_stmt(const ParseNode &parse_tree, ObOLAPAsyncSubmitJobStmt &stmt);
  int resolve_cancel_job_stmt(const ParseNode &parse_tree, ObOLAPAsyncCancelJobStmt *stmt);
  int execute_submit_job(ObOLAPAsyncSubmitJobStmt &stmt);
  int init_select_stmt(ObOLAPAsyncSubmitJobStmt &stmt);
  int parse_and_resolve_select_sql(const common::ObString &select_sql);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObOLAPAsyncJobResolver);
};
}//namespace sql
}//namespace oceanbase
#endif // _OB_OLAP_ASYNC_JOB_RESOLVER_H