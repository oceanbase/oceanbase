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

#ifndef OCEANBASE_LOAD_DATA_EXECUTOR_H_
#define OCEANBASE_LOAD_DATA_EXECUTOR_H_
#include "sql/resolver/cmd/ob_load_data_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObLoadDataStmt;
class ObLoadDataExecutor
{
public:
  ObLoadDataExecutor() {}
  virtual ~ObLoadDataExecutor() {}

  int execute(ObExecContext &ctx, ObLoadDataStmt &stmt);
private:
  int check_is_direct_load(ObTableDirectInsertCtx &ctx, const ObLoadDataHint &load_hint);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataExecutor);
  // function members
private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_LOAD_DATA_EXECUTOR_H_ */
