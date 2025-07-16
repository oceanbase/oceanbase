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

#ifndef _OB_LOCATION_UTILS_EXECUTOR_H
#define _OB_LOCATION_UTILS_EXECUTOR_H 1
#include "sql/resolver/cmd/ob_location_utils_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObLocationUtilsExecutor
{
public:
  ObLocationUtilsExecutor() {}
  virtual ~ObLocationUtilsExecutor() {}

  int execute(ObExecContext &ctx, ObLocationUtilsStmt &stmt);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLocationUtilsExecutor);
private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_LOCATION_UTILS_EXECUTOR_H */
