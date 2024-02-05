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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_MLOG_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_MLOG_EXECUTOR_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
struct ObCreateMLogStmt;
class ObDropMLogStmt;
struct ObExecContext;

class ObCreateMLogExecutor
{
public:
  ObCreateMLogExecutor();
  virtual ~ObCreateMLogExecutor();
  int execute(ObExecContext &ctx, ObCreateMLogStmt &stmt);
};

class ObDropMLogExecutor
{
public:
  ObDropMLogExecutor();
  virtual ~ObDropMLogExecutor();

  int execute(ObExecContext &ctx, ObDropMLogStmt &stmt);
};
} // namespace sql
} // namespace oceanbase
#endif