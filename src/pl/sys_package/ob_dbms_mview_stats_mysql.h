/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "sql/engine/ob_exec_context.h"
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
namespace pl
{

class ObPLExecCtx;

class ObDBMSMViewStatsMysql
{
public:
  ObDBMSMViewStatsMysql() {}
  virtual ~ObDBMSMViewStatsMysql() {}

#define DECLARE_FUNC(func) \
  static int func(ObPLExecCtx &pl_ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(set_system_default);
  DECLARE_FUNC(set_mvref_stats_params);
  DECLARE_FUNC(purge_refresh_stats);

#undef DECLARE_FUNC
};

} // namespace pl
} // namespace oceanbase
