/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "sql/engine/ob_exec_context.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "pl/ob_pl.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSMViewMysql
{
public:
enum ObDBMSMViewRefreshParam
{
  MV_LIST = 0,
  METHOD = 1,
  REFRESH_PARALLEL = 2,
  NESTED = 3,
  NESTED_REFRESH_MODE = 4,
  ASYNC = 5,
  FORCE = 6,
  MAX_PARAM
};

enum ObDBMSMViewRefreshReportParam
{
  REPORT_REFRESH_ID = 0,
  REPORT_MV_NAME = 1,
  REPORT_TENANT_ID = 2,
  REPORT_FORMAT = 3,
  REPORT_MAX_PARAM
};

public:
  ObDBMSMViewMysql() {}
  virtual ~ObDBMSMViewMysql() {}

#define DECLARE_FUNC(func) \
  static int func(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(purge_log);
  DECLARE_FUNC(refresh);
  DECLARE_FUNC(refresh_report);
  DECLARE_FUNC(kill);
  DECLARE_FUNC(set_refresh_params);

#undef DECLARE_FUNC

  static int explain_refresh(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

};

} // namespace pl
} // namespace oceanbase
