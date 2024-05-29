/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_mview_mysql.h"
#include "storage/mview/cmd/ob_mview_purge_log_executor.h"
#include "storage/mview/cmd/ob_mview_refresh_executor.h"

namespace oceanbase
{
namespace pl
{
using namespace common;
using namespace sql;
using namespace storage;

/*
PROCEDURE purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 1);
*/
int ObDBMSMViewMysql::purge_log(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  if (2 != params.count()
      || !params.at(0).is_varchar()
      || !params.at(1).is_int32()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for mlog purge", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObMViewPurgeLogArg purge_params;
    ObMViewPurgeLogExecutor purge_executor;
    // fill params
    purge_params.master_ = params.at(0).get_varchar();
    purge_params.purge_log_parallel_ = params.at(1).get_int() >= 0 ? params.at(1).get_int() : 1;
    if (OB_FAIL(purge_executor.execute(ctx, purge_params))) {
      LOG_WARN("fail to execute mlog purge", KR(ret), K(purge_params));
    }
  }
  return ret;
}

/*
PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 1);
*/
int ObDBMSMViewMysql::refresh(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  if (3 != params.count()
     || !params.at(0).is_varchar()
     || (!params.at(1).is_null() && !params.at(1).is_varchar())
     || !params.at(2).is_int32()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for materialized view refresh", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObMViewRefreshArg refresh_params;
    ObMViewRefreshExecutor refresh_executor;
    refresh_params.list_ = params.at(0).get_varchar();
    params.at(1).is_varchar() ? refresh_params.method_ = params.at(1).get_varchar() : NULL;
    refresh_params.refresh_parallel_ = params.at(2).get_int();
    if (OB_FAIL(refresh_executor.execute(ctx, refresh_params))) {
      LOG_WARN("fail to execute mview refresh", KR(ret), K(refresh_params));
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
