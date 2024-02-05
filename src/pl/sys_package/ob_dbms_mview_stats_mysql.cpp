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

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_mview_stats_mysql.h"
#include "storage/mview/cmd/ob_mview_stats_purge_refresh_stats_executor.h"
#include "storage/mview/cmd/ob_mview_stats_set_mvref_stats_params_executor.h"
#include "storage/mview/cmd/ob_mview_stats_set_system_default_executor.h"

namespace oceanbase
{
namespace pl
{
using namespace common;
using namespace share::schema;
using namespace sql;
using namespace storage;

/*
PROCEDURE do_set_system_default(
    IN     parameter_name         VARCHAR(65535),
    IN     collection_level       VARCHAR(65535)     DEFAULT NULL,
    IN     retention_period       INT                DEFAULT NULL);
*/
int ObDBMSMViewStatsMysql::set_system_default(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  CK(OB_LIKELY(3 == params.count()));
  CK(OB_LIKELY(params.at(0).is_varchar()) /*parameter_name*/,
     OB_LIKELY(params.at(1).is_null() || params.at(1).is_varchar()) /*collection_level*/,
     OB_LIKELY(params.at(2).is_null() || params.at(2).is_int32()) /*retention_period*/);
  if (OB_SUCC(ret)) {
    ObMViewStatsSetSystemDefaultArg set_arg;
    ObMViewStatsSetSystemDefaultExecutor set_executor;
    set_arg.parameter_name_ = params.at(0).get_string();
    if (!params.at(1).is_null() &&
        FALSE_IT(set_arg.collection_level_ = params.at(1).get_string())) {
    } else if (!params.at(2).is_null() &&
               FALSE_IT(set_arg.retention_period_ = params.at(2).get_int())) {
    } else if (OB_FAIL(set_executor.execute(ctx, set_arg))) {
      LOG_WARN("fail to execute mview stats set system default", KR(ret), K(set_arg));
    }
  }
  return ret;
}

/*
PROCEDURE set_mvref_stats_params(
    IN     mv_name                VARCHAR(65535),
    IN     collection_level       VARCHAR(65535)     DEFAULT NULL,
    IN     retention_period       INT                DEFAULT NULL);
*/
int ObDBMSMViewStatsMysql::set_mvref_stats_params(ObExecContext &ctx, ParamStore &params,
                                                  ObObj &result)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  CK(OB_LIKELY(3 == params.count()));
  CK(OB_LIKELY(params.at(0).is_null() || params.at(0).is_varchar()) /*mv_name*/,
     OB_LIKELY(params.at(1).is_null() || params.at(1).is_varchar()) /*collection_level*/,
     OB_LIKELY(params.at(2).is_null() || params.at(2).is_int32()) /*retention_period*/);
  if (OB_SUCC(ret)) {
    ObMViewStatsSetMVRefStatsParamsArg set_arg;
    ObMViewStatsSetMVRefStatsParamsExecutor set_executor;
    if (!params.at(0).is_null() && FALSE_IT(set_arg.mv_list_ = params.at(0).get_string())) {
    } else if (!params.at(1).is_null() &&
               FALSE_IT(set_arg.collection_level_ = params.at(1).get_string())) {
    } else if (!params.at(2).is_null() &&
               FALSE_IT(set_arg.retention_period_ = params.at(2).get_int())) {
    } else if (OB_FAIL(set_executor.execute(ctx, set_arg))) {
      LOG_WARN("fail to execute mview stats set mvref stats params", KR(ret), K(set_arg));
    }
  }
  return ret;
}

/*
PROCEDURE purge_refresh_stats(
    IN     mv_name                VARCHAR(65535),
    IN     retention_period       INT);
*/
int ObDBMSMViewStatsMysql::purge_refresh_stats(ObExecContext &ctx, ParamStore &params,
                                               ObObj &result)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  CK(OB_LIKELY(2 == params.count()));
  CK(OB_LIKELY(params.at(0).is_null() || params.at(0).is_varchar()) /*mv_name*/,
     OB_LIKELY(params.at(1).is_null() || params.at(1).is_int32()) /*retention_period*/);
  if (OB_SUCC(ret)) {
    ObMViewStatsPurgeRefreshStatsArg purge_arg;
    ObMViewStatsPurgeRefreshStatsExecutor purge_executor;
    if (!params.at(0).is_null() && FALSE_IT(purge_arg.mv_list_ = params.at(0).get_string())) {
    } else if (!params.at(1).is_null() &&
               FALSE_IT(purge_arg.retention_period_ = params.at(1).get_int())) {
    } else if (OB_FAIL(purge_executor.execute(ctx, purge_arg))) {
      LOG_WARN("fail to execute mview purge refresh stats", KR(ret), K(purge_arg));
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
