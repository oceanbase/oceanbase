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

#include "ob_dbms_vector_mysql.h"
#include "src/pl/ob_pl.h"
#include "storage/vector_index/cmd/ob_vector_refresh_index_executor.h"

namespace oceanbase
{
namespace pl
{
using namespace common;
using namespace sql;
using namespace storage;

/*
PROCEDURE refresh_index(
  IN       IDX_NAME            VARCHAR(65535),               ---- 索引名
  IN       TABLE_NAME          VARCHAR(65535),               ---- 表名
  IN       IDX_VECTOR_COL      VARCHAR(65535) DEFAULT NULL,  ---- 向量列名
  IN       REFRESH_THRESHOLD   INT DEFAULT 10000,            ---- 3号表记录数达到阈值，触发增量刷新
  IN       REFRESH_TYPE        VARCHAR(65535) DEFAULT NULL   ---- 预留: 目前默行为是增量刷新: FAST
);
*/
int ObDBMSVectorMySql::refresh_index(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0);
  CK(OB_LIKELY(5 == params.count()));
  CK(OB_LIKELY(params.at(0).is_varchar()),
      OB_LIKELY(params.at(1).is_varchar()),
      OB_LIKELY(params.at(2).is_null() || params.at(2).is_varchar()),
      OB_LIKELY(params.at(3).is_int32()),
      OB_LIKELY(params.at(4).is_null() || params.at(4).is_varchar()));
  if (OB_SUCC(ret)) {
      ObVectorRefreshIndexArg refresh_arg;
      ObVectorRefreshIndexExecutor refresh_executor;
      refresh_arg.idx_name_ = params.at(0).get_varchar();
      refresh_arg.table_name_ = params.at(1).get_varchar();
      params.at(2).is_varchar() ? refresh_arg.idx_vector_col_ = params.at(2).get_varchar() : NULL;
      refresh_arg.refresh_threshold_ = params.at(3).get_int();
      params.at(4).is_varchar() ? refresh_arg.refresh_type_ = params.at(4).get_varchar() : NULL;
      if (OB_FAIL(refresh_executor.execute_refresh(ctx, refresh_arg))) {
          LOG_WARN("fail to execute refresh index", KR(ret), K(refresh_arg));
      }
  }
  return ret;
}

/*
PROCEDURE rebuild_index (
    IN      IDX_NAME                VARCHAR(65535),                      ---- 索引名
    IN      TABLE_NAME              VARCHAR(65535),                      ---- 表名
    IN      IDX_VECTOR_COL          VARCHAR(65535) DEFAULT NULL,         ---- 向量列名
    IN      DELTA_RATE_THRESHOLD    FLOAT DEFAULT 0.2,                   ---- (3号表记录数+4号表记录数)/基表记录数达到阈值时，触发重建
    IN      IDX_ORGANIZATION        VARCHAR(65535) DEFAULT NULL,         ---- 索引类型，本期不允许rebuild修改索引类型
    IN      IDX_DISTANCE_METRICS    VARCHAR(65535) DEFAULT 'EUCLIDEAN',  ---- 距离类型，本期不允许修改
    IN      IDX_PARAMETERS          LONGTEXT DEFAULT NULL,               ---- 索引参数，本期不允许修改
    IN      IDX_PARALLEL_CREATION   INT DEFAULT 1                        ---- 并行构建索引的并行度，预留，仅语法支持
);
*/
int ObDBMSVectorMySql::rebuild_index(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0);
  CK(OB_LIKELY(8 == params.count()));
  CK(OB_LIKELY(params.at(0).is_varchar()),
      OB_LIKELY(params.at(1).is_varchar()),
      OB_LIKELY(params.at(2).is_null() || params.at(2).is_varchar()),
      OB_LIKELY(params.at(3).is_float()),
      OB_LIKELY(params.at(4).is_null() || params.at(4).is_varchar()),
      OB_LIKELY(params.at(5).is_varchar()),
      OB_LIKELY(params.at(6).is_null() || params.at(6).is_text()),
      OB_LIKELY(params.at(7).is_int32()));
  if (OB_SUCC(ret)) {
      ObVectorRebuildIndexArg rebuild_arg;
      ObVectorRefreshIndexExecutor rebuild_executor;
      rebuild_arg.idx_name_ = params.at(0).get_varchar();
      rebuild_arg.table_name_ = params.at(1).get_varchar();
      params.at(2).is_varchar() ? rebuild_arg.idx_vector_col_ = params.at(2).get_varchar() : NULL;
      rebuild_arg.delta_rate_threshold_ = params.at(3).get_float();
      params.at(4).is_varchar() ? rebuild_arg.idx_organization_ = params.at(4).get_varchar() : NULL;
      rebuild_arg.idx_distance_metrics_ = params.at(5).get_varchar();
      rebuild_arg.idx_parallel_creation_ = params.at(7).get_int();

      rebuild_arg.idx_parameters_ = NULL;
      if (params.at(6).is_text() && OB_FAIL(params.at(6).get_string(rebuild_arg.idx_parameters_))) {
          LOG_WARN("fail to get string", K(ret));
      } else if (OB_FAIL(rebuild_executor.execute_rebuild(ctx, rebuild_arg))) {
          LOG_WARN("fail to execute refresh index", KR(ret), K(rebuild_arg));
      }
  }
  return ret;
}

int ObDBMSVectorMySql::refresh_index_inner(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0);
  CK(OB_LIKELY(3 == params.count()));
  CK(OB_LIKELY(params.at(0).is_int()),
      OB_LIKELY(params.at(1).is_int32()),
      OB_LIKELY(params.at(2).is_null() || params.at(2).is_varchar()));
  if (OB_SUCC(ret)) {
    ObVectorRefreshIndexInnerArg refresh_arg;
    ObVectorRefreshIndexExecutor refresh_executor;
    refresh_arg.idx_table_id_ = params.at(0).get_int();
    refresh_arg.refresh_threshold_ = params.at(1).get_int();
    params.at(2).is_varchar() ? refresh_arg.refresh_type_ = params.at(2).get_varchar() : NULL;
    if (OB_FAIL(refresh_executor.execute_refresh_inner(ctx, refresh_arg))) {
        LOG_WARN("fail to execute refresh index", KR(ret), K(refresh_arg));
    }
  }
  return ret;
}

int ObDBMSVectorMySql::rebuild_index_inner(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0);
  CK(OB_LIKELY(6 == params.count()));
  CK(OB_LIKELY(params.at(0).is_int()),
      OB_LIKELY(params.at(1).is_float()),
      OB_LIKELY(params.at(2).is_null() || params.at(2).is_varchar()),
      OB_LIKELY(params.at(3).is_varchar()),
      OB_LIKELY(params.at(4).is_null() || params.at(4).is_text()),
      OB_LIKELY(params.at(5).is_int32()));
  if (OB_SUCC(ret)) {
    ObVectorRebuildIndexInnerArg rebuild_arg;
    ObVectorRefreshIndexExecutor rebuild_executor;
    rebuild_arg.idx_table_id_ = params.at(0).get_int();
    rebuild_arg.delta_rate_threshold_ = params.at(1).get_float();
    params.at(2).is_varchar() ? rebuild_arg.idx_organization_ = params.at(2).get_varchar() : NULL;
    rebuild_arg.idx_distance_metrics_ = params.at(3).get_varchar();
    rebuild_arg.idx_parallel_creation_ = params.at(5).get_int();

    rebuild_arg.idx_parameters_ = NULL;
    if (params.at(4).is_text() && OB_FAIL(params.at(4).get_string(rebuild_arg.idx_parameters_))) {
        LOG_WARN("fail to get string", K(ret));
    } else if (OB_FAIL(rebuild_executor.execute_rebuild_inner(ctx, rebuild_arg))) {
        LOG_WARN("fail to execute refresh index", KR(ret), K(rebuild_arg));
    }
  }
  return ret;
}

}
}