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
#include "lib/oblog/ob_warning_buffer.h"
#include "storage/vector_index/cmd/ob_vector_refresh_index_executor.h"
#include "share/vector_index/ob_vector_recall_calc_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/utility/ob_fast_convert.h"
#include "share/schema/ob_schema_struct.h"

#define KEYWORD_APPROXIMATE " APPROXIMATE "
#define KEYWORD_APPROX " APPROX "
#define DEFAULT_NUM_VECTORS 25

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
  if (!params.at(0).is_varchar()
      || !params.at(1).is_varchar()
      || (!params.at(2).is_null() && !params.at(2).is_varchar())
      || !(!params.at(3).is_null() && params.at(3).is_int32())
      || (!params.at(4).is_null() && !params.at(4).is_varchar())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument for refresh index", KR(ret));
  }
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
  if (!params.at(0).is_varchar()
      || !params.at(1).is_varchar()
      || (!params.at(2).is_null() && !params.at(2).is_varchar())
      || !(!params.at(3).is_null() && params.at(3).is_float())
      || (!params.at(4).is_null() && !params.at(4).is_varchar())
      || !params.at(5).is_varchar()
      || (!params.at(6).is_null() && !params.at(6).is_text())
      || !(!params.at(7).is_null() && params.at(7).is_int32())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument for rebuild index", KR(ret));
  }
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

/*
PROCEDURE flush_index(
  IN       TABLE_NAME          VARCHAR(65535),               ---- 表名
  IN       IDX_NAME            VARCHAR(65535)                ---- 索引名
);
*/
int ObDBMSVectorMySql::flush_index(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0);
  CK(OB_LIKELY(2 == params.count()));
  if (!params.at(0).is_varchar()
      || !params.at(1).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for flush index", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObVectorRefreshIndexArg refresh_arg;
    ObVectorRefreshIndexExecutor refresh_executor;
    refresh_arg.table_name_ = params.at(0).get_varchar();
    refresh_arg.idx_name_ = params.at(1).get_varchar();
    if (OB_FAIL(refresh_executor.execute_flush(ctx, refresh_arg))) {
      LOG_WARN("fail to execute flush index", KR(ret), K(refresh_arg));
    }
  }
  return ret;
}

/*
PROCEDURE compact_index(
  IN      TABLE_NAME     VARCHAR(65535),                      ---- 表名
  IN      IDX_NAME       VARCHAR(65535)                       ---- 索引名
);
*/
int ObDBMSVectorMySql::compact_index(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0);
  CK(OB_LIKELY(2 == params.count()));
  if (!params.at(0).is_varchar()
      || !params.at(1).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for compact index", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObVectorRefreshIndexArg refresh_arg;
    ObVectorRefreshIndexExecutor refresh_executor;
    refresh_arg.table_name_ = params.at(0).get_varchar();
    refresh_arg.idx_name_ = params.at(1).get_varchar();
    if (OB_FAIL(refresh_executor.execute_compact(ctx, refresh_arg))) {
      LOG_WARN("fail to execute compact index", KR(ret), K(refresh_arg));
    }
  }
  return ret;
}

/*
PROCEDURE set_attribute (
    IN      IDX_NAME                VARCHAR(65535),                      ---- 索引名
    IN      TABLE_NAME              VARCHAR(65535),                      ---- 表名
    IN      ATTRIBUTE               VARCHAR(65535),                      ---- 修改名
    IN      VALUE                   VARCHAR(65535),                      ---- 修改值
);
*/
int ObDBMSVectorMySql::set_attribute(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0);
  CK(OB_LIKELY(4 == params.count()));
  if (!params.at(0).is_varchar()
      || !params.at(1).is_varchar()
      || !params.at(2).is_varchar()
      || !params.at(3).is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument set attribute", KR(ret));
  }

  if (OB_SUCC(ret)) {
    ObVectorRebuildIndexArg rebuild_arg;
    ObVectorRefreshIndexExecutor rebuild_executor;
    rebuild_arg.idx_name_ = params.at(0).get_varchar();
    rebuild_arg.table_name_ = params.at(1).get_varchar();
    rebuild_arg.attribute_ = params.at(2).get_varchar();
    rebuild_arg.value_ = params.at(3).get_varchar();

    if (OB_FAIL(rebuild_executor.set_attribute(ctx, rebuild_arg))) {
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

/*
FUNCTION index_vector_memory_advisor (
    IN     idx_type           VARCHAR(65535),
    IN     num_vectors        BIGINT UNSIGNED,
    IN     dim_count          INT UNSIGNED,
    IN     dim_type           VARCHAR(65535) DEFAULT 'FLOAT32',
    IN     idx_parameters     LONGTEXT DEFAULT NULL,
    IN     max_tablet_vectors BIGINT UNSIGNED DEFAULT 0)
RETURN VARCHAR(65535);
NOTE: for sparse vector index, dim_count is the average length of sparse vectors.
*/
int ObDBMSVectorMySql::index_vector_memory_advisor(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_3_5_3);
  CK(OB_LIKELY(6 == params.count()));
  if (OB_FAIL(ret)) {
  } else if (!params.at(0).is_varchar()
             || !params.at(1).is_uint64()
             || !params.at(2).is_uint32()
             || !params.at(3).is_varchar()
             || (!params.at(4).is_text() && !params.at(4).is_null())
             || !params.at(5).is_uint64()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObIAllocator *allocator = &ctx.exec_ctx_->get_allocator();
    ObString idx_type_str = params.at(0).get_varchar();
    uint64_t num_vectors = params.at(1).get_uint64();
    uint64_t max_tablet_vectors = params.at(5).get_uint64();
    if (max_tablet_vectors == 0) {
      max_tablet_vectors = num_vectors;
    }
    uint32_t dim_count = params.at(2).get_uint32();
    ObString dim_type_str = params.at(3).get_varchar();
    ObString idx_param_str;
    ObVectorIndexParam index_param;

    if (max_tablet_vectors > num_vectors) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid max_tablet_vectors", KR(ret), K(max_tablet_vectors), K(num_vectors));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "max_tablet_vectors large than num_vectors");
    } else if (dim_type_str.case_compare("FLOAT32") != 0) { // for future use
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support vector index dim type", K(ret), K(dim_type_str));
    } else if (params.at(4).is_text() && OB_FAIL(params.at(4).get_string(idx_param_str))) {
      LOG_WARN("failed to get index param string", K(ret));
    } else if (OB_FAIL(parse_idx_param(idx_type_str, idx_param_str, dim_count, index_param))) {
      LOG_WARN("failed to parase index param", K(ret), K(idx_type_str), K(idx_param_str));
    } else if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else {
      ObStringBuffer res_buf(allocator);
      uint64_t tablet_count = ceil(num_vectors / max_tablet_vectors);
      if (OB_FAIL(get_estimate_memory_str(index_param, num_vectors, max_tablet_vectors, tablet_count, res_buf, dim_count))) {
        LOG_WARN("failed to get estimate memory string");
      } else {
        result.set_varchar(res_buf.ptr(), res_buf.length());
        result.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
    }
  }
  return ret;
}

/*
FUNCTION index_vector_memory_estimate (
    IN     table_name        VARCHAR(65535),
    IN     column_name       VARCHAR(65535),
    IN     idx_type          VARCHAR(65535),
    IN     idx_parameters    LONGTEXT DEFAULT NULL)
RETURN VARCHAR(65535);
*/
int ObDBMSVectorMySql::index_vector_memory_estimate(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  CK(GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_3_5_3);
  CK(OB_LIKELY(4 == params.count()));
  if (OB_FAIL(ret)) {
  } else if (!params.at(0).is_varchar()
             || !params.at(1).is_varchar()
             || !params.at(2).is_varchar()
             || (!params.at(3).is_text() && !params.at(3).is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObIAllocator *allocator = &ctx.exec_ctx_->get_allocator();
    sql::ObSQLSessionInfo *session_info;
    sql::ObExecContext *exec_ctx = NULL;
    share::schema::ObSchemaGetterGuard *schema_guard = NULL;
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    ObCollationType cs_type = CS_TYPE_INVALID;

    ObString param_table_name = params.at(0).get_varchar();
    ObString column_name = params.at(1).get_varchar();
    ObString idx_type_str = params.at(2).get_varchar();
    ObString idx_param_str;

    ObString database_name, table_name;
    uint64_t table_id = OB_INVALID_ID;
    const ObColumnSchemaV2 *col_schema = nullptr;

    int64_t dim_count = 0;
    uint64_t num_vectors = 0;
    uint64_t tablet_max_num_vectors = 0;
    uint64_t tablet_count = 0;
    ObVectorIndexParam index_param;

    // resolve table name and column name,
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else if (OB_ISNULL(exec_ctx = ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec context is null", KR(ret));
    } else if (OB_ISNULL(session_info = exec_ctx->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info is null", KR(ret));
    } else if (OB_ISNULL(schema_guard = exec_ctx->get_virtual_table_ctx().schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null", KR(ret));
    } else if (OB_FAIL(session_info->get_name_case_mode(case_mode))) {
      LOG_WARN("fail to get name case mode", KR(ret));
    } else if (OB_FAIL(session_info->get_collation_connection(cs_type))) {
      LOG_WARN("fail to get collation_connection", KR(ret));
    } else if (OB_FAIL(ObVectorRefreshIndexExecutor::resolve_table_name(
                  cs_type, case_mode, lib::is_oracle_mode(), param_table_name,
                  database_name, table_name))) {
      LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(param_table_name));
    } else if (database_name.empty() && FALSE_IT(database_name = session_info->get_database_name())) {
    } else if (OB_UNLIKELY(database_name.empty())) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("No database selected", KR(ret));
    } else if (OB_FAIL(schema_guard->get_table_id(
                  exec_ctx->get_my_session()->get_effective_tenant_id(),
                  database_name,
                  table_name,
                  false, /*is_index*/
                  ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
                  table_id))) {
      LOG_WARN("failed to get table id", K(ret), K(database_name), K(table_name));
    } else if (table_id == OB_INVALID_ID) {
      ret = OB_TABLE_NOT_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, helper.convert(database_name), helper.convert(table_name));
    } else if (OB_FAIL(schema_guard->get_column_schema(
                   exec_ctx->get_my_session()->get_effective_tenant_id(),
                   table_id,
                   column_name,
                   col_schema))) {
      LOG_WARN("failed to get column shcema", K(ret), K(table_id), K(column_name));
    } else if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not found", K(ret));
    } else if (OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(col_schema->get_extended_type_info(), dim_count))) {
      LOG_WARN("fail to get vector dim", K(ret), K(col_schema));
    } else {
      // get row count of the target table
      const int64_t sum_pos = 0;
      const int64_t max_pos = 1;
      const int64_t count_pos = 2;
      ObObj sum_result_obj;
      ObObj max_result_obj;
      ObObj count_result_obj;
      const uint64_t tenant_id = exec_ctx->get_my_session()->get_effective_tenant_id();
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObSqlString query_string;
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(query_string.assign_fmt("SELECT cast(sum(table_rows) as unsigned) as sum, max(table_rows) as max, count(*) as count from information_schema.PARTITIONS WHERE table_schema='%.*s' and table_name='%.*s'",
                database_name.length(), database_name.ptr(), table_name.length(), table_name.ptr()))) {
          LOG_WARN("assign sql string failed", K(ret), K(database_name), K(table_name), K(column_name));
        } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, query_string.ptr()))) {
          LOG_WARN("read record failed", K(ret), K(tenant_id), K(query_string));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", K(ret), K(tenant_id), K(query_string));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("get next result failed", K(ret), K(tenant_id), K(query_string));
        } else if (OB_FAIL(result->get_obj(sum_pos, sum_result_obj))) {
          LOG_WARN("failed to get object", K(ret));
        } else if (OB_FAIL(result->get_obj(max_pos, max_result_obj))) {
          LOG_WARN("failed to get object", K(ret));
        } else if (OB_FAIL(result->get_obj(count_pos, count_result_obj))) {
          LOG_WARN("failed to get object", K(ret));
        } else if ((!sum_result_obj.is_null() && OB_UNLIKELY(!sum_result_obj.is_integer_type())) ||
                   (!max_result_obj.is_null() && OB_UNLIKELY(!max_result_obj.is_integer_type())) ||
                   (!count_result_obj.is_null() && OB_UNLIKELY(!count_result_obj.is_integer_type()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected obj type", K(ret), K(sum_result_obj.get_type()), K(max_result_obj.get_type()));
        } else if (!sum_result_obj.is_null() && OB_FALSE_IT(num_vectors = sum_result_obj.get_int())) {
        } else if (!max_result_obj.is_null() && OB_FALSE_IT(tablet_max_num_vectors = max_result_obj.get_int())) {
        } else if (!count_result_obj.is_null() && OB_FALSE_IT(tablet_count = count_result_obj.get_int())) {
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (params.at(3).is_text() && OB_FAIL(params.at(3).get_string(idx_param_str))) {
      LOG_WARN("fail to get index param string", K(ret));
    } else if (idx_param_str.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid params", K(ret), K(idx_param_str));
    } else if (OB_FAIL(parse_idx_param(idx_type_str, idx_param_str, dim_count, index_param))) {
      LOG_WARN("failed to parase index param", K(ret), K(idx_type_str), K(idx_param_str));
    } else if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else {
      uint32_t avg_sparse_length = 0;
      if (index_param.type_ == ObVectorIndexAlgorithmType::VIAT_IPIVF ||
          index_param.type_ == ObVectorIndexAlgorithmType::VIAT_IPIVF_SQ) {
        if (OB_FAIL(sample_sparse_vectors_and_calc_avg_length(
                *exec_ctx, database_name, table_name, column_name, num_vectors, avg_sparse_length))) {
          LOG_WARN("failed to sample sparse vectors and calculate average length", K(ret));
          avg_sparse_length = 0;
          ret = OB_SUCCESS;
        }
      }

      ObStringBuffer res_buf(allocator);
      if (OB_FAIL(get_estimate_memory_str(index_param, num_vectors, tablet_max_num_vectors, tablet_count, res_buf, avg_sparse_length))) {
        LOG_WARN("failed to get estimate memory string");
      } else {
        result.set_varchar(res_buf.ptr(), res_buf.length());
        result.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
    }
  }

  return ret;
}

int ObDBMSVectorMySql::parse_idx_param(const ObString &idx_type_str,
                                       const ObString &idx_param_str,
                                       uint32_t dim_count,
                                       ObVectorIndexParam &index_param)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_DIM_LIMITED = 4096;
  ObArenaAllocator tmp_alloc;
  ObVectorIndexType idx_type = VIT_MAX;
  ObStringBuffer param_str_buf(&tmp_alloc);
  ObString param_str;

  // parse idx_type
  if (idx_type_str.case_compare("HNSW") == 0
      || idx_type_str.case_compare("HNSW_SQ") == 0
      || idx_type_str.case_compare("HNSW_BQ") == 0
      || idx_type_str.case_compare("SINDI") == 0
      || idx_type_str.case_compare("SINDI_SQ") == 0) {
    idx_type = ObVectorIndexType::VIT_HNSW_INDEX;
  } else if (idx_type_str.case_compare("IVF_FLAT") == 0
             || idx_type_str.case_compare("IVF_SQ8") == 0
             || idx_type_str.case_compare("IVF_PQ") == 0) {
    idx_type = ObVectorIndexType::VIT_IVF_INDEX;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support vector index type", K(ret), K(idx_type_str));
  }

  // parse idx_param
  if (OB_FAIL(ret)) {
  } else if (idx_param_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(idx_param_str));
  } else if (OB_FAIL(param_str_buf.append(idx_param_str))) {
    LOG_WARN("failed to append string", K(ret), K(idx_param_str));
  } else if (OB_FAIL(param_str_buf.append(",TYPE="))) {
    LOG_WARN("failed to append string", K(ret));
  } else if (OB_FAIL(param_str_buf.append(idx_type_str))) {
    LOG_WARN("failed to append string", K(ret), K(idx_type_str));
  } else if (OB_FAIL(param_str_buf.get_result_string(param_str))) {
    LOG_WARN("failed to get result string", K(ret), K(param_str_buf));
  } else if (OB_FAIL(ob_simple_low_to_up(tmp_alloc, param_str, param_str))) {
    LOG_WARN("string low to up failed", K(ret), K(param_str));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(param_str, idx_type, index_param))) {
    LOG_WARN("fail to parser params from string", K(ret), K(param_str));
  } else if (index_param.dist_algorithm_ == VIDA_MAX) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unexpected setting of vector index param, distance has not been set",
      K(ret), K(index_param.dist_algorithm_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the vector index params of distance not set is");
  } else if (index_param.type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_BQ
             && ObCharset::locate(CS_TYPE_UTF8MB4_GENERAL_CI, param_str.ptr(), param_str.length(), "REFINE_TYPE", 11, 1) <= 0
             && OB_FALSE_IT(index_param.refine_type_ = common::obvsag::QuantizationType::SQ8)) {
  } else if ((index_param.type_ != ObVectorIndexAlgorithmType::VIAT_IPIVF && index_param.type_ != ObVectorIndexAlgorithmType::VIAT_IPIVF_SQ) &&
            OB_UNLIKELY(dim_count == 0 || dim_count > MAX_DIM_LIMITED)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("vector index dim equal to 0 or larger than 4096 is not supported", K(ret), K(index_param.type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "vec index dim equal to 0 or larger than 4096 is");
  } else {
    index_param.dim_ = dim_count;
  }

  return ret;
}

// Helper function to find the last occurrence of a substring in a string
static const char *strrstr(const char *haystack, int64_t haystack_len, const char *needle, int64_t needle_len)
{
  if (OB_ISNULL(haystack) || OB_ISNULL(needle) || needle_len > haystack_len || needle_len == 0) {
    return nullptr;
  }
  for (int64_t i = haystack_len - needle_len; i >= 0; i--) {
    if (memcmp(haystack + i, needle, needle_len) == 0) {
      return haystack + i;
    }
  }
  return nullptr;
}

// Helper function to find the first occurrence of a substring within a bounded string (no null terminator required)
static const char *strnstr(const char *haystack, int64_t haystack_len, const char *needle, int64_t needle_len)
{
  if (OB_ISNULL(haystack) || OB_ISNULL(needle) || needle_len > haystack_len || needle_len == 0) {
    return nullptr;
  }
  for (int64_t i = 0; i <= haystack_len - needle_len; i++) {
    if (memcmp(haystack + i, needle, needle_len) == 0) {
      return haystack + i;
    }
  }
  return nullptr;
}

static double parse_similarity_from_params(const char *params_content, int64_t content_len)
{
  double similarity = -1.0;
  if (OB_ISNULL(params_content) || content_len <= 0) {
    return similarity;
  }

  const int64_t key_len = 10;  // strlen("SIMILARITY")
  const char *sim_pos = nullptr;
  for (int64_t i = 0; i <= content_len - key_len; ++i) {
    if (strncasecmp(params_content + i, "SIMILARITY", key_len) == 0) {
      sim_pos = params_content + i;
      break;
    }
  }
  if (OB_ISNULL(sim_pos) || sim_pos >= params_content + content_len) {
    return similarity;
  }

  // Skip "SIMILARITY" and find '='
  const char *p = sim_pos + key_len;
  while (p < params_content + content_len && (*p == ' ' || *p == '\t')) {
    p++;
  }
  if (p < params_content + content_len && *p == '=') {
    p++;
    while (p < params_content + content_len && (*p == ' ' || *p == '\t')) {
      p++;
    }
    // Parse the numeric value
    char *endptr = nullptr;
    similarity = strtod(p, &endptr);
    if (endptr == p || similarity < 0.0 || similarity > 1.0) {
      similarity = -1.0;
    }
  }
  return similarity;
}

// Extract distance function and its arguments from ORDER BY clause
// e.g., "ORDER BY cosine_distance(embedding, @v)" -> func_name="COSINE_DISTANCE", func_args="embedding, @v"
static int extract_distance_func_from_order_by(const char *order_by_pos, int64_t max_len,
                                               const char *orig_sql, int64_t orig_offset,
                                               ObSqlString &func_args)
{
  int ret = OB_SUCCESS;
  func_args.reset();

  if (OB_ISNULL(order_by_pos)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Skip "ORDER BY " and whitespace
    const char *p = order_by_pos + 9; // strlen("ORDER BY ")
    while (p < order_by_pos + max_len && (*p == ' ' || *p == '\t')) {
      p++;
    }

    // Find the opening parenthesis of distance function
    const char *paren_start = strchr(p, '(');
    if (OB_ISNULL(paren_start) || paren_start >= order_by_pos + max_len) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      // Find matching closing parenthesis
      int depth = 1;
      const char *paren_end = paren_start + 1;
      while (paren_end < order_by_pos + max_len && *paren_end != '\0' && depth > 0) {
        if (*paren_end == '(') depth++;
        else if (*paren_end == ')') depth--;
        paren_end++;
      }

      if (depth != 0) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        // Extract arguments from original SQL (preserve original case)
        int64_t args_start_offset = orig_offset + (paren_start + 1 - order_by_pos);
        int64_t args_len = paren_end - paren_start - 2; // exclude '(' and ')'
        if (args_len > 0) {
          if (OB_FAIL(func_args.append(orig_sql + args_start_offset, args_len))) {
            LOG_WARN("fail to append func args", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// Get similarity function name from distance function name (case-insensitive)
// e.g., "l2_distance" or "L2_DISTANCE" -> "l2_similarity"
static const char *get_similarity_func_by_dist_name(const char *dist_func_name)
{
  if (OB_ISNULL(dist_func_name)) {
    return nullptr;
  }
  if (strncasecmp(dist_func_name, "COSINE_DISTANCE", 15) == 0) {
    return "cosine_similarity";
  } else if (strncasecmp(dist_func_name, "L2_DISTANCE", 11) == 0) {
    return "l2_similarity";
  } else if (strncasecmp(dist_func_name, "NEGATIVE_INNER_PRODUCT", 22) == 0) {
    return "inner_product";
  }
  return nullptr;
}

// Get similarity function name from distance function in ORDER BY clause
static const char *get_similarity_func_from_distance(const char *order_by_pos)
{
  if (OB_ISNULL(order_by_pos)) {
    return nullptr;
  }
  // Skip "ORDER BY " and whitespace
  const char *p = order_by_pos + 9;
  while (*p == ' ' || *p == '\t') {
    p++;
  }
  return get_similarity_func_by_dist_name(p);
}

int ObDBMSVectorMySql::build_brute_force_sql_from_approximate(const ObString &approx_sql,
                                                             common::ObSqlString &brute_sql)
{
  int ret = OB_SUCCESS;

  if (approx_sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("approximate sql is empty", K(ret));
  } else {
    ObArenaAllocator tmp_alloc("VecIdxRecall", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObString sql_upper;
    if (OB_FAIL(ob_simple_low_to_up(tmp_alloc, approx_sql, sql_upper))) {
      LOG_WARN("fail to convert sql to uppercase", K(ret));
    } else {
      const char *sql_ptr = sql_upper.ptr();
      int64_t sql_len = sql_upper.length();

      // Find APPROX/APPROXIMATE keyword (search from end to avoid string literals)
      const char *approx_pos_long = strrstr(sql_ptr, sql_len, KEYWORD_APPROXIMATE, strlen(KEYWORD_APPROXIMATE));
      const char *approx_pos_short = strrstr(sql_ptr, sql_len, KEYWORD_APPROX, strlen(KEYWORD_APPROX));

      const char *approx_pos = nullptr;
      int64_t keyword_len = 0;

      if (OB_NOT_NULL(approx_pos_long) && OB_NOT_NULL(approx_pos_short)) {
        if (approx_pos_long >= approx_pos_short) {
          approx_pos = approx_pos_long;
          keyword_len = strlen(KEYWORD_APPROXIMATE);
        } else {
          approx_pos = approx_pos_short;
          keyword_len = strlen(KEYWORD_APPROX);
        }
      } else if (OB_NOT_NULL(approx_pos_long)) {
        approx_pos = approx_pos_long;
        keyword_len = strlen(KEYWORD_APPROXIMATE);
      } else if (OB_NOT_NULL(approx_pos_short)) {
        approx_pos = approx_pos_short;
        keyword_len = strlen(KEYWORD_APPROX);
      }

      if (OB_ISNULL(approx_pos)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "APPROXIMATE or APPROX keyword not found in SQL");
        LOG_WARN("APPROXIMATE or APPROX keyword not found in SQL", K(ret));
      } else {
        int64_t prefix_len = approx_pos - sql_ptr;
        const char *suffix_start = approx_pos + keyword_len;
        int64_t suffix_len = sql_len - (suffix_start - sql_ptr);

        // Parse SIMILARITY from PARAMETERS clause (all within sql_ptr + sql_len; ObString may not be null-terminated)
        double similarity_threshold = -1.0;
        const char *const sql_end = sql_ptr + sql_len;
        const char *params_pos = strnstr(suffix_start, suffix_len, "PARAMETERS", 10);
        int64_t params_start_in_suffix = suffix_len;

        if (OB_NOT_NULL(params_pos)) {
          int64_t params_offset = params_pos - suffix_start;
          const char *paren_start = nullptr;
          for (const char *p = params_pos; p < sql_end; p++) {
            if (*p == '(') {
              paren_start = p;
              break;
            }
          }
          if (OB_NOT_NULL(paren_start)) {
            // Find matching closing parenthesis; do not read past sql_end
            int depth = 1;
            const char *paren_end = paren_start + 1;
            while (paren_end < sql_end && depth > 0) {
              if (*paren_end == '(') {
                depth++;
              } else if (*paren_end == ')') {
                depth--;
              }
              paren_end++;
            }

            if (depth == 0) {
              // Parse SIMILARITY value (content between '(' and ')')
              int64_t content_len = paren_end - paren_start - 1;
              similarity_threshold = parse_similarity_from_params(paren_start + 1, content_len);

              // Trim spaces before PARAMETERS
              params_start_in_suffix = params_offset;
              while (params_start_in_suffix > 0 &&
                     (*(suffix_start + params_start_in_suffix - 1) == ' ' ||
                      *(suffix_start + params_start_in_suffix - 1) == '\t')) {
                params_start_in_suffix--;
              }
            }
          }
        }

        // Build brute-force SQL
        brute_sql.reset();

        if (similarity_threshold < 0.0) {
          // No SIMILARITY param, just remove APPROX and PARAMETERS
          if (OB_FAIL(brute_sql.append(approx_sql.ptr(), prefix_len))) {
            LOG_WARN("fail to append prefix", K(ret));
          } else if (params_start_in_suffix > 0 &&
                     OB_FAIL(brute_sql.append(approx_sql.ptr() + prefix_len + keyword_len, params_start_in_suffix))) {
            LOG_WARN("fail to append suffix", K(ret));
          }
        } else {
          // Has SIMILARITY param, need to add WHERE clause
          // Find ORDER BY position (search backwards from APPROX to avoid string literals)
          const char *order_by_pos = strrstr(sql_ptr, prefix_len, " ORDER BY ", 10);
          if (OB_ISNULL(order_by_pos)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ORDER BY not found in SQL", K(ret));
          } else {
            // Extract distance function arguments
            ObSqlString func_args;
            int64_t order_by_offset = order_by_pos - sql_ptr;
            int64_t order_by_to_approx = prefix_len - order_by_offset;

            const char *similarity_func = get_similarity_func_from_distance(order_by_pos);
            if (OB_ISNULL(similarity_func)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unknown distance function in ORDER BY", K(ret));
            } else if (OB_FAIL(extract_distance_func_from_order_by(
                order_by_pos, order_by_to_approx, approx_sql.ptr(), order_by_offset, func_args))) {
              LOG_WARN("fail to extract distance func args", K(ret));
            } else {
              // Find WHERE clause position (between FROM and ORDER BY)
              const char *from_pos = strstr(sql_ptr, " FROM ");
              const char *where_pos = nullptr;

              // Search for WHERE between FROM and ORDER BY
              if (OB_NOT_NULL(from_pos) && from_pos < order_by_pos) {
                const char *search_start = from_pos + 6;
                while (search_start < order_by_pos) {
                  const char *tmp = strstr(search_start, " WHERE ");
                  if (OB_ISNULL(tmp) || tmp >= order_by_pos) {
                    break;
                  }
                  where_pos = tmp;
                  search_start = tmp + 7;
                }
              }

              // Insert similarity condition before ORDER BY
              // If WHERE exists: "... WHERE xxx AND similarity_cond ORDER BY ..."
              // If no WHERE: "... WHERE similarity_cond ORDER BY ..."
              if (OB_FAIL(brute_sql.append(approx_sql.ptr(), order_by_offset))) {
                LOG_WARN("fail to append sql before ORDER BY", K(ret));
              } else if (OB_ISNULL(where_pos) && OB_FAIL(brute_sql.append(" WHERE "))) {
                LOG_WARN("fail to append WHERE", K(ret));
              } else if (OB_NOT_NULL(where_pos) && OB_FAIL(brute_sql.append(" AND "))) {
                LOG_WARN("fail to append AND", K(ret));
              } else if (OB_FAIL(brute_sql.append_fmt("%s(%.*s) > %f",
                  similarity_func,
                  static_cast<int>(func_args.length()), func_args.ptr(),
                  similarity_threshold))) {
                LOG_WARN("fail to append similarity condition", K(ret));
              } else if (OB_FAIL(brute_sql.append(approx_sql.ptr() + order_by_offset, prefix_len - order_by_offset))) {
                // Append from ORDER BY to APPROX keyword
                LOG_WARN("fail to append ORDER BY clause", K(ret));
              } else if (params_start_in_suffix > 0 &&
                         OB_FAIL(brute_sql.append(approx_sql.ptr() + prefix_len + keyword_len, params_start_in_suffix))) {
                // Append from after APPROX to PARAMETERS (excluding PARAMETERS)
                LOG_WARN("fail to append suffix", K(ret));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDBMSVectorMySql::check_sql_valid(const ObString &sql_query)
{
  int ret = OB_SUCCESS;

  if (sql_query.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql query is empty", K(ret));
  } else {
    // Check for subqueries by counting SELECT keywords outside of string literals
    int select_count = 0;
    const char *ptr = sql_query.ptr();
    const char *end = ptr + sql_query.length();

    while (ptr < end) {
      // Skip string literals (single quotes)
      if (*ptr == '\'') {
        ptr++;
        while (ptr < end && *ptr != '\'') {
          if (*ptr == '\\' && ptr + 1 < end) {
            ptr += 2;  // Skip escaped character
          } else {
            ptr++;
          }
        }
        if (ptr < end) ptr++;  // Skip closing quote
        continue;
      }

      // Skip string literals (double quotes)
      if (*ptr == '"') {
        ptr++;
        while (ptr < end && *ptr != '"') {
          if (*ptr == '\\' && ptr + 1 < end) {
            ptr += 2;  // Skip escaped character
          } else {
            ptr++;
          }
        }
        if (ptr < end) ptr++;  // Skip closing quote
        continue;
      }

      // Check for SELECT keyword (case-insensitive)
      if (ptr + 6 <= end &&
          (ptr[0] == 'S' || ptr[0] == 's') &&
          (ptr[1] == 'E' || ptr[1] == 'e') &&
          (ptr[2] == 'L' || ptr[2] == 'l') &&
          (ptr[3] == 'E' || ptr[3] == 'e') &&
          (ptr[4] == 'C' || ptr[4] == 'c') &&
          (ptr[5] == 'T' || ptr[5] == 't')) {
        // Check if it's a standalone keyword (not part of another identifier)
        bool is_word_start = (ptr == sql_query.ptr()) ||
                             (!isalnum(static_cast<unsigned char>(*(ptr - 1))) && *(ptr - 1) != '_');
        bool is_word_end = (ptr + 6 >= end) ||
                           (!isalnum(static_cast<unsigned char>(*(ptr + 6))) && *(ptr + 6) != '_');

        if (is_word_start && is_word_end) {
          select_count++;
          if (select_count > 1) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "subquery in recall rate calculation SQL");
            LOG_WARN("subquery detected in sql_query, not supported", K(ret), K(sql_query));
            break;
          }
          ptr += 6;
          continue;
        }
      }

      ptr++;
    }
  }

  return ret;
}

int ObDBMSVectorMySql::get_estimate_memory_str(ObVectorIndexParam index_param,
                                               uint64_t num_vectors,
                                               uint64_t tablet_max_num_vectors,
                                               uint64_t tablet_count,
                                               ObStringBuffer &res_buf,
                                               uint32_t avg_sparse_length)
{
  int ret = OB_SUCCESS;
  const static double VEC_MEMORY_HOLD_FACTOR = 1.2;
  switch(index_param.type_) {
    case ObVectorIndexAlgorithmType::VIAT_HNSW:
    case ObVectorIndexAlgorithmType::VIAT_HNSW_SQ:
    case ObVectorIndexAlgorithmType::VIAT_HGRAPH: {
      uint64_t estimate_mem = 0;
      uint64_t max_tablet_estimate_mem = 0;
      if (OB_FAIL(ObVectorIndexUtil::estimate_hnsw_memory(num_vectors, index_param, estimate_mem))) {
        LOG_WARN("failed to estimate hnsw vector index memory", K(num_vectors), K(index_param));
      } else if (OB_FAIL(ObVectorIndexUtil::estimate_hnsw_memory(tablet_max_num_vectors, index_param, max_tablet_estimate_mem))) {
        LOG_WARN("failed to estimate hnsw vector index memory", K(tablet_max_num_vectors), K(index_param));
      } else if (OB_FALSE_IT(estimate_mem = ceil((estimate_mem + max_tablet_estimate_mem) * VEC_MEMORY_HOLD_FACTOR))) { // multiple 1.2
      } else if (OB_FAIL(res_buf.append(ObString("Suggested minimum vector memory is "), estimate_mem))) {
        LOG_WARN("failed to append to buffer", K(ret));
      } else if (OB_FAIL(print_mem_size(estimate_mem, res_buf))) {
        LOG_WARN("failed to append memory size", K(ret));
      }
      break;
    }
    case ObVectorIndexAlgorithmType::VIAT_HNSW_BQ: {
      uint64_t estimate_mem = 0;
      uint64_t suggested_mem = 0;
      if (OB_FAIL(ObVectorIndexUtil::estimate_hnsw_memory(num_vectors, index_param, estimate_mem))) {
        LOG_WARN("failed to estimate hnsw vector index memory", K(num_vectors), K(index_param));
      } else if (OB_FALSE_IT(estimate_mem = ceil(estimate_mem * VEC_MEMORY_HOLD_FACTOR))) { // multiple 1.2
      } else if (OB_FAIL(ObVectorIndexUtil::estimate_hnsw_memory(tablet_max_num_vectors, index_param, suggested_mem, true/*+is_build*/))) {
        LOG_WARN("failed to estimate hnsw vector index memory", K(num_vectors), K(index_param));
      } else {
        suggested_mem = estimate_mem + suggested_mem * VEC_MEMORY_HOLD_FACTOR;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(res_buf.append(ObString("Suggested minimum vector memory is "), 0))) {
        LOG_WARN("failed to append to buffer", K(ret));
      } else if (OB_FAIL(print_mem_size(suggested_mem, res_buf))) {
        LOG_WARN("failed to append memory size", K(ret));
      } else if (OB_FAIL(res_buf.append(ObString(", memory consumption when providing search service is "), 0))) {
        LOG_WARN("failed to append to buffer", K(ret));
      } else if (OB_FAIL(print_mem_size(estimate_mem, res_buf))) {
        LOG_WARN("failed to append memory size", K(ret));
      }
      break;
    }
    case ObVectorIndexAlgorithmType::VIAT_IVF_FLAT:
    case ObVectorIndexAlgorithmType::VIAT_IVF_SQ8:
    case ObVectorIndexAlgorithmType::VIAT_IVF_PQ: {
      uint64_t suggested_mem = 0;
      uint64_t buff_mem = 0;
      uint64_t construct_mem = 0;
      if (OB_FAIL(ObVectorIndexUtil::estimate_ivf_memory(tablet_max_num_vectors, index_param, construct_mem, buff_mem))) {
        LOG_WARN("failed to estimate ivf vector index memory", K(tablet_max_num_vectors), K(index_param));
      } else if (OB_FALSE_IT(suggested_mem = construct_mem + buff_mem * tablet_count)) {
      } else if (OB_FAIL(res_buf.append(ObString("Suggested minimum vector memory is "), 0))) {
        LOG_WARN("failed to append to buffer", K(ret));
      } else if (OB_FAIL(print_mem_size(suggested_mem, res_buf))) {
        LOG_WARN("failed to append memory size", K(ret));
      } else if (OB_FAIL(res_buf.append(ObString(", memory consumption when providing search service is "), 0))) {
        LOG_WARN("failed to append to buffer", K(ret));
      } else if (OB_FAIL(print_mem_size(buff_mem * tablet_count, res_buf))) {
        LOG_WARN("failed to append memory size", K(ret));
      }
      break;
    }
    case ObVectorIndexAlgorithmType::VIAT_IPIVF:
    case ObVectorIndexAlgorithmType::VIAT_IPIVF_SQ: {
      uint64_t estimate_mem = 0;
      uint64_t max_tablet_estimate_mem = 0;
      if (OB_FAIL(ObVectorIndexUtil::estimate_sparse_memory(num_vectors, index_param, estimate_mem, avg_sparse_length))) {
        LOG_WARN("failed to estimate sparse vector index memory", K(num_vectors), K(index_param), K(avg_sparse_length));
      } else if (OB_FAIL(ObVectorIndexUtil::estimate_sparse_memory(
                     tablet_max_num_vectors, index_param, max_tablet_estimate_mem, avg_sparse_length))) {
        LOG_WARN("failed to estimate sparse vector index memory", K(tablet_max_num_vectors), K(index_param), K(avg_sparse_length));
      } else if (OB_FALSE_IT(estimate_mem = ceil(
                                 (estimate_mem + max_tablet_estimate_mem) * VEC_MEMORY_HOLD_FACTOR))) {  // multiple 1.2
      } else if (OB_FAIL(res_buf.append(ObString("Suggested minimum vector memory is "), estimate_mem))) {
        LOG_WARN("failed to append to buffer", K(ret));
      } else if (OB_FAIL(print_mem_size(estimate_mem, res_buf))) {
        LOG_WARN("failed to append memory size", K(ret));
      }
      break;
    }
    case ObVectorIndexAlgorithmType::VIAT_SPIV:
    {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "esitamte sparse vector memory is");
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ivf algorithm type", K(ret), K(index_param));
    }
  }
  return ret;
}

int ObDBMSVectorMySql::print_mem_size(uint64_t mem_size, ObStringBuffer &res_buf)
{
  int ret = OB_SUCCESS;
  if (mem_size < 1024) {
    ObFastFormatInt size_str(mem_size);
    if (OB_FAIL(res_buf.append(size_str.ptr(), size_str.length(), 0))) {
      LOG_WARN("failed to append to buffer", K(ret));
    } else if (OB_FAIL(res_buf.append(ObString(" Bytes"), 0))) {
      LOG_WARN("failed to append to buffer", K(ret));
    }
  } else {
    const char* units[] = {"KB", "MB", "GB"};
    char mem_size_str[128] = "";
    int unit_index = 0;
    float float_mem_size = mem_size / 1024.0;
    while (float_mem_size >= 1024 && unit_index < 2) {
      float_mem_size /= 1024;
      unit_index++;
    }
    int res_len = snprintf(mem_size_str, 128, "%.1f %s", float_mem_size, units[unit_index]);
    if (OB_FAIL(res_buf.append(mem_size_str,res_len, 0))) {
      LOG_WARN("failed to append to buffer", K(ret));
    }
  }
  return ret;
}

int ObDBMSVectorMySql::sample_sparse_vectors_and_calc_avg_length(
    sql::ObExecContext &exec_ctx,
    const ObString &database_name,
    const ObString &table_name,
    const ObString &column_name,
    uint64_t num_vectors,
    uint32_t &avg_sparse_length)
{
  int ret = OB_SUCCESS;
  avg_sparse_length = 0;
  const uint64_t tenant_id = exec_ctx.get_my_session()->get_effective_tenant_id();

  if (num_vectors == 0) {
    avg_sparse_length = 0;
  } else {
    const uint64_t TARGET_SAMPLE_COUNT = 100;
    const uint64_t FIXED_SAMPLE_SEED = 12345;
    uint64_t total_length = 0;
    uint64_t valid_sample_count = 0;
    ObArenaAllocator tmp_alloc("SparseVecSample", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);

    // Calculate sample percentage to get approximately TARGET_SAMPLE_COUNT samples
    // Add buffer to account for NULL values and ensure enough samples
    double sample_percent = (TARGET_SAMPLE_COUNT * 100.0 / num_vectors) * 1.5;
    if (sample_percent >= 100.0) {
      sample_percent = 99.9;
    }

    ObSqlString query_string;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;

      // Use SAMPLE syntax with fixed seed for consistent results
      if (OB_FAIL(query_string.assign_fmt(
              "SELECT `%.*s` FROM `%.*s`.`%.*s` SAMPLE(%.2f) SEED(%lu) "
              "WHERE `%.*s` IS NOT NULL "
              "LIMIT %lu",
              column_name.length(), column_name.ptr(),
              database_name.length(), database_name.ptr(),
              table_name.length(), table_name.ptr(),
              sample_percent,
              FIXED_SAMPLE_SEED,
              column_name.length(), column_name.ptr(),
              TARGET_SAMPLE_COUNT))) {
        LOG_WARN("assign sql string failed", K(ret), K(database_name), K(table_name), K(column_name));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, query_string.ptr()))) {
        LOG_WARN("read record failed", K(ret), K(tenant_id), K(query_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), K(tenant_id), K(query_string));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ObObj vec_obj;
          ObString blob_data;
          const int64_t col_idx = 0;

          if (OB_FAIL(result->get_obj(col_idx, vec_obj))) {
            LOG_WARN("failed to get vector object", K(ret));
          } else if (vec_obj.is_null()) {
            continue;
          } else if (FALSE_IT(blob_data = vec_obj.get_string())) {
          } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
                      &tmp_alloc,
                      ObLongTextType,
                      CS_TYPE_BINARY,
                      true,
                      blob_data))) {
            LOG_WARN("fail to get real data", K(ret), K(blob_data));
          } else if (blob_data.length() < sizeof(uint32_t)) {
            continue;
          } else {
            // Sparse vector format: first 4 bytes is length, followed by (key, value) pairs
            uint32_t length = *(uint32_t *)(blob_data.ptr());
            total_length += length;
            valid_sample_count++;
          }
        }

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (valid_sample_count > 0) {
        avg_sparse_length = static_cast<uint32_t>(total_length / valid_sample_count);
        LOG_INFO("sample sparse vectors and calculate average length",
                 K(TARGET_SAMPLE_COUNT), K(valid_sample_count), K(avg_sparse_length),
                 K(num_vectors), K(sample_percent));
      } else {
        avg_sparse_length = 0;
        LOG_WARN("no valid samples found, use default avg_sparse_length", K(TARGET_SAMPLE_COUNT));
      }
    }
  }

  return ret;
}

// Parse query_timeout(N) hint from SQL string; return N (us) or 0 if not found/invalid.
static int64_t parse_query_timeout_hint_us(const ObString &sql)
{
  int64_t val = 0;
  if (sql.empty() || sql.length() < 14) { return 0; }
  const char *p = sql.ptr();
  const char *end = sql.ptr() + sql.length();
  const char *needle = "query_timeout";
  const size_t needle_len = 12;
  while (p + needle_len <= end) {
    if (0 == strncasecmp(p, needle, needle_len)) {
      p += needle_len;
      while (p < end && (*p == ' ' || *p == '\t')) { ++p; }
      if (p < end && *p == '(') {
        ++p;
        while (p < end && (*p == ' ' || *p == '\t')) { ++p; }
        if (p < end && *p >= '0' && *p <= '9') {
          val = 0;
          while (p < end && *p >= '0' && *p <= '9') {
            val = val * 10 + (*p - '0');
            ++p;
          }
          return val;
        }
      }
      break;
    }
    ++p;
  }
  return 0;
}

/*
FUNCTION query_recall (
  IN     sql_query         LONGTEXT)
RETURN FLOAT;
*/
int ObDBMSVectorMySql::query_recall(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  double recall_rate = 0.0;

  CK(OB_LIKELY(1 == params.count()));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!params.at(0).is_text()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for query_recall", KR(ret));
  } else {
    // Single string parameter - SQL query mode
    ObString sql_query;
    if (OB_FAIL(params.at(0).get_string(sql_query))) {
      LOG_WARN("fail to get sql query string", K(ret));
    } else if (OB_FAIL(calculate_recall_rate_sql(ctx, sql_query, recall_rate))) {
      LOG_WARN("fail to calculate recall rate from sql", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    result.set_float(recall_rate);
    result.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  }

  return ret;
}

int ObDBMSVectorMySql::calculate_recall_rate_sql(ObPLExecCtx &ctx,
                                                  const ObString &sql_query,
                                                  double &recall_rate)
{
  int ret = OB_SUCCESS;
  recall_rate = 0.0;

  ObMySQLProxy *sql_proxy = nullptr;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  if (OB_FAIL(check_sql_valid(sql_query))) {
    LOG_WARN("sql query validation failed", K(ret), K(sql_query));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is null", K(ret));
  } else {
    tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();

    // Get current database name
    ObString database_name = ctx.exec_ctx_->get_my_session()->get_database_name();
    if (database_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get database name", K(ret));
    } else {
      ObArenaAllocator tmp_alloc("VecIdxRecall", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObString table_name;
      ObString db_name;
      const share::schema::ObTableSchema *table_schema = nullptr;
      share::schema::ObSchemaGetterGuard schema_guard;

      // Step 1: Parse table name from SQL query
      if (OB_FAIL(ObVectorRecallCalcUtil::parse_table_name_from_sql(
                    sql_query, database_name, &tmp_alloc, table_name, db_name))) {
        LOG_WARN("fail to parse table name from sql", K(ret));
      }
      // Step 2: Get table schema by name
      else if (OB_FAIL(ObVectorRecallCalcUtil::get_table_schema_by_name(
                         tenant_id, db_name, table_name, table_schema, schema_guard))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(db_name), K(table_name));
      } else if (OB_FAIL(check_table_select_privilege(ctx.exec_ctx_->get_my_session(), schema_guard, db_name, table_name))) {
        LOG_WARN("fail to check table select privilege", K(ret), K(db_name), K(table_name));
      } else {
        LOG_INFO("get table schema from sql success", K(table_name), K(db_name));

        // Step 3: Build modified SQL with primary key columns
        ObSqlString modified_approx_sql;
        if (OB_FAIL(ObVectorRecallCalcUtil::build_pk_select_sql(
                      sql_query, table_schema, db_name, table_name, modified_approx_sql))) {
          LOG_WARN("fail to build pk select sql", K(ret));
        } else {
          // Step 3b: Inject session-level search params (ob_ivf_nprobes, ob_hnsw_ef_search) into the SQL
          ObSqlString approx_sql_with_params;
          sql::ObSQLSessionInfo *session = ctx.exec_ctx_->get_my_session();
          uint64_t session_nprobes = 0;
          uint64_t session_ef_search = 0;
          if (OB_NOT_NULL(session)) {
            if (OB_FAIL(session->get_ob_ivf_nprobes(session_nprobes))) {
              LOG_WARN("fail to get ob_ivf_nprobes from session", K(ret));
            } else if (OB_FAIL(session->get_ob_hnsw_ef_search(session_ef_search))) {
              LOG_WARN("fail to get ob_hnsw_ef_search from session", K(ret));
            } else if (OB_FAIL(ObVectorRecallCalcUtil::inject_session_params_into_approx_sql(
                           ObString(modified_approx_sql.length(), modified_approx_sql.ptr()),
                           session_nprobes, session_ef_search, &tmp_alloc, approx_sql_with_params))) {
              LOG_WARN("fail to inject session params into approx sql", K(ret));
            }
          }
          const ObSqlString &approx_sql_to_exec = approx_sql_with_params.length() > 0 ? approx_sql_with_params : modified_approx_sql;
          CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_VECTOR_LOW);
          // Step 4: Execute approximate search SQL
          const ObRowkeyInfo *rowkey_info_ptr = &(table_schema->get_rowkey_info());
          ObVectorSearchResult approx_result;
          if (OB_FAIL(ObVectorRecallCalcUtil::execute_search_sql(
                        tenant_id, approx_sql_to_exec, sql_proxy,
                        rowkey_info_ptr, &tmp_alloc, approx_result, GET_GROUP_ID()))) {
            LOG_WARN("fail to execute approximate search sql", K(ret), K(tenant_id));
            if (OB_TIMEOUT == ret) {
              int64_t hint_us = parse_query_timeout_hint_us(sql_query);
              if (hint_us > 0) {
                LOG_USER_ERROR(OB_TIMEOUT, hint_us);
              }
            }
          } else {
            // Step 5: Build brute-force search SQL by removing APPROXIMATE
            ObString modified_sql_str(approx_sql_to_exec.length(), approx_sql_to_exec.ptr());
            ObSqlString brute_sql;
            if (OB_FAIL(build_brute_force_sql_from_approximate(modified_sql_str, brute_sql))) {
              LOG_WARN("fail to build brute force sql", K(ret));
            } else {
              // Step 6: Execute brute-force search SQL
              ObVectorSearchResult brute_result;
              if (OB_FAIL(ObVectorRecallCalcUtil::execute_search_sql(
                            tenant_id, brute_sql, sql_proxy,
                            rowkey_info_ptr, &tmp_alloc, brute_result, GET_GROUP_ID()))) {
                LOG_WARN("fail to execute brute force search sql", K(ret), K(tenant_id));
                if (OB_TIMEOUT == ret) {
                  int64_t hint_us = parse_query_timeout_hint_us(sql_query);
                  if (hint_us > 0) {
                    LOG_USER_ERROR(OB_TIMEOUT, hint_us);
                  }
                }
              } else {
                // Step 7: Calculate recall rate
                if (OB_FAIL(ObVectorRecallCalcUtil::calculate_recall_rate(
                              approx_result, brute_result, recall_rate))) {
                  LOG_WARN("fail to calculate recall rate", K(ret));
                } else {
                  LOG_INFO("calculate recall rate success", K(recall_rate),
                           "approx_count", approx_result.rowkeys_.count(),
                           "brute_count", brute_result.rowkeys_.count());
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

/*
FUNCTION index_recall (
  IN     table_name        VARCHAR(65535),
  IN     index_name        VARCHAR(65535),
  IN     vectors           LONGTEXT DEFAULT NULL,
  IN     num_vectors       INT DEFAULT 25,
  IN     top_k             INT DEFAULT 10,
  IN     filter            LONGTEXT DEFAULT NULL,
  IN     parallel          INT DEFAULT 1,
  IN     parameters        VARCHAR(65535) DEFAULT NULL)
RETURN FLOAT;
*/
int ObDBMSVectorMySql::index_recall(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  double avg_recall_rate = 0.0;

  CK(OB_LIKELY(8 == params.count()));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!params.at(0).is_varchar() || !params.at(1).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: table_name and index_name must be varchar", KR(ret));
  } else if (!params.at(2).is_null() && !params.at(2).is_text()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: vectors must be text or null", KR(ret));
  } else if (!params.at(3).is_null() && !params.at(3).is_int32()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: num_vectors must be int or null", KR(ret));
  } else if (!params.at(4).is_int32()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: top_k must be int", KR(ret));
  } else if (!params.at(5).is_null() && !params.at(5).is_text()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: filter must be text or null", KR(ret));
  } else if (!params.at(6).is_null() && !params.at(6).is_int32()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: parallel must be int or null", KR(ret));
  } else if (!params.at(7).is_null() && !params.at(7).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: parameters must be varchar or null", KR(ret));
  } else {
    ObString param_table_name = params.at(0).get_varchar();
    ObString index_name = params.at(1).get_varchar();
    ObString vectors_str;
    bool vectors_provided = false;
    int64_t num_vectors = DEFAULT_NUM_VECTORS; // default value
    if (!params.at(3).is_null()) {
      num_vectors = params.at(3).get_int32();
    }
    int64_t top_k = params.at(4).get_int32();
    ObString filter_str;
    int64_t parallel = 1; // default value
    if (!params.at(6).is_null()) {
      parallel = params.at(6).get_int32();
    }
    ObString parameters_str;

    // Get vectors string if provided
    if (params.at(2).is_text()) {
      vectors_provided = true;
      if (OB_FAIL(params.at(2).get_string(vectors_str))) {
        LOG_WARN("fail to get vectors string", K(ret));
      }
    }

    // Get filter string if provided
    if (OB_SUCC(ret) && params.at(5).is_text()) {
      if (OB_FAIL(params.at(5).get_string(filter_str))) {
        LOG_WARN("fail to get filter string", K(ret));
      }
    }

    // Get parameters string if provided
    if (OB_SUCC(ret) && params.at(7).is_varchar()) {
      parameters_str = params.at(7).get_varchar();
    }

    if (OB_SUCC(ret)) {
      ObArenaAllocator tmp_alloc("VecIdxRecall", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      sql::ObSQLSessionInfo *session_info = nullptr;
      ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
      ObCollationType cs_type = CS_TYPE_INVALID;
      ObString database_name, table_name;

      if (OB_ISNULL(ctx.exec_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec context is null", KR(ret));
      } else if (OB_ISNULL(session_info = ctx.exec_ctx_->get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session info is null", KR(ret));
      } else if (OB_FAIL(session_info->get_name_case_mode(case_mode))) {
        LOG_WARN("fail to get name case mode", KR(ret));
      } else if (OB_FAIL(session_info->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation_connection", KR(ret));
      } else if (OB_FAIL(ObVectorRefreshIndexExecutor::resolve_table_name(
                    cs_type, case_mode, lib::is_oracle_mode(), param_table_name,
                    database_name, table_name))) {
        LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(param_table_name));
      } else if (database_name.empty() && FALSE_IT(database_name = session_info->get_database_name())) {
      } else if (OB_UNLIKELY(database_name.empty())) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("no database selected", KR(ret));
      } else {
        // Get vector index info
        const share::schema::ObTableSchema *table_schema = nullptr;
        const share::schema::ObTableSchema *index_schema = nullptr;
        ObString vector_column;
        share::ObVectorIndexDistAlgorithm dist_algo = share::VIDA_MAX;
        share::schema::ObSchemaGetterGuard schema_guard;

        if (OB_FAIL(get_vector_index_info(ctx, table_name, database_name, index_name,
                                          table_schema, index_schema, vector_column,
                                          dist_algo, schema_guard))) {
          LOG_WARN("fail to get vector index info", K(ret), K(table_name), K(database_name), K(index_name));
        } else if (OB_FAIL(check_table_select_privilege(session_info, schema_guard, database_name, table_name))) {
          LOG_WARN("fail to check table select privilege", K(ret), K(database_name), K(table_name));
        } else {
          // do nothing
        }

        const char *dist_func_name = nullptr;
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(get_distance_func_name(dist_algo, dist_func_name))) {
          LOG_WARN("fail to get distance function name", K(ret), K(dist_algo));
        } else {
          // Get or sample vectors
          ObSEArray<ObString, 32> vector_list;
          const int64_t MIN_VECTORS = 1;
          const int64_t MAX_VECTORS = 50;
          if (vectors_provided && vectors_str.empty()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("vectors string is empty", K(ret));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "vectors string cannot be empty");
          } else if (!vectors_str.empty()) {
            // Parse user-provided vectors
            if (OB_FAIL(parse_vectors_string(vectors_str, &tmp_alloc, vector_list))) {
              LOG_WARN("fail to parse vectors string", K(ret));
            } else if (vector_list.count() < MIN_VECTORS || vector_list.count() > MAX_VECTORS) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("vectors count out of range, must be [1, 50]",
                       K(ret), K(vector_list.count()), K(MIN_VECTORS), K(MAX_VECTORS));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "vectors count, must be between 1 and 50");
            }
          } else {
            // Validate num_vectors range when vectors not provided
            if (num_vectors < MIN_VECTORS || num_vectors > MAX_VECTORS) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("num_vectors out of range, must be [1, 50]",
                       K(ret), K(num_vectors), K(MIN_VECTORS), K(MAX_VECTORS));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "num_vectors, must be between 1 and 50");
            } else if (OB_FAIL(sample_vectors_from_table(ctx, table_name, database_name, vector_column,
                                                  num_vectors, &tmp_alloc, vector_list))) {
              // Sample vectors from table
              LOG_WARN("fail to sample vectors from table", K(ret), K(table_name), K(vector_column), K(num_vectors));
            }
          }

          if (OB_SUCC(ret) && vector_list.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no vectors to calculate recall rate", K(ret));
          }

          ObSqlString merged_parameters_str;
          ObString parameters_to_use = parameters_str;
          if (OB_SUCC(ret) && OB_FAIL(merge_session_search_params(session_info, index_schema,
                                                                   parameters_str, merged_parameters_str))) {
            LOG_WARN("fail to merge session search params", K(ret));
          } else if (OB_SUCC(ret) && merged_parameters_str.length() > 0) {
            parameters_to_use.assign(merged_parameters_str.ptr(), merged_parameters_str.length());
          }

          // Calculate recall rate for each vector
          if (OB_SUCC(ret)) {
            double total_recall = 0.0;
            int64_t success_count = 0;

            // Build full table name with database prefix
            ObSqlString full_table_name;
            if (OB_FAIL(full_table_name.append_fmt("%.*s.%.*s",
                database_name.length(), database_name.ptr(),
                table_name.length(), table_name.ptr()))) {
              LOG_WARN("fail to build full table name", K(ret));
            }

            for (int64_t i = 0; OB_SUCC(ret) && i < vector_list.count(); ++i) {
              // Check if query is killed (cancelled)
              if (OB_UNLIKELY(session_info->is_query_killed())) {
                ret = OB_ERR_QUERY_INTERRUPTED;
                LOG_WARN("query is killed during recall calculation", K(ret), K(i), K(vector_list.count()));
                LOG_USER_ERROR(OB_ERR_QUERY_INTERRUPTED, "query is killed during recall calculation");
              } else {
                double single_recall = 0.0;
                ObString full_table_str(full_table_name.length(), full_table_name.ptr());
                if (OB_FAIL(calculate_single_vector_recall(ctx, full_table_str,
                                                           vector_column, vector_list.at(i),
                                                           dist_func_name, top_k, filter_str,
                                                           table_schema, parallel, parameters_to_use,
                                                           single_recall))) {
                  LOG_WARN("fail to calculate single vector recall", K(ret), K(i), K(vector_list.at(i)));
                  // Do not continue if any vector calculation fails
                } else {
                  total_recall += single_recall;
                  success_count++;
                  LOG_DEBUG("single vector recall rate", K(i), K(single_recall), K(vector_list.at(i)));
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (success_count == 0) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("no successful recall rate calculation", K(ret));
              } else {
                avg_recall_rate = total_recall / static_cast<double>(success_count);
                LOG_INFO("calculate recall rate by index success",
                         K(avg_recall_rate), K(success_count), K(vector_list.count()),
                         K(table_name), K(index_name));
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.set_float(avg_recall_rate);
    result.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  }

  return ret;
}

int ObDBMSVectorMySql::get_distance_func_name(share::ObVectorIndexDistAlgorithm dist_algo,
                                              const char *&func_name)
{
  int ret = OB_SUCCESS;
  switch (dist_algo) {
    case share::VIDA_L2:
      func_name = "l2_distance";
      break;
    case share::VIDA_IP:
      func_name = "negative_inner_product";
      break;
    case share::VIDA_COS:
      func_name = "cosine_distance";
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid distance algorithm", K(ret), K(dist_algo));
  }
  return ret;
}

int ObDBMSVectorMySql::parse_vectors_string(const ObString &vectors_str,
                                            ObIAllocator *allocator,
                                            ObIArray<ObString> &vector_list)
{
  int ret = OB_SUCCESS;
  vector_list.reset();

  if (vectors_str.empty() || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(vectors_str), KP(allocator));
  } else {
    // Parse vectors separated by "],["
    // Format: "[0.1,0.2,0.3],[0.4,0.5,0.6]"
    const char *ptr = vectors_str.ptr();
    const char *end = ptr + vectors_str.length();
    const char *vec_start = nullptr;
    int bracket_depth = 0;

    for (const char *p = ptr; p < end && OB_SUCC(ret); ++p) {
      if (*p == '[') {
        if (bracket_depth == 0) {
          vec_start = p;
        }
        bracket_depth++;
      } else if (*p == ']') {
        bracket_depth--;
        if (bracket_depth == 0 && vec_start != nullptr) {
          // Found a complete vector
          int64_t vec_len = p - vec_start + 1;
          char *vec_buf = static_cast<char*>(allocator->alloc(vec_len));
          if (OB_ISNULL(vec_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for vector", K(ret), K(vec_len));
          } else {
            MEMCPY(vec_buf, vec_start, vec_len);
            ObString vec_str(vec_len, vec_buf);
            if (OB_FAIL(vector_list.push_back(vec_str))) {
              LOG_WARN("fail to push vector to list", K(ret));
            }
          }
          vec_start = nullptr;
        }
      }
    }

    if (OB_SUCC(ret) && vector_list.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no valid vectors found in string", K(ret));
    }

    LOG_DEBUG("parse vectors string", K(ret), K(vector_list.count()));
  }

  return ret;
}

int ObDBMSVectorMySql::sample_vectors_from_table(ObPLExecCtx &ctx,
                                                 const ObString &table_name,
                                                 const ObString &db_name,
                                                 const ObString &vector_column,
                                                 int64_t num_vectors,
                                                 ObIAllocator *allocator,
                                                 ObIArray<ObString> &vector_list)
{
  int ret = OB_SUCCESS;
  vector_list.reset();

  if (table_name.empty() || db_name.empty() || vector_column.empty() ||
      num_vectors <= 0 || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_name), K(db_name),
             K(vector_column), K(num_vectors), KP(allocator));
  } else {
    ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;

    if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", K(ret));
    } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec context or session is null", K(ret));
    } else {
      tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();

      // Step 1: Get total row count from information_schema to calculate sampling percentage
      int64_t total_rows = 0;
      {
        SMART_VAR(ObMySQLProxy::MySQLResult, count_res) {
          sqlclient::ObMySQLResult *count_result = nullptr;
          ObSqlString count_sql;
          if (OB_FAIL(count_sql.append_fmt(
                "SELECT CAST(table_rows AS UNSIGNED) AS total_rows "
                "FROM information_schema.TABLES "
                "WHERE table_schema='%.*s' AND table_name='%.*s'",
                db_name.length(), db_name.ptr(),
                table_name.length(), table_name.ptr()))) {
            LOG_WARN("fail to build count sql", K(ret));
          } else if (OB_FAIL(sql_proxy->read(count_res, tenant_id, count_sql.ptr()))) {
            LOG_WARN("fail to execute count sql", K(ret), K(tenant_id), K(count_sql));
          } else if (OB_ISNULL(count_result = count_res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("count result is null", K(ret));
          } else if (OB_FAIL(count_result->next())) {
            LOG_WARN("fail to get count result", K(ret));
          } else {
            ObObj count_obj;
            if (OB_FAIL(count_result->get_obj(0L, count_obj))) {
              LOG_WARN("fail to get count object", K(ret));
            } else if (!count_obj.is_null() && count_obj.is_integer_type()) {
              total_rows = count_obj.get_int();
            }
          }
        }
      }

      // Step 2: Build sample SQL using SAMPLE clause for better performance
      ObSqlString sample_sql;
      if (OB_FAIL(ret)) {
        // Error already logged above
      } else if (total_rows <= 0) {
        // Fallback to LIMIT if we can't get row count
        LOG_INFO("unable to get table row count, using simple LIMIT", K(table_name), K(db_name));
        if (OB_FAIL(sample_sql.append_fmt(
              "SELECT `%.*s` FROM `%.*s`.`%.*s` LIMIT %ld",
              vector_column.length(), vector_column.ptr(),
              db_name.length(), db_name.ptr(),
              table_name.length(), table_name.ptr(),
              num_vectors))) {
          LOG_WARN("fail to build sample sql", K(ret));
        }
      } else {
        // Calculate sampling percentage with buffer factor (150) to ensure enough samples
        const int num_sample = 150;
        double percentage = (static_cast<double>(num_sample) / total_rows) * 100.0;

        // Clamp percentage to valid range [0.000001, 100)
        if (percentage < 0.000001) {
          percentage = 0.000001;
        } else if (percentage >= 100.0) {
          percentage = 99.999999;
        }

        if (percentage >= 99.0) {
          // If we need almost all data, just use LIMIT without SAMPLE
          if (OB_FAIL(sample_sql.append_fmt(
                "SELECT `%.*s` FROM `%.*s`.`%.*s` LIMIT %ld",
                vector_column.length(), vector_column.ptr(),
                db_name.length(), db_name.ptr(),
                table_name.length(), table_name.ptr(),
                num_vectors))) {
            LOG_WARN("fail to build sample sql", K(ret));
          }
        } else {
          // Use SAMPLE clause for efficient sampling
          if (OB_FAIL(sample_sql.append_fmt(
                "SELECT `%.*s` FROM `%.*s`.`%.*s` SAMPLE(%lf) LIMIT %ld",
                vector_column.length(), vector_column.ptr(),
                db_name.length(), db_name.ptr(),
                table_name.length(), table_name.ptr(),
                percentage,
                num_vectors))) {
            LOG_WARN("fail to build sample sql", K(ret));
          }
        }

        LOG_DEBUG("using SAMPLE clause for efficient sampling",
                  K(total_rows), K(num_vectors), K(percentage), K(sample_sql));
      }

      if (OB_SUCC(ret)) {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          sqlclient::ObMySQLResult *mysql_result = nullptr;
          if (OB_FAIL(sql_proxy->read(res, tenant_id, sample_sql.ptr()))) {
            LOG_WARN("fail to execute sample sql", K(ret), K(tenant_id), K(sample_sql));
          } else if (OB_ISNULL(mysql_result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mysql result is null", K(ret));
          } else {
            while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
              ObObj vec_obj;
              ObString blob_data;
              if (OB_FAIL(mysql_result->get_obj(0L, vec_obj))) {
                LOG_WARN("fail to get vector object from result", K(ret));
              } else if (vec_obj.is_null()) {
                // Skip null vectors
                continue;
              } else if (FALSE_IT(blob_data = vec_obj.get_string())) {
              } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(allocator,
                                                                            ObLongTextType,
                                                                            CS_TYPE_BINARY,
                                                                            true,
                                                                            blob_data))) {
                LOG_WARN("fail to get real data.", K(ret));
              } else if (blob_data.empty()) {
                // Skip empty vectors
                continue;
              } else {
                // Convert binary vector data to text format [0.1, 0.2, 0.3]
                int64_t dim = blob_data.length() / sizeof(float);
                if (dim <= 0) {
                  LOG_WARN("invalid vector dimension", K(ret), K(blob_data.length()));
                  continue;
                }
                const float *float_data = reinterpret_cast<const float*>(blob_data.ptr());

                // Estimate buffer size: "[" + dim * (max_float_len + ", ") + "]"
                // max float string length is about 20 chars
                int64_t buf_size = 2 + dim * 22;
                char *vec_buf = static_cast<char*>(allocator->alloc(buf_size));
                if (OB_ISNULL(vec_buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to allocate memory for vector string", K(ret), K(buf_size));
                } else {
                  // Build vector string in format [0.1, 0.2, 0.3]
                  int64_t pos = 0;
                  vec_buf[pos++] = '[';
                  for (int64_t i = 0; i < dim && pos < buf_size - 1; ++i) {
                    if (i > 0) {
                      vec_buf[pos++] = ',';
                    }
                    int64_t len = snprintf(vec_buf + pos, buf_size - pos, "%g", float_data[i]);
                    if (len > 0) {
                      pos += len;
                    }
                  }
                  vec_buf[pos++] = ']';
                  vec_buf[pos] = '\0';

                  ObString vec_str(pos, vec_buf);
                  if (OB_FAIL(vector_list.push_back(vec_str))) {
                    LOG_WARN("fail to push vector to list", K(ret));
                  }
                }
              }
            }

            if (ret == OB_ITER_END) {
              ret = OB_SUCCESS;
            }

            if (OB_SUCC(ret) && vector_list.empty()) {
              ret = OB_EMPTY_RESULT;
              LOG_WARN("no vectors sampled from table, table may be empty", K(ret), K(table_name));
            }

            LOG_DEBUG("sample vectors from table", K(ret), K(table_name),
                      K(vector_column), K(vector_list.count()), K(num_vectors));
          }
        }
      }
    }
  }

  return ret;
}

int ObDBMSVectorMySql::get_vector_index_info(ObPLExecCtx &ctx,
                                             const ObString &table_name,
                                             const ObString &db_name,
                                             const ObString &index_name,
                                             const share::schema::ObTableSchema *&table_schema,
                                             const share::schema::ObTableSchema *&index_schema,
                                             ObString &vector_column,
                                             share::ObVectorIndexDistAlgorithm &dist_algo,
                                             share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  table_schema = nullptr;
  index_schema = nullptr;
  dist_algo = share::VIDA_MAX;

  if (table_name.empty() || db_name.empty() || index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_name), K(db_name), K(index_name));
  } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context or session is null", K(ret));
  } else {
    uint64_t tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
    uint64_t database_id = OB_INVALID_ID;

    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, db_name, database_id))) {
      LOG_WARN("fail to get database id", K(ret), K(tenant_id), K(db_name));
    } else if (OB_INVALID_ID == database_id) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database not exist", K(ret), K(db_name));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, database_id, table_name,
                                                     false/*is_index*/, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(table_name));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(db_name), K(table_name));
    } else {
      // Get index schema by finding the matching index from table's index list
      ObSEArray<const ObSimpleTableSchemaV2 *, 16> index_schemas;
      if (OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(tenant_id,
                                                                   table_schema->get_table_id(),
                                                                   index_schemas))) {
        LOG_WARN("fail to get index schemas for table", K(ret), K(tenant_id), K(table_schema->get_table_id()));
      } else {
        bool found = false;
        for (int64_t i = 0; OB_SUCC(ret) && !found && i < index_schemas.count(); ++i) {
          const ObSimpleTableSchemaV2 *curr_index_schema = index_schemas.at(i);
          if (OB_ISNULL(curr_index_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index schema is null", K(ret), K(i));
          } else {
            // Get the logical index name from physical index name
            ObString logical_index_name;
            if (OB_FAIL(curr_index_schema->get_index_name(logical_index_name))) {
              LOG_WARN("fail to get logical index name", K(ret), K(curr_index_schema->get_table_name()));
            } else if (ObCharset::case_insensitive_equal(index_name, logical_index_name)) {
              // Found matching index
              if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                       curr_index_schema->get_table_id(),
                                                       index_schema))) {
                LOG_WARN("fail to get full index schema", K(ret), K(tenant_id), K(curr_index_schema->get_table_id()));
              } else {
                found = true;
              }
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_ERR_KEY_DOES_NOT_EXISTS;
          LOG_WARN("index not exist", K(ret), K(index_name), K(table_name));
        } else if (OB_SUCC(ret) && !index_schema->is_vec_index()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index is not a vector index", K(ret), K(index_name));
        } else if (OB_SUCC(ret)) {
          // Get vector column name
          ObSEArray<ObString, 1> col_names;
          if (OB_FAIL(share::ObVectorIndexUtil::get_vector_index_column_name(
                        *table_schema, *index_schema, col_names))) {
            LOG_WARN("fail to get vector index column name", K(ret), K(index_name));
          } else if (col_names.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no vector column found for index", K(ret), K(index_name));
          } else {
            vector_column = col_names.at(0);

            // Get distance algorithm from index params
            share::ObVectorIndexType index_type = share::VIT_MAX;
            if(OB_FAIL(ret)){
              // do nothing
            } else if (index_schema->is_vec_ivf_index()) {
              index_type = share::VIT_IVF_INDEX;
            } else if (index_schema->is_vec_hnsw_index()) {
              index_type = share::VIT_HNSW_INDEX;
            } else if (index_schema->is_vec_spiv_index()) {
              index_type = share::VIT_SPIV_INDEX;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid index type", K(ret), K(index_name));
            }

            share::ObVectorIndexParam index_param;
            if (OB_FAIL(ret)){
              // do nothing
            } else if (OB_FAIL(share::ObVectorIndexUtil::parser_params_from_string(
                          index_schema->get_index_params(), index_type, index_param))) {
              LOG_WARN("fail to parse index params", K(ret), K(index_schema->get_index_params()));
            } else {
              dist_algo = index_param.dist_algorithm_;
              if (dist_algo == share::VIDA_MAX) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("distance algorithm not specified in index params", K(ret), K(index_name));
              } else {
                LOG_DEBUG("get vector index info success", K(table_name), K(index_name),
                          K(vector_column), K(dist_algo));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDBMSVectorMySql::merge_session_search_params(sql::ObSQLSessionInfo *session_info,
                                        const share::schema::ObTableSchema *index_schema,
                                        const ObString &parameters_str,
                                        ObSqlString &merged_parameters)
{
  int ret = OB_SUCCESS;
  merged_parameters.reset();
  if (OB_ISNULL(session_info) || OB_ISNULL(index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session or index_schema is null", K(ret));
    return ret;
  }
  uint64_t session_nprobes = 0;
  uint64_t session_ef_search = 0;
  if (OB_FAIL(session_info->get_ob_ivf_nprobes(session_nprobes))) {
    LOG_WARN("fail to get ob_ivf_nprobes from session", K(ret));
    return ret;
  }
  if (OB_FAIL(session_info->get_ob_hnsw_ef_search(session_ef_search))) {
    LOG_WARN("fail to get ob_hnsw_ef_search from session", K(ret));
    return ret;
  }
  const bool is_ivf = index_schema->is_vec_ivf_index();
  const bool is_hnsw = index_schema->is_vec_hnsw_index();
  const char *key = is_ivf ? "IVF_NPROBES" : (is_hnsw ? "EF_SEARCH" : nullptr);
  const uint64_t value = is_ivf ? session_nprobes : (is_hnsw ? session_ef_search : 0);
  if (OB_ISNULL(key) || value == 0) {
    if (!parameters_str.empty() && OB_FAIL(merged_parameters.assign(parameters_str.ptr(), parameters_str.length()))) {
      LOG_WARN("fail to assign parameters", K(ret));
    }
    return ret;
  }
  if (parameters_str.empty()) {
    if (OB_FAIL(merged_parameters.append_fmt("%s=%lu", key, value))) {
      LOG_WARN("fail to append session param", K(ret), K(key));
    }
    return ret;
  }

  ObString rest(parameters_str);
  bool found_key = false;
  const size_t key_len = strlen(key);
  while (!rest.empty() && OB_SUCC(ret)) {
    int32_t rest_len = rest.length();
    const char *rest_ptr = rest.ptr();
    int32_t comma_offset = -1;
    for (int32_t k = 0; k < rest_len; ++k) {
      if (rest_ptr[k] == ',') {
        comma_offset = k;
        break;
      }
    }
    ObString token;
    if (comma_offset >= 0) {
      token.assign_ptr(rest_ptr, comma_offset);
      rest.assign_ptr(rest_ptr + comma_offset + 1, rest_len - comma_offset - 1);
    } else {
      token = rest;
      rest.reset();
    }
    int32_t tl = token.length();
    int32_t trim_start = 0;
    while (trim_start < tl) {
      unsigned char c = static_cast<unsigned char>(token.ptr()[trim_start]);
      if (c != ' ' && c != '\t') break;
      ++trim_start;
    }
    if (trim_start >= tl) continue;
    token.assign_ptr(token.ptr() + trim_start, tl - trim_start);
    tl = token.length();
    if (tl == 0) continue;
    const char *tp = token.ptr();
    // Allow optional spaces between key and '=' (e.g. "ef_search = 200")
    int32_t eq_pos = key_len;
    while (eq_pos < tl && (tp[eq_pos] == ' ' || tp[eq_pos] == '\t')) {
      ++eq_pos;
    }
    bool is_our_key = (tl > key_len &&
                       strncasecmp(tp, key, key_len) == 0 &&
                       eq_pos < tl && tp[eq_pos] == '=');
    if (is_our_key) {
      found_key = true;
      // Keep user's value, do not replace
      if (merged_parameters.length() > 0 && OB_FAIL(merged_parameters.append(","))) return ret;
      if (OB_FAIL(merged_parameters.append(token.ptr(), token.length()))) return ret;
    } else {
      if (merged_parameters.length() > 0 && OB_FAIL(merged_parameters.append(","))) return ret;
      if (OB_FAIL(merged_parameters.append(token.ptr(), token.length()))) return ret;
    }
  }
  if (OB_SUCC(ret) && !found_key) {
    if (merged_parameters.length() > 0 && OB_FAIL(merged_parameters.append(","))) return ret;
    if (OB_FAIL(merged_parameters.append_fmt("%s=%lu", key, value))) return ret;
  }
  return ret;
}

int ObDBMSVectorMySql::calculate_single_vector_recall(ObPLExecCtx &ctx,
                                                      const ObString &table_name,
                                                      const ObString &vector_column,
                                                      const ObString &query_vector,
                                                      const char *dist_func_name,
                                                      int64_t top_k,
                                                      const ObString &filter,
                                                      const share::schema::ObTableSchema *table_schema,
                                                      int64_t parallel,
                                                      const ObString &parameters,
                                                      double &recall_rate)
{
  int ret = OB_SUCCESS;
  recall_rate = 0.0;

  if (table_name.empty() || vector_column.empty() ||
      query_vector.empty() || OB_ISNULL(dist_func_name) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_name),
             K(vector_column), KP(dist_func_name), KP(table_schema));
  } else {
    ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObArenaAllocator tmp_alloc("VecIdxRecall", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

    if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", K(ret));
    } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec context or session is null", K(ret));
    } else {
      tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();

      // Build primary key columns list
      const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
      const int64_t rowkey_cnt = rowkey_info.get_size();
      ObSqlString pk_cols_str;

      if (rowkey_cnt > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
          uint64_t column_id = OB_INVALID_ID;
          if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
            LOG_WARN("fail to get column id", K(ret), K(i));
          } else {
            const share::schema::ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(column_id);
            if (OB_ISNULL(column_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema is null", K(ret), K(column_id));
            } else {
              ObString col_name = column_schema->get_column_name_str();
              if (i > 0 && OB_FAIL(pk_cols_str.append(", "))) {
                LOG_WARN("fail to append comma", K(ret));
              } else if (OB_FAIL(pk_cols_str.append_fmt("`%.*s`", col_name.length(), col_name.ptr()))) {
                LOG_WARN("fail to append column name", K(ret), K(col_name));
              }
            }
          }
        }
      } else {
        // No primary key, use hidden column
        if (OB_FAIL(pk_cols_str.append_fmt("`%s`", OB_HIDDEN_PK_INCREMENT_COLUMN_NAME))) {
          LOG_WARN("fail to append hidden pk column", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        // Build approximate search SQL
        ObSqlString approx_sql;
        if (rowkey_cnt > 0) {
          if (parallel >= 1) {
            if (OB_FAIL(approx_sql.append_fmt(
                  "SELECT /*+ parallel(%ld) */ %.*s FROM %.*s ",
                  parallel,
                  pk_cols_str.length(), pk_cols_str.ptr(),
                  table_name.length(), table_name.ptr()))) {
              LOG_WARN("fail to append select clause with parallel", K(ret));
            }
          } else {
            if (OB_FAIL(approx_sql.append_fmt(
                  "SELECT %.*s FROM %.*s ",
                  pk_cols_str.length(), pk_cols_str.ptr(),
                  table_name.length(), table_name.ptr()))) {
              LOG_WARN("fail to append select clause", K(ret));
            }
          }
        } else {
          if (parallel >= 1) {
            if (OB_FAIL(approx_sql.append_fmt(
                  "SELECT /*+ parallel(%ld) opt_param('hidden_column_visible','true') */ %.*s FROM %.*s ",
                  parallel,
                  pk_cols_str.length(), pk_cols_str.ptr(),
                  table_name.length(), table_name.ptr()))) {
              LOG_WARN("fail to append select clause with hidden column and parallel", K(ret));
            }
          } else {
            if (OB_FAIL(approx_sql.append_fmt(
                  "SELECT /*+ opt_param('hidden_column_visible','true') */ %.*s FROM %.*s ",
                  pk_cols_str.length(), pk_cols_str.ptr(),
                  table_name.length(), table_name.ptr()))) {
              LOG_WARN("fail to append select clause with hidden column", K(ret));
            }
          }
        }

        // Add filter if provided
        if (OB_SUCC(ret) && !filter.empty()) {
          if (OB_FAIL(approx_sql.append_fmt("WHERE %.*s ", filter.length(), filter.ptr()))) {
            LOG_WARN("fail to append filter", K(ret), K(filter));
          }
        }

        // Add ORDER BY with APPROXIMATE
        if (OB_SUCC(ret)) {
          if (!parameters.empty()) {
            // With PARAMETERS clause
            if (OB_FAIL(approx_sql.append_fmt(
                  "ORDER BY %s(`%.*s`, %.*s) APPROXIMATE LIMIT %ld PARAMETERS(%.*s)",
                  dist_func_name,
                  vector_column.length(), vector_column.ptr(),
                  query_vector.length(), query_vector.ptr(),
                  top_k,
                  parameters.length(), parameters.ptr()))) {
              LOG_WARN("fail to append order by clause with parameters", K(ret));
            }
          } else {
            // Without PARAMETERS clause
            if (OB_FAIL(approx_sql.append_fmt(
                  "ORDER BY %s(`%.*s`, %.*s) APPROXIMATE LIMIT %ld",
                  dist_func_name,
                  vector_column.length(), vector_column.ptr(),
                  query_vector.length(), query_vector.ptr(),
                  top_k))) {
              LOG_WARN("fail to append order by clause", K(ret));
            }
          }
        }

        // Build brute-force search SQL
        ObSqlString brute_sql;
        if (OB_SUCC(ret)) {
          if (rowkey_cnt > 0) {
            if (parallel >= 1) {
              if (OB_FAIL(brute_sql.append_fmt(
                    "SELECT /*+ parallel(%ld) */ %.*s FROM %.*s ",
                    parallel,
                    pk_cols_str.length(), pk_cols_str.ptr(),
                    table_name.length(), table_name.ptr()))) {
                LOG_WARN("fail to append select clause with parallel", K(ret));
              }
            } else {
              if (OB_FAIL(brute_sql.append_fmt(
                    "SELECT %.*s FROM %.*s ",
                    pk_cols_str.length(), pk_cols_str.ptr(),
                    table_name.length(), table_name.ptr()))) {
                LOG_WARN("fail to append select clause", K(ret));
              }
            }
          } else {
            if (parallel >= 1) {
              if (OB_FAIL(brute_sql.append_fmt(
                    "SELECT /*+ parallel(%ld) opt_param('hidden_column_visible','true') */ %.*s FROM %.*s ",
                    parallel,
                    pk_cols_str.length(), pk_cols_str.ptr(),
                    table_name.length(), table_name.ptr()))) {
                LOG_WARN("fail to append select clause with hidden column and parallel", K(ret));
              }
            } else {
              if (OB_FAIL(brute_sql.append_fmt(
                    "SELECT /*+ opt_param('hidden_column_visible','true') */ %.*s FROM %.*s ",
                    pk_cols_str.length(), pk_cols_str.ptr(),
                    table_name.length(), table_name.ptr()))) {
                LOG_WARN("fail to append select clause with hidden column", K(ret));
              }
            }
          }

          // Parse SIMILARITY from parameters for brute SQL WHERE condition
          double similarity_threshold = -1.0;
          const char *similarity_func = nullptr;
          if (OB_SUCC(ret) && !parameters.empty()) {
            similarity_threshold = parse_similarity_from_params(parameters.ptr(), parameters.length());
            if (similarity_threshold >= 0.0) {
              similarity_func = get_similarity_func_by_dist_name(dist_func_name);
            }
          }

          // Add WHERE: filter and/or similarity condition (e.g. cosine_similarity(embedding, @v) > 0.93)
          if (OB_SUCC(ret) && (!filter.empty() || (similarity_threshold >= 0.0 && OB_NOT_NULL(similarity_func)))) {
            if (OB_FAIL(brute_sql.append("WHERE "))) {
              LOG_WARN("fail to append WHERE", K(ret));
            } else if (!filter.empty() && OB_FAIL(brute_sql.append_fmt("%.*s ", filter.length(), filter.ptr()))) {
              LOG_WARN("fail to append filter", K(ret), K(filter));
            }
            if (OB_SUCC(ret) && !filter.empty() && similarity_threshold >= 0.0 && OB_NOT_NULL(similarity_func) &&
                OB_FAIL(brute_sql.append("AND "))) {
              LOG_WARN("fail to append AND", K(ret));
            }
            if (OB_SUCC(ret) && similarity_threshold >= 0.0 && OB_NOT_NULL(similarity_func) &&
                OB_FAIL(brute_sql.append_fmt("%s(`%.*s`, %.*s) > %f ",
                    similarity_func,
                    static_cast<int>(vector_column.length()), vector_column.ptr(),
                    static_cast<int>(query_vector.length()), query_vector.ptr(),
                    similarity_threshold))) {
              LOG_WARN("fail to append similarity condition", K(ret));
            }
          }

          // Add ORDER BY without APPROXIMATE
          if (OB_SUCC(ret)) {
            if (OB_FAIL(brute_sql.append_fmt(
                  "ORDER BY %s(`%.*s`, %.*s) LIMIT %ld",
                  dist_func_name,
                  vector_column.length(), vector_column.ptr(),
                  query_vector.length(), query_vector.ptr(),
                  top_k))) {
              LOG_WARN("fail to append order by clause", K(ret));
            }
          }
        }

        CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_VECTOR_LOW);
        // Execute both SQLs and calculate recall rate
        if (OB_SUCC(ret)) {
          const ObRowkeyInfo *rowkey_info_ptr = &(table_schema->get_rowkey_info());
          ObVectorSearchResult approx_result;
          ObVectorSearchResult brute_result;

          LOG_DEBUG("executing approximate search sql");
          if (OB_FAIL(ObVectorRecallCalcUtil::execute_search_sql(
                        tenant_id, approx_sql, sql_proxy,
                        rowkey_info_ptr, &tmp_alloc, approx_result, GET_GROUP_ID()))) {
            LOG_WARN("fail to execute approximate search sql", K(ret));
          } else {
            LOG_DEBUG("executing brute force search sql");
            if (OB_FAIL(ObVectorRecallCalcUtil::execute_search_sql(
                          tenant_id, brute_sql, sql_proxy,
                          rowkey_info_ptr, &tmp_alloc, brute_result, GET_GROUP_ID()))) {
              LOG_WARN("fail to execute brute force search sql", K(ret));
            } else if (OB_FAIL(ObVectorRecallCalcUtil::calculate_recall_rate(
                                 approx_result, brute_result, recall_rate))) {
              LOG_WARN("fail to calculate recall rate", K(ret));
            } else {
              LOG_DEBUG("calculate single vector recall success",
                       K(recall_rate),
                       "approx_count", approx_result.rowkeys_.count(),
                       "brute_count", brute_result.rowkeys_.count());
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDBMSVectorMySql::check_table_select_privilege(
    sql::ObSQLSessionInfo *session_info,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObString &database_name,
    const common::ObString &table_name)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    // Check user's SELECT privilege on the target table
    share::schema::ObSessionPrivInfo session_priv;
    const share::schema::ObNeedPriv need_priv(database_name, table_name, share::schema::OB_PRIV_TABLE_LEVEL, OB_PRIV_SELECT, false);
    if (OB_FAIL(session_info->get_session_priv_info(session_priv))) {
      LOG_WARN("fail to get session priv info", K(ret));
    } else if (OB_FAIL(schema_guard.check_single_table_priv(
                   session_priv,
                   session_info->get_enable_role_array(),
                   need_priv))) {
      LOG_WARN("fail to check table privilege", K(ret), K(database_name), K(table_name));
    }
  }

  return ret;
}

}
}
