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
      if (OB_FAIL(get_estimate_memory_str(index_param, num_vectors, max_tablet_vectors, res_buf))) {
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
      ObObj sum_result_obj;
      ObObj max_result_obj;
      const uint64_t tenant_id = exec_ctx->get_my_session()->get_effective_tenant_id();
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObSqlString query_string;
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(query_string.assign_fmt("SELECT cast(sum(table_rows) as unsigned) as sum, max(table_rows) as max from information_schema.PARTITIONS WHERE table_schema='%.*s' and table_name='%.*s'",
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
        } else if ((!sum_result_obj.is_null() && OB_UNLIKELY(!sum_result_obj.is_integer_type())) ||
                   (!max_result_obj.is_null() && OB_UNLIKELY(!max_result_obj.is_integer_type()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected obj type", K(ret), K(sum_result_obj.get_type()), K(max_result_obj.get_type()));
        } else if (!sum_result_obj.is_null() && OB_FALSE_IT(num_vectors = sum_result_obj.get_int())) {
        } else if (!max_result_obj.is_null() && OB_FALSE_IT(tablet_max_num_vectors = max_result_obj.get_int())) {
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
      ObStringBuffer res_buf(allocator);
      if (OB_FAIL(get_estimate_memory_str(index_param, num_vectors, tablet_max_num_vectors, res_buf))) {
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
      || idx_type_str.case_compare("SINDI") == 0) {
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
  } else if (OB_UNLIKELY(dim_count == 0 || dim_count > MAX_DIM_LIMITED)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("vector index dim equal to 0 or larger than 4096 is not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "vec index dim equal to 0 or larger than 4096 is");
  } else {
    index_param.dim_ = dim_count;
  }

  return ret;
}

int ObDBMSVectorMySql::get_estimate_memory_str(ObVectorIndexParam index_param,
                                               uint64_t num_vectors,
                                               uint64_t tablet_max_num_vectors,
                                               ObStringBuffer &res_buf)
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
      if (OB_FAIL(ObVectorIndexUtil::estimate_ivf_memory(num_vectors, index_param, construct_mem, buff_mem))) {
        LOG_WARN("failed to estimate ivf vector index memory", K(num_vectors), K(index_param));
      } else if (OB_FALSE_IT(suggested_mem = construct_mem + buff_mem)) {
      } else if (OB_FAIL(res_buf.append(ObString("Suggested minimum vector memory is "), 0))) {
        LOG_WARN("failed to append to buffer", K(ret));
      } else if (OB_FAIL(print_mem_size(suggested_mem, res_buf))) {
        LOG_WARN("failed to append memory size", K(ret));
      } else if (OB_FAIL(res_buf.append(ObString(", memory consumption when providing search service is "), 0))) {
        LOG_WARN("failed to append to buffer", K(ret));
      } else if (OB_FAIL(print_mem_size(buff_mem, res_buf))) {
        LOG_WARN("failed to append memory size", K(ret));
      }
      break;
    }
    case ObVectorIndexAlgorithmType::VIAT_IPIVF: {
      uint64_t estimate_mem = 0;
      uint64_t max_tablet_estimate_mem = 0;
      if (OB_FAIL(ObVectorIndexUtil::estimate_sparse_memory(num_vectors, index_param, estimate_mem))) {
        LOG_WARN("failed to estimate sparse vector index memory", K(num_vectors), K(index_param));
      } else if (OB_FAIL(ObVectorIndexUtil::estimate_sparse_memory(
                     tablet_max_num_vectors, index_param, max_tablet_estimate_mem))) {
        LOG_WARN("failed to estimate sparse vector index memory", K(tablet_max_num_vectors), K(index_param));
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

}
}