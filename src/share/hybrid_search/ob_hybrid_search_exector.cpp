/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_hybrid_search_exector.h"

#define USING_LOG_PREFIX SHARE

namespace oceanbase {
namespace share {

ObHybridSearchExecutor::ObHybridSearchExecutor()
    : ctx_(NULL), allocator_("HybridSearch") {}

ObHybridSearchExecutor::~ObHybridSearchExecutor() {}

int ObHybridSearchExecutor::init_search_arg(const ObHybridSearchArg &arg) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  }
  search_arg_.search_params_ = arg.search_params_;
  search_arg_.search_type_ = arg.search_type_;
  search_arg_.table_name_ = arg.table_name_;
  search_arg_.search_type_ = arg.search_type_;
  result_type_ = SearchResultType::SQL_RESULT;

  return ret;
}

int ObHybridSearchExecutor::init(const pl::ObPLExecCtx &ctx, const ObHybridSearchArg &arg) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.exec_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec context is not initialized", K(ret));
  } else if (OB_FAIL(init(ctx.exec_ctx_, arg))) {
    LOG_WARN("fail to init", KR(ret));
  }
  return ret;
}

int ObHybridSearchExecutor::init(sql::ObExecContext *ctx, const ObHybridSearchArg &arg) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx->get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is not initialized", K(ret));
  } else if (init_search_arg(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else {
    ctx_ = ctx;
    session_info_ = ctx_->get_my_session();
    tenant_id_ = session_info_->get_effective_tenant_id();
  }
  return ret;
}

int ObHybridSearchExecutor::execute_search(ObObj &query_res) {
  int ret = OB_SUCCESS;
  ObString query_sql;
  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec context is not initialized", K(ret));
  } else if (OB_FAIL(do_get_sql(search_arg_.search_params_, query_sql, true))) {
    LOG_WARN("fail to do get sql", KR(ret));
  } else {
    common::ObMySQLProxy* sql_proxy = ctx_->get_sql_proxy();
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql_proxy->read(result, tenant_id_, query_sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(query_sql), K(tenant_id_));
      } else if (OB_NOT_NULL(result.get_result())) {
        if (OB_SUCCESS == (ret = result.get_result()->next())) {
          ObObj tmp_res;
          if (OB_FAIL(result.get_result()->get_obj("hits", tmp_res))) {
            if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {
              query_res.set_null();
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to extract result. ", K(ret));
            }
          } else if (OB_FAIL(common::deep_copy_obj(ctx_->get_allocator(), tmp_res, query_res))) {
            LOG_WARN("deep copy query result failed", K(ret));
          }
        } else if (OB_ITER_END == ret) {
          LOG_INFO("no result return!", K(ret), K(tenant_id_));
          query_res.set_null();
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next", K(ret), K(tenant_id_));
        }
      }
    }
  }

  return ret;
}

int ObHybridSearchExecutor::execute_get_sql(ObString &sql_result) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_get_sql(search_arg_.search_params_, sql_result))) {
      LOG_WARN("fail to do get sql", KR(ret));
  }
  return ret;
}

int ObHybridSearchExecutor::do_get_sql(const ObString &search_params_str,
                                       ObString &sql_result, bool need_wrap_result /*= false*/) {
  int ret = OB_SUCCESS;
  share::ObQueryReqFromJson *query_req = nullptr;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec context is not initialized", K(ret));
  } else {
    if (OB_FAIL(parse_search_params(search_params_str, query_req, need_wrap_result))) {
      LOG_WARN("fail to parse search params", KR(ret));
    } else if (OB_ISNULL(query_req)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query request is null", KR(ret));
    } else {
      char *buf = NULL;
      int64_t res_len = 0;
      bool is_complete = false;
      ObIAllocator &alloc = ctx_->get_allocator();
      for (int64_t i = 1; OB_SUCC(ret) && !is_complete && i <= 1024; i = i * 2) {
        const int64_t length = OB_MAX_SQL_LENGTH * i;
        res_len = 0;
        if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(length)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for sql", K(ret), K(length));
        } else if (FALSE_IT(MEMSET(buf, 0, length))) {
        } else if (OB_FAIL(query_req->translate(buf, length, res_len))) {
          LOG_WARN("fail to translate to sql", KR(ret));
        }
        if (OB_SUCC(ret)) {
          is_complete = true;
          sql_result.assign_ptr(buf, res_len);
        } else if (OB_SIZE_OVERFLOW == ret) {
          // retry
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchExecutor::parse_search_params(
    const ObString &search_params_str, share::ObQueryReqFromJson *&query_req, bool need_wrap_result) {

  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  ObString table_name;
  ObString database_name;
  if (OB_ISNULL(search_params_str.ptr()) || search_params_str.length() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("search_params_str is invalid", K(ret), K(search_params_str));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  } else if (OB_FAIL(ObVectorRefreshIndexExecutor::resolve_table_name(
              cs_type, case_mode, lib::is_oracle_mode(), search_arg_.table_name_,
              database_name, table_name))) {
    LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(search_arg_.table_name_));
  } else if (database_name.empty() && FALSE_IT(database_name = session_info_->get_database_name())) {
  } else if (OB_UNLIKELY(database_name.empty())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("No database selected", KR(ret));
  } else {
    ObESQueryParser parser(allocator_, need_wrap_result, &table_name, &database_name);
    if (OB_FAIL(construct_column_index_info(allocator_, database_name, table_name, parser.get_index_name_map(), parser.get_user_column_names()))) {
      LOG_WARN("fail to construnct column index info", KR(ret), K(search_params_str));
    } else if (OB_FAIL(parser.parse(search_params_str, query_req))) {
      LOG_WARN("fail to parse search params", KR(ret), K(search_params_str));
    }
  }
  return ret;
}

int ObHybridSearchExecutor::construct_column_index_info(ObIAllocator &alloc, const ObString &database_name, const ObString &table_name,
                                                        ColumnIndexNameMap &column_index_info, ObIArray<ObString> &col_names)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *data_table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ObCStringHelper helper;

  if (OB_ISNULL(schema_guard = ctx_->get_virtual_table_ctx().schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", KR(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id_, database_name, table_name,
                  false, data_table_schema))) {
    LOG_WARN("failed to get table id", K(ret), K(database_name), K(table_name));
  } else if (data_table_schema == NULL) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, helper.convert(database_name), helper.convert(table_name));
  } else if (!data_table_schema->is_table_with_hidden_pk_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "table with user provided primary key");
    LOG_WARN("table with user provided primary key isn't supported", K(ret));
  } else if (OB_FAIL(data_table_schema->get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else if (OB_FAIL(get_basic_column_names(data_table_schema, col_names))) {
    LOG_WARN("fail to get all column names", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_table_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(tenant_id_, simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id_), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (index_table_schema->is_built_in_index()) {
        // skip built in vector index table
      } else {
        // handle delta_buffer_table index table
        const ObRowkeyInfo &rowkey_info = index_table_schema->get_rowkey_info();
        for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_info.get_size(); j++) {
          const ObRowkeyColumn *rowkey_column = rowkey_info.get_column(j);
          const int64_t column_id = rowkey_column->column_id_;
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema(column_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(column_id), KPC(index_table_schema));
          } else if ((index_table_schema->is_fts_index() && !col_schema->is_fulltext_column()) ||
                     (index_table_schema->is_vec_index() && col_schema->is_vec_hnsw_vid_column()) ||
                     (!index_table_schema->is_fts_index() && !index_table_schema->is_vec_index())) {
            // do nothing
          } else {
            // get generated column cascaded column id info
            // (vector index table key, like `c1` in "create table xxx vector index idx(c1)")
            ObArray<uint64_t> cascaded_column_ids;
            // get column_schema from data table using generate column id
            const ObColumnSchemaV2 *table_column = data_table_schema->get_column_schema(col_schema->get_column_id());
            ObStringBuffer column_names(&alloc);
            if (OB_ISNULL(table_column)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected table column", K(ret));
            } else if (OB_FAIL(table_column->get_cascaded_column_ids(cascaded_column_ids))) {
              LOG_WARN("failed to get cascaded column ids", K(ret));
            } else {
              for (int64_t k = 0; OB_SUCC(ret) && k < cascaded_column_ids.count(); ++k) {
                const ObColumnSchemaV2 *cascaded_column = NULL;
                ObString new_col_name;
                if (OB_ISNULL(cascaded_column = data_table_schema->get_column_schema(cascaded_column_ids.at(k)))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected cascaded column", K(ret));
                } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                          alloc,
                          cascaded_column->get_column_name_str(),
                          new_col_name,
                          false))) {
                  LOG_WARN("fail to generate new name with escape character", K(ret), K(cascaded_column->get_column_name_str()));
                } else if (OB_FAIL(column_names.append(new_col_name))) {
                  LOG_WARN("fail to print column name", K(ret), K(new_col_name));
                } else if (k != cascaded_column_ids.count() - 1 && OB_FAIL(column_names.append(", "))) {
                  LOG_WARN("fail to print column name", K(ret), K(new_col_name));
                }
              }
              ObString index_name;
              ObColumnIndexInfo *index_info = NULL;
              if (OB_FAIL(ret)) {
              } else if (!column_index_info.created() && OB_FAIL(column_index_info.create(simple_index_infos.count(), "HybridSearch"))) {
                LOG_WARN("fail to create column index info map", KR(ret));
              } else if (OB_FAIL(column_index_info.get_refactored(column_names.string(), index_info))) {
                if (ret == OB_HASH_NOT_EXIST) {
                  ret = OB_SUCCESS;
                  index_info = OB_NEWx(ObColumnIndexInfo, &alloc);
                  if (OB_ISNULL(index_info)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("fail to create index info", K(ret));
                  } else if (OB_FAIL(ObTableSchema::get_index_name(alloc, data_table_schema->get_table_id(),
                              ObString::make_string(index_table_schema->get_table_name()), index_name))) {
                    LOG_WARN("get index table name failed", K(ret));
                  } else if (FALSE_IT(index_info->index_name_ = index_name)) {
                  } else if (FALSE_IT(index_info->index_type_ = index_table_schema->get_index_type())) {
                  } else if (index_table_schema->is_vec_index()) {
                    ObVectorIndexType index_type = ObVectorIndexType::VIT_MAX;
                    if (index_table_schema->is_vec_ivf_index()) {
                      index_type = ObVectorIndexType::VIT_IVF_INDEX;
                    } else if (index_table_schema->is_vec_hnsw_index()) {
                      index_type = ObVectorIndexType::VIT_HNSW_INDEX;
                    }
                    ObVectorIndexParam index_param;
                    if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(index_table_schema->get_index_params(), index_type, index_param))) {
                      LOG_WARN("failed to parser vec index param", K(ret), K(index_table_schema->get_index_params()));
                    } else {
                      index_info->dist_algorithm_ = index_param.dist_algorithm_;
                    }
                  }
                  if (OB_FAIL(ret)) {
                  } else if (OB_FAIL(column_index_info.set_refactored(column_names.string(), index_info))) {
                    LOG_WARN("failed to set_refactored column name", K(ret), K(column_names.string()));
                  } else {
                    LOG_INFO("column index info", K(ret), K(column_names.string()), K(index_name));
                  }
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

int ObHybridSearchExecutor::get_basic_column_names(const ObTableSchema *table_schema, ObIArray<ObString> &col_names)
{
  int ret = OB_SUCCESS;
  ObColumnIterByPrevNextID iter(*table_schema);
  const ObColumnSchemaV2 *column_schema = NULL;
  int i = 0;
  while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema))) {
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is null", K(ret));
    } else if (column_schema->is_shadow_column() ||
               column_schema->is_invisible_column() ||
               column_schema->is_hidden()) {
      // don't show shadow columns for select * from idx
      continue;
    } else  if (OB_FAIL(col_names.push_back(column_schema->get_column_name_str()))) {
      LOG_WARN("push back column name failed", K(ret));
    }
  }
  if (ret != OB_ITER_END) {
    LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase