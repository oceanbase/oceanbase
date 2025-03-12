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
// #define DBMS_VECTOR_MOCK_TEST
#define USING_LOG_PREFIX STORAGE

#include "storage/vector_index/cmd/ob_vector_refresh_index_executor.h"
#include "storage/vector_index/ob_vector_index_refresh.h"
#include "pl/ob_pl.h"
#include "share/ob_vec_index_builder_util.h"

namespace oceanbase {
namespace storage {

ObVectorRefreshIndexExecutor::ObVectorRefreshIndexExecutor()
  : ctx_(nullptr), session_info_(nullptr), tenant_id_(OB_INVALID_TENANT_ID) {}

ObVectorRefreshIndexExecutor::~ObVectorRefreshIndexExecutor() {}

int ObVectorRefreshIndexExecutor::execute_refresh(
    pl::ObPLExecCtx &ctx, const ObVectorRefreshIndexArg &arg) {
  int ret = OB_SUCCESS;
  ctx_ = ctx.exec_ctx_;
  pl_ctx_ = &ctx;
  CK(OB_NOT_NULL(ctx_));
  CK(OB_NOT_NULL(session_info_ = ctx_->get_my_session()));
  CK(OB_NOT_NULL(ctx_->get_sql_ctx()));
  CK(OB_NOT_NULL(ctx_->get_sql_ctx()->schema_guard_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OZ(schema_checker_.init(*(ctx_->get_sql_ctx()->schema_guard_),
                          session_info_->get_sessid()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObVectorRefreshIndexExecutor::check_min_data_version(
      tenant_id_, DATA_VERSION_4_3_3_0,
      "tenant's data version is below 4.3.3.0, refreshing vector index is not "
      "supported."));
  OZ(resolve_refresh_arg(arg));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_refresh())) {
    LOG_WARN("fail to do refresh", KR(ret));
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::execute_refresh_inner(
  pl::ObPLExecCtx &ctx, const ObVectorRefreshIndexInnerArg &arg)
{
  int ret = OB_SUCCESS;
  bool in_recycle_bin = false;
  ctx_ = ctx.exec_ctx_;
  pl_ctx_ = &ctx;
  CK(OB_NOT_NULL(ctx_));
  CK(OB_NOT_NULL(session_info_ = ctx_->get_my_session()));
  CK(OB_NOT_NULL(ctx_->get_sql_ctx()));
  CK(OB_NOT_NULL(ctx_->get_sql_ctx()->schema_guard_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OZ(schema_checker_.init(*(ctx_->get_sql_ctx()->schema_guard_),
                          session_info_->get_sessid()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObVectorRefreshIndexExecutor::check_min_data_version(
      tenant_id_, DATA_VERSION_4_3_3_0,
      "tenant's data version is below 4.3.3.0, refreshing vector index is not "
      "supported."));
  OZ(resolve_refresh_inner_arg(arg, in_recycle_bin));

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(in_recycle_bin)) {
    // do nothing
    LOG_DEBUG("delta buffer table is in recyclebin, do nothing");
  } else if (OB_FAIL(do_refresh_with_retry())) {
    LOG_WARN("fail to do refresh", KR(ret));
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::execute_rebuild(
    pl::ObPLExecCtx &ctx, const ObVectorRebuildIndexArg &arg) {
  int ret = OB_SUCCESS;
  ctx_ = ctx.exec_ctx_;
  pl_ctx_ = &ctx;
  CK(OB_NOT_NULL(ctx_));
  CK(OB_NOT_NULL(session_info_ = ctx_->get_my_session()));
  CK(OB_NOT_NULL(ctx_->get_sql_ctx()));
  CK(OB_NOT_NULL(ctx_->get_sql_ctx()->schema_guard_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OZ(schema_checker_.init(*(ctx_->get_sql_ctx()->schema_guard_),
                          session_info_->get_sessid()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObVectorRefreshIndexExecutor::check_min_data_version(
      tenant_id_, DATA_VERSION_4_3_3_0,
      "tenant's data version is below 4.3.3.0, refreshing vector index is not "
      "supported."));
  OZ(resolve_rebuild_arg(arg));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_rebuild())) {
    LOG_WARN("fail to do refresh", KR(ret));
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::execute_rebuild_inner(
  pl::ObPLExecCtx &ctx, const ObVectorRebuildIndexInnerArg &arg)
{
  int ret = OB_SUCCESS;
  bool in_recycle_bin = false;
  ctx_ = ctx.exec_ctx_;
  pl_ctx_ = &ctx;
  CK(OB_NOT_NULL(ctx_));
  CK(OB_NOT_NULL(session_info_ = ctx_->get_my_session()));
  CK(OB_NOT_NULL(ctx_->get_sql_ctx()));
  CK(OB_NOT_NULL(ctx_->get_sql_ctx()->schema_guard_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OZ(schema_checker_.init(*(ctx_->get_sql_ctx()->schema_guard_),
                          session_info_->get_sessid()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObVectorRefreshIndexExecutor::check_min_data_version(
      tenant_id_, DATA_VERSION_4_3_3_0,
      "tenant's data version is below 4.3.3.0, refreshing vector index is not "
      "supported."));
  OZ(resolve_rebuild_inner_arg(arg, in_recycle_bin));

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(in_recycle_bin)) {
    // do nothing
    LOG_DEBUG("delta buffer table is in recyclebin, do nothing");
  } else if (OB_FAIL(do_rebuild_with_retry())) {
    LOG_WARN("fail to do refresh", KR(ret));
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::check_min_data_version(
    const uint64_t tenant_id, const uint64_t min_data_version,
    const char *errmsg) {
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < min_data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret),
             K(tenant_id), K(compat_version), K(min_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, errmsg);
  }
  return ret;
}

void ObVectorRefreshIndexExecutor::upper_db_table_name(
    const ObNameCaseMode case_mode, const bool is_oracle_mode, ObString &name) {
  if (is_oracle_mode) {
    str_toupper(name.ptr(), name.length());
  } else {
    if (OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
      str_tolower(name.ptr(), name.length());
    }
  }
}

int ObVectorRefreshIndexExecutor::resolve_table_name(
    const ObCollationType cs_type, const ObNameCaseMode case_mode,
    const bool is_oracle_mode, const ObString &name, ObString &database_name,
    ObString &table_name) {
  int ret = OB_SUCCESS;
  static const char split_character = '.';
  database_name.reset();
  table_name.reset();
  if (OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(name));
  } else {
    ObString name_str = name;
    const char *p = name_str.find(split_character);
    if (p == nullptr) {
      table_name = name_str;
    } else {
      database_name = name_str.split_on(p);
      table_name = name_str;
      if (OB_UNLIKELY(database_name.empty() || table_name.empty() ||
                      nullptr != table_name.find(split_character))) {
        ret = OB_WRONG_TABLE_NAME;
        LOG_WARN("wrong table name", KR(ret), K(name));
      }
    }
    if (OB_SUCC(ret)) {
      const bool preserve_lettercase =
          is_oracle_mode ? true : (case_mode != OB_LOWERCASE_AND_INSENSITIVE);
      upper_db_table_name(case_mode, is_oracle_mode, database_name);
      upper_db_table_name(case_mode, is_oracle_mode, table_name);
      if (!database_name.empty() &&
          OB_FAIL(ObSQLUtils::check_and_convert_db_name(
              cs_type, preserve_lettercase, database_name))) {
        LOG_WARN("fail to check and convert database name", KR(ret),
                 K(database_name));
      } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(
                     cs_type, preserve_lettercase, table_name,
                     is_oracle_mode))) {
        LOG_WARN("fail to check and convert table name", KR(ret), K(cs_type),
                 K(preserve_lettercase), K(table_name));
      }
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::generate_vector_aux_index_name(
    VectorIndexAuxType index_type, const uint64_t data_table_id,
    const ObString &index_name, ObString &real_index_name) {
  int ret = OB_SUCCESS;
  char *name_buf = nullptr;
  ObIAllocator *allocator = pl_ctx_->allocator_;
  CK(OB_NOT_NULL(allocator));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(name_buf = static_cast<char *>(
                    allocator->alloc(OB_MAX_TABLE_NAME_LENGTH)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret));
  } else {
    int64_t pos = 0;
    ObString suffix_index_name;
    if (VectorIndexAuxType::DOMAIN_INDEX == index_type) {
      if (OB_FAIL(databuff_printf(name_buf, OB_MAX_TABLE_NAME_LENGTH, pos,
                                  "%.*s",
                                  index_name.length(), index_name.ptr()))) {
        LOG_WARN("failed to print", K(ret));
      }
    } else if (VectorIndexAuxType::INDEX_ID_INDEX == index_type) {
      if (OB_FAIL(databuff_printf(name_buf, OB_MAX_TABLE_NAME_LENGTH, pos,
                                  "%.*s%s", index_name.length(),
                                  index_name.ptr(),
                                  ObVecIndexBuilderUtil::INDEX_ID_TABLE_NAME_SUFFIX))) {
        LOG_WARN("failed to print", K(ret));
      }
    } else if (VectorIndexAuxType::MOCK_INDEX_1 == index_type) {
      if (OB_FAIL(databuff_printf(name_buf, OB_MAX_TABLE_NAME_LENGTH, pos,
                                  "%.*s1", index_name.length(),
                                  index_name.ptr()))) {
        LOG_WARN("failed to print", K(ret));
      }
    } else if (VectorIndexAuxType::MOCK_INDEX_2 == index_type) {
      if (OB_FAIL(databuff_printf(name_buf, OB_MAX_TABLE_NAME_LENGTH, pos,
                                  "%.*s2", index_name.length(),
                                  index_name.ptr()))) {
        LOG_WARN("failed to print", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(suffix_index_name.assign_ptr(
                   name_buf, static_cast<int32_t>(pos)))) {
    } else if (OB_FAIL(ObTableSchema::build_index_table_name(
                   *allocator, data_table_id, suffix_index_name,
                   real_index_name))) {
      LOG_WARN("fail to build index table name", K(ret), K(data_table_id),
               K(suffix_index_name), K(real_index_name));
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::mock_check_idx_col_name(
    const ObString &idx_col_name,
    const share::schema::ObTableSchema *base_table_schema,
    const share::schema::ObTableSchema *delta_buf_table_schema,
    const share::schema::ObTableSchema *index_id_table_schema) {
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(base_table_schema),
     OB_NOT_NULL(delta_buf_table_schema),
     OB_NOT_NULL(index_id_table_schema));
  if (OB_FAIL(ret)) {
  } else {
    const ObIndexInfo &delta_table_idx_info =
        delta_buf_table_schema->get_index_info();
    const ObIndexInfo &index_id_idx_info =
        index_id_table_schema->get_index_info();
    uint64_t delta_table_idx_col_id = OB_INVALID_ID;
    uint64_t index_id_idx_col_id = OB_INVALID_ID;
    const ObColumnSchemaV2 *col_schema = nullptr;
    if (OB_UNLIKELY(1 != delta_table_idx_info.get_size() ||
                    1 != index_id_idx_info.get_size())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the count of index column is not 1", KR(ret),
              K(delta_table_idx_info.get_size()),
              K(index_id_idx_info.get_size()));
    } else if (OB_ISNULL(delta_table_idx_info.get_column(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret));
    } else if (OB_ISNULL(index_id_idx_info.get_column(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret));
    } else if (FALSE_IT(delta_table_idx_col_id =
                            delta_table_idx_info.get_column(0)->column_id_)) {
    } else if (FALSE_IT(index_id_idx_col_id =
                            index_id_idx_info.get_column(0)->column_id_)) {
    } else if (OB_UNLIKELY(delta_table_idx_col_id != index_id_idx_col_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delta_buf_table & index_id_idx have different index column id",
              KR(ret), K(delta_table_idx_col_id), K(index_id_idx_col_id));
    } else if (FALSE_IT(col_schema = base_table_schema->get_column_schema(
                            delta_table_idx_col_id))) {
    } else if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column not exist", KR(ret), K(delta_table_idx_col_id));
    } else if (OB_UNLIKELY(idx_col_name != col_schema->get_column_name_str())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("idx column name does not match with index table", KR(ret),
              K(idx_col_name), K(col_schema->get_column_name_str()));
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::check_idx_col_name(
    const ObString &idx_col_name,
    const share::schema::ObTableSchema *base_table_schema,
    const share::schema::ObTableSchema *delta_buf_table_schema,
    const share::schema::ObTableSchema *index_id_table_schema) {
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(base_table_schema),
     OB_NOT_NULL(delta_buf_table_schema),
     OB_NOT_NULL(index_id_table_schema));
  if (OB_FAIL(ret)) {
  } else {
    const ObIndexInfo &delta_table_idx_info =
        delta_buf_table_schema->get_index_info();
    const ObIndexInfo &index_id_idx_info =
        index_id_table_schema->get_index_info();
    uint64_t delta_table_idx_col_id = OB_INVALID_ID;
    uint64_t index_id_idx_col_id = OB_INVALID_ID;
    const ObColumnSchemaV2 *col_schema = nullptr;
    if (OB_UNLIKELY(1 != delta_table_idx_info.get_size() ||
                    1 != index_id_idx_info.get_size())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the count of index column is not 1", KR(ret),
              K(delta_table_idx_info.get_size()),
              K(index_id_idx_info.get_size()));
    } else if (OB_ISNULL(delta_table_idx_info.get_column(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret));
    } else if (OB_ISNULL(index_id_idx_info.get_column(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret));
    } else if (FALSE_IT(delta_table_idx_col_id =
                            delta_table_idx_info.get_column(0)->column_id_)) {
    } else if (FALSE_IT(index_id_idx_col_id =
                            index_id_idx_info.get_column(0)->column_id_)) {
    } else if (OB_UNLIKELY(delta_table_idx_col_id != index_id_idx_col_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delta_buf_table & index_id_idx have different index column id",
              KR(ret), K(delta_table_idx_col_id), K(index_id_idx_col_id));
    } else if (FALSE_IT(col_schema = base_table_schema->get_column_schema(
                            delta_table_idx_col_id))) {
    } else if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column not exist", KR(ret), K(delta_table_idx_col_id));
    } else if (OB_UNLIKELY(idx_col_name != col_schema->get_column_name_str())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("idx column name does not match with index table", KR(ret),
              K(idx_col_name), K(col_schema->get_column_name_str()));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "index column name");
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::mock_resolve_and_check_table_valid(
    const ObString &arg_idx_name, const ObString &arg_base_name,
    const ObString &idx_col_name,
    const share::schema::ObTableSchema *&base_table_schema,
    const share::schema::ObTableSchema *&domain_table_schema,
    const share::schema::ObTableSchema *&index_id_table_schema) {
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_ISNULL(session_info_)) {
    ret =  OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", KR(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  } else {
    ObString base_db_name, base_name, index_db_name, index_name;
    ObString new_base_db_name, new_base_name;
    ObString domain_table_name, index_id_table_name;
    bool has_synonym = false;
    base_table_schema = domain_table_schema = index_id_table_schema = nullptr;
    uint64_t base_table_id = -1;
    if (OB_FAIL(ObVectorRefreshIndexExecutor::resolve_table_name(
            cs_type, case_mode, lib::is_oracle_mode(), arg_base_name,
            base_db_name, base_name))) {
      LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode),
              K(arg_base_name));
      LOG_USER_ERROR(OB_WRONG_TABLE_NAME,
                    static_cast<int>(arg_base_name.length()),
                    arg_base_name.ptr());
    } else if (OB_FAIL(ObVectorRefreshIndexExecutor::resolve_table_name(
                  cs_type, case_mode, lib::is_oracle_mode(), arg_idx_name,
                  index_db_name, index_name))) {
      LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode),
              K(arg_idx_name));
      LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int>(arg_idx_name.length()),
                    arg_idx_name.ptr());
    } else if (base_db_name.empty() &&
              FALSE_IT(base_db_name = session_info_->get_database_name())) {
    } else if (index_db_name.empty() &&
              FALSE_IT(index_db_name = session_info_->get_database_name())) {
    } else if (OB_UNLIKELY(base_db_name.empty() || index_db_name.empty())) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("No database selected", KR(ret));
    } else if (OB_UNLIKELY(base_db_name != index_db_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("different db name is not supported.");
    } else if (OB_FAIL(schema_checker_.get_table_schema_with_synonym(
                  tenant_id_, base_db_name, base_name, false /*is_index_table*/,
                  has_synonym, new_base_db_name, new_base_name,
                  base_table_schema))) {
      LOG_WARN("fail to get table schema with synonym", KR(ret), K(base_db_name),
              K(base_name));
    } else if (OB_ISNULL(base_table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("base table not exist", KR(ret), K(base_db_name), K(base_name),
              KP(base_table_schema));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "base table name");
    } else if (FALSE_IT(base_table_id = base_table_schema->get_table_id())) {
    } else if (OB_FAIL(generate_vector_aux_index_name(
                  VectorIndexAuxType::MOCK_INDEX_1, base_table_id, index_name,
                  domain_table_name))) {
      LOG_WARN("fail to generate delta buf index table name", KR(ret),
              K(base_table_id), K(index_name));
    } else if (OB_FAIL(generate_vector_aux_index_name(
                  VectorIndexAuxType::MOCK_INDEX_2, base_table_id, index_name,
                  index_id_table_name))) {
      LOG_WARN("fail to generate index id index table name", KR(ret),
              K(base_table_id), K(index_name));
    } else if (OB_FAIL(schema_checker_.get_table_schema(
                  tenant_id_, index_db_name, domain_table_name, true,
                  domain_table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(index_db_name),
              K(domain_table_name));
    } else if (OB_FAIL(schema_checker_.get_table_schema(tenant_id_, index_db_name,
                                                        index_id_table_name, true,
                                                        index_id_table_schema,
                                                        false, /*with_hidden_flag*/
                                                        true /*is_built_in_index*/))) {
      LOG_WARN("fail to get table schema", KR(ret), K(index_db_name),
              K(index_id_table_name));
    } else if (OB_ISNULL(domain_table_schema) ||
              OB_ISNULL(index_id_table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("delta_buf_table or index_id_table is not exist", KR(ret), K(arg_idx_name),
               KP(domain_table_schema), KP(index_id_table_schema));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "idx name");
    } else if (!idx_col_name.empty() &&
              OB_FAIL(mock_check_idx_col_name(idx_col_name, base_table_schema,
                                              domain_table_schema,
                                              index_id_table_schema))) {
      LOG_WARN("fail to check idx column name", KR(ret), K(idx_col_name));
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::resolve_and_check_table_valid(
    const ObString &arg_idx_name, const ObString &arg_base_name,
    const ObString &idx_col_name,
    const share::schema::ObTableSchema *&base_table_schema,
    const share::schema::ObTableSchema *&domain_table_schema,
    const share::schema::ObTableSchema *&index_id_table_schema) {
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", KR(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  } else {
    ObString base_db_name, base_name, index_db_name, index_name;
    ObString new_base_db_name, new_base_name;
    ObString index_id_table_name;
    ObString domain_index_table_name;
    bool has_synonym = false;
    base_table_schema = domain_table_schema = index_id_table_schema = nullptr;
    uint64_t base_table_id = -1;
    ObString base_vector_index_col_name;
    if (OB_FAIL(ObVectorRefreshIndexExecutor::resolve_table_name(
            cs_type, case_mode, lib::is_oracle_mode(), arg_base_name,
            base_db_name, base_name))) {
      LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode),
              K(arg_base_name));
      LOG_USER_ERROR(OB_WRONG_TABLE_NAME,
                    static_cast<int>(arg_base_name.length()),
                    arg_base_name.ptr());
    } else if (OB_FAIL(ObVectorRefreshIndexExecutor::resolve_table_name(
                  cs_type, case_mode, lib::is_oracle_mode(), arg_idx_name,
                  index_db_name, index_name))) {
      LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode),
              K(arg_idx_name));
      LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int>(arg_idx_name.length()),
                    arg_idx_name.ptr());
    } else if (base_db_name.empty() &&
              FALSE_IT(base_db_name = session_info_->get_database_name())) {
    } else if (index_db_name.empty() &&
              FALSE_IT(index_db_name = session_info_->get_database_name())) {
    } else if (OB_UNLIKELY(base_db_name.empty() || index_db_name.empty())) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("No database selected", KR(ret));
    } else if (OB_UNLIKELY(base_db_name != index_db_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("different db name is not supported.");
    } else if (OB_FAIL(schema_checker_.get_table_schema_with_synonym(
                  tenant_id_, base_db_name, base_name, false /*is_index_table*/,
                  has_synonym, new_base_db_name, new_base_name,
                  base_table_schema))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to get table schema with synonym", KR(ret), K(base_db_name),
              K(base_name));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "base table name");
    } else if (OB_ISNULL(base_table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("base table not exist", KR(ret), K(base_db_name), K(base_name),
              KP(base_table_schema));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "base table name");
    } else if (FALSE_IT(base_table_id = base_table_schema->get_table_id())) {
    } else if (OB_FAIL(generate_vector_aux_index_name(
                  VectorIndexAuxType::DOMAIN_INDEX, base_table_id, index_name,
                  domain_index_table_name))) {
      LOG_WARN("fail to generate delta buf index table name", KR(ret),
              K(base_table_id), K(index_name));
    } else if (OB_FAIL(schema_checker_.get_table_schema(
                  tenant_id_, index_db_name, domain_index_table_name, true,
                  domain_table_schema))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to get table schema", KR(ret), K(index_db_name),
              K(domain_index_table_name));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "index name");
    } else if (OB_ISNULL(domain_table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("domain index table is not exist", KR(ret), K(domain_index_table_name));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "index name");
    } else if (!domain_table_schema->is_vec_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rebuild or refresh not vector index is not support ", K(ret), K(domain_index_table_name));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rebuild or refresh not vector index is");
    }
    // get
    if (OB_FAIL(ret)) {
    } else if (domain_table_schema->is_vec_hnsw_index()) {
      if (OB_FAIL(generate_vector_aux_index_name(
                  VectorIndexAuxType::INDEX_ID_INDEX,
                  base_table_id, index_name, index_id_table_name))) {
        LOG_WARN("fail to generate index id index table name", KR(ret),
                K(base_table_id), K(index_name));
      } else if (OB_FAIL(schema_checker_.get_table_schema(tenant_id_, index_db_name,
                                                          index_id_table_name, true,
                                                          index_id_table_schema,
                                                          false, /*with_hidden_flag*/
                                                          true /*is_built_in_index*/))) {
        LOG_WARN("fail to get table schema", KR(ret), K(index_db_name),
                K(index_id_table_name));
      } else if (OB_ISNULL(index_id_table_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("index_id_table is not exist",
          KR(ret), K(arg_idx_name), KP(index_id_table_schema));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "index name");
      }
    }
    // check index column match
    if (OB_FAIL(ret)) {
    } else if (!idx_col_name.empty() &&
              OB_FAIL(get_vector_index_column_name(
                  base_table_schema, domain_table_schema,
                  base_vector_index_col_name))) {
      LOG_WARN("fail to get vector index column name", KR(ret));
    } else if (!idx_col_name.empty() &&
              0 != idx_col_name.case_compare(base_vector_index_col_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("vector index column name is not match", KR(ret), K(idx_col_name),
              K(base_vector_index_col_name));
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::resolve_table_id_and_check_table_valid(
  const int64_t idx_table_id,
  const share::schema::ObTableSchema *&base_table_schema,
  const share::schema::ObTableSchema *&domain_table_schema,
  const share::schema::ObTableSchema *&index_id_table_schema,
  bool& in_recycle_bin)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  base_table_schema = domain_table_schema = index_id_table_schema = nullptr;
  ObString user_index_name;
  ObString index_id_table_name;
  const ObDatabaseSchema *database_schema = nullptr;
  in_recycle_bin = false;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", KR(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  } else if (OB_FAIL(schema_checker_.get_table_schema(tenant_id_, idx_table_id, domain_table_schema))) {
    LOG_WARN("fail to get index id table table schema", KR(ret), K(idx_table_id));
  } else if (OB_ISNULL(domain_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret), KP(domain_table_schema));
  } else if (OB_UNLIKELY(domain_table_schema->is_in_recyclebin())) {
    in_recycle_bin = true;
    LOG_DEBUG("domain table is in recyclebin, do nothing");
  } else if (OB_UNLIKELY(!domain_table_schema->is_vec_domain_index())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid index table type", KR(ret), K(domain_table_schema->is_vec_domain_index()));
  } else if (OB_FAIL(schema_checker_.get_table_schema(tenant_id_, domain_table_schema->get_data_table_id(), base_table_schema))) {
    LOG_WARN("fail to get base table schema", KR(ret), K(idx_table_id));
  } else if (OB_ISNULL(base_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret), KP(base_table_schema));
  } else if (OB_FAIL(schema_checker_.get_database_schema(tenant_id_, domain_table_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret), K(tenant_id_), K(domain_table_schema->get_database_id()));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database_schema is null", K(ret), KP(database_schema));
  }
  // get index id table schema if need
  if (OB_FAIL(ret)) {
  } else if (!domain_table_schema->is_vec_hnsw_index()) {   // skip not hnsw index
  } else if (OB_FAIL(ObTableSchema::get_index_name(domain_table_schema->get_table_name_str(), user_index_name))) {
    LOG_WARN("fail to get user index name", K(ret), K(domain_table_schema->get_table_name_str()));
  } else if (OB_FAIL(generate_vector_aux_index_name(
                     VectorIndexAuxType::INDEX_ID_INDEX, domain_table_schema->get_data_table_id(),
                     user_index_name, index_id_table_name))) {
    LOG_WARN("fail to generate index id index table name", KR(ret),
              K(domain_table_schema->get_data_table_id()), K(user_index_name));
  } else if (OB_FAIL(schema_checker_.get_table_schema(tenant_id_, database_schema->get_database_name_str(),
                                                      index_id_table_name, true,
                                                      index_id_table_schema,
                                                      false, /*with_hidden_flag*/
                                                      true /*is_built_in_index*/))) {
    LOG_WARN("fail to get table schema", KR(ret), K(database_schema->get_database_name_str()),
            K(index_id_table_name));
  } else if (OB_ISNULL(index_id_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret), KP(index_id_table_schema));
  } else if (OB_UNLIKELY(!index_id_table_schema->is_vec_index_id_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid index table type", KR(ret), K(index_id_table_schema->is_vec_index_id_type()));
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::to_refresh_method(
    const ObString &arg_refresh_method,
    share::schema::ObVectorRefreshMethod &method, bool is_rebuild) {
  int ret = OB_SUCCESS;
  if (is_rebuild) {
    method = schema::ObVectorRefreshMethod::REBUILD_COMPLETE;
  } else if (arg_refresh_method.empty() ||
             0 == arg_refresh_method.case_compare("FAST")) {
    method = schema::ObVectorRefreshMethod::REFRESH_DELTA;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Vector index refresh method is not supported.", KR(ret),
             K(arg_refresh_method));
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::to_vector_index_organization(
    const ObString &idx_organization_str,
    share::schema::ObVectorIndexOrganization &idx_organization) {
  int ret = OB_SUCCESS;
  if (idx_organization_str.empty() ||
      0 == idx_organization_str.case_compare("IN MEMORY NEIGHBOR GRAPH")) {
    idx_organization = ObVectorIndexOrganization::IN_MEMORY_NEIGHBOR_GRAPH;
  } else if (0 == idx_organization_str.case_compare("NEIGHBOR PARTITION")) {
    idx_organization = ObVectorIndexOrganization::NEIGHBOR_PARTITION;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Vector index organization is not supported.", KR(ret),
             K(idx_organization_str));
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::to_vector_index_distance_metric(
    const ObString &idx_distance_metric_str,
    share::schema::ObVetcorIndexDistanceMetric &idx_distance_metric) {
  int ret = OB_SUCCESS;
  if (idx_distance_metric_str.empty() ||
      0 == idx_distance_metric_str.case_compare("EUCLIDEAN")) {
    idx_distance_metric = ObVetcorIndexDistanceMetric::EUCLIDEAN;
  } else if (0 == idx_distance_metric_str.case_compare("EUCLIDEAN_SQUARED")) {
    idx_distance_metric = ObVetcorIndexDistanceMetric::EUCLIDEAN_SQUARED;
  } else if (0 == idx_distance_metric_str.case_compare("DOT")) {
    idx_distance_metric = ObVetcorIndexDistanceMetric::DOT;
  } else if (0 == idx_distance_metric_str.case_compare("COSINE")) {
    idx_distance_metric = ObVetcorIndexDistanceMetric::COSINE;
  } else if (0 == idx_distance_metric_str.case_compare("MANHATTAN")) {
    idx_distance_metric = ObVetcorIndexDistanceMetric::MANHATTAN;
  } else if (0 == idx_distance_metric_str.case_compare("HAMMING")) {
    idx_distance_metric = ObVetcorIndexDistanceMetric::HAMMING;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Vector index distance metrics is not supported.", KR(ret),
             K(idx_distance_metric_str));
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::get_vector_index_column_name(
    const share::schema::ObTableSchema *base_table_schema,
    const share::schema::ObTableSchema *domain_index_schema,
    ObString &col_name) {
  int ret = OB_SUCCESS;
  col_name.reset();
  CK(OB_NOT_NULL(base_table_schema),
     OB_NOT_NULL(domain_index_schema));
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!domain_index_schema->is_vec_domain_index())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is not a domain index table", KR(ret), K(domain_index_schema));
  } else {
    ObSEArray<ObString, 1> col_names;
    if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_name(*base_table_schema,
                                                                *domain_index_schema,
                                                                col_names))) {
      LOG_WARN("fail to get vector index col name", K(ret));
    } else if (col_names.count() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get col name array wrong", K(col_names));
    } else {
      col_name = col_names.at(0);
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::is_refresh_retry_ret_code(int ret_code) {
  return OB_OLD_SCHEMA_VERSION == ret_code || OB_EAGAIN == ret_code ||
         OB_INVALID_QUERY_TIMESTAMP == ret_code ||
         OB_TASK_EXPIRED == ret_code || is_master_changed_error(ret_code) ||
         is_partition_change_error(ret_code) ||
         is_ddl_stmt_packet_retry_err(ret_code);
}

int ObVectorRefreshIndexExecutor::resolve_refresh_arg(
    const ObVectorRefreshIndexArg &arg) {
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *base_table_schema = nullptr;
  const share::schema::ObTableSchema *domain_table_schema = nullptr;
  const share::schema::ObTableSchema *index_id_table_schema = nullptr;

#ifdef DBMS_VECTOR_MOCK_TEST
  if (OB_FAIL(mock_resolve_and_check_table_valid(
          arg.idx_name_, arg.table_name_, arg.idx_vector_col_,
          base_table_schema, domain_table_schema, index_id_table_schema)))
#else
  if (OB_FAIL(resolve_and_check_table_valid(
          arg.idx_name_, arg.table_name_, arg.idx_vector_col_,
          base_table_schema, domain_table_schema, index_id_table_schema)))
#endif
  {
    LOG_WARN("fail to resolve and check table valid", KR(ret), K(arg));
  } else if (OB_ISNULL(base_table_schema) ||
             OB_ISNULL(domain_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schemas are null", K(ret), KP(base_table_schema), KP(domain_table_schema));
  } else if (domain_table_schema->is_vec_ivf_index()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("refresh ivf index is not support", K(ret), K(domain_table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh ivf index is");
  } else if (domain_table_schema->is_vec_hnsw_index() && OB_ISNULL(index_id_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schemas are null", K(ret), KP(index_id_table_schema));
  } else {
    base_tb_id_ = base_table_schema->get_table_id();
    domain_tb_id_ = domain_table_schema->get_table_id();
    index_id_tb_id_ = index_id_table_schema == nullptr ? OB_INVALID_ID : index_id_table_schema->get_table_id();
    refresh_threshold_ = arg.refresh_threshold_;
  }
  // resolve method
  if (OB_SUCC(ret)) {
    refresh_method_ = schema::ObVectorRefreshMethod::MAX;
    if (OB_FAIL(ObVectorRefreshIndexExecutor::to_refresh_method(
            arg.refresh_type_, refresh_method_))) {
      LOG_WARN("fail to resolve refresh method", KR(ret));
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::resolve_refresh_inner_arg(const ObVectorRefreshIndexInnerArg &arg,
                                                            bool& in_recycle_bin)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *base_table_schema = nullptr;
  const share::schema::ObTableSchema *domain_table_schema = nullptr;
  const share::schema::ObTableSchema *index_id_table_schema = nullptr;
  in_recycle_bin = false;
  if (OB_FAIL(resolve_table_id_and_check_table_valid(arg.idx_table_id_, base_table_schema, domain_table_schema, index_id_table_schema, in_recycle_bin))) {
    LOG_WARN("fail to resolve table id and check table valid", KR(ret), K(arg));
  } else if (OB_UNLIKELY(in_recycle_bin)) {
    LOG_DEBUG("delta buffer table is in recyclebin, do nothing");
  } else if (OB_ISNULL(base_table_schema) ||
             OB_ISNULL(domain_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schemas are null", K(ret), KP(base_table_schema), KP(domain_table_schema));
  } else if (domain_table_schema->is_vec_hnsw_index() && OB_ISNULL(index_id_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schemas are null", K(ret), KP(index_id_table_schema));
  } else {
    base_tb_id_ = base_table_schema->get_table_id();
    domain_tb_id_ = domain_table_schema->get_table_id();
    index_id_tb_id_ = index_id_table_schema == nullptr ? OB_INVALID_ID : index_id_table_schema->get_table_id();
    refresh_threshold_ = arg.refresh_threshold_;
  }

  if (OB_SUCC(ret) && !in_recycle_bin) {
    refresh_method_ = schema::ObVectorRefreshMethod::MAX;
    if (OB_FAIL(ObVectorRefreshIndexExecutor::to_refresh_method(
            arg.refresh_type_, refresh_method_))) {
      LOG_WARN("fail to resolve refresh method", KR(ret));
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::resolve_rebuild_arg(
    const ObVectorRebuildIndexArg &arg) {
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *base_table_schema = nullptr;
  const share::schema::ObTableSchema *domain_table_schema = nullptr;
  const share::schema::ObTableSchema *index_id_table_schema = nullptr;

#ifdef DBMS_VECTOR_MOCK_TEST
  if (OB_FAIL(mock_resolve_and_check_table_valid(
          arg.idx_name_, arg.table_name_, arg.idx_vector_col_,
          base_table_schema, domain_table_schema, index_id_table_schema)))
#else
  if (OB_FAIL(resolve_and_check_table_valid(
          arg.idx_name_, arg.table_name_, arg.idx_vector_col_,
          base_table_schema, domain_table_schema, index_id_table_schema)))
#endif
  {
    LOG_WARN("fail to resolve and check table valid", KR(ret), K(arg));
  } else if (OB_ISNULL(base_table_schema) ||
             OB_ISNULL(domain_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schemas are null", K(ret), KP(base_table_schema), KP(domain_table_schema));
  } else if (domain_table_schema->is_vec_hnsw_index() && OB_ISNULL(index_id_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schemas are null", K(ret), KP(index_id_table_schema));
  } else {
    base_tb_id_ = base_table_schema->get_table_id();
    domain_tb_id_ = domain_table_schema->get_table_id();
    index_id_tb_id_ = index_id_table_schema == nullptr ? OB_INVALID_ID : index_id_table_schema->get_table_id();
    refresh_method_ = schema::ObVectorRefreshMethod::REBUILD_COMPLETE;
    // TODO:(@wangmiao) resolve vector index and check if it is the same as the
    // origin parameter.
    idx_parameters_ = arg.idx_parameters_;
    // TODO:(@wangmiao) idx_parallel_creation is not effective now.
    idx_parallel_creation_ = arg.idx_parallel_creation_;
    delta_rate_threshold_ = arg.delta_rate_threshold_;
  }
  // resolve idx_organization
  if (OB_SUCC(ret)) {
    // TODO:(@wangmiao) check if it is the same as origin idx_organization.
    if (OB_FAIL(ObVectorRefreshIndexExecutor::to_vector_index_organization(
            arg.idx_organization_, idx_organization_))) {
      LOG_WARN("fail to vector index organization", KR(ret));
    }
  }
  // resolve idx_distance_metric
  if (OB_SUCC(ret)) {
    // TODO:(@wangmiao) check if it is the same as origin idx_distance_metrics.
    if (OB_FAIL(ObVectorRefreshIndexExecutor::to_vector_index_distance_metric(
            arg.idx_distance_metrics_, idx_distance_metrics_))) {
      LOG_WARN("fail to vector index distance metric", KR(ret));
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::resolve_rebuild_inner_arg(const ObVectorRebuildIndexInnerArg &arg,
                                                            bool& in_recycle_bin)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *base_table_schema = nullptr;
  const share::schema::ObTableSchema *domain_table_schema = nullptr;
  const share::schema::ObTableSchema *index_id_table_schema = nullptr;
  in_recycle_bin = false;
  if (OB_FAIL(resolve_table_id_and_check_table_valid(arg.idx_table_id_, base_table_schema, domain_table_schema, index_id_table_schema, in_recycle_bin))) {
    LOG_WARN("fail to resolve table id and check table valid", KR(ret), K(arg));
  } else if (OB_UNLIKELY(in_recycle_bin)) {
    LOG_DEBUG("domain table is in recyclebin, do nothing");
  } else if (OB_ISNULL(base_table_schema) ||
             OB_ISNULL(domain_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret), KP(base_table_schema), KP(domain_table_schema));
  } else if (domain_table_schema->is_vec_hnsw_index() && OB_ISNULL(index_id_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret), KP(index_id_table_schema));
  } else {
    base_tb_id_ = base_table_schema->get_table_id();
    domain_tb_id_ = domain_table_schema->get_table_id();
    index_id_tb_id_ = index_id_table_schema != nullptr ? index_id_table_schema->get_table_id() : OB_INVALID_ID;
    refresh_method_ = schema::ObVectorRefreshMethod::REBUILD_COMPLETE;
    idx_parameters_ = arg.idx_parameters_;
    idx_parallel_creation_ = arg.idx_parallel_creation_;
    delta_rate_threshold_ = arg.delta_rate_threshold_;
  }

  if (OB_SUCC(ret) && !in_recycle_bin) {
    if (OB_FAIL(ObVectorRefreshIndexExecutor::to_vector_index_organization(
            arg.idx_organization_, idx_organization_))) {
      LOG_WARN("fail to vector index organization", KR(ret));
    }
  }
  if (OB_SUCC(ret) && !in_recycle_bin) {
    if (OB_FAIL(ObVectorRefreshIndexExecutor::to_vector_index_distance_metric(
            arg.idx_distance_metrics_, idx_distance_metrics_))) {
      LOG_WARN("fail to vector index distance metric", KR(ret));
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::do_refresh() {
  int ret = OB_SUCCESS;
  ObVectorRefreshIndexCtx refresh_ctx;
  refresh_ctx.tenant_id_ = tenant_id_;
  refresh_ctx.base_tb_id_ = base_tb_id_;
  refresh_ctx.domain_tb_id_ = domain_tb_id_;
  refresh_ctx.index_id_tb_id_ = index_id_tb_id_;
  refresh_ctx.refresh_method_ = refresh_method_;
  refresh_ctx.refresh_threshold_ = refresh_threshold_;

  ObVectorRefreshIdxTransaction trans;
  ObVectorIndexRefresher refresher;
  CK(OB_NOT_NULL(ctx_));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(ctx_->get_my_session(), ctx_->get_sql_proxy()))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (FALSE_IT(refresh_ctx.trans_ = &trans)) {
  } else if (OB_FAIL(refresher.init(*ctx_, refresh_ctx))) {
    LOG_WARN("fail to init refresher", KR(ret), K(refresh_ctx));
  } else if (OB_FAIL(refresher.refresh())) {
    LOG_WARN("fail to do refresh", KR(ret), K(refresh_ctx));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  if (ret == OB_EAGAIN) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Calling dbms_vector.refresh when other refresh/rebuild tasks may be running is");
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::do_refresh_with_retry()
{
  int ret = OB_SUCCESS;
  ObVectorRefreshIndexCtx refresh_ctx;
  refresh_ctx.tenant_id_ = tenant_id_;
  refresh_ctx.base_tb_id_ = base_tb_id_;
  refresh_ctx.domain_tb_id_ = domain_tb_id_;
  refresh_ctx.index_id_tb_id_ = index_id_tb_id_;
  refresh_ctx.refresh_method_ = refresh_method_;
  refresh_ctx.refresh_threshold_ = refresh_threshold_;
  int retry_cnt = 0;

  CK(OB_NOT_NULL(ctx_));
  while (OB_SUCC(ret) && OB_SUCC(ctx_->check_status())) {
    ObVectorRefreshIdxTransaction trans;
    ObVectorIndexRefresher refresher;
    if (OB_FAIL(trans.start(ctx_->get_my_session(), ctx_->get_sql_proxy()))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (FALSE_IT(refresh_ctx.trans_ = &trans)) {
    } else if (OB_FAIL(refresher.init(*ctx_, refresh_ctx))) {
      LOG_WARN("fail to init refresher", KR(ret), K(refresh_ctx));
    } else if (OB_FAIL(refresher.refresh())) {
      LOG_WARN("fail to do refresh", KR(ret), K(refresh_ctx));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
    if (OB_FAIL(ret)) {
      if (retry_cnt < MAX_REFRESH_RETRY_THRESHOLD &&
          ObVectorRefreshIndexExecutor::is_refresh_retry_ret_code(ret)) {
        ret = OB_SUCCESS;
        refresh_ctx.reuse();
        ++retry_cnt;
        ob_usleep(1LL * 1000 * 1000);
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::do_rebuild() {
  int ret = OB_SUCCESS;
  ObVectorRefreshIndexCtx refresh_ctx;
  refresh_ctx.tenant_id_ = tenant_id_;
  refresh_ctx.base_tb_id_ = base_tb_id_;
  refresh_ctx.domain_tb_id_ = domain_tb_id_;
  refresh_ctx.index_id_tb_id_ = index_id_tb_id_;
  refresh_ctx.refresh_method_ = refresh_method_;
  refresh_ctx.idx_organization_ = idx_organization_;
  refresh_ctx.idx_distance_metric_ = idx_distance_metrics_;
  refresh_ctx.idx_parameters_ = idx_parameters_;
  refresh_ctx.idx_parallel_creation_ = idx_parallel_creation_;
  refresh_ctx.delta_rate_threshold_ = delta_rate_threshold_;

  ObVectorRefreshIdxTransaction trans;
  ObVectorIndexRefresher refresher;
  CK(OB_NOT_NULL(ctx_));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(ctx_->get_my_session(), ctx_->get_sql_proxy()))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (FALSE_IT(refresh_ctx.trans_ = &trans)) {
  } else if (OB_FAIL(refresher.init(*ctx_, refresh_ctx))) {
    LOG_WARN("fail to init refresher", KR(ret), K(refresh_ctx));
  } else if (OB_FAIL(refresher.refresh())) {
    LOG_WARN("fail to do refresh", KR(ret), K(refresh_ctx));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  if (ret == OB_EAGAIN) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Calling dbms_vector.refresh when other refresh/rebuild tasks may be running is");
  }
  return ret;
}

int ObVectorRefreshIndexExecutor::do_rebuild_with_retry()
{
  int ret = OB_SUCCESS;
  ObVectorRefreshIndexCtx refresh_ctx;
  refresh_ctx.tenant_id_ = tenant_id_;
  refresh_ctx.base_tb_id_ = base_tb_id_;
  refresh_ctx.domain_tb_id_ = domain_tb_id_;
  refresh_ctx.index_id_tb_id_ = index_id_tb_id_;
  refresh_ctx.refresh_method_ = refresh_method_;
  refresh_ctx.idx_organization_ = idx_organization_;
  refresh_ctx.idx_distance_metric_ = idx_distance_metrics_;
  refresh_ctx.idx_parameters_ = idx_parameters_;
  refresh_ctx.idx_parallel_creation_ = idx_parallel_creation_;
  refresh_ctx.delta_rate_threshold_ = delta_rate_threshold_;
  int retry_cnt = 0;

  CK(OB_NOT_NULL(ctx_));
  while (OB_SUCC(ret) && OB_SUCC(ctx_->check_status())) {
    ObVectorRefreshIdxTransaction trans;
    ObVectorIndexRefresher refresher;
    if (OB_FAIL(trans.start(ctx_->get_my_session(), ctx_->get_sql_proxy()))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (FALSE_IT(refresh_ctx.trans_ = &trans)) {
    } else if (OB_FAIL(refresher.init(*ctx_, refresh_ctx))) {
      LOG_WARN("fail to init refresher", KR(ret), K(refresh_ctx));
    } else if (OB_FAIL(refresher.refresh())) {
      LOG_WARN("fail to do refresh", KR(ret), K(refresh_ctx));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
    if (OB_FAIL(ret)) {
      if (retry_cnt < MAX_REFRESH_RETRY_THRESHOLD &&
          ObVectorRefreshIndexExecutor::is_refresh_retry_ret_code(ret)) {
        ret = OB_SUCCESS;
        refresh_ctx.reuse();
        ++retry_cnt;
        ob_usleep(1LL * 1000 * 1000);
      }
    } else {
      break;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase