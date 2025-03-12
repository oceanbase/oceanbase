/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_mview_compaction_util.h"
#include "ob_basic_tablet_merge_ctx.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_result.h"
#include "ob_tenant_tablet_scheduler.h"

ERRSIM_POINT_DEF(EN_VALIDATE_COLLECT_MV_REFRESH, "validate collect mv refresh");

namespace oceanbase
{
namespace compaction
{

ObMviewMergeParameter::ObMviewMergeParameter()
  : database_id_(0),
    mview_id_(0),
    container_table_id_(0),
    container_tablet_id_(),
    schema_version_(0),
    refresh_scn_range_(),
    refresh_sql_count_(0),
    validation_sql_()
{
}

ObMviewMergeParameter::~ObMviewMergeParameter()
{
}

int ObMviewMergeParameter::init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  refresh_sql_count_ = REFRESH_SQL_COUNT;
  container_tablet_id_ = merge_param.static_param_.get_tablet_id();
  schema_version_ = merge_param.get_schema()->get_schema_version();
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<uint64_t, 1> table_ids;
  ObSchemaGetterGuard schema_guard;
  sql::ObSQLSessionInfo *session = nullptr;
  const ObTableSchema *table_schema = nullptr;
  const uint64_t tenant_id = MTL_ID();
  sql::ObFreeSessionCtx free_session_ctx;
  if (OB_FAIL(tablet_ids.push_back(container_tablet_id_))) {
    LOG_WARN("Failed to add tablet id", K(ret), K_(container_tablet_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tablet_to_table_history(tenant_id, tablet_ids, schema_version_, table_ids))) {
    LOG_WARN("Failed to get table id according to tablet id", K(ret), K_(container_tablet_id), K_(schema_version));
  } else if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected empty table id", K(ret), K_(container_tablet_id), K_(schema_version));
  } else if (OB_UNLIKELY(table_ids.at(0) == OB_INVALID_ID)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("table is deleted", K(ret), K_(container_tablet_id), K_(schema_version));
  } else {
    container_table_id_ = table_ids.at(0);
  }
  if (FAILEDx(ObMviewCompactionHelper::get_mview_id_from_container_table(container_table_id_, mview_id_))) {
    LOG_WARN("Failed to get mview id", K(ret), K(tenant_id), K_(container_table_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("Failed to get tenant schema guard", K(ret), K(tenant_id), K_(container_tablet_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, container_table_id_, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K_(container_table_id), K_(container_tablet_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null table schema", K(ret), K(tenant_id), K_(container_table_id), K_(container_tablet_id));
  } else {
    database_id_ = table_schema->get_database_id();
  }
  if (FAILEDx(refresh_scn_range_.start_scn_.convert_for_sql(merge_param.merge_version_range_.base_version_))) {
    LOG_WARN("Failed to convert to scn", K(ret), K(merge_param.merge_version_range_));
  } else if (OB_FAIL(refresh_scn_range_.end_scn_.convert_for_sql(merge_param.merge_version_range_.snapshot_version_))) {
    LOG_WARN("Failed to convert to scn", K(ret), K(merge_param.merge_version_range_));
  } else if (OB_UNLIKELY(!refresh_scn_range_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected refresh scn range", K(ret), K_(refresh_scn_range));
  } else if (OB_FAIL(ObMviewCompactionHelper::create_inner_session(merge_param.get_schema()->is_oracle_mode(), database_id_, free_session_ctx, session))) {
    LOG_WARN("Failed to create inner session", K(ret),K(tenant_id), K_(container_table_id), K_(container_tablet_id), K(database_id_));
  } else if (OB_FAIL(ObMviewCompactionHelper::generate_mview_refresh_sql(session, schema_guard, table_schema, merge_param.merge_range_,
                                                                         merge_param.static_param_.rowkey_read_info_, *this))) {
    LOG_WARN("Failed to generate mview refresh sql", K(ret), K(tenant_id), K_(container_table_id), K_(container_tablet_id));
  }
  if (OB_SUCC(ret)) {
    FLOG_INFO("[MVIEW COMPACTION]: success to init mview merge param", K(ret), K(*this));
  }
  ObMviewCompactionHelper::release_inner_session(free_session_ctx, session);
  return ret;
}

int64_t ObMviewMergeParameter::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(database_id),
       K_(mview_id),
       K_(container_table_id),
       K_(container_tablet_id),
       K_(schema_version),
       K_(refresh_scn_range),
       K_(refresh_sql_count),
       K_(validation_sql));
  J_COMMA();
  if (is_valid()) {
    for (int64_t i = 0; i < refresh_sql_count_; ++i) {
      J_KV(K(i));
      J_COMMA();
      J_KV("refresh_sql", refresh_sqls_[i]);
      J_COMMA();
    }
  }
  J_OBJ_END();
  return pos;
}

ObMviewCompactionValidation::ObMviewCompactionValidation()
  : first_validated_(false),
    second_validated_(false),
    force_validated_(false),
    merge_version_(0)
{
}

void ObMviewCompactionValidation::refresh(const int64_t new_version)
{
  if (new_version != merge_version_) {
    merge_version_ = new_version;
    ATOMIC_STORE(&first_validated_, false);
    ATOMIC_STORE(&second_validated_, false);
    force_validated_ = false;
  }
}

bool ObMviewCompactionValidation::need_do_validation()
{
  bool need = force_validated_;
  if (!need) {
    if (false == ATOMIC_VCAS(&first_validated_, false, true)) {
      need = true;
      LOG_INFO("[MVIEW COMPACTION]: need do validation for first task");
    } else {
      bool choice = rand() % RANDOM_SELECT_BASE == 0;
      if (choice && false == ATOMIC_VCAS(&second_validated_, false, true)) {
        need = true;
        LOG_INFO("[MVIEW COMPACTION]: need do validation for second random task");
      }
    }
  }
  if (!need && EN_VALIDATE_COLLECT_MV_REFRESH) {
    need = true;
    LOG_INFO("[MVIEW COMPACTION]: need do validation for trace point");
  }
  return need;
}

int ObMviewCompactionHelper::get_mview_id_from_container_table(const uint64_t container_table_id, uint64_t &mview_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT table_id FROM %s WHERE data_table_id = %ld AND table_type = %d",
                              OB_ALL_TABLE_TNAME, container_table_id, MATERIALIZED_VIEW))) {
    LOG_WARN("Failed to assign sql", K(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("Failed to execute sql", K(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null result", K(ret), K(sql), K(tenant_id));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("Failed to get next result", K(ret), K(sql), K(tenant_id));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "table_id", mview_id, uint64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("Failed to get int from result", K(ret), K(sql));
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(OB_ITER_END != result->next())) {
        LOG_WARN("Unexpected ret, must be only one result", K(ret));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

int ObMviewCompactionHelper::generate_mview_refresh_sql(
    sql::ObSQLSessionInfo *session,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema *table_schema,
    const blocksstable::ObDatumRange &merge_range,
    const ObRowkeyReadInfo *rowkey_read_info,
    ObMviewMergeParameter &mview_param)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t sub_part_idx = OB_INVALID_INDEX;
  sql::ObMVProvider mv_provider(tenant_id, mview_param.mview_id_);
  if (table_schema->is_partitioned_table()) {
    if (OB_FAIL(table_schema->get_part_idx_by_tablet(mview_param.container_tablet_id_, part_idx, sub_part_idx))) {
      LOG_WARN("Failed to get part idx by tablet", K(ret), K(mview_param));
    }
  }
  if (OB_SUCC(ret)) {
    ObArenaAllocator allocator("MVCompaction", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
    ObNewRange sql_range;
    sql_range.table_id_ = mview_param.mview_id_;
    if (merge_range.is_whole_range()) {
      sql_range.set_whole_range();
    } else if (OB_FAIL(convert_datum_range(allocator, rowkey_read_info, merge_range, sql_range))) {
      LOG_WARN("Failed to convert datum range", K(ret), K(mview_param));
    }
    if (FAILEDx(mv_provider.init_mv_provider(mview_param.refresh_scn_range_.start_scn_,
                                             mview_param.refresh_scn_range_.end_scn_,
                                             &schema_guard,
                                             session,
                                             part_idx,
                                             sub_part_idx,
                                             sql_range))) {
      LOG_WARN("Failed to init mv provider", K(ret), K(tenant_id), K(mview_param));
    } else {
      LOG_INFO("[MVIEW COMPACTION] yuanzhe debug", K(part_idx), K(sub_part_idx), K(sql_range), K(merge_range));
    }
  }
  if (OB_SUCC(ret)) {
    ObMviewMergeSQL *refresh_sqls = mview_param.refresh_sqls_;
    const common::ObIArray<ObString> &mv_ops = mv_provider.get_operators();
    if (OB_UNLIKELY(mview_param.refresh_sql_count_ + 1 != mv_ops.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected number of refresh sql", K(ret), K(mv_ops.count()));
    }
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < mview_param.refresh_sql_count_; ++i) {
      refresh_sqls[i].type_ = ObMviewMergeIterType::MVIEW_REPLACE;
      if (OB_FAIL(refresh_sqls[i].sql_.append(mv_ops.at(i)))) {
        LOG_WARN("Failed to append mv sql", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(mview_param.validation_sql_.append(mv_ops.at(i)))) {
        LOG_WARN("Failed to append mv validation sql", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObMviewCompactionHelper::create_inner_session(
    const bool is_oracle_mode,
    const uint64_t database_id,
    sql::ObFreeSessionCtx &free_session_ctx,
    sql::ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  session = nullptr;
  uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  const schema::ObTenantSchema *tenant_info = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("Failed to create sess id", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
             tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), session))) {
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session = nullptr;
    LOG_WARN("Failed to create session", K(ret), K(sid), K(tenant_id), K(database_id));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  if (FAILEDx(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("Failed to get tenant schema guard", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("Failed to get tenant info", K(ret), K(tenant_id), K(database_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null tenant schema", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_id, database_schema))) {
    LOG_WARN("Failed to get database schema", K(ret), K(tenant_id), K(database_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null database schema", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(session->load_default_sys_variable(false, false))) {
    LOG_WARN("Failed to load default sys variable", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(session->load_default_configs_in_pc())) {
    LOG_WARN("Failed to load default configs in pc", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(session->init_tenant(tenant_info->get_tenant_name(), tenant_id))) {
     LOG_WARN("Failed to init tenant in session", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(session->set_default_database(database_schema->get_database_name()))) {
    LOG_WARN("Failed to set default database", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(set_params_to_session(is_oracle_mode, session))) {
    LOG_WARN("Failed to set params to session", K(ret));
  } else {
    session->set_inner_session();
    session->set_compatibility_mode(is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE);
    session->get_ddl_info().set_major_refreshing_mview(true);
    session->get_ddl_info().set_refreshing_mview(true);
    session->set_database_id(database_id);
    session->set_query_start_time(ObTimeUtil::current_time());
    LOG_INFO("[MVIEW COMPACTION]: Succ to create inner session", K(ret), K(tenant_id), K(database_id), KP(session));
  }
  if (OB_FAIL(ret)) {
    release_inner_session(free_session_ctx, session);
  }
  return ret;
}

void ObMviewCompactionHelper::release_inner_session(sql::ObFreeSessionCtx &free_session_ctx, sql::ObSQLSessionInfo *&session)
{
  if (nullptr != session) {
    LOG_INFO("[MVIEW COMPACTION]: Release inner session", KP(session));
    session->get_ddl_info().set_major_refreshing_mview(false);
    session->get_ddl_info().set_refreshing_mview(false);
    session->set_session_sleep();
    GCTX.session_mgr_->revert_session(session);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
    session = nullptr;
  }
}

int ObMviewCompactionHelper::create_inner_connection(sql::ObSQLSessionInfo *session, common::sqlclient::ObISQLConnection *&connection)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnectionPool *conn_pool = static_cast<observer::ObInnerSQLConnectionPool *>(GCTX.sql_proxy_->get_pool());
  if (OB_FAIL(conn_pool->acquire(session, connection))) {
    LOG_WARN("Failed to acquire conn_", K(ret));
  } else {
    LOG_INFO("[MVIEW COMPACTION]: Succ to create inner connection", K(ret), KP(session), KP(connection));
  }
  return ret;
}

void ObMviewCompactionHelper::release_inner_connection(common::sqlclient::ObISQLConnection *&connection)
{
  if (nullptr != connection) {
    LOG_INFO("[MVIEW COMPACTION]: Release inner connection", KP(connection));
    GCTX.sql_proxy_->get_pool()->release(connection, true);
    connection = nullptr;
  }
}

int ObMviewCompactionHelper::set_params_to_session(const bool is_oracle_mode, sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObObj param_val;
  param_val.set_int(REFRESH_SQL_TIMEOUT_US);
  OZ(session->update_sys_variable(SYS_VAR_OB_QUERY_TIMEOUT, param_val));
  if (OB_SUCC(ret)) {
    param_val.set_int(ObConsistencyLevel::WEAK);
    OZ(session->update_sys_variable(SYS_VAR_OB_READ_CONSISTENCY, param_val));
  }
  if (OB_SUCC(ret)) {
    param_val.set_int(is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE);
    OZ(session->update_sys_variable(SYS_VAR_OB_COMPATIBILITY_MODE, param_val));
  }
  if (OB_SUCC(ret)) {
    ObObj result_val;
    if (OB_FAIL(session->get_sys_variable(SYS_VAR_OB_QUERY_TIMEOUT, result_val))) {
      LOG_WARN("Failed to get sys var", K(ret));
    } else {
      LOG_INFO("[MVIEW COMPACTION]: SYS_VAR_OB_QUERY_TIMEOUT=", K(result_val));
    }
    if (FAILEDx(session->get_sys_variable(SYS_VAR_OB_READ_CONSISTENCY, result_val))) {
      LOG_WARN("Failed to get sys var", K(ret));
    } else {
      LOG_INFO("[MVIEW COMPACTION]: SYS_VAR_OB_READ_CONSISTENCY=", K(result_val));
    }
  }
  return ret;
}

int ObMviewCompactionHelper::validate_row_count(const ObMergeParameter &merge_param, const int64_t major_row_count)
{
  int ret = OB_SUCCESS;
  int64_t join_row_count = 0;
  sql::ObFreeSessionCtx free_session_ctx;
  sql::ObSQLSessionInfo *session = nullptr;
  sqlclient::ObISQLConnection *conn = nullptr;
  observer::ObInnerSQLResult *sql_result = nullptr;
  SMART_VAR(ObISQLClient::ReadResult, read_result) {
    const ObSqlString &sql = merge_param.mview_merge_param_->validation_sql_;
    if (OB_FAIL(create_inner_session(merge_param.get_schema()->is_oracle_mode(),
                                    merge_param.mview_merge_param_->database_id_,
                                    free_session_ctx, session))) {
      LOG_WARN("Failed to create inner session", K(ret), KPC(merge_param.mview_merge_param_));
    } else if (OB_FAIL(ObMviewCompactionHelper::create_inner_connection(session, conn))) {
      LOG_WARN("Failed to create inner connection", K(ret));
    } else {
      if (OB_FAIL(conn->execute_read(GCONF.cluster_id, MTL_ID(), sql.ptr(), read_result))) {
        LOG_WARN("Failed to execute", K(ret), K(sql));
      } else if (OB_ISNULL(sql_result = static_cast<observer::ObInnerSQLResult *>(read_result.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null sql result", K(ret), K(sql));
      } else if (OB_FAIL(sql_result->next())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to get next row", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        const ObNewRow *new_row = sql_result->get_row();
        if (OB_UNLIKELY(nullptr == new_row || 1 != new_row->get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected result row", K(ret), KPC(new_row));
        } else if (merge_param.get_schema()->is_oracle_mode()) {
          const number::ObNumber nmb(new_row->get_cell(0).get_number());
          if (OB_FAIL(nmb.extract_valid_int64_with_trunc(join_row_count))) {
            STORAGE_LOG(WARN, "Failed to cast number to int64", K(ret), K(new_row->get_cell(0)));
          }
        } else {
          join_row_count = new_row->get_cell(0).get_int();
        }
      }
    }
  }
  if (OB_SUCC(ret) && major_row_count != join_row_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MV major row count is not equal with join row count", K(ret), K(major_row_count), K(join_row_count));
    MTL(ObTenantTabletScheduler*)->get_mview_validation().set_force_do_validation();
  }
  read_result.~ReadResult();
  ObMviewCompactionHelper::release_inner_connection(conn);
  ObMviewCompactionHelper::release_inner_session(free_session_ctx, session);
  return ret;
}

int ObMviewCompactionHelper::convert_datum_range(
    common::ObIAllocator &allocator,
    const storage::ObRowkeyReadInfo *rowkey_read_info,
    const blocksstable::ObDatumRange &merge_range,
    ObNewRange &sql_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey_read_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null rowkey read info", K(ret));
  } else {
    sql_range.border_flag_ = merge_range.get_border_flag();
    const int64_t schema_rowkey_cnt = rowkey_read_info->get_schema_rowkey_count();
    const common::ObIArray<share::schema::ObColDesc> &col_descs = rowkey_read_info->get_columns_desc();
    void *buf = nullptr;
    ObObj *start_key = nullptr;
    ObObj *end_key = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * schema_rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory for start key", K(ret));
    } else if (FALSE_IT(start_key = new (buf) ObObj[schema_rowkey_cnt])) {
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * schema_rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory for end key", K(ret));
    } else if (FALSE_IT(end_key = new (buf) ObObj[schema_rowkey_cnt])) {
    } else {
       for (int64_t i = 0; OB_SUCC(ret) && i < schema_rowkey_cnt; ++i) {
        if (i < merge_range.get_start_key().get_datum_cnt()) {
          if (OB_FAIL(merge_range.get_start_key().get_datum(i).to_obj_enhance(start_key[i], col_descs.at(i).col_type_))) {
            LOG_WARN("Failed to convert to obj", K(ret), K(i));
          }
        } else {
          start_key[i].set_min_value();
        }
        if (OB_FAIL(ret)) {
        } else if (i < merge_range.get_end_key().get_datum_cnt()) {
          if (OB_FAIL(merge_range.get_end_key().get_datum(i).to_obj_enhance(end_key[i], col_descs.at(i).col_type_))) {
            LOG_WARN("Failed to convert to obj", K(ret), K(i));
          }
        } else {
          end_key[i].set_max_value();
        }
      }
      if (OB_SUCC(ret)) {
        sql_range.start_key_.assign(start_key, schema_rowkey_cnt);
        sql_range.end_key_.assign(end_key, schema_rowkey_cnt);
      }
    }
  }
  return ret;
}

}
}
