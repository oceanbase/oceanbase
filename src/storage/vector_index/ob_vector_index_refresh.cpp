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

#include "storage/vector_index/ob_vector_index_refresh.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"

namespace oceanbase {
namespace storage {
using namespace common;
using namespace observer;
using namespace share;
using namespace share::schema;
using namespace sql;

ObVectorIndexRefresher::ObVectorIndexRefresher()
  : ctx_(nullptr), refresh_ctx_(nullptr), is_inited_(false) {}

ObVectorIndexRefresher::~ObVectorIndexRefresher() {}

int ObVectorIndexRefresher::init(sql::ObExecContext &ctx,
                                 ObVectorRefreshIndexCtx &refresh_ctx) {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObVectorIndexRefresher init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx.get_my_session() ||
                         nullptr == ctx.get_sql_proxy() ||
                         nullptr == refresh_ctx.trans_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ctx), K(refresh_ctx));
  } else {
    ctx_ = &ctx;
    refresh_ctx_ = &refresh_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObVectorIndexRefresher::refresh() {
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(refresh_ctx_));
  if (OB_SUCC(ret)) {
    const ObVectorRefreshMethod refresh_type = refresh_ctx_->refresh_method_;
    if (ObVectorRefreshMethod::REBUILD_COMPLETE == refresh_type) {
      if (OB_FAIL(do_rebuild())) {
        LOG_WARN("fail to rebuild index", KR(ret));
      }
    } else if (ObVectorRefreshMethod::REFRESH_DELTA == refresh_type) {
      if (OB_FAIL(do_refresh())) {
        LOG_WARN("fail to refresh index", KR(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("refresh type is not supported", KR(ret), K(refresh_type));
    }
  }
  return ret;
}

int ObVectorIndexRefresher::get_current_scn(share::SCN &current_scn) {
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", KR(ret));
  } else {
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx,
                                                     DEFAULT_TIMEOUT))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    } else if (OB_FAIL(txs->get_read_snapshot_version(
                   timeout_ctx.get_abs_timeout(), current_scn))) {
      LOG_WARN("get read snapshot version", KR(ret));
    }
  }
  return ret;
}

int ObVectorIndexRefresher::lock_domain_tb(
    ObVectorRefreshIdxTransaction &trans, const uint64_t tenant_id,
    const uint64_t domain_tb_id, const bool try_lock) {
  int ret = OB_SUCCESS;
  ObTableLockOwnerID owner_id;
  if (OB_UNLIKELY(!trans.is_started() || OB_INVALID_TENANT_ID == tenant_id ||
                  OB_INVALID_ID == domain_tb_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(trans.is_started()), K(tenant_id),
             K(domain_tb_id));
  } else if (OB_FAIL(owner_id.convert_from_value(
                 ObLockOwnerType::DEFAULT_OWNER_TYPE, get_tid_cache()))) {
    LOG_WARN("failed to get owner id", K(ret), K(get_tid_cache()));
  } else {
    const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
    ObInnerSQLConnection *conn = nullptr;
    ObLockObjRequest lock_arg;
    lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_REFRESH_VECTOR_INDEX;
    lock_arg.obj_id_ = domain_tb_id;
    lock_arg.owner_id_ = owner_id;
    lock_arg.lock_mode_ = EXCLUSIVE;
    lock_arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(
                      trans.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("conn_ is NULL", KR(ret));
    } else if (try_lock) {
      lock_arg.timeout_us_ = 0;
    } else {
      ObTimeoutCtx timeout_ctx;
      if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx,
                                                       DEFAULT_TIMEOUT))) {
        LOG_WARN("fail to set default timeout ctx", KR(ret));
      } else {
        lock_arg.timeout_us_ = timeout_ctx.get_timeout();
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("lock obj start", K(lock_arg));
      if (OB_FAIL(
              ObInnerConnectionLockUtil::lock_obj(tenant_id, lock_arg, conn))) {
        LOG_WARN("fail to lock obj", KR(ret));
      }
      LOG_DEBUG("lock obj end", KR(ret));
    }
  }
  return ret;
}

int ObVectorIndexRefresher::get_table_row_count(const ObString &db_name,
                                                const ObString &table_name,
                                                const share::SCN &scn,
                                                int64_t &row_cnt) {
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(refresh_ctx_));
  if (OB_SUCC(ret)) {
    const uint64_t exec_tenant_id =
        ObSchemaUtils::get_exec_tenant_id(refresh_ctx_->tenant_id_);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT COUNT(*) AS CNT FROM `%.*s`.`%.*s` AS OF SNAPSHOT %ld",
              static_cast<int>(db_name.length()), db_name.ptr(),
              static_cast<int>(table_name.length()), table_name.ptr(),
              scn.get_val_for_tx()))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_ISNULL(refresh_ctx_->trans_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("trans is null", K(ret));
      } else if (OB_FAIL(refresh_ctx_->trans_->read(res, refresh_ctx_->tenant_id_,
                                                    sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get count", KR(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "CNT", row_cnt, int64_t);
        LOG_DEBUG("############# DBMS_VECTOR ############### get table row cnt ",
                  K(table_name), K(row_cnt));
      }
    }
  }
  return ret;
}

int ObVectorIndexRefresher::get_vector_index_col_names(
    const ObTableSchema *table_schema,
    bool is_collect_col_id,
    ObIArray<uint64_t>& col_ids,
    ObSqlString &col_names) {
  int ret = OB_SUCCESS;
  ObArray<ObString> col_name_array;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", KR(ret));
  } else if (is_collect_col_id && col_ids.count() != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collect col id while col id array not empty", K(ret), K(col_ids), KPC(table_schema));
  } else if (!is_collect_col_id && col_ids.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not collect col id while col id array empty", K(ret), K(col_ids), KPC(table_schema));
  } else if (!is_collect_col_id) {  // not collect id, index id table
    // first get scn column
    for (ObTableSchema::const_column_iterator iter =
             table_schema->column_begin();
         OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column schema is NULL", K(ret));
      } else if (column_schema->get_column_name_str().prefix_match(OB_VEC_SCN_COLUMN_NAME_PREFIX)) {
        if (OB_FAIL(col_name_array.push_back(column_schema->get_column_name_str()))) {
          LOG_WARN("fail to push back col name", K(ret), KPC(column_schema));
        }
      }
    }
    // and than get col id cols
    for (int64_t i = 0; i < col_ids.count() && OB_SUCC(ret); i++) {
      const ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(col_ids.at(i));
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column schema is NULL", K(ret));
      } else if (OB_FAIL(col_name_array.push_back(column_schema->get_column_name_str()))) {
        LOG_WARN("fail to push back col name", K(ret), KPC(column_schema));
      }
    }
  } else { // collect col id, for delta buffer table, get type,vid,part key cols
    for (ObTableSchema::const_column_iterator iter =
             table_schema->column_begin();
         OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column schema is NULL", K(ret));
      } else if (column_schema->get_column_name_str().prefix_match(OB_VEC_SCN_COLUMN_NAME_PREFIX)) {
        // do nothing
      } else if (column_schema->get_column_name_str().prefix_match(OB_VEC_VECTOR_COLUMN_NAME_PREFIX)) {
        // do nothing
      } else if (OB_FAIL(col_ids.push_back(column_schema->get_column_id()))) {
        LOG_WARN("fail to push back col id", K(ret), KPC(column_schema));
      } else if (OB_FAIL(col_name_array.push_back(column_schema->get_column_name_str()))) {
        LOG_WARN("fail to push back col name", K(ret), KPC(column_schema));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (col_name_array.count() < 2) { // at least type vid col
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column array count is not expected", KR(ret), K(col_name_array));
    }
  }
  for (int64_t i = 0; i < col_name_array.count() && OB_SUCC(ret); i++) {
    bool last_col = (i == (col_name_array.count() - 1));
    ObString &cur_col_name = col_name_array.at(i);
    if (last_col && OB_FAIL(col_names.append_fmt("%.*s", static_cast<int>(cur_col_name.length()), cur_col_name.ptr()))) {
      LOG_WARN("fail to append str", KR(ret), K(cur_col_name));
    } else if (!last_col && OB_FAIL(col_names.append_fmt("%.*s, ", static_cast<int>(cur_col_name.length()), cur_col_name.ptr()))) {
      LOG_WARN("fail to append str", KR(ret), K(cur_col_name));
    }
  }
  return ret;
}

int ObVectorIndexRefresher::lock_domain_table_for_refresh() {
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(refresh_ctx_));
  CK(OB_NOT_NULL(refresh_ctx_->trans_));
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = refresh_ctx_->tenant_id_;
    const uint64_t domain_tb_id = refresh_ctx_->domain_tb_id_;
    int64_t retries = 0;
    while (OB_SUCC(ret) && OB_SUCC(ctx_->check_status())) {
      if (OB_FAIL(ObVectorIndexRefresher::lock_domain_tb(
              *(refresh_ctx_->trans_), tenant_id, domain_tb_id, true))) {
        if (OB_UNLIKELY(OB_TRY_LOCK_ROW_CONFLICT != ret)) {
          LOG_WARN("fail to lock delta_buf_table for refresh", KR(ret),
                  K(tenant_id), K(domain_tb_id));
        } else {
          ret = OB_SUCCESS;
          ++retries;
          if (retries % 10 == 0) {
            LOG_WARN("retry too many times", K(retries), K(tenant_id),
                    K(domain_tb_id));
          }
          ob_usleep(100LL * 1000);
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObVectorIndexRefresher::do_refresh() {
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  ObSQLSessionInfo *session_info = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *domain_table_schema = nullptr;
  const ObTableSchema *index_id_tb_schema = nullptr;
  const ObDatabaseSchema *db_schema = nullptr;
  int64_t domain_table_row_cnt = 0;
  ObSqlString index_id_tb_col_names;
  ObSqlString domain_tb_col_names;
  ObTimeoutCtx timeout_ctx;
  const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT =
      ObDDLUtil::calc_inner_sql_execute_timeout();

  ObArray<uint64_t> col_ids;
  if (OB_ISNULL(refresh_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("refresh_ctx is null", K(ret));
  } else if (OB_ISNULL(refresh_ctx_->trans_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans is null", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (OB_FALSE_IT(tenant_id = refresh_ctx_->tenant_id_)) {
  } else if (OB_FAIL(lock_domain_table_for_refresh())) {
    LOG_WARN("fail to lock delta_buf_table for refresh", KR(ret));
  } else if (OB_ISNULL(session_info = ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret), KPC(ctx_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObVectorIndexRefresher::get_current_scn(refresh_ctx_->scn_))) {
    LOG_WARN("fail to get current scn", KR(ret));
  } else {
    DEBUG_SYNC(BEFORE_DBMS_VECTOR_REFRESH);
  }
  // get delta_buf_table row count
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                 refresh_ctx_->domain_tb_id_,
                                                 domain_table_schema))) {
    LOG_WARN("fail to get delta buf table schema", KR(ret), K(tenant_id),
             K(refresh_ctx_->domain_tb_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id, refresh_ctx_->index_id_tb_id_,
                 index_id_tb_schema))) {
    LOG_WARN("fail to get index id table schema", KR(ret), K(tenant_id),
             K(refresh_ctx_->index_id_tb_id_));
  } else if (OB_ISNULL(domain_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delta_buf_table not exist", KR(ret), K(tenant_id),
             K(refresh_ctx_->domain_tb_id_));
  } else if (OB_ISNULL(index_id_tb_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index_id_table not exist", KR(ret), K(tenant_id),
             K(refresh_ctx_->index_id_tb_id_));
  } else if (OB_UNLIKELY(INDEX_STATUS_AVAILABLE != domain_table_schema->get_index_status() ||
                         INDEX_STATUS_AVAILABLE != index_id_tb_schema->get_index_status())) {
    if ((INDEX_STATUS_AVAILABLE == domain_table_schema->get_index_status() ||
         INDEX_STATUS_UNAVAILABLE == domain_table_schema->get_index_status()) &&
        (INDEX_STATUS_AVAILABLE == index_id_tb_schema->get_index_status() ||
         INDEX_STATUS_UNAVAILABLE == index_id_tb_schema->get_index_status())) {
      // Return OB_EAGAIN for dbms_vector.refresh_index_inner to do inner retry.
      // For dbms_vector.refresh_index, the error code will return to user.
      ret = OB_EAGAIN;
      LOG_WARN("delta buffer table or index id table is not available", K(ret), K(domain_table_schema->get_index_status()),
              K(index_id_tb_schema->get_index_status()));
    } else {
      ret = OB_ERR_INDEX_UNAVAILABLE;
    }
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id, domain_table_schema->get_database_id(),
                 db_schema))) {
    LOG_WARN("fail to get db schema", KR(ret), K(tenant_id),
             K(domain_table_schema->get_database_id()));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database not exist", KR(ret));
  } else if (OB_UNLIKELY(db_schema->is_in_recyclebin() ||
                         domain_table_schema->is_in_recyclebin() ||
                         index_id_tb_schema->is_in_recyclebin())) {
    // do nothing
    LOG_DEBUG("table or db in recyclebin");
  } else if (OB_FAIL(get_table_row_count(
                 db_schema->get_database_name_str(),
                 domain_table_schema->get_table_name_str(), refresh_ctx_->scn_,
                 domain_table_row_cnt))) {
    LOG_WARN("fail to get delta_buf_table row count", KR(ret),
             K(domain_table_schema->get_table_name_str()));
  } else if (domain_table_row_cnt < refresh_ctx_->refresh_threshold_) {
    // refreshing is not triggered.
  }
#ifndef DBMS_VECTOR_MOCK_TEST
  else if (OB_FAIL(get_vector_index_col_names(domain_table_schema,
                                              true,
                                              col_ids,
                                              domain_tb_col_names))) {
    LOG_WARN("fail to get vid & type col names", KR(ret),
             K(domain_table_schema->get_table_name_str()));
  } else if (OB_FAIL(get_vector_index_col_names(index_id_tb_schema,
                                                false,
                                                col_ids,
                                                index_id_tb_col_names))) {
    LOG_WARN("fail to get vid & type col names", KR(ret),
             K(index_id_tb_schema->get_table_name_str()));
  }
#endif
  else if (OB_FAIL(
               timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
    LOG_WARN("set trx timeout failed", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
    LOG_WARN("set timeout failed", K(ret));
  } else {
    // do refresh
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      // 1. insert into index_id_table select ... from delta_buf_table
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = nullptr;
        ObSqlString insert_sel_sql;
#ifdef DBMS_VECTOR_MOCK_TEST
        if (OB_FAIL(insert_sel_sql.append_fmt(
                "INSERT INTO `%.*s`.`%.*s` SELECT * FROM `%.*s`.`%.*s` WHERE "
                "ora_rowscn <= %lu",
                static_cast<int>(db_schema->get_database_name_str().length()),
                db_schema->get_database_name_str().ptr(),
                static_cast<int>(
                    index_id_tb_schema->get_table_name_str().length()),
                index_id_tb_schema->get_table_name_str().ptr(),
                static_cast<int>(db_schema->get_database_name_str().length()),
                db_schema->get_database_name_str().ptr(),
                static_cast<int>(
                    domain_table_schema->get_table_name_str().length()),
                domain_table_schema->get_table_name_str().ptr(),
                refresh_ctx_->scn_.get_val_for_sql())))
#else
        if (OB_FAIL(insert_sel_sql.append_fmt(
                "INSERT INTO `%.*s`.`%.*s` (%.*s) SELECT ora_rowscn, %.*s FROM "
                "`%.*s`.`%.*s` WHERE ora_rowscn <= %lu",
                static_cast<int>(db_schema->get_database_name_str().length()),
                db_schema->get_database_name_str().ptr(),
                static_cast<int>(
                    index_id_tb_schema->get_table_name_str().length()),
                index_id_tb_schema->get_table_name_str().ptr(),
                static_cast<int>(index_id_tb_col_names.length()),
                index_id_tb_col_names.ptr(),
                static_cast<int>(domain_tb_col_names.length()),
                domain_tb_col_names.ptr(),
                static_cast<int>(db_schema->get_database_name_str().length()),
                db_schema->get_database_name_str().ptr(),
                static_cast<int>(
                    domain_table_schema->get_table_name_str().length()),
                domain_table_schema->get_table_name_str().ptr(),
                refresh_ctx_->scn_.get_val_for_sql())))
#endif
        {
          LOG_WARN("fail to assign sql", KR(ret));
        } else if (OB_FAIL(refresh_ctx_->trans_->write(
                       tenant_id, insert_sel_sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute insert into select sql", KR(ret),
                   K(tenant_id), K(insert_sel_sql));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      // 2. delete from delta_buf_table
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = nullptr;
        ObSqlString delete_sql;
        if (OB_FAIL(delete_sql.append_fmt(
                "DELETE FROM `%.*s`.`%.*s` WHERE ora_rowscn <= %lu",
                static_cast<int>(db_schema->get_database_name_str().length()),
                db_schema->get_database_name_str().ptr(),
                static_cast<int>(
                    domain_table_schema->get_table_name_str().length()),
                domain_table_schema->get_table_name_str().ptr(),
                refresh_ctx_->scn_.get_val_for_sql()))) {
          LOG_WARN("fail to assign sql", KR(ret));
        } else if (OB_FAIL(refresh_ctx_->trans_->write(
                       tenant_id, delete_sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute insert into select sql", KR(ret),
                   K(tenant_id), K(delete_sql));
        }
      }
    }

    // freeze major tablet
    ObArray<uint64_t> tablet_ids;
    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = nullptr;
        ObSqlString select_sql;
        if (OB_FAIL(select_sql.append_fmt(
                "select tablet_id from oceanbase.DBA_OB_TABLE_LOCATIONS where table_id = %lu",
                domain_table_schema->get_table_id()))) {
          LOG_WARN("fail to assign sql", KR(ret));
        } else if (OB_FAIL(refresh_ctx_->trans_->read(
                       res, domain_table_schema->get_tenant_id(), select_sql.ptr()))) {
          LOG_WARN("fail to execute select sql", KR(ret),
                   K(tenant_id), K(delete_sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is NULL", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            uint64_t tablet_id = 0;
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END != ret) {
                LOG_WARN("next failed", K(ret));
              }
            } else {
              EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", tablet_id, uint64_t);
              if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
                LOG_WARN("failed to store tablet id", K(ret));
              }
            }
          }

          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }

    if (OB_SUCC(ret) && tablet_ids.count() > 0) {
      FLOG_INFO("get freeze tablet id", K(tablet_ids));
      for (int i = 0 ; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
        int64_t affected_rows = 0;
        uint64_t tablet_id = tablet_ids.at(i);
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          common::sqlclient::ObMySQLResult *result = nullptr;
          ObSqlString freeze_sql;
          if (OB_FAIL(freeze_sql.append_fmt(
                "alter system major freeze tablet_id = %lu", tablet_id))) {
            LOG_WARN("fail to assign sql", KR(ret));
          } else if (OB_FAIL(refresh_ctx_->trans_->write(tenant_id, freeze_sql.ptr(), affected_rows))) {
            ret = OB_SUCCESS;
            LOG_WARN("fail to execute major freeze sql, continue other tablet id", K(tablet_id),
                      K(tenant_id), K(freeze_sql));
          } else {
            LOG_INFO("execute major freeze success", K(tablet_id), K(affected_rows));
          }
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexRefresher::do_rebuild() {
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  ObSQLSessionInfo *session_info = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *base_table_schema = nullptr;
  const ObTableSchema *domain_table_schema = nullptr;
  const ObTableSchema *index_id_tb_schema = nullptr;
  const ObDatabaseSchema *db_schema = nullptr;
  int64_t base_table_row_cnt = 0;
  int64_t domain_table_row_cnt = 0;
  int64_t index_id_table_row_cnt = 0;
  bool triggered = true;
  // refresh_ctx_->delta_rate_threshold_ = 0; // yjl, for test
  if (OB_ISNULL(refresh_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("refresh_ctx is null", K(ret));
  } else if (OB_ISNULL(refresh_ctx_->trans_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans is null", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (OB_FALSE_IT(tenant_id = refresh_ctx_->tenant_id_)) {
  } else if (OB_FAIL(lock_domain_table_for_refresh())) {
    LOG_WARN("fail to lock domain for refresh", KR(ret));
  } else if (OB_ISNULL(session_info = ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret), KPC(ctx_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObVectorIndexRefresher::get_current_scn(refresh_ctx_->scn_))) {
    LOG_WARN("fail to get current scn", KR(ret));
  } else {
    DEBUG_SYNC(BEFORE_DBMS_VECTOR_REBUILD);
  }
  // 1. get base_table row count ( if need )
  // 2. get domain_table row count (if need )
  // 3. get index_id_table row count (if need)
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, refresh_ctx_->base_tb_id_, base_table_schema))) {
    LOG_WARN("fail to get base table schema", KR(ret), K(tenant_id),K(refresh_ctx_->base_tb_id_));
  } else if (OB_ISNULL(base_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base_table not exist", KR(ret), K(tenant_id), K(refresh_ctx_->base_tb_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, refresh_ctx_->domain_tb_id_, domain_table_schema))) {
    LOG_WARN("fail to get delta buf table schema", KR(ret), K(tenant_id), K(refresh_ctx_->domain_tb_id_));
  } else if (OB_ISNULL(domain_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delta_buf_table not exist", KR(ret), K(tenant_id), K(refresh_ctx_->domain_tb_id_));
  } else if (OB_UNLIKELY(INDEX_STATUS_AVAILABLE != domain_table_schema->get_index_status())) {
    ret = OB_ERR_INDEX_UNAVAILABLE;
    if (INDEX_STATUS_UNAVAILABLE == domain_table_schema->get_index_status()) {
      ret = OB_EAGAIN;
      LOG_WARN("domain table is not available now", K(ret), K(domain_table_schema->get_index_status()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (domain_table_schema->is_vec_hnsw_index()) {
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, refresh_ctx_->index_id_tb_id_, index_id_tb_schema))) {
      LOG_WARN("fail to get index id table schema", KR(ret), K(tenant_id), K(refresh_ctx_->index_id_tb_id_));
    } else if (OB_ISNULL(index_id_tb_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index_id_table not exist", KR(ret), K(tenant_id), K(refresh_ctx_->index_id_tb_id_));
    } else if (INDEX_STATUS_AVAILABLE != index_id_tb_schema->get_index_status()) {
      ret = OB_ERR_INDEX_UNAVAILABLE;
      if (INDEX_STATUS_UNAVAILABLE == index_id_tb_schema->get_index_status()) {
        ret = OB_EAGAIN;
        LOG_WARN("index id table is not available now", K(ret), K(index_id_tb_schema->get_index_status()));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id,
                                                      domain_table_schema->get_database_id(),
                                                      db_schema))) {
    LOG_WARN("fail to get db schema", KR(ret), K(tenant_id), K(domain_table_schema->get_database_id()));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database not exist", KR(ret));
  } else if (OB_UNLIKELY(db_schema->is_in_recyclebin() ||
                         domain_table_schema->is_in_recyclebin() ||
                         (OB_NOT_NULL(index_id_tb_schema) && index_id_tb_schema->is_in_recyclebin()))) {
    // do nothing
    triggered = false;
    LOG_DEBUG("table or db in recyclebin");
  } else if (OB_UNLIKELY(0 == refresh_ctx_->delta_rate_threshold_)) {
    // do nothing
  } else if (!domain_table_schema->is_vec_hnsw_index()) {
    // no need to get table cnt if not hnsw index
  } else if (OB_FAIL(get_table_row_count(db_schema->get_database_name_str(),
                                         base_table_schema->get_table_name_str(),
                                         refresh_ctx_->scn_,
                                         base_table_row_cnt))) {
    LOG_WARN("fail to get base table row count", KR(ret),
             K(base_table_schema->get_table_name_str()));
  } else if (OB_FAIL(get_table_row_count(db_schema->get_database_name_str(),
                                         domain_table_schema->get_table_name_str(),
                                         refresh_ctx_->scn_,
                                         domain_table_row_cnt))) {
    LOG_WARN("fail to get delta_buf_table row count", KR(ret), K(domain_table_schema->get_table_name_str()));
  } else if (OB_FAIL(get_table_row_count(db_schema->get_database_name_str(),
                                         index_id_tb_schema->get_table_name_str(),
                                         refresh_ctx_->scn_,
                                         index_id_table_row_cnt))) {
    LOG_WARN("fail to get index_id_table row count", KR(ret), K(index_id_tb_schema->get_table_name_str()));
  } else if (0 != base_table_row_cnt &&
             (index_id_table_row_cnt + domain_table_row_cnt) * 1.0 /
                     base_table_row_cnt <
                 refresh_ctx_->delta_rate_threshold_) {
    // rebuilding is not triggered.
    triggered = false;
    LOG_WARN("no need to start rebuild", K(base_table_row_cnt));
  }

  if (OB_SUCC(ret) && triggered) {
    LOG_INFO("start to rebuild vec index");
    const int64_t DEFAULT_TIMEOUT_US = GCONF.internal_sql_execute_timeout;
    ObTimeoutCtx timeout_ctx;
    ObAddr rs_addr;
    obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
    ObRebuildIndexArg rebuild_index_arg;
    obrpc::ObAlterTableRes rebuild_index_res;
    const bool is_support_cancel = true;
    rebuild_index_arg.tenant_id_ = tenant_id;
    rebuild_index_arg.exec_tenant_id_ = tenant_id;
    rebuild_index_arg.session_id_ = session_info->get_sessid();
    rebuild_index_arg.database_name_ = db_schema->get_database_name_str();
    rebuild_index_arg.table_name_ = base_table_schema->get_table_name_str();
    rebuild_index_arg.index_name_ = domain_table_schema->get_table_name_str();
    rebuild_index_arg.index_table_id_ = domain_table_schema->get_table_id();
    rebuild_index_arg.index_action_type_ = obrpc::ObIndexArg::ADD_INDEX;
    rebuild_index_arg.parallelism_ = refresh_ctx_->idx_parallel_creation_;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT_US))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    } else if (OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.rs_mgr is null", K(ret));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("fail to rootservice address", KR(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected common_rpc_proxy nullptr", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->to(rs_addr).rebuild_vec_index(rebuild_index_arg, rebuild_index_res))) {
      LOG_WARN("failed to post backup ls data res", K(ret), K(rebuild_index_arg));
    } else {
      LOG_INFO("succ to send rebuild vector index rpc", K(rs_addr), K(refresh_ctx_));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(rebuild_index_arg.tenant_id_,
                                                     rebuild_index_res.task_id_,
                                                     false/*do not retry at executor*/,
                                                     session_info,
                                                     common_rpc_proxy,
                                                     is_support_cancel))) {
        LOG_WARN("fail wait rebuild vec index finish", K(ret));
      } else {
        LOG_INFO("succ to wait rebuild vec index", K(ret), K(rebuild_index_res.task_id_), K(rebuild_index_arg));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
