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

#define USING_LOG_PREFIX RS

#include "ob_index_build_task.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "share/ob_ddl_common.h"
#include "share/ob_ddl_sim_point.h"
#include "rootserver/ob_root_service.h"
#include "share/scn.h"
#include "share/schema/ob_mlog_info.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/vector_index/ob_hnsw_index_builder.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

int ObIndexSSTableBuildTask::set_nls_format(const ObString &nls_date_format,
                                            const ObString &nls_timestamp_format,
                                            const ObString &nls_timestamp_tz_format)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_ob_string(allocator_,
                                  nls_date_format,
                                  nls_date_format_))) {
    LOG_WARN("fail to deep copy nls date format", K(ret));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_,
                                         nls_timestamp_format,
                                         nls_timestamp_format_))) {
    LOG_WARN("fail to deep copy nls timestamp format", K(ret));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_,
                                         nls_timestamp_tz_format,
                                         nls_timestamp_tz_format_))) {
    LOG_WARN("fail to deep copy nls timestamp tz format", K(ret));
  }
  return ret;
}

int ObIndexSSTableBuildTask::inner_normal_process(
    const ObTableSchema &data_schema,
    const ObTableSchema &index_schema,
    const bool is_oracle_mode,
    const ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  bool need_padding = false;
  if (OB_FAIL(data_schema.is_need_padding_for_generated_column(need_padding))) {
    LOG_WARN("fail to check need padding", K(ret));
  } else {
    common::ObCommonSqlProxy *user_sql_proxy = nullptr;
    int64_t affected_rows = 0;
    ObSQLMode sql_mode = SMO_STRICT_ALL_TABLES | (need_padding ? SMO_PAD_CHAR_TO_FULL_LENGTH : 0);
    ObSessionParam session_param;
    session_param.sql_mode_ = (int64_t *)&sql_mode;
    session_param.tz_info_wrap_ = nullptr;
    session_param.ddl_info_.set_is_ddl(true);
    session_param.ddl_info_.set_source_table_hidden(data_schema.is_user_hidden_table());
    session_param.ddl_info_.set_dest_table_hidden(index_schema.is_user_hidden_table());
    session_param.nls_formats_[ObNLSFormatEnum::NLS_DATE] = nls_date_format_;
    session_param.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = nls_timestamp_format_;
    session_param.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = nls_timestamp_tz_format_;
    session_param.use_external_session_ = true;  // means session id dispatched by session mgr
    session_param.consumer_group_id_ = consumer_group_id_;

    common::ObAddr *sql_exec_addr = nullptr;
    if (inner_sql_exec_addr_.is_valid()) {
      sql_exec_addr = &inner_sql_exec_addr_;
      LOG_INFO("inner sql execute addr" , K(*sql_exec_addr));
    }
    int tmp_ret = OB_SUCCESS;
    if (is_oracle_mode) {
      user_sql_proxy = GCTX.ddl_oracle_sql_proxy_;
    } else {
      user_sql_proxy = GCTX.ddl_sql_proxy_;
    }
    DEBUG_SYNC(BEFORE_INDEX_SSTABLE_BUILD_TASK_SEND_SQL);
    ObTimeoutCtx timeout_ctx;
    const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT = ObDDLUtil::calc_inner_sql_execute_timeout();
    LOG_INFO("execute sql" , K(sql_string), K(data_table_id_), K(tenant_id_), K(DDL_INNER_SQL_EXECUTE_TIMEOUT));
    if (OB_FAIL(timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
      LOG_WARN("set trx timeout failed", K(ret));
    } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
      LOG_WARN("set timeout failed", K(ret));
    } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, CREATE_INDEX_BUILD_SSTABLE_FAILED))) {
      LOG_WARN("ddl sim failure: create index build sstable failed", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(user_sql_proxy->write(tenant_id_, sql_string.ptr(), affected_rows,
                is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE, &session_param, sql_exec_addr))) {
      LOG_WARN("fail to execute build replica sql", K(ret), K(tenant_id_));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObCheckTabletDataComplementOp::check_finish_report_checksum(tenant_id_, dest_table_id_, execution_id_, task_id_))) {
        LOG_WARN("fail to check sstable checksum_report_finish",
          K(ret), K(tenant_id_), K(dest_table_id_), K(execution_id_), K(task_id_));
      }
    }
  }
  return ret;
}

// TODO(@wangmiao) : may reuse the existed interface
int ObIndexSSTableBuildTask::inner_hnsw_process(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &data_schema,
    const ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard trace_id_guard(trace_id_);
  ObTabletID unused_tablet_id;
  const ObDatabaseSchema *data_db_schema = nullptr;
  const ObDatabaseSchema *index_db_schema = nullptr;
  ObSqlString base_scan_sql;
  int64_t real_parallelism = std::max(1L, parallelism_);
  real_parallelism = std::min(blocksstable::ObMacroDataSeq::MAX_PARALLEL_IDX + 1, real_parallelism);
  int64_t vector_dim = -1;
  int64_t max_ep_level = -1;
  ObSqlString vector_hnsw_index_sql;
  int64_t total_vector_cnt = 0;

  if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, data_schema.get_database_id(), data_db_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(tenant_id_), K(data_schema.get_database_id()));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, index_schema.get_database_id(), index_db_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(tenant_id_), K(index_schema.get_database_id()));
  } else if (OB_FAIL(base_scan_sql.append_fmt("select /*+ monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) opt_param('enable_newsort', 'false') use_px */ ",
                                               real_parallelism, execution_id_, task_id_))) {
    LOG_WARN("failed to append base_scan_sql", K(ret));
  } else {
    const common::ObRowkeyInfo &base_rowkeys = data_schema.get_rowkey_info();
    const common::ObRowkeyColumn *index_info = index_schema.get_index_info().get_column(0);
    uint64_t base_rowkey_column_id = 0;
    common::ObString column_name;
    bool is_col_exist = false;
    bool is_first_col_name = true;
    // append select column sql
    for (int64_t i = 0; OB_SUCC(ret) && i < base_rowkeys.get_size(); ++i) {
      if (OB_FAIL(base_rowkeys.get_column_id(i, base_rowkey_column_id))) {
        LOG_WARN("failed to get column id", K(ret), K(i), K(base_rowkey_column_id));
      } else if (OB_FALSE_IT(data_schema.get_column_name_by_column_id(base_rowkey_column_id, column_name, is_col_exist))) {
      } else if (OB_UNLIKELY(!is_col_exist)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column not exist with unexpection", K(ret));
      } else if (OB_FAIL(base_scan_sql.append_fmt(is_first_col_name ? "%.*s" : ",%.*s",
                                                  static_cast<int>(column_name.length()),
                                                  column_name.ptr()))) {
        LOG_WARN("failed to append base_scan_sql", K(ret), K(column_name));
      }
      is_first_col_name = false;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FALSE_IT(index_schema.get_column_name_by_column_id(index_info->column_id_, column_name, is_col_exist))) {
    } else if (OB_UNLIKELY(!is_col_exist)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column not exist with unexpection", K(ret));
    } else if (OB_FAIL(base_scan_sql.append_fmt(",%.*s", static_cast<int>(column_name.length() - 1),
                                                column_name.ptr() + 1))) {
      LOG_WARN("failed to append base_scan_sql", K(ret), K(column_name));
    }
    // append select table
    ObString data_db_name = data_db_schema->get_database_name_str();
    ObString data_tb_name = data_schema.get_table_name_str();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(base_scan_sql.append_fmt(" from `%.*s`.`%.*s` as of snapshot %ld",
                                                static_cast<int>(data_db_name.length()), data_db_name.ptr(),
                                                static_cast<int>(data_tb_name.length()), data_tb_name.ptr(),
                                                snapshot_version_))) {
      LOG_WARN("failed to append base_scan_sql", K(ret), K(data_db_name), K(data_tb_name));
    }

    // start sql proxy to read vectors from base table
    if (OB_FAIL(ret)) {
    } else {
      uint64_t base_rowkey_cnt = base_rowkeys.get_size();
      uint64_t select_column_cnt = base_rowkey_cnt + 1;
      common::ObCommonSqlProxy *user_sql_proxy = GCTX.sql_proxy_;
      common::ObMySQLTransaction trans(true);
      ObTimeoutCtx timeout_ctx;
      const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT = ObDDLUtil::calc_inner_sql_execute_timeout();
      common::ObAddr *sql_exec_addr = nullptr;
      ObTypeVector::VectorIndexDistanceFunc dfunc = nullptr;
      if (inner_sql_exec_addr_.is_valid()) {
        sql_exec_addr = &inner_sql_exec_addr_;
        LOG_INFO("inner sql execute addr" , K(*sql_exec_addr));
      }
      LOG_INFO("execute sql" , K(base_scan_sql), K(data_table_id_), K(tenant_id_), K(DDL_INNER_SQL_EXECUTE_TIMEOUT));
      if (OB_FAIL(ObTypeVector::get_vector_dfunc(get_vd_type(), dfunc))) {
        LOG_WARN("fail to get vector dfunc", K(ret), K(get_vd_type()));
      } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
        LOG_WARN("set trx timeout failed", K(ret));
      } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
        LOG_WARN("set timeout failed", K(ret));
      } else if (OB_FAIL(trans.start(user_sql_proxy, tenant_id_))) {
        LOG_WARN("fail to start transaction", K(ret), K(tenant_id_));
      } else {
        const int64_t hnsw_m = vector_hnsw_m_;
        const int64_t hnsw_ef_construction = vector_hnsw_ef_construction_;
        lib::ObMemAttr mattr(tenant_id_, "HNSW");
        ObArenaAllocator allocator(mattr);
        ObArenaAllocator unsafe_in_ring_allocator1(mattr);
        ObArenaAllocator unsafe_out_ring_allocator1(mattr);
        ObArenaAllocator unsafe_in_ring_allocator2(mattr);
        ObArenaAllocator unsafe_out_ring_allocator2(mattr);
        common::ObSafeArenaAllocator in_ring_allocator1(unsafe_in_ring_allocator1);
        common::ObSafeArenaAllocator out_ring_allocator1(unsafe_out_ring_allocator1);
        common::ObSafeArenaAllocator in_ring_allocator2(unsafe_in_ring_allocator2);
        common::ObSafeArenaAllocator out_ring_allocator2(unsafe_out_ring_allocator2);
        const int64_t mem_allow_limit = lib::get_tenant_memory_limit(tenant_id_);
        const double hnsw_build_mem_percentage = 0.5;
        LOG_INFO("############ hnsw build index info", K(mem_allow_limit), K(hnsw_build_mem_percentage));

        share::ObHNSWIndexBuilder hnsw_builder(hnsw_m, hnsw_ef_construction, dfunc,
                                                index_schema.get_table_name_str(), index_db_schema->get_database_name_str(), &allocator, base_rowkey_cnt, column_name,
                                                &trans, tenant_id_, &in_ring_allocator1, &out_ring_allocator1, &in_ring_allocator2, &out_ring_allocator2,
                                                4);
        // hnsw_builder.get
        SMART_VAR(ObMySQLProxy::MySQLResult, read_res) {
          sqlclient::ObMySQLResult *result = NULL;
          if (OB_FAIL(user_sql_proxy->read(read_res, tenant_id_, base_scan_sql.ptr(), sql_exec_addr))) {
            LOG_WARN("fail to scan base table", K(ret), K(tenant_id_));
          } else if (OB_ISNULL(result = read_res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get sql result", K(ret), KP(result));
          } else {
            // TODO: shk
            if (OB_FAIL(hnsw_builder.init())) {
              LOG_WARN("fail to init hnsw builder", K(ret));
            }
            while (OB_SUCC(ret)) {
              if (OB_FAIL(result->next())) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
                } else {
                  LOG_WARN("fail to get next row", K(ret));
                }
              } else if (OB_FAIL(hnsw_builder.insert_vector(result))) {
                LOG_WARN("fail to insert vector into hnsw index table", K(ret));
              } else if (OB_FALSE_IT(++total_vector_cnt)) {
              } else if (hnsw_builder.get_cache_mem_usage() > hnsw_build_mem_percentage * mem_allow_limit) {
                hnsw_builder.show_cache_info();
                if (OB_FAIL(hnsw_builder.reset())) {
                  LOG_WARN("fail to reset hnsw_builder", K(ret));
                }
              }
              if (OB_SUCC(ret) && 0 == total_vector_cnt % 3000) {
                // if (OB_FAIL(hnsw_builder.write_all_updates())) {
                //   LOG_WARN("fail to write all updates", K(ret));
                // } else {
                  LOG_INFO("############ hnsw build index count", K(total_vector_cnt));
                // }
              }
              if (OB_SUCC(ret) && 0 == total_vector_cnt % 10000) {
                if (OB_FAIL(hnsw_builder.write_all_updates())) {
                  LOG_WARN("fail to write all updates", K(ret));
                }
              }
            }
            LOG_INFO("############ hnsw ready for write all updates", K(ret));
            if (OB_SUCC(ret) && OB_FAIL(hnsw_builder.write_all_updates())) {
              LOG_WARN("fail to write remaining updates", K(ret));
            }
            LOG_INFO("############ hnsw finsh write all updates", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          vector_dim = hnsw_builder.get_hnsw_vector_dim();
          max_ep_level = hnsw_builder.get_max_ep_level();
          const ObHNSWElement *ep = hnsw_builder.get_ep();
          if (OB_NOT_NULL(ep)) {
            ObHNSWCPoolPtr pkey_ptr = ep->pk_ptr_;
            ObArenaAllocator tmp_allocator;
            if (OB_FAIL(vector_hnsw_index_sql.append_fmt("insert into %s (vector_index_table_id,rowkey_id,vector_distance_type,hnsw_dim,hnsw_m,hnsw_ef_construction,hnsw_entry_level,rowkey_value) values ",
                                                          OB_ALL_VECTOR_HNSW_INDEX_TNAME))) {
              LOG_WARN("fail to append vector hnsw index insertion sql", K(ret));
            }
            ObCastCtx cast_ctx(&tmp_allocator, NULL, CM_NONE, ObCharset::get_system_collation());
            ObObj tmp_obj;
            for (int64_t i = 0; OB_SUCC(ret) && i < base_rowkey_cnt; ++i) {
              if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, pkey_ptr.objs_[i], tmp_obj))) {
                  LOG_WARN("fail to cast obj to varchar", K(pkey_ptr.objs_[i]));
              } else if (OB_FAIL(vector_hnsw_index_sql.append_fmt(i == 0 ? "(%ld,%ld,%ld,%ld,%ld,%ld,%ld,'%.*s')" : ",(%ld,%ld,%ld,%ld,%ld,%ld,%ld,'%.*s')",
                                                                  index_schema.get_table_id(), i,
                                                                  static_cast<int64_t>(get_vd_type()), vector_dim,
                                                                  hnsw_m, hnsw_ef_construction, max_ep_level,
                                                                  static_cast<int>(tmp_obj.get_string().length()),
                                                                  tmp_obj.get_string().ptr()))) {
                LOG_WARN("fail to append sql", K(ret), K(tmp_obj.get_string()));
              }
            }
          } else {
            if (OB_FAIL(vector_hnsw_index_sql.append_fmt("insert into %s (vector_index_table_id,rowkey_id,vector_distance_type,hnsw_dim,hnsw_m,hnsw_ef_construction,hnsw_entry_level,rowkey_value) values ",
                                                          OB_ALL_VECTOR_HNSW_INDEX_TNAME))) {
              LOG_WARN("fail to append vector hnsw index insertion sql", K(ret));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < base_rowkey_cnt; ++i) {
                if (OB_FAIL(vector_hnsw_index_sql.append_fmt(i == 0 ? "(%ld,%ld,%ld,%ld,%ld,%ld,%ld,'')" : ",(%ld,%ld,%ld,%ld,%ld,%ld,%ld,'')",
                                                                    index_schema.get_table_id(), i,
                                                                    static_cast<int64_t>(get_vd_type()), vector_dim,
                                                                    16L, 200L, max_ep_level))) {
                  LOG_WARN("fail to append sql", K(ret));
                }
              }
            }
          }
        }

        if (trans.is_started()) {
          bool commit = (OB_SUCCESS == ret);
          LOG_INFO("############ hnsw start commit", K(ret), K(commit));
          int tmp_ret = trans.end(commit);
          LOG_INFO("############ hnsw finish commit", K(tmp_ret));
          if (OB_SUCCESS != tmp_ret) {
            ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
          }
        }
      }
    }
  }

  // set __all_vector_hnsw_index
  if (OB_SUCC(ret)) {
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret), K(tenant_id_));
    } else if (OB_FAIL(trans.write(tenant_id_, vector_hnsw_index_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to do insertion", K(ret));
    }
    if (trans.is_started()) {
      bool commit = (OB_SUCCESS == ret);
      int tmp_ret = trans.end(commit);
      if (OB_SUCCESS != tmp_ret) {
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }

  LOG_INFO("build hnsw index finish", K(ret), K(*this));
  ObDDLTaskKey task_key(tenant_id_, dest_table_id_, schema_version_);
  ObDDLTaskInfo info;
  int tmp_ret = root_service_->get_ddl_scheduler().on_sstable_complement_job_reply(
      unused_tablet_id, task_key, snapshot_version_, execution_id_, ret, info);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("report build finish failed", K(ret), K(tmp_ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObIndexSSTableBuildTask::inner_ivf_process(
    const ObTableSchema &data_schema,
    const ObTableSchema &index_schema,
    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED; // TODO(@jinshui)
    LOG_WARN("not supported now", K(ret), K(is_oracle_mode));
  } else {
    ObString index_table_name;
    ObString data_table_name;
    ObString database_name;
    ObSqlString select_column_str;
    common::ObArray<uint64_t> index_col_ids;
    int64_t affected_rows = 0;
    schema::ObSchemaGetterGuard schema_guard;
    const schema::ObTableSchema *index_table_schema = nullptr;
    const schema::ObTableSchema *data_table_schema = nullptr;
    const schema::ObDatabaseSchema *db_schema = nullptr;
    common::ObCommonSqlProxy *user_sql_proxy = nullptr;
    ObTimeoutCtx timeout_ctx;
    const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT = ObDDLUtil::calc_inner_sql_execute_timeout();
    user_sql_proxy = GCTX.sql_proxy_;
    if (OB_FAIL(timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
      LOG_WARN("set trx timeout failed", K(ret));
    } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
      LOG_WARN("set timeout failed", K(ret));
    } else if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret), K(data_table_id_));
    } else if (OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("fail to check formal guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id_, data_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K_(tenant_id), K(data_table_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dest_table_id_, index_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K_(tenant_id), K(dest_table_id_));
    } else if (OB_ISNULL(index_table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KP(index_table_schema), KP(data_table_schema),
        K_(tenant_id), K(dest_table_id_), K(data_table_id_));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, data_table_schema->get_database_id(), db_schema))) {
      LOG_WARN("fail to get database schema", K(ret), K_(tenant_id), K(data_table_schema->get_database_id()));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, database schema must not be nullptr", K(ret));
    } else if (OB_FAIL(index_table_schema->get_column_ids(index_col_ids))) {
      LOG_WARN("fail to get index table column ids", K(ret));
    } else {
      data_table_name = data_table_schema->get_table_name();
      index_table_name = index_table_schema->get_table_name();
      database_name = db_schema->get_database_name();
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < index_col_ids.count(); ++i) {
      const ObColumnSchemaV2 *index_col_schema = index_table_schema->get_column_schema(index_col_ids.at(i));
      const ObString& index_col_name = index_col_schema->get_column_name();
      if (index_col_schema->is_hidden() ||
          index_col_name == "center_idx") {
        // do nothing
      } else if (OB_FAIL(select_column_str.append_fmt(select_column_str.empty() ? "%.*s" : ",%.*s",
                  static_cast<int>(index_col_name.length()), index_col_name.ptr()))) {
        LOG_WARN("fail to append select_column_str", K(ret), K(index_col_name));
      }
    }

    ObSqlString ivfflat_build_sql_string;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ivfflat_build_sql_string.append_fmt("insert /*+parallel(%ld)*/ into `%.*s`.`%.*s` select %.*s from `%.*s`.`%.*s`",
        parallelism_,
        static_cast<int>(database_name.length()), database_name.ptr(),
        static_cast<int>(index_table_name.length()), index_table_name.ptr(),
        static_cast<int>(select_column_str.length()), select_column_str.ptr(),
        static_cast<int>(database_name.length()), database_name.ptr(),
        static_cast<int>(data_table_name.length()), data_table_name.ptr()))) {
      LOG_WARN("fail to append ivfflat_build_sql_string", K(database_name), K(index_table_name), K(data_table_name));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, read_res) { // 这里又调一次set_end_stmt
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(user_sql_proxy->read(read_res, tenant_id_, ivfflat_build_sql_string.ptr()))) {
          LOG_WARN("fail to scan base table", K(ret), K(tenant_id_));
        } else if (OB_ISNULL(result = read_res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", K(ret), KP(result));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("fail to get next row", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuildTask::process()
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard trace_id_guard(trace_id_);
  ObSqlString sql_string;
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  bool oracle_mode = false;
  ObTabletID unused_tablet_id;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;

  bool need_exec_new_inner_sql = true;

  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(data_table_id_));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("fail to check formal guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(
      tenant_id_, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed",
        K(ret), K(tenant_id_));
  } else if (NULL == sys_variable_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is NULL", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, BUILD_REPLICA_ASYNC_TASK_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(sys_variable_schema->get_oracle_mode(oracle_mode))) {
    LOG_WARN("get oracle mode failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id_, data_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(data_table_id_));
  } else if (nullptr == data_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(tenant_id_), K(data_table_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dest_table_id_, index_schema))) {
    LOG_WARN("get index schema failed", K(ret), K(tenant_id_), K(dest_table_id_));
  } else if (nullptr == index_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, index schema must not be nullptr", K(ret), K(tenant_id_), K(dest_table_id_));
  } else if (is_using_hnsw_index()) {
    if (OB_FAIL(inner_hnsw_process(schema_guard, *data_schema, *index_schema))) {
      LOG_WARN("failed to process hnsw index", K(ret));
    }
  } else if (is_using_ivf_index()) {
    if (OB_FAIL(inner_ivf_process(*data_schema, *index_schema, oracle_mode))) {
      LOG_WARN("failed to process ivf index", K(ret));
    }
  } else if (OB_FAIL(ObDDLUtil::generate_build_replica_sql(tenant_id_, data_table_id_,
                                                          dest_table_id_,
                                                          data_schema->get_schema_version(),
                                                          snapshot_version_,
                                                          execution_id_,
                                                          task_id_,
                                                          parallelism_,
                                                          false/*use_heap_table_ddl*/,
                                                          !data_schema->is_user_hidden_table()/*use_schema_version_hint_for_src_table*/,
                                                          nullptr,
                                                          sql_string))) {
    LOG_WARN("fail to generate build replica sql", K(ret));
  } else if (OB_FAIL(inner_normal_process(*data_schema, *index_schema, oracle_mode, sql_string))) {
    LOG_WARN("failed to process normal index", K(ret));
  }

  ObDDLTaskKey task_key(tenant_id_, dest_table_id_, schema_version_);
  ObDDLTaskInfo info;
  int tmp_ret = root_service_->get_ddl_scheduler().on_sstable_complement_job_reply(
      unused_tablet_id, task_key, snapshot_version_, execution_id_, ret, info);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("report build finish failed", K(ret), K(tmp_ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  add_event_info(ret, "index sstable build task finish");
  LOG_INFO("build index sstable finish", K(ret), "ddl_event_info", ObDDLEventInfo(), K(*this), K(sql_string));
  return ret;
}

void ObIndexSSTableBuildTask::add_event_info(const int ret, const ObString &ddl_event_stmt)
{
  char table_id_buffer[256];
  snprintf(table_id_buffer, sizeof(table_id_buffer), "data_table_id:%ld, dest_table_id:%ld",
            data_table_id_, dest_table_id_);
  ROOTSERVICE_EVENT_ADD("ddl scheduler", ddl_event_stmt.ptr(),
    "tenant_id", tenant_id_,
    "ret", ret,
    K_(trace_id),
    K_(task_id),
    "table_id", table_id_buffer,
    "sql_exec_addr", inner_sql_exec_addr_);
}

ObAsyncTask *ObIndexSSTableBuildTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObIndexSSTableBuildTask *task = NULL;
  if (NULL == buf || buf_size < (sizeof(*task))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", KP(buf), K(buf_size));
  } else {
    task = new (buf) ObIndexSSTableBuildTask(
        task_id_,
        tenant_id_,
        data_table_id_,
        dest_table_id_,
        schema_version_,
        snapshot_version_,
        execution_id_,
        consumer_group_id_,
        trace_id_,
        parallelism_,
        root_service_,
        inner_sql_exec_addr_,
        is_vector_index_);
    task->set_vector_index_using_type(vector_index_using_type_);
    task->set_vd_type(vd_type_);
    task->set_vector_pq_seg(vector_pq_seg_);
    task->set_vector_hnsw_m(vector_hnsw_m_);
    task->set_vector_hnsw_ef_construction(vector_hnsw_ef_construction_);
    task->set_container_table_id(container_table_id_);
    task->set_second_container_table_id(second_container_table_id_);
    if (OB_SUCCESS != (task->set_nls_format(nls_date_format_, nls_timestamp_format_, nls_timestamp_tz_format_))) {
      task->~ObIndexSSTableBuildTask();
      task = nullptr;
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "failed to set nls format");
    }
  }
  return task;
}
/***************         ObIndexBuildTask        *************/

ObIndexBuildTask::ObIndexBuildTask()
  : ObDDLTask(ObDDLType::DDL_CREATE_INDEX), index_table_id_(target_object_id_),
    is_unique_index_(false), is_global_index_(false), root_service_(nullptr), snapshot_held_(false),
    is_sstable_complete_task_submitted_(false), sstable_complete_request_time_(0), sstable_complete_ts_(0),
    check_unique_snapshot_(0), complete_sstable_job_ret_code_(INT64_MAX), create_index_arg_(), target_cg_cnt_(0)
{

}

ObIndexBuildTask::~ObIndexBuildTask()
{

}

int ObIndexBuildTask::process()
{
  int ret = OB_SUCCESS;
  int64_t start_status = task_status_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check health failed", K(ret));
  } else if (!need_retry()) {
    // by pass
  } else {
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
    case ObDDLTaskStatus::PREPARE: {
      if (OB_FAIL(prepare())) {
        LOG_WARN("prepare failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_TRANS_END: {
      if (OB_FAIL(wait_trans_end())) {
        LOG_WARN("wait trans end failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::REDEFINITION: {
      if (OB_FAIL(wait_data_complement())) {
        LOG_WARN("wait data complement failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::VALIDATE_CHECKSUM: {
      if (OB_FAIL(verify_checksum())) {
        LOG_WARN("verify checksum failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::TAKE_EFFECT: {
      if (OB_FAIL(enable_index())) {
        LOG_WARN("enable index failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::FAIL: {
      if (OB_FAIL(clean_on_failed())) {
        LOG_WARN("clean failed_task failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::SUCCESS: {
      if (OB_FAIL(succ())) {
        LOG_WARN("clean task on finish failed", K(ret), K(*this));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected status", K(ret), K(status), K(*this));
      break;
    }
    } // end switch
    ddl_tracing_.release_span_hierarchy();
    if (OB_FAIL(ret)) {
      add_event_info("index build task process fail");
      LOG_INFO("index build task process fail", "ddl_event_info", ObDDLEventInfo());
    }
  }
  return ret;
}

void ObIndexBuildTask::flt_set_task_span_tag() const
{
  FLT_SET_TAG(ddl_task_id, task_id_, ddl_parent_task_id, parent_task_id_,
              ddl_data_table_id, object_id_, ddl_index_table_id, index_table_id_,
              ddl_is_unique_index, is_unique_index_, ddl_is_global_index, is_global_index_,
              ddl_schema_version, schema_version_);
}

void ObIndexBuildTask::flt_set_status_span_tag() const
{
  switch (task_status_) {
  case ObDDLTaskStatus::PREPARE: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::WAIT_TRANS_END: {
    FLT_SET_TAG(ddl_data_table_id, object_id_, ddl_index_table_id, index_table_id_, ddl_schema_version, schema_version_,
                ddl_snapshot_version, snapshot_version_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::REDEFINITION: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::VALIDATE_CHECKSUM: {
    FLT_SET_TAG(ddl_check_unique_snapshot, check_unique_snapshot_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::TAKE_EFFECT: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::FAIL: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::SUCCESS: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  default: {
    break;
  }
  }
}

int ObIndexBuildTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const share::ObDDLType &ddl_type,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    const int64_t schema_version,
    const int64_t parallelism,
    const int64_t consumer_group_id,
    const int32_t sub_task_trace_id,
    const obrpc::ObCreateIndexArg &create_index_arg,
    const int64_t parent_task_id /* = 0 */,
    const uint64_t tenant_data_version,
    const ObTableSchema *container_schema /* nullptr */,
    const ObTableSchema *second_container_schema /* nullptr */,
    const int64_t task_status /* = TaskStatus::PREPARE */,
    const int64_t snapshot_version /* = 0 */)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root_service is null", K(ret), KP(root_service_));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service", K(ret));
  } else if (OB_UNLIKELY(
        !(data_table_schema != nullptr
          && OB_INVALID_ID != tenant_id
          && index_schema != nullptr
          && schema_version > 0
          && tenant_data_version > 0
          && (task_status >= ObDDLTaskStatus::PREPARE && task_status <= ObDDLTaskStatus::SUCCESS)
          && task_id > 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_table_schema), K(index_schema),
        K(schema_version), K(task_status), K(snapshot_version), K(task_id));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg, create_index_arg_))) {
    LOG_WARN("fail to copy create index arg", K(ret), K(create_index_arg));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_UNLIKELY((ObIndexArg::ADD_MLOG == create_index_arg_.index_action_type_)
      && (!index_schema->is_mlog_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index action is add_mlog but index schema is not mlog",
        KR(ret), K(create_index_arg_.index_action_type_), K(index_schema->get_table_type()));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    is_global_index_ = index_schema->is_global_index_table();
    is_unique_index_ = index_schema->is_unique_index();
    tenant_id_ = tenant_id;
    object_id_ = data_table_schema->get_table_id();
    index_table_id_ = index_schema->get_table_id();
    if (OB_NOT_NULL(container_schema)) {
      create_index_arg_.container_table_id_ = container_schema->get_table_id();
    }
    if (OB_NOT_NULL(second_container_schema)) {
      create_index_arg_.second_container_table_id_ = second_container_schema->get_table_id();
    }
    schema_version_ = schema_version;
    parallelism_ = parallelism;
    create_index_arg_.exec_tenant_id_ = tenant_id_;
    if (snapshot_version > 0) {
      snapshot_version_ = snapshot_version;
    }
    if (ObDDLTaskStatus::VALIDATE_CHECKSUM == task_status) {
      sstable_complete_ts_ = ObTimeUtility::current_time();
    }
    consumer_group_id_ = consumer_group_id;
    sub_task_trace_id_ = sub_task_trace_id;
    task_id_ = task_id;
    task_type_ = ddl_type;
    parent_task_id_ = parent_task_id;
    task_version_ = OB_INDEX_BUILD_TASK_VERSION;
    start_time_ = ObTimeUtility::current_time();
    data_format_version_ = tenant_data_version;
    if (OB_SUCC(ret)) {
      task_status_ = static_cast<ObDDLTaskStatus>(task_status);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_ddl_task_monitor_info(index_schema->get_table_id()))) {
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else {
      dst_tenant_id_ = tenant_id_;
      dst_schema_version_ = schema_version_;
      is_inited_ = true;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_schema->get_store_column_group_count(target_cg_cnt_))) {
      LOG_WARN("fail to get column group cnt", K(ret), K(index_schema));
    }
    ddl_tracing_.open();
  }

  return ret;
}

int ObIndexBuildTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  const uint64_t data_table_id = task_record.object_id_;
  const uint64_t index_table_id = task_record.target_object_id_;
  const int64_t schema_version = task_record.schema_version_;
  int64_t pos = 0;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  const char *ddl_type_str = nullptr;
  const char *target_name = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root_service is null", K(ret), KP(root_service_));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service", K(ret));
  } else if (!task_record.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_FAIL(DDL_SIM(task_record.tenant_id_, task_record.task_id_, DDL_TASK_INIT_BY_RECORD_FAILED))) {
    LOG_WARN("ddl sim failure", K(task_record.tenant_id_), K(task_record.task_id_));
  } else if (OB_FAIL(deserlize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
    LOG_WARN("deserialize params from message failed", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
          task_record.tenant_id_, schema_guard, schema_version))) {
    LOG_WARN("fail to get schema guard", K(ret), K(index_table_id), K(schema_version));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(task_record.tenant_id_, data_table_id, data_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(task_record.tenant_id_, index_table_id, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
  } else if (OB_ISNULL(data_schema) || OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), K(data_schema), K(index_schema));
  } else if (OB_UNLIKELY((ObIndexArg::ADD_MLOG == create_index_arg_.index_action_type_)
      && (!index_schema->is_mlog_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index action is add_mlog but index schema is not mlog",
        KR(ret), K(create_index_arg_.index_action_type_), K(index_schema->get_table_type()));
  } else {
    is_global_index_ = index_schema->is_global_index_table();
    is_unique_index_ = index_schema->is_unique_index();
    tenant_id_ = task_record.tenant_id_;
    object_id_ = data_table_id;
    index_table_id_ = index_table_id;
    schema_version_ = schema_version;
    snapshot_version_ = task_record.snapshot_version_;
    execution_id_ = task_record.execution_id_;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    task_type_ = task_record.ddl_type_; // could be create index / mlog

    if (ObDDLTaskStatus::VALIDATE_CHECKSUM == task_status_) {
      sstable_complete_ts_ = ObTimeUtility::current_time();
    }
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    ret_code_ = task_record.ret_code_;
    start_time_ = ObTimeUtility::current_time();

    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    if (OB_FAIL(init_ddl_task_monitor_info(index_schema->get_table_id()))) {
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else {
      is_inited_ = true;

      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

bool ObIndexBuildTask::is_valid() const
{
  return is_inited_ && !trace_id_.is_invalid();
}

int ObIndexBuildTask::deep_copy_index_arg(common::ObIAllocator &allocator,
                                          const ObCreateIndexArg &source_arg,
                                          ObCreateIndexArg &dest_arg)
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = source_arg.get_serialize_size();
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(serialize_size));
  } else if (OB_FAIL(source_arg.serialize(buf, serialize_size, pos))) {
    LOG_WARN("serialize alter table arg", K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(dest_arg.deserialize(buf, serialize_size, pos))) {
    LOG_WARN("deserialize alter table arg failed", K(ret));
  }
  if (OB_FAIL(ret) && nullptr != buf) {
    allocator.free(buf);
  }
  return ret;
}

int ObIndexBuildTask::check_health()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service, not need retry", K(ret), K(object_id_), K(index_table_id_));
    need_retry_ = false; // only stop run the task, need not clean up task context
  } else if (OB_FAIL(refresh_status())) { // refresh task status
    LOG_WARN("refresh status failed", K(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else {
    ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = nullptr;
    bool is_data_table_exist = false;
    bool is_index_table_exist = false;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get tanant schema guard failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, object_id_, is_data_table_exist))) {
      LOG_WARN("check data table exist failed", K(ret), K_(tenant_id), K(object_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, index_table_id_, is_index_table_exist))) {
      LOG_WARN("check index table exist failed", K(ret), K_(tenant_id), K(index_table_id_));
    } else if (!is_data_table_exist || !is_index_table_exist) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table or index table not exist", K(ret), K(is_data_table_exist), K(is_index_table_exist));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_table_id_, index_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(index_table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index schema is null, but index table exist", K(ret), K(index_table_id_));
    } else if (ObIndexStatus::INDEX_STATUS_INDEX_ERROR == index_schema->get_index_status()) {
      ret = OB_SUCCESS == ret_code_ ? OB_ERR_ADD_INDEX : ret_code_;
      LOG_WARN("index status error", K(ret), K(index_table_id_), K(index_schema->get_index_status()));
    }
    #ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = check_errsim_error();
      }
    #endif
    if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
      const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
      const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
      switch_status(new_status, false, ret);
      LOG_WARN("switch status to build_failed", K(ret), K(old_status), K(new_status));
    }
    if (ObDDLTaskStatus::FAIL == static_cast<ObDDLTaskStatus>(task_status_)
        || ObDDLTaskStatus::SUCCESS == static_cast<ObDDLTaskStatus>(task_status_)) {
      ret = OB_SUCCESS; // allow clean up
    }
  }
  check_ddl_task_execute_too_long();
  return ret;
}

int ObIndexBuildTask::prepare()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::PREPARE != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else {
    state_finished = true;
  }

  if (state_finished) {
    (void)switch_status(ObDDLTaskStatus::WAIT_TRANS_END, true, ret);
    LOG_INFO("prepare finished", K(ret), K(*this));
  }
  return ret;
}

int ObIndexBuildTask::wait_trans_end()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::WAIT_TRANS_END != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (snapshot_version_ > 0 && snapshot_held_) {
    state_finished = true;
  }

  // wait all trans before the schema_version elapsed
  if (OB_SUCC(ret) && !state_finished && snapshot_version_ <= 0) {
    bool is_trans_end = false;
    int64_t tmp_snapshot_version = 0;
    if (!wait_trans_ctx_.is_inited() && OB_FAIL(wait_trans_ctx_.init(
            tenant_id_, task_id_, object_id_, ObDDLWaitTransEndCtx::WaitTransType::WAIT_SCHEMA_TRANS, schema_version_))) {
      LOG_WARN("init wait_trans_ctx failed", K(ret), K(object_id_), K(index_table_id_));
    } else if (OB_FAIL(wait_trans_ctx_.try_wait(is_trans_end, tmp_snapshot_version))) {
      LOG_WARN("try wait transaction end failed", K(ret), K(object_id_), K(index_table_id_));
    } else if (is_trans_end) {
      snapshot_version_ = tmp_snapshot_version;
      LOG_INFO("succ to wait schema transaction end",
          K(object_id_), K(index_table_id_), K(schema_version_), K(snapshot_version_));
      wait_trans_ctx_.reset();
    }
  }

  DEBUG_SYNC(CREATE_INDEX_WAIT_TRANS_END);

  // persistent snapshot_version into inner table and hold snapshot of data_table and index table
  if (OB_SUCC(ret) && !state_finished && snapshot_version_ > 0 && !snapshot_held_) {
    if (OB_FAIL(ObDDLTaskRecordOperator::update_snapshot_version(root_service_->get_sql_proxy(),
                                                                 tenant_id_,
                                                                 task_id_,
                                                                 snapshot_version_))) {
      LOG_WARN("update snapshot version failed", K(ret), K(task_id_), K(snapshot_version_));
    } else if (OB_FAIL(hold_snapshot(snapshot_version_))) {
      if (OB_SNAPSHOT_DISCARDED == ret) {
        ret = OB_SUCCESS;
        snapshot_version_ = 0;
        snapshot_held_ = false;
        LOG_INFO("snapshot discarded, need retry waiting trans", K(ret));
      } else {
        LOG_WARN("hold snapshot failed", K(ret), K(snapshot_version_));
      }
    } else {
      snapshot_held_ = true;
      state_finished = true;
    }
  }

  if (state_finished || OB_FAIL(ret)) {
    // a newly-created mlog is empty
    ObDDLTaskStatus next_status = (ObIndexArg::ADD_MLOG == create_index_arg_.index_action_type_) ?
                                      ObDDLTaskStatus::TAKE_EFFECT : ObDDLTaskStatus::REDEFINITION;
    (void)switch_status(next_status, true, ret);
    LOG_INFO("wait_trans_end finished", K(ret), K(*this));
  }
  return ret;
}

int ObIndexBuildTask::hold_snapshot(const int64_t snapshot)
{
  int ret = OB_SUCCESS;
  SCN snapshot_scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (snapshot <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("snapshot version not valid", K(ret), K(snapshot));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_HOLD_SNAPSHOT_FAILED))) {
    LOG_WARN("ddl sim failure: hold snapshot failed", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot))) {
    LOG_WARN("failed to convert", K(snapshot), K(ret));
  } else {
    ObDDLService &ddl_service = root_service_->get_ddl_service();
    ObSEArray<ObTabletID, 2> tablet_ids;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_table_schema = nullptr;
    const ObTableSchema *index_table_schema = nullptr;
    ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
    bool need_acquire_lob = false;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(object_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, index_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(target_object_id_));
    } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(index_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(object_id_), K(target_object_id_), KP(data_table_schema), KP(index_table_schema));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, object_id_, tablet_ids))) {
      LOG_WARN("failed to get data table snapshot", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, target_object_id_, tablet_ids))) {
      LOG_WARN("failed to get dest table snapshot", K(ret));
    } else if (OB_FAIL(check_need_acquire_lob_snapshot(data_table_schema, index_table_schema, need_acquire_lob))) {
      LOG_WARN("failed to check if need to acquire lob snapshot", K(ret));
    } else if (need_acquire_lob && data_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
               OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, data_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
      LOG_WARN("failed to get data lob meta table snapshot", K(ret));
    } else if (need_acquire_lob && data_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
               OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, data_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
      LOG_WARN("failed to get data lob piece table snapshot", K(ret));
    } else if (OB_FAIL(ddl_service.get_snapshot_mgr().batch_acquire_snapshot(
            ddl_service.get_sql_proxy(), SNAPSHOT_FOR_DDL, tenant_id_, schema_version_, snapshot_scn, nullptr, tablet_ids))) {
      LOG_WARN("batch acquire snapshot failed", K(ret), K(tablet_ids));
    }
  }
  LOG_INFO("hold snapshot finished", K(ret), K(snapshot), K(object_id_), K(index_table_id_), K(schema_version_));
  return ret;
}

int ObIndexBuildTask::release_snapshot(const int64_t snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_RELEASE_SNAPSHOT_FAILED))) {
    LOG_WARN("ddl sim failure: release snapshot failed", K(ret), K(tenant_id_), K(task_id_));
  } else {
    ObDDLService &ddl_service = root_service_->get_ddl_service();
    ObSEArray<ObTabletID, 2> tablet_ids;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_table_schema = nullptr;
    ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
    if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, object_id_, tablet_ids))) {
      if (OB_TABLE_NOT_EXIST == ret || OB_TENANT_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get data table snapshot", K(ret));
      }
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, target_object_id_, tablet_ids))) {
      if (OB_TABLE_NOT_EXIST == ret || OB_TENANT_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get dest table snapshot", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(object_id_));
    } else if (OB_ISNULL(data_table_schema)) {
      // ignore ret
      LOG_INFO("table not exist", K(ret), K(object_id_), K(target_object_id_), KP(data_table_schema));
    } else if (data_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
                OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, data_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get data lob meta table snapshot", K(ret));
      }
    } else if ( data_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
                OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, data_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get data lob piece table snapshot", K(ret));
      }
    }

    if (OB_SUCC(ret) && tablet_ids.count() > 0 && OB_FAIL(batch_release_snapshot(snapshot, tablet_ids))) {
      LOG_WARN("batch relase snapshot failed", K(ret), K(tablet_ids));
    }
  }
  LOG_INFO("release snapshot finished", K(ret), K(snapshot), K(object_id_), K(index_table_id_), K(schema_version_));
  return ret;
}

int ObIndexBuildTask::reap_old_replica_build_task(bool &need_exec_new_inner_sql)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const int64_t data_table_id = object_id_;
  const int64_t dest_table_id = target_object_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexBuildTask has not been inited", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(data_table_id));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("error unexpected, table schema must not be nullptr", K(ret));
  } else {
    const int64_t old_execution_id = get_execution_id();
    const ObTabletID unused_tablet_id;
    const ObDDLTaskInfo unused_addition_info;
    const int old_ret_code = OB_SUCCESS;
    ObAddr invalid_addr;
    if (old_execution_id < 0) {
      need_exec_new_inner_sql = true;
    } else if (OB_FAIL(ObCheckTabletDataComplementOp::check_and_wait_old_complement_task(tenant_id_, dest_table_id,
        task_id_, old_execution_id, invalid_addr, trace_id_,
        table_schema->get_schema_version(), snapshot_version_, need_exec_new_inner_sql))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to check and wait old complement task", K(ret));
      }
    } else if (!need_exec_new_inner_sql) {
      if (OB_FAIL(update_complete_sstable_job_status(unused_tablet_id, snapshot_version_, old_execution_id, old_ret_code, unused_addition_info))) {
        LOG_INFO("succ to wait and complete old task finished!", K(ret));
      }
    }
  }
  return ret;
}

// construct ObIndexSSTableBuildTask build task
int ObIndexBuildTask::send_build_single_replica_request()
{
  int ret = OB_SUCCESS;
  int64_t new_execution_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexBuildTask has not been inited", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_SEND_BUILD_REPLICA_REQUEST_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(ObDDLTask::push_execution_id(tenant_id_, task_id_, new_execution_id))) {
    LOG_WARN("failed to fetch new execution id", K(ret));
  } else {
    if (OB_FAIL(ObDDLUtil::get_sys_ls_leader_addr(GCONF.cluster_id, tenant_id_, create_index_arg_.inner_sql_exec_addr_))) {
      LOG_WARN("get sys ls leader addr fail", K(ret), K(tenant_id_));
      ret = OB_SUCCESS; // ingore ret
    } else {
      set_sql_exec_addr(create_index_arg_.inner_sql_exec_addr_); // set to switch_status, if task cancel, we should kill session with inner_sql_exec_addr_
    }
    ObIndexSSTableBuildTask task(
        task_id_,
        tenant_id_,
        object_id_,
        target_object_id_,
        schema_version_,
        snapshot_version_,
        new_execution_id,
        consumer_group_id_,
        trace_id_,
        parallelism_,
        root_service_,
        create_index_arg_.inner_sql_exec_addr_,
        create_index_arg_.is_vector_index());
    if (create_index_arg_.is_vector_index()) {
      task.set_vector_index_using_type(create_index_arg_.index_using_type_);
      task.set_vd_type(create_index_arg_.index_columns_.at(0).vd_type_);
      task.set_vector_pq_seg(create_index_arg_.vector_pq_seg_);
      task.set_vector_hnsw_m(create_index_arg_.vector_hnsw_m_);
      task.set_vector_hnsw_ef_construction(create_index_arg_.vector_hnsw_ef_construction_);
      if (create_index_arg_.is_vector_ivf_index()) {
        if (create_index_arg_.vector_help_schema_.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected empty vector help schema", K(ret), K_(create_index_arg));
        } else {
          task.set_container_table_id(create_index_arg_.container_table_id_);
        }
        if (USING_IVFPQ == create_index_arg_.index_using_type_) {
          if (create_index_arg_.vector_help_schema_.count() < 2) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected not enough vector help schema", K(ret), K_(create_index_arg));
          } else {
            task.set_second_container_table_id(create_index_arg_.second_container_table_id_);
          }
        }
      }
    }
    if (OB_FAIL(task.set_nls_format(create_index_arg_.nls_date_format_,
                                    create_index_arg_.nls_timestamp_format_,
                                    create_index_arg_.nls_timestamp_tz_format_))) {
      LOG_WARN("failed to set nls format", K(ret), K(create_index_arg_));
    } else if (OB_FAIL(root_service_->submit_ddl_single_replica_build_task(task))) {
      LOG_WARN("fail to submit task", K(ret), KPC(this));
    } else {
      is_sstable_complete_task_submitted_ = true;
      sstable_complete_request_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

//check whether all leaders have completed the complement task
int ObIndexBuildTask::check_build_single_replica(bool &is_end)
{
  int ret = OB_SUCCESS;
  is_end = false;
  TCWLockGuard guard(lock_);
  if (INT64_MAX == complete_sstable_job_ret_code_) {
    // not complete
  } else if (OB_SUCCESS == complete_sstable_job_ret_code_) {
    is_end = true;
  } else if (OB_SUCCESS != complete_sstable_job_ret_code_) {
    ret = complete_sstable_job_ret_code_;
    LOG_WARN("sstable complete job has failed", K(ret), K(object_id_), K(index_table_id_));
    if (is_replica_build_need_retry(ret)) {
      // retry sql job by re-submit
      is_sstable_complete_task_submitted_ = false;
      complete_sstable_job_ret_code_ = INT64_MAX;
      ret = OB_SUCCESS;
      LOG_INFO("retry complete sstable job", K(ret), K(object_id_), K(index_table_id_));
    } else {
      is_end = true;
    }
  }

  if (OB_SUCC(ret) && !is_end) {
    if (sstable_complete_request_time_ + ObDDLUtil::calc_inner_sql_execute_timeout() < ObTimeUtility::current_time()) {
      is_sstable_complete_task_submitted_ = false;
      sstable_complete_request_time_ = 0;
    }
  }
  return ret;
}

bool ObIndexBuildTask::is_sstable_complete_task_submitted()
{
  TCRLockGuard guard(lock_);
  return is_sstable_complete_task_submitted_;
}

int ObIndexBuildTask::wait_data_complement()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::REDEFINITION != task_status_) {
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_UNLIKELY(snapshot_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected snapshot", K(ret), KPC(this));
  }
  bool is_request_end = false;

  // submit a job to complete sstable for the index table on snapshot_version
  if (OB_SUCC(ret) && !state_finished && !is_sstable_complete_task_submitted()) {
    bool need_exec_new_inner_sql = false;
    if (OB_FAIL(reap_old_replica_build_task(need_exec_new_inner_sql))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS; // retry
      } else {
        LOG_WARN("failed to reap old task", K(ret));
      }
    } else if (!need_exec_new_inner_sql) {
      state_finished = true;
    } else if (OB_FAIL(send_build_single_replica_request())) {
      LOG_WARN("fail to send build single replica request", K(ret));
    }
  }

  DEBUG_SYNC(CREATE_INDEX_REPLICA_BUILD);

  if (OB_SUCC(ret) && !state_finished && is_sstable_complete_task_submitted()) {
    if (OB_FAIL(check_build_single_replica(is_request_end))) {
      LOG_WARN("fail to check build single replica", K(ret));
    } else if (is_request_end) {
      ret = complete_sstable_job_ret_code_;
      state_finished = true;
    }
  }
  if (OB_SUCC(ret) && state_finished &&
      !create_index_arg_.is_spatial_index() &&
      !create_index_arg_.is_vector_index()) {
    bool dummy_equal = false;
    bool need_verify_checksum = true;
#ifdef ERRSIM
    // when the major compaction is delayed, skip verify column checksum
    need_verify_checksum = 0 == GCONF.errsim_ddl_major_delay_time;
#endif
    if (need_verify_checksum && OB_FAIL(ObDDLChecksumOperator::check_column_checksum(
            tenant_id_, get_execution_id(), object_id_, index_table_id_, task_id_, false/*index build*/, dummy_equal, root_service_->get_sql_proxy()))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to check column checksum", K(ret), K(index_table_id_), K(object_id_), K(task_id_));
        state_finished = true;
      } else if (REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_INFO("index checksum has not been reported", K(ret), K(index_table_id_), K(object_id_), K(task_id_));
      }
    }
  }

  if (state_finished || OB_FAIL(ret)) {
    (void)switch_status(ObDDLTaskStatus::VALIDATE_CHECKSUM, true, ret);
    LOG_INFO("wait data complement finished", K(ret), K(*this));
  }
  return ret;
}

int ObIndexBuildTask::check_need_verify_checksum(bool &need_verify)
{
  int ret = OB_SUCCESS;
  need_verify = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_unique_index_) {
    need_verify = true;
  } else if (create_index_arg_.is_spatial_index() || create_index_arg_.is_vector_index()) {
    need_verify = false;
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = nullptr;
    const ObTableSchema *data_table_schema = nullptr;
    ObArray<ObColDesc> index_column_ids;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, index_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(target_object_id_));
    } else if (OB_ISNULL(index_schema)) {
      // do nothing
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, index schema exist while data table schema not exist", K(ret));
    } else if (OB_FAIL(index_schema->get_column_ids(index_column_ids))) {
      LOG_WARN("get column ids failed", K(ret));
    } else {
      const ObColumnSchemaV2 *column_schema = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_column_ids.count(); ++i) {
        const uint64_t column_id = index_column_ids.at(i).col_id_;
        if (!is_shadow_column(column_id)) {
          if (OB_ISNULL(column_schema = data_table_schema->get_column_schema(column_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, column schema must not be nullptr", K(ret), K(column_id));
          } else if (column_schema->is_generated_column_using_udf()) {
            need_verify = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexBuildTask::check_need_acquire_lob_snapshot(const ObTableSchema *data_table_schema,
                                                      const ObTableSchema *index_table_schema,
                                                      bool &need_acquire)
{
  int ret = OB_SUCCESS;
  need_acquire = false;
  ObTableSchema::const_column_iterator iter = index_table_schema->column_begin();
  ObTableSchema::const_column_iterator iter_end = index_table_schema->column_end();
  for (; OB_SUCC(ret) && !need_acquire && iter != iter_end; iter++) {
    const ObColumnSchemaV2 *index_col = *iter;
    if (OB_ISNULL(index_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret));
    } else {
      const ObColumnSchemaV2 *col = data_table_schema->get_column_schema(index_col->get_column_id());
      if (OB_ISNULL(col)) {
      } else if (col->is_generated_column()) {
        ObSEArray<uint64_t, 8> ref_columns;
        if (OB_FAIL(col->get_cascaded_column_ids(ref_columns))) {
          STORAGE_LOG(WARN, "Failed to get cascaded column ids", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && !need_acquire && i < ref_columns.count(); i++) {
            const ObColumnSchemaV2 *data_table_col = data_table_schema->get_column_schema(ref_columns.at(i));
            if (OB_ISNULL(data_table_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema is null", K(ret));
            } else if (is_lob_storage(data_table_col->get_data_type())) {
              need_acquire = true;
            }
          }
        }
      }
    }
  }
  return ret;
}

// verify column checksum between data table and index table
int ObIndexBuildTask::verify_checksum()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  bool need_verify = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::VALIDATE_CHECKSUM != task_status_) {
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_FAIL(check_need_verify_checksum(need_verify))) {
    LOG_WARN("check need verify checksum failed", K(ret));
  } else if (!need_verify) {
    state_finished = true;
  }

  // make sure all trans use complete sstable
  if (OB_SUCC(ret) && !state_finished && check_unique_snapshot_ <= 0) {
    bool is_trans_end = false;
    int64_t tmp_snapshot_version = 0;
    if (!wait_trans_ctx_.is_inited() && OB_FAIL(wait_trans_ctx_.init(
            tenant_id_, task_id_, object_id_, ObDDLWaitTransEndCtx::WaitTransType::WAIT_SSTABLE_TRANS, sstable_complete_ts_))) {
      LOG_WARN("init wait_trans_ctx failed", K(ret), K(object_id_), K(index_table_id_));
    } else if (OB_FAIL(wait_trans_ctx_.try_wait(is_trans_end, tmp_snapshot_version))) {
      LOG_WARN("try wait transaction end failed", K(ret), K(object_id_), K(index_table_id_));
    } else if (is_trans_end) {
      check_unique_snapshot_ = tmp_snapshot_version;
      LOG_INFO("succ to wait sstable transaction end",
          K(object_id_), K(index_table_id_), K(sstable_complete_ts_), K(check_unique_snapshot_));
      wait_trans_ctx_.reset();
    }
  }

  DEBUG_SYNC(CREATE_INDEX_VERIFY_CHECKSUM);

  // send column checksum calculation request and wait finish, then verify column checksum
  if (OB_SUCC(ret) && !state_finished && check_unique_snapshot_ > 0) {
    static int64_t checksum_wait_timeout = 10 * 1000 * 1000L; // 10s
    bool is_column_checksum_ready = false;
    bool dummy_equal = false;
    if (!wait_column_checksum_ctx_.is_inited() && OB_FAIL(wait_column_checksum_ctx_.init(
            task_id_, tenant_id_, object_id_, index_table_id_, schema_version_, check_unique_snapshot_, get_execution_id(), checksum_wait_timeout))) {
      LOG_WARN("init context of wait column checksum failed", K(ret), K(object_id_), K(index_table_id_));
    } else {
      if (OB_FAIL(wait_column_checksum_ctx_.try_wait(is_column_checksum_ready))) {
        LOG_WARN("try wait column checksum failed", K(ret), K(object_id_), K(index_table_id_));
        state_finished = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_column_checksum_ready) {
      // do nothing
    } else {
      if (OB_FAIL(ObDDLChecksumOperator::check_column_checksum(
              tenant_id_, get_execution_id(), object_id_, index_table_id_, task_id_, true/*check unique index*/, dummy_equal, root_service_->get_sql_proxy()))) {
        if (OB_CHECKSUM_ERROR == ret && is_unique_index_) {
          ret = OB_ERR_DUPLICATED_UNIQUE_KEY;
        }
        LOG_WARN("fail to check column checksum", K(ret), K(index_table_id_), K(object_id_), K(check_unique_snapshot_), K(snapshot_version_));
      }
      state_finished = true; // no matter checksum right or not, state finished
    }
  }

  if (state_finished) {
    (void)switch_status(ObDDLTaskStatus::TAKE_EFFECT, true, ret);
    LOG_INFO("verify checksum finished", K(ret), K(*this));
  }
  return ret;
}

int ObIndexBuildTask::update_column_checksum_calc_status(
    const common::ObTabletID &tablet_id,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  bool is_latest_execution_id = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else if (ObDDLTaskStatus::VALIDATE_CHECKSUM != task_status_) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("task expired", K(ret), K(task_status_));
  } else {
    if (OB_FAIL(wait_column_checksum_ctx_.update_status(tablet_id, ret_code))) {
      LOG_WARN("update column checksum calculation status failed", K(ret), K(tablet_id), K(ret_code));
    }
  }
  return ret;
}

int ObIndexBuildTask::update_complete_sstable_job_status(
    const common::ObTabletID &tablet_id,
    const int64_t snapshot_version,
    const int64_t execution_id,
    const int ret_code,
    const ObDDLTaskInfo &addition_info)
{
  int ret = OB_SUCCESS;
  UNUSED(addition_info);
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(ret_code));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, UPDATE_COMPLETE_SSTABLE_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id_), K(task_id_));
  } else if (ObDDLTaskStatus::REDEFINITION != task_status_) {
    // by pass, may be network delay
    LOG_INFO("not waiting data complete, may finished", K(task_status_));
  } else if (snapshot_version != snapshot_version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot version not match", K(ret), K(snapshot_version), K(snapshot_version_));
  } else if (execution_id < execution_id_) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("receive a mismatch execution result", K(ret), K(ret_code), K(execution_id), K(execution_id_));
  } else {
    complete_sstable_job_ret_code_ = ret_code;
    sstable_complete_ts_ = ObTimeUtility::current_time();
    execution_id_ = execution_id; // update ObIndexBuildTask::execution_id_ from ObIndexSSTableBuildTask::execution_id_
  }
  LOG_INFO("update complete sstable job return code", K(ret), K(target_object_id_), K(tablet_id), K(snapshot_version), K(ret_code), K(execution_id_));
  return ret;
}

// enable index in schema service
int ObIndexBuildTask::enable_index()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  ObDDLTaskStatus next_status = ObDDLTaskStatus::SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::TAKE_EFFECT != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_TAKE_EFFECT_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else {
    share::schema::ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = NULL;
    bool index_table_exist = false;
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id_;
    int64_t version_in_inner_table = OB_INVALID_VERSION;
    int64_t local_schema_version = OB_INVALID_VERSION;
    if (GCTX.is_standby_cluster()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create global index in slave cluster is not allowed", K(ret), K(index_table_id_));
    } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, index_table_id_, index_table_exist))) {
      LOG_WARN("fail to check table exist", K(ret), K_(tenant_id), K(index_table_id_));
    } else if (!index_table_exist) {
      ret = OB_SCHEMA_ERROR;
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_table_id_, index_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id_), K(index_table_id_));
    } else if (OB_UNLIKELY(NULL == index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema ptr is null", K(ret), K(index_table_id_));
    } else {
      ObIndexStatus index_status = index_schema->get_index_status();
      if (INDEX_STATUS_AVAILABLE == index_status) {
        state_finished = true;
      } else if (INDEX_STATUS_UNAVAILABLE != index_status) {
        if (INDEX_STATUS_UNUSABLE == index_status) {
          // the index is unused, for example dropped
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("the index status is unusable, maybe dropped", K(ret), K(index_table_id_), K(index_status));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index status not match", K(ret), K(index_table_id_), K(index_status));
        }
      } else if ((ObIndexArg::ADD_MLOG == create_index_arg_.index_action_type_)
          && OB_FAIL(update_mlog_last_purge_scn())) {
        LOG_WARN("failed to update mlog last purge scn", KR(ret));
      } else if (OB_FAIL(update_index_status_in_schema(*index_schema, INDEX_STATUS_AVAILABLE))) {
        LOG_WARN("fail to try notify index take effect", K(ret), K(index_table_id_));
      } else {
        state_finished = true;
      }
    }
  }
  DEBUG_SYNC(CREATE_INDEX_TAKE_EFFECT);
  if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
    state_finished = true;
    next_status = ObDDLTaskStatus::TAKE_EFFECT;
  }
  if (state_finished) {
    (void)switch_status(next_status, true, ret);
    LOG_INFO("enable index finished", K(ret), K(*this));
  }
  return ret;
}

int ObIndexBuildTask::update_index_status_in_schema(const ObTableSchema &index_schema, const ObIndexStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    obrpc::ObUpdateIndexStatusArg arg;
    arg.index_table_id_ = index_schema.get_table_id();
    arg.status_ = new_status;
    arg.exec_tenant_id_ = tenant_id_;
    arg.in_offline_ddl_white_list_ = true;
    arg.task_id_ = task_id_;
    int64_t ddl_rpc_timeout = 0;
    int64_t tmp_timeout = 0;
    if (INDEX_STATUS_AVAILABLE == new_status) {
      // For create index syntax, create_index_arg_ will record the user sql, and generate the ddl_stmt_str when nabling index.
      // For alter table add index syntax, create_index_arg_ will not record the user sql, and generate the ddl_stmt_str when generating index schema.
      arg.ddl_stmt_str_ = create_index_arg_.ddl_stmt_str_;
    }

    DEBUG_SYNC(BEFORE_UPDATE_GLOBAL_INDEX_STATUS);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(index_schema.get_all_part_num(), ddl_rpc_timeout))) {
      LOG_WARN("get ddl rpc timeout fail", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, index_schema.get_data_table_id(), tmp_timeout))) {
      LOG_WARN("get ddl rpc timeout fail", K(ret));
    } else if (OB_FALSE_IT(ddl_rpc_timeout += tmp_timeout)) {
    } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, UPDATE_INDEX_STATUS_FAILED))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(root_service_->get_common_rpc_proxy().to(GCTX.self_addr()).timeout(ddl_rpc_timeout).update_index_status(arg))) {
      LOG_WARN("update index status failed", K(ret), K(arg));
    } else {
      LOG_INFO("notify index status changed finish", K(new_status), K(index_table_id_), K(ddl_rpc_timeout), "ddl_stmt_str", arg.ddl_stmt_str_);
    }
  }
  return ret;
}

int ObIndexBuildTask::clean_on_failed()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::FAIL != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else {
    // mark the schema of index to ERROR, so that observer can do clean up
    const ObTableSchema *index_schema = nullptr;
    bool is_index_exist = true;
    bool state_finished = true;
    ObSchemaGetterGuard schema_guard;
    bool drop_index_on_failed = true; // TODO@wenqu: index building triggered by truncate partition may need keep the failed index schema
    bool index_status_is_available = false;
    if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get tenant schema failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, index_table_id_, is_index_exist))) {
      LOG_WARN("check table exist failed", K(ret), K_(tenant_id), K(index_table_id_));
    } else if (!is_index_exist) {
      // by pass
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_table_id_, index_schema))) {
      LOG_WARN("get index schema failed", K(ret), K(tenant_id_), K(index_table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index schema is null", K(ret), K(index_table_id_));
    } else if (index_schema->is_in_recyclebin()) {
      // the index has been dropped, just finish this task
    } else if (ObIndexStatus::INDEX_STATUS_UNAVAILABLE == index_schema->get_index_status()
               && OB_FAIL(update_index_status_in_schema(*index_schema, ObIndexStatus::INDEX_STATUS_INDEX_ERROR))) {
      LOG_WARN("update index schema failed", K(ret));
    } else if (drop_index_on_failed) {
      DEBUG_SYNC(CREATE_INDEX_FAILED);
      bool is_trans_end = false;
      int64_t tmp_snapshot_version = 0;
      if (ObIndexStatus::INDEX_STATUS_AVAILABLE == index_schema->get_index_status()) {
        LOG_INFO("index take effect but ddl task failed", K(ret), K(ret_code_), K(index_table_id_));
        index_status_is_available = true;
        state_finished = true;
      } else if (ObIndexStatus::INDEX_STATUS_INDEX_ERROR != index_schema->get_index_status()) {
        state_finished = false;
      } else if (!wait_trans_ctx_.is_inited() && OB_FAIL(wait_trans_ctx_.init(
              tenant_id_, task_id_, object_id_, ObDDLWaitTransEndCtx::WaitTransType::WAIT_SCHEMA_TRANS, index_schema->get_schema_version()))) {
        LOG_WARN("init wait_trans_ctx failed", K(ret), K(object_id_), K(index_table_id_));
      } else if (OB_FAIL(wait_trans_ctx_.try_wait(is_trans_end, tmp_snapshot_version))) {
        LOG_WARN("try wait transaction end failed", K(ret), K(object_id_), K(index_table_id_));
      } else if (is_trans_end) {
        LOG_INFO("succ to wait schema transaction end on failure",
            K(object_id_), K(index_table_id_), K(index_schema->get_schema_version()));
        // drop failed index
        const ObDatabaseSchema *database_schema = nullptr;
        const ObTableSchema *data_table_schema = nullptr;
        const ObSysVariableSchema *sys_variable_schema = nullptr;
        ObSqlString drop_index_sql;
        bool is_oracle_mode = false;
        ObString index_name;
        if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, index_schema->get_database_id(), database_schema))) {
          LOG_WARN("get database schema failed", K(ret), K_(tenant_id), K(index_schema->get_database_id()));
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_schema->get_data_table_id(), data_table_schema))) {
          LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(index_schema->get_data_table_id()));
        } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id_, sys_variable_schema))) {
          LOG_WARN("get sys variable schema failed", K(ret), K(tenant_id_));
        } else if (OB_UNLIKELY(nullptr == sys_variable_schema || nullptr == database_schema || nullptr == data_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null schema", K(ret), KP(database_schema), KP(data_table_schema), KP(sys_variable_schema));
        } else if (OB_FAIL(sys_variable_schema->get_oracle_mode(is_oracle_mode))) {
          LOG_WARN("get oracle mode failed", K(ret));
        } else if (index_schema->is_in_recyclebin()) {
          // index is already in recyclebin, skip get index name, use a fake one, this is just to pass IndexArg validity check
          index_name = "__fake";
        } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
          LOG_WARN("get index name failed", K(ret));
        } else if (0 == parent_task_id_) {
          if (!create_index_arg_.ddl_stmt_str_.empty()) { // means create index syntax.
            // 1. rollback of alter table add index is completed by the following splicing sql.
            // 2. rollback of create index does nothing here because the stmt only be recorded when enabling index.
          } else if (is_oracle_mode) {
            if (OB_FAIL(drop_index_sql.append_fmt("drop index \"%.*s\"", index_name.length(), index_name.ptr()))) {
              LOG_WARN("generate drop index sql failed", K(ret));
            }
          } else {
            if (OB_FAIL(drop_index_sql.append_fmt("drop index %.*s on %.*s", index_name.length(), index_name.ptr(),
                    data_table_schema->get_table_name_str().length(), data_table_schema->get_table_name_str().ptr()))) {
              LOG_WARN("generate drop index sql failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          int64_t ddl_rpc_timeout = 0;
          obrpc::ObDropIndexArg drop_index_arg;
          obrpc::ObDropIndexRes drop_index_res;
          drop_index_arg.tenant_id_         = tenant_id_;
          drop_index_arg.exec_tenant_id_    = tenant_id_;
          drop_index_arg.index_table_id_    = index_table_id_;
          drop_index_arg.session_id_        = create_index_arg_.session_id_;
          drop_index_arg.index_name_        = index_name;
          drop_index_arg.table_name_        = data_table_schema->get_table_name();
          drop_index_arg.database_name_     = database_schema->get_database_name_str();
          drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
          drop_index_arg.ddl_stmt_str_      = drop_index_sql.string();
          drop_index_arg.is_add_to_scheduler_ = false;
          drop_index_arg.is_hidden_         = data_table_schema->is_user_hidden_table(); // just use to fetch data table schema.
          drop_index_arg.is_in_recyclebin_  = index_schema->is_in_recyclebin();
          drop_index_arg.is_inner_          = true;
          drop_index_arg.task_id_ = task_id_;
          if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(index_schema->get_all_part_num() + data_table_schema->get_all_part_num(), ddl_rpc_timeout))) {
            LOG_WARN("get ddl rpc timeout fail", K(ret));
          } else if (OB_FAIL(root_service_->get_common_rpc_proxy().timeout(ddl_rpc_timeout).drop_index(drop_index_arg, drop_index_res))) {
            LOG_WARN("drop index failed", K(ret));
          }
          LOG_INFO("drop index when build failed", K(ret), K(drop_index_arg));
          wait_trans_ctx_.reset();
        }
      } else {
        state_finished = false;
      }
    }
    if (OB_SUCC(ret) && state_finished) {
      if (index_status_is_available) {
        ret_code_ = OB_SUCCESS;
      }
      if (OB_FAIL(cleanup())) {
        LOG_WARN("cleanup failed", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexBuildTask::succ()
{
  ret_code_ = OB_SUCCESS;
  return cleanup();
}

int ObIndexBuildTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (snapshot_version_ > 0 && OB_FAIL(release_snapshot(snapshot_version_))) {
    LOG_WARN("release snapshot failed", K(ret), K(object_id_), K(index_table_id_), K(snapshot_version_));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  }

  DEBUG_SYNC(CREATE_INDEX_SUCCESS);

  if(OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(), tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", K(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }

  if (OB_SUCC(ret) && parent_task_id_ > 0) {
    const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
    root_service_->get_ddl_task_scheduler().on_ddl_task_finish(parent_task_id, get_task_key(), ret_code_, trace_id_);
  }
  LOG_INFO("clean task finished", K(ret), K(*this));
  return ret;
}

int ObIndexBuildTask::collect_longops_stat(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
  databuff_printf(stat_info_.message_, MAX_LONG_OPS_MESSAGE_LENGTH, pos, "TENANT_ID: %ld, TASK_ID: %ld, ", tenant_id_, task_id_);
  switch(status) {
    case ObDDLTaskStatus::PREPARE: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: PREPARE"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_TRANS_END: {
      if (snapshot_version_ > 0) {
        if (OB_FAIL(databuff_printf(stat_info_.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: ACQUIRE SNAPSHOT, SNAPSHOT_VERSION: %ld",
                                    snapshot_version_))) {
          LOG_WARN("failed to print", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(stat_info_.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: WAIT TRANS END, PENDING_TX_ID: %ld",
                                    wait_trans_ctx_.get_pending_tx_id().get_id()))) {
          LOG_WARN("failed to print", K(ret));
        }
      }
      break;
    }
    case ObDDLTaskStatus::REDEFINITION: {
      int64_t row_scanned = 0;
      int64_t row_sorted = 0;
      int64_t row_inserted_cg = 0;
      int64_t row_inserted_file = 0;

      if (OB_FAIL(gather_redefinition_stats(tenant_id_, task_id_, *GCTX.sql_proxy_, row_scanned, row_sorted, row_inserted_cg, row_inserted_file))) {
        LOG_WARN("failed to gather redefinition stats", K(ret));
      }

      if (OB_FAIL(ret)){
      } else if (target_cg_cnt_ > 1) {
        if (OB_FAIL(databuff_printf(stat_info_.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED_INTO_TMP_FILE: %ld, ROW_INSERTED: %ld out of %ld column group rows",
                                    row_scanned,
                                    row_sorted,
                                    row_inserted_file,
                                    row_inserted_cg,
                                    row_scanned * target_cg_cnt_))) {
          LOG_WARN("failed to print", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(stat_info_.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED: %ld",
                                    row_scanned,
                                    row_sorted,
                                    row_inserted_file))) {
          LOG_WARN("failed to print", K(ret));
        }
      }
      break;
    }
    case ObDDLTaskStatus::VALIDATE_CHECKSUM: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: VALIDATE CHECKSUM"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::TAKE_EFFECT: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: ENABLE INDEX"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::FAIL: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: CLEAN ON FAIL"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::SUCCESS: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: CLEAN ON SUCCESS"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected status", K(ret), K(status), K(*this));
      break;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_COLLECT_LONGOPS_STAT_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(copy_longops_stat(value))) {
    LOG_WARN("failed to collect common longops stat", K(ret));
  }

  return ret;
}

int ObIndexBuildTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("ObDDLTask serialize failed", K(ret));
  } else if (OB_FAIL(create_index_arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize create index arg failed", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, check_unique_snapshot_, target_cg_cnt_);
  }
  return ret;
}

int ObIndexBuildTask::deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg tmp_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserlize_params_from_message(tenant_id, buf, data_len, pos))) {
    LOG_WARN("ObDDLTask deserlize failed", K(ret));
  } else if (OB_FAIL(tmp_arg.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize table failed", K(ret));
  } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(tenant_id, tmp_arg))) {
    LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, tmp_arg, create_index_arg_))) {
    LOG_WARN("deep copy create index arg failed", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, check_unique_snapshot_, target_cg_cnt_);
  }
  return ret;
}

int64_t ObIndexBuildTask::get_serialize_param_size() const
{
  return create_index_arg_.get_serialize_size()
      + serialization::encoded_length_i64(check_unique_snapshot_)
      + ObDDLTask::get_serialize_param_size()
      + serialization::encoded_length_i64(target_cg_cnt_);
}

int ObIndexBuildTask::update_mlog_last_purge_scn()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObMySQLTransaction trans;
    ObMLogInfo mlog_info;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObMLogInfo::fetch_mlog_info(trans,
        tenant_id_, index_table_id_, mlog_info, true/*for_update*/))) {
      LOG_WARN("failed to fetch mlog info", KR(ret));
    } else {
      mlog_info.set_last_purge_scn(snapshot_version_);
      mlog_info.set_last_purge_date(ObTimeUtility::current_time());
      mlog_info.set_last_purge_time(0);
      mlog_info.set_last_purge_rows(0);
      if (OB_FAIL(mlog_info.set_last_purge_trace_id(ObCurTraceId::get_trace_id_str()))) {
        LOG_WARN("failed to set last purge trace id", KR(ret));
      } else if (OB_FAIL(ObMLogInfo::update_mlog_last_purge_info(trans, mlog_info))) {
        LOG_WARN("failed to update mlog last purge info", KR(ret), K(mlog_info));
      }
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}
