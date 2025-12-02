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
#define USING_LOG_PREFIX PL

#include "ob_pl_recompile_task_helper.h"
#include "share/ob_tenant_info_proxy.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_stmt.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_inner_sql_connection.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_pl_dbms_utility_helper.h"
#endif

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace dbms_scheduler;
using namespace observer;
namespace pl
{

int ObPLRecompileTaskHelper::check_job_exists(ObMySQLTransaction &trans,
                                                        const uint64_t tenant_id,
                                                        const ObString &job_name,
                                                        bool &is_job_exists)
{
  int ret = OB_SUCCESS;
  is_job_exists = false;
  ObSqlString select_sql;
  int64_t row_count = 0;
  if (OB_FAIL(select_sql.append_fmt("SELECT count(*) FROM %s WHERE tenant_id = %ld and job_name = '%.*s';",
                                    share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
                                    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    job_name.length(), job_name.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      if (OB_FAIL(trans.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          int64_t idx = 0;
          ObObj obj;
          if (OB_FAIL(client_result->get_obj(idx, obj))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(obj.get_int(row_count))) {
            LOG_WARN("failed to get int", K(ret), K(obj));
          } else if (OB_UNLIKELY(row_count != 2 && row_count != 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(row_count));
          } else {
            is_job_exists = row_count > 0;
          }
        }
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObPLRecompileTaskHelper::init_tenant_recompile_job(const share::schema::ObSysVariableSchema &sys_variable,
                                       uint64_t tenant_id,
                                       ObMySQLTransaction &trans)
{
    int ret = OB_SUCCESS;
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(trans, tenant_id);
    bool is_job_exists = false;
    ObString job_action("dbms_utility.POLLING_ASK_JOB()");
    bool is_oracle_mode = false;
    char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
    int64_t pos = 0;
    int64_t job_id = OB_INVALID_INDEX;
    const int64_t latency = 30 * 60 * 1000000LL;
    int64_t current_time = ObTimeUtility::current_time() + latency;
    ObString job_name("POLLING_ASK_JOB_FOR_PL_RECOMPILE");
    if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
        LOG_WARN("failed to get oracle mode", KR(ret));
    } else if (is_oracle_mode) {
        if (OB_FAIL(check_job_exists(trans, tenant_id, job_name, is_job_exists))) {
            LOG_WARN("fail to check ncomp dll job", K(ret));
        } else if (is_job_exists) {
            // do nothing
            LOG_WARN("job has existed", KR(ret), K(tenant_id), K(is_job_exists), K(job_name));
        } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
            LOG_WARN("failed to gen exec env", KR(ret));
        } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(
                          tenant_id, job_id))) {
            LOG_WARN("get new job_id failed", KR(ret), K(tenant_id));
        } else {
            ObString exec_env(pos, buf);
            HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
                job_info.tenant_id_ = tenant_id;
                job_info.job_ = job_id;
                job_info.job_name_ = job_name;
                job_info.job_action_ = job_action;
                job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
                job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
                job_info.cowner_ = is_oracle_mode ? ObString("SYS") :  ObString("oceanbase");
                job_info.job_style_ = ObString("regular");
                job_info.job_type_ = ObString("STORED_PROCEDURE");
                job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
                job_info.start_date_ = current_time;
                job_info.end_date_ = 64060560000000000;
                job_info.repeat_interval_ = ObString("FREQ=MINUTELY; INTERVAL=30");
                job_info.enabled_ = true;
                job_info.auto_drop_ = false;
                job_info.max_run_duration_ = SECS_PER_HOUR * 2;
                job_info.exec_env_ = exec_env;
                job_info.comments_ = ObString("used to check if need create pl recompile job");
                job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::POLLING_ASK_JOB_FOR_PL_RECOMPILE;
                if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(trans,
                                                                                        tenant_id,
                                                                                        job_id,
                                                                                        job_info))) {
                    LOG_WARN("[PLRECOMPILE]: failed to create pl background recomp task", KR(ret), K(job_action));
                } else {
                    LOG_INFO("[PLRECOMPILE]: finish create pl background recomp task", K(ret), K(tenant_id),
                        K(job_id), K(exec_env), K(current_time), K(job_action));
                }
            }
        }
  } else {
    // TODO: mysql mode
  }
  return ret;
}
#define SET_ITERATE_END_RET \
do { \
  if (OB_ITER_END == ret) { \
    ret = OB_SUCCESS; \
  } else { \
    ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret; \
    LOG_WARN("[PLRECOMPILE]: read from sql result set failed", K(ret)); \
  } \
} while (0)

int ObPLRecompileTaskHelper::construct_select_dep_table_sql(ObSqlString& query_inner_sql,
                                    common::hash::ObHashMap<int64_t, std::pair<ObString, int64_t>>& ddl_drop_obj_map,
                                    ObIArray<int64_t>& ddl_alter_obj_infos,
                                    uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  static constexpr char get_dep_objs_info[] =
    "SELECT * FROM %s where tenant_id = %ld and dep_obj_id > 300000 and dep_obj_type in (5, 6, 7, 9, 12)"
    " and ref_obj_id in ( " ;

  OZ (query_inner_sql.assign_fmt(get_dep_objs_info,
                                       OB_ALL_TENANT_DEPENDENCY_TNAME,
                                       OB_INVALID_TENANT_ID));
  if (OB_SUCC(ret)) {
    common::hash::ObHashMap<int64_t, std::pair<ObString, int64_t>>::iterator iter = ddl_drop_obj_map.begin();
    int64_t map_size = ddl_drop_obj_map.size();
    int64_t cnt = 0;
    for (; OB_SUCC(ret) && iter != ddl_drop_obj_map.end(); ++iter, ++cnt) {
      int64_t ref_obj_id = iter->first;
      OZ (query_inner_sql.append_fmt(" %ld %s", ref_obj_id, cnt < map_size - 1 ? " , " : " "));
    }
    if (OB_SUCC(ret)) {
      if (ddl_alter_obj_infos.empty()) {
        OZ (query_inner_sql.append_fmt(" ) "));
      } else if (!ddl_drop_obj_map.empty()){
        OZ (query_inner_sql.append_fmt(" , "));
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < ddl_alter_obj_infos.count(); ++i) {
          OZ (query_inner_sql.append_fmt(" %ld %s ", ddl_alter_obj_infos.at(i),
                       i < ddl_alter_obj_infos.count() - 1 ? " , " : " ) "));
        }
      }
    }
  }
  return ret;
}

int ObPLRecompileTaskHelper::collect_delta_error_data(common::ObMySQLProxy* sql_proxy,
                                                    uint64_t tenant_id,
                                                    int64_t last_max_schema_version,
                                                    ObIAllocator& allocator,
                                                    ObIArray<ObPLRecompileInfo>& dep_objs)
{
  int ret = OB_SUCCESS;
  ObSqlString query_inner_sql;
  common::sqlclient::ObMySQLResult *result = NULL;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    OZ (query_inner_sql.assign_fmt(
            "SELECT * FROM %s WHERE tenant_id = %ld and OBJ_ID > 300000 and schema_version > %ld "
            " and obj_type in (5, 6, 7, 9, 12) and ERROR_NUMBER in (-5055, -5019, -5543, -5544, -5201, -5733, -5559) ",
             OB_ALL_VIRTUAL_ERROR_TNAME, tenant_id, last_max_schema_version));
    OZ (sql_proxy->read(res, OB_SYS_TENANT_ID, query_inner_sql.ptr()));
    CK (OB_NOT_NULL(result = res.get_result()));
    if (OB_SUCC(ret)) {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t obj_id = 0;
          ObString err_text;
          int64_t schema_version = 0;
          int64_t err_num = 0;
          int64_t text_len = 0;
          char* err_text_array = NULL;
          EXTRACT_INT_FIELD_MYSQL(*result, "obj_id", obj_id, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "text", err_text);
          EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", schema_version, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "error_number", err_num, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "text_length", text_len, int64_t);
          if (OB_SUCC(ret)) {
            if (NULL == (err_text_array = static_cast<char*>(allocator.alloc(text_len + 1)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret), "size", text_len);
            } else {
              MEMCPY(err_text_array, err_text.ptr(), text_len);
              err_text_array[text_len] = '\0';
            }
          }
        ObPLRecompileInfo tmp_tuple(obj_id);
        OX (tmp_tuple.schema_version_ = schema_version);
          int64_t i = 0;
          for (; OB_SUCC(ret) && i < dep_objs.count(); ++i) {
            if (dep_objs.at(i).recompile_obj_id_ == obj_id) {
              dep_objs.at(i) = tmp_tuple;
              break;
            }
          }
          if (OB_SUCC(ret) && i == dep_objs.count()) {
            OZ (dep_objs.push_back(tmp_tuple));
          }
          LOG_TRACE("[PLRECOMPILE]: collect delta error data", K(ret), K(tmp_tuple));
        }
      SET_ITERATE_END_RET;
    }
  }
  return ret;
}

int ObPLRecompileTaskHelper::collect_delta_ddl_operation_data(
                                      common::ObMySQLProxy* sql_proxy,
                                      uint64_t tenant_id,
                                      ObIArray<ObPLRecompileInfo>& dep_objs,
                                      ObArray<int64_t>& ddl_alter_obj_infos,
                                      common::hash::ObHashMap<int64_t, std::pair<ObString, int64_t>>& ddl_drop_obj_map)
{
  int ret = OB_SUCCESS;
  ObSqlString query_inner_sql;
  common::sqlclient::ObMySQLResult *result = NULL;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    if (!ddl_drop_obj_map.empty() || !ddl_alter_obj_infos.empty()) {
      OZ (construct_select_dep_table_sql(query_inner_sql, ddl_drop_obj_map, ddl_alter_obj_infos, tenant_id));
      OZ (sql_proxy->read(res, tenant_id, query_inner_sql.ptr()));
      CK (OB_NOT_NULL(result = res.get_result()));
      if (OB_SUCC(ret)) {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t dep_obj_id = OB_INVALID_ID;
          int64_t dep_obj_type = 0;
          int64_t ref_obj_id = OB_INVALID_ID;
          int64_t ref_obj_type = 0;
          std::pair<ObString, int64_t> dropped_ref_obj;
          EXTRACT_INT_FIELD_MYSQL(*result, "dep_obj_id", dep_obj_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "dep_obj_type", dep_obj_type, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "ref_obj_id", ref_obj_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "ref_obj_type", ref_obj_type, int64_t);
          CK (dep_obj_type >= static_cast<int64_t>(share::schema::ObObjectType::INVALID)
              && dep_obj_type <= static_cast<int64_t>(share::schema::ObObjectType::MAX_TYPE)
              && ref_obj_type >= static_cast<int64_t>(share::schema::ObObjectType::INVALID)
              && ref_obj_type <= static_cast<int64_t>(share::schema::ObObjectType::MAX_TYPE));
          if (OB_SUCC(ret) && is_pl_object_type(static_cast<share::schema::ObObjectType>(dep_obj_type))) {
            ObPLRecompileInfo tmp_tuple(dep_obj_id);
            int tmp_ret = ddl_drop_obj_map.get_refactored(ref_obj_id, dropped_ref_obj);
            if (OB_SUCCESS == tmp_ret && ref_obj_type != static_cast<int64_t>(share::schema::ObObjectType::SYNONYM)) {
              OX (tmp_tuple.schema_version_ = dropped_ref_obj.second);
            } else if (OB_HASH_NOT_EXIST == tmp_ret) {
            } else {
              ret = OB_SUCCESS == ret ? tmp_ret : ret;
            }
            OZ (add_var_to_array_no_dup(dep_objs, tmp_tuple));
          }
        }
        SET_ITERATE_END_RET;
      }
    }
  }
  return ret;
}

int ObPLRecompileTaskHelper::collect_delta_recompile_obj_data(common::ObMySQLProxy* sql_proxy,
                                                    uint64_t tenant_id,
                                                    ObIAllocator& allocator,
                                                    int64_t& max_schema_version,
                                                    share::schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(sql_proxy));
  ObString Dummy = ObString("__dummuy");
  static constexpr char get_dummy_schema_version[] =
    "select schema_version from __all_pl_recompile_objinfo "
    " where ref_obj_name = UPPER('%.*s') and recompile_obj_id = 0 ";

  static constexpr char get_delta_ddl_operation[] =
    "SELECT * FROM %s where tenant_id = %ld and ddl_stmt_str != '' "
    " and  schema_version > %ld  and operation_type in ( 2, 3, 4, 16, 19, "
    " 21, 25, 26, 902, 903, 904, 1202, 1203, 1204, 1205, 1252, 1253, 1254, 1312, 1313, "
    " 1314, 1322, 1323, 1324, 1325, 1952, 1953, 1954) " ;

  common::sqlclient::ObMySQLResult *result = NULL;
  ObSqlString query_inner_sql;
  ObArray<int64_t> ddl_alter_obj_infos;
  common::hash::ObHashMap<int64_t, std::pair<ObString, int64_t>> ddl_drop_obj_map;
  ObArray<ObPLRecompileInfo> dep_objs;
  int64_t last_max_schema_version = 0;
  int64_t cur_max_schema_version = 0;
  OZ (ddl_drop_obj_map.create(32, "PlRecompileJob"));
  CK (OB_NOT_NULL(schema_guard));
  if (OB_SUCC(ret)) {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      // step1 : get last max_schema_version from dummy column
      OZ (query_inner_sql.assign_fmt(get_dummy_schema_version, Dummy.length(), Dummy.ptr()));
      OZ (sql_proxy->read(res, tenant_id, query_inner_sql.ptr()));
      CK (OB_NOT_NULL(result = res.get_result()));
      if (OB_SUCC(ret)) {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", last_max_schema_version, int64_t);
        }
        SET_ITERATE_END_RET;
      }
      OX (cur_max_schema_version = last_max_schema_version) ;

      // step2 : get delta ddl operation
      OZ (query_inner_sql.assign_fmt(get_delta_ddl_operation,
          OB_ALL_VIRTUAL_DDL_OPERATION_TNAME, tenant_id, last_max_schema_version));
      // user tenant can not get real table data
      OZ (sql_proxy->read(res, OB_SYS_TENANT_ID, query_inner_sql.ptr()));
      CK (OB_NOT_NULL(result = res.get_result()));
      if (OB_SUCC(ret)) {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t op_type = OB_INVALID_ID;
          int64_t table_id = OB_INVALID_ID;
          int64_t schema_version = 0;
          ObString table_name;
          ObString table_name_deep_copy;
          EXTRACT_INT_FIELD_MYSQL(*result, "operation_type", op_type, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", table_name);
          EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", schema_version, int64_t);
          if (OB_SUCC(ret)) {
            CK (op_type > static_cast<int64_t>(OB_INVALID_DDL_OP) &&
                op_type < static_cast<int64_t>(OB_DDL_MAX_OP));
            ObSchemaOperationType operation_type = static_cast<ObSchemaOperationType>(op_type);
            cur_max_schema_version = cur_max_schema_version > schema_version ? cur_max_schema_version : schema_version;
            if (is_pl_drop_ddl_operation(operation_type) || is_sql_drop_ddl_operation(operation_type)) {
              OZ (ob_write_string(allocator, table_name, table_name_deep_copy));
              OZ (ddl_drop_obj_map.set_refactored(table_id, std::make_pair(table_name_deep_copy, schema_version), 1));
            } else if (is_pl_create_ddl_operation(operation_type)) {
              if (operation_type == OB_DDL_CREATE_PACKAGE ||
                  operation_type == OB_DDL_ALTER_PACKAGE ) {
                const ObPackageInfo *package_info = nullptr;
                uint64_t tenant_id = get_tenant_id_by_object_id(table_id);
                OZ (schema_guard->get_package_info(tenant_id, table_id, package_info));
                // do not collect package header
                if (OB_NOT_NULL(package_info) && package_info->is_package_body()) {
                  ObPLRecompileInfo tmp_tuple(table_id);
                  OZ (add_var_to_array_no_dup(dep_objs, tmp_tuple));
                }
              } else {
                ObPLRecompileInfo tmp_tuple(table_id);
                OZ (add_var_to_array_no_dup(dep_objs, tmp_tuple));
              }
              OZ (ddl_alter_obj_infos.push_back(table_id));
            } else if (is_sql_create_ddl_operation(operation_type)) {
              OZ (ddl_alter_obj_infos.push_back(table_id));
            }
          }
        }
        SET_ITERATE_END_RET;
      }
      // step3 : get delta recompile pl obj info from ddl operation
      OZ (collect_delta_ddl_operation_data(sql_proxy, tenant_id, dep_objs, ddl_alter_obj_infos, ddl_drop_obj_map));
      // step4 : get delta recompile pl obj info from resolve error
      OZ (collect_delta_error_data(sql_proxy, tenant_id, last_max_schema_version,
                              allocator, dep_objs));
      // step5 : write delta recompile pl obj into __all_pl_recompile_objinfo
      OZ (batch_insert_recompile_obj_info(sql_proxy, tenant_id, cur_max_schema_version, dep_objs));
    }
  }
  OX (max_schema_version = cur_max_schema_version);
  if (ddl_drop_obj_map.created()) {
    ddl_drop_obj_map.destroy();
  }
  return ret;
}

int ObPLRecompileTaskHelper::batch_insert_recompile_obj_info(common::ObMySQLProxy* sql_proxy,
                                                          uint64_t tenant_id,
                                                          int64_t cur_max_schema_version,
                                                          ObIArray<ObPLRecompileInfo>& dep_objs)
{
  int ret = OB_SUCCESS;
  static constexpr char insert_recomp_table[] =
    "INSERT INTO %s (recompile_obj_id, ref_obj_name, schema_version, fail_count) "
    " VALUES ";
  ObString Dummy = ObString("__dummuy");
  ObSqlString query_inner_sql;
  int64_t batch_num = 1000;
  int64_t affected_rows = 0;
  for (int i = 0; OB_SUCC(ret) && i < dep_objs.count(); i = i + batch_num) {
    int64_t start = i;
    int64_t end = i + batch_num > dep_objs.count() ? dep_objs.count() : i + batch_num;
    OZ (query_inner_sql.assign_fmt(insert_recomp_table, OB_ALL_PL_RECOMPILE_OBJINFO_TNAME));
    for (; OB_SUCC(ret) && start < end; ++start) {
      OZ (query_inner_sql.append_fmt("( %ld, '%.*s', %ld , %ld ) , ",
                dep_objs.at(start).recompile_obj_id_,
                dep_objs.at(start).ref_obj_name_.length(),
                dep_objs.at(start).ref_obj_name_.ptr(),
                dep_objs.at(start).schema_version_,
                dep_objs.at(start).fail_cnt_));
    }
    OZ (query_inner_sql.append_fmt("( 0, '%.*s', %ld, 0) ON DUPLICATE KEY UPDATE "
                                " recompile_obj_id = VALUES(recompile_obj_id), "
                                " ref_obj_name = VALUES(ref_obj_name), "
                                " schema_version = VALUES(schema_version), "
                                " fail_count = VALUES(fail_count); ",
                                  Dummy.length(),
                                  Dummy.ptr(),
                                  cur_max_schema_version));
    OZ (sql_proxy->write(tenant_id, query_inner_sql.ptr(), affected_rows), query_inner_sql);

    LOG_TRACE("[PLRECOMPILE]: Insert into __all_pl_recompile_objinfo", K(ret), K(affected_rows),
                K(dep_objs), K(query_inner_sql), K(cur_max_schema_version));
  }
  return ret;
}

int ObPLRecompileTaskHelper::update_dropped_obj(common::hash::ObHashMap<ObString, int64_t>& dropped_ref_objs,
                                          common::ObMySQLProxy* sql_proxy,
                                          uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString query_inner_sql;
  common::sqlclient::ObMySQLResult *result = NULL;
  ObArray<ObString> valid_obj_names;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    common::hash::ObHashMap<ObString, int64_t>::iterator iter = dropped_ref_objs.begin();
    for (; OB_SUCC(ret) && iter != dropped_ref_objs.end(); ++iter) {
      ObString dropped_obj_name = iter->first;
      int64_t dropped_obj_schema_version = iter->second;
      OZ (query_inner_sql.assign_fmt("SELECT * FROM %s where tenant_id = %ld and ddl_stmt_str != '' "
                                    " and  table_name = UPPER('%.*s') and schema_version > %ld "
                                    " and operation_type in (4, 21, 902, 1202, 1252, 1312, 1322, 1952 ) ;",
                                    OB_ALL_VIRTUAL_DDL_OPERATION_TNAME, tenant_id,
                                    dropped_obj_name.length(), dropped_obj_name.ptr(),
                                    dropped_obj_schema_version));
      OZ (sql_proxy->read(res, OB_SYS_TENANT_ID, query_inner_sql.ptr()));
      CK (OB_NOT_NULL(result = res.get_result()));
      if (OB_SUCC(ret)) {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          OZ (add_var_to_array_no_dup(valid_obj_names, dropped_obj_name));
        }
        SET_ITERATE_END_RET;
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < valid_obj_names.count(); ++i) {
    int tmp_ret = dropped_ref_objs.erase_refactored(valid_obj_names.at(i));
    ret = tmp_ret == OB_HASH_NOT_EXIST ? OB_SUCCESS : tmp_ret;
  }
  return ret;
}


int ObPLRecompileTaskHelper::update_recomp_table(ObIArray<ObPLRecompileInfo>& dep_objs,
                                common::ObMySQLProxy* sql_proxy,
                                uint64_t tenant_id,
                                int64_t last_max_schema_version,
                                int64_t start,
                                int64_t end,
                                schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  if (dep_objs.count() > 0 && end > 0) {
    common::sqlclient::ObMySQLResult *result = NULL;
    ObSqlString query_inner_sql;
    ObSqlString update_sql;
    int64_t affected_rows = 0;
    bool need_comma = false;
    bool update_need_comma = false;
    common::hash::ObHashMap<int64_t, bool> batch_dep_objs;
    OZ (batch_dep_objs.create(32, "PlRecompileJob"));
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      OZ (batch_dep_objs.set_refactored(dep_objs.at(i).recompile_obj_id_, false, 1));
    }
    OZ (query_inner_sql.assign_fmt("select KEY_ID, MERGE_VERSION, EXTRA_INFO FROM %s",
                  OB_ALL_NCOMP_DLL_V2_TNAME));
    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        OZ (sql_proxy->read(res, tenant_id, query_inner_sql.ptr()));
        CK (OB_NOT_NULL(result = res.get_result()));
        if (OB_SUCC(ret)) {
          while (OB_SUCC(ret) && OB_SUCC(result->next())) {
            int64_t key_id = 0;
            bool is_valid = false;
            int64_t merge_version = 0;
            ObString extra_info;
            EXTRACT_INT_FIELD_MYSQL(*result, "KEY_ID", key_id, int64_t);
            if (share::schema::ObTriggerInfo::is_trigger_body_package_id(key_id)) {
              key_id = share::schema::ObTriggerInfo::get_package_trigger_id(key_id);
            } else if (share::schema::ObUDTObjectType::is_object_id(key_id)) {
              uint64_t coll_type = 0;
              coll_type = share::schema::ObUDTObjectType::clear_object_id_mask(key_id);
              OZ (find_udt_id(sql_proxy, tenant_id, coll_type, key_id));
            }
            int tmp_ret = batch_dep_objs.get_refactored(key_id, is_valid);
            if (OB_SUCCESS == tmp_ret) {
              EXTRACT_INT_FIELD_MYSQL(*result, "MERGE_VERSION", merge_version, int64_t);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "EXTRA_INFO", extra_info);
#ifdef OB_BUILD_ORACLE_PL
              OZ (DbmsUtilityHelper::check_disk_cache_obj_expired_inner(merge_version,
                                                                        tenant_id,
                                                                        key_id,
                                                                        sql_proxy,
                                                                        schema_guard,
                                                                        extra_info,
                                                                        is_valid));
#endif
              if (OB_SUCC(ret) && is_valid) {
                batch_dep_objs.set_refactored(key_id, true, 1);
              }
            }
          }
          SET_ITERATE_END_RET;
        }
      }
    }
    OZ (query_inner_sql.assign_fmt("DELETE FROM %s where recompile_obj_id in ( ",
              OB_ALL_PL_RECOMPILE_OBJINFO_TNAME));
    OZ (update_sql.assign_fmt("UPDATE %s SET fail_count = fail_count + 1 WHERE recompile_obj_id in ( ",
              OB_ALL_PL_RECOMPILE_OBJINFO_TNAME));
    common::hash::ObHashMap<int64_t, bool>::iterator iter = batch_dep_objs.begin();
    for (; OB_SUCC(ret) && iter != batch_dep_objs.end(); ++iter) {
      if (iter->second) {
        OZ (query_inner_sql.append_fmt(" %s %ld ", need_comma ? ", " : "",
                      iter->first));
        OX (need_comma = true);
      } else {
        OZ (update_sql.append_fmt(" %s %ld ", update_need_comma ? ", " : "",
                      iter->first));
        OX (update_need_comma = true);
      }
    }
    if (need_comma) {
      OZ (query_inner_sql.append_fmt(")"));
      OZ (sql_proxy->write(tenant_id, query_inner_sql.ptr(), affected_rows));
    }
    if (update_need_comma) {
      OZ (update_sql.append_fmt(")"));
      OZ (sql_proxy->write(tenant_id, update_sql.ptr(), affected_rows));
    }
    if (batch_dep_objs.created()) {
      int tmp_ret = batch_dep_objs.destroy();
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObPLRecompileTaskHelper::find_udt_id(common::ObMySQLProxy* sql_proxy,
                            uint64_t tenant_id,
                            uint64_t coll_type,
                            int64_t& udt_id)
{
  int ret = OB_SUCCESS;
  ObSqlString query_inner_sql;
  common::sqlclient::ObMySQLResult *result = NULL;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    // here udt body and udt spec share same object_type_id, we only update it if body recompiled successful.
    OZ (query_inner_sql.assign_fmt("SELECT object_type_id FROM %s where coll_type = %ld and type = 2;",
                 OB_ALL_TENANT_OBJECT_TYPE_TNAME, coll_type));
    OZ (sql_proxy->read(res, tenant_id, query_inner_sql.ptr()));
    CK (OB_NOT_NULL(result = res.get_result()));
    if (OB_SUCC(ret)) {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        EXTRACT_INT_FIELD_MYSQL(*result, "object_type_id", udt_id, int64_t);
      }
      SET_ITERATE_END_RET;
    }
  }
  return ret;
}

int ObPLRecompileTaskHelper::recompile_single_obj(ObPLRecompileInfo& obj_info,
                                ObISQLConnection *connection,
                                uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  OZ (sql.assign_fmt("begin dbms_utility.VALIDATE(%ld, true); end ", obj_info.recompile_obj_id_));
  if (OB_SUCC(ret)) {
    int tmp_ret = connection->execute_write(tenant_id, sql.string(), affected_rows);
    LOG_TRACE("[PLRECOMPILE] recompile single obj using inner sql!", K(tmp_ret), K(sql), K(obj_info.recompile_obj_id_));
  }
  return ret;
}

int ObPLRecompileTaskHelper::get_recompile_pl_objs(common::ObMySQLProxy* sql_proxy,
                                      common::hash::ObHashMap<ObString, int64_t>& dropped_ref_objs,
                                      ObIArray<ObPLRecompileInfo>& dep_objs,
                                      uint64_t tenant_id,
                                      ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObSqlString query_inner_sql;
  common::sqlclient::ObMySQLResult *result = NULL;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    OZ (query_inner_sql.assign_fmt("SELECT * FROM %s where fail_count < 3 and recompile_obj_id > 300000", OB_ALL_PL_RECOMPILE_OBJINFO_TNAME));
    OZ (sql_proxy->read(res, tenant_id, query_inner_sql.ptr()));
    CK (OB_NOT_NULL(result = res.get_result()));
    if (OB_SUCC(ret)) {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        int64_t dep_obj_id = OB_INVALID_ID;
        ObString droped_ref_obj_name;
        int64_t fail_count = 0;
        int64_t droped_ref_obj_schema_version;
        EXTRACT_INT_FIELD_MYSQL(*result, "recompile_obj_id", dep_obj_id, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "ref_obj_name", droped_ref_obj_name);
        EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", droped_ref_obj_schema_version, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "fail_count", fail_count, int64_t);
        ObPLRecompileInfo tmp_tuple(dep_obj_id, fail_count);
        OZ (ob_write_string(allocator, droped_ref_obj_name, tmp_tuple.ref_obj_name_));
        OX (tmp_tuple.schema_version_ = droped_ref_obj_schema_version);
        if (OB_SUCC(ret) &&!tmp_tuple.ref_obj_name_.empty()) {
          int tmp_ret = dropped_ref_objs.set_refactored(tmp_tuple.ref_obj_name_, droped_ref_obj_schema_version);
          ret = OB_HASH_EXIST == tmp_ret ? OB_SUCCESS : tmp_ret;
        }
        OZ (dep_objs.push_back(tmp_tuple));
      }
      SET_ITERATE_END_RET;
    }
  }
  return ret;
}
#undef SET_ITERATE_END_RET

int ObPLRecompileTaskHelper::batch_recompile_obj(common::ObMySQLProxy* sql_proxy,
                                              uint64_t tenant_id,
                                              int64_t max_schema_version,
                                              ObISQLConnection *connection,
                                              ObIArray<ObPLRecompileInfo>& dep_objs,
                                              common::hash::ObHashMap<ObString, int64_t>& dropped_ref_objs,
                                              int64_t recompile_start,
                                              schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  int64_t batch_num = 200;
  for (int i = 0; OB_SUCC(ret) && i < dep_objs.count(); i = i + batch_num) {
    int64_t start = i;
    int64_t end = i + batch_num > dep_objs.count() ? dep_objs.count() : i + batch_num;
    for (; OB_SUCC(ret) && start < end; ++start) {
      ObString ref_obj_name = dep_objs.at(start).ref_obj_name_;
      bool need_check_dropped_ref_obj = !ref_obj_name.empty();
      int64_t dropped_schema_version = 0;
      int64_t cur_time = ObTimeUtility::current_time();
      if (cur_time - recompile_start > 1680000000) {
          // 28 * 60 * 1000000 us
          break;
      } else if (need_check_dropped_ref_obj) {
        int64_t tmp_ret = dropped_ref_objs.get_refactored(ref_obj_name, dropped_schema_version);
        ret = tmp_ret == OB_HASH_NOT_EXIST ? OB_SUCCESS : tmp_ret;
      }
      if (OB_SUCC(ret)) {
        if (dropped_schema_version != 0) {
          // do nothing
        } else if (dep_objs.at(start).fail_cnt_ < 3) {
          OZ (recompile_single_obj(dep_objs.at(start), connection, tenant_id));
        }
      } else {
        LOG_WARN("[PLRECOMPILE]: Unexpected error!", K(ret));
      }
    }
    int tmp_ret = update_recomp_table(dep_objs, sql_proxy, tenant_id, max_schema_version, i, start, schema_guard);
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObPLRecompileTaskHelper::recompile_pl_objs(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  UNUSED(result);
  UNUSED(params);
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  common::ObMySQLProxy* sql_proxy = NULL;
  ObSqlCtx *sql_ctx = NULL;
  ObSQLSessionInfo *sess_info = NULL;
  bool enable_pl_recompile = false;
  int64_t recompile_start = ObTimeUtility::current_time();
  uint64_t consumer_group_id = 0;
  schema::ObSchemaGetterGuard *schema_guard = NULL;
  // prepare basic env info
  if (OB_ISNULL(ctx.exec_ctx_) ||
      OB_ISNULL(ctx.exec_ctx_->get_sql_ctx()) ||
      OB_ISNULL(ctx.exec_ctx_->get_sql_ctx()->session_info_) ||
      OB_ISNULL(ctx.exec_ctx_->get_sql_proxy()) ||
      OB_ISNULL(ctx.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PLRECOMPILE]: Basic info can not be null", K(ret));
  } else {
    sql_ctx = ctx.exec_ctx_->get_sql_ctx();
    sess_info = sql_ctx->session_info_;
    sql_proxy = ctx.exec_ctx_->get_sql_proxy();
    tenant_id = sess_info->get_effective_tenant_id();
    schema_guard = sql_ctx->schema_guard_;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      enable_pl_recompile = tenant_config->_enable_pl_recompile_job;
    }
    OZ (G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_function_type(tenant_id, ObFunctionType::PRIO_PL_RECOMPILE, consumer_group_id));
  }
  lib::ConsumerGroupIdGuard consumer_group_id_guard_(consumer_group_id);
  uint64_t data_version = 0;
  OZ (GET_MIN_DATA_VERSION(tenant_id, data_version));
  bool enable = enable_pl_recompile
                && !GCONF.in_upgrade_mode()
                && (data_version >= DATA_VERSION_4_3_5_2 && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_5_2);
  LOG_INFO("[PLRECOMPILE]: before recompile ", K(ret), K(consumer_group_id), K(enable), K(recompile_start));
  if (enable) {
    int64_t max_schema_version = 0;
    OZ (collect_delta_recompile_obj_data(sql_proxy,
                                          tenant_id,
                                          ctx.exec_ctx_->get_allocator(),
                                          max_schema_version,
                                          schema_guard));
    if (OB_SUCC(ret)) {
      ObArray<ObPLRecompileInfo> dep_objs;
      common::hash::ObHashMap<ObString, int64_t> dropped_ref_objs;
      ObInnerSQLConnection *connection = nullptr;
      ObInnerSQLConnectionPool *pool = nullptr;
      ObFreeSessionCtx free_session_ctx;
      ObSQLSessionInfo *session_info = nullptr;
      uint64_t compat_version;
      ObObj compat_obj;
      int tmp_ret = OB_SUCCESS;
      OZ (sess_info->get_compatibility_version(compat_version));
      OX (compat_obj.set_uint64(compat_version));
      CK (OB_NOT_NULL(pool = static_cast<ObInnerSQLConnectionPool *>(sql_proxy->get_pool())));
      OZ (create_session(tenant_id, free_session_ctx, session_info));
      CK (OB_NOT_NULL(session_info));
      OZ (init_session(*session_info, tenant_id, sess_info->get_tenant_name(), ObCompatibilityMode::ORACLE_MODE, compat_obj));
      OZ (pool->acquire_spi_conn(session_info, connection));
      CK (OB_NOT_NULL(connection));
      OX (connection->set_oracle_compat_mode());
      OZ (dropped_ref_objs.create(32, "PlRecompileJob"));
      OZ (get_recompile_pl_objs(sql_proxy, dropped_ref_objs, dep_objs, tenant_id, ctx.exec_ctx_->get_allocator()));
      OZ (update_dropped_obj(dropped_ref_objs, sql_proxy, tenant_id));
      OZ (batch_recompile_obj(sql_proxy, tenant_id, max_schema_version, connection,
                            dep_objs, dropped_ref_objs, recompile_start, schema_guard));
      if (dropped_ref_objs.created()) {
        tmp_ret = dropped_ref_objs.destroy();
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
      if (session_info != nullptr) {
        tmp_ret = destroy_session(free_session_ctx, session_info);
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
      if (pool != nullptr && connection != nullptr) {
        tmp_ret = pool->release(connection, true);
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("[PLRECOMPILE]: finish pl background recompile task!", K(ret), K(enable), K(end_time));
  return ret;
}

int ObPLRecompileTaskHelper::create_session(const uint64_t tenant_id,
                            ObFreeSessionCtx &free_session_ctx,
                            ObSQLSessionInfo *&session_info)
{
  int ret = OB_SUCCESS;
  uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null");
  } else if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("alloc session id failed");
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
                 tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), session_info))) {
    LOG_WARN("create session failed", K(ret), K(sid));
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session_info = nullptr;
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info is null", K(ret));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  return ret;
}

int ObPLRecompileTaskHelper::init_session(ObSQLSessionInfo &session,
                                          const uint64_t tenant_id,
                                          const ObString &tenant_name,
                                          const ObCompatibilityMode compat_mode,
                                          ObObj &compat_obj)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
  const bool print_info_log = true;
  const bool is_sys_tenant = true;
  ObPCMemPctConf pc_mem_conf;
  ObObj compatibility_mode;
  ObObj sql_mode;
  const ObUserInfo *user_info = nullptr;

  CK (OB_NOT_NULL(GCTX.schema_service_));
  OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
  if (ObCompatibilityMode::ORACLE_MODE == compat_mode) {
    compatibility_mode.set_int(1);
    sql_mode.set_uint(ObUInt64Type, DEFAULT_ORACLE_MODE);
  } else {
    compatibility_mode.set_int(0);
    sql_mode.set_uint(ObUInt64Type, DEFAULT_MYSQL_MODE);
  }
  OX (session.set_inner_session());
  OZ (session.load_default_sys_variable(print_info_log, is_sys_tenant));
  OZ (session.update_max_packet_size());
  OZ (session.init_tenant(tenant_name.ptr(), tenant_id));
  OZ (session.load_all_sys_vars(schema_guard));
  OZ (session.update_sys_variable(share::SYS_VAR_SQL_MODE, sql_mode));
  OZ (session.update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, compatibility_mode));
  OZ (session.update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_VERSION, compat_obj));
  OZ (session.get_pc_mem_conf(pc_mem_conf));
  CK (OB_NOT_NULL(GCTX.sql_engine_));

  if (ObCompatibilityMode::ORACLE_MODE == compat_mode) {
    OX (session.set_database_id(OB_ORA_SYS_DATABASE_ID));
    OZ (session.set_default_database(OB_ORA_SYS_SCHEMA_NAME));
    OZ (schema_guard.get_user_info(tenant_id, OB_ORA_SYS_USER_ID, user_info));
  } else {
    OX (session.set_database_id(OB_SYS_DATABASE_ID));
    OZ (session.set_default_database(OB_SYS_DATABASE_NAME));
    OZ (schema_guard.get_user_info(tenant_id, OB_SYS_USER_ID, user_info));
  }
  CK (OB_NOT_NULL(user_info));
  OZ (session.set_user(
          user_info->get_user_name(), user_info->get_host_name_str(), user_info->get_user_id()));
  OX (session.set_priv_user_id(user_info->get_user_id()));
  OX (session.set_user_priv_set(user_info->get_priv_set()));
  OZ (schema_guard.get_db_priv_set(
          tenant_id, user_info->get_user_id(), OB_SYS_DATABASE_NAME, db_priv_set));
  OX (session.set_db_priv_set(db_priv_set));
  return ret;
}

int ObPLRecompileTaskHelper::destroy_session(ObFreeSessionCtx &free_session_ctx,
                            ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null");
  } else if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info is null");
  } else {
    session_info->set_session_sleep();
    GCTX.session_mgr_->revert_session(session_info);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
  }
  return ret;
}

bool ObPLRecompileTaskHelper::is_pl_create_ddl_operation(ObSchemaOperationType op_type)
{
  return  op_type == OB_DDL_CREATE_ROUTINE ||
          op_type == OB_DDL_REPLACE_ROUTINE ||
          op_type == OB_DDL_ALTER_ROUTINE ||
          op_type == OB_DDL_CREATE_PACKAGE ||
          op_type == OB_DDL_ALTER_PACKAGE ||
          op_type == OB_DDL_CREATE_UDT ||
          op_type == OB_DDL_REPLACE_UDT ||
          op_type == OB_DDL_CREATE_TRIGGER ||
          op_type == OB_DDL_ALTER_TRIGGER;
}

bool ObPLRecompileTaskHelper::is_pl_drop_ddl_operation(ObSchemaOperationType op_type)
{
  return op_type == OB_DDL_DROP_ROUTINE ||
          op_type == OB_DDL_DROP_PACKAGE ||
          op_type == OB_DDL_DROP_UDT ||
          op_type == OB_DDL_DROP_UDT_BODY ||
          op_type == OB_DDL_DROP_TRIGGER;
}

bool ObPLRecompileTaskHelper::is_sql_create_ddl_operation(ObSchemaOperationType op_type)
{
  return  op_type == OB_DDL_CREATE_TABLE ||
          op_type == OB_DDL_ALTER_TABLE ||
          op_type == OB_DDL_CREATE_VIEW ||
          op_type == OB_DDL_CREATE_SYNONYM ||
          op_type == OB_DDL_REPLACE_SYNONYM ||
          op_type == OB_DDL_CREATE_SEQUENCE ||
          op_type == OB_DDL_ALTER_SEQUENCE;
}

bool ObPLRecompileTaskHelper::is_sql_drop_ddl_operation(ObSchemaOperationType op_type)
{
  return  op_type == OB_DDL_DROP_TABLE ||
          op_type == OB_DDL_DROP_TABLE_TO_RECYCLEBIN ||
          op_type == OB_DDL_TABLE_RENAME ||
          op_type == OB_DDL_DROP_VIEW ||
          op_type == OB_DDL_DROP_VIEW_TO_RECYCLEBIN ||
          op_type == OB_DDL_DROP_SYNONYM ||
          op_type == OB_DDL_DROP_SEQUENCE;
  }

bool ObPLRecompileTaskHelper::is_pl_object_type(ObObjectType obj_type)
{
  return  obj_type == ObObjectType::PACKAGE_BODY ||
          obj_type == ObObjectType::TYPE_BODY ||
          obj_type == ObObjectType::TRIGGER ||
          obj_type == ObObjectType::FUNCTION ||
          obj_type == ObObjectType::PROCEDURE ||
          obj_type == ObObjectType::SYS_PACKAGE;
}


}//end namespace share
}//end namespace oceanbase
