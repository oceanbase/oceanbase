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

#include "ob_redis_importer.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_basic_session_info.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "sql/engine/ob_exec_context.h"
#include "share/table/redis/ob_redis_util.h"

#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
using namespace share;
using namespace sql;
using namespace common;
using namespace obrpc;
namespace table
{
//*******************
// ObModuleDataArg //
//*******************

bool ObModuleDataArg::is_valid() const
{
  return op_ > ObModuleDataArg::INVALID_OP
      && op_ < ObModuleDataArg::MAX_OP
      && target_tenant_id_ != OB_INVALID_TENANT_ID
      && module_ > ObModuleDataArg::INVALID_MOD
      && module_ < ObModuleDataArg::MAX_MOD;
}

int ObModuleDataArg::assign(const ObModuleDataArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    file_path_ = arg.file_path_;
    op_ = arg.op_;
    target_tenant_id_ = arg.target_tenant_id_;
    module_ = arg.module_;
  }
  return ret;
}

//*******************
// ObRedisImporter //
//*******************

int ObRedisImporter::exec_op(table::ObModuleDataArg::ObInfoOpType op)
{
  int ret = OB_SUCCESS;
  if (op == ObModuleDataArg::LOAD_INFO) {
    ret = import_redis_info();
  } else if (op == ObModuleDataArg::CHECK_INFO) {
    ret = check_redis_info();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exec op", K(ret), K(op));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to operate info", K(ret), K(op));
  }
  return ret;
}

int ObRedisImporter::get_sql_uint_result(const char *sql, const char *col_name, uint64_t &sql_res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not null", K(ret), KP(exec_ctx_.get_sql_proxy()));
  } else {
    ObCommonSqlProxy *sql_proxy = exec_ctx_.get_sql_proxy();
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy->read(res, tenant_id_, sql))) {
        SHARE_LOG(WARN, "failed to read", K(ret), K(tenant_id_), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "failed to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        // should has one line
        LOG_WARN("fail to get next result", K(ret));
      } else {
        ObObjMeta meta;
        if (OB_FAIL(result->get_type(col_name, meta))) {
          LOG_WARN("fail to get type", K(ret), K(col_name));
        } else if (meta.is_number()) {
          common::number::ObNumber num;
          if (OB_FAIL(result->get_number(col_name, num))) {
            LOG_WARN("fail to get column in row. ", "column_name", col_name, K(ret));
          } else if (OB_FAIL(ObJsonBaseUtil::number_to_uint(num, sql_res))) {
            LOG_WARN("fail to convert number to uint", K(ret), K(num));
          }
        } else if (meta.is_int()) {
          int64_t int_res = 0;
          if (OB_FAIL(result->get_int(col_name, int_res))) {
            LOG_WARN("fail to get column in row. ", "column_name", col_name, K(ret));
          } else if (int_res < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected sql res", K(ret), K(int_res));
          } else {
            sql_res = static_cast<uint64_t>(int_res);
          }
        }
      }
    }
  }
  return ret;
}

int ObRedisImporter::get_tenant_memory_size(uint64_t &memory_size)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMemAttr attr(MTL_ID(), "RedisImpt");
  sql.set_attr(attr);
  if (OB_FAIL(
          sql.assign_fmt("SELECT sum(memory_size) as MEM from oceanbase.%s where tenant_id = %lu;",
                         OB_ALL_VIRTUAL_UNIT_TNAME,
                         tenant_id_))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else if (OB_FAIL(get_sql_uint_result(sql.ptr(), "MEM", memory_size))) {
    LOG_WARN("fail to get sql uint result", K(ret));
  }
  return ret;
}

int ObRedisImporter::get_kv_mode(ObKvModeType &kv_mode_type)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard sys_schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID,
                                                                   sys_schema_guard))) {
    LOG_WARN("get sys schema guard failed", KR(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObSysVarSchema *var_schema = nullptr;
    ObObj kv_mode_obj;
    ObArenaAllocator tmp_allocator;
    int64_t kv_mode_val = 0;
    const common::ObDataTypeCastParams dtc_params(nullptr);
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard failed", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_tenant_system_variable(
                   tenant_id_, OB_SV_KV_MODE, var_schema))) {
      LOG_WARN("get tenant system variable failed", KR(ret), K(tenant_id_));
    } else if (OB_ISNULL(var_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("var schema is null", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(var_schema->get_value(&tmp_allocator, dtc_params, kv_mode_obj))) {
      LOG_WARN("get value from sysvar schema failed", K(ret));
    } else if (OB_FAIL(kv_mode_obj.get_int(kv_mode_val))) {
      LOG_WARN("fail to get int from obj", K(ret), K(kv_mode_obj));
    } else {
      kv_mode_type = static_cast<ObKvModeType>(kv_mode_val);
    }
  }
  return ret;
}

// check
// 1. mysql mode
// 2. version >= 4.2.5
// 3. kv mode allow redis
int ObRedisImporter::check_basic_info(bool &need_import)
{
  int ret = OB_SUCCESS;
  ObKvModeType kv_mode = ObKvModeType::NONE;
  need_import = false;
  if (OB_ISNULL(exec_ctx_.get_sql_proxy()) || OB_ISNULL(exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy and session must not null", K(ret), KP(exec_ctx_.get_sql_proxy()));
  } else if (!lib::is_mysql_mode()
      || GET_MIN_CLUSTER_VERSION() < MOCK_CLUSTER_VERSION_4_2_5_0
      || (CLUSTER_VERSION_4_3_0_0 <= GET_MIN_CLUSTER_VERSION() && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_1)) {
    const char *err_msg = "Redis running in oracle mode or ob version < 4.2.5.0 or between 4.3.0.0 and 4.3.5.1";
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
    LOG_WARN(err_msg, K(ret), K(GET_MIN_CLUSTER_VERSION()), K(lib::is_mysql_mode()));
  } else if (OB_FAIL(get_kv_mode(kv_mode))) {
    LOG_WARN("fial to get kv mode", K(ret), K(tenant_id_));
  } else if (kv_mode != ObKvModeType::REDIS && kv_mode != ObKvModeType::ALL) {
    const char *err_msg = "kv_mode other than redis and all";
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
    LOG_WARN(err_msg, K(ret), K(kv_mode));
  } else {
    need_import = true;
  }
  return ret;
}

int ObRedisImporter::check_redis_info()
{
  int ret = OB_SUCCESS;
  bool need_import = false;
  bool is_valid = true;
  if (OB_FAIL(check_basic_info(need_import))) {
    LOG_WARN("fail to check basic info", K(ret));
  } else if (need_import) {
    // 1. check database
    ObSqlString sql;
    ObMemAttr attr(MTL_ID(), "RedisImpt");
    sql.set_attr(attr);
    uint64_t cnt = 0;
    uint64_t db_id = 0;
    const char *CNT = "cnt";
    if (OB_FAIL(sql.assign_fmt("SELECT DATABASE_ID from oceanbase.%s where database_name = \"%s\";",
                               OB_ALL_DATABASE_TNAME,
                               ObRedisInfoV1::DB_NAME))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(get_sql_uint_result(sql.ptr(), "DATABASE_ID", db_id))) {
      LOG_WARN("fail to get sql uint result", K(ret), K(sql));
    }

    // 2. check tables
    for (int i = 0; OB_SUCC(ret) && i < ObRedisInfoV1::REDIS_MODEL_NUM; ++i) {
      sql.reset();
      uint64_t table_id = 0;
      ObString table_name;
      if (OB_FAIL(
              ObRedisHelper::get_table_name_by_model(static_cast<ObRedisModel>(i), table_name))) {
        LOG_WARN("failed to get table name by model", K(ret), K(i));
      } else if (OB_FAIL(sql.assign_fmt("SELECT TABLE_ID from oceanbase.%s where database_id = %lu "
                                        "and table_name = \"%s\";",
                                        OB_ALL_TABLE_TNAME,
                                        db_id,
                                        table_name.ptr()))) {
        LOG_WARN("failed to set sql", K(ret), K(sql));
      } else if (OB_FAIL(get_sql_uint_result(sql.ptr(), "TABLE_ID", table_id))) {
        LOG_WARN("fail to get sql uint result", K(ret), K(sql));
      }
    }

    // 3. check __all_kv_redis_table
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(sql.reset())) {
    } else if (OB_FAIL(sql.assign_fmt(
                   "SELECT count(*) as %s from oceanbase.%s;", CNT, OB_ALL_KV_REDIS_TABLE_TNAME))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(get_sql_uint_result(sql.ptr(), CNT, cnt))) {
      LOG_WARN("fail to get sql uint result", K(ret));
    } else if (cnt != ObRedisInfoV1::COMMAND_NUM) {
      ret = OB_PARTIAL_FAILED;
      LOG_WARN("The number of rows in __all_kv_redis_table is incorrect", K(ret), K(cnt));
    }
  }

  if (OB_FAIL(ret)) {
    ret = OB_PARTIAL_FAILED;
    LOG_USER_ERROR(OB_PARTIAL_FAILED, "redis info is not complete，please retry loading");
    LOG_WARN("redis info is not complete，please retry loading", K(ret));
  }
  return ret;
}

int ObRedisImporter::import_redis_info()
{
  int ret = OB_SUCCESS;
  bool need_import = false;
  LOG_DEBUG("0. check basic info", K(ret));
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(check_basic_info(need_import))) {
    LOG_WARN("fail to check basic info", K(ret));
  } else if (need_import) {
    // 1. get tenant memory to decide partition num
    uint64_t memory_size = 0;
    uint64_t partition_num = 0;
    ObSessionParam session_param;
    ObCommonSqlProxy *sql_proxy = exec_ctx_.get_sql_proxy();
    ObSQLSessionInfo *session = exec_ctx_.get_my_session();
    ObSQLMode sess_sql_mode = session->get_sql_mode();
    session_param.sql_mode_ = reinterpret_cast<int64_t *>(&sess_sql_mode);
    LOG_DEBUG("1. get tenant memory size",
             K(ret),
             K(start_time),
             "cost_time",
             ObTimeUtility::current_time() - start_time);
    if (OB_FAIL(get_tenant_memory_size(memory_size))) {
      LOG_WARN("fail to get tenant memory size", K(ret));
    } else {
      memory_size /= ObRedisInfoV1::GB;  // byte -> G
      partition_num = memory_size > 2 ? ObRedisInfoV1::PARTITION_NUM_LARGER :
        ObRedisInfoV1::PARTITION_NUM;
    }

    // 2. create database
    LOG_DEBUG("2. create database", K(ret), "cost_time", ObTimeUtility::current_time() - start_time);
    ObSqlString sql_str;
    ObMemAttr attr(MTL_ID(), "RedisImpt");
    sql_str.set_attr(attr);
    int64_t affected_rows = 0;  // unused
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_str.assign_fmt("create database %s", ObRedisInfoV1::DB_NAME))) {
      LOG_WARN("failed to set sql", K(ret), K(sql_str));
    } else if (OB_FAIL(sql_proxy->write(tenant_id_, sql_str.ptr(), affected_rows, ObCompatibilityMode::MYSQL_MODE, &session_param))) {
      if (ret == OB_DATABASE_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to execute sql write", K(ret), K(sql_str));
      }
    }

    // 3. create tables
    LOG_DEBUG("3. create tables", K(ret), "cost_time", ObTimeUtility::current_time() - start_time);
    for (int i = 0; OB_SUCC(ret) && i < ObRedisInfoV1::REDIS_MODEL_NUM; ++i) {
      sql_str.reset();
      if (OB_FAIL(sql_str.assign_fmt("%s %lu", ObRedisInfoV1::TABLE_SQLS[i], partition_num))) {
        LOG_WARN("failed to set sql", K(ret), K(sql_str));
      } else if (OB_FAIL(sql_proxy->write(tenant_id_, sql_str.ptr(), affected_rows, ObCompatibilityMode::MYSQL_MODE, &session_param))) {
        LOG_WARN("failed to execute sql write", K(ret), K(sql_str));
      }
    }

    // 4. import data to inner table
    LOG_DEBUG("4. import data to inner table",
             K(ret),
             "cost_time",
             ObTimeUtility::current_time() - start_time);
    ObMySQLTransaction trans;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.start(sql_proxy, tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < ObRedisInfoV1::COMMAND_NUM; ++i) {
      sql_str.reset();
      if (OB_FAIL(
              sql_str.assign_fmt("REPLACE INTO oceanbase.%s(command_name, table_name) VALUES (%s);",
                                 OB_ALL_KV_REDIS_TABLE_TNAME,
                                 ObRedisInfoV1::ALL_KV_REDIS_TABLE_SQLS[i]))) {
        LOG_WARN("failed to set sql", K(ret), K(sql_str));
      } else if (OB_FAIL(trans.write(tenant_id_, sql_str.ptr(), affected_rows))) {
        LOG_WARN("failed to execute sql write", K(ret), K(sql_str));
      } else {
        affected_rows_ += affected_rows;
      }
    }
    if (trans.is_started()) {
      bool commit = (OB_SUCCESS == ret);
      int tmp_ret = ret;
      if (OB_FAIL(trans.end(commit))) {
        LOG_WARN("faile to end trans", "commit", commit, KR(ret));
      }
      ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    }
  }
  return ret;
}
}  // end namespace table
}  // namespace oceanbase
