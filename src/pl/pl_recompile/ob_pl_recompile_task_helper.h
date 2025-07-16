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
#ifndef OCEANBASE_SRC_PL_OB_PL_RECOMPILE_TASK_HELPER_H_
#define OCEANBASE_SRC_PL_OB_PL_RECOMPILE_TASK_HELPER_H_

#include "share/schema/ob_schema_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_inner_sql_connection_pool.h"

namespace oceanbase
{
using namespace common::sqlclient;
namespace pl
{

class ObPLRecompileTaskHelper
{
public:
struct ObPLRecompileInfo
{
public:
  ObPLRecompileInfo(int64_t dep_obj_id = OB_INVALID_ID,
                   int64_t fail_cnt = 0,
                   int64_t dropped_ref_obj_schema_version = 0,
                   const ObString& ref_obj_name = ObString())
    : recompile_obj_id_(dep_obj_id),
      fail_cnt_(fail_cnt),
      schema_version_(dropped_ref_obj_schema_version),
      ref_obj_name_(ref_obj_name) {}

  ~ObPLRecompileInfo() {
    recompile_obj_id_ = OB_INVALID_ID;
    fail_cnt_ = 0;
    schema_version_ = 0;
    ref_obj_name_.reset();
  }

  bool operator ==(const ObPLRecompileInfo &other) const
  {
    return ((this == &other)
        || (this->recompile_obj_id_ == other.recompile_obj_id_
          && this->fail_cnt_ == other.fail_cnt_
          && this->schema_version_ == other.schema_version_
          && this->ref_obj_name_ == other.ref_obj_name_));
  }

  TO_STRING_KV(K_(recompile_obj_id), K_(fail_cnt),
              K_(schema_version), K_(ref_obj_name));

  int64_t recompile_obj_id_;
  int64_t fail_cnt_;
  int64_t schema_version_;
  ObString ref_obj_name_;
};
  static int recompile_pl_objs(pl::ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int collect_delta_recompile_obj_data(common::ObMySQLProxy* sql_proxy,
                                                      uint64_t tenant_id,
                                                      ObIAllocator& allocator,
                                                      int64_t& last_max_schema_version,
                                                      share::schema::ObSchemaGetterGuard *schema_guard);
  static int collect_delta_error_data(common::ObMySQLProxy* sql_proxy,
                                                      uint64_t tenant_id,
                                                      int64_t last_max_schema_version,
                                                      ObIAllocator& allocator,
                                                      ObIArray<ObPLRecompileInfo>& dep_objs);
  static int collect_delta_ddl_operation_data(common::ObMySQLProxy* sql_proxy,
                                      uint64_t tenant_id,
                                      ObIArray<ObPLRecompileInfo>& dep_objs,
                                      ObArray<int64_t>& ddl_alter_obj_infos,
                                      common::hash::ObHashMap<int64_t, std::pair<ObString, int64_t>>& ddl_drop_obj_map);
  static int batch_insert_recompile_obj_info(common::ObMySQLProxy* sql_proxy,
                                                      uint64_t tenant_id,
                                                      int64_t cur_max_schema_version,
                                                      ObIArray<ObPLRecompileInfo>& dep_objs);
  static int batch_recompile_obj(common::ObMySQLProxy* sql_proxy,
                                              uint64_t tenant_id,
                                              int64_t max_schema_version,
                                              ObISQLConnection *connection,
                                              ObIArray<ObPLRecompileInfo>& dep_objs,
                                              common::hash::ObHashMap<ObString, int64_t>& dropped_ref_objs,
                                              int64_t recompile_start);
  static int construct_select_dep_table_sql(ObSqlString& query_inner_sql,
                                      common::hash::ObHashMap<int64_t, std::pair<ObString, int64_t>>& ddl_drop_obj_map,
                                      ObIArray<int64_t>& ddl_alter_obj_infos,
                                      uint64_t tenant_id);
  static int get_recompile_pl_objs(common::ObMySQLProxy* sql_proxy,
                                      common::hash::ObHashMap<ObString, int64_t>& dropped_ref_objs,
                                      ObIArray<ObPLRecompileInfo>& dep_objs,
                                      uint64_t tenant_id,
                                      ObIAllocator& allocator);
  static int update_dropped_obj(common::hash::ObHashMap<ObString, int64_t>& dropped_ref_objs,
                                  common::ObMySQLProxy* sql_proxy,
                                  uint64_t tenant_id);
  static int update_recomp_table(ObIArray<ObPLRecompileInfo>& dep_objs,
                                  common::ObMySQLProxy* sql_proxy,
                                  uint64_t tenant_id,
                                  int64_t last_max_schema_version,
                                  int64_t start,
                                  int64_t end);
  static int recompile_single_obj(ObPLRecompileInfo& obj_info,
                                  ObISQLConnection *connection,
                                  uint64_t tenant_id);
  static bool is_pl_create_ddl_operation(ObSchemaOperationType op_type);
  static bool is_pl_drop_ddl_operation(ObSchemaOperationType op_type);
  static bool is_sql_create_ddl_operation(ObSchemaOperationType op_type);
  static bool is_sql_drop_ddl_operation(ObSchemaOperationType op_type);
  static bool is_pl_object_type(ObObjectType obj_type);
  static int init_tenant_recompile_job(const share::schema::ObSysVariableSchema &sys_variable,
                                      uint64_t tenant_id,
                                      ObMySQLTransaction &trans);
  static int check_job_exists(ObMySQLTransaction &trans,
                                            const uint64_t tenant_id,
                                            const ObString &job_name,
                                            bool &is_job_exists);
  static int find_udt_id(common::ObMySQLProxy* sql_proxy,
                          uint64_t tenant_id,
                          uint64_t coll_type,
                          int64_t& udt_id);

};

}//end namespace pl
}//end namespace oceanbase
#endif
