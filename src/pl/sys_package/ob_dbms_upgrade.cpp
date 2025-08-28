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

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_upgrade.h"
#include "pl/ob_pl_package_manager.h"

namespace oceanbase
{
namespace pl
{

int ObDBMSUpgrade::upgrade_single(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString package_name;
  bool load_from_file = true;
  ObCompatibilityMode mode = lib::is_oracle_mode() ? ObCompatibilityMode::ORACLE_MODE
                                                   : ObCompatibilityMode::MYSQL_MODE;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  // OBServer 4.2.4 has added new parameters on the __DBMS_UPGRADE
  // interface to control whether to load the system package source code from
  // a file or embeded c string. To maintain compatibility during upgarding,
  // it is necessary to distinguish the old and new versions of the interface.
  // However, the system package does not have version control, so it depends
  // on the number of parameters to judge the old and new versions.
  if (OB_FAIL(ret)) {
  } else if (1 == params.count()) {
    OV (params.at(0).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params.at(0).get_string(package_name));
    OV (!package_name.empty(), OB_INVALID_ARGUMENT);
  } else if (2 == params.count()) {
    OV (params.at(0).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params.at(0).get_string(package_name));
    OV (!package_name.empty(), OB_INVALID_ARGUMENT);
    OV (params.at(1).is_tinyint(), OB_INVALID_ARGUMENT);
    OZ (params.at(1).get_bool(load_from_file));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("__DBMS_UPGRADE.UPGRADE_SINGLE require one or two arguments", K(ret), K(params));
  }
  OZ (ObPLPackageManager::load_sys_package(*ctx.get_sql_proxy(), package_name, mode, load_from_file));
  return ret;
}

int ObDBMSUpgrade::upgrade_all(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  bool load_from_file = true;
  ObCompatibilityMode mode = lib::is_oracle_mode() ? ObCompatibilityMode::ORACLE_MODE
                                                   : ObCompatibilityMode::MYSQL_MODE;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  if (OB_FAIL(ret)) {
  } else if (0 == params.count()) {
    // do nothing
  } else if (1 == params.count()) {
    OV (params.at(0).is_tinyint(), OB_INVALID_ARGUMENT);
    OZ (params.at(0).get_bool(load_from_file));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("__DBMS_UPGRADE.UPGRADE_ALL require none or one arguments", K(ret), K(params));
  }
  OZ (ObPLPackageManager::load_all_common_sys_package(*ctx.get_sql_proxy(), mode, load_from_file));
  return ret;
}

int ObDBMSUpgrade::get_job_action(ObSqlString &job_action, ObSqlString &query_sql)
{
  int ret = OB_SUCCESS;

  common::ObZone zone;
  ObArray<ObServerInfoInTable> servers_info;
  common::hash::ObHashSet<ObServerInfoInTable::ObBuildVersion> observer_version_set;
  bool need_comma = false;

  job_action.reset();
  query_sql.reset();

  OZ (observer_version_set.create((4)));
  OZ (share::ObAllServerTracer::get_instance().get_servers_info(zone, servers_info));
  for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); ++i) {
    OZ (observer_version_set.set_refactored(servers_info.at(i).get_build_version()));
  }

  //OZ (get_package_and_svn(build_version, sizeof(build_version)));
  //OZ (job_action.assign_fmt("delete FROM %s where build_version != '%s'", OB_ALL_NCOMP_DLL_V2_TNAME, build_version));
  OZ (job_action.assign_fmt("delete FROM %s where build_version not in (", OB_ALL_NCOMP_DLL_V2_TNAME));
  OZ (query_sql.assign_fmt("select key_id from %s where build_version not in (", OB_ALL_NCOMP_DLL_V2_TNAME));
  for (common::hash::ObHashSet<ObServerInfoInTable::ObBuildVersion>::const_iterator iter = observer_version_set.begin();
      OB_SUCC(ret) && iter != observer_version_set.end();
      iter++) {
    OZ (job_action.append_fmt("%s'%s'", need_comma ? ", " : "", iter->first.ptr()));
    OZ (query_sql.append_fmt("%s'%s'", need_comma ? ", " : "", iter->first.ptr()));
    OX (need_comma = true);
  }
  OZ (job_action.append(")"));
  OZ (query_sql.append(")"));

  if (observer_version_set.created()) {
    observer_version_set.destroy();
  }

  return ret;
}

int ObDBMSUpgrade::flush_dll_ncomp(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(params);
  UNUSED(result);
  ObSqlString job_action;
  ObSqlString query_sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  sql::ObSQLSessionInfo *session = NULL;
  common::sqlclient::ObMySQLResult *mysql_result = NULL;
  ObArray<int64_t> key_ids;

  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(session = ctx.get_my_session()));
  OZ (get_job_action(job_action, query_sql));
  OX (tenant_id = session->get_effective_tenant_id());
  if (OB_SUCC(ret)) {
    // get deleted pl obj and insert into __all_pl_recompile_objinfo
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      OZ (ctx.get_sql_proxy()->read(res, tenant_id, query_sql.ptr()));
      CK (OB_NOT_NULL(mysql_result = res.get_result()));
      if (OB_SUCC(ret)) {
        int64_t key_id; 
        while (OB_SUCC(mysql_result->next())) {
          EXTRACT_INT_FIELD_MYSQL(*mysql_result, "key_id", key_id, int64_t);
          OZ (key_ids.push_back(key_id));
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("Unexpected iterate error!", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !key_ids.empty()) {
      const int iter_num = 100;
      const int round = key_ids.count() / iter_num + 1;
      for (int64_t i = 0; OB_SUCC(ret) && i < round; ++i) {
        const int floor = i * iter_num;
        const int ceil = i == round - 1 ? key_ids.count() : (i+1)*iter_num;
        OZ (query_sql.assign_fmt("INSERT INTO %s (recompile_obj_id, ref_obj_name, schema_version, fail_count) VALUES ", 
                  OB_ALL_PL_RECOMPILE_OBJINFO_TNAME));
        for (int64_t j = floor; OB_SUCC(ret) && j < ceil ; ++j) { // [floor, ceil)
          OZ (query_sql.append_fmt("( %ld, '', 0 , 0 ) %s ", 
                    key_ids.at(j), j < ceil - 1  ? " , " : " "));        
        }
        OZ (query_sql.append_fmt("ON DUPLICATE KEY UPDATE"
                                  " recompile_obj_id = VALUES(recompile_obj_id), "
                                  " ref_obj_name = VALUES(ref_obj_name), "
                                  " schema_version = VALUES(schema_version), "
                                  " fail_count = VALUES(fail_count); "));
        OZ (ctx.get_sql_proxy()->write(tenant_id, query_sql.ptr(), affected_rows));
      }
    }
  }
  // delete old version disk cache obj
  OZ (ctx.get_sql_proxy()->write(tenant_id, job_action.ptr(), affected_rows));
  LOG_INFO("flush dll ncomp", K(ret), K(job_action), K(query_sql), K(affected_rows));

  return ret;
}

} // end of pl
} // end oceanbase
