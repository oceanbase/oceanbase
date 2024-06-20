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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_dependency_info.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
namespace sql
{
bool ObMVDepInfo::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_)
             && (OB_INVALID_ID != mview_id_)
             && (OB_INVALID_ID != p_obj_);
}

int ObMVDepUtils::get_mview_dep_infos(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t mview_table_id,
    ObIArray<ObMVDepInfo> &dep_infos)
{
  int ret = OB_SUCCESS;
  if ((OB_INVALID_TENANT_ID == tenant_id)
      || (OB_INVALID_ID == mview_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or mview_table_id", KR(ret), K(tenant_id), K(mview_table_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      ObMySQLResult *result = NULL;
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      if (OB_FAIL(sql.assign_fmt("SELECT p_order, p_obj, p_type, qbcid, flags FROM %s"
                                 " WHERE tenant_id = %lu AND mview_id = %lu ORDER BY p_order",
                                 OB_ALL_MVIEW_DEP_TNAME,
                                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                 mview_table_id))) {
        LOG_WARN("failed to assign sql", KR(ret));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), KP(result));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            ObMVDepInfo dep_info;
            EXTRACT_INT_FIELD_MYSQL(*result, "p_order", dep_info.p_order_, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "p_obj", dep_info.p_obj_, uint64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "p_type", dep_info.p_type_, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "qbcid", dep_info.qbcid_, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "flags", dep_info.flags_, int64_t);

            if (OB_SUCC(ret)) {
              dep_info.tenant_id_ = tenant_id;
              dep_info.mview_id_ = mview_table_id;
              if (OB_FAIL(dep_infos.push_back(dep_info))) {
                LOG_WARN("failed to add dep info", KR(ret), K(dep_info));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMVDepUtils::insert_mview_dep_infos(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t mview_table_id,
    const ObIArray<ObMVDepInfo> &dep_infos)
{
  int ret = OB_SUCCESS;
  if ((OB_INVALID_TENANT_ID == tenant_id)
      || (OB_INVALID_ID == mview_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or mview_table_id", KR(ret), K(tenant_id), K(mview_table_id));
  } else {
    ObDMLSqlSplicer dml;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    for (int64_t i = 0; OB_SUCC(ret) && (i < dep_infos.count()); ++i) {
      const ObMVDepInfo &dep_info = dep_infos.at(i);
      if (!dep_info.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid dep info", KR(ret), K(dep_info));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id",
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("mview_id", mview_table_id))
          || OB_FAIL(dml.add_column("p_order", dep_info.p_order_))
          || OB_FAIL(dml.add_column("p_obj", dep_info.p_obj_))
          || OB_FAIL(dml.add_column("p_type", dep_info.p_type_))
          || OB_FAIL(dml.add_column("qbcid", dep_info.qbcid_))
          || OB_FAIL(dml.add_column("flags", dep_info.flags_))) {
        LOG_WARN("failed to add column", KR(ret), K(dep_info));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish dml row", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObSqlString sql;
      if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_MVIEW_DEP_TNAME, sql))) {
        LOG_WARN("failed to splice batch insert sql", KR(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute write", KR(ret));
      } else if (affected_rows != dep_infos.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected rows does not match the count of dep infos",
            KR(ret), K(affected_rows), K(dep_infos.count()));
      }
    }
  }

  return ret;
}

int ObMVDepUtils::delete_mview_dep_infos(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t mview_table_id)
{
  int ret = OB_SUCCESS;
  if ((OB_INVALID_TENANT_ID == tenant_id)
      || (OB_INVALID_ID == mview_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or mview_table_id", KR(ret), K(tenant_id), K(mview_table_id));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND mview_id = %ld",
                               OB_ALL_MVIEW_DEP_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               mview_table_id))) {
      LOG_WARN("failed to delete from __all_mview_dep table",
          KR(ret), K(tenant_id), K(mview_table_id));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute write", KR(ret), K(sql));
    }
  }

  return ret;
}

int ObMVDepUtils::convert_to_mview_dep_infos(
    const ObIArray<ObDependencyInfo> &deps,
    ObIArray<ObMVDepInfo> &mv_deps)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < deps.count()); ++i) {
    const ObDependencyInfo &dep_info = deps.at(i);
    ObMVDepInfo mv_dep;
    mv_dep.tenant_id_ = dep_info.get_tenant_id();
    mv_dep.mview_id_ = dep_info.get_dep_obj_id();
    mv_dep.p_order_ = dep_info.get_order();
    mv_dep.p_obj_ = dep_info.get_ref_obj_id();
    mv_dep.p_type_ = static_cast<int64_t>(dep_info.get_ref_obj_type());
    if (OB_FAIL(mv_deps.push_back(mv_dep))) {
      LOG_WARN("failed to add mv dep to array", KR(ret), K(mv_dep));
    }
  }

  return ret;
}

int ObMVDepUtils::get_table_ids_only_referenced_by_given_mv(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t mview_table_id,
    ObIArray<uint64_t> &ref_table_ids)
{
  int ret = OB_SUCCESS;
  if ((OB_INVALID_TENANT_ID == tenant_id)
      || (OB_INVALID_ID == mview_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or mview_table_id",
        KR(ret), K(tenant_id), K(mview_table_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      ObMySQLResult *result = NULL;
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      if (OB_FAIL(sql.assign_fmt("select a.p_obj from"
                                 " (select p_obj, count(*) cnt from %s group by p_obj) a,"
                                 " (select p_obj, count(*) cnt from %s where tenant_id = %lu"
                                 " and mview_id = %lu group by p_obj) b"
                                 " where a.p_obj = b.p_obj and a.cnt = b.cnt",
                                 OB_ALL_MVIEW_DEP_TNAME,
                                 OB_ALL_MVIEW_DEP_TNAME,
                                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                 mview_table_id))) {
        LOG_WARN("failed to assign sql", KR(ret));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), KP(result));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            uint64_t ref_table_id = OB_INVALID_ID;
            EXTRACT_INT_FIELD_MYSQL(*result, "p_obj", ref_table_id, uint64_t);
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ref_table_ids.push_back(ref_table_id))) {
                LOG_WARN("failed to add ref table id to array", KR(ret), K(ref_table_id));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}
} // end of sql
} // end of oceanbase