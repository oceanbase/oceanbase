/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_dependency_info.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "sql/resolver/mv/ob_mv_checker.h"

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
    ObIArray<ObMVDepInfo> &dep_infos,
    bool ignore_udt_udf)
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
      if (OB_FAIL(sql.assign_fmt("SELECT p_order, p_obj, p_type, qbcid, flags FROM %s.%s"
                                 " WHERE tenant_id = %lu AND mview_id = %lu ",
                                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                 mview_table_id))) {
        LOG_WARN("failed to assign sql", KR(ret));
      } else if (ignore_udt_udf && OB_FAIL(sql.append_fmt(" AND p_type NOT IN (%ld, %ld) ",
                                   static_cast<int64_t>(ObObjectType::TYPE),
                                   static_cast<int64_t>(ObObjectType::FUNCTION)))) {
        LOG_WARN("failed to append not in udt and udf sql", KR(ret));
      } else if (OB_FAIL(sql.append_fmt(" ORDER BY p_order"))) {
        LOG_WARN("failed to append order by sql", KR(ret), K(sql));
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
    const ObObjectType ref_obj_type = dep_info.get_ref_obj_type();
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

int ObMVDepUtils::get_table_ids_only_referenced_by_given_fast_lsm_mv(
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
      if (OB_FAIL(sql.assign_fmt(
              "select a.p_obj from"
              " (select p_obj from %s dep, %s mv where dep.mview_id = mv.mview_id and "
              "mv.refresh_mode in (%ld) group by p_obj having count(*) = 1) a,"
              " (select p_obj from %s dep, %s mv where dep.mview_id = mv.mview_id and "
              "mv.refresh_mode in (%ld) and dep.tenant_id = %lu and dep.mview_id = %lu) b "
              "where a.p_obj = b.p_obj",
              OB_ALL_MVIEW_DEP_TNAME, OB_ALL_MVIEW_TNAME,
              ObMVRefreshMode::MAJOR_COMPACTION,
              OB_ALL_MVIEW_DEP_TNAME, OB_ALL_MVIEW_TNAME,
              ObMVRefreshMode::MAJOR_COMPACTION,
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), mview_table_id))) {
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
int ObMVDepUtils::get_referring_mv_of_base_table(ObISQLClient &sql_client, const uint64_t tenant_id,
                                                 const uint64_t base_table_id,
                                                 ObIArray<uint64_t> &mview_ids,
                                                 bool &exists_nested_mv)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  exists_nested_mv = false;

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("SELECT mview_id, (SELECT COUNT(*) FROM %s WHERE p_obj = A.mview_id) AS cnt FROM %s A WHERE p_obj = %ld",
                               share::OB_ALL_MVIEW_DEP_TNAME, share::OB_ALL_MVIEW_DEP_TNAME, base_table_id))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
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
          uint64_t mview_id = 0;
          int64_t cnt = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "mview_id", mview_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(mview_ids.push_back(mview_id))) {
              LOG_WARN("failed to add ref table id to array", KR(ret), K(mview_id));
            } else if (cnt > 0) {
              exists_nested_mv = true;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObMVDepUtils::check_database_referenced_by_mv_from_other_database(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t database_id,
    bool &exists)
{
  int ret = OB_SUCCESS;
  exists = false;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or database_id", KR(ret), K(tenant_id), K(database_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      ObMySQLResult *result = nullptr;
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      const uint64_t extract_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
      if (OB_FAIL(sql.assign_fmt(
              "SELECT t.table_id AS table_id, mv.table_id AS mview_id, mv.database_id AS mview_database_id "
              "FROM %s.%s dep, %s.%s t, %s.%s mv "
              "WHERE t.tenant_id = %lu AND t.database_id = %lu "
              "AND dep.tenant_id = %lu AND dep.mview_id = mv.table_id AND dep.p_obj = t.table_id "
              "AND mv.tenant_id = %lu AND mv.database_id != %lu LIMIT 1",
              OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
              OB_SYS_DATABASE_NAME, OB_ALL_TABLE_TNAME,
              OB_SYS_DATABASE_NAME, OB_ALL_TABLE_TNAME,
              extract_tenant_id, database_id, extract_tenant_id, extract_tenant_id, database_id))) {
        LOG_WARN("failed to assign sql", KR(ret));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next", KR(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        uint64_t table_id = OB_INVALID_ID;
        uint64_t mview_id = OB_INVALID_ID;
        uint64_t mview_database_id = OB_INVALID_ID;
        EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "mview_id", mview_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "mview_database_id", mview_database_id, uint64_t);
        if (OB_SUCC(ret)) {
          exists = true;
          LOG_INFO("database has table referenced by materialized view from other database",
                   K(tenant_id), K(database_id), K(table_id), K(mview_id), K(mview_database_id));
        }
      }
    }
  }
  return ret;
}

int ObMVDepUtils::fetch_mview_ids_by_sql(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObSqlString &sql,
    ObIArray<uint64_t> &mview_ids)
{
  int ret = OB_SUCCESS;
  mview_ids.reuse();
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
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
          uint64_t mview_id = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "mview_id", mview_id, uint64_t);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(mview_ids.push_back(mview_id))) {
              LOG_WARN("failed to add ref table id to array", KR(ret), K(mview_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMVDepUtils::build_mview_dep_recursive_cte(const uint64_t target_mview_id,
                                                const char *cte_name,
                                                ObSqlString &cte_sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cte_sql.assign_fmt(
            "WITH RECURSIVE %s (level, parent_mview_id, mview_id) AS ("
            "  SELECT 0, CAST(NULL AS SIGNED), mview_id from %s.%s where tenant_id = 0 and mview_id = %lu "
            "  UNION ALL "
            "  SELECT /*+leading(n_deps dep mv) use_nl(dep mv) */ n_deps.level + 1, n_deps.mview_id, mv.mview_id FROM %s n_deps"
            "  INNER JOIN %s.%s dep ON n_deps.mview_id = dep.mview_id "
            "  INNER JOIN %s.%s mv ON dep.p_obj = mv.mview_id "
            "  WHERE dep.tenant_id = 0 and mv.tenant_id = 0) ",
            cte_name, OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME, target_mview_id, cte_name,
            OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
            OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME))) {
    LOG_WARN("fail to assign sql", KR(ret));
  }
  return ret;
}

int ObMVDepUtils::get_mview_ids_in_topo_refresh_order(ObISQLClient &sql_client,
                                                      const uint64_t tenant_id,
                                                      const uint64_t target_mview_id,
                                                      ObIArray<uint64_t> &topo_ordered_mview_ids)
{
  int ret = OB_SUCCESS;
  topo_ordered_mview_ids.reuse();
  ObSqlString sql;
  const char cte_name[] = "nested_mv_deps";
  if (OB_FAIL(build_mview_dep_recursive_cte(target_mview_id, cte_name, sql))) {
    LOG_WARN("fail to build mview dep recursive cte", KR(ret));
  } else if (OB_FAIL(sql.append_fmt("SELECT mview_id FROM %s group by mview_id ORDER BY max(level) desc, mview_id", cte_name))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else if (OB_FAIL(fetch_mview_ids_by_sql(sql_client, tenant_id, sql, topo_ordered_mview_ids))) {
    LOG_WARN("fail to fetch mview ids by sql", KR(ret), K(sql));
  }
  return ret;
}

int ObMVDepUtils::get_mds_locked_mview_ids(ObISQLClient &sql_client,
                                           const uint64_t tenant_id,
                                           const uint64_t target_mview_id,
                                           const int64_t unrefreshed_mv_count,
                                           ObIArray<uint64_t> &mds_locked_mview_ids)
{
  int ret = OB_SUCCESS;
  mds_locked_mview_ids.reuse();
  ObSqlString sql;
  const char cte_name[] = "nested_mv_deps";
  if (OB_UNLIKELY(unrefreshed_mv_count < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unrefreshed mv count is less than 0", KR(ret), K(unrefreshed_mv_count));
  } else if (OB_FAIL(build_mview_dep_recursive_cte(target_mview_id, cte_name, sql))) {
    LOG_WARN("fail to build mview dep recursive cte", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(
              "SELECT mview_id FROM %s where parent_mview_id in (SELECT mview_id FROM %s group by mview_id ORDER BY max(level), mview_id desc limit %ld) "
              "UNION (SELECT mview_id FROM %s group by mview_id ORDER BY max(level), mview_id desc limit %ld) ",
          cte_name, cte_name, unrefreshed_mv_count, cte_name, unrefreshed_mv_count))) {
    LOG_WARN("fail to append sql", KR(ret));
  } else if (OB_FAIL(fetch_mview_ids_by_sql(sql_client, tenant_id, sql, mds_locked_mview_ids))) {
    LOG_WARN("fail to fetch mview ids by sql", KR(ret));
  }
  return ret;
}

int ObMVDepUtils::update_mview_dep_base_table(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t old_base_table_id,
    const uint64_t new_base_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == old_base_table_id
      || OB_INVALID_ID == new_base_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        KR(ret), K(tenant_id), K(old_base_table_id), K(new_base_table_id));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET p_obj = %lu, flags = flags | %ld WHERE tenant_id = %ld AND p_obj = %lu",
                               OB_ALL_MVIEW_DEP_TNAME,
                               new_base_table_id,
                               ObMVDepInfo::IS_COMPLETE_REFRESH_ONLY_FLAG,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               old_base_table_id))) {
      LOG_WARN("failed to assign update sql for mview dep", KR(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute update mview dep", KR(ret), K(sql));
    } else {
      LOG_INFO("updated mview dep base table", K(tenant_id), K(old_base_table_id),
               K(new_base_table_id), K(affected_rows));
    }
  }
  return ret;
}

int ObMVDepUtils::check_dep_mview_refresh_interval_order(const ObSelectStmt &stmt,
                                                         const uint64_t tenant_id,
                                                         const int64_t refresh_interval)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> dep_mview_ids;
  if (refresh_interval <= 0) {
  } else if (OB_FAIL(ObMVChecker::collect_dep_mview_ids_from_stmt(stmt, dep_mview_ids))) {
    LOG_WARN("failed to collect dependency mview ids from stmt", KR(ret));
  } else if (dep_mview_ids.empty()) {
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(check_mview_refresh_interval_order(
                 *GCTX.sql_proxy_, tenant_id, dep_mview_ids, refresh_interval))) {
    LOG_WARN("failed to check mview refresh interval order", KR(ret), K(tenant_id),
             K(refresh_interval), K(dep_mview_ids));
  }
  return ret;
}

int ObMVDepUtils::check_mview_refresh_interval_order(ObISQLClient &sql_client,
                                                     const uint64_t tenant_id,
                                                     const ObIArray<uint64_t> &mview_ids,
                                                     const int64_t refresh_interval)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  ObSqlString sql;
  common::sqlclient::ObMySQLResult *result = nullptr;
  int64_t invalid_mview_count = 0;
  if (refresh_interval <= 0 || mview_ids.empty()) {
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(mview_ids.count()));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else if (!((MOCK_DATA_VERSION_4_4_2_4 <= compat_version && DATA_VERSION_4_5_0_0 > compat_version)
               || DATA_VERSION_5_0_2_0 <= compat_version)) {
  } else if (OB_FAIL(sql.assign_fmt(
                 "WITH RECURSIVE up_dep (mview_id, refresh_interval) AS ("
                 "  SELECT mv.mview_id, mv.refresh_interval FROM %s.%s mv "
                 "  WHERE mv.tenant_id = 0 AND mv.mview_id IN (",
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mview_ids.count(); ++i) {
      if (OB_FAIL(sql.append_fmt("%s%lu", 0 == i ? "" : ", ", mview_ids.at(i)))) {
        LOG_WARN("failed to append mview id", KR(ret), K(i), K(mview_ids.at(i)));
      }
    }
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql.append_fmt(
                     ") UNION ALL "
                     "  SELECT /*+leading(u dep mv) use_nl(dep mv) */ "
                     "         mv.mview_id, mv.refresh_interval "
                     "    FROM up_dep u, %s.%s dep, %s.%s mv "
                     "   WHERE u.refresh_interval <= 0 "
                     "     AND dep.tenant_id = 0 AND mv.tenant_id = 0 "
                     "     AND dep.mview_id = u.mview_id AND dep.p_obj = mv.mview_id"
                     ") SELECT COUNT(*) AS invalid_mview_count "
                     "FROM up_dep WHERE refresh_interval > %ld",
                     OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                     OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME,
                     refresh_interval))) {
        LOG_WARN("failed to append sql", KR(ret));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get invalid mview count", KR(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "invalid_mview_count", invalid_mview_count, int64_t);
        if (OB_SUCC(ret) && invalid_mview_count > 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "a materialized view refresh interval shorter than its referenced "
                         "materialized view refresh interval");
          LOG_WARN("downstream mview refreshes faster than its upstream mview", KR(ret),
                   K(tenant_id), K(refresh_interval), K(mview_ids), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObMVDepUtils::check_mview_refresh_interval_order(ObISQLClient &sql_client,
                                                     const uint64_t tenant_id,
                                                     const uint64_t mview_id,
                                                     const int64_t refresh_interval)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  ObSqlString sql;
  common::sqlclient::ObMySQLResult *result = nullptr;
  int64_t invalid_mview_count = 0;
  if (refresh_interval <= 0) {
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else if (!((MOCK_DATA_VERSION_4_4_2_4 <= compat_version && DATA_VERSION_4_5_0_0 > compat_version)
               || DATA_VERSION_5_0_2_0 <= compat_version)) {
  } else if (OB_FAIL(sql.assign_fmt(
                 "WITH RECURSIVE up_dep (mview_id, refresh_interval) AS ("
                 "  SELECT mv.mview_id, mv.refresh_interval "
                 "    FROM %s.%s dep, %s.%s mv "
                 "   WHERE dep.tenant_id = 0 AND mv.tenant_id = 0 "
                 "     AND dep.mview_id = %lu AND dep.p_obj = mv.mview_id "
                 "  UNION ALL "
                 "  SELECT /*+leading(u dep mv) use_nl(dep mv) */ "
                 "         mv.mview_id, mv.refresh_interval "
                 "    FROM up_dep u, %s.%s dep, %s.%s mv "
                 "   WHERE u.refresh_interval <= 0 "
                 "     AND dep.tenant_id = 0 AND mv.tenant_id = 0 "
                 "     AND dep.mview_id = u.mview_id AND dep.p_obj = mv.mview_id"
                 "), down_dep (mview_id, refresh_interval) AS ("
                 "  SELECT /*+ leading(dep mv) use_nl(mv) */ "
                 "         mv.mview_id, mv.refresh_interval "
                 "    FROM %s.%s dep, %s.%s mv "
                 "   WHERE dep.tenant_id = 0 AND mv.tenant_id = 0 "
                 "     AND dep.p_obj = %lu AND dep.mview_id = mv.mview_id "
                 "  UNION ALL "
                 "  SELECT /*+leading(d dep mv) use_hash(dep) */ "
                 "         mv.mview_id, mv.refresh_interval "
                 "    FROM down_dep d, %s.%s dep, %s.%s mv "
                 "   WHERE d.refresh_interval <= 0 "
                 "     AND dep.tenant_id = 0 AND mv.tenant_id = 0 "
                 "     AND dep.p_obj = d.mview_id AND dep.mview_id = mv.mview_id"
                 ") SELECT COUNT(*) AS invalid_mview_count FROM ("
                 "  SELECT 1 FROM up_dep WHERE refresh_interval > %ld "
                 "  UNION ALL "
                 "  SELECT 1 FROM down_dep "
                 "   WHERE refresh_interval > 0 AND refresh_interval < %ld"
                 ") t",
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME,
                 mview_id,
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME,
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME,
                 mview_id,
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME,
                 refresh_interval, refresh_interval))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(mview_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get invalid mview count", KR(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "invalid_mview_count", invalid_mview_count, int64_t);
        if (OB_SUCC(ret) && invalid_mview_count > 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "a materialized view refresh interval shorter than its referenced "
                         "materialized view refresh interval");
          LOG_WARN("downstream mview refreshes faster than its upstream mview", KR(ret),
                   K(tenant_id), K(mview_id), K(refresh_interval), K(sql));
        }
      }
    }
  }
  return ret;
}

} // end of sql
} // end of oceanbase
