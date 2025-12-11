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
#include "rootserver/mview/ob_mview_dependency_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_mview_info.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace sql;
namespace rootserver
{
ObMViewDependencyService::ObMViewDependencyService(ObMultiVersionSchemaService &schema_service)
  : schema_service_(schema_service)
{

}

ObMViewDependencyService::~ObMViewDependencyService()
{

}

int ObMViewDependencyService::remove_mview_dep_infos(
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const uint64_t mview_table_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> stale_ref_table_ids;
  ObArray<uint64_t> stale_fast_lsm_ref_table_ids;
  // during upgrading, dropping mview should still work,
  // hence, do nothing if __all_mview_dep does not exists
  bool all_mview_dep_table_exists = false;
  if (OB_FAIL(share::schema::ObSchemaUtils::check_sys_table_exist_by_sql(
      trans, tenant_id, OB_ALL_MVIEW_DEP_TID, all_mview_dep_table_exists))) {
    LOG_WARN("failed to check whether __all_mview_dep table exists", KR(ret));
  } else if (all_mview_dep_table_exists) {
    if (OB_FAIL(ObMVDepUtils::get_table_ids_only_referenced_by_given_mv(
        trans, tenant_id, mview_table_id, stale_ref_table_ids))) {
      LOG_WARN("failed to get table ids only referenced by given mv", KR(ret));
    } else if (!stale_ref_table_ids.empty()) {
      ObUpdateMViewRefTableOpt opt;
      opt.set_table_flag(ObTableReferencedByMVFlag::IS_NOT_REFERENCED_BY_MV);
      opt.set_mv_flag(ObTableReferencedByFastLSMMVFlag::IS_NOT_REFERENCED_BY_FAST_LSM_MV);
      if (OB_FAIL(update_mview_reference_table_status(trans,
                                                      schema_guard,
                                                      tenant_id,
                                                      stale_ref_table_ids,
                                                      opt))) {
        LOG_WARN("failed to update mview reference table status", KR(ret), K(tenant_id),
                 K(stale_ref_table_ids), K(opt));
      }
    } else if (OB_FAIL(ObMVDepUtils::get_table_ids_only_referenced_by_given_fast_lsm_mv(
                   trans, tenant_id, mview_table_id, stale_fast_lsm_ref_table_ids))) {
      LOG_WARN("failed to get table ids only referenced by given fast lsm mv", KR(ret));
    } else if (!stale_fast_lsm_ref_table_ids.empty()) {
      ObUpdateMViewRefTableOpt opt;
      opt.set_mv_flag(ObTableReferencedByFastLSMMVFlag::IS_NOT_REFERENCED_BY_FAST_LSM_MV);
      if (OB_FAIL(update_mview_reference_table_status(trans,
                                                      schema_guard,
                                                      tenant_id,
                                                      stale_fast_lsm_ref_table_ids,
                                                      opt))) {
        LOG_WARN("failed to update mview reference table status", KR(ret), K(tenant_id),
                 K(stale_fast_lsm_ref_table_ids), K(opt));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql::ObMVDepUtils::delete_mview_dep_infos(
        trans, tenant_id, mview_table_id))) {
      LOG_WARN("failed to delete mview dep infos", KR(ret), K(mview_table_id));
    }
  }
  return ret;
}

int ObMViewDependencyService::update_mview_dep_infos(
    ObMySQLTransaction &trans,
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const uint64_t mview_table_id,
    const common::ObIArray<ObDependencyInfo> &dep_infos)
{
  int ret = OB_SUCCESS;
  ObArray<ObMVDepInfo> cur_mv_dep_infos;
  ObArray<ObMVDepInfo> prev_mv_dep_infos;
  ObArray<uint64_t> new_ref_table_ids;// table_referenced_by_mv_flag will be set
  ObArray<uint64_t> stale_ref_table_ids; // table_referenced_by_mv_flag will be cleared
  ObArray<uint64_t> stale_fast_lsm_ref_table_ids;
  ObArray<uint64_t> table_ids_only_ref_by_this_mv;
  ObArray<uint64_t> table_ids_only_ref_by_this_fast_lsm_mv;
  ObMViewInfo mview_info;
  // during upgrading, creating mview should still work,
  // hence, do nothing if __all_mview_dep does not exists
  bool all_mview_dep_table_exists = false;

  if ((OB_INVALID_TENANT_ID == tenant_id) || (OB_INVALID_ID == mview_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or mview_table_id", KR(ret), K(tenant_id), K(mview_table_id));
  } else if (OB_FAIL(share::schema::ObSchemaUtils::check_sys_table_exist_by_sql(
      trans, tenant_id, OB_ALL_MVIEW_DEP_TID, all_mview_dep_table_exists))) {
    LOG_WARN("failed to check is system table name", KR(ret));
  } else if (all_mview_dep_table_exists) {
    if (OB_FAIL(ObMViewInfo::fetch_mview_info(trans, tenant_id, mview_table_id, mview_info,
                                              false /*for_update*/, true /*nowait*/))) {
      LOG_WARN("fail to fetch mview info", KR(ret), K(mview_table_id));
    } else if (OB_FAIL(sql::ObMVDepUtils::convert_to_mview_dep_infos(dep_infos, cur_mv_dep_infos))) {
      LOG_WARN("failed to convert to mview dep infos", KR(ret));
    } else if (OB_FAIL(sql::ObMVDepUtils::get_mview_dep_infos(
        trans, tenant_id, mview_table_id, prev_mv_dep_infos))) {
      LOG_WARN("failed to get mview dep infos", KR(ret));
    } else if (OB_FAIL(ObMVDepUtils::get_table_ids_only_referenced_by_given_mv(
        trans, tenant_id, mview_table_id, table_ids_only_ref_by_this_mv))) {
      LOG_WARN("failed to get table ids only referenced by given mv", KR(ret));
    } else if (OB_FAIL(ObMVDepUtils::get_table_ids_only_referenced_by_given_fast_lsm_mv(
        trans, tenant_id, mview_table_id, table_ids_only_ref_by_this_fast_lsm_mv))) {
      LOG_WARN("failed to get table ids only referenced by given fast lsm mv", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < cur_mv_dep_infos.count()); ++i) {
        const ObMVDepInfo &cur_mv_dep = cur_mv_dep_infos.at(i);
        if (OB_FAIL(new_ref_table_ids.push_back(cur_mv_dep.p_obj_))) {
          LOG_WARN("failed to add cur ref table id to array", KR(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (prev_mv_dep_infos.empty()) { // creating a new mview
    } else if (OB_UNLIKELY(prev_mv_dep_infos.count() != cur_mv_dep_infos.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv dep infos count not match",
          KR(ret), K(prev_mv_dep_infos), K(cur_mv_dep_infos));
    } else { // updating an existing mview
      for (int64_t i = 0; OB_SUCC(ret) && (i < cur_mv_dep_infos.count()); ++i) {
        const ObMVDepInfo &prev_mv_dep = prev_mv_dep_infos.at(i);
        const ObMVDepInfo &cur_mv_dep = cur_mv_dep_infos.at(i);
        if (OB_UNLIKELY(prev_mv_dep.p_order_ != cur_mv_dep.p_order_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("p_order is not match",
              KR(ret), K(prev_mv_dep.p_order_), K(cur_mv_dep.p_order_));
        } else if (prev_mv_dep.p_obj_ != cur_mv_dep.p_obj_) {
          const uint64_t old_ref_table_id = prev_mv_dep.p_obj_;
          // if an old_ref_table_id exists in the cur_mv_dep_infos,
          // then its table_referenced_by_mv_flag does not need to be cleared
          if (has_exist_in_array(new_ref_table_ids, old_ref_table_id)) {
          } else if (has_exist_in_array(table_ids_only_ref_by_this_mv, old_ref_table_id)) {
            // only when an old_ref_table_id exists in the list of table_ids_only_ref_by_this_mv,
            // its table_referenced_by_mv_flag needs to be cleared
            if (OB_FAIL(stale_ref_table_ids.push_back(old_ref_table_id))) {
              LOG_WARN("failed to add old ref table id to array", KR(ret), K(old_ref_table_id));
            }
          } else if (has_exist_in_array(table_ids_only_ref_by_this_fast_lsm_mv, old_ref_table_id)) {
            if (OB_FAIL(stale_fast_lsm_ref_table_ids.push_back(old_ref_table_id))) {
              LOG_WARN("failed to add old ref table id to array", KR(ret), K(old_ref_table_id));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && !stale_ref_table_ids.empty()) {
      ObUpdateMViewRefTableOpt opt;
      opt.set_table_flag(ObTableReferencedByMVFlag::IS_NOT_REFERENCED_BY_MV);
      opt.set_mv_flag(ObTableReferencedByFastLSMMVFlag::IS_NOT_REFERENCED_BY_FAST_LSM_MV);
      if (OB_FAIL(update_mview_reference_table_status(trans,
                                                      schema_guard,
                                                      tenant_id,
                                                      stale_ref_table_ids,
                                                      opt))) {
        LOG_WARN("failed to update mview reference table status", KR(ret), K(tenant_id),
                 K(stale_ref_table_ids), K(opt));
      }
    }

    if (OB_SUCC(ret) && !stale_fast_lsm_ref_table_ids.empty()) {
      ObUpdateMViewRefTableOpt opt;
      opt.set_mv_flag(ObTableReferencedByFastLSMMVFlag::IS_NOT_REFERENCED_BY_FAST_LSM_MV);
      if (OB_FAIL(update_mview_reference_table_status(trans,
                                                      schema_guard,
                                                      tenant_id,
                                                      stale_fast_lsm_ref_table_ids,
                                                      opt))) {
        LOG_WARN("failed to update mview reference table status", KR(ret), K(tenant_id),
                 K(stale_fast_lsm_ref_table_ids), K(opt));
      }
    }

    if (OB_SUCC(ret) && !new_ref_table_ids.empty()) {
      ObUpdateMViewRefTableOpt opt;
      opt.set_table_flag(ObTableReferencedByMVFlag::IS_REFERENCED_BY_MV);
      if (mview_info.is_fast_lsm_mv()) {
        opt.set_mv_flag(ObTableReferencedByFastLSMMVFlag::IS_REFERENCED_BY_FAST_LSM_MV);
      }
      if (OB_FAIL(sql::ObMVDepUtils::delete_mview_dep_infos(
          trans, tenant_id, mview_table_id))) {
        LOG_WARN("failed to delete mview dep infos", KR(ret), K(mview_table_id));
      } else if (OB_FAIL(sql::ObMVDepUtils::insert_mview_dep_infos(
          trans, tenant_id, mview_table_id, cur_mv_dep_infos))) {
        LOG_WARN("failed to insert mview dep infos", KR(ret), K(new_ref_table_ids));
      } else if (OB_FAIL(update_mview_reference_table_status(trans,
                                                             schema_guard,
                                                             tenant_id,
                                                             new_ref_table_ids,
                                                             opt))) {
        LOG_WARN("failed to update mview reference table status", KR(ret), K(opt));
      }
    }
  }

  return ret;
}

int ObMViewDependencyService::update_mview_reference_table_status(
    ObMySQLTransaction &trans,
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const ObIArray<uint64_t> &ref_table_ids,
    const ObUpdateMViewRefTableOpt &update_opt)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("failed to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3.1.0 does not support this operation", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
        "tenant's data version is below 4.3.1.0, update mview reference table status is ");
  } else {
    ObSchemaService *schema_service = schema_service_.get_schema_service();
    for (int64_t i = 0; OB_SUCC(ret) && (i < ref_table_ids.count()); ++i) {
      int64_t new_schema_version = OB_INVALID_VERSION;
      const uint64_t ref_table_id = ref_table_ids.at(i);
      const ObTableSchema *ref_table_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id, ref_table_id, ref_table_schema))) {
        LOG_WARN("failed to get table schema", KR(ret), K(tenant_id), K(ref_table_id));
      } else if (OB_ISNULL(ref_table_schema)) {
        // the reference table has already been dropped, ignore it
        LOG_TRACE("ref table schema is null", KR(ret), K(tenant_id), K(ref_table_id));
      } else if (OB_FAIL(check_table_exist_(trans, tenant_id, ref_table_id))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("ref table has already been dropped, ignore it", KR(ret), K(tenant_id), K(ref_table_id));
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id));
      } else if ((update_opt.need_update_table_flag_ && ref_table_schema->get_table_mode_struct().table_referenced_by_mv_flag_ != update_opt.table_flag_) ||
                 (update_opt.need_update_mv_flag_ && ref_table_schema->get_mv_mode_struct().table_referenced_by_fast_lsm_mv_flag_ != update_opt.mv_flag_)) {
        SMART_VAR(ObTableSchema, new_ref_table_schema) {
          if (OB_FAIL(new_ref_table_schema.assign(*ref_table_schema))) {
            LOG_WARN("fail to assign ref table schema", KR(ret));
          } else {
            new_ref_table_schema.set_table_id(ref_table_id);
            new_ref_table_schema.set_schema_version(new_schema_version);
            if (update_opt.need_update_table_flag_) {
              new_ref_table_schema.set_table_referenced_by_mv(update_opt.table_flag_);
            }
            if (update_opt.need_update_mv_flag_) {
              new_ref_table_schema.set_table_referenced_by_fast_lsm_mv(update_opt.mv_flag_);
            }
            if (OB_FAIL(schema_service->get_table_sql_service().update_mview_reference_table_status(
                new_ref_table_schema, trans))) {
              LOG_WARN("failed to update mview reference table status", KR(ret), K(ref_table_id),
                       K(update_opt));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewDependencyService::check_table_exist_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt("SELECT 1 FROM %s.%s WHERE table_id = %ld",
                              OB_SYS_DATABASE_NAME, OB_ALL_TABLE_TNAME, table_id))) {
    LOG_WARN("failed to append sql", KR(ret), K(table_id), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next", KR(ret));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("base table not exist", KR(ret), K(tenant_id), K(table_id), K(sql));
        }
      }
    }
  }
  return ret;
}

} // end of sql
} // end of oceanbase
