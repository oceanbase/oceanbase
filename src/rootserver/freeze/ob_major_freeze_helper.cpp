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

#define USING_LOG_PREFIX RS_COMPACTION

#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/location_cache/ob_location_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_tenant_info_proxy.h"
#include "src/observer/ob_srv_network_frame.h"

namespace oceanbase
{
using namespace share;
namespace rootserver
{

int ObMajorFreezeParam::add_freeze_info(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  obrpc::ObSimpleFreezeInfo info(tenant_id);
  if (OB_FAIL(freeze_info_array_.push_back(info))) {
    LOG_WARN("fail to push_back", K(info));
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////

int ObMajorFreezeHelper::major_freeze(const ObMajorFreezeParam &param)
{
  ObArray<int> merge_results;
  return major_freeze(param, merge_results);
}

int ObMajorFreezeHelper::major_freeze(
    const ObMajorFreezeParam &param,
    ObIArray<int> &merge_results)
{
  int ret = OB_SUCCESS;
  ObSEArray<obrpc::ObSimpleFreezeInfo, 32> freeze_info_array;
  bool want_to_freeze_all = (param.freeze_all_ || param.freeze_all_user_ || param.freeze_all_meta_);
  if (OB_UNLIKELY(!param.is_valid()
                  || (!want_to_freeze_all && param.freeze_info_array_.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param), KR(ret));
  } else if (OB_FAIL(get_freeze_info(param, freeze_info_array))) {
    LOG_WARN("fail to get tenant id", KR(ret), K(param));
  } else if (!freeze_info_array.empty()) { // may be empty due to skipping restore and standby tenants
    if (OB_FAIL(do_major_freeze(*param.transport_, freeze_info_array, merge_results))) {
      LOG_WARN("fail to do major freeze", KR(ret), K(freeze_info_array));
    }
  }
  return ret;
}

int ObMajorFreezeHelper::tablet_major_freeze(const ObTabletMajorFreezeParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_UNLIKELY(nullptr == GCTX.location_service_ || nullptr == GCTX.net_frame_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid GCTX", KR(ret));
  } else {
    LOG_INFO("tablet major freeze", K(ret), K(param));
    const int64_t start_time = ObTimeUtility::fast_current_time();
    obrpc::ObTabletMajorFreezeRpcProxy proxy;
    ObAddr leader;
    uint64_t tenant_id = param.tenant_id_;
    obrpc::ObTabletMajorFreezeRequest req;
    req.tenant_id_ = tenant_id;
    req.tablet_id_ = param.tablet_id_;
    req.is_rebuild_column_group_ = param.is_rebuild_column_group_;
    obrpc::ObMajorFreezeResponse resp;
    bool is_cache_hit = false;

    if (OB_FAIL(GCTX.location_service_->get(
      tenant_id, param.tablet_id_, MAX_PROCESS_TIME_US, is_cache_hit, req.ls_id_))) {
      LOG_WARN("failed to get ls_id", K(ret), K(tenant_id), K(param.tablet_id_));
    } else if (OB_FAIL(proxy.init(GCTX.net_frame_->get_req_transport()))) {
      LOG_WARN("failed to init proxy", K(ret));
    } else {
      const int64_t MAX_RETRY_COUNT = 5;
      bool major_freeze_done = false;

      for (int64_t i = 0; OB_SUCC(ret) && (!major_freeze_done) && (i < MAX_RETRY_COUNT); ++i) {
        if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(GCONF.cluster_id,
                    tenant_id, req.ls_id_, leader))) {
          LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id));
        } else if (OB_FAIL(proxy.to(leader)
                                .trace_time(true)
                                .max_process_handler_time(MAX_PROCESS_TIME_US)
                                .by(tenant_id)
                                .dst_cluster_id(GCONF.cluster_id)
                                .tablet_major_freeze(req, resp))) {
          if (OB_LEADER_NOT_EXIST == ret || OB_EAGAIN == ret) {
            const int64_t idle_time = 200 * 1000 * (i + 1);
            LOG_WARN("leader may switch or ddl confilict, will retry", KR(ret), K(tenant_id), K(param),
              "ori_leader", leader, K(idle_time));
            USLEEP(idle_time);
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to send command", K(ret), K(req));
          }
        } else {
          major_freeze_done = true;
        }
      } // end of for

      if (OB_SUCC(ret) && !major_freeze_done) {
        ret = OB_LEADER_NOT_EXIST;
        LOG_WARN("fail to retry major freeze cuz switching role", KR(ret), K(MAX_RETRY_COUNT));
      }
    }
    const int64_t cost_time = ObTimeUtility::current_time() - start_time;
    LOG_INFO("do tenant major freeze", KR(ret), K(tenant_id), K(leader), K(param), K(cost_time));
  }
  return ret;
}

int ObMajorFreezeHelper::get_freeze_info(
    const ObMajorFreezeParam &param,
    ObIArray<obrpc::ObSimpleFreezeInfo> &freeze_info_array)
{
  int ret = OB_SUCCESS;

  ObArray<obrpc::ObSimpleFreezeInfo> tmp_info_array;
  bool want_to_freeze_all = param.freeze_all_ || param.freeze_all_user_ || param.freeze_all_meta_;
  if (want_to_freeze_all) {
    if (OB_FAIL(get_specific_tenant_freeze_info(param.freeze_all_, param.freeze_all_user_,
                                                param.freeze_all_meta_, tmp_info_array))) {
      LOG_WARN("fail to get specific tenant freeze info", KR(ret));
    }
  } else {
    if (OB_FAIL(tmp_info_array.assign(param.freeze_info_array_))) {
      LOG_WARN("fail to assign", K(param), KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (tmp_info_array.empty() && !want_to_freeze_all) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("freeze info array should not be empty", KR(ret), K(param));
  } else {
    const int64_t info_cnt = tmp_info_array.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < info_cnt); ++i) {
      share::ObAllTenantInfo tenant_info;
      bool is_restore = false;
      const uint64_t tenant_id = tmp_info_array.at(i).tenant_id_;
      if (OB_FAIL(check_tenant_is_restore(tenant_id, is_restore))) {
        LOG_WARN("fail to check tenant is restore", KR(ret), K(i), "freeze_info", tmp_info_array.at(i));
      } else if (is_restore) {
        LOG_INFO("skip restoring tenant to do major freeze", K(tenant_id));
        const char *warn_buf = "tenant is in restore, major freeze is not allowed now";
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(add_user_warning(tenant_id, warn_buf))) {
          LOG_WARN("fail to add user warning", KR(tmp_ret), K(tenant_id));
        }
      } else if (OB_FAIL(share::ObAllTenantInfoProxy::load_tenant_info(tenant_id, GCTX.sql_proxy_,
                                                                false, tenant_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS; // ignore ret, so as to process the next tenant
          LOG_WARN("tenant may be deleted, skip major freeze for this tenant", K(tenant_id));
        } else {
          LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
        }
      }
      // Skip major freeze for standby tenants and thus avoid OB_MAJOR_FREEZE_NOT_ALLOW incurred by
      // standby tenants, only when launching major freeze on more than one tenant or all_user or all.
      else if (tenant_info.is_standby() && ((info_cnt > 1) || param.freeze_all_user_ || param.freeze_all_)) {
        LOG_INFO("skip major freeze for standby tenant", K(tenant_info));
        const char *warn_buf = "standby tenant sync freeze info from primary tenant, not allowed to launch major freeze";
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(add_user_warning(tenant_id, warn_buf))) {
          LOG_WARN("fail to add user warning", KR(tmp_ret), K(tenant_id));
        }
      } else if (OB_FAIL(freeze_info_array.push_back(tmp_info_array.at(i)))) {
        LOG_WARN("fail to push back freeze info", KR(ret), K(i), "freeze_info", tmp_info_array.at(i));
      }
    }
  }

  return ret;
}

int ObMajorFreezeHelper::check_tenant_is_restore(
    const uint64_t tenant_id,
    bool &is_restore)
{
  int ret = OB_SUCCESS;
  is_restore = false;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().check_tenant_is_restore(NULL, tenant_id, is_restore))) {
    LOG_WARN("fail to check tenant restore", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObMajorFreezeHelper::get_all_tenant_freeze_info(
    ObIArray<obrpc::ObSimpleFreezeInfo> &freeze_info_array)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 32> tenant_ids;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid GCTX", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else {
    for (int64_t i = 0; (i < tenant_ids.count()) && OB_SUCC(ret); ++i) {
      // only launch major freeze for tenant whose status is normal, and skip major freeze for
      // tenant whose status is not normal.
      const ObSimpleTenantSchema *tenant_schema = nullptr;
      if (OB_FAIL(schema_guard.get_tenant_info(tenant_ids[i], tenant_schema))) {
        LOG_WARN("fail to get simple tenant schema", KR(ret), "tenant_id", tenant_ids[i]);
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("tenant schema is null", KR(ret), "tenant_id", tenant_ids[i]);
      } else if (OB_UNLIKELY(!tenant_schema->is_normal())) {
        LOG_WARN("tenant status is not normal, skip major freeze for this tenant", "tenant_id",
                 tenant_ids[i], "status", tenant_schema->get_status());
      } else { // tenant_schema->is_normal()
        obrpc::ObSimpleFreezeInfo info(tenant_ids[i]);
        if(OB_FAIL(freeze_info_array.push_back(info))) {
          LOG_WARN("fail to push back", KR(ret), "tenant_id", tenant_ids[i]);
        }
      }
    }
  }
  return ret;
}

int ObMajorFreezeHelper::get_specific_tenant_freeze_info(
    bool freeze_all,
    bool freeze_all_user,
    bool freeze_all_meta,
    common::ObIArray<obrpc::ObSimpleFreezeInfo> &freeze_info_array)
{
  int ret = OB_SUCCESS;
  // if min_cluster_version < 4.2.1.0，disable all_user/all_meta,
  // and make tenant=all effective for all tenants.
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) {
    if (freeze_all_user || freeze_all_meta) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("all_user/all_meta are not supported when min_cluster_version is less than 4.2.1.0",
               KR(ret), K(freeze_all_user), K(freeze_all_meta));
    } else if (freeze_all) {
      freeze_info_array.reset();
      if (OB_FAIL(get_all_tenant_freeze_info(freeze_info_array))) {
        LOG_WARN("fail to get all tenant freeze info", KR(ret));
      }
    }
  } else {
    ObSEArray<obrpc::ObSimpleFreezeInfo, 32> tmp_freeze_info_array;
    if (OB_FAIL(get_all_tenant_freeze_info(tmp_freeze_info_array))) {
      LOG_WARN("fail to get all tenant freeze info", KR(ret));
    } else {
      using FUNC_TYPE = bool (*) (const uint64_t);
      FUNC_TYPE func = nullptr;
      // caller guarantees that at most one of freeze_all/freeze_all_user/freeze_all_meta is true.
      if (freeze_all || freeze_all_user) {
        func = is_user_tenant;
      } else {
        func = is_meta_tenant;
      }
      freeze_info_array.reset();
      for (int64_t i = 0; OB_SUCC(ret) && (i < tmp_freeze_info_array.count()); ++i) {
        if (func(tmp_freeze_info_array.at(i).tenant_id_)) {
          if (OB_FAIL(freeze_info_array.push_back(tmp_freeze_info_array.at(i)))) {
            LOG_WARN("fail to push back freeze info",
                     KR(ret), K(i), "freeze_info", tmp_freeze_info_array.at(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObMajorFreezeHelper::do_major_freeze(
    const rpc::frame::ObReqTransport &transport,
    const ObIArray<obrpc::ObSimpleFreezeInfo> &freeze_info_array,
    ObIArray<int> &merge_results)
{
  int ret = OB_SUCCESS;
  int final_ret = OB_SUCCESS;
  if (freeze_info_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    const int64_t tenant_count = freeze_info_array.count();
    for (int i = 0; (i < tenant_count) && OB_SUCC(ret); ++i) {
      const uint64_t tenant_id = freeze_info_array.at(i).tenant_id_;
      if (OB_FAIL(do_one_tenant_major_freeze(transport, freeze_info_array.at(i)))) {
        if ((OB_MAJOR_FREEZE_NOT_FINISHED != ret) && (OB_FROZEN_INFO_ALREADY_EXIST != ret)) {
          final_ret = ret;
          LOG_WARN("fail do tenant major freeze", KR(ret), K(tenant_count), "freeze_info", freeze_info_array.at(i));
        }
      }

      // push ret of 'do_one_tenant_major_freeze' into array. if fail, finish loop.
      if (OB_SUCCESS != (ret = merge_results.push_back(ret))) {
        final_ret = ret;
        LOG_WARN("fail to push back", KR(ret), K(tenant_id), K(tenant_count));
      }
    }

    ret = final_ret;
  }

  return ret;
}

int ObMajorFreezeHelper::do_one_tenant_major_freeze(
  const rpc::frame::ObReqTransport &transport,
  const obrpc::ObSimpleFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  if (!freeze_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(freeze_info));
  } else {
    const int64_t launch_start_time = ObTimeUtility::current_time();
    obrpc::ObMajorFreezeRpcProxy proxy;
    ObAddr leader;
    obrpc::ObMajorFreezeRequest req(freeze_info);
    obrpc::ObMajorFreezeResponse resp;
    uint64_t tenant_id = freeze_info.tenant_id_;

    uint64_t min_data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, min_data_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else if (min_data_version < DATA_VERSION_4_1_0_0) {
      ret = OB_MAJOR_FREEZE_NOT_ALLOW;
      LOG_WARN("major freeze is not allowed now, please upgrade observer first", KR(ret),
               K(tenant_id), K(min_data_version));
    } else if (OB_ISNULL(GCTX.location_service_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid GCTX", KR(ret));
    } else if (OB_FAIL(proxy.init(&transport))) {
      LOG_WARN("fail to init", KR(ret));
    } else {
      const int64_t MAX_RETRY_COUNT = 5;
      bool major_freeze_done = false;

      for (int64_t i = 0; OB_SUCC(ret) && (!major_freeze_done) && (i < MAX_RETRY_COUNT); ++i) {
        const int64_t timeout_us = MAX(GCONF.rpc_timeout * 5, MAX_PROCESS_TIME_US); // timeout >= 10s
        if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(GCONF.cluster_id,
                    tenant_id, share::SYS_LS, leader))) {
          LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id));
        } else if (OB_FAIL(proxy.to(leader)
                                .trace_time(true)
                                .timeout(timeout_us)
                                .by(tenant_id)
                                .dst_cluster_id(GCONF.cluster_id)
                                .major_freeze(req, resp))) {
          LOG_WARN("tenant_major_freeze rpc failed", KR(ret), K(tenant_id), K(leader),
                   K(freeze_info), K(timeout_us));
        } else if (FALSE_IT(ret = resp.err_code_)) {
        } else if (OB_FAIL(ret)) {
          if (OB_LEADER_NOT_EXIST == ret || OB_EAGAIN == ret) {
            const int64_t RESERVED_TIME_US = 600 * 1000; // 600 ms
            const int64_t timeout_remain_us = THIS_WORKER.get_timeout_remain();
            const int64_t idle_time_us = 200 * 1000 * (i + 1);
            if (timeout_remain_us - idle_time_us > RESERVED_TIME_US) {
              LOG_WARN("leader may switch or ddl confilict, will retry", KR(ret), K(tenant_id), K(freeze_info),
                "ori_leader", leader, K(timeout_remain_us), K(idle_time_us), K(RESERVED_TIME_US));
              USLEEP(idle_time_us);
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("leader may switch or ddl confilict, will not retry cuz timeout_remain is "
                "not enough", KR(ret), K(tenant_id), K(freeze_info), "ori_leader", leader,
                K(timeout_remain_us), K(idle_time_us), K(RESERVED_TIME_US));
            }
          } else if ((OB_MAJOR_FREEZE_NOT_FINISHED != ret) && (OB_FROZEN_INFO_ALREADY_EXIST != ret)) {
            LOG_WARN("fail to major_freeze", KR(ret), K(tenant_id), K(leader), K(freeze_info));
          }
        } else {
          major_freeze_done = true;
        }
      }

      if (OB_SUCC(ret) && !major_freeze_done) {
        ret = OB_EAGAIN;
        LOG_WARN("fail to retry major freeze cuz switching role", KR(ret), K(MAX_RETRY_COUNT));
      }
    }

    const int64_t launch_cost_time = ObTimeUtility::current_time() - launch_start_time;
    LOG_INFO("do tenant major freeze", KR(ret), K(tenant_id), K(leader), K(freeze_info), K(launch_cost_time));
  }
  // TODO oushen
  // 1. parallel major_freeze
  return ret;
}

int ObMajorFreezeHelper::suspend_merge(const ObTenantAdminMergeParam &param)
{
  return do_tenant_admin_merge(param, obrpc::ObTenantAdminMergeType::SUSPEND_MERGE);
}

int ObMajorFreezeHelper::resume_merge(const ObTenantAdminMergeParam &param)
{
  return do_tenant_admin_merge(param, obrpc::ObTenantAdminMergeType::RESUME_MERGE);
}

int ObMajorFreezeHelper::clear_merge_error(const ObTenantAdminMergeParam &param)
{
  return do_tenant_admin_merge(param, obrpc::ObTenantAdminMergeType::CLEAR_MERGE_ERROR);
}

int ObMajorFreezeHelper::do_tenant_admin_merge(
    const ObTenantAdminMergeParam &param,
    const obrpc::ObTenantAdminMergeType &admin_type)
{
  int ret = OB_SUCCESS;
  ObSEArray<obrpc::ObSimpleFreezeInfo, 32> freeze_info_array;
  bool want_to_freeze_all = false;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param), KR(ret));
  } else {
    want_to_freeze_all = param.need_all_ || param.need_all_user_ || param.need_all_meta_;
    if (want_to_freeze_all) {
      if (OB_FAIL(get_specific_tenant_freeze_info(param.need_all_, param.need_all_user_,
                                                  param.need_all_meta_, freeze_info_array))) {
        LOG_WARN("fail to get specific tenant freeze info", KR(ret));
      }
    } else {
      for (int64_t i = 0; (i < param.tenant_array_.count()) && OB_SUCC(ret); ++i) {
        obrpc::ObSimpleFreezeInfo freeze_info;
        freeze_info.tenant_id_ = param.tenant_array_.at(i);
        if (OB_FAIL(freeze_info_array.push_back(freeze_info))) {
          LOG_WARN("fail to push back freeze info", KR(ret), K(param));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(freeze_info_array.empty() && !want_to_freeze_all)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get valid tenant freeze info", KR(ret), K(admin_type));
  } else {
    int final_ret = OB_SUCCESS;
    for (int i = 0; (i < freeze_info_array.count()) && OB_SUCC(ret); ++i) {
      const uint64_t tenant_id = freeze_info_array.at(i).tenant_id_;
      if (OB_FAIL(do_one_tenant_admin_merge(*param.transport_, tenant_id, admin_type))) {
        LOG_WARN("fail do tenant admin merge", KR(ret), K(tenant_id), K(admin_type));
      }
      if (OB_FAIL(ret) && (OB_SUCCESS == final_ret)) {
        final_ret = ret;
      }
      // ignore ret, continue
      ret = OB_SUCCESS;
    }

    ret = final_ret;
  }

  return ret;
}

int ObMajorFreezeHelper::do_one_tenant_admin_merge(
    const rpc::frame::ObReqTransport &transport,
    const uint64_t tenant_id,
    const obrpc::ObTenantAdminMergeType &admin_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(admin_type));
  } else {
    obrpc::ObTenantAdminMergeRpcProxy proxy;
    ObAddr leader;
    obrpc::ObTenantAdminMergeRequest req(tenant_id, admin_type);
    obrpc::ObTenantAdminMergeResponse resp;

    if (OB_ISNULL(GCTX.location_service_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid GCTX", KR(ret));
    } else if (OB_FAIL(proxy.init(&transport))) {
      LOG_WARN("fail to init", KR(ret));
    } else {
      const int64_t MAX_RETRY_COUNT = 5;
      bool admin_merge_done = false;

      for (int64_t i = 0; OB_SUCC(ret) && (!admin_merge_done) && (i < MAX_RETRY_COUNT); ++i) {
        if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(GCONF.cluster_id,
                    tenant_id, share::SYS_LS, leader))) {
          LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id));
        } else if (OB_FAIL(proxy.to(leader)
                                .trace_time(true)
                                .timeout(THIS_WORKER.get_timeout_remain())
                                .by(tenant_id)
                                .dst_cluster_id(GCONF.cluster_id)
                                .tenant_admin_merge(req, resp))) {
          LOG_WARN("tenant_admin_merge rpc failed", KR(ret), K(tenant_id), K(leader), K(admin_type));
        } else if (FALSE_IT(ret = resp.err_code_)) {
        } else if (OB_FAIL(ret)) {
          if (OB_LEADER_NOT_EXIST == ret || OB_EAGAIN == ret) {
            const int64_t RESERVED_TIME_US = 600 * 1000; // 600 ms
            const int64_t timeout_remain_us = THIS_WORKER.get_timeout_remain();
            const int64_t idle_time_us = 200 * 1000 * (i + 1);
            if (timeout_remain_us - idle_time_us > RESERVED_TIME_US) {
              LOG_WARN("leader may switch, will retry", KR(ret), K(tenant_id), K(admin_type),
                "ori_leader", leader, K(timeout_remain_us), K(idle_time_us), K(RESERVED_TIME_US));
              USLEEP(idle_time_us);
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("leader may switch, will not retry cuz timeout_remain is not enough",
                KR(ret), K(tenant_id), K(admin_type), "ori_leader", leader, K(timeout_remain_us),
                K(idle_time_us), K(RESERVED_TIME_US));
            }
          } else {
            LOG_WARN("fail to execute tenant_admin_merge", K(tenant_id), K(leader), K(admin_type), KR(ret));
          }
        } else {
          admin_merge_done = true;
        }
      }

      if (OB_SUCC(ret) && !admin_merge_done) {
        ret = OB_LEADER_NOT_EXIST;
        LOG_WARN("fail to retry admin merge cuz switching role", KR(ret), K(MAX_RETRY_COUNT));
      }
    }

    LOG_INFO("finish to do tenant admin mrege", K(tenant_id), K(leader), K(admin_type), KR(ret));
  }
  return ret;
}

int ObMajorFreezeHelper::get_frozen_status(
    const int64_t tenant_id,
    const SCN &frozen_scn,
    share::ObFreezeInfo &frozen_status)
{
  int ret = OB_SUCCESS;
  share::ObFreezeInfoProxy freeze_info_proxy(tenant_id);

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid GCTX", KR(ret));
  } else if (OB_FAIL(freeze_info_proxy.get_freeze_info(*GCTX.sql_proxy_, frozen_scn, frozen_status))) {
    if (OB_ITER_END != ret && OB_TABLE_NOT_EXIST != ret) {
      LOG_WARN("fail to get freeze info", KR(ret), K(frozen_scn), K(tenant_id));
    }
  }

  return ret;
}

int ObMajorFreezeHelper::get_frozen_scn(
    const int64_t tenant_id,
    SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  share::ObFreezeInfo frozen_status;

  // use min_scn to get frozen_status, means get one with biggest frozen_scn
  if (OB_FAIL(get_frozen_status(tenant_id, SCN::min_scn(), frozen_status))) {
    if (OB_ITER_END != ret && OB_TABLE_NOT_EXIST != ret) {
      LOG_WARN("fail to get frozen info", KR(ret));
    }
  } else {
    frozen_scn = frozen_status.frozen_scn_;
  }

  return ret;
}

int ObMajorFreezeHelper::add_user_warning(
    const uint64_t tenant_id,
    const char *buf)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_WARNING_LEN = 1000;
  char warn_buf[1024] = { 0 };
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(buf));
  } else if (STRLEN(buf) > MAX_WARNING_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf is too long", KR(ret), "buf_len", STRLEN(buf), K(MAX_WARNING_LEN));
  } else if (OB_FAIL(databuff_printf(warn_buf, 1024, "[T%lu]%s", tenant_id, buf))) {
    LOG_WARN("fail to construct warn buf", KR(ret));
  } else {
    LOG_USER_WARN(OB_MAJOR_FREEZE_NOT_ALLOW, warn_buf);
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
