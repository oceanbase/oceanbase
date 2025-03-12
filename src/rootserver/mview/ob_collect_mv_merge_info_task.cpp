/**
 * Copyright (c) 2023 OceanBase
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
#include "rootserver/mview/ob_collect_mv_merge_info_task.h"
#include "src/storage/high_availability/ob_storage_ha_src_provider.h"
#include "share/ob_global_stat_proxy.h"

namespace oceanbase
{
namespace rootserver
{
int ObCollectMvMergeInfoTask::init()
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
      LOG_WARN("ObCollectMvMergeInfoTask init twice", KR(ret), KP(this));
    } else {
      const uint64_t tenant_id = MTL_ID();
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
    return ret;
}

int ObCollectMvMergeInfoTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(SLEEP_SECONDS, true /*repeat*/))) {
      LOG_WARN("fail to schedule mview maintenance task", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObCollectMvMergeInfoTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObCollectMvMergeInfoTask::wait() { wait_task(); }

void ObCollectMvMergeInfoTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

// todo siyu :: use new interface to get stable memberlist
int ObCollectMvMergeInfoTask::get_stable_member_list_and_config_version(const uint64_t tenant_id,
                                              const share::ObLSID &ls_id,
                                              common::ObIArray<common::ObAddr> &addr_list,
                                              palf::LogConfigVersion &log_config_version)
{
  int ret = OB_SUCCESS;

  using namespace storage;
  ObLSService *ls_svr = NULL;
  ObStorageRpc *storage_rpc = NULL;
  ObStorageHAGetMemberHelper get_member_helper;
  common::ObAddr leader_addr;
  obrpc::ObFetchStableMemberListArg arg;
  obrpc::ObFetchStableMemberListInfo member_info;
  const uint64_t cluster_id = GCONF.cluster_id;
  int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;

  if (!ls_id.is_valid() || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tenant_id));
  } else if (OB_ISNULL(ls_svr = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls service should not be NULL", KR(ret), KP(ls_svr));
  } else if (OB_ISNULL(storage_rpc = ls_svr->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage rpc shoulde not be null", KR(ret), KP(storage_rpc));
  } else if (OB_FAIL(arg.init(ls_id, tenant_id))) {
    LOG_WARN("failed to fetch stable member list arg", KR(ret), K(arg));
  } else if (OB_FAIL(get_member_helper.init(storage_rpc))) {
    LOG_WARN("failed to init get member helper", KR(ret), KP(storage_rpc));
  } else if (OB_FAIL(get_member_helper.get_ls_leader(tenant_id, ls_id, leader_addr))) {
    LOG_WARN("fail to get ls leader", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr)
             .dst_cluster_id(cluster_id)
             .by(tenant_id)
             .timeout(rpc_timeout)
             .fetch_stable_member_list(arg, member_info))) {
    LOG_WARN("get ls member list failed", KR(ret), K(leader_addr), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(member_info.get_member_list().get_addr_array(addr_list))) {
    LOG_WARN("failed to get member_list addr array", KR(ret), K(member_info));
  }

  return ret;
}

int ObCollectMvMergeInfoTask::check_ls_attr_state_(const ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  if (!ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_attr));
  } else if (!ls_attr.ls_is_normal()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ls not in normal state, skip task", KR(ret), K(ls_attr));
  }
  return ret;
}

int ObCollectMvMergeInfoTask::check_ls_list_state(const ObLSAttrArray &ls_attr_array)
{
  int ret = OB_SUCCESS;

  ARRAY_FOREACH(ls_attr_array, i) {
    const ObLSAttr &ls_attr = ls_attr_array.at(i);
    if (OB_FAIL(check_ls_attr_state_(ls_attr))) {
      LOG_WARN("fail to check ls attr state", KR(ret), K(ls_attr));
    }
  }

  return ret;
}

int ObCollectMvMergeInfoTask::check_ls_list_match(const ObLSAttrArray &ls_attr_array,
                                                  const ObLSAttrArray &ls_attr_array_new)
{
  int ret = OB_SUCCESS;

  if (ls_attr_array.count() != ls_attr_array_new.count()) {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("ls list not match, skip task", KR(ret),
             K(ls_attr_array.count()), K(ls_attr_array_new.count()));
  } else {
    // sorted ls list
    ARRAY_FOREACH(ls_attr_array, i) {
      const ObLSAttr &ls_attr = ls_attr_array.at(i);
      bool is_match = false;
      const ObLSAttr &ls_attr_new = ls_attr_array_new.at(i);
      if (ls_attr.get_ls_id() == ls_attr_new.get_ls_id()) {
        is_match = true;
      } else {
        ret = OB_ITEM_NOT_MATCH;
        LOG_WARN("ls list not match, skip task", KR(ret));
      }
    }
  }

  return ret;
}

int ObCollectMvMergeInfoTask::sync_get_ls_member_merge_info(const common::ObAddr &server,
                                                            const uint64_t user_tenant_id,
                                                            const ObLSID &ls_id,
                                                            storage::ObMajorMVMergeInfo &mv_merge_info,
                                                            uint64_t rpc_timeout,
                                                            const bool need_check_leader,
                                                            const bool need_update)
{
  int ret = OB_SUCCESS;

  ObCollectMvMergeInfoArg arg;
  if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gctx rpc proxy should not be null", K(ret), KP(GCTX.srv_rpc_proxy_));
  } else if (!server.is_valid() || !ls_id.is_valid() || OB_INVALID_TENANT_ID == user_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(ls_id), K(user_tenant_id));
  } else if (OB_FAIL(arg.init(ls_id, user_tenant_id, need_check_leader, need_update))) {
    LOG_WARN("init arg failed", KR(ret));
  } else {
    rootserver::ObCollectMvMergeInfoProxy rpc_proxy(
      *(GCTX.srv_rpc_proxy_), &obrpc::ObSrvRpcProxy::collect_mv_merge_info);
    if (OB_FAIL(rpc_proxy.call(server, rpc_timeout, arg))) {
      LOG_WARN("fail to call collect mv merge info rpc", K(ret), K(server), K(arg));
    }
    int tmp_ret = OB_SUCCESS;
    ObArray<int> return_code_array;
    if (OB_TMP_FAIL(rpc_proxy.wait_all(return_code_array))) {
      LOG_WARN("fail to wait rpc back", KR(ret), K(server), K(arg));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rpc_proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return count", KR(ret), K(server), K(arg));
    } else {
      const int64_t res_idx = 0;
      if (OB_FAIL(return_code_array.at(res_idx))) {
        if (ret == OB_TIMEOUT) {
          ret = OB_WAIT_NEXT_TIMEOUT; // rewrite ret code
          LOG_WARN("rpc return timeout, maybe need retry", KR(ret), K(server), K(arg), K(rpc_timeout));
        } else {
          LOG_WARN("rpc return code failed", KR(ret), K(server), K(arg));
        }
      } else {
        const ObCollectMvMergeInfoResult *res = rpc_proxy.get_results().at(res_idx);
        if (OB_ISNULL(res)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(server), K(arg), KP(res));
        } else if (OB_SUCC(res->get_ret())) {
          mv_merge_info = res->get_mv_merge_info();
        } else {
          LOG_WARN("fail to get server merge info", KR(ret), K(server), K(arg));
        }
      }
    }
  }

  return ret;
}

int ObCollectMvMergeInfoTask::collect_ls_member_merge_info(const uint64_t tenant_id,
                                                           const ObLSID &ls_id,
                                                           share::SCN &merge_scn)
{
  int ret = OB_SUCCESS;
  merge_scn.set_max();
  common::ObArray<common::ObAddr> addr_list;
  palf::LogConfigVersion log_config_version;
  const bool need_check_leader = false;
  const bool need_update = true;
  if (!ls_id.is_valid() || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tenant_id));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gctx rpc proxy should not be null", KR(ret), K(ls_id));
  } else if (OB_FAIL(get_stable_member_list_and_config_version(tenant_id, ls_id,
                                                              addr_list, log_config_version))) {
    LOG_WARN("fail to get member list", KR(ret), K(ls_id));
  } else {
    // batch collect ls merge info
    ObCollectMvMergeInfoProxy batch_proxy(
      *(GCTX.srv_rpc_proxy_), &obrpc::ObSrvRpcProxy::collect_mv_merge_info);
    ObCollectMvMergeInfoArg arg;
    if (OB_FAIL(arg.init(ls_id, tenant_id, need_check_leader, need_update))) {
      LOG_WARN("failed to collect merge info arg", KR(ret), K(arg));
    } else {
      ARRAY_FOREACH(addr_list, i) {
        const common::ObAddr &addr = addr_list.at(i);
        // for debug
        LOG_INFO("iter ls memberlist", K(ls_id), K(addr), K(i), K(arg));
        int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
        if (OB_FAIL(batch_proxy.call(addr, rpc_timeout, arg))) {
          LOG_WARN("fail to call collect mv merge info rpc", KR(ret));
        }
      }
    }

    ObArray<int> return_code_array;
    // wait all rpc back
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(batch_proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(batch_proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("return cnt not match", KR(ret), "return_cnt", return_code_array.count());
    } else {
      int64_t min_idx = OB_INVALID_ID;
      ARRAY_FOREACH(batch_proxy.get_results(), idx) {
        const ObCollectMvMergeInfoResult *res = batch_proxy.get_results().at(idx);
        if (OB_ISNULL(res)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result should not be null", KR(ret), KP(res));
        } else if (OB_FAIL(res->get_ret())) {
          LOG_WARN("return error code, skip task", KR(ret), KP(res));
        } else if (res->get_mv_merge_scn().is_valid()) {
          if (merge_scn > res->get_mv_merge_scn()) {
            min_idx = idx;
            merge_scn = res->get_mv_merge_scn();
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("collect invalid mv merge scn", KR(ret), K(res->get_mv_merge_scn()));
        }
      }
      LOG_INFO("iter ls memberlist to get merge scn", K(ls_id), K(merge_scn), K(ret), K(min_idx));
      if (OB_SUCC(ret)) {
        if (min_idx != OB_INVALID_ID) {
          LOG_INFO("iter ls memberlist to get merge scn", K(ls_id), K(min_idx),
                  K(merge_scn), K(batch_proxy.get_dests().at(min_idx)));
        }
        common::ObArray<common::ObAddr> addr_list_new;
        palf::LogConfigVersion log_config_version_new;
        if (OB_FAIL(get_stable_member_list_and_config_version(tenant_id, ls_id,
                                                             addr_list_new, log_config_version_new))) {
          LOG_WARN("fail to get member list", KR(ret), K(ls_id));
        } else if (log_config_version_new != log_config_version) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("member list or leader changed, skip task", KR(ret),
                   K(ls_id), K(log_config_version), K(log_config_version_new));
        }
      }
    }
  }

  return ret;
}

int ObCollectMvMergeInfoTask::double_check_ls_list_in_trans(const ObLSAttrArray &ls_attr_array,
                                                            common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;

  const int64_t timeout = GCONF.internal_sql_execute_timeout;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id_));
  } else if (!trans.is_started()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans is not started", KR(ret));
  } else {
    ObLSAttrArray ls_attr_array_new;
    share::ObLSAttrOperator ls_attr_operator(tenant_id_, GCTX.sql_proxy_);
    if (OB_FAIL(ls_attr_operator.get_all_ls_by_order_in_trans(true /*lock sys ls*/,
                                                              ls_attr_array_new,
                                                              trans,
                                                              true /*only_existing_ls*/))) {
      LOG_WARN("fail to get all ls in trans", KR(ret));
    } else if (OB_FAIL(check_ls_list_state(ls_attr_array_new))) {
      LOG_WARN("with not normal ls state, skip task", KR(ret));
    } else if (OB_FAIL(check_ls_list_match(ls_attr_array, ls_attr_array_new))) {
      LOG_WARN("ls list not match, skip task", KR(ret));
    }
  }

  return ret;
}

int ObCollectMvMergeInfoTask::check_and_update_tenant_merge_scn(const ObLSAttrArray &ls_attr_array,
                                                                const share::SCN &merge_scn)
{
  int ret = OB_SUCCESS;

  common::ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start tans", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(double_check_ls_list_in_trans(ls_attr_array, trans))) {
    LOG_WARN("fail to double check ls list", KR(ret), K(tenant_id_));
  } else {
    ObGlobalStatProxy proxy(trans, tenant_id_);
    share::SCN tmp_merge_scn;
    if (OB_FAIL(proxy.get_major_refresh_mv_merge_scn(true /* for_update */, tmp_merge_scn))) {
      LOG_WARN("fail to get major_refresh_mv_merge_scn", KR(ret), K(tenant_id_));
    }
    if (OB_SUCC(ret)) {
      if (!tmp_merge_scn.is_valid() || tmp_merge_scn < merge_scn) {
        if (OB_FAIL(proxy.update_major_refresh_mv_merge_scn(merge_scn))) {
          LOG_WARN("fail to update major refresh mv merge scn", KR(ret), K(tenant_id_));
        }
      }
    }
  }

  bool is_commit = ret == OB_SUCCESS ? true : false;
  int tmp_ret = OB_SUCCESS;
  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(is_commit))) {
      LOG_WARN("fail to end trans", K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_SKIP_COLLECT_MV_MERGE_INFO_TASK);
void ObCollectMvMergeInfoTask::runTimerTask() {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  share::SCN merge_scn(share::SCN::max_scn());
  share::ObLSAttrOperator ls_attr_operator(tenant_id_, GCTX.sql_proxy_);
  ObLSAttrArray ls_attr_array;
  bool need_schedule = false;
  share::SCN latest_merge_scn;
  share::SCN major_mv_merge_scn;
  if (OB_UNLIKELY(ERRSIM_SKIP_COLLECT_MV_MERGE_INFO_TASK)) {
    tmp_ret = ERRSIM_SKIP_COLLECT_MV_MERGE_INFO_TASK;
    LOG_INFO("errsim to block collect_mv_merge_info_task", K(tmp_ret));
  }
  if (OB_TMP_FAIL(tmp_ret)) {
    // errsim to skip task
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCollectMvMergeInfoTask not init", KR(ret), KP(this));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy should not null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(is_stop_)) {
    // do nothing
  } else if (OB_FAIL(need_push_major_mv_merge_scn(tenant_id_, need_schedule, latest_merge_scn, major_mv_merge_scn))) {
    LOG_WARN("fail to check need task should schedule or not", KR(ret), K(tenant_id_));
  }

  if (OB_FAIL(ret) || OB_TMP_FAIL(tmp_ret)) {
  } else if (!need_schedule) {
    // do nothing
  } else if (OB_FAIL(ls_attr_operator.get_all_ls_by_order(ls_attr_array))) {
    LOG_WARN("fail to get all ls", KR(ret));
  } else if (OB_FAIL(check_ls_list_state(ls_attr_array))) {
    LOG_WARN("with not normal ls state, skip task", KR(ret));
  } else {
    share::SCN min_merge_scn(share::SCN::max_scn());
    int64_t min_idx = OB_INVALID_ID;
    ARRAY_FOREACH(ls_attr_array, i) {
      const ObLSAttr &ls_attr = ls_attr_array.at(i);
      if (OB_FAIL(check_ls_attr_state_(ls_attr))) {
        LOG_WARN("fail to get all ls", KR(ret), K(ls_attr));
      } else if (ls_attr.get_ls_id().is_sys_ls()) {
        // skip sys ls
      } else if (OB_FAIL(collect_ls_member_merge_info(tenant_id_, ls_attr.get_ls_id(), merge_scn))) {
        LOG_WARN("fail to collect ls member merge scn", KR(ret), K(ls_attr));
      }
      if (OB_SUCC(ret)) {
        if (min_merge_scn > merge_scn) {
          min_idx = i;
          min_merge_scn = merge_scn;
        }
      }
    }
    // double check ls list and update tenant mv merge scn in trans
    if (OB_SUCC(ret) && !min_merge_scn.is_max()) {
      if (min_idx != OB_INVALID_ID) {
        LOG_INFO("iter all ls and get min merge scn", K(min_idx), K(min_merge_scn),
                 K(ls_attr_array.at(min_idx)));
      }
      if (OB_FAIL(check_and_update_tenant_merge_scn(ls_attr_array, min_merge_scn))) {
        LOG_WARN("fail to check and update tenant merge scn", KR(ret));
      }
    }
  }
  LOG_INFO("collect_mv_merge_info task", KR(ret), K(need_schedule), K(latest_merge_scn), K(major_mv_merge_scn));
}

int ObCollectMvMergeInfoTask::get_min_mv_tablet_major_compaction_scn(share::SCN &compaction_scn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t mv_mode = 1; // bit_map 0x00000001 (32bit), 1 means is new mv tablet
  const uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy should not null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (!is_valid_tenant_id(user_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT MIN(C.COMPACTION_SCN) AS MIN_COMPACTION_SCN FROM \
                                     `%s`.`%s` AS A \
                                     JOIN `%s`.`%s` AS B \
                                     ON A.TABLE_ID = B.TABLE_ID \
                                     JOIN `%s`.`%s` AS C \
                                     ON A.TABLET_ID = C.TABLET_ID \
                                     JOIN `%s`.`%s` AS D \
                                     ON C.SVR_IP = D.SVR_IP and C.SVR_PORT = D.SVR_PORT \
                                     AND A.LS_ID = D.LS_ID \
                                     WHERE (B.MV_MODE & %ld) <> 0",
                                     OB_SYS_DATABASE_NAME, OB_ALL_TABLET_TO_LS_TNAME,
                                     OB_SYS_DATABASE_NAME, OB_ALL_TABLE_TNAME,
                                     OB_SYS_DATABASE_NAME, OB_ALL_VIRTUAL_TABLET_META_TABLE_TNAME,
                                     OB_SYS_DATABASE_NAME, OB_ALL_VIRTUAL_LOG_STAT_TNAME,
                                     mv_mode))) {
    LOG_WARN("fail to format sql", KR(ret), K(sql), K(user_tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res,
                                        user_tenant_id,
                                        sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql), K(user_tenant_id));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), KP(mysql_result));
      } else if (OB_FAIL(mysql_result->next())) {
        LOG_WARN("fail to get next", K(ret), KP(mysql_result));
      } else {
        uint64_t min_compaction_scn = 0;
        EXTRACT_UINT_FIELD_MYSQL(*mysql_result, "MIN_COMPACTION_SCN", min_compaction_scn, uint64_t);
        if (OB_SUCC(ret)) {
          compaction_scn.convert_for_gts(min_compaction_scn);
        }
      }
    }
    // for debug
    LOG_INFO("get mv tablet min compaction scn", K(ret), K(compaction_scn), K(sql));
  }
  return ret;
}
}
}
