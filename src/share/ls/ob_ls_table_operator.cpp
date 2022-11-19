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

#define USING_LOG_PREFIX SHARE_PT

#include "rootserver/ob_root_utils.h"       // for rootserver::ObRootUtils::get_rs_default_timeout_ctx
#include "ob_ls_table_operator.h"   // for declarations of functions in this cpp

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace obrpc;
ObLSTableOperator::ObLSTableOperator()
  : ObLSTable(),
    inited_(false),
    root_ls_(&inmemory_ls_),
    inmemory_ls_(),
    rpc_ls_(),
    persistent_ls_()
{
}

ObLSTableOperator::~ObLSTableOperator()
{
}

int ObLSTableOperator::init(ObISQLClient &sql_proxy, common::ObServerConfig *config /*=NULL*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("log stream table operator has already inited", KR(ret));
  } else if (OB_FAIL(persistent_ls_.init(sql_proxy, config))) {
    LOG_WARN("init persistent log stream table failed", KR(ret));
  } else {
    root_ls_ = &inmemory_ls_;
    inited_ = true;
  }
  return ret;
}

int ObLSTableOperator::set_use_memory_ls_(ObIRsListChangeCb &rs_list_change_cb)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_UNLIKELY(!inmemory_ls_.is_inited())) {
      if (OB_FAIL(inmemory_ls_.init(rs_list_change_cb))) {
        LOG_WARN("inmemory_ls_ init failed", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      root_ls_ = &inmemory_ls_;
    }
  }
  return ret;
}

int ObLSTableOperator::set_use_rpc_ls_(
    ObCommonRpcProxy &rpc_proxy,
    obrpc::ObSrvRpcProxy &srv_rpc_proxy,
    ObRsMgr &rs_mgr,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (!rpc_ls_.is_inited()) {
      if (OB_FAIL(rpc_ls_.init(rpc_proxy, srv_rpc_proxy, rs_mgr, sql_proxy))) {
        LOG_WARN("rpc_ls_ init failed", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      inmemory_ls_.reuse();
      root_ls_ = &rpc_ls_;
    }
  }
  return ret;
}

// RS should use inmemory_ls_table to get infos
int ObLSTableOperator::set_callback_for_rs(
    ObIRsListChangeCb &rs_list_change_cb)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_use_memory_ls_(rs_list_change_cb))) {
    LOG_WARN("fail to set use memory table", KR(ret));
  }
  return ret;
}

// Observers should use rpc_ls_table to get infos
int ObLSTableOperator::set_callback_for_obs(
    ObCommonRpcProxy &rpc_proxy,
    obrpc::ObSrvRpcProxy &srv_rpc_proxy,
    ObRsMgr &rs_mgr,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_use_rpc_ls_(rpc_proxy, srv_rpc_proxy, rs_mgr, sql_proxy))) {
    LOG_WARN("fail to set user rpc table", KR(ret));
  }
  return ret;
}

int ObLSTableOperator::update(const ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  const int64_t begin = ObTimeUtility::fast_current_time();
  ObLSTable *lst = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica));
  } else if (OB_FAIL(get_ls_table_(GCONF.cluster_id, replica.get_tenant_id(), replica.get_ls_id(), lst))) {
    LOG_WARN("get ls table failed", KR(ret), K(replica));
  } else if (OB_ISNULL(lst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ls table", KR(ret));
  } else if (OB_FAIL(lst->update(replica))) {
    LOG_WARN("update replica failed", KR(ret), K(replica));
  }
  LOG_INFO("update ls replica",
      KR(ret), "time_used", ObTimeUtility::fast_current_time() - begin, K(replica));
  return ret;
}

int ObLSTableOperator::get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::fast_current_time();
  ObLSTable *lst = NULL;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_valid_key(tenant_id, ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KT(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_ls_table_(cluster_id, tenant_id, ls_id, lst))) {
    LOG_WARN("get ls table failed", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(lst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ls table", KR(ret));
  } else {
    ls_info.reset();
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(lst->get(cluster_id, tenant_id, ls_id, ls_info))) {
      LOG_WARN("get log stream info failed", KR(ret), KT(tenant_id), K(ls_id));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t now = ObTimeUtility::fast_current_time();
    EVENT_INC(LS_ALL_TABLE_OPERATOR_GET_COUNT);
    EVENT_ADD(LS_ALL_TABLE_OPERATOR_GET_TIME, now - start_time);
  }
  return ret;
}

int ObLSTableOperator::get_ls_table_(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObLSTable *&ls_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_valid_key(tenant_id, ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (is_sys_tenant(tenant_id)) {
    if (GCONF.cluster_id == cluster_id) {
      ls_table = root_ls_;
    } else {
      //get rs_list from remote cluster must use rpc_ls_operator
      //can not get rs_list of remote cluster in inmemory
      ls_table = static_cast<ObLSTable *>(&rpc_ls_);
    }
  } else {
    ls_table = static_cast<ObLSTable *>(&persistent_ls_);
  }
  return ret;
}

int ObLSTableOperator::get_by_tenant(
    const uint64_t tenant_id,
    ObIArray<ObLSInfo> &ls_infos)
{
  int ret = OB_SUCCESS;
  ls_infos.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (is_sys_tenant(tenant_id)) {
    ObLSInfo ls_info;
    if (OB_FAIL(root_ls_->get(GCONF.cluster_id, OB_SYS_TENANT_ID, SYS_LS, ls_info))) {
      LOG_WARN("fail to get sys_tenant ls", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ls_infos.push_back(ls_info))) {
      LOG_WARN("fail to assign", KR(ret), K(ls_info));
    }
  } else if (is_meta_tenant(tenant_id) || is_user_tenant(tenant_id)) {
    if (OB_FAIL(persistent_ls_.get_by_tenant(tenant_id, ls_infos))) {
      LOG_WARN("get all ls info by persistent_ls_ failed", KR(ret), K(tenant_id));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObLSTableOperator::load_all_ls_in_tenant(
    const uint64_t exec_tenant_id,
    ObIArray<ObLSInfo> &ls_infos)
{
  int ret = OB_SUCCESS;
  ls_infos.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_user_tenant(exec_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can not be user tenant", KR(ret), K(exec_tenant_id));
  }

  if (FAILEDx(persistent_ls_.load_all_ls_in_tenant(exec_tenant_id, ls_infos))) {
    LOG_WARN("load tenant ls info failed", KR(ret), K(exec_tenant_id));
  } else if (is_sys_tenant(exec_tenant_id)) {
    ObLSInfo ls_info;
    if (OB_ISNULL(root_ls_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root ls is null", KR(ret));
    } else if (OB_FAIL(root_ls_->get(GCONF.cluster_id, OB_SYS_TENANT_ID, SYS_LS, ls_info))) {
      LOG_WARN("fail to get sys_tenant ls", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(ls_infos.push_back(ls_info))) {
      LOG_WARN("fail to assign", KR(ret), K(ls_info));
    }
  }
  return ret;
}

int ObLSTableOperator::remove(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObAddr &server)
{
  int ret = OB_SUCCESS;
  ObLSTable *lst = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!ls_id.is_valid_with_tenant(tenant_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(server));
  } else if (OB_FAIL(get_ls_table_(GCONF.cluster_id, tenant_id, ls_id, lst))) {
    LOG_WARN("get ls table failed", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(lst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ls table", KR(ret), K(tenant_id), K(ls_id), K(server));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(lst->remove(tenant_id, ls_id, server))) {
      LOG_WARN("remove ls failed", KR(ret), K(tenant_id), K(ls_id), K(server));
    } else {
      LOG_INFO("success to remove ls by operator",
          KR(ret), K(tenant_id), K(ls_id), K(server));
    }
  }
  return ret;
}


int ObLSTableOperator::remove_residual_ls(
    const uint64_t tenant_id,
    const ObAddr &server,
    int64_t &residual_count)
{
  int ret = OB_SUCCESS;
  residual_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(server));
  } else if (OB_UNLIKELY(is_sys_tenant(tenant_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support remove residual ls from sys tenant", KR(ret), K(tenant_id));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(persistent_ls_.remove_residual_ls(tenant_id, server, residual_count))) {
      LOG_WARN("remove residual ls failed", KR(ret), K(tenant_id), K(server));
    } else if (residual_count > 0) {
      LOG_INFO("success to remove ls residual by persistent_ls_",
          KR(ret), K(tenant_id), K(server), K(residual_count));
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
