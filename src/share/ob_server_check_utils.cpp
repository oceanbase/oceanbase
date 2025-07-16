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
#define USING_LOG_PREFIX SHARE

#include "ob_server_check_utils.h"

#include "share/ls/ob_ls_info.h"//ObLSInfo
#include "share/ls/ob_ls_table_operator.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_unit_table_operator.h"
#include "observer/ob_server_struct.h"
#include "lib/utility/ob_print_utils.h"  // for databuff_printf etc.

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace obrpc;
int ObServerCheckUtils::check_if_tenant_ls_replicas_exist_in_servers(
    const uint64_t tenant_id,
    const common::ObArray<common::ObAddr> &servers,
    bool &exist)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObLSInfo> tenant_ls_infos;
  ObArray<ObAddr> empty_servers;
  exist = false;
  // if a tenant has ls replicas on a server, the server is not empty.
  empty_servers.reset();
  if (OB_ISNULL(GCTX.lst_operator_)) {
    ret  = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.lst_operator_ is null", KR(ret), KP(GCTX.lst_operator_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.lst_operator_->load_all_ls_in_tenant(gen_meta_tenant_id(tenant_id), tenant_ls_infos))) {
    LOG_WARN("fail to execute load_all_ls_in_tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(empty_servers.assign(servers))) {
    // assumpt that all servers are empty
    // (i.e. assumpt that the tenant does not have any ls replicas on these servers)
    // not empty servers will be removed from empty_servers array in func check_server_empty_by_ls_
    LOG_WARN("fail to assign servers to another array", KR(ret), K(servers));
  } else {
    for (int64_t i = 0; i < tenant_ls_infos.count() && OB_SUCC(ret) && empty_servers.count() == servers.count(); i++) {
      // if empty_servers.count() < servers.count()
      // it means that there is a not empty server
      // the check can be returned
      const ObLSInfo &ls_info = tenant_ls_infos.at(i);
      if (OB_FAIL(check_server_empty_by_ls(ls_info, empty_servers))) {
        LOG_WARN("fail to check server empty", KR(ret), K(ls_info));
      } else if (empty_servers.count() < servers.count()) {
        exist = true;
        LOG_INFO("the tenant has ls replicas on one of the given servers", KR(ret),
            K(tenant_id), K(ls_info), K(empty_servers), K(servers));
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(CHECK_SERVER_EMPTY_WHEN_LS_HAS_NO_LEADER);
int ObServerCheckUtils::check_server_empty_by_ls(
    const share::ObLSInfo &ls_info,
    common::ObArray<common::ObAddr> &empty_servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls info is invalid", KR(ret), K(ls_info));
  } else {
    const ObIArray<ObLSReplica> &replica_array = ls_info.get_replicas();
    const ObLSReplica *replica = NULL;
    int64_t idx = -1;
    // filter leader member_list
    if (OB_FAIL(ls_info.find_leader(replica))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_LEADER_NOT_EXIST;
      }
      LOG_WARN("find leader failed", K(ret), K(ls_info));
    } else if (OB_ISNULL(replica)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL replica pointer", K(ret));
    } else {
      // check whether has member on empty servers
      FOREACH_CNT_X(m, replica->get_member_list(), OB_SUCC(ret) && empty_servers.count() > 0) {
        const ObAddr &addr = m->get_server();
        if (has_exist_in_array(empty_servers, addr, &idx)) {
          //has member in server
          LOG_INFO("ls replica has member on server", K(ls_info), K(addr), K(empty_servers));
          if (OB_FAIL(empty_servers.remove(idx))) {
            LOG_WARN("failed to remove addr from empty servers", KR(ret), K(idx), K(empty_servers));
          }
        }
      }  // end FORECAH member_list
      ObMember learner;
      for (int64_t index = 0;
          OB_SUCC(ret) && index < replica->get_learner_list().get_member_number() && empty_servers.count() > 0;
          ++index) {
        learner.reset();
        if (OB_FAIL(replica->get_learner_list().get_member_by_index(index, learner))) {
          LOG_WARN("fail to get learner by index", KR(ret), K(index));
        } else {
          const ObAddr &addr = learner.get_server();
          if (has_exist_in_array(empty_servers, addr, &idx)) {
            //has learner in server
            LOG_INFO("ls replica has learner on server", K(ls_info), K(addr), K(empty_servers));
            if (OB_FAIL(empty_servers.remove(idx))) {
              LOG_WARN("failed to remove addr from empty servers", KR(ret), K(idx), K(empty_servers));
            }
          }
        }
      }
    }
    // filter server of replicas
    for (int64_t i = 0; i < replica_array.count() && OB_SUCC(ret); ++i) {
      const ObAddr &addr = replica_array.at(i).get_server();
      if (has_exist_in_array(empty_servers, addr, &idx)) {
        //has member in server
        LOG_INFO("this sever has ls replica", K(ls_info), K(addr));
        if (OB_FAIL(empty_servers.remove(idx))) {
          LOG_WARN("failed to remove addr from empty servers", KR(ret), K(idx));
        }
      }
    }//end for
  }
  if (OB_SUCC(ret) && CHECK_SERVER_EMPTY_WHEN_LS_HAS_NO_LEADER) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("errsim CHECK_SERVER_EMPTY_WHEN_LS_HAS_NO_LEADER opened", KR(ret));
  }
  return ret;
}

#define PRINT_USER_ERROR(error_msg) \
  do { \
    int tmp_ret = OB_SUCCESS; \
    const int64_t COMMENT_LENGTH = 512; \
    char comment[COMMENT_LENGTH] = {0}; \
    int64_t pos = 0; \
    if (OB_TMP_FAIL(databuff_printf(comment, COMMENT_LENGTH, pos, \
                "%s, %s is", error_msg, op_str))) { \
      LOG_WARN("failed to printf to comment", KR(ret), KR(tmp_ret), K(op_str)); \
    } else { \
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, comment); \
    } \
  } while(0)

int ObServerCheckUtils::check_tenant_server_online(const uint64_t tenant_id, const char * const op_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObAddr> offline_servers;
  common::ObArray<common::ObAddr> tenant_servers;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenants_servers(tenant_id, tenant_servers))) {
    LOG_WARN("fail to get tenant servers", KR(ret), K(tenant_id));
  } else {
    ObServerInfoInTable server_info;
    for (int64_t i = 0; i < tenant_servers.size() && OB_SUCC(ret); ++i) {
      const ObAddr &server = tenant_servers[i];
      if (OB_FAIL(SVR_TRACER.get_server_info(server, server_info))) {
        LOG_WARN("fail to execute get_server_info", KR(ret), K(server));
      } else if (server_info.is_temporary_offline() || server_info.is_permanent_offline()) {
        if (OB_FAIL(offline_servers.push_back(server))) {
          LOG_WARN("fail to push back", KR(ret), K(server), K(offline_servers));
        }
      }
    }
  }
  if (OB_SUCC(ret) && offline_servers.count() > 0) {
      bool exists = false;
      if (OB_FAIL(check_if_tenant_ls_replicas_exist_in_servers(tenant_id, offline_servers, exists))) {
        LOG_WARN("fail to check if the tenant's LS replicas exist in offline_servers",
            KR(ret), K(tenant_id), K(offline_servers));
      if (OB_LEADER_NOT_EXIST == ret) {
        ret = OB_OP_NOT_ALLOW;
        PRINT_USER_ERROR("The tenant has LS replicas without leader");
      }
    } else if (exists) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("the tenant has LS replicas on at least one of the offline servers",
          KR(ret), K(tenant_id), K(exists), K(offline_servers));
      PRINT_USER_ERROR("The tenant has LS replicas on at least one of the offline servers");
    }
  }
  return ret;
}

int ObServerCheckUtils::get_tenants_servers(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &tenant_servers)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  tenant_servers.reset();
  ObUnitTableOperator unit_operator;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(unit_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id, units))) {
    LOG_WARN("failed to get tenant unit", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
    const ObAddr &server = units[i].server_;
    const ObAddr &migrate_from_server = units[i].migrate_from_server_;
    if (OB_FAIL(tenant_servers.push_back(server))) {
      LOG_WARN("fail to push back server", KR(ret), K(server), K(tenant_servers));
    } else if (migrate_from_server.is_valid() && OB_FAIL(tenant_servers.push_back(migrate_from_server))) {
      LOG_WARN("fail to push back server", KR(ret), K(migrate_from_server), K(tenant_servers));
    }
  }
  return ret;
}

int ObServerCheckUtils::check_offline_and_get_tenants_servers(
    const uint64_t tenant_id,
    const bool allow_temp_offline,
    common::ObIArray<common::ObAddr> &target_servers,
    const char * const op_str)
{
  int ret = OB_SUCCESS;
  target_servers.reset();
  common::ObArray<common::ObAddr> tenant_servers;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenants_servers(tenant_id, tenant_servers))) {
    LOG_WARN("fail to get tenant servers", KR(ret), K(tenant_id));
  } else {
    ObServerInfoInTable server_info;
    for (int64_t i = 0; i < tenant_servers.size() && OB_SUCC(ret); ++i) {
      const ObAddr &server = tenant_servers[i];
      if (OB_FAIL(SVR_TRACER.get_server_info(server, server_info))) {
        LOG_WARN("fail to execute get_server_info", KR(ret), K(server));
      } else if (server_info.is_permanent_offline()) {
        // skip
      } else if (!allow_temp_offline && server_info.is_temporary_offline()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("the tenant has units on temporary offline servers", KR(ret), K(server_info));
        if (OB_NOT_NULL(op_str)) {
          PRINT_USER_ERROR("The tenant has units on temporary offline servers");
        }
      } else if (OB_FAIL(target_servers.push_back(server))) {
        LOG_WARN("fail to push back", KR(ret), K(server), K(server_info));
      }
    }
  }
  return ret;
}
}
}