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

#include "ob_lost_replica_checker.h"

#include "lib/time/ob_time_utility.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/container/ob_se_array.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ls/ob_ls_table_iterator.h"//ObTenantLSTableIterator
#include "share/ls/ob_ls_info.h"//ObLSInfo
#include "share/ob_all_server_tracer.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
namespace rootserver
{

ObLostReplicaChecker::ObLostReplicaChecker()
  : inited_(false), cond_(),
    lst_operator_(NULL),
    schema_service_(NULL)
{
}

ObLostReplicaChecker::~ObLostReplicaChecker()
{
}

int ObLostReplicaChecker::check_cancel_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLostReplicaChecker not inited", KR(ret), K(inited_));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service is null", KR(ret));
  } else if (!GCTX.root_service_->in_service()) {
    ret = OB_CANCELED;
    LOG_WARN("root service is stop", KR(ret));
  } else {
    //nothing todo
  }
  return ret;
}

int ObLostReplicaChecker::init(ObLSTableOperator &lst_operator, ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT))) {
    LOG_WARN("fail to init thread cond, ", K(ret));
  } else if (OB_FAIL(create(thread_cnt, "LostRepCheck"))) {
    LOG_WARN("create empty server checker thread failed", K(ret), K(thread_cnt));
  } else {
    lst_operator_ = &lst_operator;
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

void ObLostReplicaChecker::run3()
{
  LOG_INFO("lost replica checker start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t wait_time_ms = 10 * 1000;//10s;
    while (!stop_) {
      ret = OB_SUCCESS;
      ObThreadCondGuard guard(cond_);
      if (OB_FAIL(check_lost_replicas())) {
        LOG_WARN("failed to check lost replica", KR(ret));
      }
      if (OB_SUCCESS != cond_.wait(wait_time_ms)) {
          LOG_DEBUG("wait timeout", K(wait_time_ms));
      }
    }
  }
  LOG_INFO("lost replica checker stop");

}

int ObLostReplicaChecker::check_lost_replicas()
{
  int ret = OB_SUCCESS;
  ObLSInfo ls_info;
  int tmp_ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_id_array;
  LOG_INFO("start checking lost replicas");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel_())) {
    LOG_WARN("need cancel", KR(ret));
 } else if (OB_ISNULL(schema_service_) || OB_ISNULL(lst_operator_)) {
    ret  = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), KP(schema_service_), KP(lst_operator_));
 } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_id_array))) {
   LOG_WARN("fail to get tenant id array", KR(ret));
 } else {
   for (int64_t i = 0; OB_SUCC(ret) && i < tenant_id_array.count(); ++i) {
     ObTenantLSTableIterator iter;
     const uint64_t tenant_id = tenant_id_array.at(i);
     if (is_user_tenant(tenant_id)) {
       //nothing
     } else if (OB_FAIL(iter.init(*lst_operator_, tenant_id))) {
       LOG_WARN("failed to init iter", KR(ret), K(tenant_id));
     } else {
       while (OB_SUCC(ret)) {
         ls_info.reset();
         if (OB_FAIL(iter.next(ls_info))) {
           if (OB_ITER_END == ret) {
             ret = OB_SUCCESS;
           } else {
             LOG_WARN("iterate partition table failed", KR(ret));
           }
           break;
         } else if (OB_SUCCESS != (tmp_ret = check_lost_replica_by_ls_(ls_info))) {
           LOG_WARN("failed to check lost replica by ls", KR(ret), KR(tmp_ret),
               K(ls_info));
         }
       }//end while
     }
     //ignore each tenant error
     ret = OB_SUCCESS;
   }//end for
 }
  return ret;
}

int ObLostReplicaChecker::check_lost_replica_by_ls_(const share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  bool is_lost_replica = false;
  int64_t lost_count = 0;
  LOG_DEBUG("start checking lost replicas by ls", K(ls_info));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls info invalid", KR(ret), K(ls_info));
  } else if (OB_ISNULL(lst_operator_)) {
    ret  = OB_ERR_UNEXPECTED;
    LOG_WARN("ls operator is null", KR(ret), KP(lst_operator_));
  } else {
    const share::ObLSInfo::ReplicaArray &replicas = ls_info.get_replicas();
    FOREACH_CNT_X(replica, replicas, OB_SUCC(ret)) {
      is_lost_replica = false;
      if (OB_FAIL(check_lost_replica_(ls_info, *replica, is_lost_replica))) {
        LOG_WARN("check_lost_replica failed", KR(ret), K(ls_info), KPC(replica));
      } else if (is_lost_replica) {
        lost_count++;
        if (OB_FAIL(lst_operator_->remove(replica->get_tenant_id(),
                                          replica->get_ls_id(),
                                          replica->get_server(),
                                          false/*inner_table_only*/))) {
          LOG_WARN("lst_operator remove replica failed", KR(ret), KPC(replica));
        } else {
          LOG_INFO("lost replica checker remove lost replica finish", KR(ret), KPC(replica));
        }

        if (OB_SUCC(ret) && is_sys_tenant(replica->get_tenant_id())) {
          if (OB_FAIL(lst_operator_->remove(replica->get_tenant_id(),
                                            replica->get_ls_id(),
                                            replica->get_server(),
                                            true/*inner_table_only*/))) {
            LOG_WARN("lst_operator remove replica from inner table failed",
                     KR(ret), KPC(replica));
          } else {
            LOG_INFO("lost replica checker remove lost replica from inner table finish",
                     KR(ret), KPC(replica));
          }
        }
      } else {
        // do nothing
      }
    }
  }

  LOG_DEBUG("finish checking lost replicas by pt, lost_count means count of replicas "
      "that on server offline long enough", K(lost_count), K(ls_info), K(ret));
  return ret;
}

int ObLostReplicaChecker::check_lost_replica_(const ObLSInfo &ls_info,
                                             const ObLSReplica &replica,
                                             bool &is_lost_replica) const
{
  int ret = OB_SUCCESS;
  is_lost_replica = false;
  bool is_lost_server = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ls_info.is_valid() || !replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_info or invalid replica", KR(ret), K(ls_info), K(replica));
  } else if (OB_FAIL(check_lost_server_(replica.get_server(), is_lost_server))) {
    LOG_WARN("check lost server failed", "server", replica.get_server(), K(ret));
  } else if (is_lost_server) {
    /*
     * make sure whether a replica is lost, a lost replica could be cleaned from meta table
     * a replica is lost if satisfied consitions below:
     * 1. server is lost.
     * 2. replica is not in service.
     *    (F-replica is in-service when exists in member_list;
     *     R-replica is in-service when exists in learner_list)
     * 3. replica is in service but not exists in __all_ls_status
     *    (if exists, let remove_member handle this replica;
     *     if not exist, this replica is lost, maybe GC module didn't clean it)
     *
     */
    if (!replica.is_in_service()) {
      is_lost_replica = true;
      LOG_INFO("replica not in service", K(replica));
    } else {
      // go on check ls_status
      ObLSStatusOperator status_op;
      share::ObLSStatusInfo status_info;
      if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql proxy is null", KR(ret));
      } else if (OB_FAIL(status_op.get_ls_status_info(
              ls_info.get_tenant_id(), ls_info.get_ls_id(), status_info, *GCTX.sql_proxy_))) {
        LOG_WARN("failed to get ls status info", KR(ret), K(ls_info));
        if (OB_ENTRY_NOT_EXIST == ret) {
          is_lost_replica = true;
          LOG_INFO("replica not in __all_ls_status", K(replica));
          ret = OB_SUCCESS;
        }
      }
    }
    LOG_INFO("finish check lost replica", KR(ret), K(is_lost_replica), K(replica));
  }
  return ret;
}


int ObLostReplicaChecker::check_lost_server_(const ObAddr &server, bool &is_lost_server) const
{
  int ret = OB_SUCCESS;
  is_lost_server = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (!SVR_TRACER.has_build()) {
    is_lost_server = false;
  } else {
    ObServerInfoInTable server_info;
    ret = SVR_TRACER.get_server_info(server, server_info);
    if (OB_ENTRY_NOT_EXIST != ret && OB_SUCCESS != ret) {
      LOG_WARN("get_server_status failed", K(server), K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_lost_server = true;
      LOG_INFO("server not exist", K(server));
    } else if (server_info.is_permanent_offline()) {
      is_lost_server = true;
    }
  }
  return ret;
}

void ObLostReplicaChecker::wakeup()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    cond_.broadcast();
  }
}

void ObLostReplicaChecker::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    ObThreadCondGuard guard(cond_);
    cond_.broadcast();
  }
}

}//end namespace rootserver
}//end namespace oceanbase
