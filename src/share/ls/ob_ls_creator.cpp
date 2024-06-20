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

#include "common/ob_timeout_ctx.h"
#include "share/ob_share_util.h"
#define USING_LOG_PREFIX SHARE
#include "ob_ls_creator.h"
#include "share/ob_rpc_struct.h" //ObLSCreatorArg, ObSetMemberListArgV2
#include "share/ls/ob_ls_status_operator.h" //ObLSStatusOperator
#include "share/ls/ob_ls_operator.h" //ObLSHistoryOperator
#include "share/ls/ob_ls_table.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ls/ob_ls_info.h"
#include "share/ob_tenant_info_proxy.h"
#include "rootserver/ob_root_utils.h" //majority
#include "share/ob_unit_table_operator.h" //ObUnitTableOperator
#include "logservice/leader_coordinator/table_accessor.h"
#include "logservice/palf/palf_base_info.h"//palf::PalfBaseInfo
#include "share/scn.h"
#include "share/ls/ob_ls_life_manager.h"
#include "rootserver/ob_root_utils.h"//notify_switch_leader
#ifdef OB_BUILD_ARBITRATION
#include "share/arbitration_service/ob_arbitration_service_info.h" // for ObArbitrationServiceInfo
#include "share/arbitration_service/ob_arbitration_service_table_operator.h" // for ObArbitrationServiceTableOperator
#endif
#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"
#include "share/restore/ob_tenant_clone_table_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::palf;

namespace oceanbase
{
namespace share
{
////ObLSReplicaAddr
int ObLSReplicaAddr::init(const common::ObAddr &addr,
           const common::ObReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid()
                  || common::REPLICA_TYPE_MAX == replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(addr), K(replica_type));
  } else {
    addr_ = addr;
    replica_type_ = replica_type;
  }

  return ret;
}


/////////////////////////
bool ObLSCreator::is_valid()
{
  bool bret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || !id_.is_valid()) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "tenant id or log stream id is invalid", K(bret), K_(tenant_id), K_(id));
  }
  return bret;
}

int ObLSCreator::create_sys_tenant_ls(
    const obrpc::ObServerInfoList &rs_list,
    const common::ObIArray<share::ObUnit> &unit_array)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(0 >= rs_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs list is invalid", KR(ret), K(rs_list));
  } else if (OB_UNLIKELY(rs_list.count() != unit_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(rs_list), K(unit_array));
  } else if (OB_FAIL(tenant_info.init(OB_SYS_TENANT_ID, share::PRIMARY_TENANT_ROLE))) {
    LOG_WARN("tenant info init failed", KR(ret));
  } else {
    ObLSAddr addr;
    common::ObMemberList member_list;
    const int64_t paxos_replica_num = rs_list.count();
    ObLSReplicaAddr replica_addr;
    const common::ObReplicaProperty replica_property;
    const common::ObReplicaType replica_type = common::REPLICA_TYPE_FULL;
    const common::ObCompatibilityMode compat_mode = MYSQL_MODE;
    const SCN create_scn = SCN::base_scn();//SYS_LS no need create_scn
    palf::PalfBaseInfo palf_base_info;
    common::ObMember arbitration_service;
    common::GlobalLearnerList learner_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list.count(); ++i) {
      replica_addr.reset();
      if (rs_list.at(i).zone_ != unit_array.at(i).zone_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone not match", KR(ret), K(rs_list), K(unit_array));
      } else if (OB_FAIL(replica_addr.init(
              rs_list[i].server_,
              replica_type))) {
        LOG_WARN("failed to init replica addr", KR(ret), K(i), K(rs_list), K(replica_type),
                 K(replica_property), K(unit_array));
      } else if (OB_FAIL(addr.push_back(replica_addr))) {
        LOG_WARN("failed to push back replica addr", KR(ret), K(i), K(addr),
            K(replica_addr), K(rs_list));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_ls_(addr, paxos_replica_num, tenant_info,
            create_scn, compat_mode, false/*create_with_palf*/, palf_base_info,
            member_list, arbitration_service, learner_list))) {
      LOG_WARN("failed to create log stream", KR(ret), K_(id), K_(tenant_id),
                                              K(addr), K(paxos_replica_num), K(tenant_info),
                                              K(create_scn), K(compat_mode), K(palf_base_info));
    } else if (OB_FAIL(set_member_list_(member_list, arbitration_service, paxos_replica_num, learner_list))) {
      LOG_WARN("failed to set member list", KR(ret), K(member_list), K(arbitration_service), K(paxos_replica_num));
    }
  }
  return ret;
}

#define REPEAT_CREATE_LS()                     \
  do {                                                                         \
    if (OB_FAIL(ret)) {                        \
    } else if (0 >= member_list.get_member_number()) {                       \
      if (OB_FAIL(do_create_ls_(addr, arbitration_service, status_info, paxos_replica_num, \
              create_scn, compat_mode, member_list, create_with_palf, palf_base_info, learner_list))) {         \
        LOG_WARN("failed to create log stream", KR(ret), K_(id),             \
            K_(tenant_id), K(addr), K(paxos_replica_num),               \
            K(status_info), K(create_scn), K(palf_base_info));                  \
      }                                                                      \
    }                                                                        \
    if (FAILEDx(process_after_has_member_list_(member_list, arbitration_service,   \
            paxos_replica_num, learner_list))) {        \
      LOG_WARN("failed to process after has member list", KR(ret),           \
          K(member_list), K(paxos_replica_num));                        \
    }                                                                        \
  } while(0)

int ObLSCreator::create_user_ls(
    const share::ObLSStatusInfo &status_info,
    const int64_t paxos_replica_num,
    const share::schema::ZoneLocalityIArray &zone_locality,
    const SCN &create_scn,
    const common::ObCompatibilityMode &compat_mode,
    const bool create_with_palf,
    const palf::PalfBaseInfo &palf_base_info,
    const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time(); 
  LOG_INFO("start to create log stream", K_(id), K_(tenant_id));
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(!status_info.is_valid()
                         || !id_.is_user_ls()
                         || 0 >= zone_locality.count()
                         || 0 >= paxos_replica_num
                         || !create_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(status_info), K_(id), K(zone_locality),
             K(paxos_replica_num), K(create_scn), K(palf_base_info));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObLSAddr addr;
    common::ObMemberList member_list;
    share::ObLSStatusInfo exist_status_info;
    share::ObLSStatusOperator ls_operator;
    ObMember arbitration_service;
    common::GlobalLearnerList learner_list;
    if (OB_INVALID_TENANT_ID != source_tenant_id) {
      // for clone tenant
      if (OB_FAIL(construct_clone_tenant_ls_addrs_(source_tenant_id, addr))) {
        LOG_WARN("fail to construct locations for clone tenant log stream", KR(ret),
                                          K(source_tenant_id), K_(tenant_id), K_(id));
      }
    } else {
      if (status_info.is_duplicate_ls()) {
        if (OB_FAIL(alloc_duplicate_ls_addr_(tenant_id_, zone_locality, addr))) {
          LOG_WARN("failed to alloc duplicate ls addr", KR(ret), K_(tenant_id));
        } else {
          LOG_INFO("finish alloc duplicate ls addr", K_(tenant_id), K(addr));
        }
      } else if (OB_FAIL(alloc_user_ls_addr(tenant_id_, status_info.unit_group_id_,
                                             zone_locality, addr))) {
        LOG_WARN("failed to alloc user ls addr", KR(ret), K(tenant_id_), K(status_info));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls_operator.get_ls_init_member_list(tenant_id_, id_, member_list,
            exist_status_info, *proxy_, arbitration_service, learner_list))) {
      LOG_WARN("failed to get ls init member list", KR(ret), K(tenant_id_), K(id_));
    } else if (status_info.ls_is_created()) {
    } else if (status_info.ls_group_id_ != exist_status_info.ls_group_id_
        || status_info.unit_group_id_ != exist_status_info.unit_group_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exist status not equal to status", KR(ret),
                 K(exist_status_info), K(status_info));
    } else {
      REPEAT_CREATE_LS();
    }
  }
  const int64_t cost = ObTimeUtility::current_time() - start_time;
  LOG_INFO("finish to create log stream", KR(ret), K_(id), K_(tenant_id), K(cost));
  LS_EVENT_ADD(tenant_id_, id_, "create_ls_finish", ret, paxos_replica_num, "", K(cost));
  return ret;
}

int ObLSCreator::create_tenant_sys_ls(
    const common::ObZone &primary_zone,
    const share::schema::ZoneLocalityIArray &zone_locality,
    const ObIArray<share::ObResourcePoolName> &pool_list,
    const int64_t paxos_replica_num,
    const common::ObCompatibilityMode &compat_mode,
    const ObString &zone_priority,
    const bool create_with_palf,
    const palf::PalfBaseInfo &palf_base_info,
    const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to create log stream", K_(id), K_(tenant_id));
  const int64_t start_time = ObTimeUtility::current_time();
  share::ObLSStatusInfo status_info;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(primary_zone.is_empty()
                         || 0 >= zone_locality.count()
                         || 0 >= paxos_replica_num
                         || 0 >= pool_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone), K(zone_locality),
             K(paxos_replica_num), K(pool_list), K(palf_base_info));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObLSAddr addr;
    common::ObMemberList member_list;
    share::ObLSStatusInfo exist_status_info;
    const SCN create_scn = SCN::base_scn();
    share::ObLSStatusOperator ls_operator;
    ObMember arbitration_service;
    common::GlobalLearnerList learner_list;
    ObLSFlag flag(ObLSFlag::NORMAL_FLAG); // TODO: sys ls should be duplicate
    if (OB_FAIL(status_info.init(tenant_id_, id_, 0, share::OB_LS_CREATING, 0,
                                   primary_zone, flag))) {
      LOG_WARN("failed to init ls info", KR(ret), K(id_), K(primary_zone),
          K(tenant_id_), K(flag));
    } else if (OB_INVALID_TENANT_ID != source_tenant_id) {
      if (OB_FAIL(construct_clone_tenant_ls_addrs_(source_tenant_id, addr))) {
        LOG_WARN("failed to alloc clone tenant ls addr", KR(ret),
                      K(source_tenant_id), K(tenant_id_), K(addr), K(source_tenant_id));
      }
    } else if (OB_FAIL(alloc_sys_ls_addr(tenant_id_, pool_list,
            zone_locality, addr))) {
      LOG_WARN("failed to alloc user ls addr", KR(ret), K(tenant_id_), K(pool_list));
    }
    if (OB_FAIL(ret)) {
    } else {
      ret = ls_operator.get_ls_init_member_list(tenant_id_, id_, member_list, exist_status_info, *proxy_, arbitration_service, learner_list);
      if (OB_FAIL(ret) && OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get log stream member list", KR(ret), K_(id), K(tenant_id_));
      } else if (OB_SUCC(ret) && status_info.ls_is_created()) {
      } else {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          share::ObLSLifeAgentManager ls_life_agent(*proxy_);
          if (OB_FAIL(ls_life_agent.create_new_ls(status_info, create_scn, zone_priority,
                                                  share::NORMAL_SWITCHOVER_STATUS))) {
            LOG_WARN("failed to create new ls", KR(ret), K(status_info), K(create_scn), K(zone_priority));
          }
        }
        if (OB_SUCC(ret)) {
          REPEAT_CREATE_LS();
        }
      }
    }
  }

  const int64_t cost = ObTimeUtility::current_time() - start_time;
  LOG_INFO("finish to create log stream", KR(ret), K_(id), K_(tenant_id), K(cost));
  LS_EVENT_ADD(tenant_id_, id_, "create_ls_finish", ret, paxos_replica_num, "", K(cost));
  return ret;
}

int ObLSCreator::construct_clone_tenant_ls_addrs_(const uint64_t source_tenant_id,
                                                  ObLSAddr &ls_addrs)
{
  int ret = OB_SUCCESS;
  ls_addrs.reset();
  ObLSReplicaAddr replica_addr;
  const common::ObReplicaType replica_type = ObReplicaType::REPLICA_TYPE_FULL;
  ObTenantCloneTableOperator clone_op;
  ObTenantSnapshotTableOperator snap_op;
  ObCloneJob clone_job;
  ObArray<ObTenantSnapLSReplicaSimpleItem> simple_items;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K_(id));
  } else {
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      if (OB_FAIL(clone_op.init(tenant_id_, proxy_))) {
        LOG_WARN("fail to init clone op", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(clone_op.get_clone_job_by_source_tenant_id(source_tenant_id, clone_job))) {
        LOG_WARN("fail to get clone job", KR(ret), K(tenant_id_), K(source_tenant_id));
      } else if (OB_UNLIKELY(!clone_job.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("clone job is not valid", KR(ret), K(clone_job), K(source_tenant_id));
      } else if (OB_FAIL(snap_op.init(source_tenant_id, proxy_))) {
        LOG_WARN("failed to init snap op", KR(ret), K(source_tenant_id));
      } else if (OB_FAIL(snap_op.get_tenant_snap_ls_replica_simple_items(
              clone_job.get_tenant_snapshot_id(), id_, ObLSSnapStatus::NORMAL, simple_items))) {
        LOG_WARN("failed to get ls replica simple items", KR(ret), K(clone_job), K(id_));
      } else if (OB_UNLIKELY(simple_items.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("simple_items is empty", KR(ret), K(simple_items), K(clone_job), K(id_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_N(simple_items, i, cnt) {
      replica_addr.reset();
      const ObAddr &addr = simple_items.at(i).get_addr();
      if (OB_FAIL(replica_addr.init(addr, replica_type))) {
        LOG_WARN("fail to construct replica addr", KR(ret), K(addr), K(replica_type));
      } else if (OB_FAIL(ls_addrs.push_back(replica_addr))) {
        LOG_WARN("fail to add PbLSReplicaAddr to array", KR(ret), K(replica_addr));
      }
    }
  }
  return ret;
}

int ObLSCreator::do_create_ls_(const ObLSAddr &addr,
                              ObMember &arbitration_service,
                              const share::ObLSStatusInfo &info,
                              const int64_t paxos_replica_num,
                              const SCN &create_scn,
                              const common::ObCompatibilityMode &compat_mode,
                              ObMemberList &member_list,
                              const bool create_with_palf,
                              const palf::PalfBaseInfo &palf_base_info,
                              common::GlobalLearnerList &learner_list)
{
 int ret = OB_SUCCESS;
 ObAllTenantInfo tenant_info;
 if (OB_UNLIKELY(!is_valid())) {
   ret = OB_INVALID_ARGUMENT;
   LOG_WARN("invalid argument", KR(ret));
 } else if (OB_UNLIKELY(0 >= addr.count() || 0 >= paxos_replica_num ||
                        !info.is_valid()
                        || !create_scn.is_valid())) {
   ret = OB_INVALID_ARGUMENT;
   LOG_WARN("invalid argument", KR(ret), K(paxos_replica_num), K(info),
            K(addr), K(create_scn));
 } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, proxy_, false, tenant_info))) {
   LOG_WARN("failed to load tenant info", KR(ret), K_(tenant_id));
 } else if (OB_FAIL(create_ls_(addr, paxos_replica_num, tenant_info, create_scn,
                               compat_mode, create_with_palf, palf_base_info, member_list, arbitration_service, learner_list))) {
   LOG_WARN("failed to create log stream", KR(ret), K_(id), K_(tenant_id), K(create_with_palf),
            K(addr), K(paxos_replica_num), K(tenant_info), K(create_scn), K(compat_mode), K(palf_base_info), K(learner_list));
 } else if (OB_FAIL(persist_ls_member_list_(member_list, arbitration_service, learner_list))) {
   LOG_WARN("failed to persist log stream member list", KR(ret),
            K(member_list), K(arbitration_service), K(learner_list));
 }
  return ret;
}

int ObLSCreator::process_after_has_member_list_(
    const common::ObMemberList &member_list,
    const common::ObMember &arbitration_service,
    const int64_t paxos_replica_num,
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(set_member_list_(member_list, arbitration_service, paxos_replica_num, learner_list))) {
    LOG_WARN("failed to set member list", KR(ret), K_(id), K_(tenant_id),
        K(member_list), K(arbitration_service), K(paxos_replica_num), K(learner_list));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    //create end
    DEBUG_SYNC(BEFORE_PROCESS_AFTER_HAS_MEMBER_LIST);
    share::ObLSStatusOperator ls_operator;
    if (OB_FAIL(ls_operator.update_ls_status(
            tenant_id_, id_, share::OB_LS_CREATING, share::OB_LS_CREATED,
            share::NORMAL_SWITCHOVER_STATUS, *proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(id_));
    } else if (id_.is_sys_ls()) {
      if (OB_FAIL(ls_operator.update_ls_status(
                  tenant_id_, id_, share::OB_LS_CREATED, share::OB_LS_NORMAL,
                  share::NORMAL_SWITCHOVER_STATUS, *proxy_))) {
        LOG_WARN("failed to update ls status", KR(ret), K(id_));
      }
    }
  }
  return ret;
}

int ObLSCreator::create_ls_(const ObILSAddr &addrs,
                           const int64_t paxos_replica_num,
                           const share::ObAllTenantInfo &tenant_info,
                           const SCN &create_scn,
                           const common::ObCompatibilityMode &compat_mode,
                           const bool create_with_palf,
                           const palf::PalfBaseInfo &palf_base_info,
                           common::ObMemberList &member_list,
                           common::ObMember &arbitration_service,
                           common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  bool need_create_arb_replica = false;
  int64_t arb_replica_count = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(0 >= addrs.count()
                         || 0 >= paxos_replica_num
                         || rootserver::majority(paxos_replica_num) > addrs.count()
                         || !tenant_info.is_valid()
                         || !create_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(addrs), K(paxos_replica_num), K(tenant_info),
        K(create_scn));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else {
      obrpc::ObCreateLSArg arg;
      int tmp_ret = OB_SUCCESS;
      ObArray<int> return_code_array;
      const common::ObReplicaProperty replica_property;
      lib::Worker::CompatMode new_compat_mode = compat_mode == ORACLE_MODE ?
                                         lib::Worker::CompatMode::ORACLE :
                                         lib::Worker::CompatMode::MYSQL;

      for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); ++i) {
        arg.reset();
        const ObLSReplicaAddr &addr = addrs.at(i);
        if (OB_FAIL(arg.init(tenant_id_, id_, addr.replica_type_,
                replica_property, tenant_info, create_scn, new_compat_mode,
                create_with_palf, palf_base_info))) {
          LOG_WARN("failed to init create log stream arg", KR(ret), K(addr), K(create_with_palf), K(replica_property),
              K_(id), K_(tenant_id), K(tenant_info), K(create_scn), K(new_compat_mode), K(palf_base_info));
        } else if (OB_TMP_FAIL(create_ls_proxy_.call(addr.addr_, ctx.get_timeout(),
                GCONF.cluster_id, tenant_id_, arg))) {
          LOG_WARN("failed to all async rpc", KR(tmp_ret), K(addr), K(ctx.get_timeout()),
              K(arg), K(tenant_id_));
        }
      }
#ifdef OB_BUILD_ARBITRATION
      // try to create A-replica if needed
      // (1) ignore any erros
      //     arb replica task will generated by back groud process later to add A-replica
      ObAddr arbitration_service_addr;
      if (OB_FAIL(ret)) {
      } else if (OB_SUCCESS != (tmp_ret = check_need_create_arb_replica_(
                                              need_create_arb_replica,
                                              arbitration_service_addr))) {
        LOG_WARN("fail to check need create arb replica", KR(tmp_ret));
      } else if (!need_create_arb_replica) {
        // do nothing
        LOG_INFO("no need to create A-replica for this log stream", K_(tenant_id), K_(id));
      } else if (OB_SUCCESS != (tmp_ret = try_create_arbitration_service_replica_(
                                              tenant_info.get_tenant_role(),
                                              arbitration_service_addr))) {
        LOG_WARN("fail to create arbitration service replica", KR(tmp_ret), K(tenant_info));
      } else {
        int64_t timestamp = 1;
        arbitration_service = ObMember(arbitration_service_addr, timestamp);
        arb_replica_count = 1;
      }
#endif
      //wait all
      if (OB_TMP_FAIL(create_ls_proxy_.wait_all(return_code_array))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to wait all async rpc", KR(ret), KR(tmp_ret));
      }
      if (FAILEDx(check_create_ls_result_(paxos_replica_num, return_code_array, member_list, learner_list,
                                          need_create_arb_replica, arb_replica_count))) {
        LOG_WARN("failed to check ls result", KR(ret), K(paxos_replica_num), K(return_code_array), K(learner_list),
                 K(need_create_arb_replica), K(arb_replica_count));
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int ObLSCreator::check_need_create_arb_replica_(
    bool &need_create_arb_replica,
    ObAddr &arbitration_service)
{
  int ret = OB_SUCCESS;
  need_create_arb_replica = false;
  ObSqlString sql;
  const uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
  const ObString arbitration_service_key = "default";
  const bool lock_line = false;
  ObArbitrationServiceInfo arbitration_service_info;
  ObArbitrationServiceTableOperator arbitration_service_table_operator;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K_(id));
  } else if (OB_ISNULL(proxy_)
             || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(proxy_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(sql.assign_fmt("SELECT arbitration_service_status IN ('ENABLING', 'ENABLED') AS is_enabling "
                                    "FROM %s WHERE tenant_id = %ld",
                                    OB_ALL_TENANT_TNAME, tenant_id_))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id));
  } else {
    int64_t is_enabling = 0;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(GCTX.sql_proxy_->read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get sql result failed", KR(ret), "sql", sql.ptr());
      } else if (OB_FAIL(result.get_result()->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_TENANT_NOT_EXIST;
        }
        LOG_WARN("get result next failed", KR(ret), "sql", sql.ptr());
      } else if (OB_FAIL(result.get_result()->get_int(0L, is_enabling))) {
        if (OB_ERR_COLUMN_NOT_FOUND != ret) {
          LOG_WARN("get arbitration status failed", KR(ret), "sql", sql.ptr());
        } else {
          // ignore column not found
          ret = OB_SUCCESS;
        }
      } else {
        need_create_arb_replica = (1 == is_enabling);
      }
    }
  }

  // no need to lock __all_arbitration_service line because:
  // (1) ls status is setted in creating status BEFORE get arbitration status from __all_tenant
  // (2) there is no ls in creating status BEFORE arbitration service is removed
  // after get a enabling status in __all_tenant means
  // any remove operations later could see ls in creating status in __all_ls_status, thus disallowing remove it
  if (OB_FAIL(ret) || !need_create_arb_replica) {
  } else if (OB_FAIL(arbitration_service_table_operator.get(
                         *proxy_,
                         arbitration_service_key,
                         lock_line,
                         arbitration_service_info))) {
    LOG_WARN("fail to get arbitration service info", KR(ret), K(arbitration_service_key), K(lock_line));
  } else if (OB_FAIL(arbitration_service_info.get_arbitration_service_addr(arbitration_service))) {
    LOG_WARN("fail to get arbitration service addr", KR(ret), K(arbitration_service_info));
  }
  return ret;
}

int ObLSCreator::try_create_arbitration_service_replica_(
    const ObTenantRole &tenant_role,
    const ObAddr &arbitration_service)
{
  int ret = OB_SUCCESS;
  ObCreateArbArg create_arb_arg;
  ObCreateArbResult result;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (!tenant_role.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant role is invalid", KR(ret), K_(tenant_id), K_(id), K(tenant_role));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(create_arb_arg.init(tenant_id_, id_, tenant_role))) {
    LOG_WARN("fail to init ObCreateArbArg", KR(ret), K_(tenant_id), K_(id), K(tenant_role));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(arbitration_service).timeout(ctx.get_timeout()).create_arb(create_arb_arg, result))) {
    // do nothing, let arb service add replica
    LOG_WARN("fail to create arbitration service replica", KR(ret), K(arbitration_service), "timeout", ctx.get_timeout(), K(create_arb_arg));
  }
  return ret;
}
#endif

int ObLSCreator::check_create_ls_result_(
    const int64_t paxos_replica_num,
    const ObIArray<int> &return_code_array,
    common::ObMemberList &member_list,
    common::GlobalLearnerList &learner_list,
    const bool with_arbitration_service,
    const int64_t arb_replica_num)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  learner_list.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (return_code_array.count() != create_ls_proxy_.get_results().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc count not equal to result count", KR(ret),
             K(return_code_array.count()), K(create_ls_proxy_.get_results().count()));
  } else {
    const int64_t timestamp = 1;
    // don't use arg/dest here because call() may has failure.
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
      if (OB_SUCCESS != return_code_array.at(i)) {
        LOG_WARN("rpc is failed", KR(ret), K(return_code_array.at(i)), K(i));
      } else {
        const auto *result = create_ls_proxy_.get_results().at(i);
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i));
        } else if (OB_SUCCESS != result->get_result()) {
          LOG_WARN("rpc is failed", KR(ret), K(*result), K(i));
        } else {
          ObAddr addr;
          if (result->get_addr().is_valid()) {
            addr = result->get_addr();
          } else if (create_ls_proxy_.get_dests().count() == create_ls_proxy_.get_results().count()) {
            //one by one match
            addr = create_ls_proxy_.get_dests().at(i);
          }
          //TODO other replica type
          //can not get replica type from arg, arg and result is not match
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(!addr.is_valid())) {
            ret = OB_NEED_RETRY;
            LOG_WARN("addr is invalid, ls create failed", KR(ret), K(addr));
          } else if (result->get_replica_type() == REPLICA_TYPE_FULL) {
            if (OB_FAIL(member_list.add_member(ObMember(addr, timestamp)))) {
              LOG_WARN("failed to add member", KR(ret), K(addr));
            }
          } else if (result->get_replica_type() == REPLICA_TYPE_READONLY) {
            if (OB_FAIL(learner_list.add_learner(ObMember(addr, timestamp)))) {
              LOG_WARN("failed to add member", KR(ret), K(addr));
            }
          }
          LOG_TRACE("create ls result", KR(ret), K(i), K(addr), KPC(result));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!id_.is_sys_ls() && with_arbitration_service) {
      if (rootserver::majority(paxos_replica_num/*F-replica*/ + 1/*A-replica*/) > member_list.get_member_number() + arb_replica_num) {
        ret = OB_REPLICA_NUM_NOT_ENOUGH;
        LOG_WARN("success count less than majority with arb-replica", KR(ret), K(paxos_replica_num),
                 K(member_list), K(with_arbitration_service), K(arb_replica_num));
      }
    } else {
      // for sys log stream and non-arb user log stream
      if (rootserver::majority(paxos_replica_num) > member_list.get_member_number()) {
        ret = OB_REPLICA_NUM_NOT_ENOUGH;
        LOG_WARN("success count less than majority", KR(ret), K(paxos_replica_num), K(member_list));
      }
    }
    LS_EVENT_ADD(tenant_id_, id_, "create_ls", ret, paxos_replica_num, member_list);
  }
  return ret;
}

int ObLSCreator::persist_ls_member_list_(const common::ObMemberList &member_list,
                                         const ObMember &arb_member,
                                         const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_SET_LS_MEMBER_LIST);
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(!member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("member list is invalid", KR(ret), K(member_list));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    share::ObLSStatusOperator ls_operator;
    if (OB_FAIL(ls_operator.update_init_member_list(tenant_id_, id_, member_list, *proxy_, arb_member, learner_list))) {
      LOG_WARN("failed to insert ls", KR(ret), K(member_list), K(arb_member), K(learner_list));
    }
  }
  return ret;

}

ERRSIM_POINT_DEF(ERRSIM_CHECK_MEMBER_LIST_SAME_ERROR);
int ObLSCreator::inner_check_member_list_and_learner_list_(
    const common::ObMemberList &member_list,
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  ObLSInfo ls_info_to_check;
  if (OB_UNLIKELY(ERRSIM_CHECK_MEMBER_LIST_SAME_ERROR)) {
    ret = ERRSIM_CHECK_MEMBER_LIST_SAME_ERROR;
  } else if (OB_ISNULL(GCTX.lst_operator_)
             || OB_UNLIKELY(!is_valid() || !member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list));
  } else if (OB_FAIL(GCTX.lst_operator_->get(
                         GCONF.cluster_id, tenant_id_, id_,
                         share::ObLSTable::DEFAULT_MODE, ls_info_to_check))) {
    LOG_WARN("fail to get ls info", KR(ret), K_(tenant_id), K_(id));
  } else {
    // check member_list all reported in __all_ls_meta_table
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      const share::ObLSReplica *replica = nullptr;
      common::ObAddr server;
      if (OB_FAIL(member_list.get_server_by_index(i, server))) {
        LOG_WARN("fail to get server by index", KR(ret), K(i), K(member_list));
      } else {
        int tmp_ret = ls_info_to_check.find(server, replica);
        if (OB_SUCCESS == tmp_ret) {
          // good, replica exists, bypass
        } else {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("has replica only in member list, need try again", KR(ret), KR(tmp_ret),
                   K(member_list), K(ls_info_to_check), K(i), K(server));
        }
      }
    }
    // check learner_list all reported in __all_ls_meta_table
    for (int64_t i = 0; OB_SUCC(ret) && i < learner_list.get_member_number(); ++i) {
      const share::ObLSReplica *replica = nullptr;
      common::ObAddr server;
      if (OB_FAIL(learner_list.get_server_by_index(i, server))) {
        LOG_WARN("fail to get server by index", KR(ret), K(i), K(learner_list));
      } else {
        int tmp_ret = ls_info_to_check.find(server, replica);
        if (OB_SUCCESS == tmp_ret) {
          // replica exists, bypass
        } else {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("has replica only in learner list, need try again", KR(ret), KR(tmp_ret),
                   K(learner_list), K(ls_info_to_check), K(i), K(server));
        }
      }
    }
  }
  return ret;
}

int ObLSCreator::check_member_list_and_learner_list_all_in_meta_table_(
    const common::ObMemberList &member_list,
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval_us = 1000l * 1000l; // 1s
  ObTimeoutCtx ctx;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid() || !member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait member list and learner list all reported to meta table timeout",
                 KR(ret), K(member_list), K(learner_list), K_(tenant_id), K_(id));
      } else if (OB_SUCCESS != (tmp_ret = inner_check_member_list_and_learner_list_(
                                     member_list, learner_list))) {
        LOG_WARN("fail to check member list and learner list all reported", KR(tmp_ret),
                 K_(tenant_id), K_(id), K(member_list), K(learner_list));
        // has replica only in member_list or learner_list, need try again later
        ob_usleep(retry_interval_us);
      } else {
        // good, all replicas in member_list and learner_list has already reported
        break;
      }
    }
  }
  return ret;
}

int ObLSCreator::construct_paxos_replica_number_to_persist_(
    const int64_t paxos_replica_num,
    const int64_t arb_replica_num,
    const common::ObMemberList &member_list,
    int64_t &paxos_replica_number_to_persist)
{
  int ret = OB_SUCCESS;
  paxos_replica_number_to_persist = 0;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(!member_list.is_valid())
             || OB_UNLIKELY(0 >= paxos_replica_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list), K(paxos_replica_num));
  } else if (!id_.is_user_ls()) {
    // for sys log stream
    if (member_list.get_member_number() >= rootserver::majority(paxos_replica_num)) {
      // good, majority of F-replica created successfully, set paxos_replica_num equal to locality
      paxos_replica_number_to_persist = paxos_replica_num;
    } else {
      // sys log stream needs majority of F-replicas created success
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("majority is not satisfied", KR(ret), K_(tenant_id), K_(id),
               K(member_list), K(paxos_replica_num), K(arb_replica_num));
    }
  } else {
    // for user log stream
    if (member_list.get_member_number() >= rootserver::majority(paxos_replica_num)) {
      // good, majority of F-replica created successfully, set paxos_replica_num equal to locality
      paxos_replica_number_to_persist = paxos_replica_num;
#ifdef OB_BUILD_ARBITRATION
    } else if (member_list.get_member_number() + arb_replica_num >= rootserver::majority(paxos_replica_num)) {
      // check majority with arb-replica for user log stream
      if (OB_UNLIKELY(1 != arb_replica_num)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("arb replica number must equals to 1 after check of majority with arb_replica_num",
                 KR(ret), K(paxos_replica_num), K(arb_replica_num), K(member_list));
      } else if (paxos_replica_num != 2 && paxos_replica_num != 4) {
        // with arb-replica, paxos_replica_number in locality should be 2F1A or 4F1A
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("tenant with arbitration service should has paxos_replica_num is 2 or 4", KR(ret), K(paxos_replica_num));
      } else {
        // when tenant's locality is 2F1A, but only 1F1A created successfully, paxos_replica_number should set 1
        // when tenant's locality is 4F1A, but only 2F1A created successfully, paxos_replica_number should set 3
        paxos_replica_number_to_persist = paxos_replica_num - 1;
      }
#endif
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("majority is not satisfied", KR(ret), K_(tenant_id), K_(id),
               K(member_list), K(paxos_replica_num), K(arb_replica_num));
    }
  }
  return ret;
}

int ObLSCreator::set_member_list_(const common::ObMemberList &member_list,
                                  const common::ObMember &arb_replica,
                                  const int64_t paxos_replica_num,
                                  const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObAddr> server_list;
  int tmp_ret = OB_SUCCESS;
  int64_t paxos_replica_number_to_persist = 0;
  int64_t arb_replica_count = arb_replica.is_valid() ? 1 : 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(!member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list));
  } else if (!is_sys_tenant(tenant_id_) && OB_FAIL(check_member_list_and_learner_list_all_in_meta_table_(member_list, learner_list))) {
    LOG_WARN("fail to check member_list all in meta table", KR(ret), K(member_list), K(learner_list), K_(tenant_id), K_(id));
  } else if (OB_FAIL(construct_paxos_replica_number_to_persist_(
                         paxos_replica_num,
                         arb_replica_count,
                         member_list,
                         paxos_replica_number_to_persist))) {
    LOG_WARN("fail to construct paxos replica number to set", KR(ret), K_(tenant_id), K_(id),
             K(paxos_replica_num), K(arb_replica_count), K(member_list));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else {
      ObArray<int> return_code_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
        ObAddr addr;
        ObSetMemberListArgV2 arg;
        if (OB_FAIL(arg.init(tenant_id_, id_, paxos_replica_number_to_persist, member_list, arb_replica, learner_list))) {
          LOG_WARN("failed to init set member list arg", KR(ret), K_(id), K_(tenant_id),
              K(paxos_replica_number_to_persist), K(member_list), K(arb_replica), K(learner_list));
        } else if (OB_FAIL(member_list.get_server_by_index(i, addr))) {
          LOG_WARN("failed to get member by index", KR(ret), K(i), K(member_list));
        } else if (OB_TMP_FAIL(set_member_list_proxy_.call(addr, ctx.get_timeout(),
                GCONF.cluster_id, tenant_id_, arg))) {
          LOG_WARN("failed to set member list", KR(tmp_ret), K(ctx.get_timeout()), K(arg),
              K(tenant_id_));
        } else if (OB_FAIL(server_list.push_back(addr))) {
          LOG_WARN("failed to push back server list", KR(ret), K(addr));
        }
      }

      if (OB_TMP_FAIL(set_member_list_proxy_.wait_all(return_code_array))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to wait all async rpc", KR(ret), KR(tmp_ret));

      }
      if (FAILEDx(check_set_memberlist_result_(return_code_array, paxos_replica_number_to_persist))) {
        LOG_WARN("failed to check set member liset result", KR(ret),
            K(paxos_replica_num), K(return_code_array));
      }
    }
  }
  if (OB_SUCC(ret)) {
    obrpc::ObNotifySwitchLeaderArg arg;
    if (OB_FAIL(arg.init(tenant_id_, id_, ObAddr(),
            obrpc::ObNotifySwitchLeaderArg::CREATE_LS))) {
      LOG_WARN("failed to init arg", KR(ret), K(tenant_id_), K(id_));
    } else if (OB_TMP_FAIL(rootserver::ObRootUtils::notify_switch_leader(
            GCTX.srv_rpc_proxy_, tenant_id_, arg, server_list))) {
      LOG_WARN("failed to notiry switch leader", KR(ret), KR(tmp_ret), K(tenant_id_), K(arg), K(server_list));
    }
  }
  return ret;
}

int ObLSCreator::check_set_memberlist_result_(
    const ObIArray<int> &return_code_array,
    const int64_t paxos_replica_num)
{
  int ret = OB_SUCCESS;
  int64_t success_cnt = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (return_code_array.count() != set_member_list_proxy_.get_results().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc count not equal to result count", KR(ret),
        K(return_code_array.count()), K(set_member_list_proxy_.get_results().count()));
  } else {
    // don't use arg/dest here because call() may has failure.
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
      if (OB_SUCCESS != return_code_array.at(i)) {
        LOG_WARN("rpc is failed", KR(ret), K(return_code_array.at(i)), K(i));
      } else {
        const auto *result = set_member_list_proxy_.get_results().at(i);
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i));
        } else if (OB_SUCCESS != result->get_result()) {
          LOG_WARN("rpc is failed", KR(ret), K(*result), K(i));
        } else {
          success_cnt++;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (rootserver::majority(paxos_replica_num) > success_cnt) {
      ret = OB_REPLICA_NUM_NOT_ENOUGH;
      LOG_WARN("success count less than majority", KR(ret), K(success_cnt),
               K(paxos_replica_num));
    }
  }
  LS_EVENT_ADD(tenant_id_, id_, "set_ls_member_list", ret, paxos_replica_num, success_cnt);
  return ret;
}

// OceanBase 4.0 interface
int ObLSCreator::construct_ls_addrs_according_to_locality_(
    const share::schema::ZoneLocalityIArray &zone_locality_array,
    const common::ObIArray<share::ObUnit> &unit_info_array,
    const bool is_sys_ls,
    const bool is_duplicate_ls,
    ObILSAddr &ls_addr)
{
  int ret = OB_SUCCESS;
  ls_addr.reset();
  if (OB_UNLIKELY(0 >= zone_locality_array.count())
      || OB_UNLIKELY(0 >= unit_info_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_locality_array), K(unit_info_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality_array.count(); ++i) {
      const share::ObZoneReplicaAttrSet &zone_locality = zone_locality_array.at(i);
      ObLSReplicaAddr replica_addr;
      if (OB_FAIL(alloc_zone_ls_addr(is_sys_ls, zone_locality, unit_info_array, replica_addr))) {
        LOG_WARN("fail to alloc zone ls addr", KR(ret), K(zone_locality), K(unit_info_array));
      } else if (OB_FAIL(ls_addr.push_back(replica_addr))) {
        LOG_WARN("fail to push back", KR(ret), K(replica_addr));
      } else if (is_duplicate_ls
                 && OB_FAIL(compensate_zone_readonly_replica_(
                                zone_locality,
                                replica_addr,
                                unit_info_array,
                                ls_addr))) {
        LOG_WARN("fail to compensate readonly replica", KR(ret), K(zone_locality),
                 K(replica_addr), K(ls_addr));
      }
    }
  }
  return ret;
}

int ObLSCreator::alloc_sys_ls_addr(
    const uint64_t tenant_id,
    const ObIArray<share::ObResourcePoolName> &pools,
    const share::schema::ZoneLocalityIArray &zone_locality_array,
    ObILSAddr &ls_addr)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit> unit_info_array;
  ObUnitTableOperator unit_operator;
  ls_addr.reset();

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || pools.count() <= 0
                         || zone_locality_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pools), K(zone_locality_array));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy ptr is null", KR(ret));
  } else if (OB_FAIL(unit_operator.init(*proxy_))) {
    LOG_WARN("unit operator init failed", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_resource_pools(pools, unit_info_array))) {
    LOG_WARN("fail to get unit infos", KR(ret), K(pools));
  } else if (OB_FAIL(construct_ls_addrs_according_to_locality_(
                         zone_locality_array,
                         unit_info_array,
                         true/*is_sys_ls*/,
                         false/*is_duplicate_ls*/,
                         ls_addr))) {
    LOG_WARN("fail to construct ls addrs for tenant sys ls", KR(ret), K(zone_locality_array),
             K(unit_info_array), K(ls_addr));
  }
  return ret;
}

int ObLSCreator::alloc_user_ls_addr(
    const uint64_t tenant_id,
    const uint64_t unit_group_id,
    const share::schema::ZoneLocalityIArray &zone_locality_array,
    ObILSAddr &ls_addr)
{
  int ret = OB_SUCCESS;
  ObUnitTableOperator unit_operator;
  common::ObArray<share::ObUnit> unit_info_array;
  ls_addr.reset();

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || 0 == unit_group_id
                         || OB_INVALID_ID == unit_group_id
                         || zone_locality_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id),
             K(unit_group_id), K(zone_locality_array));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy ptr is null", KR(ret));
  } else if (OB_FAIL(unit_operator.init(*proxy_))) {
    LOG_WARN("unit operator init failed", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_unit_group_id(unit_group_id, unit_info_array))) {
    LOG_WARN("fail to get unit group", KR(ret), K(tenant_id), K(unit_group_id));
  } else if (OB_FAIL(construct_ls_addrs_according_to_locality_(
                         zone_locality_array,
                         unit_info_array,
                         false/*is_sys_ls*/,
                         false/*is_duplicate_ls*/,
                         ls_addr))) {
    LOG_WARN("fail to construct ls addrs for tenant user ls", KR(ret), K(zone_locality_array),
             K(unit_info_array), K(ls_addr));
  }
  return ret;
}

int ObLSCreator::alloc_duplicate_ls_addr_(
    const uint64_t tenant_id,
    const share::schema::ZoneLocalityIArray &zone_locality_array,
    ObILSAddr &ls_addr)
{
  //TODO: alloc_sys_ls_addr and alloc_duplicate_ls_addr should merge into one function
  int ret = OB_SUCCESS;
  ObUnitTableOperator unit_operator;
  common::ObArray<share::ObUnit> unit_info_array;
  ls_addr.reset();

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || zone_locality_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(zone_locality_array));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy ptr is null", KR(ret));
  } else if (OB_FAIL(unit_operator.init(*proxy_))) {
    LOG_WARN("unit operator init failed", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id, unit_info_array))) {
    LOG_WARN("fail to get unit info array", KR(ret), K(tenant_id));
  } else if (OB_FAIL(construct_ls_addrs_according_to_locality_(
                         zone_locality_array,
                         unit_info_array,
                         true/*is_sys_ls*/,
                         true/*is_duplicate_ls*/,
                         ls_addr))) {
    // although duplicate log stream is a user log steam, we use the same logic to alloc addrs as sys log stream
    // so set is_sys_ls to true when execute construct_ls_addrs_according_to_locality_
    LOG_WARN("fail to construct ls addrs for tenant user ls", KR(ret), K(zone_locality_array),
             K(unit_info_array), K(ls_addr));
  }
  return ret;
}

int ObLSCreator::compensate_zone_readonly_replica_(
    const share::ObZoneReplicaAttrSet &zlocality,
    const ObLSReplicaAddr &exclude_replica,
    const common::ObIArray<share::ObUnit> &unit_info_array,
    ObILSAddr &ls_addr)
{
  int ret = OB_SUCCESS;
  const common::ObZone &locality_zone = zlocality.zone_;
  if (OB_UNLIKELY(0 >= unit_info_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_info_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_info_array.count(); ++i) {
      const share::ObUnit &unit = unit_info_array.at(i);
      if (locality_zone != unit.zone_) {
        // not match
      } else if (exclude_replica.addr_ == unit.server_) {
        // already exists in ls_addr
      } else if (ObUnit::UNIT_STATUS_DELETING == unit.status_) {
        // unit may be deleting
        LOG_TRACE("unit is not active", K(unit));
      } else {
        ObLSReplicaAddr ls_replica_addr;
        if (OB_FAIL(ls_replica_addr.init(
                      unit.server_,
                      ObReplicaType::REPLICA_TYPE_READONLY))) {
          LOG_WARN("fail to init ls replica addr", KR(ret), K(unit), K(locality_zone));
        } else if (OB_FAIL(ls_addr.push_back(ls_replica_addr))) {
          LOG_WARN("fail to push back", KR(ret), K(ls_replica_addr));
        }
      }
    }
  }
  return ret;
}

int ObLSCreator::alloc_zone_ls_addr(
    const bool is_sys_ls,
    const share::ObZoneReplicaAttrSet &zlocality,
    const common::ObIArray<share::ObUnit> &unit_info_array,
    ObLSReplicaAddr &ls_replica_addr)
{
  int ret = OB_SUCCESS;

    bool found = false;
    const common::ObZone &locality_zone = zlocality.zone_;
    ls_replica_addr.reset();
    for (int64_t i = 0; !found && OB_SUCC(ret) && i < unit_info_array.count(); ++i) {
      const share::ObUnit &unit = unit_info_array.at(i);
      if (locality_zone != unit.zone_) {
        // not match
      } else {
        found = true;
        if (zlocality.replica_attr_set_.get_full_replica_attr_array().count() > 0) {
          if (OB_FAIL(ls_replica_addr.init(
                  unit.server_,
                  ObReplicaType::REPLICA_TYPE_FULL))) {
            LOG_WARN("fail to init ls replica addr", KR(ret));
          }
        } else if (zlocality.replica_attr_set_.get_logonly_replica_attr_array().count() > 0) {
          if (OB_FAIL(ls_replica_addr.init(
                  unit.server_,
                  ObReplicaType::REPLICA_TYPE_LOGONLY))) {
            LOG_WARN("fail to init ls replica addr", KR(ret));
          }
        } else if (zlocality.replica_attr_set_.get_encryption_logonly_replica_attr_array().count() > 0) {
          if (OB_FAIL(ls_replica_addr.init(
                  unit.server_,
                  ObReplicaType::REPLICA_TYPE_ENCRYPTION_LOGONLY))) {
            LOG_WARN("fail to init ls replica addr", KR(ret));
          }
        } else if (zlocality.replica_attr_set_.get_readonly_replica_attr_array().count() > 0) {
          if (OB_FAIL(ls_replica_addr.init(
                  unit.server_,
                  ObReplicaType::REPLICA_TYPE_READONLY))) {
            LOG_WARN("fail to init ls replica addr", KR(ret));
          }
        } else {  // zone locality shall has a paxos replica in 4.0 by
                  // now(2021.10.25)
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("this zone locality not supported", KR(ret), K(zlocality));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "this zone locality");
        }
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("locality zone not found", KR(ret), K(zlocality), K(unit_info_array));
    }
  return ret;
}

}
}
