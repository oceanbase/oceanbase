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

#ifndef OCEANBASE_SHARE_OB_LS_CREATOR_H_
#define OCEANBASE_SHARE_OB_LS_CREATOR_H_

#include "rootserver/ob_rs_async_rpc_proxy.h" //async rpc
#include "share/ob_ls_id.h"//share::ObLSID
#include "share/unit/ob_unit_info.h"//ResourcePoolName

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObUnit;
struct ObAllTenantInfo;
struct ObLSStatusInfo;
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace palf
{
struct PalfBaseInfo;
}
namespace share
{

class SCN;
struct ObLSReplicaAddr
{
  common::ObAddr addr_;
  common::ObReplicaType replica_type_;

  ObLSReplicaAddr()
      : addr_(),
        replica_type_(common::REPLICA_TYPE_MAX) {}
  void reset() { *this = ObLSReplicaAddr(); }
  int init(const common::ObAddr &addr,
           const common::ObReplicaType replica_type);
  TO_STRING_KV(K_(addr),
               K_(replica_type));

};
typedef common::ObArray<ObLSReplicaAddr> ObLSAddr;
typedef common::ObIArray<ObLSReplicaAddr> ObILSAddr;

class ObLSCreator
{
public:
  ObLSCreator(obrpc::ObSrvRpcProxy &rpc_proxy,
                     const int64_t tenant_id,
                     const share::ObLSID &id,
                     ObMySQLProxy *proxy = NULL)
    :
      create_ls_proxy_(rpc_proxy, &obrpc::ObSrvRpcProxy::create_ls),
      set_member_list_proxy_(rpc_proxy, &obrpc::ObSrvRpcProxy::set_member_list),
      tenant_id_(tenant_id), id_(id), proxy_(proxy) {}
  virtual ~ObLSCreator() {}
  int create_tenant_sys_ls(const ObZone &primary_zone,
              const share::schema::ZoneLocalityIArray &zone_locality,
              const ObIArray<share::ObResourcePoolName> &pools,
              const int64_t paxos_replica_num,
              const common::ObCompatibilityMode &compat_mode,
              const ObString &zone_priority,
              const bool create_with_palf,
              const palf::PalfBaseInfo &palf_base_info,
              const uint64_t source_tenant_id);
  int create_user_ls(const share::ObLSStatusInfo &status_info,
                     const int64_t paxos_replica_num,
                     const share::schema::ZoneLocalityIArray &zone_locality,
                     const SCN &create_scn,
                     const common::ObCompatibilityMode &compat_mode,
                     const bool create_with_palf,
                     const palf::PalfBaseInfo &palf_base_info,
                     const uint64_t source_tenant_id);
  int create_sys_tenant_ls(const obrpc::ObServerInfoList &rs_list,
      const common::ObIArray<share::ObUnit> &unit_array);
  bool is_valid();

private:
 int construct_clone_tenant_ls_addrs_(const uint64_t source_tenant_id,
                                      ObLSAddr &addr);
 int do_create_ls_(const ObLSAddr &addr,
                   ObMember &arbitration_service,
                   const share::ObLSStatusInfo &info,
                   const int64_t paxos_replica_num,
                   const SCN &create_scn,
                   const common::ObCompatibilityMode &compat_mode,
                   common::ObMemberList &member_list,
                   const bool create_with_palf,
                   const palf::PalfBaseInfo &palf_base_info,
                   common::GlobalLearnerList &learner_list);
 int process_after_has_member_list_(const common::ObMemberList &member_list,
                                    const common::ObMember &arbitration_service,
                                    const int64_t paxos_replica_num,
                                    const common::GlobalLearnerList &learner_list);
 int create_ls_(const ObILSAddr &addr, const int64_t paxos_replica_num,
                const share::ObAllTenantInfo &tenant_info,
                const SCN &create_scn,
                const common::ObCompatibilityMode &compat_mode,
                const bool create_with_palf,
                const palf::PalfBaseInfo &palf_base_info,
                common::ObMemberList &member_list,
                common::ObMember &arbitration_service,
                common::GlobalLearnerList &learner_list);
 int check_member_list_and_learner_list_all_in_meta_table_(
                const common::ObMemberList &member_list,
                const common::GlobalLearnerList &learner_list);
 int inner_check_member_list_and_learner_list_(
                const common::ObMemberList &member_list,
                const common::GlobalLearnerList &learner_list);
 int construct_paxos_replica_number_to_persist_(
                const int64_t paxos_replica_num,
                const int64_t arb_replica_num,
                const common::ObMemberList &member_list,
                int64_t &paxos_replica_number_to_persist);
 int set_member_list_(const common::ObMemberList &member_list,
                      const common::ObMember &arb_replica,
                      const int64_t paxos_replica_num,
                      const common::GlobalLearnerList &learner_list);
#ifdef OB_BUILD_ARBITRATION
 int set_arbitration_service_list_(
     const common::ObMember &arbitration_service,
     const ObTimeoutCtx &ctx,
     const obrpc::ObSetMemberListArgV2 &arg);
#endif
 int persist_ls_member_list_(const common::ObMemberList &member_list,
                             const ObMember &arb_member,
                             const common::GlobalLearnerList &learner_list);

 // interface for oceanbase 4.0
 int construct_ls_addrs_according_to_locality_(
     const share::schema::ZoneLocalityIArray &zone_locality_array,
     const common::ObIArray<share::ObUnit> &unit_info_array,
     const bool is_sys_ls,
     const bool is_duplicate_ls,
     ObILSAddr &ls_addr);

 int alloc_sys_ls_addr(const uint64_t tenant_id,
                       const ObIArray<share::ObResourcePoolName> &pools,
                       const share::schema::ZoneLocalityIArray &zone_locality,
                       common::ObIArray<ObLSReplicaAddr> &addrs);

 int alloc_user_ls_addr(const uint64_t tenant_id, const uint64_t unit_group_id,
                        const share::schema::ZoneLocalityIArray &zone_locality,
                        common::ObIArray<ObLSReplicaAddr> &addrs);

 int alloc_zone_ls_addr(const bool is_sys_ls,
                        const share::ObZoneReplicaAttrSet &zone_locality,
                        const common::ObIArray<share::ObUnit> &unit_info_array,
                        ObLSReplicaAddr &ls_replica_addr);
#ifdef OB_BUILD_ARBITRATION
  int check_need_create_arb_replica_(
      bool &need_create_arb_replica,
      ObAddr &arbitration_service);

  int try_create_arbitration_service_replica_(
      const ObTenantRole &tenant_role,
      const ObAddr &arbitration_service);
#endif
 int check_create_ls_result_(const int64_t paxos_replica_num,
                             const ObIArray<int> &return_code_array,
                             common::ObMemberList &member_list,
                             common::GlobalLearnerList &learner_list,
                             const bool with_arbitration_service,
                             const int64_t arb_replica_num);
 int check_set_memberlist_result_(const ObIArray<int> &return_code_array,
                                  const int64_t paxos_replica_num);

  // alloc ls addr for duplicate log stream
  // @params[in]  tenant_id, which tenant's log stream
  // @params[in]  zone_locality_array, locality describtion
  // @params[out] ls_addr, which server to create this log stream
  int alloc_duplicate_ls_addr_(
      const uint64_t tenant_id,
      const share::schema::ZoneLocalityIArray &zone_locality_array,
      ObILSAddr &ls_addr);

  // compensate readonly replica for duplicate ls
  // @params[in]  zlocality, locality describtion in one zone
  // @params[in]  exclude_replica, already allocated-replica in locality
  // @params[in]  unit_info_array, tenant's all unit
  // @params[out] ls_addr, which server to create this lpg stream
  int compensate_zone_readonly_replica_(
      const share::ObZoneReplicaAttrSet &zlocality,
      const ObLSReplicaAddr &exclude_replica,
      const common::ObIArray<share::ObUnit> &unit_info_array,
      ObILSAddr &ls_addr);

private:
  rootserver::ObLSCreatorProxy create_ls_proxy_;
  rootserver::ObSetMemberListProxy set_member_list_proxy_;
  const int64_t tenant_id_;
  const share::ObLSID id_;
  ObMySQLProxy *proxy_;
};
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_CREATOR_H_ */
