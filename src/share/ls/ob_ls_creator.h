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

class ObServerManager;
struct ObLSReplicaAddr
{
  common::ObAddr addr_;
  common::ObReplicaType replica_type_;
  common::ObReplicaProperty replica_property_;
  uint64_t unit_group_id_;
  uint64_t unit_id_;
  common::ObZone zone_;

  ObLSReplicaAddr()
      : addr_(),
        replica_type_(common::REPLICA_TYPE_MAX),
        replica_property_(),
        unit_group_id_(common::OB_INVALID_ID),
        unit_id_(common::OB_INVALID_ID),
        zone_() {}
  void reset() { *this = ObLSReplicaAddr(); }
  int init(const common::ObAddr &addr,
           const common::ObReplicaType replica_type,
           const common::ObReplicaProperty &replica_property,
           const uint64_t unit_group_id,
           const uint64_t unit_id,
           const common::ObZone &zone);
  int64_t get_memstore_percent() const {return replica_property_.get_memstore_percent();}
  int set_memstore_percent(const int64_t mp) {return replica_property_.set_memstore_percent(mp);}
  TO_STRING_KV(K_(addr),
               K_(replica_type),
               K_(replica_property),
               K_(unit_group_id),
               K_(unit_id),
               K_(zone));

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
              const palf::PalfBaseInfo &palf_base_info);
  int create_user_ls(const share::ObLSStatusInfo &status_info,
                     const int64_t paxos_replica_num,
                     const share::schema::ZoneLocalityIArray &zone_locality,
                     const int64_t create_ts_ns,
                     const common::ObCompatibilityMode &compat_mode,
                     const bool create_with_palf,
                     const palf::PalfBaseInfo &palf_base_info);
  int create_sys_tenant_ls(const obrpc::ObServerInfoList &rs_list,
      const common::ObIArray<share::ObUnit> &unit_array);
  bool is_valid();
private:
 int do_create_ls_(const ObLSAddr &addr, const share::ObLSStatusInfo &info,
                   const int64_t paxos_replica_num,
                   const int64_t create_ts_ns,
                   const common::ObCompatibilityMode &compat_mode,
                   common::ObMemberList &member_list,
                   const bool create_with_palf,
                   const palf::PalfBaseInfo &palf_base_info);
 int process_after_has_member_list_(const common::ObMemberList &member_list,
                                    const int64_t paxos_replica_num);
 int create_ls_(const ObILSAddr &addr, const int64_t paxos_replica_num,
                const share::ObAllTenantInfo &tenant_info,
                const int64_t create_ts_ns,
                const common::ObCompatibilityMode &compat_mode,
                const bool create_with_palf,
                const palf::PalfBaseInfo &palf_base_info,
                common::ObMemberList &member_list);
 int check_member_list_all_in_meta_table_(const common::ObMemberList &member_list);
 int set_member_list_(const common::ObMemberList &member_list,
                      const int64_t paxos_replica_num);
 int persist_ls_member_list_(const common::ObMemberList &member_list);

 // interface for oceanbase 4.0
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
 int check_create_ls_result_(const int64_t rpc_count,
                            const int64_t paxos_replica_num,
                            common::ObMemberList &member_list);
 int check_set_memberlist_result_(const int64_t rpc_count,
                            const int64_t paxos_replica_num);

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
