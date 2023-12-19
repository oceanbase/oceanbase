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

#ifndef OCEANBASE_ROOTSERVER_OB_LS_SERVICE_HELPER_H
#define OCEANBASE_ROOTSERVER_OB_LS_SERVICE_HELPER_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"//SCN
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusOperator
#include "share/ls/ob_ls_operator.h" //ObLSAttr
#include "share/ob_thread_mgr.h" //OBTGDefIDEnum
#include "logservice/palf/palf_iterator.h"          //PalfBufferIterator
#include "share/unit/ob_unit_info.h"//ObUnit::Status
#include "lib/thread/thread_mgr_interface.h"          // TGRunnable
#include "lib/lock/ob_thread_cond.h"//ObThreadCond


namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
class ObISQLClient;
class ObMySQLTransaction;
class ObClusterVersion;
}
namespace share
{
class ObLSTableOperator;
class SCN;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
}
}
namespace logservice
{
class ObLogHandler;
}
namespace transaction
{
class ObTxLogBlock;
class ObTxBufferNode;
}
namespace palf
{
struct PalfBaseInfo;
}
namespace rootserver
{
struct ObUnitGroupInfo
{
  ObUnitGroupInfo() : unit_group_id_(OB_INVALID_ID), unit_status_(share::ObUnit::UNIT_STATUS_MAX),
                      ls_group_ids_() {}
  virtual ~ObUnitGroupInfo() {}
  bool is_valid() const;
  int init(const uint64_t unit_group_id,
           const share::ObUnit::Status &unit_status);
  void reset();
  int assign(const ObUnitGroupInfo &other);
  int remove_ls_group(const uint64_t ls_group_id);
  bool operator==(const ObUnitGroupInfo &other) const;

  uint64_t unit_group_id_;
  share::ObUnit::Status unit_status_;
  ObArray<uint64_t> ls_group_ids_;
  TO_STRING_KV(K_(unit_group_id), K_(unit_status), K_(ls_group_ids));
};
typedef ObArray<ObUnitGroupInfo> ObUnitGroupInfoArray;
typedef ObIArray<ObUnitGroupInfo> ObUnitGroupInfoIArray;

struct ObLSGroupInfo
{
  ObLSGroupInfo() : ls_group_id_(OB_INVALID_ID), unit_group_id_(OB_INVALID_ID),
                           ls_ids_() {}
  virtual ~ObLSGroupInfo() {}
  bool is_valid() const;
  int init(const uint64_t unit_group_id, const uint64_t ls_group_id);
  int assign(const ObLSGroupInfo &other);
  void reset();
  int remove_ls(const share::ObLSID &ls_id);
  uint64_t ls_group_id_;
  uint64_t unit_group_id_;
  ObArray<share::ObLSID> ls_ids_;
  TO_STRING_KV(K_(ls_group_id), K_(unit_group_id), K_(ls_ids));
};

typedef ObArray<ObLSGroupInfo> ObLSGroupInfoArray;
typedef ObIArray<ObLSGroupInfo> ObLSGroupInfoIArray;

struct ObLSStatusMachineParameter
{
  ObLSStatusMachineParameter() : ls_id_(), status_info_(), ls_info_() {}
  virtual ~ObLSStatusMachineParameter() {}
  bool is_valid() const
  {
    return ls_id_.is_valid()
           && (share::OB_LS_EMPTY == status_info_.status_
               || status_info_.ls_id_ == ls_id_);
  }
  int init(const share::ObLSID &id, const share::ObLSStatusInfo &status_info,
           const share::ObLSAttr &ls_info);
  void reset();
  share::ObLSID ls_id_;
  share::ObLSStatusInfo status_info_;//for create ls and status of __all_ls_status
  share::ObLSAttr ls_info_;
  TO_STRING_KV(K_(ls_id), K_(status_info), K_(ls_info));
};

/*descripthin: Tenant log stream status information: Statistical log stream
 * status on __all_ls_status and __all_ls, tenant primary_zone and unit_num
 * information. Provides location information for the newly created log stream.
 * Whether the build needs to create or delete log streams.*/
class ObTenantLSInfo
{
public:
  ObTenantLSInfo(ObMySQLProxy *sql_proxy,
                 const share::schema::ObTenantSchema *tenant_schema,
                 const uint64_t tenant_id,
                 common::ObMySQLTransaction *trans = NULL)
     : sql_proxy_(sql_proxy),
       tenant_schema_(tenant_schema),
       status_operator_(),
       status_array_(),
       unit_group_array_(),
       ls_group_array_(),
       primary_zone_(),
       tenant_id_(tenant_id),
       trans_(trans) {}

  virtual ~ObTenantLSInfo(){};
  void reset();
  bool is_valid() const;
  int gather_stat();
  //get ls group info from ls_group_array_ by ls_group_id
  //the interface must used after gather_stat();
  int get_ls_group_info(const uint64_t ls_group_id, ObLSGroupInfo &info) const;
  //get ls status info from status_array_ by ls_id
  //the interface must used after gather_stat();
  int get_ls_status_info(const share::ObLSID &id, share::ObLSStatusInfo &info,
                         int64_t &info_index) const;
  int get_next_unit_group(int64_t &group_index);
  // get the primary zone not in ls group
  int get_next_primary_zone(const ObLSGroupInfo &group_info,
      ObZone &primary_zone);

  uint64_t get_tenant_id() const { return tenant_id_; }
  const share::schema::ObTenantSchema * get_tenant_schema() const
  {
    return tenant_schema_;
  }
  ObUnitGroupInfoArray& get_unit_group_array()
  {
    return unit_group_array_;
  }
  const ObIArray<ObZone> &get_primary_zone() const
  {
    return primary_zone_;
  }
  ObLSGroupInfoArray& get_ls_group_array()
  {
    return ls_group_array_;
  }
  TO_STRING_KV(K_(tenant_id), K_(is_load), K_(status_array), K_(unit_group_array),
      K_(ls_group_array), K_(primary_zone));
private:
  // get from __all_ls_status and __all_ls
  int gather_all_ls_info_();

  // base on status_array construct ls_array
  int add_ls_to_ls_group_(const share::ObLSStatusInfo &info);

  // base on ls_array construct unit_group_array and __all_unit
  int add_ls_group_to_unit_group_(const ObLSGroupInfo &group_info);

  int add_ls_status_info_(const share::ObLSStatusInfo &ls_info);
private:
  ObMySQLProxy *sql_proxy_;
  const share::schema::ObTenantSchema *tenant_schema_;
  share::ObLSStatusOperator status_operator_;
  bool is_load_;
  share::ObLSStatusInfoArray status_array_;
  common::hash::ObHashMap<share::ObLSID, int64_t> status_map_;
  ObUnitGroupInfoArray unit_group_array_;
  ObLSGroupInfoArray ls_group_array_;
  ObArray<common::ObZone> primary_zone_;
  uint64_t tenant_id_;
  ObMySQLTransaction *trans_;
};

class ObLSServiceHelper
{
public:
  ObLSServiceHelper() {};
  virtual ~ObLSServiceHelper() {};

public:
  static int construct_ls_status_machine(
      const bool lock_sys,
      const uint64_t tenant_id,
      ObMySQLProxy *sql_proxy,
      common::ObIArray<ObLSStatusMachineParameter> &status_machine_array);
  static int fetch_new_ls_group_id(ObMySQLProxy *sql_proxy, const uint64_t tenant_id, uint64_t &ls_group_id);
  static int fetch_new_ls_id(ObMySQLProxy *sql_proxy, const uint64_t tenant_id, share::ObLSID &ls_id);
  static int get_primary_zone_unit_array(const share::schema::ObTenantSchema *tenant_schema,
      ObIArray<ObZone> &primary_zone, ObIArray<share::ObSimpleUnitGroup> &unit_group_array);
  static int process_status_to_steady(
      const bool lock_sys_ls,
      const share::ObTenantSwitchoverStatus &working_sw_status,
      ObTenantLSInfo& tenant_ls_info);
  //for recovery tenant, create new ls according to ls_id and ls_group_id
  static int create_new_ls_in_trans(const share::ObLSID &ls_id,
      const uint64_t ls_group_id,
      const share::SCN &create_scn,
      const share::ObTenantSwitchoverStatus &working_sw_status,
      ObTenantLSInfo& tenant_ls_info,
      common::ObMySQLTransaction &trans,
      const share::ObLSFlag &ls_flag,
      const uint64_t source_tenant_id);
  static int balance_ls_group(
      const bool need_execute_balance,
      ObTenantLSInfo& tenant_ls_info,
      bool &is_balanced);//for standby tenant
  static int update_ls_recover_in_trans(
            const share::ObLSRecoveryStat &ls_recovery_stat,
            const bool only_update_readable_scn,
            common::ObMySQLTransaction &trans);
  static int offline_ls(const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObLSStatus &cur_ls_status,
      const ObTenantSwitchoverStatus &working_sw_status);
  static int get_ls_replica_sync_scn(const uint64_t tenant_id,
      const ObLSID &ls_id, share::SCN &create_scn);
  static int process_alter_ls(const share::ObLSID &ls_id,
      const uint64_t &old_ls_group_id,
      const uint64_t &new_ls_group_id,
      const uint64_t &old_unit_group_id,
      ObTenantLSInfo& tenant_ls_info,
      common::ObISQLClient &sql_proxy);
  static int wait_all_tenants_user_ls_sync_scn(common::hash::ObHashMap<uint64_t, share::SCN> &tenants_sys_ls_target_scn);
  static int check_transfer_task_replay(const uint64_t tenant_id,
      const share::ObLSID &src_ls,
      const share::ObLSID &dest_id,
      const share::SCN &transfer_scn,
      bool &replay_finish);
private:
  static int check_if_need_wait_user_ls_sync_scn_(const uint64_t tenant_id, const share::SCN &sys_ls_target_scn);
  static int revision_to_equal_status_(
      const ObLSStatusMachineParameter &status_machine,
      const share::ObTenantSwitchoverStatus &working_sw_status,
      ObTenantLSInfo& tenant_ls_info);
  static int balance_ls_group_between_unit_group_(
     ObTenantLSInfo& tenant_ls_info,
     const int64_t min_index, const int64_t max_index);
  static int try_shrink_standby_unit_group_(
      ObTenantLSInfo& tenant_ls_info,
      int64_t &task_cnt);
  static int try_update_ls_unit_group_(
      ObTenantLSInfo& tenant_ls_info,
      const uint64_t ls_group_id,
      ObUnitGroupInfo &src_info,
      ObUnitGroupInfo &dest_info);
  static int construct_unit_group_id_and_primary_zone_for_clone_tenant_(
      const share::ObLSID &ls_id,
      const uint64_t source_tenant_id,
      const uint64_t tenant_id,
      uint64_t &unit_group_id,
      ObZone &primary_zone);

  static int construct_unit_group_id_and_primary_zone_(
      const share::ObLSID &ls_id,
      const uint64_t ls_group_id,
      const share::ObLSFlag &ls_flag,
      ObTenantLSInfo &tenant_ls_info,
      uint64_t &unit_group_id,
      ObZone &primary_zone);
  static int get_ls_all_replica_readable_scn_(const uint64_t tenant_id,
      const share::ObLSID &src_ls,
      share::SCN &readable_scn);
  static int check_ls_transfer_replay_(const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::SCN &transfer_scn,
      bool &replay_finish);
};



}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_LS_SERVICE_HELPER_H */
