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
                 const uint64_t tenant_id, obrpc::ObSrvRpcProxy *rpc_proxy,
                 share::ObLSTableOperator *lst_operator)
     : sql_proxy_(sql_proxy),
       tenant_schema_(tenant_schema),
       status_operator_(),
       ls_operator_(tenant_id, sql_proxy),
       status_array_(),
       unit_group_array_(),
       ls_group_array_(),
       primary_zone_(),
       rpc_proxy_(rpc_proxy),
       lst_operator_(lst_operator),
       max_ls_id_(OB_INVALID_ID),
       max_ls_group_id_(OB_INVALID_ID) {}

  virtual ~ObTenantLSInfo(){};
  void reset();
  bool is_valid() const;

  int gather_stat(bool for_recovery);
  int process_next_ls_status(int64_t &task_cnt);

  // need create or delete new ls group
  int check_ls_group_match_unitnum();

  //need create or delete new ls
  int check_ls_match_primary_zone();

  int fetch_new_ls_group_id(const uint64_t tenant_id, uint64_t &ls_group_id);
  int fetch_new_ls_id(const uint64_t tenant_id, share::ObLSID &ls_id);

  //get ls group info from ls_group_array_ by ls_group_id
  //the interface must used after gather_stat();
  int get_ls_group_info(const uint64_t ls_group_id, ObLSGroupInfo &info) const;

  //get ls status info from status_array_ by ls_id
  //the interface must used after gather_stat();
  int get_ls_status_info(const share::ObLSID &id, share::ObLSStatusInfo &info,
                         int64_t &info_index) const;
  //process dropping tenant, set status to tenant_dropping in __all_ls
  int drop_tenant();
  //for recovery tenant, create new ls according to ls_id and ls_group_id
  int create_new_ls_in_trans(const share::ObLSID &ls_id,
                                    const uint64_t ls_group_id,
                                    const share::SCN &create_scn,
                                    common::ObMySQLTransaction &trans);
  int construct_ls_status_machine_(
      common::ObIArray<ObLSStatusMachineParameter> &status_machine_array);
  //only for upgrade, before 4.1, make __all_ls_status equal to __all_ls
  int revision_to_equal_status(common::ObMySQLTransaction &trans);
  int check_ls_can_offline_by_rpc(const share::ObLSStatusInfo &info,
      bool &can_offline);
private:
  // get from __all_ls_status and __all_ls
  int gather_all_ls_info_();

  // base on status_array construct ls_array
  int add_ls_to_ls_group_(const share::ObLSStatusInfo &info);

  // base on ls_array construct unit_group_array and __all_unit
  int add_ls_group_to_unit_group_(const ObLSGroupInfo &group_info);

  int check_unit_group_valid_(const uint64_t unit_group_id, bool &is_valid);
  int add_ls_status_info_(const share::ObLSStatusInfo &ls_info);
  int create_new_ls_for_empty_unit_group_(const uint64_t unit_group_id);

  int get_next_unit_group_(const bool is_recovery, int64_t &group_index);
  // get the primary zone not in ls group
  int get_next_primary_zone_(const bool is_recovery, const ObLSGroupInfo &group_info,
      ObZone &primary_zone);
  int create_new_ls_(const share::ObLSStatusInfo &ls_info);

  // drop ls group info
  int do_tenant_drop_ls_(const share::ObLSStatusInfo &ls_info);
  int sys_ls_tenant_drop_(const share::ObLSStatusInfo &info);
  int check_sys_ls_can_offline_(bool &can_offline);


private:
  //modify __all_ls
  int process_next_ls_status_(const ObLSStatusMachineParameter &status_machine, bool &is_steady);

private:
  ObMySQLProxy *sql_proxy_;
  const share::schema::ObTenantSchema *tenant_schema_;
  share::ObLSStatusOperator status_operator_;
  share::ObLSAttrOperator ls_operator_;
  bool is_load_;
  share::ObLSStatusInfoArray status_array_;
  common::hash::ObHashMap<share::ObLSID, int64_t> status_map_;
  ObUnitGroupInfoArray unit_group_array_;
  ObLSGroupInfoArray ls_group_array_;
  ObArray<common::ObZone> primary_zone_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  share::ObLSTableOperator *lst_operator_;
  //ensure ls can not be concurrent created
  uint64_t max_ls_id_;
  uint64_t max_ls_group_id_;
};

/*description:
 *only active on the leader of the system log stream under the user tenant*/
class ObRecoveryLSHelper
{
public:
  ObRecoveryLSHelper(const uint64_t tenant_id,
                     common::ObMySQLProxy *sql_proxy) :
  tenant_id_(tenant_id), proxy_(sql_proxy) {}
  virtual ~ObRecoveryLSHelper() {}
  int do_work(palf::PalfBufferIterator &iterator,
              share::SCN &start_scn);
private:
 //get log iterator by start_scn
 int seek_log_iterator_(const share::SCN &syn_scn,
                        palf::PalfBufferIterator &iterator);
 int process_ls_log_(const ObAllTenantInfo &tenant_info,
                     share::SCN &start_scn,
                     palf::PalfBufferIterator &iterator);
 int process_upgrade_log_(const transaction::ObTxBufferNode &node);
 int process_ls_tx_log_(transaction::ObTxLogBlock &tx_log,
                        const share::SCN &syn_scn);
 int process_ls_operator_(const share::ObLSAttr &ls_attr,
                          const share::SCN &syn_scn);
 int create_new_ls_(const share::ObLSAttr &ls_attr,
                    const share::SCN &syn_scn,
                    common::ObMySQLTransaction &trans);
 //wait other ls is larger than sycn ts
 int check_valid_to_operator_ls_(const share::ObLSAttr &ls_attr,
                                 const share::SCN &syn_scn);
 //readable scn need report
 int report_tenant_sys_recovery_scn_(const share::SCN &sys_recovery_scn, const bool update_sys_recovery_scn,
                             ObISQLClient *proxy);
 int report_tenant_sys_recovery_scn_trans_(const share::SCN &sys_recovery_scn, const bool update_sys_recovery_scn,
                             common::ObMySQLTransaction &trans);
 int process_ls_operator_in_trans_(const share::ObLSAttr &ls_attr,
     const share::SCN &sys_recovery_scn, common::ObMySQLTransaction &trans);
 int try_to_process_sys_ls_offline_();
private:
  uint64_t tenant_id_;
  common::ObMySQLProxy *proxy_;
};

}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_LS_SERVICE_HELPER_H */
