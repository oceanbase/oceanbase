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

#ifndef OCEANBASE_ROOTSERVER_OB_PRIMARY_LS_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_PRIMARY_LS_SERVICE_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"//SCN
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusOperator
#include "share/ls/ob_ls_operator.h" //ObLSAttr
#include "share/ob_thread_mgr.h" //OBTGDefIDEnum
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
class ObMySQLTransaction;
}
namespace share
{
class ObLSTableOperator;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
}

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
  ObLSStatusMachineParameter() : ls_id_(), status_info_(), ls_info_(share::OB_LS_EMPTY) {}
  virtual ~ObLSStatusMachineParameter() {}
  bool is_valid() const
  {
    return ls_id_.is_valid()
           && (share::OB_LS_EMPTY == status_info_.status_
               || status_info_.ls_id_ == ls_id_);
  }
  int init(const share::ObLSID &id, const share::ObLSStatusInfo &status_info,
           const share::ObLSStatus &ls_info);
  void reset();
  share::ObLSID ls_id_;
  share::ObLSStatusInfo status_info_;//for create ls and status of __all_ls_status
  share::ObLSStatus ls_info_;//status in __all_ls
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
  //some ls not in __all_ls, but in __all_ls_status
  //it need delete by gc, no need process it anymore.
  //check the ls status, and delete no need ls
  int process_ls_status_missmatch(
      const bool lock_sys_ls,
      const share::ObTenantSwitchoverStatus &working_sw_status);

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
  int drop_tenant(const share::ObTenantSwitchoverStatus &working_sw_status);
  //for recovery tenant, create new ls according to ls_id and ls_group_id
  int create_new_ls_for_recovery(const share::ObLSID &ls_id,
                                    const uint64_t ls_group_id,
                                    const share::SCN &create_scn,
                                    common::ObMySQLTransaction &trans,
                                    const share::ObLSFlag &ls_flag);
  //for recovery tenant, if ls is in creating in __all_ls_status, create the ls
  int process_ls_stats_for_recovery();

  int adjust_user_tenant_primary_zone();

  /*
  * description: create ls with palf base info for recovery
    @param[in] info: status info in __all_ls_status_info
    @param[in] create_scn : create scn of ls
    @param[in] create_ls_with_palf: restore create init ls
    @param[in] palf_base_info : palf base info
  */
  int create_ls_with_palf(const share::ObLSStatusInfo &info, const share::SCN &create_scn,
                          const bool create_ls_with_palf,
                          const palf::PalfBaseInfo &palf_base_info);

  static int get_zone_priority(const ObZone &primary_zone,
                                 const share::schema::ObTenantSchema &tenant_schema,
                                 ObSqlString &primary_zone_str);
  static int try_update_ls_primary_zone_(const share::ObLSPrimaryZoneInfo &primary_zone_info,
                                  const common::ObZone &new_primary_zone,
                                  const common::ObSqlString &zone_priority);
  static int construct_ls_status_machine_(
      const share::ObLSStatusInfoIArray &statua_info_array,
      const share::ObLSAttrIArray &ls_array,
      common::ObIArray<ObLSStatusMachineParameter> &status_machine_array);

  int create_duplicate_ls();

private:
  int fix_ls_status_(const ObLSStatusMachineParameter &status_machine,
                     const share::ObTenantSwitchoverStatus &working_sw_status);
  // get from __all_ls_status and __all_ls
  int gather_all_ls_info_();

  // base on status_array construct ls_array
  int add_ls_to_ls_group_(const share::ObLSStatusInfo &info);

  // base on ls_array construct unit_group_array and __all_unit
  int add_ls_group_to_unit_group_(const ObLSGroupInfo &group_info);

  int process_creating_ls_(const share::ObLSAttr &ls_operation);

  int check_unit_group_valid_(const uint64_t unit_group_id, bool &is_valid);
  int add_ls_status_info_(const share::ObLSStatusInfo &ls_info);
  int create_new_ls_for_empty_unit_group_(const uint64_t unit_group_id);

  int get_next_unit_group_(const bool is_recovery, int64_t &group_index);
  // get the primary zone not in ls group
  int get_next_primary_zone_(const bool is_recovery, const ObLSGroupInfo &group_info,
      ObZone &primary_zone);
  int create_new_ls_(const share::ObLSStatusInfo &ls_info,
                     const share::ObTenantSwitchoverStatus &working_sw_status);

  // drop ls group info
  int try_drop_ls_of_deleting_unit_group_(const ObUnitGroupInfo &info);
  int drop_ls_(const share::ObLSStatusInfo &ls_info,
               const share::ObTenantSwitchoverStatus &working_sw_status);
  int do_drop_ls_(const share::ObLSStatusInfo &ls_info,
                  const share::ObTenantSwitchoverStatus &working_sw_status);
  int do_tenant_drop_ls_(const share::ObLSStatusInfo &ls_info,
                         const share::ObTenantSwitchoverStatus &working_sw_status);
  int do_create_ls_(const share::ObLSStatusInfo &info, const share::SCN &create_scn);
  int sys_ls_tenant_drop_(const share::ObLSStatusInfo &info,
                          const share::ObTenantSwitchoverStatus &working_sw_status);
  int check_sys_ls_can_offline_(bool &can_offline);
  int check_ls_empty_(const share::ObLSStatusInfo &info, bool &empty);
  int check_ls_can_offline_by_rpc_(const share::ObLSStatusInfo &info,
      const share::ObLSStatus &current_ls_status,
      bool &can_offline);
  int process_ls_status_after_created_(const share::ObLSStatusInfo &info,
                                       const share::ObTenantSwitchoverStatus &working_sw_status);

private:
  static int set_ls_to_primary_zone(const common::ObIArray<common::ObZone> &primary_zone_array,
                             const share::ObLSPrimaryZoneInfoArray &primary_zone_infos,
                             common::ObIArray<common::ObZone> &ls_primary_zone,
                             common::ObIArray<uint64_t> &count_group_by_zone);
  static int balance_ls_primary_zone(const common::ObIArray<common::ObZone> &primary_zone_array,
                              common::ObIArray<common::ObZone> &ls_primary_zone,
                              common::ObIArray<uint64_t> &count_group_by_zone);
  int adjust_primary_zone_by_ls_group_(const common::ObIArray<common::ObZone> &primary_zone_array,
                                       const share::ObLSPrimaryZoneInfoArray &primary_zone_infos,
                                       const share::schema::ObTenantSchema &tenant_schema);

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

class ObTenantThreadHelper : public lib::TGRunnable,
  public logservice::ObIRoleChangeSubHandler
{
public:
  ObTenantThreadHelper() : tg_id_(-1), thread_cond_(), is_created_(false), is_first_time_to_start_(true), thread_name_("") {}
  virtual ~ObTenantThreadHelper() {}
  virtual void do_work() = 0;
  virtual void run1() override;
  virtual void destroy();
  int start();
  void stop();
  void wait();
  void mtl_thread_stop();
  void mtl_thread_wait();
  int create(const char* thread_name, int tg_def_id, ObTenantThreadHelper &tenant_thread);
  void idle(const int64_t idle_time_us);
public:
  virtual void switch_to_follower_forcedly() override;

  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override
  {
    stop();
    return OB_SUCCESS;
  }
  virtual int resume_leader() override
  {
    return OB_SUCCESS;
  }

#define DEFINE_MTL_FUNC(TYPE)\
  static int mtl_init(TYPE *&ka) {\
    int ret = OB_SUCCESS;\
    if (OB_ISNULL(ka)) {\
      ret = OB_ERR_UNEXPECTED;\
    } else if (OB_FAIL(ka->init())) {\
    }\
    return ret;\
  }\
  static void mtl_stop(TYPE *&ka) {\
    if (OB_NOT_NULL(ka)) {\
      ka->mtl_thread_stop();\
    }\
  }\
  static void mtl_wait(TYPE *&ka) {\
    if (OB_NOT_NULL(ka)) {\
      ka->mtl_thread_wait();\
    }\
  }

protected:
  int tg_id_;
private:
  common::ObThreadCond thread_cond_;
  bool is_created_;
  bool is_first_time_to_start_;
  const char* thread_name_;
};

/*description:
 *Log stream management thread: Started on the leader of the system log stream
 * under each tenant. Under the meta tenant, the primary_zone information of
 * each log stream needs to be adjusted. Under the user tenant, log streams need
 * to be created and deleted according to the changes of primary_zone and
 * unit_num. And since the update of __all_ls and __all_ls_status is not atomic,
 * it is also necessary to deal with the mismatch problem caused by this
 * non-atomic. When the tenant is in the deletion state, it is also necessary to
 * advance the state of each log stream to the end.*/
class ObPrimaryLSService : public ObTenantThreadHelper,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler 
{
public:
  ObPrimaryLSService():inited_(false), tenant_id_(OB_INVALID_TENANT_ID) {}
  virtual ~ObPrimaryLSService() {}
  int init();
  void destroy();
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObPrimaryLSService)

public:
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(share::SCN &scn) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }

  int create_duplicate_ls();
private:

  int process_user_tenant_(const share::schema::ObTenantSchema &tenant_schema);
  //no need create ls or drop ls, but need adjust primary_zone
  int process_meta_tenant_(const share::schema::ObTenantSchema &tenant_schema);
  //for primary cluster, sys ls recovery stat need report,
  //standby cluster will be reported in RecoveryLSService
  int report_sys_ls_recovery_stat_();
  // force drop user tenant if tenant is in dropping status
  int try_force_drop_tenant_(
      const uint64_t user_tenant_id);
  int gather_tenant_recovery_stat_();
private:
  bool inited_;
  uint64_t tenant_id_;

};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_PRIMARY_LS_SERVICE_H */
