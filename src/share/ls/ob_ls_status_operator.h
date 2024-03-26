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

#ifndef OCEANBASE_SHARE_OB_LS_STATUS_OPERATOR_H_
#define OCEANBASE_SHARE_OB_LS_STATUS_OPERATOR_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ls/ob_ls_i_life_manager.h" //ObLSLifeIAgent
#include "common/ob_zone.h"//ObZone
#include "common/ob_role.h"//ObRole
#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "common/ob_member_list.h"
#include "share/ob_tenant_info_proxy.h"//tenant switchover status
#include "share/ls/ob_ls_info.h" //ObLSReplica::MemberList
#include "share/ls/ob_ls_log_stat_info.h" //ObLSLogStatInfo
#include "share/ls/ob_ls_recovery_stat_operator.h"  //ObLSRecoveryStat
#include "share/ls/ob_ls_operator.h"

namespace oceanbase
{

namespace common
{
class ObISQLClient;
class ObString;
class ObSqlString;
class ObIAllocator;
class ObMySQLProxy;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class SCN;
}
namespace rootserver
{
class ObZoneManager;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}

class ObLSStatusOperator;

const int64_t MAX_MEMBERLIST_FLAG_LENGTH = 10;
class ObMemberListFlag
{
  OB_UNIS_VERSION(1);
public:
  enum MemberListFlag
  {
    INVALID_FLAG = -1,
    HAS_ARB_MEMBER = 0,
    MAX_FLAG
  };
public:
  ObMemberListFlag() : flag_(INVALID_FLAG) {}
  explicit ObMemberListFlag(MemberListFlag flag) : flag_(flag) {}
  virtual ~ObMemberListFlag() {}

  void reset() { flag_ = INVALID_FLAG; }
  const MemberListFlag &get_flag() const { return flag_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool is_valid() const { return INVALID_FLAG < flag_ && MAX_FLAG > flag_; }
  bool is_arb_member() const { return HAS_ARB_MEMBER == flag_; }

private:
  // 0: has arb member
  MemberListFlag flag_;
};

struct ObLSStatusInfo
{
  ObLSStatusInfo() : tenant_id_(OB_INVALID_TENANT_ID),
                          ls_id_(), ls_group_id_(OB_INVALID_ID),
                          status_(OB_LS_EMPTY), unit_group_id_(OB_INVALID_ID),
                          primary_zone_(), flag_(ObLSFlag::NORMAL_FLAG) {}
  virtual ~ObLSStatusInfo() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id,
           const ObLSID &id, const uint64_t ls_group_id,
           const ObLSStatus status, const uint64_t unit_group_id,
           const ObZone &primary_zone, const ObLSFlag &flag);
  bool ls_is_creating() const
  {
    return ls_is_creating_status(status_);
  }
  bool ls_is_dropping() const
  {
    return ls_is_dropping_status(status_);
  }
  bool ls_is_tenant_dropping() const
  {
    return ls_is_tenant_dropping_status(status_);
  }
  bool ls_is_wait_offline() const
  {
    return ls_is_wait_offline_status(status_);
  }
  bool ls_is_created() const
  {
    return ls_is_created_status(status_);
  }
  bool ls_is_normal() const
  {
    return ls_is_normal_status(status_);
  }
  bool ls_is_create_abort() const
  {
    return ls_is_create_abort_status(status_);
  }
  bool ls_need_create_abort() const
  {
    return ls_need_create_abort_status(status_);
  }
  bool ls_is_pre_tenant_dropping() const
  {
    return ls_is_pre_tenant_dropping_status(status_);
  }
  bool is_duplicate_ls() const
  {
    return flag_.is_duplicate_ls();
  }
  bool ls_is_block_tablet_in() const
  {
    return flag_.is_block_tablet_in();
  }
  ObLSStatus get_status() const
  {
    return status_;
  }

  ObLSID get_ls_id() const
  {
    return ls_id_;
  }

  ObLSFlag get_flag() const
  {
    return flag_;
  }
  int assign(const ObLSStatusInfo &other);
  void reset();
  bool is_normal() const
  {
    return OB_LS_NORMAL == status_;
  }
  bool operator==(const ObLSStatusInfo &other) const
  {
    return tenant_id_ == other.tenant_id_
      && ls_id_ == other.ls_id_
      && ls_group_id_ == other.ls_group_id_
      && status_ == other.status_
      && unit_group_id_ == other.unit_group_id_
      && primary_zone_ == other.primary_zone_
      && flag_ == other.flag_;
  }

  bool operator!=(const ObLSStatusInfo &other) const {
    return !(*this == other);
  }

  bool is_user_ls() const { return ls_id_.is_user_ls(); }

  uint64_t tenant_id_;
  ObLSID ls_id_;
  uint64_t ls_group_id_;
  ObLSStatus status_;
  uint64_t unit_group_id_;
  ObZone primary_zone_;
  share::ObLSFlag flag_;

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(ls_group_id),
               "status", ls_status_to_str(status_),
               K_(unit_group_id), K_(primary_zone), K_(flag));
};

typedef ObArray<ObLSStatusInfo> ObLSStatusInfoArray;
typedef ObIArray<ObLSStatusInfo> ObLSStatusInfoIArray;

struct ObLSPrimaryZoneInfo
{
  ObLSPrimaryZoneInfo() : tenant_id_(OB_INVALID_TENANT_ID), ls_group_id_(OB_INVALID_ID),
                          ls_id_(), primary_zone_(), zone_priority_() {}
  virtual ~ObLSPrimaryZoneInfo() {}
  int init(const uint64_t tenant_id, const uint64_t ls_group_id, const ObLSID ls_id,
           const ObZone &primary_zone, const ObString &zone_priority);
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid();
  }
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_group_id_ = OB_INVALID_ID;
    ls_id_.reset();
    primary_zone_.reset();
    zone_priority_.reset();
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  uint64_t get_ls_group_id() const
  {
    return ls_group_id_;
  }
  ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  const ObZone& get_primary_zone() const
  {
    return primary_zone_;
  }
  const ObString get_zone_priority_str() const
  {
    return zone_priority_.string();
  }
  const ObSqlString& get_zone_priority() const
  {
    return zone_priority_;
  }
  int assign(const ObLSPrimaryZoneInfo &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_group_id), K_(ls_id), K_(primary_zone), K_(zone_priority));
private:
  uint64_t tenant_id_;
  uint64_t ls_group_id_;
  ObLSID ls_id_;
  ObZone primary_zone_;
  ObSqlString zone_priority_;
};

typedef ObArray<ObLSPrimaryZoneInfo> ObLSPrimaryZoneInfoArray;
typedef ObIArray<ObLSPrimaryZoneInfo> ObLSPrimaryZoneInfoIArray;

/*
 * description : read or write __all_ls_status
*/
class ObLSStatusOperator : public ObLSLifeIAgent, public ObLSTemplateOperator
{
 public:
  ObLSStatusOperator() {};
  virtual ~ObLSStatusOperator(){}
public:
  /*
   * description: override of ObLSLifeIAgent
   * @param[in] ls_info: ls info
   * @param[in] create_ls_scn: ls's create scn
   * @param[in] zone_priority: for __all_ls_election_reference_info
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] trans:*/
  virtual int create_new_ls(const ObLSStatusInfo &ls_info,
                            const SCN &current_tenant_scn,
                            const common::ObString &zone_priority,
                            const share::ObTenantSwitchoverStatus &working_sw_status,
                            ObMySQLTransaction &trans) override;
  /*
   * description: override of ObLSLifeIAgent
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] trans:*/
  virtual int drop_ls(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) override;
  /*
   * description: for primary cluster set ls to wait offline from tenant_dropping or dropping status 
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: need delete ls
   * @param[in] ls_status: tenant_dropping or dropping status 
   * @param[in] drop_scn: there is no user data after drop_scn except offline
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] trans
   * */
  virtual int set_ls_offline(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObLSStatus &ls_status,
                      const SCN &drop_scn,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) override;
  /*
   * description: update ls primary zone, need update __all_ls_status and __all_ls_election_reference 
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: need update ls
   * @param[in] primary_zone: primary zone of __all_ls_status 
   * @param[in] zone_priority: primary zone of __all_ls_election_reference 
   * @param[in] trans
   * */
  int update_ls_primary_zone(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const common::ObZone &primary_zone,
      const common::ObString &zone_priority,
      ObMySQLTransaction &trans) override; 
public:
  /*
   * description: update ls's status 
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] old_status
   * @param[in] new_status
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] client: sql client*/
  int update_ls_status(const uint64_t tenant_id, const ObLSID &id,
                       const ObLSStatus &old_status,
                       const ObLSStatus &new_status, 
                       const ObTenantSwitchoverStatus &working_sw_status,
                       ObMySQLProxy &client);

  /*
   * description: update ls init member list while first create ls
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] member_list
   * @param[in] client*/
  int update_init_member_list(const uint64_t tenant_id, const ObLSID &id,
                              const ObMemberList &member_list,
                              ObISQLClient &client,
                              const ObMember &arb_member,
                              const common::GlobalLearnerList &learner_list);
   /*
   * description: update ls's ls group id
   * @param[in] tenant_id
  * @param[in] ls_id
   * @param[in] old_ls_group_id
   * @param[in] new_ls_group_id
   * @param[in] old_unit_group_id : the ls group's unit group
   * @param[in] new_unit_group_id : the new ls group's target unit group
   * @param[in] client*/
  int alter_ls_group_id(const uint64_t tenant_id, const ObLSID &id,
                       const uint64_t old_ls_group_id,
                       const uint64_t new_ls_group_id,
                       const uint64_t old_unit_group_id,
                       const uint64_t new_unit_group_id,
                       ObISQLClient &client);
   /*
   * description: update ls's unit group id
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] ls_group_id
   * @param[in] old_unit_group_id
   * @param[in] new_unit_group_id : the new ls group's target unit group
   * @param[in] client*/
  int alter_unit_group_id(const uint64_t tenant_id, const ObLSID &id,
                       const uint64_t ls_group_id,
                       const uint64_t old_unit_group_id,
                       const uint64_t new_unit_group_id,
                       ObISQLClient &client);

  int get_all_ls_status_by_order(const uint64_t tenant_id,
                                 ObLSStatusInfoIArray &ls_array,
                                 ObISQLClient &client);

  // get duplicate ls status info
  // @params[in]  tenant_id, which tenant to get
  // @params[in]  client, client to execute sql
  // @params[out] status_info, duplicate ls status info
  //
  // ATTENTION!!!
  // status_info not include visible_member_list and b_init_member_list
  // @return OB_ENTRY_NOT_EXIST if duplicate log stream not exist
  int get_duplicate_ls_status_info(const uint64_t tenant_id,
                                   ObISQLClient &client,
                                   share::ObLSStatusInfo &status_info,
                                   const int32_t group_id);
  /**
   * @description:
   *    get ls list from all_ls_status order by tenant_id, ls_id for switchover tenant
   *    if ls status is OB_LS_TENANT_DROPPING or OB_LS_PRE_TENANT_DROPPING
   *       return OB_TENANT_HAS_BEEN_DROPPED
   *
   *    if ls status is OB_LS_CREATING or OB_LS_CREATED
   *       if (ignore_need_create_abort)
   *          ignore ls
   *       else
   *          return OB_ERR_UNEXPECTED
   *
   *    if ls status is OB_LS_CREATE_ABORT
   *       ignore ls
   *
   * @param[in] tenant_id
   * @param[in] ignore_need_create_abort
   * @param[out] ls_array returned ls list
   * @param[in] client
   * @return return code
   */
  int get_all_ls_status_by_order_for_switch_tenant(const uint64_t tenant_id,
                                 const bool ignore_need_create_abort,
                                 ObLSStatusInfoIArray &ls_array,
                                 ObISQLClient &client);
  int get_ls_init_member_list(const uint64_t tenant_id, const ObLSID &id,
                              ObMemberList &member_list,
                              ObLSStatusInfo &status_info,
                              ObISQLClient &client,
                              ObMember &arb_member,
                              common::GlobalLearnerList &learner_list);
  int get_ls_status_info(const uint64_t tenant_id, const ObLSID &id,
                         ObLSStatusInfo &status_info, ObISQLClient &client,
                         const int32_t group_id = 0);
  int fill_cell(common::sqlclient::ObMySQLResult *result,
                share::ObLSStatusInfo &status_info);
  int fill_cell(common::sqlclient::ObMySQLResult *result,
                share::ObLSPrimaryZoneInfo &status_info);
  int get_ls_primary_zone_info(const uint64_t tenant_id, const ObLSID &id,
                               ObLSPrimaryZoneInfo &primary_zone_info, ObISQLClient &client);
  int get_ls_primary_zone_info_by_order_ls_group(const uint64_t tenant_id,
                                         ObLSPrimaryZoneInfoIArray &primary_zone_info_array,
                                         ObISQLClient &client);
  /*
   * description: get user tenant max ls id, only for compatible
   * @param[in] tenant_id
   * @param[out] max_id: max ls id of the tenant
   * @param[in] client*/
  int get_tenant_max_ls_id(const uint64_t tenant_id, ObLSID &max_id,
                           ObISQLClient &client);

  /**
   * @description:
   *    set ls status to create abort which is in OB_LS_CREATED, OB_LS_CREATING
   *    to avoid concurrent, only do this when status specified does not change
   * @param[in] tenant_id
   * @param[in] status
   * @param[in] client
   */
  int create_abort_ls_in_switch_tenant(
      const uint64_t tenant_id,
      const share::ObTenantSwitchoverStatus &status,
      const int64_t switchover_epoch,
      ObMySQLProxy &client);

  ////////////////////////////////////////////////////////////////////////////////
  // Get all ls paxos from __all_virtual_ls_status and __all_virtual_log_stat except 
  // those whose status is OB_LS_CREATE_ABORT. And then, check majority and log_in_sync.
  //
  // @param [in] zone_mgr: zone manager from rs
  // @param [in] to_stop_servers: servers to be stopped
  // @param [in] skip_log_sync_check: whether skip log_sync check
  // @param [in] print_str: string of operation. Used to print LOG_USER_ERROR "'print_str' not allowed"
  // @param [in] schema_service: schema_service from rs
  // @param [in] client: sql client for inner sql
  // @param [out] need_retry: if the check need retry
  // @return: OB_SUCCESS if all check is passed.
  //          OB_OP_NOT_ALLOW if ls doesn't have leader/enough member or ls' log is not in sync.
  int check_all_ls_has_majority_and_log_sync(
      const common::ObIArray<ObAddr> &to_stop_servers,
      const bool skip_log_sync_check,
      const char *print_str,
      schema::ObMultiVersionSchemaService &schema_service,
      ObISQLClient &client,
      bool &need_retry);
  // Get all ls paxos from __all_virtual_ls_status and __all_virtual_log_stat except 
  // those whose status is OB_LS_CREATE_ABORT. And then, check each ls does have leader.
  // @param [in] client: sql client for inner sql
  // @param [in] print_str: string of operation. Used to print LOG_USER_ERROR "'print_str' not allowed"
  // @param [out] has_ls_without_leader: whether there is an LS without a leader
  // @param [out] valid_error_msg: if has ls without leader, print ls and tenant_id error message
  int check_all_ls_has_leader(
      ObISQLClient &client,
      const char *print_str,
      bool &has_ls_without_leader,
      ObSqlString &error_msg);
  /*
   * description: get all tenant ls status. for user tenant: get user tenant ls status info and meta tenant ls status info
   * @param[in] sql_proxy
   * @param[in] tenant_id : maybe user tenant id and sys tenant id
   * @param[out] ls status info array
   * */
  int get_all_tenant_related_ls_status_info(
      common::ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      ObLSStatusInfoIArray &ls_status_info_array);
 /*
   * description: update ls's status
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] old_status
   * @param[in] new_status
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] trans*/
  int update_ls_status_in_trans(
      const uint64_t tenant_id,
      const ObLSID &id,
      const ObLSStatus &old_status,
      const ObLSStatus &new_status,
      const ObTenantSwitchoverStatus &working_sw_status,
      ObMySQLTransaction &trans);


private:
  template<typename T> int get_list_hex_(
      const T &list,
      common::ObIAllocator &allocator,
      common::ObString &hex_str,
      const ObMember &arb_member);

  template<typename T> int set_list_with_hex_str_(
      const common::ObString &str,
      T &learner_list,
      ObMember &arb_member);

  int get_visible_member_list_str_(const ObMemberList &member_list,
                                  common::ObIAllocator &allocator,
                                  common::ObSqlString &visible_member_list_str,
                                  const ObMember &arb_member);

  int inner_get_ls_status_(const ObSqlString &sql, const uint64_t exec_tenant_id,
                           const bool need_member_list, ObISQLClient &client,
                           ObMemberList &member_list, share::ObLSStatusInfo &status_info,
                           ObMember &arb_member, common::GlobalLearnerList &learner_list,
                           const int32_t group_id);

  int get_ls_status_(const uint64_t tenant_id, const ObLSID &id, const bool need_member_list,
                     ObMemberList &member_list,
                     ObLSStatusInfo &status_info, ObISQLClient &client,
                     ObMember &arb_member, common::GlobalLearnerList &learner_list,
                     const int32_t group_id);

  int construct_ls_primary_info_sql_(common::ObSqlString &sql);

  //////////for checking all ls log_stat_info/////////
  int construct_ls_log_stat_info_sql_(common::ObSqlString &sql);
  int parse_result_and_check_paxos_(
      common::sqlclient::ObMySQLResult &result,
      schema::ObMultiVersionSchemaService &schema_service,
      const common::ObIArray<ObAddr> &to_stop_servers,
      const bool skip_log_sync_check,
      const char *print_str,
      bool &need_retry);
  // tenant_id and ls_id is used for printing error info
  int construct_ls_log_stat_replica_(
      const common::sqlclient::ObMySQLResult &result,
      ObLSLogStatReplica &replica,
      uint64_t &tenant_id,
      int64_t &ls_id);
  // check majority and log_sync for each ls paxos
  int check_ls_log_stat_info_(
      schema::ObMultiVersionSchemaService &schema_service,
      const ObLSLogStatInfo &ls_log_stat_info,
      const common::ObIArray<ObAddr> &to_stop_servers,
      const bool skip_log_sync_check,
      const char *print_str,
      bool &need_retry);
  int generate_valid_servers_(
      const ObLSReplica::MemberList &member_list,
      const common::ObIArray<ObAddr> &to_stop_servers,
      common::ObIArray<ObAddr> &valid_servers);
  int construct_ls_leader_info_sql_(common::ObSqlString &sql);

private:
  const int64_t MAX_ERROR_LOG_PRINT_SIZE = 1024;
};

}
}

#endif /* !OCEANBASE_SHARE_OB_LS_STATUS_OPERATOR_H_ */
