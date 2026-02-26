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

#ifndef OCEANBASE_STORAGE_OB_LS_RECOVERY_STAT_HANDLER
#define OCEANBASE_STORAGE_OB_LS_RECOVERY_STAT_HANDLER

#include "lib/ob_define.h"
#include "rootserver/ob_rs_async_rpc_proxy.h" //ObGetLSReplayedScnProxy
#include "share/ls/ob_ls_recovery_stat_operator.h" // ObLSRecoveryStatOperator
#include "logservice/palf/palf_handle_impl.h"                  // PalfStat
#include "logservice/palf/log_meta_info.h"//LogConfigVersion

namespace oceanbase
{

namespace storage
{
class ObLSHandle;
}

namespace rootserver
{
class ObLSRecoveryStatHandler;
class TestLSRecoveryGuard;
struct ObLSReplicaReadableSCN
{
public:
  ObLSReplicaReadableSCN() : server_(), readable_scn_() {}
  ~ObLSReplicaReadableSCN() {}
  int init(const common::ObAddr &server, const share::SCN &readable_scn);

  share::SCN get_readable_scn() const
  {
    return readable_scn_;
  }
  common::ObAddr get_server() const
  {
    return server_;
  }
  TO_STRING_KV(K_(server), K_(readable_scn));
private:
  common::ObAddr server_;
  share::SCN readable_scn_;
};

class ObLSRecoveryGuard
{
  //To implement a mutex for ls_recovery_stat reporting and member_list changes,
  //it needs to be used on the server that is the leader of the LS for this tenant.
  //Successful initialization indicates that the mutex has been acquired successfully.
  //This interface can also be invoked by meta and sys tenants without errors and without mutual exclusion.
  //These two types of tenants do not report on ls_recovery_stat; the interface is used solely to standardize member changes.
  // The detructor of the class will clear the reference count and reset any set information
public:
  ObLSRecoveryGuard() :
    tenant_id_(OB_INVALID_TENANT_ID), ls_handle_(), ls_recovery_stat_(NULL) {}
  ~ObLSRecoveryGuard();
  /**
   * @description:
   * In this interface, implement mutual exclusion,
   * which will wait for the reference count to drop to zero
   * within the timeout period, and then increment the reference count.
   * meta and sys only set tenant_id, nothing todo
   *
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: use to get ls_recovery_stat from ObLS
   * @param[in] timeout: Timeout period, with a default value of -1, which indicates no waiting.
   *                     The reference count will be incremented if possible without waiting in case of failure.
   *                     The system will attempt to increase the reference count successfully within the timeout period.
   * @return : OB_SUCCESS: Reference increment successful.
   *           OB_EAGAIN:Wait for reference increment to fail within the timeout period due to other reference points not being successfully released.
   */

  int init(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const int64_t &timeout = -1);
  //TODO 判断是否可以加减成员, meta or sys directly return success
  /*
   * @description: Used to check whether the readable_SCN of this member has surpassed the reported readable_SCN,
   * for the purpose of judging member changes and the addition of replicas during timeout.
   * @param[in] server: target server
   * @param[in] timeout: Timeout period
   * */
  int check_can_add_member(const ObMember &member, const int64_t timeout);
  /*
   * @description: Within the timeout period, determine whether the new member list can ensure that the readable_SCN does not regress.
   * @param[in] new_member_list : new_member_list
   * @param[in] paxos_replica_num : paxos_replica_num of new member_list
   * @param[in] timeout: Timeout period
   * **/
  int check_can_change_member(const ObMemberList &new_member_list,
                              const int64_t paxos_replica_num,
                              const palf::LogConfigVersion &config_version,
                              const int64_t timeout);
  friend class TestLSRecoveryGuard;
private:
  bool skip_check_member_list_change_(const uint64_t tenant_id);
private:

  uint64_t tenant_id_;
  ObLSHandle ls_handle_;
  ObLSRecoveryStatHandler *ls_recovery_stat_;
};

/**
  * @description:
  *    ObLSRecoveryStatHandler exists on the LS of each observer and is responsible for
  *    the each LS recovery stat
  */
class ObLSRecoveryStatHandler
{
public:
  ObLSRecoveryStatHandler() { reset(true); }
  ~ObLSRecoveryStatHandler() { reset(false); }
  void reset(const bool is_init = false);
  int init(const uint64_t tenant_id, ObLS *ls);
  /**
   * @description:
   *    get ls readable_scn considering readable scn, sync scn and replayable scn.
   * @param[out] readable_scn ls readable_scn
   * @return return code
   */
  int get_ls_replica_readable_scn(share::SCN &readable_scn);
  /**
   * @description:
   *    get ls level recovery_stat by LS leader.
   *    If follower LS replica call this function, it will return OB_NOT_MASTER.
   * @param[out] ls_recovery_stat
   * @return return code
   */
  int get_ls_level_recovery_stat(share::ObLSRecoveryStat &ls_recovery_stat);

  int set_add_replica_server(const common::ObAddr &server);
  void reset_add_replica_server();
  /*
  * @description:
  * get all ls replica readable and set to replicas_scn_;
  */
  int gather_replica_readable_scn();
  /*
   * @description:
   * Add a reference, which may be used for reporting or member list changes.
   * @param[in] timeout : Add a reference within the timeout range; if unsuccessful, wait for 100ms.
   * */
  int inc_ref(const int64_t timeout);
  /*
   * @description:
   * Subtract a reference.
   * */
  void dec_ref();
  /*
   * @description:
   * Set a readable_scn of inner_table in memory and use config_version for verification.
   * */
  int set_inner_readable_scn(const palf::LogConfigVersion &config_version,
      const share::SCN &readable_scn, bool check_inner_config_valid);
  /*
   * @description:
   * clear readable_scn and config_version of inner_table in memory;
   * */
  int reset_inner_readable_scn();

  /*
   * @description: get ls all paxos replica min readable_scn
   * @param[out] min readable_scn of all paxos replica
   * @return:
   * OB_NOT_MASTER : replica not master, can not get readable_scn of other replica
   * OB_NEED_RETRY : If there's no readable scn with all replicas;
   *                 the config_version corresponding to the current cached readable scn does not match the latest config_version;
   *                 or the config_version has changed during the statistical process.
   * */
  int get_all_replica_min_readable_scn(share::SCN &readable_scn);
  /*
   * @description: Used to check whether the readable_SCN of this member has surpassed the reported readable_SCN,
   * for the purpose of judging member changes and the addition of replicas during timeout.
   * @param[in] server: target server
   * @param[in] timeout: Timeout period
  * @return:
  */
  int wait_server_readable_scn(const common::ObAddr &server, const int64_t timeout);
  /*
   * @description: Within the timeout period, determine whether the new member list can ensure that the readable_SCN does not regress.
   * @param[in] new_member_list : new_member_list
   * @param[in] paxos_replica_num : paxos_replica_num of new member_list
   * @param[in] timeout: Timeout period
   * @return:
  */
  int wait_can_change_member_list(const ObMemberList &new_member_list,
      const int64_t paxos_replica_num, const palf::LogConfigVersion &config_version, const int64_t timeout);
  int64_t get_ref_cnt();
  TO_STRING_KV(K_(tenant_id), K_(ls), K(ref_cnt_));
  friend class TestLSRecoveryGuard;
  friend class ObLSRecoveryGuard;

private:
  int check_inner_stat_();

  /**
   * @description:
   *    increase LS readable_scn when replayable_scn is pushed forward in switchover
   * @param[in/out] readable_scn
   *                  in: actual readable_scn
   *                  out: increased readable_scn
   * @return return code
   */
  int increase_ls_replica_readable_scn_(share::SCN &sync_scn);

  int do_get_ls_level_readable_scn_(share::SCN &read_scn);

  /**
   * @description:
   *    do not use this function.
   *    Since PalfHandleGuard holds lock, it may cause deadlock with other palf operations,
   *    so use a separate function to obtain palf_stat, please do not add new operations in this function
   * @param[out] palf_stat
   * @return return code
   */
  int get_palf_stat_(
      palf::PalfStat &palf_stat);

  /**
   * @description:
   *    palf_stat get from  palf_handle_guard.stat can guarantee that <config_version, paxos_member_list, paxos_replica_num, degraded_list>
   *    is a snapshot, but because it is a cache, it may be not latest, in order to ensure that
   *    the latest palf_stat can be obtained, it is necessary to obtain the latest member list
   *    and compare with palf_stat. If they are same, the obtained palf_stat is considered to be latest.
   * @param[out] palf_stat
   * @return return code
   */
  int get_latest_palf_stat_(
      palf::PalfStat &palf_stat);
  int do_get_each_replica_readable_scn_(
      const ObIArray<common::ObAddr> &ob_member_list,
      ObArray<ObLSReplicaReadableSCN> &replicas_scn);
  int get_majority_readable_scn_(
      const share::SCN &leader_readable_scn,
      share::SCN &majority_min_readable_scn);
  int do_get_majority_readable_scn_(
      const ObIArray<common::ObAddr> &ob_member_list,
      const share::SCN &leader_readable_scn,
      const int64_t need_query_member_cnt,
      share::SCN &majority_min_readable_scn);
  int do_get_readable_scn_(
      const ObIArray<common::ObAddr> &ob_member_list,
      const int64_t paxos_replica_num,
      const palf::LogConfigVersion &config_version,
      const int64_t full_replica_num,
      share::SCN &majority_min_readable_scn);
  int do_get_member_readable_scn_(
      const ObIArray<common::ObAddr> &ob_member_list,
      const palf::LogConfigVersion &config_version,
      ObIArray<SCN> &replica_readble_scn);
  int do_get_max_readable_scn_(
      const ObIArray<common::ObAddr> &new_member_list,
      const palf::PalfStat &palf_stat,
      share::SCN &readable_scn);
  int calc_majority_min_readable_scn_(
      const share::SCN &leader_readable_scn,
      const int64_t majority_cnt,
      const ObIArray<int> &return_code_array,
      const ObGetLSReplayedScnProxy &proxy,
      share::SCN &majority_min_readable_scn);
  int do_calc_majority_min_readable_scn_(
    const int64_t majority_cnt,
    ObArray<SCN> &readable_scn_list,
    share::SCN &majority_min_readable_scn);
  int construct_new_member_list_(
      const common::ObMemberList &member_list_ori,
      const common::GlobalLearnerList &degraded_list,
      const int64_t paxos_replica_number_ori,
      ObIArray<common::ObAddr> &member_list_new,
      int64_t &paxos_replica_number_new,
      int64_t &full_replica_num);
  int check_can_use_new_version_(bool &vaild_to_use);
  int construct_addr_list_(const palf::PalfStat &palf_stat,
      ObIArray<common::ObAddr> &addr_list);
  int dump_all_replica_readable_scn_(const bool force_dump);

  int try_reload_and_fix_config_version_(const palf::LogConfigVersion &current_version);

  template<typename... Args>
  int wait_func_with_timeout_(const int64_t timeout, Args &&... args);
  int check_member_change_valid_(const common::ObAddr &server, bool &is_valid);
  int check_member_change_valid_(const ObMemberList &new_member_list,
      const int64_t paxos_replica_num, const palf::LogConfigVersion &config_version, bool &is_valid);
  DISALLOW_COPY_AND_ASSIGN(ObLSRecoveryStatHandler);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObLS *ls_;
  ObCond ref_cond_;//用于加锁等待唤醒
  int64_t ref_cnt_;//use for Concurrency control
  common::SpinRWLock lock_;
  //存储内部表和统计出来的最大值
  //只在config_version_in_inner_合法的时候有效
  share::SCN readable_scn_upper_limit_;//the max readable_scn of inner_table and memory
  palf::LogConfigVersion config_version_in_inner_;//config_version in inner_table
  common::ObAddr extra_server_;//for add replica, need to gather add_replica's readable_scn
  int64_t last_dump_ts_;//用于记录上次打印内存可读点的时间戳
  palf::LogConfigVersion config_version_;//记录统计可读点replicas_scn_使用的config_version
  //成员列表里面最多只有OB_MAX_MEMBER_NUMBER，这个时候可能触发迁移，所以需要增加一个成员
  ObSEArray<ObLSReplicaReadableSCN, OB_MAX_MEMBER_NUMBER + 1, ObNullAllocator> replicas_scn_;//缓存每个副本的可读位点
};

template <typename... Args>
inline int ObLSRecoveryStatHandler::wait_func_with_timeout_(
   const int64_t timeout, Args &&...args)
{
  int ret = OB_SUCCESS;
  bool is_valid_use = false;
  if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", KR(ret), K(timeout));
  } else if (OB_FAIL(check_can_use_new_version_(is_valid_use))) {
    RS_LOG(WARN, "failed to check use new version", KR(ret));
  } else if (!is_valid_use) {
    RS_LOG(INFO, "can not use readable in memory, no need to check", K(timeout));
  } else {
    int64_t current_timeout = timeout;
    const int64_t TIME_WAIT = 100 * 1000;
    bool is_finish = false;
    do {
      if (OB_FAIL(check_member_change_valid_(std::forward<Args>(args)..., is_finish))) {
        RS_LOG(WARN, "failed to check", KR(ret));
        if (OB_NEED_RETRY == ret) {
          // if check_member_change_valid return OB_NEED_RETRY
          // it means informations in ls recovery stat may not updated to latest yet
          // try wait before timeout
          ret = OB_SUCCESS;
          is_finish = false;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!is_finish) {
        if (current_timeout > TIME_WAIT) {
          ob_usleep(TIME_WAIT);
          current_timeout -= TIME_WAIT;
        } else {
          ret = OB_TIMEOUT;
          RS_LOG(WARN, "failed to wait server readable scn", KR(ret), K(timeout));
        }
      }
    } while (current_timeout >= 0 && OB_SUCC(ret) && !is_finish);

    if (OB_SUCC(ret) && !is_finish) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "must be timeout or finished", KR(ret), K(timeout));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(dump_all_replica_readable_scn_(true))) {
        RS_LOG(WARN, "failed to dump all replica readable scn", KR(ret), KR(tmp_ret));
      }
    }
  }
  return ret;
}

}  // namespace rootserver
}

#endif // OCEANBASE_STORAGE_OB_LS_RECOVERY_STAT_HANDLER
