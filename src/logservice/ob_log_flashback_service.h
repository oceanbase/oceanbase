// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_FLASHBACK_SERVICE_
#define OCEANBASE_LOGSERVICE_OB_LOG_FLASHBACK_SERVICE_

#include "lib/container/ob_array.h"                   //ObArray
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "logrpc/ob_log_rpc_proxy.h"
#include "logrpc/ob_log_rpc_req.h"
#include "palf/log_define.h"
#include "ob_location_adapter.h"
#include "ob_reporter_adapter.h"
#include "share/ls/ob_ls_status_operator.h"            // ObLSStatusInfo
// #include "share/ob_tenant_info_proxy.h"                // ObTenantRole

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace commom
{
class ObMySQLProxy;
}

namespace logservice
{
// Log Flashback Service for tenant
class ObLogFlashbackService
{
public:
  ObLogFlashbackService();
  virtual ~ObLogFlashbackService();
  void destroy();
public:
  int init(const common::ObAddr &self,
           logservice::ObLocationAdapter *location_adapter,
           obrpc::ObLogServiceRpcProxy *rpc_proxy,
           common::ObMySQLProxy *sql_proxy);
  // @desc: flashback all log_stream's redo log of tenant 'tenant_id'
  // @params [in] const uint64_t tenant_id: id of tenant which should be flashbacked
  // @params [in] const share::SCN &flashback_scn: flashback point
  // @params [in] const int64_t timeout_us: timeout time (us)
  // @return
  //   - OB_SUCCESS
  //   - OB_INVALID_ARGUEMENT: invalid tenant_id or flashback_scn
  //   - OB_NOT_SUPPORTED: meta tenant or sys tenant can't be flashbacked
  //   - OB_EAGAIN: another flashback operation is doing
  //   - OB_TIMEOUT: timeout
  int flashback(const uint64_t tenant_id, const share::SCN &flashback_scn, const int64_t timeout_us);
  int handle_flashback_resp(const LogFlashbackMsg &resp);
private:
  class BaseLSOperator
  {
  public:
    BaseLSOperator()
        : tenant_id_(OB_INVALID_TENANT_ID),
          ls_id_(),
          self_(),
          leader_(),
          flashback_scn_(),
          location_adapter_(NULL),
          rpc_proxy_(NULL),
          ret_(OB_NOT_INIT) { }
    BaseLSOperator(const uint64_t tenant_id,
                   const share::ObLSID &ls_id,
                   const common::ObAddr &self,
                   const share::SCN &flashback_scn,
                   logservice::ObLocationAdapter *location_adapter,
                   obrpc::ObLogServiceRpcProxy *rpc_proxy)
        : tenant_id_(tenant_id),
          ls_id_(ls_id),
          self_(self),
          leader_(),
          flashback_scn_(flashback_scn),
          location_adapter_(location_adapter),
          rpc_proxy_(rpc_proxy) { }
    virtual ~BaseLSOperator() { reset(); }
    void reset()
    {
      tenant_id_ = OB_INVALID_TENANT_ID;
      ls_id_.reset();
      self_.reset();
      leader_.reset();
      flashback_scn_.reset();
      location_adapter_ = NULL;
      rpc_proxy_ = NULL;
      ret_ = OB_NOT_INIT;
    }
    bool is_valid() const {
      // leader may be invalid
      return is_valid_tenant_id(tenant_id_) &&
             ls_id_.is_valid() &&
             self_.is_valid() &&
             flashback_scn_.is_valid() &&
             OB_NOT_NULL(location_adapter_) &&
             OB_NOT_NULL(rpc_proxy_);
    }
    virtual int switch_state() = 0;
    TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(leader), K_(flashback_scn), "inner_ret", ret_);
  protected:
    int update_leader_();
    int get_leader_palf_stat_(palf::PalfStat &palf_stat);
    int get_leader_list_(common::ObMemberList &member_list,
                         common::GlobalLearnerList &learner_list);
  public:
    uint64_t tenant_id_;
    share::ObLSID ls_id_;
    common::ObAddr self_;
    common::ObAddr leader_;
    share::SCN flashback_scn_;
    logservice::ObLocationAdapter *location_adapter_;
    obrpc::ObLogServiceRpcProxy *rpc_proxy_;
    int ret_;
  };

  class CheckLSLogSyncOperator : public BaseLSOperator
  {
  public:
    CheckLSLogSyncOperator()
        : BaseLSOperator(),
          has_get_member_list_(false),
          member_list_(),
          log_sync_memberlist_(),
          learner_list_(),
          log_sync_learnerlist_() { }
    CheckLSLogSyncOperator(
        const uint64_t tenant_id,
        const share::ObLSID &ls_id,
        const common::ObAddr &self,
        const share::SCN &flashback_scn,
        logservice::ObLocationAdapter *location_adapter,
        obrpc::ObLogServiceRpcProxy *rpc_proxy)
        : BaseLSOperator(tenant_id, ls_id, self, flashback_scn,
          location_adapter, rpc_proxy),
          has_get_member_list_(false),
          member_list_(),
          log_sync_memberlist_(),
          learner_list_(),
          log_sync_learnerlist_() { }
    virtual ~CheckLSLogSyncOperator()
    {
      has_get_member_list_ = false;
      member_list_.reset();
      log_sync_memberlist_.reset();
      learner_list_.reset();
      log_sync_learnerlist_.reset();
    }
    int switch_state() override final;
    TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(leader), K_(flashback_scn),
        K_(has_get_member_list), K_(member_list), K_(log_sync_memberlist),
        K_(learner_list), K_(log_sync_learnerlist));
  public:
    bool has_get_member_list_;
    common::ObMemberList member_list_;
    common::ObMemberList log_sync_memberlist_;
    common::GlobalLearnerList learner_list_;
    common::GlobalLearnerList log_sync_learnerlist_;
  private:
    template <typename LIST>
    int check_list_log_sync(const LIST &list,
                            LIST &sync_list,
                            int64_t &unsync_member_cnt)
    {
      int ret = OB_SUCCESS;
      const int64_t CONN_TIMEOUT_US = GCONF.rpc_timeout;
      for (int i = 0; i < list.get_member_number(); i++) {
        common::ObAddr server;
        int tmp_ret = OB_SUCCESS;
        LogGetPalfStatReq get_ts_req(self_, ls_id_.id(), false);
        LogGetPalfStatResp get_ts_resp;
        if (OB_SUCCESS != (tmp_ret = list.get_server_by_index(i, server))) {
        } else if (sync_list.contains(server)) {
          // has sync, do not need check
        } else if (OB_SUCCESS != (tmp_ret = rpc_proxy_->to(server).
            timeout(CONN_TIMEOUT_US).trace_time(true).
            max_process_handler_time(static_cast<int32_t>(CONN_TIMEOUT_US)).by(tenant_id_).
            get_palf_stat(get_ts_req, get_ts_resp))) {
          CLOG_LOG(WARN, "get_palf_stat failed", K(tmp_ret), KPC(this), K(get_ts_req));
          ret = OB_EAGAIN;
          // some replicas may has been removed, try get member_list again
          has_get_member_list_ = false;
          // Note: do not execute flashback until all uncommitted logs of all members have been committed
          // Otherwise, if a lag follower's end_scn is smaller than flashback_scn and we skip it,
          // log flying in the network may be received by the follower after the flashback operation
          // has been executed by the follower. That seems like the follower hasn't been flashbacked.
        } else if (get_ts_resp.palf_stat_.end_scn_ < get_ts_resp.palf_stat_.max_scn_) {
          ret = OB_EAGAIN;
        } else if (get_ts_resp.palf_stat_.end_scn_ < flashback_scn_) {
          ret = OB_EAGAIN;
          unsync_member_cnt += 1;
        } else {
          (void) sync_list.add_server(server);
        }
      }
      return ret;
    }
  };

  class ChangeAccessModeOperator : public BaseLSOperator
  {
  public:
    ChangeAccessModeOperator()
      : BaseLSOperator(),
        mode_version_(palf::INVALID_PROPOSAL_ID),
        dst_mode_(palf::AccessMode::INVALID_ACCESS_MODE) { }
    ChangeAccessModeOperator(
        const uint64_t tenant_id,
        const share::ObLSID &ls_id,
        const common::ObAddr &self,
        const share::SCN &flashback_scn,
        logservice::ObLocationAdapter *location_adapter,
        obrpc::ObLogServiceRpcProxy *rpc_proxy)
      : BaseLSOperator(tenant_id, ls_id, self, flashback_scn,
        location_adapter, rpc_proxy),
        mode_version_(palf::INVALID_PROPOSAL_ID),
        dst_mode_(palf::AccessMode::INVALID_ACCESS_MODE) { }
    virtual ~ChangeAccessModeOperator()
    {
      mode_version_ = palf::INVALID_PROPOSAL_ID;
      dst_mode_ = palf::AccessMode::INVALID_ACCESS_MODE;
    }
    int switch_state() override final;
    TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(leader), K_(flashback_scn),
        K_(mode_version), K_(dst_mode));
  public:
    int64_t mode_version_;
    palf::AccessMode dst_mode_;
  };

  class ExecuteFlashbackOperator : public BaseLSOperator
  {
  public:
    ExecuteFlashbackOperator()
        : BaseLSOperator(),
          mode_version_(palf::INVALID_PROPOSAL_ID),
          has_get_member_list_(false),
          member_list_(),
          flashbacked_memberlist_(),
          learner_list_(),
          flashbacked_learnerlist_() { }
    ExecuteFlashbackOperator(const ChangeAccessModeOperator &op)
      : BaseLSOperator(op.tenant_id_, op.ls_id_, op.self_, op.flashback_scn_,
        op.location_adapter_, op.rpc_proxy_),
        mode_version_(op.mode_version_),
        has_get_member_list_(false),
        member_list_(),
        flashbacked_memberlist_(),
        learner_list_(),
        flashbacked_learnerlist_() { }
    ExecuteFlashbackOperator &operator=(const ExecuteFlashbackOperator &op)
    {
      tenant_id_ = op.tenant_id_;
      ls_id_ = op.ls_id_;
      self_ = op.self_;
      leader_ = op.leader_;
      flashback_scn_ = op.flashback_scn_;
      location_adapter_ = op.location_adapter_;
      rpc_proxy_ = op.rpc_proxy_;
      mode_version_ = op.mode_version_;
      has_get_member_list_ = op.has_get_member_list_;
      member_list_ = op.member_list_;
      flashbacked_memberlist_ = op.flashbacked_memberlist_;
      learner_list_ = op.learner_list_;
      flashbacked_learnerlist_ = op.flashbacked_learnerlist_;
      return *this;
    }
    ExecuteFlashbackOperator(const ExecuteFlashbackOperator &op)
    {
      *this = op;
    }
    virtual ~ExecuteFlashbackOperator()
    {
      mode_version_ = palf::INVALID_PROPOSAL_ID;
      has_get_member_list_ = false;
      member_list_.reset();
      flashbacked_memberlist_.reset();
      learner_list_.reset();
      flashbacked_learnerlist_.reset();
    }
    int switch_state() override final;
    TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(leader), K_(flashback_scn), K_(mode_version),
        K_(has_get_member_list), K_(member_list), K_(flashbacked_memberlist),
        K_(learner_list), K_(flashbacked_learnerlist));
    int handle_flashback_resp(const LogFlashbackMsg &resp);
  public:
    common::ObSpinLock lock_;
    int64_t mode_version_;
    bool has_get_member_list_;
    common::ObMemberList member_list_;
    common::ObMemberList flashbacked_memberlist_;
    common::GlobalLearnerList learner_list_;
    common::GlobalLearnerList flashbacked_learnerlist_;
  private:
    template <typename LIST>
    int flashback_list_(const LIST &list, LIST &flashbacked_list)
    {
      int ret = OB_SUCCESS;
      const int64_t CONN_TIMEOUT_US = GCONF.rpc_timeout;
      const bool is_flashback_req = true;
      LogFlashbackMsg flashback_msg(MTL_ID(), self_, ls_id_.id(), mode_version_,
          flashback_scn_, is_flashback_req);
      for (int i = 0; i < list.get_member_number(); i++) {
        common::ObAddr server;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = list.get_server_by_index(i, server))) {
        } else if (flashbacked_list.contains(server)) {
          // has been flashbacked, skip
        } else if (OB_SUCCESS != (tmp_ret = rpc_proxy_->to(server).
            timeout(CONN_TIMEOUT_US).trace_time(true).
            max_process_handler_time(static_cast<int32_t>(CONN_TIMEOUT_US)).by(tenant_id_).
            send_log_flashback_msg(flashback_msg, NULL))) {
          CLOG_LOG(WARN, "send_log_flashback_msg failed", K(tmp_ret), KPC(this), K(flashback_msg));
          ret = OB_EAGAIN;
        } else {
          ret = OB_EAGAIN;
        }
      }
      return ret;
    }
  };

private:
  typedef common::ObArray<ObLogFlashbackService::CheckLSLogSyncOperator> CheckLogOpArray;
  typedef common::ObArray<ObLogFlashbackService::ChangeAccessModeOperator> ChangeModeOpArray;
  typedef common::ObArray<ObLogFlashbackService::ExecuteFlashbackOperator> FlashbackOpArray;

private:
  int get_ls_list_(const uint64_t tenant_id,
                   share::ObLSStatusInfoArray &ls_array);
  // @returns:
  //   - OB_TIMEOUT
  int wait_all_ls_replicas_log_sync_(const uint64_t tenant_id,
                                     const share::SCN &flashback_scn,
                                     const share::ObLSStatusInfoArray &ls_array,
                                     const int64_t timeout_us) const;
  int get_and_change_access_mode_(const uint64_t tenant_id,
                                  const share::SCN &flashback_scn,
                                  const palf::AccessMode &dst_mode,
                                  const share::ObLSStatusInfoArray &ls_array,
                                  const int64_t timeout_us,
                                  ChangeModeOpArray &ls_operator_array);
  int do_flashback_(const uint64_t tenant_id,
                    const share::SCN &flashback_scn,
                    const ChangeModeOpArray &mode_op_array,
                    const int64_t timeout_us);

  // util functions
  template<typename T>
  int construct_ls_operator_list_(
      const uint64_t tenant_id,
      const share::SCN &flashback_scn,
      const share::ObLSStatusInfoArray &ls_array,
      common::ObArray<T> &ls_operator_array) const;

  template<typename SRC_T, typename DST_T>
  int cast_ls_operator_list_(
      const common::ObArray<SRC_T> &src_array,
      common::ObArray<DST_T> &dst_array) const;

  template<typename T>
  int motivate_ls_operator_list_once_(common::ObArray<T> &ls_operator_array) const;

private:
  bool is_inited_;
  common::ObSpinLock lock_;
  common::ObAddr self_;
  FlashbackOpArray flashback_op_array_;
  logservice::ObLocationAdapter *location_adapter_;
  obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  common::ObMySQLProxy *sql_proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFlashbackService);
};
} // end namespace logservice
} // end namespace oceanbase
#endif // OCEANBASE_LOGSERVICE_OB_LOG_FLASHBACK_SERVICE_
