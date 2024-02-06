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

#ifndef OCEANBASE_LOGSERVICE_LOG_NET_SERVICE_
#define OCEANBASE_LOGSERVICE_LOG_NET_SERVICE_

#include <stdint.h>
#include "lib/ob_errno.h"                   // ERRNO...
#include "log_meta_info.h"
#include "log_writer_utils.h"
#include "common/ob_member_list.h"          // ObMemberList
#include "log_rpc.h"                     // LogRpc
#include "log_req.h"                     // PushLogType

namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace palf
{
class LSN;
class LogPrepareMeta;
class LogConfigMeta;
class LogRpc;
class LogWriteBuf;

class LogNetService
{
public:
  LogNetService();
  ~LogNetService();

public:
  int init(const int64_t palf_id,
           LogRpc *log_rpc);
  void destroy();
  int start();

  template<class List>
  int submit_push_log_req(
      const List &member_list,
      const PushLogType &push_log_type,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const LSN &curr_lsn,
      const LogWriteBuf &write_buf)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(ERROR, "LogNetService has not inited!!!", K(ret));
    } else {
      LogPushReq push_log_req(push_log_type,
                              msg_proposal_id,
                              prev_log_proposal_id,
                              prev_lsn,
                              curr_lsn,
                              write_buf);
      ret = post_request_to_member_list_(member_list, push_log_req);
    }
    return ret;
  }

  template<class List>
  int submit_batch_push_log_req(
      const List &member_list,
      const PushLogType &push_log_type,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const LSN &curr_lsn,
      const LogWriteBuf &write_buf)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(ERROR, "LogNetService has not inited!!!", K(ret));
    } else {
      LogBatchPushReq push_log_req(push_log_type,
                                   msg_proposal_id,
                                   prev_log_proposal_id,
                                   prev_lsn,
                                   curr_lsn,
                                   write_buf);
      ret = post_request_to_member_list_(member_list, push_log_req);
      PALF_LOG(TRACE, "post_request_to_member_list_ success", K(member_list), K(push_log_req));
    }
    return ret;
  }

  int submit_push_log_req(
      const ObAddr &server,
      const PushLogType &push_log_type,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const LSN &curr_lsn,
      const LogWriteBuf &write_buf);


  int submit_push_log_resp(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const LSN &lsn,
      const bool is_batch);

  template<class List>
  int submit_prepare_meta_req(
      const List &member_list,
      const int64_t &log_proposal_id)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(ERROR, "LogNetService not inited!!!", K(ret), K(palf_id_));
    } else if (INVALID_PROPOSAL_ID == log_proposal_id) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(ERROR, "Invalid arguments!!!", K(ret), K(palf_id_),
          K(member_list), K(log_proposal_id));
    } else {
      LogPrepareReq prepare_meta_req(log_proposal_id);
      ret = post_request_to_member_list_(member_list, prepare_meta_req);
      PALF_LOG(INFO, "submit_prepare_meta_req success", K(ret), K(member_list));
    }
    return ret;
  }

  int submit_prepare_meta_resp(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const bool vote_granted,
      const int64_t &log_proposal_id,
      const LSN &max_flushed_lsn,
      const LSN &committed_end_lsn,
      const LogModeMeta &mode_meta);

  int submit_fetch_log_req(
      const common::ObAddr &server,
      const FetchLogType fetch_type,
      const int64_t msg_proposal_id,
      const LSN &prev_lsn,
      const LSN &lsn,
      const int64_t fetch_log_size,
      const int64_t fetch_log_count,
      const int64_t accepted_mode_pid);

  int submit_batch_fetch_log_resp(
    const common::ObAddr &server,
    const int64_t msg_proposal_id,
    const int64_t prev_log_proposal_id,
    const LSN &prev_lsn,
    const LSN &curr_lsn,
    const LogWriteBuf &write_buf);

  int submit_notify_rebuild_req(
    const ObAddr &server,
    const LSN &base_lsn,
    const LogInfo &base_prev_log_info);

  int submit_notify_fetch_log_req(
    const ObMemberList &dst_list);

  template<class List>
  int submit_change_config_meta_req(
      const List &member_list,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const int64_t &prev_mode_pid,
      const LogConfigMeta &config_meta)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      LogChangeConfigMetaReq req(msg_proposal_id, prev_log_proposal_id, prev_lsn,
          prev_mode_pid, config_meta);
      ret = post_request_to_member_list_(member_list, req);
    }
    return ret;
  }

  int submit_change_config_meta_resp(
      const common::ObAddr &server,
      const int64_t msg_proposal_id,
      const LogConfigVersion &config_version);

  template <class List>
  int submit_change_mode_meta_req(
      const List &member_list,
      const int64_t &msg_proposal_id,
      const bool is_applied_mode_meta,
      const LogModeMeta &mode_meta)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      LogChangeModeMetaReq req(msg_proposal_id, mode_meta, is_applied_mode_meta);
      ret = post_request_to_member_list_(member_list, req);
    }
    return ret;
  }

  int submit_change_mode_meta_resp(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id);

  int submit_config_change_pre_check_req(
      const common::ObAddr &server,
      const LogConfigVersion &config_version,
      const bool need_purge_throttling,
      const int64_t timeout_us,
      LogGetMCStResp &resp);

#ifdef OB_BUILD_ARBITRATION
  int submit_get_arb_member_info_req(
      const common::ObAddr &server,
      const int64_t timeout_us,
      LogGetArbMemberInfoResp &resp);
#endif

  int submit_register_parent_req(
      const common::ObAddr &server,
      const LogLearner &child_itself,
      const bool is_to_leader);

  int submit_register_parent_resp(
      const common::ObAddr &server,
      const LogLearner &parent_itself,
      const LogCandidateList &candidate_list,
      const RegisterReturn reg_ret);

  int submit_retire_parent_req(const common::ObAddr &server, const LogLearner &child_itself);
  int submit_retire_child_req(const common::ObAddr &server, const LogLearner &parent_itself);
  int submit_learner_keepalive_req(const common::ObAddr &server, const LogLearner &sender_itself);
  int submit_learner_keepalive_resp(const common::ObAddr &server, const LogLearner &sender_itself);
  int submit_committed_info_req(const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const int64_t prev_log_id,
      const int64_t &prev_log_proposal_id,
      const LSN &committed_end_lsn);
  template<class List>
  int submit_committed_info_req(
      const List &member_list,
      const int64_t &msg_proposal_id,
      const int64_t prev_log_id,
      const int64_t &prev_log_proposal_id,
      const LSN &committed_end_lsn)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(ERROR, "LogNetService has not inited!!!", K(ret));
    } else {
      CommittedInfo committed_info_req(msg_proposal_id, prev_log_id,
                              prev_log_proposal_id, committed_end_lsn);
      ret = post_request_to_member_list_(member_list, committed_info_req);
    }
    return ret;
  }
  int submit_get_stat_req(const common::ObAddr &server,
                          const int64_t timeout_us,
                          const LogGetStatReq &req,
                          LogGetStatResp &resp);

public:
  template <class ReqType>
  int post_request_to_server_(const common::ObAddr &server,
                              const ReqType &req);
  template <class ReqType, class List = common::ObMemberList>
  int post_request_to_member_list_(const List &member_list,
                                   const ReqType &req);
  template <class ReqType, class RespType>
  int post_sync_request_to_server_(const common::ObAddr &server,
                                   const int64_t timeout_us,
                                   const ReqType &req,
                                   RespType &resp);
private:
  int64_t palf_id_;
  LogRpc *log_rpc_;
  bool is_inited_;
};

template <class ReqType>
int LogNetService::post_request_to_server_(
    const common::ObAddr &server,
    const ReqType &req)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(log_rpc_->post_request(server, palf_id_, req))) {
    PALF_LOG(WARN, "LogRpc post_request failed", K(ret), K(palf_id_), K(req), K(server));
  } else {
    PALF_LOG(TRACE, "post_request_to_server_ success", K(ret), K(server), K(palf_id_), K(req));
  }
  return common::OB_SUCCESS;
}

template <class ReqType, class List>
int LogNetService::post_request_to_member_list_(
    const List &member_list,
    const ReqType &req)
{
  int ret = common::OB_SUCCESS;
  int64_t member_number = member_list.get_member_number();
  common::ObAddr server;
  if (!req.is_valid() || !member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; i < member_number; i++) {
      if (OB_FAIL(member_list.get_server_by_index(i, server))) {
        PALF_LOG(WARN, "ObMemberList get_server_by_index failed", K(ret),
            K(server), K(palf_id_), K(req));
      } else if (OB_FAIL(post_request_to_server_(server, req))) {
        // PALF_LOG(WARN, "post_request_to_server_ failed", K(ret),
        //     K(server), K(palf_id_), K(server));
      } else {
      }
    }
  }
  return ret;
}

template <class ReqType, class RespType>
int LogNetService::post_sync_request_to_server_(const common::ObAddr &server,
                                                const int64_t timeout_us,
                                                const ReqType &req,
                                                RespType &resp)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(log_rpc_->post_sync_request(server, palf_id_, timeout_us, req, resp))) {
    CLOG_LOG(WARN, "ObLogRpc post_sync_request failed", K(ret), K_(palf_id),
        K(req), K(server));
  } else {
    CLOG_LOG(TRACE, "post_sync_request_to_server_ success", K(ret), K(server), K(palf_id_), K(req));
  }
  return ret;
}
} // end namespace palf
} // end namespace oceanbase

#endif
