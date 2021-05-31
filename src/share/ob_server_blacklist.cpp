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

#include "ob_server_blacklist.h"
#include "common/ob_clock_generator.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_i_ps_cb.h"
#include "share/rpc/ob_batch_rpc.h"
#include "share/redolog/ob_log_disk_manager.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using rpc::frame::ObReqTransport;

namespace share {
uint64_t ObServerBlacklist::black_svr_cnt_ = 0;

bool ObServerBlacklist::ObMapRemoveFunctor::operator()(const ObCascadMember& member, const ObDstServerInfo& info)
{
  UNUSED(info);
  bool is_found = false;
  for (int64_t i = 0; !is_found && i < dst_list_.count(); ++i) {
    if (member == dst_list_.at(i)) {
      is_found = true;
    }
  }
  if (!is_found) {
    remove_cnt_++;
  }
  return (false == is_found);
}

bool ObServerBlacklist::ObMapResetFunctor::operator()(const ObCascadMember& member, ObDstServerInfo& info)
{
  UNUSED(member);
  if (info.is_in_blacklist_) {
    ATOMIC_DEC(&black_svr_cnt_);
  }
  info.reset();
  reset_cnt_++;
  return true;
}

bool ObServerBlacklist::ObMapMarkBlackFunctor::operator()(const ObCascadMember& member, ObDstServerInfo& info)
{
  const int64_t now = ObTimeUtility::current_time();
  if (info.last_send_timestamp_ > info.last_recv_timestamp_ &&
      now - info.last_recv_timestamp_ > BLACKLIST_MARK_THRESHOLD) {
    if (info.is_in_blacklist_) {
      // already in blacklist, skip
    } else {
      ATOMIC_INC(&black_svr_cnt_);
      info.is_in_blacklist_ = true;
      mark_cnt_++;
    }
    SHARE_LOG(WARN, "mark server in blacklist", K(member), K(info), K(black_svr_cnt_));
  }
  return true;
}

bool ObServerBlacklist::ObMapSendReqFunctor::operator()(const ObCascadMember& member, ObDstServerInfo& info)
{
  const int64_t curr_ts = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  if (NULL == blacklist_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "blacklist_ is NULL", K(ret), KP_(blacklist));
  } else if (curr_ts - info.last_send_timestamp_ < BLACKLIST_REQ_INTERVAL) {
    // not reach request interval
  } else {
    info.last_send_timestamp_ = curr_ts;
    ObBlacklistReq req(self_, curr_ts);
    if (OB_FAIL(blacklist_->send_req_(member, req))) {
      SHARE_LOG(WARN, "send_req_ failed", K(ret), K(member));
    } else {
      send_cnt_++;
    }
  }
  return (OB_SUCCESS == ret);
}

bool ObServerBlacklist::ObMapRespFunctor::operator()(const ObCascadMember& member, ObDstServerInfo& info)
{
  const int64_t req_send_ts = resp_.get_req_send_timestamp();
  const int64_t now = ObTimeUtility::current_time();
  if (info.last_send_timestamp_ != req_send_ts) {
    SHARE_LOG(WARN, "req_send_ts not match", K(member), K_(resp), K(info));
  } else {
    const int64_t rpc_trans_time = (now - req_send_ts) / 2;
    const int64_t clockdiff_time = std::abs(now - resp_.get_req_recv_timestamp() - rpc_trans_time);
    bool is_clockdiff_error = false;
    bool is_clog_disk_full = resp_.is_clog_disk_full();
    if (rpc_trans_time > RPC_TRANS_TIME_THRESHOLD) {
      SHARE_LOG(WARN, "[SERVER_BLACKLIST] rpc trans time is too large, attention !", K(rpc_trans_time), K(member));
    }
    if (clockdiff_time > CLOCKDIFF_TIME_THRESHOLD) {
      is_clockdiff_error = true;
      SHARE_LOG(WARN, "[SERVER_BLACKLIST] clock diff time is too large, attention !", K(clockdiff_time), K(member));
    }
    if (is_clog_disk_full) {
      SHARE_LOG(WARN, "[SERVER_BLACKLIST] dst's clog disk is full, attention !", K(is_clog_disk_full), K(member));
    }
    bool is_removed = info.is_in_blacklist_;
    info.is_in_blacklist_ = false;
    info.is_clockdiff_error_ = is_clockdiff_error;
    info.is_clog_disk_full_ = is_clog_disk_full;
    info.last_recv_timestamp_ = now;
    if (is_removed) {
      ATOMIC_DEC(&black_svr_cnt_);
      SHARE_LOG(INFO,
          "[SERVER_BLACKLIST] remove svr from blacklist finished",
          K(member),
          K(rpc_trans_time),
          K(clockdiff_time),
          K(is_clockdiff_error),
          K(black_svr_cnt_));
    }
  }
  return true;
}

bool ObServerBlacklist::ObMapIterFunctor::operator()(const ObCascadMember& member, ObDstServerInfo& info)
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  ObBlacklistInfo bl_info;
  bl_info.dst_svr_ = member.get_server();
  bl_info.dst_info_ = info;
  if (OB_SUCCESS != (tmp_ret = info_iter_.push(bl_info))) {
    SHARE_LOG(WARN, "info_iter_.push failed", K(tmp_ret));
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

ObServerBlacklist::ObServerBlacklist() : is_inited_(false), is_enabled_(true), self_(), batch_rpc_(NULL)
{}

ObServerBlacklist::~ObServerBlacklist()
{
  destroy();
}

void ObServerBlacklist::destroy()
{
  if (is_inited_) {
    reset();
    (void)dst_info_map_.destroy();
    SHARE_LOG(INFO, "ObServerBlacklist destroy finished");
  }
}

void ObServerBlacklist::reset()
{
  is_inited_ = false;
  is_enabled_ = true;
  black_svr_cnt_ = 0;
  self_.reset();
  batch_rpc_ = NULL;
}

ObServerBlacklist& ObServerBlacklist::get_instance()
{
  static ObServerBlacklist svr_blacklist;
  return svr_blacklist;
}

int ObServerBlacklist::init(const ObAddr& self, ObReqTransport* transport, ObBatchRpc* batch_rpc)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!self.is_valid() || NULL == transport || NULL == batch_rpc) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(self), KP(transport), KP(batch_rpc));
  } else if (OB_FAIL(blacklist_proxy_.init(transport, self))) {
    SHARE_LOG(WARN, "rpc_proxy_.init failed", K(ret));
  } else if (OB_FAIL(dst_info_map_.init(ObModIds::OB_SERVER_BLACKLIST))) {
    SHARE_LOG(WARN, "dst_info_map_ init failed", K(ret));
  } else {
    self_ = self;
    batch_rpc_ = batch_rpc;
    is_inited_ = true;
    SHARE_LOG(INFO, "ObServerBlacklist init success");
  }
  return ret;
}

int ObServerBlacklist::handle_req(const int64_t src_cluster_id, const ObBlacklistReq& req)
{
  int ret = OB_SUCCESS;
  if (!req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(req));
  } else {
    bool is_clog_disk_full = !OB_CLOG_DISK_MGR.is_disk_space_enough();
    ObBlacklistResp resp(self_, req.get_send_timestamp(), ObTimeUtility::current_time(), is_clog_disk_full);
    if (OB_FAIL(send_resp_(req.get_sender(), src_cluster_id, resp))) {
      SHARE_LOG(WARN, "send_resp_ failed", K(ret));
    }
  }
  return ret;
}

int ObServerBlacklist::handle_resp(const ObBlacklistResp& resp, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (!resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(resp));
  } else {
    ObMapRespFunctor functor(resp);
    if (OB_FAIL(dst_info_map_.operate(ObCascadMember(resp.get_sender(), cluster_id), functor))) {
      SHARE_LOG(WARN, "dst_info_map_.operate failed", K(ret));
    }
  }
  return ret;
}

int ObServerBlacklist::send_req_(const ObCascadMember& member, const ObBlacklistReq& req)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObServerBlacklist is not inited", K(ret));
  } else if (!member.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(member), K(req));
  } else if (OB_FAIL(blacklist_proxy_.send_req(member.get_server(), member.get_cluster_id(), req))) {
    SHARE_LOG(WARN, "send_req_ failed", K(ret));
  } else {
  }
  return ret;
}

int ObServerBlacklist::send_resp_(const ObAddr& server, const int64_t dst_cluster_id, const ObBlacklistResp& resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObServerBlacklist is not inited", K(ret));
  } else if (!server.is_valid() || !resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(server), K(resp));
  } else if (OB_FAIL(blacklist_proxy_.send_resp(server, dst_cluster_id, resp))) {
    SHARE_LOG(WARN, "send_resp failed", K(ret));
  } else {
  }
  return ret;
}

void ObServerBlacklist::run1()
{
  lib::set_thread_name("Blacklist");
  blacklist_loop_();
}

void ObServerBlacklist::blacklist_loop_()
{
  while (!has_set_stop()) {
    int tmp_ret = OB_SUCCESS;
    const int64_t start_time = ObTimeUtility::current_time();
    int64_t send_cnt = 0;
    ObSEArray<ObCascadMember, 32> server_array;
    if (IS_NOT_INIT) {
      SHARE_LOG(WARN, "ObServerBlacklist is not inited");
    } else if (false == ATOMIC_LOAD(&is_enabled_)) {
      // already closed, skip
    } else if (OB_SUCCESS != (tmp_ret = batch_rpc_->get_dst_svr_list(server_array))) {
      SHARE_LOG(WARN, "get_all_server_array failed", K(tmp_ret));
    } else {
      const int64_t count = server_array.count();
      for (int64_t i = 0; i < count; ++i) {
        const ObCascadMember& cur_member = server_array.at(i);
        const ObAddr& cur_server = cur_member.get_server();
        if (self_ == cur_server) {
          // skip self
        } else {
          // Insert dst_svr that is not in the map
          ObDstServerInfo svr_info;
          if (OB_ENTRY_NOT_EXIST == dst_info_map_.get(cur_member, svr_info)) {
            (void)dst_info_map_.insert_or_update(cur_member, svr_info);
          }
        }
      }
      // Clean up the dst_server that does not need to communicate in the map
      ObMapRemoveFunctor remove_functor(server_array);
      if (OB_SUCCESS != (tmp_ret = dst_info_map_.remove_if(remove_functor))) {
        SHARE_LOG(WARN, "dst_info_map_ remove_if failed", K(tmp_ret));
      }
      const int64_t remove_cnt = remove_functor.get_remove_cnt();
      if (remove_cnt > 0) {
        SHARE_LOG(INFO, "dst_info_map_ remove_if finished", K(tmp_ret), "remove count", remove_cnt);
      }
      // Statistics should be added to the server of the blacklist
      ObMapMarkBlackFunctor mark_functor(start_time);
      if (OB_SUCCESS != (tmp_ret = dst_info_map_.for_each(mark_functor))) {
        SHARE_LOG(WARN, "dst_info_map_ for_each failed", K(tmp_ret));
      }
      const int64_t mark_cnt = mark_functor.get_mark_cnt();
      if (mark_cnt > 0) {
        SHARE_LOG(INFO, "dst_info_map_ mark blacklist finished", K(tmp_ret), "mark count", mark_cnt);
      }
      // Send message to server in map
      ObMapSendReqFunctor send_req_functor(this, self_);
      if (OB_SUCCESS != (tmp_ret = dst_info_map_.for_each(send_req_functor))) {
        SHARE_LOG(WARN, "dst_info_map_ for_each failed", K(tmp_ret));
      }
      send_cnt = send_req_functor.get_send_cnt();
    }
    const int64_t cost_time = ObTimeUtility::current_time() - start_time;
    if (cost_time > 100 * 1000) {
      SHARE_LOG(WARN, "blacklist_loop cost too much time", K(cost_time));
    }
    int32_t sleep_time = BLACKLIST_LOOP_INTERVAL - static_cast<const int32_t>(cost_time);
    if (sleep_time < 0) {
      sleep_time = 0;
    }
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      SHARE_LOG(INFO,
          "blacklist_loop exec finished",
          K(cost_time),
          K_(is_enabled),
          "dst_server count",
          server_array.count(),
          K(send_cnt));
    }
    usleep(sleep_time);
  }
}

bool ObServerBlacklist::is_clockdiff_error(const share::ObCascadMember& member) const
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  ObDstServerInfo svr_info;
  if (IS_NOT_INIT) {
    SHARE_LOG(WARN, "ObServerBlacklist is not inited");
  } else if (false == ATOMIC_LOAD(&is_enabled_)) {
    // always return false when not enable server_blacklist
  } else if (OB_SUCCESS != (tmp_ret = dst_info_map_.get(member, svr_info))) {
    if (OB_ENTRY_NOT_EXIST != tmp_ret) {
      SHARE_LOG(WARN, "dst_info_map_.get failed", K(tmp_ret), K(member));
    }
  } else {
    bool_ret = svr_info.is_clockdiff_error_;
  }
  return bool_ret;
}

bool ObServerBlacklist::is_in_blacklist(const share::ObCascadMember& member) const
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  ObDstServerInfo svr_info;
  if (IS_NOT_INIT) {
    SHARE_LOG(WARN, "ObServerBlacklist is not inited");
  } else if (self_ == member.get_server()) {
    // always return false for self
  } else if (false == ATOMIC_LOAD(&is_enabled_)) {
    // always return false when not enable server_blacklist
  } else if (OB_SUCCESS != (tmp_ret = dst_info_map_.get(member, svr_info))) {
    if (OB_ENTRY_NOT_EXIST != tmp_ret) {
      SHARE_LOG(WARN, "dst_info_map_.get failed", K(tmp_ret));
    }
  } else {
    bool_ret = svr_info.is_in_blacklist_;
  }
  return bool_ret;
}

bool ObServerBlacklist::is_clog_disk_full(const share::ObCascadMember& member) const
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  ObDstServerInfo svr_info;
  if (IS_NOT_INIT) {
    SHARE_LOG(WARN, "ObServerBlacklist is not inited");
  } else if (self_ == member.get_server()) {
    bool_ret = !OB_CLOG_DISK_MGR.is_disk_space_enough();
  } else if (false == ATOMIC_LOAD(&is_enabled_)) {
    // always return false when not enable server_blacklist
  } else if (OB_SUCCESS != (tmp_ret = dst_info_map_.get(member, svr_info))) {
    if (OB_ENTRY_NOT_EXIST != tmp_ret) {
      SHARE_LOG(WARN, "dst_info_map_.get failed", K(tmp_ret));
    }
  } else {
    bool_ret = svr_info.is_clog_disk_full_;
  }
  return bool_ret;
}

bool ObServerBlacklist::is_empty() const
{
  return (ATOMIC_LOAD(&black_svr_cnt_) == 0);
}

void ObServerBlacklist::enable_blacklist()
{
  ATOMIC_STORE(&is_enabled_, true);
  SHARE_LOG(INFO, "enable_blacklist finished", K_(is_enabled));
}

void ObServerBlacklist::disable_blacklist()
{
  ATOMIC_STORE(&is_enabled_, false);
  (void)clear_blacklist();
  SHARE_LOG(INFO, "disable_blacklist finished", K_(is_enabled));
}

void ObServerBlacklist::clear_blacklist()
{
  int ret = OB_SUCCESS;
  ObMapResetFunctor functor;
  if (OB_FAIL(dst_info_map_.for_each(functor))) {
    SHARE_LOG(WARN, "dst_info_map_.for_each failed", K(ret));
  }
  SHARE_LOG(INFO, "clear_blacklist finished", K(ret), "reset count", functor.get_reset_cnt());
}

int ObServerBlacklist::iterate_blacklist_info(ObBlacklistInfoIterator& info_iter)
{
  int ret = OB_SUCCESS;
  ObMapIterFunctor functor(info_iter);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObServerBlacklist is not inited", K(ret));
  } else if (false == ATOMIC_LOAD(&is_enabled_)) {
    // always return false when not enable server_blacklist
  } else if (OB_FAIL(dst_info_map_.for_each(functor))) {
    SHARE_LOG(WARN, "dst_info_map_.for_each failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}
}  // namespace share
}  // namespace oceanbase
