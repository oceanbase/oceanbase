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

#define USING_LOG_PREFIX PALF
#include "palf_handle_impl.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"                              // OB_SUCCESS...
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"                   // PALF_LOG
#include "common/ob_member_list.h"                        // ObMemberList
#include "common/ob_role.h"                               // ObRole
#include "fetch_log_engine.h"
#include "log_engine.h"                                // LogEngine
#include "election/interface/election_priority.h"
#include "palf_iterator.h"                             // Iterator
#include "palf_env_impl.h"                             // PalfEnvImpl::

namespace oceanbase
{
using namespace common;
using namespace palf::election;
namespace palf
{
PalfHandleImpl::PalfHandleImpl()
  : lock_(),
    sw_(),
    config_mgr_(),
    mode_mgr_(),
    state_mgr_(),
    reconfirm_(),
    log_engine_(),
    election_msg_sender_(log_engine_.log_net_service_),
    election_(),
    fetch_log_engine_(NULL),
    allocator_(NULL),
    palf_id_(INVALID_PALF_ID),
    self_(),
    fs_cb_wrapper_(),
    role_change_cb_wrpper_(),
    rebuild_cb_wrapper_(),
    lc_cb_(NULL),
    last_locate_ts_ns_(OB_INVALID_TIMESTAMP),
    last_locate_block_(LOG_INVALID_BLOCK_ID),
    cannot_recv_log_warn_time_(OB_INVALID_TIMESTAMP),
    cannot_handle_committed_info_time_(OB_INVALID_TIMESTAMP),
    log_disk_full_warn_time_(OB_INVALID_TIMESTAMP),
    last_check_parent_child_ts_us_(OB_INVALID_TIMESTAMP),
    wait_slide_print_time_us_(OB_INVALID_TIMESTAMP),
    append_size_stat_time_us_(OB_INVALID_TIMESTAMP),
    replace_member_print_time_us_(OB_INVALID_TIMESTAMP),
    config_change_print_time_us_(OB_INVALID_TIMESTAMP),
    last_rebuild_lsn_(),
    last_record_append_lsn_(PALF_INITIAL_LSN_VAL),
    has_set_deleted_(false),
    palf_env_impl_(NULL),
    append_cost_stat_("[PALF STAT WRITE LOG]", 2 * 1000 * 1000),
    flush_cb_cost_stat_("[PALF STAT FLUSH CB]", 2 * 1000 * 1000),
    replica_meta_lock_(),
    rebuilding_lock_(),
    config_change_lock_(),
    mode_change_lock_(),
    last_dump_info_ts_us_(OB_INVALID_TIMESTAMP),
    is_inited_(false)
{
  log_dir_[0] = '\0';
}

PalfHandleImpl::~PalfHandleImpl()
{
  destroy();
}

int PalfHandleImpl::init(const int64_t palf_id,
                         const AccessMode &access_mode,
                         const PalfBaseInfo &palf_base_info,
                         FetchLogEngine *fetch_log_engine,
                         const char *log_dir,
                         ObILogAllocator *alloc_mgr,
                         ILogBlockPool *log_block_pool,
                         LogRpc *log_rpc,
                         LogIOWorker *log_io_worker,
                         PalfEnvImpl *palf_env_impl,
                         const common::ObAddr &self,
                         common::ObOccamTimer *election_timer,
                         const int64_t palf_epoch)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  LogMeta log_meta;
  LogSnapshotMeta snapshot_meta;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogServer has inited", K(ret), K(palf_id));
  } else if (false == is_valid_palf_id(palf_id)
             || false == is_valid_access_mode(access_mode)
             || false == palf_base_info.is_valid()
             || NULL == fetch_log_engine
             || NULL == log_dir
             || NULL == alloc_mgr
             || NULL == log_block_pool
             || NULL == log_rpc
             || NULL == log_io_worker
             || NULL == palf_env_impl
             || false == self.is_valid()
             || NULL == election_timer
             || palf_epoch < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(palf_base_info),
        K(access_mode), K(log_dir), K(alloc_mgr), K(log_block_pool), K(log_rpc),
        K(log_io_worker), K(palf_env_impl), K(self), K(election_timer), K(palf_epoch));
  } else if (OB_FAIL(log_meta.generate_by_palf_base_info(palf_base_info, access_mode))) {
    PALF_LOG(WARN, "generate_by_palf_base_info failed", K(ret), K(palf_id), K(palf_base_info), K(access_mode));
  } else if ((pret = snprintf(log_dir_, MAX_PATH_SIZE, "%s", log_dir)) && false) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "error unexpected", K(ret), K(palf_id));
  } else if (OB_FAIL(log_engine_.init(palf_id, log_dir, log_meta, alloc_mgr, log_block_pool, log_rpc, \
          log_io_worker, palf_epoch))) {
    PALF_LOG(WARN, "LogEngine init failed", K(ret), K(palf_id), K(log_dir), K(alloc_mgr),
        K(log_rpc), K(log_io_worker));
  } else if (OB_FAIL(do_init_mem_(palf_id, palf_base_info, log_meta, log_dir, self, fetch_log_engine,
          alloc_mgr, log_rpc, log_io_worker, palf_env_impl, election_timer))) {
    PALF_LOG(WARN, "PalfHandleImpl do_init_mem_ failed", K(ret), K(palf_id));
  } else {
    PALF_EVENT("PalfHandleImpl init success", palf_id_, K(ret), K(access_mode), K(palf_base_info),
        K(log_dir), K(log_meta), K(palf_epoch));
  }
  return ret;
}

bool PalfHandleImpl::check_can_be_used() const
{
  return false == ATOMIC_LOAD(&has_set_deleted_);
}

int PalfHandleImpl::load(const int64_t palf_id,
                         FetchLogEngine *fetch_log_engine,
                         const char *log_dir,
                         ObILogAllocator *alloc_mgr,
                         ILogBlockPool *log_block_pool,
                         LogRpc *log_rpc,
                         LogIOWorker *log_io_worker,
                         PalfEnvImpl *palf_env_impl,
                         const common::ObAddr &self,
                         common::ObOccamTimer *election_timer,
                         const int64_t palf_epoch)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  LogGroupEntryHeader entry_header;
  LSN max_committed_end_lsn;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (false == is_valid_palf_id(palf_id)
             || NULL == log_dir
             || NULL == fetch_log_engine
             || NULL == alloc_mgr
             || NULL == log_rpc
             || NULL == log_io_worker
             || false == self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(log_dir), K(alloc_mgr),
        K(log_rpc), K(log_io_worker));
  } else if (OB_FAIL(log_engine_.load(palf_id, log_dir, alloc_mgr, log_block_pool, log_rpc,
        log_io_worker, entry_header, palf_epoch))) {
    PALF_LOG(WARN, "LogEngine load failed", K(ret), K(palf_id));
    // NB: when 'entry_header' is invalid, means that there is no data on disk, and set max_committed_end_lsn
    //     to 'base_lsn_', we will generate default PalfBaseInfo or get it from LogSnapshotMeta(rebuild).
  } else if (FALSE_IT(max_committed_end_lsn =
        (true == entry_header.is_valid() ? entry_header.get_committed_end_lsn() : log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_))) {
  } else if (OB_FAIL(construct_palf_base_info_(max_committed_end_lsn, palf_base_info))) {
    PALF_LOG(WARN, "construct_palf_base_info_ failed", K(ret), K(palf_id), K(entry_header), K(palf_base_info));
  } else if (OB_FAIL(do_init_mem_(palf_id, palf_base_info, log_engine_.get_log_meta(), log_dir, self,
          fetch_log_engine, alloc_mgr, log_rpc, log_io_worker, palf_env_impl, election_timer))) {
    PALF_LOG(WARN, "PalfHandleImpl do_init_mem_ failed", K(ret), K(palf_id));
  } else if (OB_FAIL(append_disk_log_to_sw_(max_committed_end_lsn))) {
    PALF_LOG(WARN, "append_disk_log_to_sw_ failed", K(ret), K(palf_id));
  } else {
    PALF_EVENT("PalfHandleImpl load success", palf_id_, K(ret), K(palf_base_info), K(log_dir), K(palf_epoch));
  }
  return ret;
}

void PalfHandleImpl::destroy()
{
  WLockGuard guard(lock_);
  if (IS_INIT) {
    PALF_EVENT("PalfHandleImpl destroy", palf_id_, KPC(this));
    is_inited_ = false;
    lc_cb_ = NULL;
    self_.reset();
    palf_id_ = INVALID_PALF_ID;
    fetch_log_engine_ = NULL;
    allocator_ = NULL;
    election_.stop();
    log_engine_.destroy();
    reconfirm_.destroy();
    state_mgr_.destroy();
    config_mgr_.destroy();
    mode_mgr_.destroy();
    sw_.destroy();
    if (false == check_can_be_used()) {
      palf_env_impl_->remove_directory(log_dir_);
    }
    palf_env_impl_ = NULL;
  }
}

int PalfHandleImpl::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited!!!", K(ret));
  } else {
    PALF_LOG(INFO, "PalfHandleImpl start success", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::set_initial_member_list(
    const common::ObMemberList &member_list,
    const int64_t paxos_replica_num)
{
  int ret = OB_SUCCESS;
  LogConfigVersion config_version;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl has not inited!!!", K(ret));
  } else {
    {
      WLockGuard guard(lock_);
      const int64_t proposal_id = state_mgr_.get_proposal_id();
      if (OB_FAIL(config_mgr_.set_initial_member_list(member_list, paxos_replica_num, proposal_id, config_version))) {
        PALF_LOG(WARN, "LogConfigMgr set_initial_member_list failed", K(ret), KPC(this));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(config_mgr_.wait_config_log_persistence(config_version))) {
      PALF_LOG(WARN, "want_config_log_persistence failed", K(ret), KPC(this));
    } else {
      PALF_EVENT("set_initial_member_list success", palf_id_, K(ret), K(member_list), K(paxos_replica_num));
    }
  }
  return ret;
}

int PalfHandleImpl::set_initial_member_list(
    const common::ObMemberList &member_list,
    const common::ObMember &arb_replica,
    const int64_t paxos_replica_num)
{
  int ret = OB_SUCCESS;
  LogConfigVersion config_version;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl has not inited!!!", K(ret));
  } else {
    {
      WLockGuard guard(lock_);
      const int64_t proposal_id = state_mgr_.get_proposal_id();
      if (OB_FAIL(config_mgr_.set_initial_member_list(member_list, arb_replica, paxos_replica_num, proposal_id, config_version))) {
        PALF_LOG(WARN, "LogConfigMgr set_initial_member_list failed", K(ret), KPC(this));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(config_mgr_.wait_config_log_persistence(config_version))) {
      PALF_LOG(WARN, "want_config_log_persistence failed", K(ret), KPC(this));
    } else {
      PALF_EVENT("set_initial_member_list success", palf_id_, K(ret), K(member_list), K(arb_replica), K(paxos_replica_num));
    }
  }
  return ret;
}

int PalfHandleImpl::get_begin_lsn(LSN &lsn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", K(ret), KPC(this));
  } else {
    lsn = log_engine_.get_begin_lsn();
  }
  return ret;
}

int PalfHandleImpl::get_begin_ts_ns(int64_t &ts) const
{
  int ret = OB_SUCCESS;
  block_id_t unused_block_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", K(ret), KPC(this));
  } else if (OB_FAIL(log_engine_.get_min_block_info(unused_block_id, ts))) {
    PALF_LOG(WARN, "LogEngine get_min_block_info failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::get_base_info(const LSN &base_lsn,
                                  PalfBaseInfo &base_info)
{
  int ret = OB_SUCCESS;
  LSN curr_end_lsn = get_end_lsn();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", K(ret), KPC(this));
  } else if (base_lsn > curr_end_lsn) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(base_lsn), K(curr_end_lsn));
  } else if (OB_FAIL(construct_palf_base_info_(base_lsn, base_info))) {
    PALF_LOG(WARN, "construct_palf_base_info_ failed", K(ret), KPC(this), K(base_lsn), K(curr_end_lsn));
  } else {
    PALF_LOG(INFO, "get_base_info success", K(ret), KPC(this), K(base_lsn), K(curr_end_lsn), K(base_info));
  }
  return ret;
}

int PalfHandleImpl::set_region(const common::ObRegion &region)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(config_mgr_.set_region(region))) {
    PALF_LOG(WARN, "set_region failed", KR(ret), K(region));
  }
  return ret;
}

int PalfHandleImpl::set_paxos_member_region_map(const LogMemberRegionMap &region_map)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (config_mgr_.set_paxos_member_region_map(region_map)) {
    PALF_LOG(WARN, "set_paxos_member_region_map failed", KR(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::submit_log(
    const PalfAppendOptions &opts,
    const char *buf,
    const int64_t buf_len,
    const int64_t ref_ts_ns,
    LSN &lsn,
    int64_t &log_timestamp)
{
  int ret = OB_SUCCESS;
  const int64_t curr_ts_ns = common::ObTimeUtility::current_time_ns();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl is not inited", K(ret));
  } else if (NULL == buf || buf_len <= 0 || buf_len > MAX_LOG_BODY_SIZE
      || ref_ts_ns > curr_ts_ns + MAX_ALLOWED_SKEW_FOR_REF_TS_NS) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), KP(buf), K(buf_len), K(ref_ts_ns));
  } else {
    RLockGuard guard(lock_);
    if (false == palf_env_impl_->check_disk_space_enough()) {
      ret = OB_LOG_OUTOF_DISK_SPACE;
      if (palf_reach_time_interval(1 * 1000 * 1000, log_disk_full_warn_time_)) {
        PALF_LOG(WARN, "log outof disk space", K(ret), KPC(this), K(opts), K(ref_ts_ns));
      }
    } else if (!state_mgr_.can_append(opts.proposal_id, opts.need_check_proposal_id)) {
      ret = OB_NOT_MASTER;
      PALF_LOG(WARN, "cannot submit_log", K(ret), KPC(this), KP(buf), K(buf_len), "role",
          state_mgr_.get_role(), "state", state_mgr_.get_state(), "proposal_id",
          state_mgr_.get_proposal_id(), K(opts));
    } else if (OB_FAIL(sw_.submit_log(buf, buf_len, ref_ts_ns, lsn, log_timestamp))) {
      PALF_LOG(WARN, "submit_log failed", K(ret), KPC(this), KP(buf), K(buf_len));
    } else {
      PALF_LOG(TRACE, "submit_log success", K(ret), KPC(this), K(buf_len), K(lsn), K(log_timestamp));
      if (palf_reach_time_interval(2 * 1000 * 1000, append_size_stat_time_us_)) {
        PALF_LOG(INFO, "[PALF STAT APPEND DATA SIZE]", KPC(this), "append size", lsn.val_ - last_record_append_lsn_.val_);
        last_record_append_lsn_ = lsn;
      }
    }
  }
  return ret;
}

int PalfHandleImpl::get_role(common::ObRole &role,
                             int64_t &proposal_id,
                             bool &is_pending_state) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret));
  } else {
    ObRole curr_role = INVALID_ROLE;
    ObReplicaState curr_state = INVALID_STATE;
    int64_t curr_leader_epoch = -1;
    do {
      is_pending_state = mode_mgr_.is_in_pending_state();
      // 获取当前的proposal_id
      proposal_id = state_mgr_.get_proposal_id();
      // 获取当前的leader_epoch
      curr_leader_epoch = state_mgr_.get_leader_epoch();
      // 获取当前的role, state
      state_mgr_.get_role_and_state(curr_role, curr_state);
      if (LEADER != curr_role || ACTIVE != curr_state) {
        // not <LEADER, ACTIVE>
        role = common::FOLLOWER;
        if (RECONFIRM == curr_state || PENDING == curr_state) {
          is_pending_state = true;
        }
      } else if (false == state_mgr_.check_epoch_is_same_with_election(curr_leader_epoch)) {
        // PALF当前是<LEADER, ACTIVE>, 但epoch在election层发生了变化,
        // 说明election已经切主, PALF最终一定会切为<FOLLOWER, PENDING>, 这种情况也当做pending state,
        // 否则调用者看到PALF是<FOLLOWER, ACTIVE>, 但proposal_id却还是LEADER时的值,
        // 可能会导致非预期行为, 比如role change service可能会提前执行卸任操作.
        role = common::FOLLOWER;
        is_pending_state = true;
      } else {
        // 返回LEADER
        role = common::LEADER;
      }
      // double check proposal_id
      if (proposal_id == state_mgr_.get_proposal_id()) {
        // proposal_id does not change, exit loop
        break;
      } else {
        // proposal_id has changed, need retry
      }
    } while(true);
  }
  return ret;
}

int PalfHandleImpl::change_leader_to(const common::ObAddr &dest_addr)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret), K(dest_addr));
  } else if (OB_FAIL(election_.change_leader_to(dest_addr))) {
    PALF_LOG(WARN, "election can not change leader", K(ret), KPC(this), K(dest_addr));
  } else {
    PALF_EVENT("election leader will be changed", palf_id_, K(ret), KPC(this), K(dest_addr));
  }
  return ret;
}

int PalfHandleImpl::get_global_learner_list(common::GlobalLearnerList &learner_list) const
{
  RLockGuard guard(lock_);
  return config_mgr_.get_global_learner_list(learner_list);
}

int PalfHandleImpl::get_paxos_member_list(
    common::ObMemberList &member_list,
    int64_t &paxos_replica_num) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret));
  } else if (OB_FAIL(config_mgr_.get_curr_member_list(member_list))) {
    PALF_LOG(WARN, "get_curr_member_list failed", K(ret), KPC(this));
  } else if (OB_FAIL(config_mgr_.get_replica_num(paxos_replica_num))) {
    PALF_LOG(WARN, "get_replica_num failed", K(ret), KPC(this));
  } else {}
  return ret;
}

int PalfHandleImpl::get_memberchange_status(const ObAddr &server,
                                            const LogGetMCStReq &req,
                                            LogGetMCStResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret), K_(palf_id));
  } else {
    RLockGuard guard(lock_);
    int64_t curr_proposal_id = state_mgr_.get_proposal_id();
    resp.msg_proposal_id_ = curr_proposal_id;
    LogConfigVersion curr_config_version;
    if (OB_FAIL(config_mgr_.get_config_version(curr_config_version))) {
    } else if (req.config_version_ < curr_config_version) {
      resp.is_normal_replica_ = false;
    } else {
      LSN max_flushed_end_lsn;
      sw_.get_max_flushed_end_lsn(max_flushed_end_lsn);
      resp.is_normal_replica_ = true;
      resp.max_flushed_end_lsn_ = max_flushed_end_lsn;
      resp.need_update_config_meta_ = (req.config_version_ > curr_config_version);
    }
    PALF_LOG(INFO, "get_memberchange_status success", K(ret), KPC(this), K(server),
        K(req), K(resp), K(curr_config_version));
  }
  return ret;
}

int PalfHandleImpl::change_replica_num(
    const common::ObMemberList &member_list,
    const int64_t curr_replica_num,
    const int64_t new_replica_num,
    const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member_list.is_valid() ||
             false == is_valid_replica_num(curr_replica_num) ||
             false == is_valid_replica_num(new_replica_num) ||
             timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member_list), K(curr_replica_num),
        K(new_replica_num), K(timeout_ns));
  } else {
    LogConfigChangeArgs args(member_list, curr_replica_num, new_replica_num, CHANGE_REPLICA_NUM);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "change_replica_num failed", KR(ret), KPC(this), K(member_list),
          K(curr_replica_num), K(new_replica_num));
    } else {
      PALF_EVENT("change_replica_num success", palf_id_, KR(ret), KPC(this), K(member_list),
          K(curr_replica_num), K(new_replica_num));
    }
  }
  return ret;
}

int PalfHandleImpl::add_member(
    const common::ObMember &member,
    const int64_t new_replica_num,
    const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member.is_valid() ||
             false == is_valid_replica_num(new_replica_num) ||
             timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(new_replica_num), K(timeout_ns));
  } else {
    LogConfigChangeArgs args(member, new_replica_num, ADD_MEMBER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "add_member failed", KR(ret), KPC(this), K(member), K(new_replica_num));
    } else {
      PALF_EVENT("add_member success", palf_id_, KR(ret), KPC(this), K(member), K(new_replica_num));
    }
  }
  return ret;
}

int PalfHandleImpl::remove_member(
    const common::ObMember &member,
    const int64_t new_replica_num,
    const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member.is_valid() ||
             false == is_valid_replica_num(new_replica_num) ||
             timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(new_replica_num), K(timeout_ns));
  } else {
    LogConfigChangeArgs args(member, new_replica_num, REMOVE_MEMBER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "remove_member failed", KR(ret), KPC(this), K(member), K(new_replica_num));
    } else {
      PALF_EVENT("remove_member success", palf_id_, KR(ret), KPC(this), K(member), K(new_replica_num));
    }
  }
  return ret;
}

int PalfHandleImpl::replace_member(
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!added_member.is_valid() ||
             !removed_member.is_valid() ||
             timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_member), K(removed_member), K(timeout_ns));
  } else {
    ObMemberList old_member_list, curr_member_list;
    LogConfigChangeArgs args(added_member, 0, ADD_MEMBER_AND_NUM);
    const int64_t begin_ts_ns = common::ObTimeUtility::current_time_ns();
    if (OB_FAIL(config_mgr_.get_curr_member_list(old_member_list))) {
      PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), KPC(this));
    } else if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "add_member in replace_member failed", KR(ret), KPC(this), K(args));
    } else if (FALSE_IT(args.server_ = removed_member)) {
    } else if (FALSE_IT(args.type_ = REMOVE_MEMBER_AND_NUM)) {
    } else if (OB_FAIL(one_stage_config_change_(args, timeout_ns + begin_ts_ns - common::ObTimeUtility::current_time_ns()))) {
      if (palf_reach_time_interval(100 * 1000, replace_member_print_time_us_)) {
        PALF_LOG(WARN, "remove_member in replace_member failed", KR(ret), K(args), KPC(this));
      }
    } else if (OB_FAIL(config_mgr_.get_curr_member_list(curr_member_list))) {
      PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), KPC(this));
    } else {
      PALF_EVENT("replace_member success", palf_id_, KR(ret), KPC(this), K(added_member),
          K(removed_member), K(timeout_ns), K(old_member_list), K(curr_member_list),
          "leader replace_member cost time(ns)", common::ObTimeUtility::current_time_ns() - begin_ts_ns);
    }
  }
  return ret;
}

int PalfHandleImpl::add_learner(const common::ObMember &added_learner, const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!added_learner.is_valid() || timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    LogConfigChangeArgs args(added_learner, 0, ADD_LEARNER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "add_learner failed", KR(ret), KPC(this), K(args), K(timeout_ns));
    } else {
      PALF_EVENT("add_learner success", palf_id_, K(ret), KPC(this), K(args), K(timeout_ns));
    }
  }
  return ret;
}

int PalfHandleImpl::remove_learner(const common::ObMember &removed_learner, const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!removed_learner.is_valid() || timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    LogConfigChangeArgs args(removed_learner, 0, REMOVE_LEARNER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "remove_learner failed", KR(ret), KPC(this), K(args), K(timeout_ns));
    } else {
      PALF_EVENT("remove_learner success", palf_id_, K(ret), KPC(this), K(args), K(timeout_ns));
    }
  }
  return ret;
}

int PalfHandleImpl::switch_learner_to_acceptor(const common::ObMember &learner, const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!learner.is_valid() || timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    LogConfigChangeArgs args(learner, 0, SWITCH_LEARNER_TO_ACCEPTOR);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "switch_learner_to_acceptor failed", KR(ret), KPC(this), K(args), K(timeout_ns));
    } else {
      PALF_EVENT("switch_learner_to_acceptor success", palf_id_, K(ret), KPC(this), K(args), K(timeout_ns));
    }
  }
  return ret;
}

int PalfHandleImpl::switch_acceptor_to_learner(const common::ObMember &member, const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!member.is_valid() || timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    LogConfigChangeArgs args(member, 0, SWITCH_ACCEPTOR_TO_LEARNER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "switch_acceptor_to_learner failed", KR(ret), KPC(this), K(args), K(timeout_ns));
    } else {
      PALF_EVENT("switch_acceptor_to_learner success", palf_id_, K(ret), KPC(this), K(args), K(timeout_ns));
    }
  }
  return ret;
}

int PalfHandleImpl::add_arb_member(
    const common::ObMember &member,
    const int64_t new_replica_num,
    const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member.is_valid() ||
             false == is_valid_replica_num(new_replica_num) ||
             timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(new_replica_num), K(timeout_ns));
  } else {
    LogConfigChangeArgs args(member, new_replica_num, ADD_ARB_MEMBER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "add_arb_member failed", KR(ret), KPC(this), K(member), K(new_replica_num));
    } else {
      PALF_LOG(INFO, "add_arb_member success", KR(ret), KPC(this), K(member), K(new_replica_num));
    }
  }
  return ret;
}

int PalfHandleImpl::remove_arb_member(
    const common::ObMember &member,
    const int64_t new_replica_num,
    const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member.is_valid() ||
             false == is_valid_replica_num(new_replica_num) ||
             timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(new_replica_num), K(timeout_ns));
  } else {
    LogConfigChangeArgs args(member, new_replica_num, REMOVE_ARB_MEMBER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "remove_arb_member failed", KR(ret), KPC(this), K(member), K(new_replica_num));
    } else {
      PALF_LOG(INFO, "remove_arb_member success", KR(ret), KPC(this), K(member), K(new_replica_num));
    }
  }
  return ret;
}

int PalfHandleImpl::replace_arb_member(
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!added_member.is_valid() ||
             !removed_member.is_valid() ||
             timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_member), K(removed_member), K(timeout_ns));
  } else {
    ObMemberList old_member_list, curr_member_list;
    LogConfigChangeArgs args(removed_member, 0, REMOVE_ARB_MEMBER_AND_NUM);
    const int64_t begin_ts_ns = common::ObTimeUtility::current_time_ns();
    if (OB_FAIL(config_mgr_.get_curr_member_list(old_member_list))) {
      PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), KPC(this));
    } else if (OB_FAIL(one_stage_config_change_(args, timeout_ns))) {
      PALF_LOG(WARN, "remove_member in replace_arb_member failed", KR(ret), KPC(this), K(args));
    } else if (FALSE_IT(args.server_ = added_member)) {
    } else if (FALSE_IT(args.type_ = ADD_MEMBER_AND_NUM)) {
    } else if (OB_FAIL(one_stage_config_change_(args, timeout_ns + begin_ts_ns - common::ObTimeUtility::current_time_ns()))) {
      PALF_LOG(WARN, "add_member in replace_arb_member failed", KR(ret), K(args), KPC(this));
    } else if (OB_FAIL(config_mgr_.get_curr_member_list(curr_member_list))) {
      PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), KPC(this));
    } else {
      PALF_LOG(INFO, "replace_arb_member success", KR(ret), KPC(this), K(added_member),
          K(removed_member), K(timeout_ns), K(old_member_list), K(curr_member_list),
          "leader replace_arb_member cost time(ns)", common::ObTimeUtility::current_time_ns() - begin_ts_ns);
    }
  }
  return ret;
}

int PalfHandleImpl::degrade_acceptor_to_learner(const common::ObMemberList &member_list, const int64_t timeout_ns)
{
  // TODO by yunlong: add another arg to check if ack_ts in match_lsn_map do not change
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!member_list.is_valid() || timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; i < member_list.get_member_number() && OB_SUCC(ret); i++) {
      ObMember member;
      LogConfigChangeArgs args(member, 0, DEGRADE_ACCEPTOR_TO_LEARNER);
      const int64_t begin_ts_ns = common::ObTimeUtility::current_time_ns();
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
      } else if (FALSE_IT(args.server_ = member)) {
      } else if (OB_FAIL(one_stage_config_change_(args,
          timeout_ns + begin_ts_ns - common::ObTimeUtility::current_time_ns()))) {
        PALF_LOG(WARN, "degrade_acceptor_to_learner failed", KR(ret), KPC(this), K(member), K(timeout_ns));
      } else {
        PALF_LOG(INFO, "degrade_acceptor_to_learner success", K(ret), KPC(this), K(member), K(timeout_ns));
      }
    }
    PALF_LOG(INFO, "degrade_acceptor_to_learner finish", K(ret), KPC(this), K(member_list), K(timeout_ns));
  }
  return ret;
}

int PalfHandleImpl::upgrade_learner_to_acceptor(const common::ObMemberList &learner_list, const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!learner_list.is_valid() || timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; i < learner_list.get_member_number() && OB_SUCC(ret); i++) {
      ObMember member;
      LogConfigChangeArgs args(member, 0, UPGRADE_LEARNER_TO_ACCEPTOR);
      const int64_t begin_ts_ns = common::ObTimeUtility::current_time_ns();
      if (OB_FAIL(learner_list.get_member_by_index(i, member))) {
      } else if (FALSE_IT(args.server_ = member)) {
      } else if (OB_FAIL(one_stage_config_change_(args,
          timeout_ns + begin_ts_ns - common::ObTimeUtility::current_time_ns()))) {
        PALF_LOG(WARN, "upgrade_learner_to_acceptor failed", KR(ret), KPC(this), K(member), K(timeout_ns));
      } else {
        PALF_LOG(INFO, "upgrade_learner_to_acceptor success", K(ret), KPC(this), K(member), K(timeout_ns));
      }
    }
    PALF_LOG(INFO, "upgrade_learner_to_acceptor finish", K(ret), KPC(this), K(learner_list), K(timeout_ns));
  }
  return ret;
}

int PalfHandleImpl::change_access_mode(const int64_t proposal_id,
                                       const int64_t mode_version,
                                       const AccessMode &access_mode,
                                       const int64_t ref_ts_ns)
{
  int ret = OB_SUCCESS;
  const int64_t curr_ts_ns = common::ObTimeUtility::current_time_ns();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (INVALID_PROPOSAL_ID == proposal_id ||
             INVALID_PROPOSAL_ID == mode_version ||
             OB_INVALID_TIMESTAMP == ref_ts_ns ||
             ref_ts_ns > curr_ts_ns + MAX_ALLOWED_SKEW_FOR_REF_TS_NS ||
             false == is_valid_access_mode(access_mode)) {
    // ref_ts_ns is reasonable only when access_mode is APPEND
    // mode_version is the proposal_id of PALF when access_mode was applied
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K_(self),
        K(proposal_id), K(mode_version), K(access_mode), K(ref_ts_ns));
  } else if (OB_FAIL(mode_change_lock_.trylock())) {
    // require lock to protect status from concurrent change_access_mode
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "another change_access_mode is running, try again", K(ret), K_(palf_id),
        K_(self), K(proposal_id),K(access_mode), K(ref_ts_ns));
  } else {
    ret = OB_EAGAIN;
    bool first_run = true;
    common::ObTimeGuard time_guard("change_access_mode");
    while (OB_EAGAIN == ret) {
      bool is_state_changed = false;
      {
        RLockGuard guard(lock_);
        is_state_changed = mode_mgr_.is_state_changed();
      }
      if (is_state_changed) {
        WLockGuard guard(lock_);
        if (first_run && proposal_id != state_mgr_.get_proposal_id()) {
          ret = OB_STATE_NOT_MATCH;
          PALF_LOG(WARN, "can not change_access_mode", K(ret), K_(palf_id),
              K_(self), K(proposal_id), "palf proposal_id", state_mgr_.get_proposal_id());
        } else if (OB_SUCC(mode_mgr_.change_access_mode(mode_version, access_mode, ref_ts_ns))) {
          PALF_LOG(INFO, "change_access_mode success", K(ret), K_(palf_id), K_(self), K(proposal_id),
              K(mode_version), K(access_mode), K(ref_ts_ns));
        } else if (OB_EAGAIN != ret) {
          PALF_LOG(WARN, "change_access_mode failed", K(ret), K_(palf_id), K_(self), K(proposal_id),
              K(mode_version), K(access_mode), K(ref_ts_ns));
        }
        first_run = false;
        time_guard.click("do_once_change");
      }
      // do role_change after change_access_mode success
      if (OB_SUCC(ret)) {
        RLockGuard guard(lock_);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = role_change_cb_wrpper_.on_role_change(palf_id_))) {
          PALF_LOG(WARN, "on_role_change failed", K(tmp_ret), K_(palf_id), K_(self));
        }
        PALF_EVENT("change_access_mode success", palf_id_, K(ret), KPC(this),
            K(proposal_id), K(access_mode), K(ref_ts_ns), K(time_guard));
      }
      if (OB_EAGAIN == ret) {
        ob_usleep(1000);
      }
    }
    mode_change_lock_.unlock();
  }
  return ret;
}

int PalfHandleImpl::can_change_config_(const LogConfigChangeArgs &args, int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  proposal_id = INVALID_PROPOSAL_ID;
  RLockGuard guard(lock_);
  if (false == config_mgr_.is_leader_for_config_change(args.type_)) {
    ret = OB_NOT_MASTER;
    PALF_LOG(WARN, "not leader, can't change member", KR(ret), KPC(this),
        "role", state_mgr_.get_role(), "state", state_mgr_.get_state());
  } else if (is_remove_member_list(args.type_) && self_ == args.server_.get_server()) {
    // can not remove leader, return OB_NOT_ALLOW_REMOVING_LEADER,
    // proposer of remove_member cmd will retry later and notify election to switch leader.
    ret = OB_NOT_ALLOW_REMOVING_LEADER;
  } else {
    proposal_id = state_mgr_.get_proposal_id();
  }
  return ret;
}

int PalfHandleImpl::check_args_and_generate_config_(const LogConfigChangeArgs &args,
                                                    bool &is_already_finished,
                                                    common::ObMemberList &log_sync_memberlist,
                                                    int64_t &log_sync_repclia_num) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  LogConfigInfo config_info;
  if (OB_FAIL(config_mgr_.check_args_and_generate_config(state_mgr_.get_proposal_id(), args,
      is_already_finished, config_info))) {
    PALF_LOG(WARN, "check_args_and_generate_config failed", K(ret), KPC(this), K(args));
  } else {
    log_sync_memberlist = config_info.log_sync_memberlist_;
    log_sync_repclia_num = config_info.log_sync_replica_num_;
  }
  return ret;
}

int PalfHandleImpl::sync_get_committed_end_lsn_(const LogConfigChangeArgs &args,
                                                const ObMemberList &new_member_list,
                                                const int64_t new_replica_num,
                                                const int64_t conn_timeout_ns,
                                                LSN &committed_end_lsn,
                                                bool &added_member_has_new_version)
{
  int ret = OB_SUCCESS, tmp_ret = OB_SUCCESS;
  int64_t resp_cnt = 0;
  LogConfigVersion curr_config_version;
  ObAddr server;
  LSN lsn_array[OB_MAX_MEMBER_NUMBER];
  LSN leader_max_flushed_end_lsn;
  (void) sw_.get_max_flushed_end_lsn(leader_max_flushed_end_lsn);
  (void) config_mgr_.get_config_version(curr_config_version);

  added_member_has_new_version = true;
  for (int64_t i = 0; i < new_member_list.get_member_number(); ++i) {
    server.reset();
    LogGetMCStResp resp;
    if (OB_SUCCESS != (tmp_ret = new_member_list.get_server_by_index(i, server))) {
      PALF_LOG(ERROR, "get_server_by_index failed", KR(ret), KPC(this), K(i),
          "new_member_list size:", new_member_list.get_member_number());
    } else if (server == self_) {
      lsn_array[resp_cnt++] = leader_max_flushed_end_lsn;
      PALF_LOG(INFO, "server do not need check", KPC(this), K(server), K_(self));
    } else if (OB_SUCCESS != (tmp_ret = log_engine_.submit_get_memberchange_status_req(server,
            curr_config_version, conn_timeout_ns, resp))) {
      PALF_LOG(WARN, "submit_get_memberchange_status_req failed", KR(ret), KPC(this), K(server),
          K(conn_timeout_ns), K(resp));
    } else if (!resp.is_normal_replica_) {
      PALF_LOG(WARN, "follower is not normal replica", KPC(this), K(server), K(resp));
    } else {
      lsn_array[resp_cnt++] = resp.max_flushed_end_lsn_;
      if (is_add_log_sync_member_list(args.type_) && args.server_.get_server() == server) {
        added_member_has_new_version = !resp.need_update_config_meta_;
      }
    }
  }

  if (resp_cnt < new_replica_num / 2 + 1) {
    // do not recv majority resp, can not change member
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "connection timeout with majority of new_member_list, can't change member!",
        KPC(this), K(new_member_list), K(new_replica_num), K(resp_cnt), K(conn_timeout_ns));
  } else {
    std::sort(lsn_array, lsn_array + resp_cnt, LSNCompare());
    committed_end_lsn = lsn_array[new_replica_num / 2];
  }
  PALF_LOG(INFO, "sync_get_committed_end_lsn_ finish", KPC(this), K(new_member_list), K(new_replica_num),
      K(conn_timeout_ns), K(leader_max_flushed_end_lsn), K(committed_end_lsn),
      "lsn_array:", common::ObArrayWrap<LSN>(lsn_array, resp_cnt));
  return ret;
}

// for config change pre-check
bool PalfHandleImpl::check_follower_sync_status_(const LogConfigChangeArgs &args,
                                                 const ObMemberList &new_member_list,
                                                 const int64_t new_replica_num,
                                                 const int64_t half_timeout_ns,
                                                 bool &added_member_has_new_version)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  LSN first_leader_committed_end_lsn, second_leader_committed_end_lsn;
  LSN first_committed_end_lsn, second_committed_end_lsn;
  int64_t conn_timeout_ns = 100 * 1000L * 1000L;       // default 100ms
  (void) sw_.get_committed_end_lsn(first_leader_committed_end_lsn);

  added_member_has_new_version = true;
  if (new_member_list.get_member_number() == 0) {
  } else if (FALSE_IT(conn_timeout_ns = half_timeout_ns / (new_member_list.get_member_number()))) {
  } else if (OB_FAIL(sync_get_committed_end_lsn_(args, new_member_list, new_replica_num,
      conn_timeout_ns, first_committed_end_lsn, added_member_has_new_version))) {
    bool_ret = false;
  } else if (first_committed_end_lsn >= first_leader_committed_end_lsn) {
    // if committed lsn of new majority do not retreat, then start config change
    bool_ret = true;
    PALF_LOG(INFO, "majority of new_member_list are sync with leader, start config change", KPC(this),
            K(first_committed_end_lsn), K(first_leader_committed_end_lsn), K(new_member_list), K(new_replica_num), K(conn_timeout_ns));
  } else {
    PALF_LOG(INFO, "majority of new_member_list aren't sync with leader", KPC(this), K(first_committed_end_lsn),
        K(first_leader_committed_end_lsn), K(new_member_list), K(new_replica_num), K(conn_timeout_ns));
    // committed_lsn of new majority is behind than old majority's, we want to know if
    // they can catch up with leader during config change timeout. If they can, start config change
    ob_usleep(500 * 1000);
    sw_.get_committed_end_lsn(second_leader_committed_end_lsn);
    int64_t expected_sync_time_s;
    int64_t sync_speed_gap;
    if (OB_FAIL(sync_get_committed_end_lsn_(args, new_member_list, new_replica_num, conn_timeout_ns,
        second_committed_end_lsn, added_member_has_new_version))) {
      bool_ret = false;
    } else if (second_committed_end_lsn >= second_leader_committed_end_lsn) {
      // if committed lsn of new majority do not retreat, then start config change
      bool_ret = true;
      PALF_LOG(INFO, "majority of new_member_list are sync with leader, start config change", KPC(this),
              K(second_committed_end_lsn), K(second_leader_committed_end_lsn), K(new_member_list), K(new_replica_num), K(conn_timeout_ns));
    } else if (FALSE_IT(sync_speed_gap = ((second_committed_end_lsn - first_committed_end_lsn) * 2) - \
        ((second_leader_committed_end_lsn - first_leader_committed_end_lsn) * 2) )) {
    } else if (sync_speed_gap <= 0) {
      bool_ret = false;
      PALF_LOG(WARN, "follwer is not sync with leader after waiting 500 ms", KPC(this), K(sync_speed_gap),
              K(bool_ret), K(second_committed_end_lsn), K(second_leader_committed_end_lsn));
    } else if (FALSE_IT(expected_sync_time_s = (second_leader_committed_end_lsn - second_committed_end_lsn) / sync_speed_gap)) {
    } else if ((expected_sync_time_s * 1E9) <= half_timeout_ns) {
      bool_ret = true;
      PALF_LOG(INFO, "majority of new_member_list are sync with leader, start config change",
              KPC(this), K(bool_ret), K(second_committed_end_lsn), K(first_committed_end_lsn), K(sync_speed_gap),
              K(second_leader_committed_end_lsn), K(half_timeout_ns));
    } else {
      bool_ret = false;
      PALF_LOG(INFO, "majority of new_member_list are far behind, can not change member",
              KPC(this), K(bool_ret), K(second_committed_end_lsn), K(first_committed_end_lsn), K(sync_speed_gap),
              K(second_leader_committed_end_lsn), K(half_timeout_ns));
    }
  }
  bool_ret = bool_ret && added_member_has_new_version;
  return bool_ret;
}

int PalfHandleImpl::one_stage_config_change_(const LogConfigChangeArgs &args,
                                             const int64_t timeout_ns)
{
  int ret = OB_SUCCESS;
  int64_t curr_proposal_id = INVALID_PROPOSAL_ID;
  bool is_already_finished = false;
  ObMemberList new_log_sync_memberlist;
  int64_t new_log_sync_replica_num = 0;
  const int get_lock = config_change_lock_.trylock();
  if (OB_SUCCESS != get_lock) {
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "another config_change is running, try again", KR(ret), KPC(this), K(args), K(timeout_ns));
  } else if (!args.is_valid() || timeout_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(args));
  } else if (OB_FAIL(can_change_config_(args, curr_proposal_id))) {
    if (palf_reach_time_interval(100 * 1000, config_change_print_time_us_)) {
      PALF_LOG(WARN, "not active leader, can't change member", KR(ret), KPC(this),
          "role", state_mgr_.get_role(), "state", state_mgr_.get_state());
    }
  } else if (OB_FAIL(check_args_and_generate_config_(args, is_already_finished, new_log_sync_memberlist, new_log_sync_replica_num))) {
    PALF_LOG(WARN, "check_args_and_generate_config failed", KR(ret), KPC(this), K(args));
  } else if (is_already_finished) {
    if (palf_reach_time_interval(100 * 1000, config_change_print_time_us_)) {
      PALF_LOG(INFO, "one_stage_config_change has already finished", K(ret), KPC(this), K(args));
    }
  } else {
    PALF_LOG(INFO, "one_stage_config_change start", KPC(this), K(curr_proposal_id), K(args), K(timeout_ns));
    TimeoutChecker not_timeout(timeout_ns);
    ObTimeGuard time_guard("config_change");
    // step 1: pre sync config log when adding member
    // The reason for this is when a new member is created and is going to add Paxos Group.
    // Its member_list is empty, so it cann't vote for any leader candidate.
    // If we just add D to (A, B, C) without pre syncing config log, if leader A crashes after
    // adding D, and member_list of D if still empty, D cann't vote for any one, therefore no one
    // can be elected to be leader.
    if (is_add_log_sync_member_list(args.type_)) {
      (void) config_mgr_.pre_sync_config_log(args.server_, curr_proposal_id);
    }
    // step 2: config change remote precheck
    // Eg. 1. remove C from (A, B, C), A is leader. If log of B is far behind majority, we should not remove C,
    // otherwise paxos group will become unavailable
    // Eg. 2. add D to (A, B, C), A is leader. If log of C and D is far behind majority (A, B), we should not add D,
    // otherwise paxos group will become unavailable
    while (is_change_replica_num(args.type_) && OB_SUCC(ret) && OB_SUCC(not_timeout())) {
      bool added_member_has_new_version = true;
      if (OB_FAIL(can_change_config_(args, curr_proposal_id))) {
        PALF_LOG(WARN, "not leader, can't change member", KR(ret), KPC(this),
            "role", state_mgr_.get_role(), "state", state_mgr_.get_state());
      } else if (check_follower_sync_status_(args, new_log_sync_memberlist, new_log_sync_replica_num,
          timeout_ns / 2, added_member_has_new_version)) {
        break;
      } else {
        if (is_add_log_sync_member_list(args.type_) && false == added_member_has_new_version) {
          (void) config_mgr_.pre_sync_config_log(args.server_, curr_proposal_id);
        }
        ob_usleep(100 * 1000);
      }
    }
    time_guard.click("wait_log_sync");
    // step 3: motivate config change state switching
    ret = (OB_SUCCESS == ret)? OB_EAGAIN: ret;
    while (OB_EAGAIN == ret && OB_SUCC(not_timeout())) {
      bool need_wlock = false;
      bool need_rlock = false;
      {
        RLockGuard guard(lock_);
        if (OB_FAIL(config_mgr_.is_state_changed(need_rlock, need_wlock)) &&
            OB_EAGAIN != ret) {
          PALF_LOG(WARN, "is_state_changed failed", KR(ret), KPC(this), K(need_wlock), K(need_wlock));
        }
      }
      if (false == need_rlock && false == need_wlock) {
        ob_usleep(50 * 1000);
      }
      if (true == need_wlock) {
        WLockGuard guard(lock_);
        if (OB_FAIL(config_mgr_.change_config(args)) && OB_EAGAIN != ret) {
          PALF_LOG(WARN, "change_config failed", KR(ret), KPC(this));
        }
      } else if (true == need_rlock) {
        RLockGuard guard(lock_);
        if (OB_FAIL(config_mgr_.change_config(args)) && OB_EAGAIN != ret) {
          PALF_LOG(WARN, "change_config failed", KR(ret), KPC(this));
        }
      }
    }
    time_guard.click("finish");
    PALF_LOG(INFO, "one_stage_config_change finish", KR(ret), KPC(this), K(args),
        K(timeout_ns), K(time_guard));
    if (OB_TIMEOUT == ret) {
      config_mgr_.after_config_change_timeout();
    }
  }
  if (OB_SUCCESS == get_lock) {
    config_change_lock_.unlock();
  }
  return ret;
}

int PalfHandleImpl::handle_register_parent_req(const LogLearner &child, const bool is_to_leader)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  const bool is_leader_active = state_mgr_.is_leader_active();
  bool to_leader = is_to_leader;
  common::ObMemberList member_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!child.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_leader_active && OB_FAIL(config_mgr_.get_curr_member_list(member_list))) {
    PALF_LOG(WARN, "get_curr_member_list", KR(ret), K_(self), K_(palf_id), K(child));
  } else if (is_leader_active && member_list.contains(child.get_server())) {
    PALF_LOG(INFO, "receive register_req from acceptor, ignore", K_(self), K_(palf_id), K(child));
  } else if (!is_leader_active && !state_mgr_.is_leader_reconfirm() && to_leader) {
    // avoid learner sends registering req frequently when leader is reconfirming
    LogLearner parent;
    LogCandidateList candidate_list;
    parent.server_ = self_;
    parent.register_ts_ns_ = child.register_ts_ns_;
    if (OB_FAIL(log_engine_.submit_register_parent_resp(child.server_, parent, candidate_list, RegisterReturn::REGISTER_NOT_MASTER))) {
      PALF_LOG(WARN, "submit_register_parent_resp failed", KR(ret), K_(palf_id), K_(self), K(child));
    }
  } else {
    if (is_leader_active && !to_leader) {
        // child send register req to candidate, but the candidate has switched to leader
        to_leader = true;
    }
    if (OB_SUCC(ret) && OB_FAIL(config_mgr_.handle_register_parent_req(child, to_leader))) {
      PALF_LOG(WARN, "handle_register_parent_req failed", KR(ret), K_(self), K(child), K(is_to_leader), K(to_leader));
    }
  }
  PALF_LOG(INFO, "handle_register_parent_req finished", KR(ret), K_(palf_id), K_(self), K(child), K(is_to_leader));
  return ret;
}

int PalfHandleImpl::handle_register_parent_resp(const LogLearner &server,
                                                const LogCandidateList &candidate_list,
                                                const RegisterReturn reg_ret)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || RegisterReturn::INVALID_REG_RET == reg_ret) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!state_mgr_.is_follower_active()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(config_mgr_.handle_register_parent_resp(server, candidate_list, reg_ret))) {
    PALF_LOG(WARN, "handle_register_parent_resp failed", KR(ret), K_(palf_id), K(server), K(candidate_list), K(reg_ret));
  }
  return ret;
}

int PalfHandleImpl::handle_learner_req(const LogLearner &server, const LogLearnerReqType req_type)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || LogLearnerReqType::INVALID_LEARNER_REQ_TYPE == req_type) {
    ret = OB_INVALID_ARGUMENT;
  } else if (LogLearnerReqType::RETIRE_PARENT == req_type && OB_FAIL(config_mgr_.handle_retire_parent(server))) {
    PALF_LOG(WARN, "handle_reture_parent failed", KR(ret), K_(palf_id), K(server));
  } else if (LogLearnerReqType::RETIRE_CHILD == req_type && OB_FAIL(config_mgr_.handle_retire_child(server))) {
    PALF_LOG(WARN, "handle_retire_child failed", KR(ret), K_(palf_id), K(server));
  } else if (LogLearnerReqType::KEEPALIVE_REQ == req_type && OB_FAIL(config_mgr_.handle_learner_keepalive_req(server))) {
    PALF_LOG(WARN, "handle_learner_keepalive_req failed", KR(ret), K_(palf_id), K(server));
  } else if (LogLearnerReqType::KEEPALIVE_RESP == req_type && OB_FAIL(config_mgr_.handle_learner_keepalive_resp(server))) {
    PALF_LOG(WARN, "handle_learner_keepalive_resp failed", KR(ret), K_(palf_id), K(server));
  }
  return ret;
}

int PalfHandleImpl::set_base_lsn(
    const LSN &lsn)
{
  // NB: Guarded by lock is important, otherwise there are some problems concurrent with rebuild or migrate.
  //
  // Thread1(assume it's migrate thread)
  // 1. if the 'base_lsn' of data source is greater than or equal to local, and then
  // 2. avoid the hole between blocks, delete all blocks before 'base_lsn', submit truncate prefix blocks task
  // 3. to update base lsn, submit update snpshot meta task.
  //
  // Execute above steps in 'advance_base_info'
  //
  // Thread2(checkpoint thread)
  // 1. the clog disk is not enough, and update it via 'set_base_lsn'
  //
  // Consider that:
  //
  // Time1: thread1 has executed step1, assume the base lsn of data source is 100, local base lsn is 50,
  // and then thread2 execute 'set_base_lsn', the local base lsn set to 150
  //
  // Time2: thread1 submit truncate prefix blocks, and this task will be failed because the base lsn in this task
  // is smaller than local base lsn.
  //
  // Therefore, we need guarded by lock.
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  LSN end_lsn = get_end_lsn();
  const LSN &curr_base_lsn = log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_;
  const LSN new_base_lsn(lsn_2_block(lsn, PALF_BLOCK_SIZE) * PALF_BLOCK_SIZE);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!lsn.is_valid() || lsn > end_lsn) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(end_lsn), K(lsn));
  } else if (curr_base_lsn >= new_base_lsn) {
    PALF_LOG(WARN, "no need to set new base lsn, curr base lsn is greater than or equal to new base lsn",
        KPC(this), K(curr_base_lsn), K(new_base_lsn), K(lsn));
  } else {
    LogSnapshotMeta log_snapshot_meta;
    FlushMetaCbCtx flush_meta_cb_ctx;
    flush_meta_cb_ctx.type_ = SNAPSHOT_META;
    flush_meta_cb_ctx.base_lsn_ = new_base_lsn;
    if (OB_FAIL(log_snapshot_meta.generate(new_base_lsn))) {
      PALF_LOG(WARN, "LogSnapshotMeta generate failed", K(ret), KPC(this));
    } else if (OB_FAIL(log_engine_.submit_flush_snapshot_meta_task(flush_meta_cb_ctx, log_snapshot_meta))) {
      PALF_LOG(WARN, "submit_flush_snapshot_meta_task failed", K(ret), KPC(this));
    } else {
      PALF_EVENT("set_base_lsn success", palf_id_, K(ret), K_(palf_id), K(self_), K(lsn),
          K(log_snapshot_meta), K(new_base_lsn), K(flush_meta_cb_ctx));
    }
  }
  return ret;
}

bool PalfHandleImpl::is_sync_enabled() const
{
  bool bool_ret = false;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
  } else {
    bool_ret = state_mgr_.is_sync_enabled();
  }
  return bool_ret;
}

int PalfHandleImpl::enable_sync()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(state_mgr_.enable_sync())) {
    PALF_LOG(WARN, "enable_sync failed", K(ret), KPC(this));
  } else {
    PALF_EVENT("enable_sync success", palf_id_, K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::disable_sync()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(state_mgr_.disable_sync())) {
    PALF_LOG(WARN, "disable_sync failed", K(ret), KPC(this));
  } else {
    PALF_EVENT("disable_sync success", palf_id_, K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::disable_vote()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(set_allow_vote_flag_(false))) {
    PALF_LOG(WARN, "set_allow_vote_flag failed", K(ret), KPC(this));
  } else {
    PALF_EVENT("disable_vote success", palf_id_, KPC(this));
  }
  return ret;
}

int PalfHandleImpl::enable_vote()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(set_allow_vote_flag_(true))) {
    PALF_LOG(WARN, "set_allow_vote_flag failed", K(ret), KPC(this));
  } else {
    PALF_EVENT("enable_vote success", palf_id_, KPC(this));
  }
  return ret;
}

int PalfHandleImpl::set_allow_vote_flag_(const bool allow_vote)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(replica_meta_lock_);
  {
    RLockGuard guard(lock_);
    FlushMetaCbCtx flush_meta_cb_ctx;
    flush_meta_cb_ctx.type_ = REPLICA_PROPERTY_META;
    flush_meta_cb_ctx.allow_vote_ = allow_vote;
    LogReplicaPropertyMeta replica_property_meta = log_engine_.get_log_meta().get_log_replica_property_meta();
    replica_property_meta.allow_vote_ = allow_vote;
    if (false == allow_vote
        && LEADER == state_mgr_.get_role()
        && OB_FAIL(election_.revoke(RoleChangeReason::PalfDisableVoteToRevoke))) {
      PALF_LOG(WARN, "election revoke failed", K(ret), K_(palf_id));
    } else if (OB_FAIL(log_engine_.submit_flush_replica_property_meta_task(flush_meta_cb_ctx, replica_property_meta))) {
      PALF_LOG(WARN, "submit_flush_replica_property_meta_task failed", K(ret), K(flush_meta_cb_ctx), K(replica_property_meta));
    }
  }
  // wait until replica_property_meta has been flushed
  if (OB_SUCC(ret)) {
    while(allow_vote != state_mgr_.is_allow_vote()) {
      ob_usleep(500);
    }
  }
  return ret;
}

int PalfHandleImpl::advance_base_info(const PalfBaseInfo &palf_base_info, const bool is_rebuild)
{
  int ret = OB_SUCCESS;
  const LSN new_base_lsn = palf_base_info.curr_lsn_;
  const LogInfo prev_log_info = palf_base_info.prev_log_info_;
  LSN last_slide_lsn, committed_end_lsn;
  int64_t last_slide_log_id;
  common::ObTimeGuard time_guard("advance_base_info");
  ObSpinLockGuard rebuilding_guard(rebuilding_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!palf_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(palf_base_info));
  } else if (true == state_mgr_.is_sync_enabled()) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "is_sync_enabled, can not advance_base_info", K(ret), K_(palf_id), K(palf_base_info), K(is_rebuild));
  } else {
    while (!sw_.is_all_committed_log_slided_out(last_slide_lsn, last_slide_log_id, committed_end_lsn)) {
      if (palf_reach_time_interval(1 * 1000 * 1000, wait_slide_print_time_us_)) {
        PALF_LOG(INFO, "There is some log has not slided out, need wait and retry", K(ret), K_(palf_id), K_(self),
            K(palf_base_info), K(is_rebuild), K(last_slide_lsn), K(last_slide_log_id), K(committed_end_lsn));
      }
      ob_usleep(1000);
    }
    time_guard.click("wait_slide");
    // require wlock in here to ensure:
    // 1. snapshot_meta.base_lsn_ cann't be updated concurrently
    // 2. can not receive logs before submitting truncate prefix block task
    // require wlock "after" waitting all committed logs slided out. If not, log sliding will be locked
    // by wlock, so committed logs will not slide forever.
    WLockGuard guard(lock_);
    time_guard.click("wait_wlock");
    const LSN curr_base_lsn = log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_;
    assert(get_end_lsn() >= curr_base_lsn);
    LogSnapshotMeta log_snapshot_meta;
    FlushMetaCbCtx flush_meta_cb_ctx;
    TruncatePrefixBlocksCbCtx truncate_prefix_cb_ctx(new_base_lsn);
    flush_meta_cb_ctx.type_ = SNAPSHOT_META;
    flush_meta_cb_ctx.base_lsn_ = new_base_lsn;
    if (OB_FAIL(check_need_advance_base_info_(new_base_lsn, prev_log_info, is_rebuild))) {
      PALF_LOG(WARN, "check_need_advance_base_info failed", K(ret), KPC(this), K(palf_base_info), K(is_rebuild));
    } else if (OB_FAIL(log_snapshot_meta.generate(new_base_lsn, prev_log_info))) {
        PALF_LOG(WARN, "LogSnapshotMeta generate failed", K(ret), KPC(this), K(palf_base_info));
    } else if (OB_FAIL(log_engine_.submit_flush_snapshot_meta_task(flush_meta_cb_ctx, log_snapshot_meta))) {
      PALF_LOG(WARN, "submit_flush_snapshot_meta_task failed", K(ret), KPC(this), K(flush_meta_cb_ctx), K(log_snapshot_meta));
    } else if (OB_FAIL(log_engine_.submit_truncate_prefix_blocks_task(truncate_prefix_cb_ctx))) {
      PALF_LOG(WARN, "submit_truncate_prefix_blocks_task failed", K(ret), KPC(this), K(truncate_prefix_cb_ctx));
    } else if (OB_FAIL(sw_.truncate_for_rebuild(palf_base_info))) {
      // 与receive_log的truncate相比的不同点是，该场景truncate位点预期大于sw左边界
      // 且小于truncate位点的日志都需要回调并丢弃
      PALF_LOG(WARN, "sw_ truncate_for_rebuild failed", K(ret), KPC(this), K(palf_base_info));
    } else {
      time_guard.click("sw_truncate");
      PALF_LOG(INFO, "sw_ truncate_for_rebuild success", K(ret), KPC(this), K(palf_base_info));
    }
  }

  PALF_LOG(INFO, "advance_base_info finished", K(ret), KPC(this), K(time_guard), K(palf_base_info));
  return ret;
}

int PalfHandleImpl::locate_by_ts_ns_coarsely(const int64_t ts_ns, LSN &result_lsn)
{
  int ret = OB_SUCCESS;
  block_id_t mid_block_id = LOG_INVALID_BLOCK_ID, min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID, result_block_id = LOG_INVALID_BLOCK_ID;
  int64_t mid_ts = OB_INVALID_TIMESTAMP;
  result_lsn.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret));
  } else if (ts_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(ts_ns));
  } else if (OB_FAIL(get_binary_search_range_(ts_ns, min_block_id, max_block_id, result_block_id))) {
    PALF_LOG(WARN, "get_binary_search_range_ failed", KR(ret), KPC(this), K(ts_ns));
  } else {
    // 1. get lower bound lsn (result_lsn) by binary search
    while(OB_SUCC(ret) && min_block_id <= max_block_id) {
      mid_block_id = min_block_id + ((max_block_id - min_block_id) >> 1);
      if (OB_FAIL(log_engine_.get_block_min_ts_ns(mid_block_id, mid_ts))) {
        PALF_LOG(WARN, "get_block_min_ts_ns failed", KR(ret), KPC(this), K(mid_block_id));
        // OB_ERR_OUT_OF_UPPER_BOUND: this block is a empty active block, just return
        // OB_ERR_OUT_OF_LOWER_BOUND: block_id_ is smaller than min_block_id, this block may be recycled
        // OB_ERR_UNEXPECTED: log files lost unexpectedly, just return
        // OB_IO_ERROR: just return
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          // block mid_lsn.block_id_ may be recycled, get_binary_search_range_ again
          if (OB_FAIL(get_binary_search_range_(ts_ns, min_block_id, max_block_id, result_block_id))) {
            PALF_LOG(WARN, "get_binary_search_range_ failed", KR(ret), KPC(this), K(ts_ns));
          }
        } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else if (mid_ts < ts_ns) {
        min_block_id = mid_block_id;
        if (max_block_id == min_block_id) {
          result_block_id = mid_block_id;
          break;
        } else if (max_block_id == min_block_id + 1) {
          int64_t next_min_ts = OB_INVALID_TIMESTAMP;
          if (OB_FAIL(log_engine_.get_block_min_ts_ns(max_block_id, next_min_ts))) {
            // if fail to read next block, just return prev block lsn
            ret = OB_SUCCESS;
            result_block_id = mid_block_id;
          } else if (ts_ns < next_min_ts) {
            result_block_id = mid_block_id;
          } else {
            result_block_id = max_block_id;
          }
          break;
        }
      } else if (mid_ts > ts_ns) {
        // block_id is uint64_t, so check == 0 firstly.
        if (mid_block_id == 0 || mid_block_id - 1 < min_block_id) {
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
          PALF_LOG(WARN, "ts_ns is smaller than min ts of first block", KR(ret), KPC(this), K(min_block_id),
                          K(max_block_id), K(mid_block_id), K(ts_ns), K(mid_ts));
        } else {
          max_block_id = mid_block_id - 1;
        }
      } else {
        result_block_id = mid_block_id;
        break;
      }
    }
    // 2. convert block_id to lsn
    if (OB_SUCC(ret)) {
      result_lsn = LSN(result_block_id * PALF_BLOCK_SIZE);
      inc_update_last_locate_block_ts_(result_block_id, ts_ns);
    }
  }
  return ret;
}

void PalfHandleImpl::set_deleted()
{
  ATOMIC_STORE(&has_set_deleted_, true);
  PALF_LOG(INFO, "set_deleted success", KPC(this));
}

// NB: 1) if min_block_id_ == max_block_id_, then block max_lsn.block_id_ may be active block
//     2) if min_block_id_ < max_block_id_, then block max_lsn.block_id_ must not be active block
//     3) if min_block_id_ > max_block_id_, cache hits
int PalfHandleImpl::get_binary_search_range_(const int64_t ts_ns,
                                             block_id_t &min_block_id,
                                             block_id_t &max_block_id,
                                             block_id_t &result_block_id)
{
  int ret = OB_SUCCESS;
  result_block_id = LOG_INVALID_BLOCK_ID;
  const LSN committed_lsn = get_end_lsn();
  if (OB_FAIL(log_engine_.get_block_id_range(min_block_id, max_block_id))) {
    //PALF_LOG(, "get_block_id_range failed", KR(ret), K_(palf_id));
  } else {
    block_id_t committed_block_id = lsn_2_block(committed_lsn, PALF_BLOCK_SIZE);
    max_block_id = (committed_block_id < max_block_id)? committed_block_id : max_block_id;
    // optimization: cache last_locate_ts_ns_ to shrink binary search range
    SpinLockGuard guard(last_locate_lock_);
    if (is_valid_block_id(last_locate_block_) &&
        min_block_id <= last_locate_block_ &&
        max_block_id >= last_locate_block_) {
      if (ts_ns < last_locate_ts_ns_) {
        max_block_id = last_locate_block_;
      } else if (ts_ns > last_locate_ts_ns_) {
        min_block_id = last_locate_block_;
      } else {
        result_block_id = last_locate_block_;
        // result_lsn hits last_locate_block_ cache, don't need binary search
        // let min_block_id > max_block_id
        min_block_id = 1;
        max_block_id = 0;
      }
    }
    PALF_LOG(INFO, "get_binary_search_range_", K(ret), KPC(this), K(min_block_id), K(max_block_id),
        K(result_block_id), K(committed_lsn), K(ts_ns), K_(last_locate_ts_ns), K_(last_locate_block));
  }
  return ret;
}

void PalfHandleImpl::inc_update_last_locate_block_ts_(const block_id_t &block_id, const int64_t ts)
{
  SpinLockGuard guard(last_locate_lock_);
  if (block_id > last_locate_block_) {
    last_locate_block_ = block_id;
    last_locate_ts_ns_ = ts;
  }
}

int PalfHandleImpl::locate_by_lsn_coarsely(const LSN &lsn, int64_t &result_ts_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret));
  } else if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(lsn));
  } else if (lsn < log_engine_.get_begin_lsn()) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
    PALF_LOG(WARN, "lsn is too small, this block has been recycled", KR(ret), KPC(this),
        K(lsn), "begin_lsn", log_engine_.get_begin_lsn());
  } else {
    const LSN committed_lsn = get_end_lsn();
    LSN curr_lsn = (committed_lsn <= lsn) ? committed_lsn: lsn;
    block_id_t curr_block_id = lsn_2_block(curr_lsn, PALF_BLOCK_SIZE);
    if (OB_FAIL(log_engine_.get_block_min_ts_ns(curr_block_id, result_ts_ns))) {
      // if this block is a empty active block, read prev block if exists
      if (OB_ERR_OUT_OF_UPPER_BOUND == ret &&
          curr_block_id > 0 &&
          OB_FAIL(log_engine_.get_block_min_ts_ns(curr_block_id - 1, result_ts_ns))) {
        PALF_LOG(WARN, "get_block_min_ts_ns failed", KR(ret), KPC(this), K(curr_lsn), K(lsn));
      }
    }
    PALF_LOG(INFO, "locate_by_lsn_coarsely", KR(ret), KPC(this), K(lsn), K(committed_lsn), K(result_ts_ns));
  }
  return ret;
}

int PalfHandleImpl::get_min_block_info_for_gc(block_id_t &min_block_id, int64_t &max_ts_ns)
{
  int ret = OB_SUCCESS;
//  if (false == end_lsn.is_valid()) {
//    ret = OB_ENTRY_NOT_EXIST;
//  }
  if (OB_FAIL(log_engine_.get_min_block_info_for_gc(min_block_id, max_ts_ns))) {
    PALF_LOG(WARN, "get_min_block_info_for_gc failed", K(ret), KPC(this));
  } else {
    PALF_LOG(TRACE, "get_min_block_info_for_gc success", K(ret), KPC(this), K(min_block_id), K(max_ts_ns));
  }
  return ret;
}

int PalfHandleImpl::delete_block(const block_id_t &block_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_engine_.delete_block(block_id))) {
    PALF_LOG(WARN, "delete block failed", K(ret), KPC(this), K(block_id));
  } else {
    PALF_LOG(WARN, "delete block success", K(ret), KPC(this), K(block_id));
  }
  return ret;
}

int PalfHandleImpl::read_log(const LSN &lsn,
                             const int64_t in_read_size,
                             ReadBuf &read_buf,
                             int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == lsn.is_valid()
             || 0 >= in_read_size
             || false == read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_engine_.read_log(lsn, in_read_size,
      read_buf, out_read_size))) {
    PALF_LOG(ERROR, "PalfHandleImpl read_log failed", K(ret), KPC(this), K(lsn),
        K(in_read_size), K(read_buf));
  } else {
  }
  PALF_LOG(INFO, "PalfHandleImpl read_log finish", K(ret), KPC(this), K(lsn),
      K(in_read_size), K(read_buf), K(out_read_size));
  return ret;
}

int PalfHandleImpl::inner_append_log(const LSN &lsn,
                                     const LogWriteBuf &write_buf,
                                     const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  const int64_t begin_ts = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl not inited", K(ret), KPC(this));
  } else if (false == lsn.is_valid()
             || false == write_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(ret), KPC(this), K(lsn), K(write_buf));
  } else if (OB_FAIL(log_engine_.append_log(lsn, write_buf, log_ts))) {
    PALF_LOG(ERROR, "LogEngine pwrite failed", K(ret), KPC(this), K(lsn), K(log_ts));
  } else {
    const int64_t time_cost = ObTimeUtility::current_time() - begin_ts;
    append_cost_stat_.stat(time_cost);
    if (time_cost >= 5 * 1000) {
      PALF_LOG(WARN, "write log cost too much time", K(ret), KPC(this), K(lsn), K(log_ts), K(time_cost));
    }
  }
  return ret;
}

int PalfHandleImpl::inner_append_log(const LSNArray &lsn_array,
                                     const LogWriteBufArray &write_buf_array,
                                     const LogTsArray &log_ts_array)
{
  int ret = OB_SUCCESS;
  const int64_t begin_ts = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl not inited", K(ret), KPC(this));
  } else if (OB_FAIL(log_engine_.append_log(lsn_array, write_buf_array, log_ts_array))) {
    PALF_LOG(ERROR, "LogEngine pwrite failed", K(ret), KPC(this), K(lsn_array), K(log_ts_array));
  } else {
    const int64_t time_cost = ObTimeUtility::current_time() - begin_ts;
    append_cost_stat_.stat(time_cost);
    if (time_cost > 10 * 1000) {
      PALF_LOG(WARN, "write log cost too much time", K(ret), KPC(this), K(lsn_array),
               K(log_ts_array), K(time_cost));
    }
  }
  return ret;
}

int PalfHandleImpl::inner_append_meta(const char *buf,
                                      const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl not inited");
  } else if (NULL == buf
             || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(ret), KPC(this), K(buf), K(buf_len));
  } else if (OB_FAIL(log_engine_.append_meta(buf, buf_len))) {
    PALF_LOG(ERROR, "LogEngine append_meta failed", K(ret), KPC(this));
  } else {
  }
  return ret;
}

int PalfHandleImpl::inner_truncate_log(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl not inited");
  } else if (false == lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(ret), K(lsn), KPC(this));
  } else if (OB_FAIL(log_engine_.truncate(lsn))) {
    PALF_LOG(ERROR, "LogEngine truncate failed", K(ret), K(lsn), KPC(this));
  } else {
    PALF_LOG(INFO, "PalfHandleImpl inner_truncate_log success", K(lsn), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::inner_truncate_prefix_blocks(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl not inited");
  } else if (false == lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(ret), KPC(this), K(lsn));
  } else if (OB_FAIL(log_engine_.truncate_prefix_blocks(lsn))) {
    PALF_LOG(WARN, "ObLogEngine truncate_prefix_blocks failed", K(ret), KPC(this), K(lsn));
  } else {
    PALF_LOG(INFO, "ObLogEngine truncate_prefix_blocks success", K(ret), KPC(this), K(lsn));
  }
  return ret;
}

int PalfHandleImpl::set_scan_disk_log_finished()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(state_mgr_.set_scan_disk_log_finished())) {
    PALF_LOG(WARN, "set_scan_disk_log_finished failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::get_access_mode(AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(mode_mgr_.get_access_mode(access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::get_access_mode(int64_t &mode_version, AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(mode_mgr_.get_access_mode(mode_version, access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::alloc_palf_buffer_iterator(const LSN &offset,
                                               PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  auto get_file_end_lsn = [&]() {
    LSN max_flushed_end_lsn;
    (void)sw_.get_max_flushed_end_lsn(max_flushed_end_lsn);
    LSN committed_end_lsn;
    sw_.get_committed_end_lsn(committed_end_lsn);
    return MIN(committed_end_lsn, max_flushed_end_lsn);
  };
  if (OB_FAIL(iterator.init(offset, log_engine_.get_log_storage(), get_file_end_lsn))) {
    PALF_LOG(ERROR, "PalfBufferIterator init failed", K(ret), KPC(this));
  } else {
  }
  return ret;
}

int PalfHandleImpl::alloc_palf_group_buffer_iterator(const LSN &offset,
                                                     PalfGroupBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  auto get_file_end_lsn = [&]() {
    LSN max_flushed_end_lsn;
    (void)sw_.get_max_flushed_end_lsn(max_flushed_end_lsn);
    LSN committed_end_lsn;
    sw_.get_committed_end_lsn(committed_end_lsn);
    return MIN(committed_end_lsn, max_flushed_end_lsn);
  };
  if (OB_FAIL(iterator.init(offset, log_engine_.get_log_storage(), get_file_end_lsn))) {
    PALF_LOG(ERROR, "PalfGroupBufferIterator init failed", K(ret), KPC(this));
  } else {
  }
  return ret;
}

int PalfHandleImpl::alloc_palf_group_buffer_iterator(const int64_t ts_ns,
                                                     PalfGroupBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  LSN start_lsn;
  const auto get_file_end_lsn = [&]() {
    LSN max_flushed_end_lsn;
    (void)sw_.get_max_flushed_end_lsn(max_flushed_end_lsn);
    LSN committed_end_lsn;
    sw_.get_committed_end_lsn(committed_end_lsn);
    return MIN(committed_end_lsn, max_flushed_end_lsn);
  };
  PalfGroupBufferIterator local_iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (ts_ns <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K_(palf_id), K(ts_ns));
  } else if (OB_FAIL(locate_by_ts_ns_coarsely(ts_ns, start_lsn)) &&
             OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    PALF_LOG(WARN, "locate_by_ts_ns_coarsely failed", KR(ret), KPC(this), K(ts_ns));
  } else if (OB_SUCCESS != ret &&
            !FALSE_IT(start_lsn = log_engine_.get_begin_lsn()) &&
            start_lsn.val_ != PALF_INITIAL_LSN_VAL) {
    PALF_LOG(WARN, "log may have been recycled", KR(ret), KPC(this), K(ts_ns), K(start_lsn));
  } else if (OB_FAIL(local_iter.init(start_lsn, log_engine_.get_log_storage(), get_file_end_lsn))) {
    PALF_LOG(WARN, "PalfGroupBufferIterator init failed", KR(ret), KPC(this), K(start_lsn));
  } else {
    LogGroupEntry curr_group_entry;
    LSN curr_lsn, result_lsn;
    while (OB_SUCC(ret) && OB_SUCC(local_iter.next())) {
      if (OB_FAIL(local_iter.get_entry(curr_group_entry, curr_lsn))) {
        PALF_LOG(ERROR, "PalfGroupBufferIterator get_entry failed", KR(ret), KPC(this),
            K(curr_group_entry), K(curr_lsn), K(local_iter));
      } else if (curr_group_entry.get_log_ts() >= ts_ns) {
        result_lsn = curr_lsn;
        break;
      } else {
        continue;
      }
    }
    if (OB_SUCC(ret) &&
        result_lsn.is_valid() &&
        OB_FAIL(iterator.init(result_lsn, log_engine_.get_log_storage(), get_file_end_lsn))) {
      PALF_LOG(WARN, "PalfGroupBufferIterator init failed", KR(ret), KPC(this), K(result_lsn));
    } else {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      PALF_LOG(WARN, "locate_by_ts_ns failed", KR(ret), KPC(this), K(ts_ns), K(result_lsn));
    }
  }
  return ret;
}

int PalfHandleImpl::register_file_size_cb(palf::PalfFSCbNode *fs_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (OB_FAIL(fs_cb_wrapper_.add_cb_impl(fs_cb))) {
      PALF_LOG(WARN, "add_file_size_cb_impl failed", K(ret), KPC(this), KPC(fs_cb));
    } else {
      PALF_LOG(INFO, "register_file_size_cb success", KPC(this));
    }
  }
  return ret;
}

int PalfHandleImpl::unregister_file_size_cb(palf::PalfFSCbNode *fs_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    fs_cb_wrapper_.del_cb_impl(fs_cb);
    PALF_LOG(INFO, "unregister_file_size_cb success", KPC(this));
  }
  return ret;
}

int PalfHandleImpl::register_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (OB_FAIL(role_change_cb_wrpper_.add_cb_impl(role_change_cb))) {
      PALF_LOG(WARN, "add_role_change_cb_impl failed", K(ret), KPC(this), KPC(role_change_cb));
    } else if (OB_FAIL(role_change_cb->rc_cb_->on_role_change(palf_id_))) {
      PALF_LOG(WARN, "on_role_change failed", K(ret), KPC(this));
    } else {
      PALF_LOG(INFO, "register_role_change_cb success", KPC(this));
    }
  }
  return ret;
}

int PalfHandleImpl::unregister_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    role_change_cb_wrpper_.del_cb_impl(role_change_cb);
    PALF_LOG(INFO, "unregister_role_change_cb success", KPC(this));
  }
  return ret;
}

int PalfHandleImpl::register_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == rebuild_cb) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(rebuild_cb_wrapper_.add_cb_impl(rebuild_cb))) {
      PALF_LOG(WARN, "add_rebuild_cb_impl failed", K(ret), KPC(this), KPC(rebuild_cb));
    } else {
      PALF_LOG(INFO, "register_rebuild_cb success", KPC(this), KP(rebuild_cb));
    }
  }
  return ret;
}

int PalfHandleImpl::unregister_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    rebuild_cb_wrapper_.del_cb_impl(rebuild_cb);
    PALF_LOG(INFO, "unregister_rebuild_cb success", KPC(this), KP(rebuild_cb));
  }
  return ret;
}

int PalfHandleImpl::set_location_cache_cb(PalfLocationCacheCb *lc_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "not initted", KR(ret), KPC(this));
  } else if (OB_ISNULL(lc_cb)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "lc_cb is NULL, can't register", KR(ret), KPC(this));
  } else {
    WLockGuard guard(lock_);
    if (OB_NOT_NULL(lc_cb_)) {
      ret = OB_NOT_SUPPORTED;
      PALF_LOG(WARN, "lc_cb_ is not NULL, can't register", KR(ret), KPC(this));
    } else if (OB_FAIL(sw_.set_location_cache_cb(lc_cb))) {
      PALF_LOG(WARN, "sw_.set_location_cache_cb failed", KR(ret), KPC(this));
    } else {
      lc_cb_ = lc_cb;
      PALF_LOG(INFO, "set_location_cache_cb success", KPC(this), KP_(lc_cb));
    }
  }
  return ret;
}

int PalfHandleImpl::set_election_priority(election::ElectionPriority *priority)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(priority)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "priority is NULL, can't setted", KR(ret), KPC(this));
  } else {
    WLockGuard guard(lock_);
    if (OB_FAIL(election_.set_priority(priority))) {
      PALF_LOG(WARN, "set election priority failed", KR(ret), KPC(this));
    }
  }
  return ret;
}

int PalfHandleImpl::reset_election_priority()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle not init", KR(ret), KPC(this));
  } else {
    WLockGuard guard(lock_);
    if (OB_FAIL(election_.reset_priority())) {
      PALF_LOG(WARN, "fail to reset election priority", KR(ret), KPC(this));
    }
  }
  return ret;
}

int PalfHandleImpl::reset_location_cache_cb()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sw_.reset_location_cache_cb())) {
    PALF_LOG(WARN, "sw_.reset_location_cache_cb failed", KR(ret), KPC(this));
  } else {
    WLockGuard guard(lock_);
    lc_cb_ = NULL;
  }
  return ret;
}

int PalfHandleImpl::check_and_switch_freeze_mode()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    sw_.check_and_switch_freeze_mode();
  }
  return ret;
}

int PalfHandleImpl::period_freeze_last_log()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    sw_.period_freeze_last_log();
  }
  return ret;
}

int PalfHandleImpl::check_and_switch_state()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    bool state_changed = false;
    do {
      RLockGuard guard(lock_);
      state_changed = state_mgr_.is_state_changed();
    } while (0);
    if (state_changed) {
      WLockGuard guard(lock_);
      if (OB_FAIL(state_mgr_.switch_state())) {
        PALF_LOG(WARN, "switch_state failed", K(ret));
      }
      PALF_LOG(TRACE, "check_and_switch_state finished", K(ret), KPC(this), K(state_changed));
    }
    do {
      RLockGuard guard(lock_);
      if (OB_FAIL(config_mgr_.leader_do_loop_work())) {
        PALF_LOG(WARN, "leader_do_loop_work", KR(ret), K_(self), K_(palf_id));
      }
    } while (0);
    if (palf_reach_time_interval(PALF_CHECK_PARENT_CHILD_INTERVAL_US, last_check_parent_child_ts_us_)) {
      RLockGuard guard(lock_);
      if (state_mgr_.is_follower_active()) {
        (void) config_mgr_.check_parent_health();
      }
      (void) config_mgr_.check_children_health();
    }
    if (palf_reach_time_interval(PALF_DUMP_DEBUG_INFO_INTERVAL_US, last_dump_info_ts_us_)) {
      RLockGuard guard(lock_);
      FLOG_INFO("[PALF_DUMP]", K_(palf_id), K_(self), "[SlidingWindow]", sw_, "[StateMgr]", state_mgr_,
          "[ConfigMgr]", config_mgr_, "[ModeMgr]", mode_mgr_, "[LogEngine]", log_engine_, "[Reconfirm]",
          reconfirm_);
      (void) sw_.report_log_task_trace(sw_.get_start_id());
    }
  }
  return ret;
}

int PalfHandleImpl::handle_prepare_request(const common::ObAddr &server,
                                           const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  bool can_handle_prepare_request = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(server), K(proposal_id));
  } else {
    RLockGuard guard(lock_);
    if (state_mgr_.can_handle_prepare_request(proposal_id)) {
      can_handle_prepare_request = true;
    }
  }
  if (OB_SUCC(ret) && can_handle_prepare_request) {
    WLockGuard guard(lock_);
    if (!state_mgr_.can_handle_prepare_request(proposal_id)) {
      // can not handle prepare request
    } else if (OB_FAIL(state_mgr_.handle_prepare_request(server, proposal_id))) {
      PALF_LOG(WARN, "handle_prepare_request failed", K(ret), KPC(this), K(server), K(proposal_id));
    } else {
      // Call clean_log() when updating proposal_id to delete phantom logs(if it exists).
      (void) sw_.clean_log();
      PALF_LOG(INFO, "handle_prepare_request success", K(ret), KPC(this), K(server), K_(self), K(proposal_id));
    }
  }
  PALF_LOG(TRACE, "handle_prepare_request", K(ret), KPC(this), K(server), K(can_handle_prepare_request));
  return ret;
}

int PalfHandleImpl::handle_prepare_response(const common::ObAddr &server,
                                            const int64_t &proposal_id,
                                            const bool vote_granted,
                                            const int64_t &accept_proposal_id,
                                            const LSN &last_lsn,
                                            const LogModeMeta &log_mode_meta)
{
  int ret = OB_SUCCESS;
  bool can_handle_prepare_resp = true;
  int64_t curr_proposal_id = INVALID_PROPOSAL_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id));
  } else {
    RLockGuard guard(lock_);
    curr_proposal_id = state_mgr_.get_proposal_id();
    if (!state_mgr_.can_handle_prepare_response(proposal_id)) {
      can_handle_prepare_resp = false;
      PALF_LOG(WARN, "cannot handle parepare response", K(ret), KPC(this), K(server),
          K(proposal_id), K(curr_proposal_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (!can_handle_prepare_resp) {
      // check proposal_id
      if (proposal_id > curr_proposal_id) {
        WLockGuard guard(lock_);
        if (!state_mgr_.can_handle_prepare_request(proposal_id)) {
          // can not handle prepare request
        } else if (OB_FAIL(state_mgr_.handle_prepare_request(server, proposal_id))) {
          PALF_LOG(WARN, "handle_prepare_request failed", K(ret), KPC(this));
        } else {
          // Call clean_log() when updating proposal_id to delete phantom logs(if it exists).
          (void) sw_.clean_log();
        }
      }
    } else if (vote_granted) {
      // server grant vote for me, process preapre response
      RLockGuard guard(lock_);
      if (OB_FAIL(mode_mgr_.handle_prepare_response(server, proposal_id, accept_proposal_id,
          last_lsn, log_mode_meta))) {
        PALF_LOG(WARN, "log_mode_mgr.handle_prepare_response failed", K(ret), KPC(this), K(proposal_id),
            K(accept_proposal_id), K(last_lsn), K(log_mode_meta));
      } else if (OB_FAIL(reconfirm_.handle_prepare_response(server, proposal_id, accept_proposal_id,
              last_lsn))) {
        PALF_LOG(WARN, "reconfirm.handle_prepare_response failed", K(ret), KPC(this),
            K(proposal_id), K(accept_proposal_id), K(last_lsn));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int PalfHandleImpl::receive_mode_meta(const common::ObAddr &server,
                                      const int64_t proposal_id,
                                      const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == server.is_valid() ||
      INVALID_PROPOSAL_ID == proposal_id ||
      false == mode_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), KPC(this), K(server), K(proposal_id), K(mode_meta));
  } else if (OB_FAIL(try_update_proposal_id_(server, proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", KR(ret), KPC(this), K(server), K(proposal_id));
  } else {
    RLockGuard guard(lock_);
    bool has_accepted = false;
    if (false == mode_mgr_.can_receive_mode_meta(proposal_id, mode_meta, has_accepted)) {
      PALF_LOG(WARN, "can_receive_mode_meta failed", KR(ret), KPC(this), K(proposal_id), K(mode_meta));
    } else if (true == has_accepted) {
      if (OB_FAIL(log_engine_.submit_change_mode_meta_resp(server, proposal_id))) {
        PALF_LOG(WARN, "submit_change_mode_meta_resp failed", KR(ret), KPC(this), K(proposal_id), K(mode_meta));
      }
    } else if (OB_FAIL(mode_mgr_.receive_mode_meta(server, proposal_id, mode_meta))) {
      PALF_LOG(WARN, "receive_mode_meta failed", KR(ret), KPC(this), K(server), K(proposal_id),
          K(mode_meta));
    } else {
      PALF_LOG(INFO, "receive_mode_meta success", KR(ret), KPC(this), K(server), K(proposal_id), K(mode_meta));
    }
  }
  return ret;
}

int PalfHandleImpl::ack_mode_meta(const common::ObAddr &server,
                                  const int64_t msg_proposal_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  const int64_t &curr_proposal_id = state_mgr_.get_proposal_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret));
  } else if (msg_proposal_id != curr_proposal_id) {
    PALF_LOG(WARN, "proposal_id does not match", KR(ret), KPC(this), K(msg_proposal_id),
        K(server), K(curr_proposal_id));
  } else if (self_ != state_mgr_.get_leader()) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "self is not leader, state not match", KR(ret), KPC(this),
        K(server), K(msg_proposal_id), K(curr_proposal_id));
  } else if (OB_FAIL(mode_mgr_.ack_mode_meta(server, msg_proposal_id))) {
    PALF_LOG(WARN, "ack_mode_meta failed", KR(ret), KPC(this), K(server), K(msg_proposal_id));
  } else {
    PALF_LOG(INFO, "ack_mode_meta success", KR(ret), KPC(this), K(server), K(msg_proposal_id));
  }
  return ret;
}

int PalfHandleImpl::handle_election_message(const election::ElectionPrepareRequestMsg &msg)
{
  return election_.handle_message(msg);
}

int PalfHandleImpl::handle_election_message(const election::ElectionPrepareResponseMsg &msg)
{
  return election_.handle_message(msg);
}

int PalfHandleImpl::handle_election_message(const election::ElectionAcceptRequestMsg &msg)
{
  return election_.handle_message(msg);
}

int PalfHandleImpl::handle_election_message(const election::ElectionAcceptResponseMsg &msg)
{
  return election_.handle_message(msg);
}

int PalfHandleImpl::handle_election_message(const election::ElectionChangeLeaderMsg &msg)
{
  return election_.handle_message(msg);
}

int PalfHandleImpl::do_init_mem_(
    const int64_t palf_id,
    const PalfBaseInfo &palf_base_info,
    const LogMeta &log_meta,
    const char *log_dir,
    const common::ObAddr &self,
    FetchLogEngine *fetch_log_engine,
    ObILogAllocator *alloc_mgr,
    LogRpc *log_rpc,
    LogIOWorker *log_io_worker,
    PalfEnvImpl *palf_env_impl,
    common::ObOccamTimer *election_timer)
{
  int ret = OB_SUCCESS;
  int pret = -1;
  palf::PalfRoleChangeCbWrapper &role_change_cb_wrpper = role_change_cb_wrpper_;
  if ((pret = snprintf(log_dir_, MAX_PATH_SIZE, "%s", log_dir)) && false) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "error unexpected", K(ret), K(palf_id));
  } else if (OB_FAIL(sw_.init(palf_id, self, &state_mgr_, &config_mgr_, &mode_mgr_,
          &log_engine_, &fs_cb_wrapper_, alloc_mgr, palf_base_info))) {
    PALF_LOG(WARN, "sw_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(election_.init_and_start(palf_id,
                                              election_timer,
                                              &election_msg_sender_,
                                              self,
                                              1,
                                              [&role_change_cb_wrpper](int64_t id,
                                                                       const ObAddr &dest_addr){
    return role_change_cb_wrpper.on_need_change_leader(id, dest_addr);
  }))) {
    PALF_LOG(WARN, "election_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(state_mgr_.init(palf_id, self, log_meta.get_log_prepare_meta(), log_meta.get_log_replica_property_meta(),
          &election_, &sw_, &reconfirm_, &log_engine_, &config_mgr_, &mode_mgr_, &role_change_cb_wrpper_))) {
    PALF_LOG(WARN, "state_mgr_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(config_mgr_.init(palf_id, self, log_meta.get_log_config_meta(), &log_engine_, &sw_, &state_mgr_, &election_, &mode_mgr_))) {
    PALF_LOG(WARN, "config_mgr_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(reconfirm_.init(palf_id, self, &sw_, &state_mgr_, &config_mgr_, &mode_mgr_, &log_engine_))) {
    PALF_LOG(WARN, "reconfirm_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(mode_mgr_.init(palf_id, self, log_meta.get_log_mode_meta(), &state_mgr_, &log_engine_, &config_mgr_, &sw_))) {
    PALF_LOG(WARN, "mode_mgr_ init failed", K(ret), K(palf_id));
  } else {
    palf_id_ = palf_id;
    fetch_log_engine_ = fetch_log_engine;
    allocator_ = alloc_mgr;
    self_ = self;
    lc_cb_ = NULL;
    has_set_deleted_ = false;
    palf_env_impl_ = palf_env_impl;
    is_inited_ = true;
    PALF_LOG(INFO, "PalfHandleImpl do_init_ success", K(ret), K(palf_id), K(self), K(log_dir), K(palf_base_info),
        K(log_meta), K(fetch_log_engine), K(alloc_mgr), K(log_rpc), K(log_io_worker));
  }
  if (OB_FAIL(ret)) {
    is_inited_ = true;
    destroy();
  }
  return ret;
}

int PalfHandleImpl::get_palf_epoch(int64_t &palf_epoch) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    palf_epoch = log_engine_.get_palf_epoch();
  }
  return ret;
}

int PalfHandleImpl::check_req_proposal_id_(const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(proposal_id));
  } else {
    bool need_update_proposal_id = false;
    do {
      RLockGuard guard(lock_);
      if (proposal_id > state_mgr_.get_proposal_id()) {
        need_update_proposal_id = true;
      }
    } while(0);
    if (need_update_proposal_id) {
      WLockGuard guard(lock_);
      if (proposal_id > state_mgr_.get_proposal_id()) {
        // double check
      }
    }
  }
  return ret;
}

int PalfHandleImpl::try_update_proposal_id_(const common::ObAddr &server,
                                            const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id));
  } else {
    bool need_handle_prepare_req = false;
    do {
      // check if need update proposal_id
      RLockGuard guard(lock_);
      if (proposal_id > state_mgr_.get_proposal_id()) {
        need_handle_prepare_req = true;
      }
    } while(0);
    // try update proposal_id
    if (need_handle_prepare_req) {
      WLockGuard guard(lock_);
      if (!state_mgr_.can_handle_prepare_request(proposal_id)) {
        // can not handle prepare request
      } else if (OB_FAIL(state_mgr_.handle_prepare_request(server, proposal_id))) {
        PALF_LOG(WARN, "handle_prepare_request failed", K(ret), K(server), K(proposal_id));
      } else {
        // Call clean_log() when updating proposal_id to delete phantom logs(if it exists).
        (void) sw_.clean_log();
        PALF_LOG(INFO, "try_update_proposal_id_ finished", K(ret), K(server), K(proposal_id));
      }
    }
  }
  return ret;
}

int PalfHandleImpl::handle_committed_info(const common::ObAddr &server,
                                          const int64_t &msg_proposal_id,
                                          const int64_t prev_log_id,
                                          const int64_t &prev_log_proposal_id,
                                          const LSN &committed_end_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == msg_proposal_id || !committed_end_lsn.is_valid()
             || OB_INVALID_LOG_ID == prev_log_id || INVALID_PROPOSAL_ID == prev_log_proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(server), K(msg_proposal_id), K(prev_log_id),
       K(prev_log_proposal_id), K(committed_end_lsn));
  } else if (OB_FAIL(try_update_proposal_id_(server, msg_proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", K(ret), KPC(this), K(server), K(msg_proposal_id));
  } else {
    RLockGuard guard(lock_);
    if (!state_mgr_.can_handle_committed_info(msg_proposal_id)) {
      ret = OB_STATE_NOT_MATCH;
      if (palf_reach_time_interval(2 * 1000 * 1000, cannot_handle_committed_info_time_)) {
        PALF_LOG(WARN, "can not handle_committed_info", K(ret), KPC(this), K(server), K(msg_proposal_id),
            K(prev_log_id), K(prev_log_proposal_id), K(committed_end_lsn),
            "local proposal_id", state_mgr_.get_proposal_id(), "role", state_mgr_.get_role(),
            "state", state_mgr_.get_state(), "is_sync_enabled", state_mgr_.is_sync_enabled());
      }
    } else if (OB_FAIL(sw_.handle_committed_info(server, prev_log_id, prev_log_proposal_id, committed_end_lsn))) {
      PALF_LOG(WARN, "handle_committed_info failed", K(ret), KPC(this), K(server), K(msg_proposal_id),
          K(prev_log_id), K(prev_log_proposal_id), K(committed_end_lsn));
    } else {
      PALF_LOG(TRACE, "handle_committed_info success", K(ret), KPC(this), K(server), K(msg_proposal_id),
          K(prev_log_id), K(prev_log_proposal_id), K(committed_end_lsn));
    }
  }
  return ret;
}

int PalfHandleImpl::receive_log(const common::ObAddr &server,
                                const PushLogType push_log_type,
                                const int64_t &msg_proposal_id,
                                const LSN &prev_lsn,
                                const int64_t &prev_log_proposal_id,
                                const LSN &lsn,
                                const char *buf,
                                const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  TruncateLogInfo truncate_log_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == msg_proposal_id || !lsn.is_valid()
             || NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn),
       KP(buf), K(buf_len));
  } else if (OB_FAIL(try_update_proposal_id_(server, msg_proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", K(ret), KPC(this), K(server), K(msg_proposal_id));
  } else {
    // rdlock
    RLockGuard guard(lock_);
    if (false == palf_env_impl_->check_disk_space_enough()) {
      ret = OB_LOG_OUTOF_DISK_SPACE;
      if (palf_reach_time_interval(1 * 1000 * 1000, log_disk_full_warn_time_)) {
        PALF_LOG(WARN, "log outof disk space", K(ret), KPC(this), K(server), K(push_log_type), K(lsn));
      }
    } else if (!state_mgr_.can_receive_log(msg_proposal_id)) {
      ret = OB_STATE_NOT_MATCH;
      if (palf_reach_time_interval(2 * 1000 * 1000, cannot_recv_log_warn_time_)) {
        PALF_LOG(WARN, "can not receive log", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn),
            "local proposal_id", state_mgr_.get_proposal_id(), "role", state_mgr_.get_role(),
            "state", state_mgr_.get_state(), "is_sync_enabled", state_mgr_.is_sync_enabled());
      }
    } else if (OB_FAIL(sw_.receive_log(server, push_log_type, prev_lsn, prev_log_proposal_id, lsn,
            buf, buf_len, true, truncate_log_info))) {
      if(OB_EAGAIN == ret) {
        // rewrite -4023 to 0
        ret = OB_SUCCESS;
      } else {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          PALF_LOG(WARN, "sw_ receive_log failed", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn));
        }
      }
    } else {
      PALF_LOG(TRACE, "receive_log success", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn), K(buf_len));
    }
  }
  // check if need truncate
  if (OB_SUCC(ret) && (INVALID_TRUNCATE_TYPE != truncate_log_info.truncate_type_)) {
    PALF_LOG(INFO, "begin clean/truncate log", K(ret), KPC(this), K(server), K_(self), K(msg_proposal_id),
        K(lsn), K(buf_len), K(truncate_log_info));
    // write lock
    WLockGuard guard(lock_);
    if (!state_mgr_.can_receive_log(msg_proposal_id)) {
      ret = OB_STATE_NOT_MATCH;
      PALF_LOG(WARN, "can not receive log", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn),
          "role", state_mgr_.get_role());
    } else if (TRUNCATE_CACHED_LOG_TASK == truncate_log_info.truncate_type_) {
      if (OB_FAIL(sw_.clean_cached_log(truncate_log_info.truncate_log_id_, lsn, prev_lsn, prev_log_proposal_id))) {
        PALF_LOG(WARN, "sw_ clean_cached_log failed", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn),
            K(truncate_log_info));
      }
    } else if (TRUNCATE_LOG == truncate_log_info.truncate_type_) {
      if (!state_mgr_.can_truncate_log()) {
        ret = OB_STATE_NOT_MATCH;
        PALF_LOG(WARN, "can not truncate log", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn),
            "role", state_mgr_.get_role(), "state", state_mgr_.get_state());
      } else if (OB_FAIL(sw_.truncate(truncate_log_info, prev_lsn, prev_log_proposal_id))) {
        PALF_LOG(WARN, "sw_ truncate failed", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn));
      } else if (OB_FAIL(state_mgr_.truncate(truncate_log_info.truncate_begin_lsn_))) {
        PALF_LOG(WARN, "state_mgr_ truncate failed", K(ret), KPC(this), K(server), K(msg_proposal_id),
            K(prev_lsn), K(prev_log_proposal_id), K(lsn), K(lsn), K(lsn));
      } else {
        // do nothing
      }
    } else {
      // do nothing
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sw_.receive_log(server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, buf,
            buf_len, false, truncate_log_info))) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        PALF_LOG(WARN, "sw_ receive_log failed", K(ret), KPC(this), K(server), K(msg_proposal_id), K(lsn));
      }
    } else {
      PALF_LOG(INFO, "receive_log success", K(ret), KPC(this), K(server), K_(self), K(msg_proposal_id), K(prev_lsn),
          K(prev_log_proposal_id), K(lsn), K(truncate_log_info));
    }
  }
  return ret;
}

int PalfHandleImpl::submit_group_log(const PalfAppendOptions &opts,
                                     const LSN &lsn,
                                     const char *buf,
                                     const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!lsn.is_valid() || NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), KP(buf), K(buf_len));
  } else {
    int64_t wait_times = 0;
    while (true) {
      do {
        RLockGuard guard(lock_);
        if (false == palf_env_impl_->check_disk_space_enough()) {
          ret = OB_LOG_OUTOF_DISK_SPACE;
          if (palf_reach_time_interval(1 * 1000 * 1000, log_disk_full_warn_time_)) {
            PALF_LOG(WARN, "log outof disk space", K(ret), KPC(this), K(opts), K(lsn));
          }
        } else if (!state_mgr_.can_raw_write(opts.proposal_id, opts.need_check_proposal_id)) {
          ret = OB_NOT_MASTER;
          PALF_LOG(WARN, "cannot submit_group_log", K(ret), K_(self), K_(palf_id), KP(buf), K(buf_len),
              "role", state_mgr_.get_role(), "state", state_mgr_.get_state(),
              "current proposal_id", state_mgr_.get_proposal_id(),
              "mode_mgr can_raw_write", mode_mgr_.can_raw_write(), K(opts));
        } else if (OB_FAIL(sw_.submit_group_log(lsn, buf, buf_len))) {
          PALF_LOG(WARN, "submit_group_log failed", K(ret), K_(palf_id), K_(self), KP(buf), K(buf_len));
        } else {
          PALF_LOG(TRACE, "submit_group_log success", K(ret), K_(palf_id), K_(self), K(buf_len), K(lsn));
        }
      } while(0);

      if (opts.need_nonblock) {
        // nonblock mode, end loop
        break;
      } else if (OB_EAGAIN == ret) {
        // block mode, need sleep and retry for -4023 ret code
        static const int64_t MAX_SLEEP_US = 100;
        ++wait_times;
        int64_t sleep_us = wait_times * 10;
        if (sleep_us > MAX_SLEEP_US) {
          sleep_us = MAX_SLEEP_US;
        }
        ob_usleep(sleep_us);
      } else {
        // other ret code, end loop
        break;
      }
    }
  }
  return ret;
}

int PalfHandleImpl::ack_log(const common::ObAddr &server,
                            const int64_t &proposal_id,
                            const LSN &log_end_lsn)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id || !log_end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id), K(log_end_lsn));
  } else if (!state_mgr_.can_receive_log_ack(proposal_id)) {
    // cannot handle log ack, skip
  } else if (OB_FAIL(sw_.ack_log(server, log_end_lsn))) {
    PALF_LOG(WARN, "ack_log failed", K(ret), K(server), K(proposal_id), K(log_end_lsn));
  } else {
    PALF_LOG(TRACE, "ack_log success", K(ret), K(server), K(proposal_id), K(log_end_lsn));
  }
  return ret;
}

int PalfHandleImpl::get_last_rebuild_lsn(LSN &last_rebuild_lsn) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    SpinLockGuard guard(last_rebuild_lsn_lock_);
    last_rebuild_lsn = last_rebuild_lsn_;
  }
  return ret;
}

int PalfHandleImpl::check_need_advance_base_info_(const LSN &base_lsn,
                                                  const LogInfo &base_prev_log_info,
                                                  const bool is_rebuild)
{
  int ret = OB_SUCCESS;
  LSN committed_end_lsn;
  AccessMode curr_access_mode;
  bool unused_bool = false, need_rebuild = false;
  int64_t unused_mode_version;
  (void)mode_mgr_.get_access_mode(unused_mode_version, curr_access_mode);
  if (!base_lsn.is_valid() || !base_prev_log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(base_lsn), K(base_prev_log_info));
  } else if (state_mgr_.is_leader_active() && curr_access_mode != AccessMode::RAW_WRITE) {
    // when access mode is RAW_WRITE, that means this palf is restoring, advance_base_info can be allowed
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "self is active leader, can not advance_base_info", K(ret), K_(palf_id), K(curr_access_mode), K(base_lsn));
  } else if (OB_FAIL(sw_.get_committed_end_lsn(committed_end_lsn))) {
    PALF_LOG(WARN, "get_committed_end_lsn failed", KR(ret), K_(palf_id));
  } else if (base_lsn < committed_end_lsn) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "base_lsn is less than local committed_end_lsn, it's maybe a stale msg",
        K(ret), K_(palf_id), K(base_lsn), K(committed_end_lsn));
  } else if (true == is_rebuild && OB_FAIL(check_need_rebuild_(base_lsn, base_prev_log_info, need_rebuild, unused_bool))) {
    PALF_LOG(WARN, "check_need_rebuild failed", K(ret), KPC(this), K(base_lsn), K(base_prev_log_info), K(is_rebuild));
  } else if (true == is_rebuild && false == need_rebuild) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "do not need rebuild", K(ret), KPC(this), K(base_lsn), K(base_prev_log_info), K(is_rebuild));
  }
  return ret;
}

// caller should hold wlock when calling this function
int PalfHandleImpl::check_need_rebuild_(const LSN &base_lsn,
                                        const LogInfo &base_prev_log_info,
                                        bool &need_rebuild,
                                        bool &need_fetch_log)
{
  int ret = OB_SUCCESS;
  LSN committed_end_lsn;
  LSN last_submit_lsn;
  int64_t last_submit_log_id;
  int64_t last_submit_log_pid;
  bool unused_bool;
  if (!base_lsn.is_valid() || !base_prev_log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(base_lsn), K(base_prev_log_info));
  } else if (state_mgr_.is_leader_active()) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "self is active leader, no need execute rebuild", K(ret), K_(palf_id), K(base_lsn));
  } else if (OB_FAIL(sw_.get_committed_end_lsn(committed_end_lsn))) {
    PALF_LOG(WARN, "get_committed_end_lsn failed", KR(ret), K_(palf_id));
  } else if (base_lsn <= committed_end_lsn) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "base_lsn is less than or equal to local committed_end_lsn",
        K(ret), K_(palf_id), K(base_lsn), K(committed_end_lsn));
  } else if (OB_FAIL(sw_.get_last_submit_log_info(last_submit_lsn, last_submit_log_id, last_submit_log_pid))) {
    PALF_LOG(WARN, "get_last_submit_log_info failed", KR(ret), K_(palf_id));
  } else if (last_submit_lsn < base_prev_log_info.lsn_) {
    // previous log hasn't been submitted, need rebuild
    need_rebuild = true;
    PALF_LOG(INFO, "palf need rebuild, reason: previous log hasn't been submitted", K(ret), KPC(this),
        K(need_rebuild), K(base_lsn), K(base_prev_log_info), K(last_submit_lsn));
  } else if (last_submit_lsn == base_prev_log_info.lsn_) {
    // previous log has just been submitted, need check if matches
    if (last_submit_log_id == base_prev_log_info.log_id_ &&
        last_submit_log_pid == base_prev_log_info.log_proposal_id_) {
      // previous log is in sliding window and matches, don't rebuild, need fetch log
      need_fetch_log = true;
    } else {
      // previous log don't match, need rebuild
      need_rebuild = true;
      PALF_LOG(INFO, "palf need rebuild, previous log don't match", K(ret), KPC(this),
          K(need_rebuild), K(base_lsn), K(base_prev_log_info), K(last_submit_log_id), K(last_submit_log_pid));
    }
  } else if (sw_.is_prev_log_pid_match(base_prev_log_info.log_id_ + 1, base_lsn,
      base_prev_log_info.lsn_, base_prev_log_info.log_proposal_id_, unused_bool)) {
    // when last_submit_lsn > base_prev_log_info.lsn_, to check if previous log matches.
    // if matches, don't rebuild, need fetch log
    need_fetch_log = true;
  } else {
    // if prev log is in sliding window and don't match, do rebuild
    // if prev log isn't in sliding window, do rebuild
    need_rebuild = true;
    PALF_LOG(INFO, "palf need rebuild, previous log don't match", K(ret), KPC(this),
        K(need_rebuild), K(base_lsn), K(base_prev_log_info), K(last_submit_log_id), K(last_submit_log_pid));
  }
  return ret;
}

int PalfHandleImpl::handle_notify_rebuild_req(const common::ObAddr &server,
                                              const LSN &base_lsn,
                                              const LogInfo &base_prev_log_info)
{
  int ret = OB_SUCCESS;
  // This function will be called when:
  // This node A has sent a fetch log req to B, B find that A's fetch_start_lsn is smaller than B's base_lsn
  // (B's log file may be recycled), therefore B will notify A may need do rebuild, send it's base_lsn and prev_log_info
  // of base_lsn to A. This function will be called when A receive this message.
  // After receiving rebuild_req, A will check whether previous log entries of base_lsn are matching in this function.
  // The main check logic is:
  // 1. require write lock
  // 2. ensure base_lsn > committed_end_lsn. If base_lsn <= committed_end_lsn, that means it's possible
  // for A to get log from B in next fetch log, so ignore.
  // 3. check if previous log is cached in sliding window and matches,
  // If it does, do not need rebuild and fetch log, if not, do rebuild!!!

  // NB: Why require write lock in here?
  // In step 2, base_lsn > committed_end_lsn is guaranteed. if require write lock and don't find
  // previous log in sliding window finally, therefore we can ensure this node must do rebuild.
  // (When this function hold write lock, committed log can not be slid.)
  // If we require read lock, previous log may has slid when we read it from sliding window,
  // this will cause wrong rebuild.
  bool need_rebuild = false;
  bool need_fetch_log = false;
  do {
    int tmp_ret = OB_SUCCESS;
    // leader may send multiple notify_rebuild_req, when next req arrives, previous on_rebuild may
    // hold rlock, so try hold wlock and release it after timeout (1ms).
    const int64_t until_timeout_us = common::ObTimeUtility::current_time() + 1 * 1000;
    WLockGuardWithTimeout guard(lock_, until_timeout_us, tmp_ret);
    if (OB_SUCCESS != tmp_ret) {
      PALF_LOG(INFO, "notify_rebuild wait lock timeout", K(ret), KPC(this), K(server), K(base_lsn),
        K(base_prev_log_info));
    } else if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (!server.is_valid() || !base_lsn.is_valid() ||
        !base_prev_log_info.is_valid() || base_lsn.val_ == PALF_INITIAL_LSN_VAL) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(server), K(base_lsn));
    } else if (OB_FAIL(check_need_rebuild_(base_lsn, base_prev_log_info, need_rebuild, need_fetch_log))) {
      PALF_LOG(WARN, "check_need_rebuild failed", K(ret), KPC(this), K(server), K(base_lsn), K(base_prev_log_info));
    }
  } while (0);

  // can not hold wlock when exec on_rebuild
  if (need_rebuild) {
    if (OB_FAIL(rebuild_cb_wrapper_.on_rebuild(palf_id_, base_lsn))) {
      PALF_LOG(WARN, "on_rebuild failed", K(ret), K(server), K(base_lsn));
    } else {
      PALF_EVENT("on_rebuild success", palf_id_, K(ret), K_(self), K(server), K(base_lsn));
    }
    // Whether on_rebuild returns OB_SUCCESS or not, set value for rebuild_base_lsn_
    SpinLockGuard rebuild_guard(last_rebuild_lsn_lock_);
    last_rebuild_lsn_ = base_lsn;
  } else if (need_fetch_log && OB_FAIL(sw_.try_fetch_log(FetchTriggerType::NOTIFY_REBUILD,
      base_prev_log_info.lsn_, base_lsn, base_prev_log_info.log_id_+1))) {
      PALF_LOG(WARN, "try_fetch_log failed", KR(ret), KPC(this), K(server), K(base_lsn), K(base_prev_log_info));
  }
  return ret;
}

int PalfHandleImpl::fetch_log_from_storage(const common::ObAddr &server,
                                           const FetchLogType fetch_type,
                                           const int64_t &msg_proposal_id,
                                           const LSN &prev_lsn,
                                           const LSN &fetch_start_lsn,
                                           const int64_t fetch_log_size,
                                           const int64_t fetch_log_count,
                                           const int64_t accepted_mode_pid)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (FETCH_MODE_META == fetch_type) {
    if (OB_FAIL(submit_fetch_mode_meta_resp_(server, msg_proposal_id, accepted_mode_pid))) {
      PALF_LOG(WARN, "submit_fetch_mode_meta_resp_ failed", K(ret), K_(palf_id), K_(self),
          K(msg_proposal_id), K(accepted_mode_pid));
    }
  } else if (OB_FAIL(fetch_log_from_storage_(server, fetch_type, msg_proposal_id, prev_lsn,
      fetch_start_lsn, fetch_log_size, fetch_log_count))) {
    PALF_LOG(WARN, "fetch_log_from_storage_ failed", K(ret), K_(palf_id), K_(self),
        K(server), K(fetch_type), K(msg_proposal_id), K(prev_lsn), K(fetch_start_lsn),
        K(fetch_log_size), K(fetch_log_count), K(accepted_mode_pid));
  }
  return ret;
}

int PalfHandleImpl::try_send_committed_info_(const ObAddr &server,
                                             const LSN &log_lsn,
                                             const LSN &log_end_lsn,
                                             const int64_t &log_proposal_id)
{
  int ret = OB_SUCCESS;
  AccessMode access_mode;
  if (!log_lsn.is_valid() || !log_end_lsn.is_valid() || INVALID_PROPOSAL_ID == log_proposal_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(mode_mgr_.get_access_mode(access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret), KPC(this));
  } else if (AccessMode::APPEND == access_mode) {
    // No need send committed_info in APPEND mode, because leader will genenrate keeapAlive log periodically.
  } else if (OB_FAIL(sw_.try_send_committed_info(server, log_lsn, log_end_lsn, log_proposal_id))) {
    PALF_LOG(TRACE, "try_send_committed_info failed", K(ret), K_(palf_id), K_(self),
      K(server), K(log_lsn), K(log_end_lsn), K(log_proposal_id));
  } else {
    PALF_LOG(TRACE, "try_send_committed_info_ success", K(ret), K_(palf_id), K_(self), K(server),
      K(log_lsn), K(log_end_lsn), K(log_proposal_id));
  }
  return ret;
}

int PalfHandleImpl::fetch_log_from_storage_(const common::ObAddr &server,
                                            const FetchLogType fetch_type,
                                            const int64_t &msg_proposal_id,
                                            const LSN &prev_lsn,
                                            const LSN &fetch_start_lsn,
                                            const int64_t fetch_log_size,
                                            const int64_t fetch_log_count)
{
  int ret = OB_SUCCESS;
  PalfGroupBufferIterator iterator;
  const LSN fetch_end_lsn = fetch_start_lsn + fetch_log_size;
  const bool need_check_prev_log = (prev_lsn.is_valid() && PALF_INITIAL_LSN_VAL < fetch_start_lsn.val_);
  LSN max_flushed_end_lsn;
  LSN committed_end_lsn;
  bool is_limitted_by_end_lsn = true;
  // Assign values for max_flushed_end_lsn/committed_end_lsn/is_limitted_by_end_lsn with rdlock
  // to avoid concurrent update with switch_state/truncate (with wrlock).
  do {
    RLockGuard guard(lock_);
    sw_.get_max_flushed_end_lsn(max_flushed_end_lsn);
    committed_end_lsn = get_end_lsn();
    // Note: Self may revoke after unlock, so the uncommitted logs may be sent later.
    //       But we use req's msg_proposal_id to send response, which gurantees no harm.
    //       Because if another new leader has taken over, the majority must have advanced
    //       its proposal_id to reject fetch response with old proposal_id.
    if (FETCH_LOG_LEADER_RECONFIRM == fetch_type
        || state_mgr_.is_leader_active()
        || state_mgr_.is_leader_reconfirm()) {
      // leader reconfirm状态也要允许发送unconfirmed log，否则start_working日志可能无法达成多数派
      // 因为多数派副本日志落后时，receive config_log前向校验会失败
      // reconfirm状态下unconfirmed log可以安全地发出，因为这部分日志预期不会被truncate
      is_limitted_by_end_lsn = false;
    }
  } while(0);

  // max_flushed_end_lsn may be truncated by concurrent truncate, so itreator need handle this
  // case when it try to read some log which is being truncated.
  auto get_file_end_lsn = [&]() {
    return max_flushed_end_lsn;
  };
  LogInfo prev_log_info;
  if (prev_lsn >= max_flushed_end_lsn) {
    PALF_LOG(INFO, "no need fetch_log_from_storage", K(ret), KPC(this), K(server), K(fetch_start_lsn), K(prev_lsn),
        K(max_flushed_end_lsn));
  } else if (true == need_check_prev_log
      && OB_FAIL(get_prev_log_info_(fetch_start_lsn, prev_log_info))) {
    PALF_LOG(WARN, "get_prev_log_info_ failed", K(ret), K_(palf_id), K(prev_lsn), K(fetch_start_lsn));
  } else if (true == need_check_prev_log && prev_log_info.lsn_ != prev_lsn) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "the LSN between each replica is not same, unexpected error!!!", K(ret),
        K_(palf_id), K(fetch_start_lsn), K(prev_log_info));
  } else if (OB_FAIL(iterator.init(fetch_start_lsn, log_engine_.get_log_storage(), get_file_end_lsn))) {
    PALF_LOG(WARN, "PalfGroupBufferIterator init failed", K(ret), K_(palf_id));
  } else {
    LSN each_round_prev_lsn = prev_lsn;
    LogGroupEntry curr_group_entry;
    LSN curr_lsn;
    bool is_reach_size_limit = false;  // whether the total fetched size exceeds fetch_log_size
    bool is_reach_count_limit = false;
    bool is_reach_end = false;
    int64_t fetched_count = 0;
    LSN curr_log_end_lsn = curr_lsn + curr_group_entry.get_group_entry_size();
    LSN prev_log_end_lsn;
    int64_t prev_log_proposal_id = prev_log_info.log_proposal_id_;
    while (OB_SUCC(ret) && !is_reach_size_limit && !is_reach_count_limit && !is_reach_end
        && OB_SUCC(iterator.next())) {
      if (OB_FAIL(iterator.get_entry(curr_group_entry, curr_lsn))) {
        PALF_LOG(ERROR, "PalfGroupBufferIterator get_entry failed", K(ret), K_(palf_id),
            K(curr_group_entry), K(curr_lsn), K(iterator));
      } else if (FALSE_IT(curr_log_end_lsn = curr_lsn + curr_group_entry.get_group_entry_size())) {
      } else if (is_limitted_by_end_lsn && curr_log_end_lsn > committed_end_lsn) {
        // Only leader replica can send uncommitted logs to others,
        // the other replicas just send committed logs to avoid unexpected rewriting.
        is_reach_end = true;
        PALF_LOG(INFO, "reach committed_end_lsn(not leader active replica), end fetch", K(ret), K_(palf_id), K(server),
            K(msg_proposal_id), K(curr_lsn), K(curr_log_end_lsn), K(committed_end_lsn));
      } else if (OB_FAIL(submit_fetch_log_resp_(server, msg_proposal_id, prev_log_proposal_id, \
              each_round_prev_lsn, curr_lsn, curr_group_entry))) {
        PALF_LOG(WARN, "submit_fetch_log_resp_ failed", K(ret), K_(palf_id), K(server),
            K(msg_proposal_id), K(each_round_prev_lsn), K(fetch_start_lsn));
      } else {
        fetched_count++;
        if (fetched_count >= fetch_log_count) {
          is_reach_count_limit = true;
        }
        // check if reach size limit
        if (curr_log_end_lsn >= fetch_end_lsn) {
          is_reach_size_limit = true;
        }
        PALF_LOG(TRACE, "fetch one log success", K(ret), K_(palf_id), K_(self), K(server), K(prev_lsn),
            K(fetch_start_lsn), K(each_round_prev_lsn), K(curr_lsn), K(curr_group_entry),
            K(prev_log_proposal_id), K(fetch_end_lsn), K(curr_log_end_lsn), K(is_reach_size_limit),
            K(fetch_log_size), K(fetched_count), K(is_reach_count_limit));
        each_round_prev_lsn = curr_lsn;
        prev_log_end_lsn = curr_log_end_lsn;
        prev_log_proposal_id = curr_group_entry.get_header().get_log_proposal_id();
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    // try send committed_info to server
    if (OB_SUCC(ret)) {
      RLockGuard guard(lock_);
      (void) try_send_committed_info_(server, each_round_prev_lsn, prev_log_end_lsn, prev_log_proposal_id);
    }
  }

  if (OB_FAIL(ret) && OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    // ret is OB_ERR_OUT_OF_LOWER_BOUND, need notify dst server to trigger rebuild
    LSN base_lsn = log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_;
    LogInfo base_prev_log_info;
    if (!base_lsn.is_valid()) {
      PALF_LOG(WARN, "local base_lsn is invalid, cannot notify server to rebuild", K(ret),
          K_(palf_id), K(server), K(base_lsn), K(base_prev_log_info));
    } else if (OB_FAIL(get_prev_log_info_(base_lsn, base_prev_log_info))) {
      // NB: snapshot_meta.prev_log_info_ is invalid in most common case, so we need to read base_prev_log_info from disk
      PALF_LOG(WARN, "local base_prev_log_info is invalid, cannot notify server to rebuild", K(ret),
          K_(palf_id), K(server), K(base_lsn), K(base_prev_log_info));
    } else if (OB_FAIL(log_engine_.submit_notify_rebuild_req(server, base_lsn, base_prev_log_info))) {
      PALF_LOG(WARN, "submit_notify_rebuild_req failed", K(ret), K_(palf_id), K(server), K(base_lsn), K(base_prev_log_info));
    } else {
      PALF_LOG(INFO, "submit_notify_rebuild_req success", K(ret), K_(palf_id), K(server), K(prev_lsn),
          K(fetch_start_lsn), K(base_lsn), K(base_prev_log_info));
    }
  }
  return ret;
}

int PalfHandleImpl::submit_fetch_mode_meta_resp_(const common::ObAddr &server,
                                                 const int64_t msg_proposal_id,
                                                 const int64_t accepted_mode_pid)
{
  int ret = OB_SUCCESS;
  const LogModeMeta mode_meta = mode_mgr_.get_accepted_mode_meta();
  common::ObMemberList member_list;
  if (mode_meta.proposal_id_ > accepted_mode_pid &&
      OB_SUCC(member_list.add_server(server)) &&
      OB_FAIL(log_engine_.submit_change_mode_meta_req(member_list, msg_proposal_id, mode_meta))) {
    PALF_LOG(WARN, "submit_change_mode_meta_req failed", K(ret), K_(palf_id), K(server),
        K(server), K(msg_proposal_id), K(mode_meta));
  } else {
    PALF_LOG(INFO, "submit_change_mode_meta_req success", K(ret), K_(palf_id), K(server), K(mode_meta),
        K(accepted_mode_pid));
  }
  return ret;
}

int PalfHandleImpl::submit_fetch_log_resp_(const common::ObAddr &server,
                                           const int64_t &msg_proposal_id,
                                           const int64_t &prev_log_proposal_id,
                                           const LSN &prev_lsn,
                                           const LSN &curr_lsn,
                                           const LogGroupEntry &curr_group_entry)
{
  int ret = OB_SUCCESS;
  LogWriteBuf write_buf;
  // NB: 'curr_group_entry' generates by PalfGroupBufferIterator, the memory is safe before next();
  const char *buf = curr_group_entry.get_data_buf() - curr_group_entry.get_header().get_serialize_size();
  const int64_t buf_len = curr_group_entry.get_group_entry_size();
  int64_t pos = 0;
  const int64_t curr_log_proposal_id = curr_group_entry.get_header().get_log_proposal_id();
  if (OB_FAIL(write_buf.push_back(buf, buf_len))) {
    PALF_LOG(WARN, "push_back buf into LogWriteBuf failed", K(ret));
  } else if (OB_FAIL(log_engine_.submit_push_log_req(server, FETCH_LOG_RESP, msg_proposal_id, prev_log_proposal_id,
        prev_lsn, curr_lsn, write_buf))) {
    PALF_LOG(WARN, "submit_push_log_req failed", K(ret), K(server), K(msg_proposal_id), K(prev_log_proposal_id),
        K(prev_lsn), K(curr_lsn), K(curr_log_proposal_id), K(write_buf));
  } else {
    PALF_LOG(TRACE, "submit_fetch_log_resp_ success", K(ret), K(server), K(msg_proposal_id), K(prev_log_proposal_id),
        K(prev_lsn), K(curr_lsn), K(curr_log_proposal_id), K(write_buf));
  }
  return ret;
}

// NB: 1. there is no need to distinguish between reconfirm or follower active;
//     2. whether msg_proposal_id is equal to currentry proposal_id or not, response logs to the requester.
int PalfHandleImpl::get_log(const common::ObAddr &server,
                            const FetchLogType fetch_type,
                            const int64_t msg_proposal_id,
                            const LSN &prev_lsn,
                            const LSN &start_lsn,
                            const int64_t fetch_log_size,
                            const int64_t fetch_log_count,
                            const int64_t accepted_mode_pid)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == server.is_valid()
             || INVALID_PROPOSAL_ID == msg_proposal_id
             || false == start_lsn.is_valid()
             || 0 >= fetch_log_size
             || 0 >= fetch_log_count
             || INVALID_PROPOSAL_ID == accepted_mode_pid) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(server), K(msg_proposal_id), K(prev_lsn),
        K(start_lsn), K(fetch_log_size), K(fetch_log_count), K(accepted_mode_pid));
  } else if (OB_FAIL(try_update_proposal_id_(server, msg_proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", KR(ret), KPC(this), K(server), K(msg_proposal_id));
  } else {
    FetchLogTask *task = NULL;
    RLockGuard guard(lock_);
    if (NULL == (task = fetch_log_engine_->alloc_fetch_log_task())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PALF_LOG(WARN, "alloc fetch log task failed", K(ret), K_(palf_id));
    } else if (OB_FAIL(task->set(palf_id_, server, fetch_type, msg_proposal_id,
                                 prev_lsn, start_lsn, fetch_log_size, fetch_log_count, accepted_mode_pid))) {
      PALF_LOG(WARN, "set fetch log task error", K(ret), K_(palf_id),
               K(msg_proposal_id), K(prev_lsn), K(start_lsn), K(fetch_log_size), K(fetch_log_count), K(accepted_mode_pid));
    } else if (OB_FAIL(fetch_log_engine_->submit_fetch_log_task(task))) {
      PALF_LOG(WARN, "submit fetch log task error", K(ret), K_(palf_id), K(server),
               K(prev_lsn), K(start_lsn), K(fetch_log_size), K(fetch_log_count), K(accepted_mode_pid));
    } else {
      PALF_LOG(INFO, "submit fetch log task success", K(ret), K_(palf_id), K(server), K(fetch_type), K(prev_lsn),
               K(start_lsn), K(fetch_log_size), K(fetch_log_count), K(accepted_mode_pid));
    }
    if (OB_SUCCESS != ret) {
      fetch_log_engine_->free_fetch_log_task(task);
    }
  }
  return ret;
}

int PalfHandleImpl::receive_config_log(const common::ObAddr &server,
                                       const int64_t &msg_proposal_id,
                                       const int64_t &prev_log_proposal_id,
                                       const LSN &prev_lsn,
                                       const int64_t &prev_mode_pid,
                                       const LogConfigMeta &meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret));
  } else if (false == server.is_valid() ||
             self_ == server ||
             INVALID_PROPOSAL_ID == msg_proposal_id ||
             false == prev_lsn.is_valid() ||
             INVALID_PROPOSAL_ID == prev_mode_pid ||
             false == meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(server),
        K(msg_proposal_id), K(prev_lsn), K(meta));
  } else if (OB_FAIL(try_update_proposal_id_(server, msg_proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", KR(ret), KPC(this), K(server), K(msg_proposal_id));
  } else {
    TruncateLogInfo truncate_log_info;
    bool need_print_register_log = false;
    // need wlock in case of truncating log and writing log_ms_meta in LogConfigMgr
    WLockGuard guard(lock_);
    if (false == state_mgr_.can_receive_log(msg_proposal_id) ||
        false == config_mgr_.can_receive_ms_log(server, meta)) {
      ret = OB_STATE_NOT_MATCH;
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        PALF_LOG(WARN, "can not receive log", KR(ret), KPC(this), K(msg_proposal_id), "role", state_mgr_.get_role());
      }
    } else if (mode_mgr_.get_accepted_mode_meta().proposal_id_ < prev_mode_pid) {
      // need fetch mode_meta
      if (OB_FAIL(sw_.try_fetch_log(FetchTriggerType::MODE_META_BARRIER))) {
        PALF_LOG(WARN, "try_fetch_log with MODE_META_BARRIER failed",
            KR(ret), KPC(this), K(server), K(msg_proposal_id), K(prev_mode_pid), K(meta));
      } else {
        PALF_LOG(INFO, "pre_check_for_mode_meta don't match, try fetch mode meta",
            KR(ret), KPC(this), K(server), K(msg_proposal_id), K(prev_mode_pid), K(meta));
      }
    } else if (!sw_.pre_check_for_config_log(msg_proposal_id, prev_lsn, prev_log_proposal_id, truncate_log_info)) {
      ret = OB_STATE_NOT_MATCH;
      PALF_LOG(WARN, "pre_check_for_config_log failed, cannot receive config log",
          KR(ret), KPC(this), K(server), K(msg_proposal_id), K(prev_lsn), K(prev_log_proposal_id), K(meta));
    } else if (TRUNCATE_LOG == truncate_log_info.truncate_type_
        && OB_FAIL(sw_.truncate(truncate_log_info, prev_lsn, prev_log_proposal_id))) {
        PALF_LOG(WARN, "sw truncate failed", KR(ret), KPC(this), K(truncate_log_info));
    } else if (OB_FAIL(config_mgr_.receive_config_log(server, meta))) {
      PALF_LOG(WARN, "receive_config_log failed", KR(ret), KPC(this), K(server), K(msg_proposal_id),
          K(prev_log_proposal_id), K(prev_lsn));
    } else if (!meta.curr_.log_sync_memberlist_.contains(self_) &&
               meta.curr_.arbitration_member_.get_server() != self_ &&
               !FALSE_IT(config_mgr_.register_parent()) &&
               FALSE_IT(need_print_register_log = true)) {
    // it's a optimization. If self isn't in memberlist, then register parent right now,
    // otherwise this new added learner will register parent in 4s at most and its log will be far behind.
    } else {
      PALF_LOG(INFO, "receive_config_log success", KR(ret), KPC(this), K(server), K(msg_proposal_id),
          K(prev_lsn), K(prev_log_proposal_id), K(meta));
    }
    if (need_print_register_log) {
      PALF_LOG(INFO, "re_register_parent reason: self may in learnerlist", KPC(this), K(server), K(meta));
    }
  }
  return ret;
}

int PalfHandleImpl::ack_config_log(const common::ObAddr &server,
                                   const int64_t msg_proposal_id,
                                   const LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  const int64_t &curr_proposal_id = state_mgr_.get_proposal_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret));
  } else if (msg_proposal_id != curr_proposal_id) {
    PALF_LOG(WARN, "proposal_id does not match", KR(ret), KPC(this), K(msg_proposal_id),
        K(server), K(curr_proposal_id));
  } else if (self_ != state_mgr_.get_leader()) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "self is not leader, state not match", KR(ret), KPC(this),
        K(server), K(msg_proposal_id), K(curr_proposal_id));
  } else if (OB_FAIL(config_mgr_.ack_config_log(server, msg_proposal_id, config_version))) {
    PALF_LOG(WARN, "ObLogConfigMgr ack_config_log failed", KR(ret), KPC(this), K(server), K(msg_proposal_id), K(config_version));
  } else {
    PALF_LOG(INFO, "ack_config_log success", KR(ret), KPC(this), K(server), K(msg_proposal_id), K(config_version));
  }
  return ret;
}

int64_t PalfHandleImpl::get_total_used_disk_space() const
{
  int64_t total_used_disk_space = 0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_engine_.get_total_used_disk_space(total_used_disk_space))) {
    PALF_LOG(WARN, "get_total_used_disk_space failed", K(ret), KPC(this));
  } else {
  }
  return total_used_disk_space;
}

int PalfHandleImpl::advance_reuse_lsn(const LSN &flush_log_end_lsn)
{
  // Do not hold lock here.
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sw_.advance_reuse_lsn(flush_log_end_lsn))) {
    PALF_LOG(WARN, "sw_.advance_reuse_lsn failed", K(ret), K(flush_log_end_lsn));
  } else {
    PALF_LOG(TRACE, "advance_reuse_lsn success", K(ret), K(flush_log_end_lsn));
  }
  return ret;
}

int PalfHandleImpl::inner_after_flush_log(const FlushLogCbCtx &flush_log_cb_ctx)
{
  int ret = OB_SUCCESS;
  PALF_LOG(TRACE, "after_flush_log begin", K(flush_log_cb_ctx), K_(self),
      "cost time", ObTimeUtility::current_time() - flush_log_cb_ctx.begin_ts_);
  const int64_t begin_ts = ObTimeUtility::current_time();
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sw_.after_flush_log(flush_log_cb_ctx))) {
    PALF_LOG(WARN, "sw_.after_flush_log failed", K(ret), K(flush_log_cb_ctx));
  } else {
    const int64_t time_cost = ObTimeUtility::current_time() - begin_ts;
    flush_cb_cost_stat_.stat(time_cost);
    PALF_LOG(TRACE, "after_flush_log success", K(ret));
  }
  return ret;
}

// NB: execute 'inner_after_flush_meta' is serially.
int PalfHandleImpl::inner_after_flush_meta(const FlushMetaCbCtx &flush_meta_cb_ctx)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  PALF_LOG(TRACE, "inner_after_flush_meta", K(flush_meta_cb_ctx));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    switch(flush_meta_cb_ctx.type_) {
      case PREPARE_META:
        ret = after_flush_prepare_meta_(flush_meta_cb_ctx.proposal_id_);
        break;
      case CHANGE_CONFIG_META:
        ret = after_flush_config_change_meta_(flush_meta_cb_ctx.proposal_id_, flush_meta_cb_ctx.config_version_);
        break;
      case MODE_META:
        ret = after_flush_mode_meta_(flush_meta_cb_ctx.proposal_id_, flush_meta_cb_ctx.log_mode_meta_);
        break;
      case SNAPSHOT_META:
        ret = after_flush_snapshot_meta_(flush_meta_cb_ctx.base_lsn_);
        break;
      case REPLICA_PROPERTY_META:
        ret = after_flush_replica_property_meta_(flush_meta_cb_ctx.allow_vote_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
  return ret;
}

int PalfHandleImpl::inner_after_truncate_prefix_blocks(const TruncatePrefixBlocksCbCtx &truncate_prefix_cb_ctx)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (OB_FAIL(sw_.after_rebuild(truncate_prefix_cb_ctx.lsn_))) {
    PALF_LOG(WARN, "update_truncate_prefix_blocks_base_lsn failed", K(ret), K(truncate_prefix_cb_ctx));
  }
  return ret;
}

int PalfHandleImpl::after_flush_prepare_meta_(const int64_t &proposal_id)
{
  // caller holds lock
  int ret = OB_SUCCESS;
  const ObAddr &leader = state_mgr_.get_leader();
  if (proposal_id != state_mgr_.get_proposal_id()) {
    PALF_LOG(WARN, "proposal_id has changed during flushing", K(ret), K(proposal_id),
        "curr_proposal_id", state_mgr_.get_proposal_id());
  } else if (false == leader.is_valid() || self_ == leader) {
    // leader is self, no need submit response
    PALF_LOG(INFO, "do not need submit prepare response to self", K_(self), "leader:", leader);
  } else if (OB_FAIL(submit_prepare_response_(leader, proposal_id))) {
    PALF_LOG(WARN, "submit_prepare_response_ failed", K(ret), K(proposal_id), "leader", leader);
  } else {
    PALF_LOG(INFO, "after_flush_prepare_meta_ success", K(proposal_id));
    // do nothing
  }
  return ret;
}

int PalfHandleImpl::after_flush_config_change_meta_(const int64_t proposal_id, const LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;
  const ObAddr &leader = state_mgr_.get_leader();
  if (proposal_id != state_mgr_.get_proposal_id()) {
    PALF_LOG(WARN, "proposal_id has changed during flushing", K(ret), K_(palf_id), K_(self), K(proposal_id),
        "curr_proposal_id", state_mgr_.get_proposal_id());
  } else if (OB_FAIL(config_mgr_.after_flush_config_log(config_version))) {
    PALF_LOG(WARN, "LogConfigMgr after_flush_config_log failed", K(ret), KPC(this), K(proposal_id),
        K(config_version));
  } else if (self_ == leader) {
    if (OB_FAIL(config_mgr_.ack_config_log(self_, proposal_id, config_version))) {
      PALF_LOG(WARN, "ack_config_log failed", K(ret), KPC(this), K(config_version));
    }
  } else if (false == leader.is_valid()) {
    PALF_LOG(WARN, "leader is invalid", KPC(this), K(proposal_id), K(config_version));
  } else if (OB_FAIL(log_engine_.submit_change_config_meta_resp(leader, proposal_id, config_version))) {
    PALF_LOG(WARN, "submit_change_config_meta_resp failed", K(ret), KPC(this), K(proposal_id), K(config_version));
  } else {
    PALF_LOG(INFO, "submit_change_config_meta_resp success", K(ret), KPC(this), K(proposal_id), K(config_version));
  }
  return ret;
}

// 1. 更新snapshot_meta串行化, 实现inc update的语义.
// 2. 应用层先提交truncate任务, 再提交更新meta的任务, 最后开始拉日志.(不再依赖, 上层保证base_lsn不会回退)
//    NB: TODO by runlin, 在支持多writer后, truncate和和更新meta需要做成'类似'双向barrier的语义
int PalfHandleImpl::after_flush_snapshot_meta_(const LSN &lsn)
{
  return log_engine_.update_base_lsn_used_for_gc(lsn);
}

int PalfHandleImpl::after_flush_replica_property_meta_(const bool allow_vote)
{
  return (true == allow_vote)? state_mgr_.enable_vote(): state_mgr_.disable_vote();
}

int PalfHandleImpl::after_flush_mode_meta_(const int64_t proposal_id, const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  const ObAddr &leader = state_mgr_.get_leader();
  if (proposal_id != state_mgr_.get_proposal_id()) {
    PALF_LOG(WARN, "proposal_id has changed during flushing", K(ret), K(proposal_id),
        "curr_proposal_id", state_mgr_.get_proposal_id());
  } else if (OB_FAIL(mode_mgr_.after_flush_mode_meta(mode_meta))) {
    PALF_LOG(WARN, "after_flush_mode_meta failed", K(ret), K_(palf_id), K(proposal_id), K(mode_meta));
  } else if (self_ == leader) {
    if (OB_FAIL(mode_mgr_.ack_mode_meta(self_, mode_meta.proposal_id_))) {
      PALF_LOG(WARN, "ack_mode_meta failed", K(ret), K_(palf_id), K_(self), K(proposal_id));
    }
  } else if (false == leader.is_valid()) {
    PALF_LOG(WARN, "leader is invalid", KPC(this), K(proposal_id));
  } else if (OB_FAIL(log_engine_.submit_change_mode_meta_resp(leader, proposal_id))) {
    PALF_LOG(WARN, "submit_change_mode_meta_resp failed", K(ret), K_(self), K(leader), K(proposal_id));
  } else {
    PALF_LOG(INFO, "submit_change_mode_meta_resp success", K(ret), K_(self), K(leader), K(proposal_id));
  }
  return ret;
}

int PalfHandleImpl::inner_after_truncate_log(const TruncateLogCbCtx &truncate_log_cb_ctx)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (OB_FAIL(sw_.after_truncate(truncate_log_cb_ctx))) {
    PALF_LOG(WARN, "inner_after_truncate_log failed", K(ret), K(truncate_log_cb_ctx));
  } else {
    PALF_LOG(INFO, "after_truncate_log success", K(ret), K_(self), K_(palf_id));
  }
  return ret;
}

int PalfHandleImpl::get_prev_log_info_(const LSN &lsn,
                                       LogInfo &prev_log_info)
{
  int ret = OB_SUCCESS;
  // NB: when lsn.val_ is 0, need iterate prev block
  block_id_t lsn_block_id = lsn_2_block(lsn, PALF_BLOCK_SIZE);
  offset_t lsn_block_offset = lsn_2_offset(lsn, PALF_BLOCK_SIZE);
  LSN start_lsn;
  start_lsn.val_ =
    (0ul == lsn_block_offset ? (lsn_block_id-1) * PALF_BLOCK_SIZE : lsn_block_id * PALF_BLOCK_SIZE);
  PalfGroupBufferIterator iterator;
  const LogSnapshotMeta log_snapshot_meta = log_engine_.get_log_meta().get_log_snapshot_meta();
  const LSN base_lsn = log_snapshot_meta.base_lsn_;
  LogInfo log_info;
  auto get_file_end_lsn = [&]() { return lsn; };
  if (PALF_INITIAL_LSN_VAL == lsn.val_) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "there is no log before this LSN", K(ret), K(lsn));
    // NB: the prev log info record in LogSnapshotMeta is only valid when the 'block offset' of base lsn
    // is 0
  } else if (lsn == base_lsn && OB_SUCC(log_snapshot_meta.get_prev_log_info(log_info))) {
    prev_log_info = log_info;
  } else if (OB_FAIL(iterator.init(start_lsn, log_engine_.get_log_storage(), get_file_end_lsn))) {
    PALF_LOG(WARN, "LogGroupEntryIterator init failed", K(ret), K(start_lsn), K(lsn));
  } else {
    LSN curr_lsn;
    LSN prev_lsn;
    LogGroupEntry curr_entry;
    LogGroupEntryHeader prev_entry_header;
    while (OB_SUCC(ret) && OB_SUCC(iterator.next())) {
      if (OB_FAIL(iterator.get_entry(curr_entry, curr_lsn))) {
        PALF_LOG(ERROR, "PalfGroupBufferIterator get_entry failed", K(ret));
      } else {
        prev_entry_header = curr_entry.get_header();
        prev_lsn = curr_lsn;
      }
    }
    if (OB_ITER_END == ret) {
      if (false == prev_lsn.is_valid()) {
        ret = OB_ERR_OUT_OF_UPPER_BOUND;
        PALF_LOG(WARN, "read nothing from palf", K(ret), KPC(this), K(lsn));
      // defense code
      } else if (prev_lsn >= lsn) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "prev lsn must be smaller than lsn", K(ret), K(iterator), K(lsn), K(prev_lsn), K(prev_entry_header));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      prev_log_info.log_id_ = prev_entry_header.get_log_id();
      prev_log_info.log_ts_ = prev_entry_header.get_max_timestamp();
      prev_log_info.accum_checksum_ = prev_entry_header.get_accum_checksum();
      prev_log_info.log_proposal_id_ = prev_entry_header.get_log_proposal_id();
      prev_log_info.lsn_ = prev_lsn;
      PALF_LOG(INFO, "get_prev_log_info_ success", K(ret), K(lsn), K(prev_lsn), K(prev_entry_header),
          K(prev_log_info), K(iterator));
    }
  }
  return ret;
}

int PalfHandleImpl::submit_prepare_response_(const common::ObAddr &server,
                                             const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  const bool vote_granted = true;
  LSN unused_prev_lsn;
  LSN max_flushed_end_lsn;
  int64_t max_flushed_log_pid = INVALID_PROPOSAL_ID;
  if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id));
  } else if (OB_FAIL(sw_.get_max_flushed_log_info(unused_prev_lsn, max_flushed_end_lsn, max_flushed_log_pid))) {
    PALF_LOG(WARN, "get_max_flushed_log_info failed", K(ret), K_(palf_id));
  } else {
    const LogModeMeta accepted_mode_meta = mode_mgr_.get_accepted_mode_meta();
    // the last log info include proposal_id and lsn, the proposal_id should be the maxest
    // of redo lod and config log.
    // because config log and redo log are stored separately
    int64_t accept_proposal_id = config_mgr_.get_accept_proposal_id();
    if (INVALID_PROPOSAL_ID != max_flushed_log_pid) {
      accept_proposal_id = MAX(config_mgr_.get_accept_proposal_id(), max_flushed_log_pid);
    }
    if (OB_FAIL(log_engine_.submit_prepare_meta_resp(server, proposal_id, vote_granted, accept_proposal_id,
            max_flushed_end_lsn, accepted_mode_meta))) {
      PALF_LOG(WARN, "submit_prepare_response failed", K(ret), K_(palf_id));
    } else {
      PALF_LOG(INFO, "submit_prepare_response success", K(ret), K_(palf_id), K_(self), K(server),
          K(vote_granted), K(accept_proposal_id), K(max_flushed_end_lsn), K(accepted_mode_meta));
    }
  }
  return ret;
}

int PalfHandleImpl::construct_palf_base_info_(const LSN &max_committed_lsn,
                                              PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  LogInfo prev_log_info;
  if (false == max_committed_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(max_committed_lsn));
    // NB: for rebuild, there may be no valid block on disk, however, the 'prev_log_info' has been saved
    //     in LogMeta.
    //     for gc, there is at least two blocks on disk.
  } else if (PALF_INITIAL_LSN_VAL == max_committed_lsn.val_) {
    palf_base_info.generate_by_default();
    PALF_LOG(INFO, "there is no valid data on disk in restart, the log service will be initted by default", K(ret), K(palf_base_info));
  } else if (OB_FAIL(get_prev_log_info_(max_committed_lsn, prev_log_info))) {
    PALF_LOG(WARN, "get_prev_entry_header_before_ failed", K(ret), K(max_committed_lsn), K(prev_log_info));
  } else {
    palf_base_info.prev_log_info_ = prev_log_info;
    palf_base_info.curr_lsn_ = max_committed_lsn;
    PALF_LOG(INFO, "construct_palf_base_info_ success", K(ret), K(max_committed_lsn),
        K(palf_base_info), K(prev_log_info));
  }
  return ret;
}

int PalfHandleImpl::append_disk_log_to_sw_(const LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  PalfGroupBufferIterator iterator;
  // NB: we need append each log locate in [start_lsn, log_tail_);
  auto get_file_end_lsn = [&](){ return LSN(LOG_MAX_LSN_VAL); };
  // if there is no valid data on disk, the 'start_lsn' which meash max committed log offset
  // of this log stream may be invalid.
  if (false == start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(iterator.init(start_lsn, log_engine_.get_log_storage(), get_file_end_lsn))) {
    PALF_LOG(WARN, "PalfGroupBufferIterator init failed", K(ret), K(log_engine_), K(start_lsn));
  } else {
    LogGroupEntry group_entry;
    LSN lsn;
    while (OB_SUCC(ret) && OB_SUCC(iterator.next())) {
      if (OB_FAIL(iterator.get_entry(group_entry, lsn))) {
        PALF_LOG(ERROR, "get_entry failed", K(ret), K(group_entry), K(lsn));
      } else if (OB_FAIL(sw_.append_disk_log(lsn, group_entry))) {
        PALF_LOG(WARN, "append_disk_log failed", K(ret), K(lsn), K(group_entry));
      } else {
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "append_disk_log_to_sw_ success", K(ret), K(iterator), K(start_lsn));
    }
  }
  return ret;
}

int PalfHandleImpl::revoke_leader(const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not inited", K(ret), K_(palf_id));
  } else if (false == state_mgr_.can_revoke(proposal_id)) {
    ret = OB_NOT_MASTER;
    PALF_LOG(WARN, "revoke_leader failed, not master", K(ret), K_(palf_id), K(proposal_id));
  } else if (OB_FAIL(election_.revoke(RoleChangeReason::AskToRevoke))) {
    PALF_LOG(WARN, "PalfHandleImpl revoke leader failed", K(ret), K_(palf_id));
  } else {
    PALF_LOG(INFO, "PalfHandleImpl revoke leader success", K(ret), K_(palf_id));
  }
  return ret;
}

int PalfHandleImpl::stat(PalfStat &palf_stat)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    int64_t unused_mode_version;
    block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
    int64_t min_block_min_ts = -1;
    ObRole curr_role = INVALID_ROLE;
    ObReplicaState curr_state = INVALID_STATE;
    palf_stat.self_ = self_;
    palf_stat.palf_id_ = palf_id_;
    state_mgr_.get_role_and_state(curr_role, curr_state);
    palf_stat.role_ = (LEADER == curr_role && curr_state == ACTIVE)? LEADER: FOLLOWER;
    palf_stat.log_proposal_id_ = state_mgr_.get_proposal_id();
    (void)config_mgr_.get_config_version(palf_stat.config_version_);
    (void)mode_mgr_.get_access_mode(unused_mode_version, palf_stat.access_mode_);
    (void)config_mgr_.get_curr_member_list(palf_stat.paxos_member_list_);
    (void)config_mgr_.get_replica_num(palf_stat.paxos_replica_num_);
    palf_stat.allow_vote_ = state_mgr_.is_allow_vote();
    palf_stat.replica_type_ = state_mgr_.get_replica_type();
    palf_stat.base_lsn_ = log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_;
    (void)log_engine_.get_min_block_info(min_block_id, min_block_min_ts);
    palf_stat.begin_lsn_ = LSN(min_block_id * PALF_BLOCK_SIZE);
    palf_stat.begin_ts_ns_ = min_block_min_ts;
    palf_stat.end_lsn_ = get_end_lsn();
    palf_stat.end_ts_ns_ = get_end_ts_ns();
    palf_stat.max_lsn_ = get_max_lsn();
    palf_stat.max_ts_ns_ = get_max_ts_ns();
    PALF_LOG(TRACE, "PalfHandleImpl stat", K(palf_stat));
  }
  return ret;
}

int PalfHandleImpl::diagnose(PalfDiagnoseInfo &diagnose_info) const
{
  int ret = OB_SUCCESS;
  state_mgr_.get_role_and_state(diagnose_info.palf_role_, diagnose_info.palf_state_);
  diagnose_info.palf_proposal_id_ = state_mgr_.get_proposal_id();
  state_mgr_.get_election_role(diagnose_info.election_role_, diagnose_info.election_epoch_);
  return ret;
}

} // end namespace palf
} // end namespace oceanbase
