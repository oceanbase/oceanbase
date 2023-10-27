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
#include "lib/ob_lib_config.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"                   // PALF_LOG
#include "common/ob_member_list.h"                        // ObMemberList
#include "common/ob_role.h"                               // ObRole
#include "fetch_log_engine.h"
#include "log_engine.h"                                // LogEngine
#include "election/interface/election_priority.h"
#include "palf_iterator.h"                             // Iterator
#include "palf_env_impl.h"                             // IPalfEnvImpl::
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf::election;
namespace palf
{

PalfHandleImpl::PalfHandleImpl()
  : lock_(common::ObLatchIds::PALF_HANDLE_IMPL_LOCK),
    sw_(),
    config_mgr_(),
    mode_mgr_(),
    state_mgr_(),
    reconfirm_(),
    log_engine_(),
    election_msg_sender_(log_engine_.log_net_service_),
    election_(),
    hot_cache_(),
    fetch_log_engine_(NULL),
    allocator_(NULL),
    palf_id_(INVALID_PALF_ID),
    self_(),
    fs_cb_wrapper_(),
    role_change_cb_wrpper_(),
    rebuild_cb_wrapper_(),
    plugins_(),
    last_locate_scn_(),
    last_locate_block_(LOG_INVALID_BLOCK_ID),
    cannot_recv_log_warn_time_(OB_INVALID_TIMESTAMP),
    cannot_handle_committed_info_time_(OB_INVALID_TIMESTAMP),
    log_disk_full_warn_time_(OB_INVALID_TIMESTAMP),
    last_check_parent_child_time_us_(OB_INVALID_TIMESTAMP),
    wait_slide_print_time_us_(OB_INVALID_TIMESTAMP),
    append_size_stat_time_us_(OB_INVALID_TIMESTAMP),
    replace_member_print_time_us_(OB_INVALID_TIMESTAMP),
    config_change_print_time_us_(OB_INVALID_TIMESTAMP),
    last_rebuild_lsn_(),
    last_rebuild_meta_info_(),
    last_record_append_lsn_(PALF_INITIAL_LSN_VAL),
    has_set_deleted_(false),
    palf_env_impl_(NULL),
    append_cost_stat_("[PALF STAT WRITE LOG COST TIME]", PALF_STAT_PRINT_INTERVAL_US),
    flush_cb_cost_stat_("[PALF STAT FLUSH CB COST TIME]", PALF_STAT_PRINT_INTERVAL_US),
    last_accum_write_statistic_time_(OB_INVALID_TIMESTAMP),
    accum_write_log_size_(0),
    last_accum_fetch_statistic_time_(OB_INVALID_TIMESTAMP),
    accum_fetch_log_size_(0),
    replica_meta_lock_(),
    rebuilding_lock_(),
    config_change_lock_(),
    mode_change_lock_(),
    flashback_lock_(),
    last_dump_info_time_us_(OB_INVALID_TIMESTAMP),
    flashback_state_(LogFlashbackState::FLASHBACK_INIT),
    last_check_sync_time_us_(OB_INVALID_TIMESTAMP),
    last_renew_loc_time_us_(OB_INVALID_TIMESTAMP),
    last_print_in_sync_time_us_(OB_INVALID_TIMESTAMP),
    last_hook_fetch_log_time_us_(OB_INVALID_TIMESTAMP),
    chaning_config_warn_time_(OB_INVALID_TIMESTAMP),
    cached_is_in_sync_(false),
    has_higher_prio_config_change_(false),
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
                         const LogReplicaType replica_type,
                         FetchLogEngine *fetch_log_engine,
                         const char *log_dir,
                         ObILogAllocator *alloc_mgr,
                         ILogBlockPool *log_block_pool,
                         LogRpc *log_rpc,
                         LogIOWorker *log_io_worker,
                         IPalfEnvImpl *palf_env_impl,
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
             || INVALID_REPLICA == replica_type
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
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(palf_base_info), K(replica_type),
        K(access_mode), K(log_dir), K(alloc_mgr), K(log_block_pool), K(log_rpc),
        K(log_io_worker), K(palf_env_impl), K(self), K(election_timer), K(palf_epoch));
  } else if (OB_FAIL(log_meta.generate_by_palf_base_info(palf_base_info, access_mode, replica_type))) {
    PALF_LOG(WARN, "generate_by_palf_base_info failed", K(ret), K(palf_id), K(palf_base_info), K(access_mode), K(replica_type));
  } else if ((pret = snprintf(log_dir_, MAX_PATH_SIZE, "%s", log_dir)) && false) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "error unexpected", K(ret), K(palf_id));
  } else if (OB_FAIL(log_engine_.init(palf_id, log_dir, log_meta, alloc_mgr, log_block_pool, &hot_cache_, \
          log_rpc, log_io_worker, &plugins_, palf_epoch, PALF_BLOCK_SIZE, PALF_META_BLOCK_SIZE))) {
    PALF_LOG(WARN, "LogEngine init failed", K(ret), K(palf_id), K(log_dir), K(alloc_mgr),
        K(log_rpc), K(log_io_worker));
  } else if (OB_FAIL(do_init_mem_(palf_id, palf_base_info, log_meta, log_dir, self, fetch_log_engine,
          alloc_mgr, log_rpc, palf_env_impl, election_timer))) {
    PALF_LOG(WARN, "PalfHandleImpl do_init_mem_ failed", K(ret), K(palf_id));
  } else {
    PALF_REPORT_INFO_KV(K_(palf_id));
    append_cost_stat_.set_extra_info(EXTRA_INFOS);
    flush_cb_cost_stat_.set_extra_info(EXTRA_INFOS);
    last_accum_write_statistic_time_ = ObTimeUtility::current_time();
    last_accum_fetch_statistic_time_ = ObTimeUtility::current_time();
    PALF_EVENT("PalfHandleImpl init success", palf_id_, K(ret), K(self), K(access_mode), K(palf_base_info),
        K(replica_type), K(log_dir), K(log_meta), K(palf_epoch));
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
                         IPalfEnvImpl *palf_env_impl,
                         const common::ObAddr &self,
                         common::ObOccamTimer *election_timer,
                         const int64_t palf_epoch,
                         bool &is_integrity)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  LogGroupEntryHeader entry_header;
  LSN max_committed_end_lsn;
  LogSnapshotMeta snapshot_meta;
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
  } else if (OB_FAIL(log_engine_.load(palf_id, log_dir, alloc_mgr, log_block_pool, &hot_cache_, log_rpc,
        log_io_worker, &plugins_, entry_header, palf_epoch, is_integrity, PALF_BLOCK_SIZE, PALF_META_BLOCK_SIZE))) {
    PALF_LOG(WARN, "LogEngine load failed", K(ret), K(palf_id));
    // NB: when 'entry_header' is invalid, means that there is no data on disk, and set max_committed_end_lsn
    //     to 'base_lsn_', we will generate default PalfBaseInfo or get it from LogSnapshotMeta(rebuild).
  } else if (false == is_integrity) {
    PALF_LOG(INFO, "palf instance is not integrity", KPC(this));
  } else if (FALSE_IT(snapshot_meta = log_engine_.get_log_meta().get_log_snapshot_meta())) {
  } else if (FALSE_IT(max_committed_end_lsn =
         (true == entry_header.is_valid() ? entry_header.get_committed_end_lsn() : snapshot_meta.base_lsn_)))  {
  } else if (OB_FAIL(construct_palf_base_info_(max_committed_end_lsn, palf_base_info))) {
    PALF_LOG(WARN, "construct_palf_base_info_ failed", K(ret), K(palf_id), K(entry_header), K(palf_base_info));
  } else if (OB_FAIL(do_init_mem_(palf_id, palf_base_info, log_engine_.get_log_meta(), log_dir, self,
          fetch_log_engine, alloc_mgr, log_rpc, palf_env_impl, election_timer))) {
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
    diskspace_enough_ = true;
    cached_is_in_sync_ = false;
    plugins_.destroy();
    self_.reset();
    palf_id_ = INVALID_PALF_ID;
    fetch_log_engine_ = NULL;
    allocator_ = NULL;
    election_.stop();
    hot_cache_.destroy();
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
    last_accum_write_statistic_time_ = OB_INVALID_TIMESTAMP;
    accum_write_log_size_ = 0;
    last_accum_fetch_statistic_time_ = OB_INVALID_TIMESTAMP;
    accum_fetch_log_size_ = 0;
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
    const int64_t paxos_replica_num,
    const common::GlobalLearnerList &learner_list)
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
      if (OB_FAIL(config_mgr_.set_initial_member_list(member_list, paxos_replica_num, learner_list,
          proposal_id, config_version))) {
        PALF_LOG(WARN, "LogConfigMgr set_initial_member_list failed", K(ret), KPC(this));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(config_mgr_.wait_config_log_persistence(config_version))) {
      PALF_LOG(WARN, "want_config_log_persistence failed", K(ret), KPC(this));
    } else {
      PALF_EVENT("set_initial_member_list success", palf_id_, K(ret), K(member_list),
          K(learner_list), K(paxos_replica_num));
      report_set_initial_member_list_(paxos_replica_num, member_list);
    }
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int PalfHandleImpl::set_initial_member_list(
    const common::ObMemberList &member_list,
    const common::ObMember &arb_member,
    const int64_t paxos_replica_num,
    const common::GlobalLearnerList &learner_list)
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
      if (OB_FAIL(config_mgr_.set_initial_member_list(member_list, arb_member, paxos_replica_num, \
          learner_list, proposal_id, config_version))) {
        PALF_LOG(WARN, "LogConfigMgr set_initial_member_list failed", K(ret), KPC(this));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(config_mgr_.wait_config_log_persistence(config_version))) {
      PALF_LOG(WARN, "want_config_log_persistence failed", K(ret), KPC(this));
    } else {
      PALF_EVENT("set_initial_member_list success", palf_id_, K(ret), K(member_list), K(arb_member),
          K(learner_list), K(paxos_replica_num));
      report_set_initial_member_list_with_arb_(paxos_replica_num, member_list, arb_member);
    }
  }
  return ret;
}
#endif

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

int PalfHandleImpl::get_begin_scn(SCN &scn)
{
  int ret = OB_SUCCESS;
  block_id_t unused_block_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", K(ret), KPC(this));
  } else if (OB_FAIL(log_engine_.get_min_block_info(unused_block_id, scn))) {
    PALF_LOG(WARN, "LogEngine get_min_block_info failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::get_base_lsn(LSN &lsn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", K(ret), KPC(this));
  } else {
    lsn = get_base_lsn_used_for_block_gc();
  }
  return ret;
}

int PalfHandleImpl::get_base_info(const LSN &base_lsn, PalfBaseInfo &base_info)
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
    const SCN &ref_scn,
    LSN &lsn,
    SCN &scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl is not inited");
  } else if (NULL == buf || buf_len <= 0 || buf_len > MAX_LOG_BODY_SIZE
             || !ref_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K_(palf_id), KP(buf), K(buf_len), K(ref_scn));
  } else {
    RLockGuard guard(lock_);
    if (false == palf_env_impl_->check_disk_space_enough()) {
      ret = OB_LOG_OUTOF_DISK_SPACE;
      if (palf_reach_time_interval(1 * 1000 * 1000, log_disk_full_warn_time_)) {
        PALF_LOG(WARN, "log outof disk space", KPC(this), K(opts), K(ref_scn));
      }
    } else if (!state_mgr_.can_append(opts.proposal_id, opts.need_check_proposal_id)) {
      ret = OB_NOT_MASTER;
      PALF_LOG(WARN, "cannot submit_log", KPC(this), KP(buf), K(buf_len), "role",
          state_mgr_.get_role(), "state", state_mgr_.get_state(), "proposal_id",
          state_mgr_.get_proposal_id(), K(opts), "mode_mgr can_append", mode_mgr_.can_append());
    } else if (OB_FAIL(sw_.submit_log(buf, buf_len, ref_scn, lsn, scn))) {
      if (OB_EAGAIN != ret) {
        PALF_LOG(WARN, "submit_log failed", KPC(this), KP(buf), K(buf_len));
      }
    } else {
      PALF_LOG(TRACE, "submit_log success", K(ret), KPC(this), K(buf_len), K(lsn), K(scn));
      if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, append_size_stat_time_us_)) {
        PALF_LOG(INFO, "[PALF STAT APPEND DATA SIZE]", KPC(this), "append size", lsn.val_ - last_record_append_lsn_.val_);
        last_record_append_lsn_ = lsn;
      }
    }
  }
  return ret;
}

int PalfHandleImpl::get_palf_id(int64_t &palf_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret));
  } else {
    palf_id = palf_id_;
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
    plugins_.record_election_leader_change_event(palf_id_, dest_addr);
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
  } else if (OB_FAIL(config_mgr_.get_curr_member_list(member_list, paxos_replica_num))) {
    PALF_LOG(WARN, "get_curr_member_list failed", K(ret), KPC(this));
  } else {}
  return ret;
}

int PalfHandleImpl::get_config_version(LogConfigVersion &config_version) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret), K_(palf_id));
  } else {
    RLockGuard guard(lock_);
    if (OB_FAIL(config_mgr_.get_config_version(config_version))) {
      PALF_LOG(WARN, "failed to get_config_version", K(ret), K_(palf_id));
    }
  }
  return ret;
}

int PalfHandleImpl::get_paxos_member_list_and_learner_list(
    common::ObMemberList &member_list,
    int64_t &paxos_replica_num,
    GlobalLearnerList &learner_list) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret));
  } else if (OB_FAIL(config_mgr_.get_curr_member_list(member_list, paxos_replica_num))) {
    PALF_LOG(WARN, "get_curr_member_list failed", K(ret), KPC(this));
  } else if (OB_FAIL(config_mgr_.get_global_learner_list(learner_list))) {
    PALF_LOG(WARN, "get_global_learner_list failed", K(ret), KPC(this));
  } else {}
  return ret;
}

int PalfHandleImpl::get_election_leader(ObAddr &addr) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  ret = get_election_leader_without_lock_(addr);
  return ret;
}

int PalfHandleImpl::handle_config_change_pre_check(const ObAddr &server,
                                                   const LogGetMCStReq &req,
                                                   LogGetMCStResp &resp)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  int tmp_ret = common::OB_SUCCESS;
  const bool is_vote_enabled = state_mgr_.is_allow_vote();
  const bool is_sync_enabled = state_mgr_.is_sync_enabled();
  LSN last_rebuild_lsn;
  do {
    SpinLockGuard guard(last_rebuild_meta_info_lock_);
    last_rebuild_lsn = last_rebuild_lsn_;
  } while (0);
  const bool need_rebuild = is_need_rebuild(get_end_lsn(), last_rebuild_lsn);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret), K_(palf_id));
  } else if (OB_TMP_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_data_version))) {
    // Note: if the DATA_VERSION in the replica is empty, it is not allowed
    //       to be added in the Paxos group. Check PalfHandleImpl only
    resp.is_normal_replica_ = false;
    PALF_LOG(WARN, "get tenant data version failed", K(tmp_ret), K(req), K(resp));
  } else if (false == is_vote_enabled || false == is_sync_enabled || true == need_rebuild) {
    resp.is_normal_replica_ = false;
    PALF_LOG(WARN, "replica has been disabled vote/sync", K(ret), K(req), K(resp),
        K(is_vote_enabled), K(is_sync_enabled), K(need_rebuild));
  } else {
    RLockGuard guard(lock_);
    if (req.need_purge_throttling_) {
      int tmp_ret = OB_SUCCESS;
      const PurgeThrottlingType purge_type = PurgeThrottlingType::PURGE_BY_GET_MC_REQ;
      if (OB_SUCCESS != (tmp_ret = log_engine_.submit_purge_throttling_task(purge_type))) {
        PALF_LOG_RET(WARN, tmp_ret, "failed to submit_purge_throttling_task with handle_config_change_pre_check", K_(palf_id));
      }
    }
    resp.msg_proposal_id_ = state_mgr_.get_proposal_id();
    LogConfigVersion curr_config_version;
    sw_.get_max_flushed_end_lsn(resp.max_flushed_end_lsn_);
    resp.last_slide_log_id_ = sw_.get_last_slide_log_id();
    if (OB_FAIL(config_mgr_.get_config_version(curr_config_version))) {
    } else {
      resp.need_update_config_meta_ = (req.config_version_ != curr_config_version);
    }
    resp.is_normal_replica_ = true;

    // it's a optimization. To add one F members into 1F1A group,
    // leader will not accept appended logs until max_flushed_end_lsn of
    // added F member reaches log barrier. So we fetch log from leader right now
    // to reduce waiting time.
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = sw_.try_fetch_log(FetchTriggerType::ADD_MEMBER_PRE_CHECK))) {
      PALF_LOG(WARN, "try_fetch_log with ADD_MEMBER_PRE_CHECK failed",
          KR(tmp_ret), KPC(this), K(server));
    } else {
      PALF_LOG(INFO, "try_fetch_log with ADD_MEMBER_PRE_CHECK success", KR(tmp_ret), KPC(this));
    }
    PALF_LOG(INFO, "handle_config_change_pre_check success", K(ret), KPC(this), K(server),
        K(req), K(resp), K(curr_config_version));
  }
  return ret;
}

int PalfHandleImpl::force_set_as_single_replica()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else {
    const int64_t now_time_us = common::ObTimeUtility::current_time();
    const ObMember member(self_, now_time_us);
    const int64_t new_replica_num = 1;
    LogConfigChangeArgs args(member, new_replica_num, FORCE_SINGLE_MEMBER);
    const int64_t timeout_us = 10 * 1000 * 1000L;
    int64_t prev_replica_num;
    (void) config_mgr_.get_replica_num(prev_replica_num);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "one_stage_config_change_ failed", KR(ret), KPC(this), K(member), K(new_replica_num));
    } else {
      PALF_EVENT("force_set_as_single_replica success", palf_id_, KR(ret), KPC(this), K(member), K(new_replica_num));
      report_force_set_as_single_replica_(prev_replica_num, new_replica_num, member);
    }
  }
  return ret;
}

int PalfHandleImpl::change_replica_num(
    const common::ObMemberList &member_list,
    const int64_t curr_replica_num,
    const int64_t new_replica_num,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member_list.is_valid() ||
             false == is_valid_replica_num(curr_replica_num) ||
             false == is_valid_replica_num(new_replica_num) ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member_list), K(curr_replica_num),
        K(new_replica_num), K(timeout_us));
  } else {
    LogConfigChangeArgs args(member_list, curr_replica_num, new_replica_num, CHANGE_REPLICA_NUM);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "change_replica_num failed", KR(ret), KPC(this), K(member_list),
          K(curr_replica_num), K(new_replica_num));
    } else {
      PALF_EVENT("change_replica_num success", palf_id_, KR(ret), KPC(this), K(member_list),
          K(curr_replica_num), K(new_replica_num));
      report_change_replica_num_(curr_replica_num, new_replica_num, member_list);
    }
  }
  return ret;
}

int PalfHandleImpl::add_member(
    const common::ObMember &member,
    const int64_t new_replica_num,
    const LogConfigVersion &config_version,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t prev_replica_num;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member.is_valid() ||
             false == is_valid_replica_num(new_replica_num) ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(new_replica_num), K(timeout_us));
  } else if (OB_FAIL(config_mgr_.get_replica_num(prev_replica_num))) {
    PALF_LOG(WARN, "get prev_replica_num failed", KR(ret), KPC(this));
  } else {
    LogConfigChangeArgs args(member, new_replica_num, config_version, ADD_MEMBER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "add_member failed", KR(ret), KPC(this), K(member), K(new_replica_num));
    } else {
      PALF_EVENT("add_member success", palf_id_, KR(ret), KPC(this), K(member), K(new_replica_num));
      report_add_member_(prev_replica_num, new_replica_num, member);
    }
  }
  return ret;
}

int PalfHandleImpl::remove_member(
    const common::ObMember &member,
    const int64_t new_replica_num,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t prev_replica_num;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member.is_valid() ||
             false == is_valid_replica_num(new_replica_num) ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(new_replica_num), K(timeout_us));
  } else if (OB_FAIL(config_mgr_.get_replica_num(prev_replica_num))) {
    PALF_LOG(WARN, "get prev_replica_num failed", KR(ret), KPC(this));
  } else {
    LogConfigChangeArgs args(member, new_replica_num, REMOVE_MEMBER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "remove_member failed", KR(ret), KPC(this), K(member), K(new_replica_num));
    } else {
      PALF_EVENT("remove_member success", palf_id_, KR(ret), KPC(this), K(member), K(new_replica_num));
      report_remove_member_(prev_replica_num, new_replica_num, member);
    }
  }
  return ret;
}

int PalfHandleImpl::replace_member(
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const LogConfigVersion &config_version,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!added_member.is_valid() ||
             !removed_member.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_member), K(removed_member),
             K(timeout_us));
  } else {
    ObMemberList old_member_list, curr_member_list;
    int64_t old_replica_num = -1, curr_replica_num = -1;
    LogConfigChangeArgs args(added_member, 0, config_version, ADD_MEMBER_AND_NUM);
    const int64_t begin_time_us = common::ObTimeUtility::current_time();
    if (OB_FAIL(config_mgr_.get_curr_member_list(old_member_list, old_replica_num))) {
      PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), KPC(this));
    } else if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "add_member in replace_member failed", KR(ret), KPC(this), K(args));
    } else if (FALSE_IT(args.server_ = removed_member)) {
    } else if (FALSE_IT(args.type_ = REMOVE_MEMBER_AND_NUM)) {
    } else if (OB_FAIL(one_stage_config_change_(args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
      if (palf_reach_time_interval(100 * 1000, replace_member_print_time_us_)) {
        PALF_LOG(WARN, "remove_member in replace_member failed", KR(ret), K(args), KPC(this));
      }
    } else if (OB_FAIL(config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num))) {
      PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), KPC(this));
    } else {
      PALF_EVENT("replace_member success", palf_id_, KR(ret), KPC(this), K(added_member),
          K(removed_member), K(timeout_us), K(old_member_list), K(old_replica_num),
          K(curr_member_list), K(curr_replica_num),
          "leader replace_member cost time(us)", common::ObTimeUtility::current_time() - begin_time_us);
      report_replace_member_(added_member, removed_member, curr_member_list, "REPLACE_MEMBER");
    }
  }
  return ret;
}

int PalfHandleImpl::add_learner(const common::ObMember &added_learner, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!added_learner.is_valid() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    LogConfigChangeArgs args(added_learner, 0, ADD_LEARNER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "add_learner failed", KR(ret), KPC(this), K(args), K(timeout_us));
    } else {
      PALF_EVENT("add_learner success", palf_id_, K(ret), KPC(this), K(args), K(timeout_us));
      report_add_learner_(added_learner);
    }
  }
  return ret;
}

int PalfHandleImpl::remove_learner(const common::ObMember &removed_learner, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!removed_learner.is_valid() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    LogConfigChangeArgs args(removed_learner, 0, REMOVE_LEARNER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "remove_learner failed", KR(ret), KPC(this), K(args), K(timeout_us));
    } else {
      PALF_EVENT("remove_learner success", palf_id_, K(ret), KPC(this), K(args), K(timeout_us));
      report_remove_learner_(removed_learner);
    }
  }
  return ret;
}

int PalfHandleImpl::switch_learner_to_acceptor(const common::ObMember &learner,
                                               const int64_t new_replica_num,
                                               const LogConfigVersion &config_version,
                                               const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!learner.is_valid() ||
             !is_valid_replica_num(new_replica_num) ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KPC(this), K(learner), K(timeout_us));
  } else {
    LogConfigChangeArgs args(learner, new_replica_num, config_version, SWITCH_LEARNER_TO_ACCEPTOR);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "switch_learner_to_acceptor failed", KR(ret), KPC(this), K(args), K(timeout_us));
    } else {
      PALF_EVENT("switch_learner_to_acceptor success", palf_id_, K(ret), KPC(this), K(args), K(timeout_us));
      report_switch_learner_to_acceptor_(learner);
    }
  }
  return ret;
}

int PalfHandleImpl::switch_acceptor_to_learner(const common::ObMember &member,
                                               const int64_t new_replica_num,
                                               const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!member.is_valid() ||
             !is_valid_replica_num(new_replica_num) ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    LogConfigChangeArgs args(member, new_replica_num, SWITCH_ACCEPTOR_TO_LEARNER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "switch_acceptor_to_learner failed", KR(ret), KPC(this), K(args), K(timeout_us));
    } else {
      PALF_EVENT("switch_acceptor_to_learner success", palf_id_, K(ret), KPC(this), K(args), K(timeout_us));
      report_switch_acceptor_to_learner_(member);
    }
  }
  return ret;
}

int PalfHandleImpl::replace_learners(const common::ObMemberList &added_learners,
                                     const common::ObMemberList &removed_learners,
                                     const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!added_learners.is_valid() || !removed_learners.is_valid() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_learners), K(removed_learners), K(timeout_us));
  } else {
    LogConfigChangeArgs args(added_learners, removed_learners, REPLACE_LEARNERS);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "replace_learners failed", KR(ret), KPC(this), K(args), K(timeout_us));
    } else {
      PALF_EVENT("replace_learners success", palf_id_, K(ret), KPC(this), K(args), K(timeout_us));
      report_replace_learners_(added_learners, removed_learners);
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_REPLACE_MEMBER_NOT_REMOVE_ERROR);
int PalfHandleImpl::replace_member_with_learner(const common::ObMember &added_member,
                                                const common::ObMember &removed_member,
                                                const palf::LogConfigVersion &config_version,
                                                const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!added_member.is_valid() ||
             !removed_member.is_valid() ||
             false == config_version.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_member),
        K(removed_member), K(config_version), K(timeout_us));
  } else {
    ObMemberList old_member_list, curr_member_list;
    int64_t old_replica_num = -1, curr_replica_num = -1;
    LogConfigChangeArgs args(added_member, 0, config_version, SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM);
    const int64_t begin_time_us = common::ObTimeUtility::current_time();
    if (OB_FAIL(config_mgr_.get_curr_member_list(old_member_list, old_replica_num))) {
      PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), KPC(this));
    } else if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "add_member in replace_member_with_learner failed", KR(ret), KPC(this), K(args));
    } else if (OB_UNLIKELY(ERRSIM_REPLACE_MEMBER_NOT_REMOVE_ERROR)) {
      // do nothing
    } else if (FALSE_IT(args.server_ = removed_member)) {
    } else if (FALSE_IT(args.type_ = REMOVE_MEMBER_AND_NUM)) {
    } else if (OB_FAIL(one_stage_config_change_(args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
      if (palf_reach_time_interval(100 * 1000, replace_member_print_time_us_)) {
        PALF_LOG(WARN, "remove_member in replace_member_with_learner failed", KR(ret), K(args), KPC(this));
      }
    } else if (OB_FAIL(config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num))) {
      PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), KPC(this));
    } else {
      PALF_EVENT("replace_member_with_learner success", palf_id_, KR(ret), KPC(this), K(added_member),
          K(removed_member), K(config_version), K(timeout_us), K(old_member_list), K(old_replica_num),
          K(curr_member_list), K(curr_replica_num),
          "leader replace_member_with_learner cost time(us)", common::ObTimeUtility::current_time() - begin_time_us);
      report_replace_member_(added_member, removed_member, curr_member_list, "REPLACE_MEMBER_WITH_LEARNER");
    }
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int PalfHandleImpl::add_arb_member(
    const common::ObMember &member,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member.is_valid() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(timeout_us));
  } else {
    LogConfigChangeArgs args(member, 0, ADD_ARB_MEMBER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "add_arb_member failed", KR(ret), KPC(this), K(member));
    } else {
      PALF_EVENT("add_arb_member success", palf_id_, KR(ret), KPC(this), K(member));
      report_add_arb_member_(member);
    }
  }
  return ret;
}

int PalfHandleImpl::remove_arb_member(
    const common::ObMember &member,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (!member.is_valid() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(timeout_us));
  } else {
    LogConfigChangeArgs args(member, 0, REMOVE_ARB_MEMBER);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "remove_arb_member failed", KR(ret), KPC(this), K(member));
    } else {
      PALF_EVENT("remove_arb_member success", palf_id_, KR(ret), KPC(this), K(member));
      report_remove_arb_member_(member);
    }
  }
  return ret;
}

int PalfHandleImpl::degrade_acceptor_to_learner(const LogMemberAckInfoList &degrade_servers, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (0 == degrade_servers.count() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(degrade_servers), K(timeout_us));
  } else if (OB_FAIL(pre_check_before_degrade_upgrade_(degrade_servers, DEGRADE_ACCEPTOR_TO_LEARNER))) {
    PALF_LOG(WARN, "pre_check_before_degrade_upgrade_ failed", KR(ret), KPC(this), K(degrade_servers));
  } else {
    const int64_t begin_time_us = common::ObTimeUtility::current_time();
    for (int64_t i = 0; i < degrade_servers.count() && OB_SUCC(ret); i++) {
      const ObMember &member = degrade_servers.at(i).member_;
      LogConfigChangeArgs args(member, 0, DEGRADE_ACCEPTOR_TO_LEARNER);
      const int64_t curr_time_us = common::ObTimeUtility::current_time();
      if (OB_FAIL(one_stage_config_change_(args, timeout_us + begin_time_us - curr_time_us))) {
        PALF_LOG(WARN, "degrade_acceptor_to_learner failed", KR(ret), KPC(this), K(member), K(timeout_us));
      } else {
        PALF_EVENT("degrade_acceptor_to_learner success", palf_id_, K(ret), K_(self), K(member), K(timeout_us));
      }
    }
    PALF_EVENT("degrade_acceptor_to_learner finish", palf_id_, K(ret), KPC(this), K(degrade_servers), K(timeout_us));
  }
  return ret;
}

int PalfHandleImpl::upgrade_learner_to_acceptor(const LogMemberAckInfoList &upgrade_servers, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (0 == upgrade_servers.count() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(upgrade_servers), K(timeout_us));
  // } else if (OB_FAIL(pre_check_before_degrade_upgrade_(upgrade_servers, UPGRADE_LEARNER_TO_ACCEPTOR))) {
  //   PALF_LOG(WARN, "pre_check_before_degrade_upgrade_ failed", KR(ret), KPC(this), K(upgrade_servers));
  } else {
    const int64_t begin_time_us = common::ObTimeUtility::current_time();
    for (int64_t i = 0; i < upgrade_servers.count() && OB_SUCC(ret); i++) {
      const ObMember &member = upgrade_servers.at(i).member_;
      LogConfigChangeArgs args(member, 0, UPGRADE_LEARNER_TO_ACCEPTOR);
      const int64_t curr_time_us = common::ObTimeUtility::current_time();
      if (OB_FAIL(one_stage_config_change_(args, timeout_us + begin_time_us - curr_time_us))) {
        PALF_LOG(WARN, "upgrade_learner_to_acceptor failed", KR(ret), KPC(this), K(member), K(timeout_us));
      } else {
        PALF_LOG(INFO, "upgrade_learner_to_acceptor success", K(ret), KPC(this), K(member), K(timeout_us));
      }
    }
    PALF_EVENT("upgrade_learner_to_acceptor finish", palf_id_, K(ret), KPC(this), K(upgrade_servers), K(timeout_us));
  }
  return ret;
}

int PalfHandleImpl::get_remote_arb_member_info(ArbMemberInfo &arb_member_info)
{
  int ret = OB_SUCCESS;
  ObMember arb_member;
  (void) config_mgr_.get_arbitration_member(arb_member);
  LogGetArbMemberInfoResp resp;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == state_mgr_.is_leader_active()) {
    // If self is not in <LEADER, ACTIVE> state, return -4038.
    ret = OB_NOT_MASTER;
  } else if (!arb_member.is_valid()) {
    // arb_member is invalid, return -4109.
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(log_engine_.sync_get_arb_member_info(arb_member.get_server(), \
          PALF_SYNC_RPC_TIMEOUT_US, resp))) {
    PALF_LOG(WARN, "submit_get_arb_member_info_req failed", KR(ret), K_(palf_id), K_(self),
        K(arb_member), K(resp));
  } else {
    arb_member_info = resp.arb_member_info_;
  }
  return ret;
}

int PalfHandleImpl::get_arb_member_info(palf::ArbMemberInfo &arb_member_info) const
{
  int ret = OB_SUCCESS;
  return ret;
}

int PalfHandleImpl::get_arbitration_member(common::ObMember &arb_member) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(config_mgr_.get_arbitration_member(arb_member))) {
    PALF_LOG(WARN, "get_arbitration_member failed", K_(palf_id), K_(self), K(arb_member));
  }
  return ret;
}
#endif

int PalfHandleImpl::change_access_mode(const int64_t proposal_id,
                                       const int64_t mode_version,
                                       const AccessMode &access_mode,
                                       const SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  AccessMode prev_access_mode;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (INVALID_PROPOSAL_ID == proposal_id ||
             INVALID_PROPOSAL_ID == mode_version ||
             !ref_scn.is_valid() ||
             false == is_valid_access_mode(access_mode)) {
    // ref_scn is reasonable only when access_mode is APPEND
    // mode_version is the proposal_id of PALF when access_mode was applied
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K_(self),
        K(proposal_id), K(mode_version), K(access_mode), K(ref_scn));
  } else if (OB_FAIL(mode_change_lock_.trylock())) {
    // require lock to protect status from concurrent change_access_mode
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "another change_access_mode is running, try again", K(ret), K_(palf_id),
        K_(self), K(proposal_id),K(access_mode), K(ref_scn));
  } else if (OB_FAIL(config_change_lock_.trylock())) {
    // forbid to change access mode when reconfiguration is doing
    mode_change_lock_.unlock();
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "reconfiguration is running, try again", K(ret), K_(palf_id),
        K_(self), K(proposal_id), K(access_mode), K(ref_scn));
  } else if (OB_FAIL(mode_mgr_.get_access_mode(prev_access_mode))) {
    PALF_LOG(WARN, "get old change_access mode failed", K(ret), K_(palf_id),
        K_(self), K(proposal_id),K(access_mode), K(ref_scn));
  } else {
    PALF_EVENT("start change_access_mode", palf_id_, K(ret), KPC(this),
        K(proposal_id), K(access_mode), K(ref_scn), K_(sw));
    ret = OB_EAGAIN;
    bool first_run = true;
    common::ObTimeGuard time_guard("change_access_mode", 10 * 1000);
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
        } else if (OB_SUCC(mode_mgr_.change_access_mode(mode_version, access_mode, ref_scn))) {
          PALF_LOG(INFO, "change_access_mode success", K(ret), K_(palf_id), K_(self), K(proposal_id),
              K(mode_version), K(access_mode), K(ref_scn));
        } else if (OB_EAGAIN != ret) {
          PALF_LOG(WARN, "change_access_mode failed", K(ret), K_(palf_id), K_(self), K(proposal_id),
              K(mode_version), K(access_mode), K(ref_scn));
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
            K(proposal_id), K(access_mode), K(ref_scn), K(time_guard), K_(sw));
        int64_t curr_mode_version;
        mode_mgr_.get_mode_version(curr_mode_version);
        PALF_REPORT_INFO_KV(K(proposal_id), K(ref_scn));
        plugins_.record_access_mode_change_event(palf_id_, mode_version, curr_mode_version, prev_access_mode, access_mode, EXTRA_INFOS);
      }
      if (OB_EAGAIN == ret) {
        ob_usleep(1000);
      }
    }
    PALF_EVENT("change_access_mode finish", palf_id_, K(ret), KPC(this),
        K(proposal_id), K(access_mode), K(ref_scn), K(time_guard), K_(sw));
    config_change_lock_.unlock();
    mode_change_lock_.unlock();
  }
  return ret;
}

int PalfHandleImpl::pre_check_before_degrade_upgrade_(const LogMemberAckInfoList &servers,
                                                      const LogConfigChangeType &type)
{
  int ret = OB_SUCCESS;
  const bool is_degrade = (type == DEGRADE_ACCEPTOR_TO_LEARNER);
  if (OB_FAIL(sw_.pre_check_before_degrade_upgrade(servers, is_degrade))) {
    PALF_LOG(WARN, "pre_check_before_degrade_upgrade failed", KR(ret), KPC(this), K(servers), K(is_degrade));
  }
  return ret;
}

int PalfHandleImpl::check_args_and_generate_config_(const LogConfigChangeArgs &args,
                                                    const int64_t proposal_id,
                                                    const int64_t election_epoch,
                                                    bool &is_already_finished,
                                                    LogConfigInfoV2 &new_config_info) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (OB_FAIL(config_mgr_.check_args_and_generate_config(args, proposal_id,
      election_epoch, is_already_finished, new_config_info))) {
    if (palf_reach_time_interval(100 * 1000, config_change_print_time_us_)) {
      PALF_LOG(WARN, "check_args_and_generate_config failed", K(ret), KPC(this), K(args));
    }
  } else { }
  return ret;
}

int PalfHandleImpl::wait_log_barrier_(const LogConfigChangeArgs &args,
                                      const LogConfigInfoV2 &new_config_info,
                                      TimeoutChecker &not_timeout)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCC(not_timeout())) {
    bool need_wlock = (false == state_mgr_.is_changing_config_with_arb());
    bool need_rlock = !need_wlock;
    if (DEGRADE_ACCEPTOR_TO_LEARNER != args.type_ &&
        true == ATOMIC_LOAD(&has_higher_prio_config_change_)) {
      ret = OB_EAGAIN;
      PALF_LOG(WARN, "reconfiguration is interrupted, try again", K(ret),
          K_(palf_id), K_(self), K(args));
      break;
    }
    if (true == need_wlock) {
      WLockGuard guard(lock_);
      if (OB_FAIL(config_mgr_.renew_config_change_barrier())) {
        PALF_LOG(WARN, "renew_config_change_barrier failed", KR(ret), KPC(this), K(args));
      } else if (OB_FAIL(state_mgr_.set_changing_config_with_arb())) {
        PALF_LOG(WARN, "set_changing_config_with_arb failed", KR(ret), KPC(this), K(args));
      }
    } else if (true == need_rlock) {
      RLockGuard guard(lock_);
      if (OB_FAIL(config_mgr_.wait_log_barrier(args, new_config_info)) && OB_EAGAIN != ret) {
        PALF_LOG(WARN, "wait_log_barrier_ failed", KR(ret), KPC(this), K(args));
      } else if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        ob_usleep(10 * 1000);
      } else {
        break;
      }
    }
  }
  return ret;
}

int PalfHandleImpl::one_stage_config_change_(const LogConfigChangeArgs &args,
                                             const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool doing_degrade = false;
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  int64_t election_epoch = INVALID_PROPOSAL_ID;
  bool is_already_finished = false;
  int get_lock = OB_EAGAIN;
  LogConfigInfoV2 new_config_info;
  if (DEGRADE_ACCEPTOR_TO_LEARNER == args.type_) {
    // for concurrent DEGRADE
    if (ATOMIC_BCAS(&has_higher_prio_config_change_, false, true)) {
      get_lock = config_change_lock_.lock();
    }
  } else if (false == ATOMIC_LOAD(&has_higher_prio_config_change_)) {
    get_lock = config_change_lock_.trylock();
  }
  LogConfigVersion config_version;
  if (OB_SUCCESS != get_lock) {
    ret = OB_EAGAIN;
    if (palf_reach_time_interval(100 * 1000, config_change_print_time_us_)) {
      PALF_LOG(WARN, "another config_change is running, try again", KR(ret), KPC(this), K(args), K(timeout_us));
    }
  } else if (!args.is_valid() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(args));
    // a degrade operation will interrupt startworking log running in background
  } else if (OB_FAIL(config_mgr_.start_change_config(proposal_id, election_epoch, args.type_))) {
    PALF_LOG(WARN, "start_change_config failed", KR(ret), KPC(this), K(args));
  } else if (FALSE_IT(doing_degrade = (args.type_ == DEGRADE_ACCEPTOR_TO_LEARNER))) {
  } else if (OB_FAIL(check_args_and_generate_config_(args, proposal_id, election_epoch,
      is_already_finished, new_config_info))) {
    if (palf_reach_time_interval(100 * 1000, config_change_print_time_us_)) {
      PALF_LOG(WARN, "check_args_and_generate_config failed", KR(ret), KPC(this), K(args));
    }
  } else if (is_already_finished) {
    if (palf_reach_time_interval(100 * 1000, config_change_print_time_us_)) {
      PALF_LOG(INFO, "one_stage_config_change has already finished", K(ret), KPC(this), K(args));
    }
  } else {
    PALF_LOG(INFO, "one_stage_config_change start", KPC(this), K(proposal_id), K(args), K(timeout_us));
    TimeoutChecker not_timeout(timeout_us);
    ObTimeGuard time_guard("config_change");
    // step 1: pre sync config log when adding member
    // The reason for this is when a new member is created and is going to add Paxos Group.
    // Its member_list is empty, so it cann't vote for any leader candidate.
    // If we just add D to (A, B, C) without pre syncing config log, if leader A crashes after
    // adding D, and member_list of D if still empty, D cann't vote for any one, therefore no one
    // can be elected to be leader.
    if (is_add_member_list(args.type_)) {
      RLockGuard guard(lock_);
      (void) config_mgr_.pre_sync_config_log_and_mode_meta(args.server_, proposal_id);
    }
    // step 2: config change remote precheck
    // Eg. 1. remove C from (A, B, C), A is leader. If log of B is far behind majority, we should not remove C,
    // otherwise paxos group will become unavailable
    // Eg. 2. add D to (A, B, C), A is leader. If log of C and D is far behind majority (A, B), we should not add D,
    // otherwise paxos group will become unavailable
    while (is_may_change_replica_num(args.type_) && OB_SUCC(ret) && OB_SUCC(not_timeout())) {
      bool added_member_has_new_version = true;
      const int64_t curr_proposal_id = state_mgr_.get_proposal_id();
      if (DEGRADE_ACCEPTOR_TO_LEARNER != args.type_ && true == ATOMIC_LOAD(&has_higher_prio_config_change_)) {
        ret = OB_EAGAIN;
        PALF_LOG(WARN, "reconfiguration is interrupted, try again", K(ret), K_(palf_id), K_(self), K(args));
      } else if (proposal_id != curr_proposal_id) {
        ret = OB_NOT_MASTER;
        PALF_LOG(WARN, "leader has been switched, try to change config again", KR(ret), KPC(this),
            K(proposal_id), K(curr_proposal_id));
      } else if (OB_SUCC(config_mgr_.check_follower_sync_status(args, new_config_info,
          added_member_has_new_version))) {
      // check log synchronization progress of new memberlist majority synchronically
        break;
      } else if (OB_EAGAIN != ret) {
        PALF_LOG(WARN, "check_follower_sync_status_ fails", K(ret), K_(palf_id), K(new_config_info));
      } else if (is_upgrade_or_degrade(args.type_)) {
        ret = OB_EAGAIN;
        PALF_LOG(WARN, "degrade/upgrade eagain, arb_reason: check_follower_sync_status_ return false",
            K(ret), K_(palf_id), K_(self), K(args));
      } else {
        if (false == added_member_has_new_version) {
          RLockGuard guard(lock_);
          (void) config_mgr_.pre_sync_config_log_and_mode_meta(args.server_, proposal_id);
        }
        ret = OB_SUCCESS;
        ob_usleep(100 * 1000);
      }
    }
    time_guard.click("precheck");
    // step 3: waiting for log barrier if a arbitration member exists
    if (OB_SUCC(ret) && true == new_config_info.config_.arbitration_member_.is_valid()) {
      ret = wait_log_barrier_(args, new_config_info, not_timeout);
    }
    time_guard.click("wait_barrier");
    // step 4: motivate reconfiguration
    while (OB_SUCCESS == ret && OB_SUCC(not_timeout())) {
      bool need_wlock = false;
      bool need_rlock = false;
      bool can_be_interrupted = !config_version.is_valid();
      {
        RLockGuard guard(lock_);
        if (OB_FAIL(config_mgr_.is_state_changed(need_rlock, need_wlock))) {
          PALF_LOG(WARN, "is_state_changed failed", KR(ret), KPC(this), K(need_wlock), K(need_wlock));
        }
      }
      // higher priority request arrives and can be interrupted.
      // 1. the reason: 2F1A, reconfiguration requests will not change member list until new committed_end_lsn
      // reaches log barrier. But if one F crashes, leader will not exit until timeout reaches,
      // degrade operation may get stuck. So normal reconfiguration can be interrupted by degrade operation.
      // 2. the condition: if config_version is invalid, that means config meta hasn't been changed,
      // can be interrupted
      if (DEGRADE_ACCEPTOR_TO_LEARNER != args.type_ &&
          true == ATOMIC_LOAD(&has_higher_prio_config_change_) &&
          true == can_be_interrupted) {
        ret = OB_EAGAIN;
        PALF_LOG(WARN, "reconfiguration is interrupted, try again", K(ret), K_(palf_id), K_(self), K(args));
        break;
      }
      if (false == need_rlock && false == need_wlock) {
        const int64_t SLEEP_US = (state_mgr_.is_changing_config_with_arb())? 10 * 1000: 50 * 1000;
        ob_usleep(SLEEP_US);
      }
      if (true == need_wlock) {
        WLockGuard guard(lock_);
        if (OB_FAIL(config_mgr_.change_config(args, proposal_id, election_epoch, config_version)) && OB_EAGAIN != ret) {
          PALF_LOG(WARN, "change_config failed", KR(ret), KPC(this), K(args), K(proposal_id), K(election_epoch), K(config_version));
        } else if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          break;
        }
      } else if (true == need_rlock) {
        RLockGuard guard(lock_);
        if (OB_FAIL(config_mgr_.change_config(args, proposal_id, election_epoch, config_version)) && OB_EAGAIN != ret) {
          PALF_LOG(WARN, "change_config failed", KR(ret), KPC(this), K(args), K(proposal_id), K(election_epoch), K(config_version));
        } else if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          break;
        }
      }
    }
    time_guard.click("reconfigure");
    PALF_LOG(INFO, "one_stage_config_change finish", KR(ret), KPC(this), K(args), K(config_version),
        K(timeout_us), K(time_guard));
    ret = (OB_LOG_NOT_SYNC == ret)? OB_EAGAIN: ret;
    if (OB_FAIL(ret)) {
      if (config_version.is_valid()) {
        config_mgr_.after_config_change_timeout(config_version);
      } else {
        // encounter unexpected error, reset flag
        WLockGuard guard(lock_);
        state_mgr_.reset_changing_config_with_arb();
      }
    }
  }
  if (OB_SUCCESS == get_lock) {
    config_change_lock_.unlock();
    if (DEGRADE_ACCEPTOR_TO_LEARNER == args.type_) {
      ATOMIC_STORE(&has_higher_prio_config_change_, false);
    }
  }
  if (doing_degrade) {
    (void) config_mgr_.end_degrade();
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
  int64_t replica_num = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!child.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_leader_active && OB_FAIL(config_mgr_.get_curr_member_list(member_list, replica_num))) {
    PALF_LOG(WARN, "get_curr_member_list failed", KR(ret), K_(self), K_(palf_id), K(child));
  } else if (is_leader_active && member_list.contains(child.get_server())) {
    PALF_LOG(INFO, "receive register_req from acceptor, ignore", K_(self), K_(palf_id), K(child));
  } else if (!is_leader_active && !state_mgr_.is_leader_reconfirm() && to_leader) {
    // avoid learner sends registering req frequently when leader is reconfirming
    LogLearner parent;
    LogCandidateList candidate_list;
    parent.server_ = self_;
    parent.register_time_us_ = child.register_time_us_;
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
      plugins_.record_set_base_lsn_event(palf_id_, new_base_lsn);
    }
  }
  return ret;
}

bool PalfHandleImpl::is_sync_enabled() const
{
  bool bool_ret = false;
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
  bool is_sync_enabled = state_mgr_.is_sync_enabled();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_sync_enabled) {
    PALF_LOG(INFO, "sync has been enabled", KPC(this));
  } else if (OB_FAIL(state_mgr_.enable_sync())) {
    PALF_LOG(WARN, "enable_sync failed", K(ret), KPC(this));
  } else {
    PALF_EVENT("enable_sync success", palf_id_, K(ret), KPC(this));
    plugins_.record_enable_sync_event(palf_id_);
  }
  return ret;
}

int PalfHandleImpl::disable_sync()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  bool is_sync_enabled = state_mgr_.is_sync_enabled();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_sync_enabled) {
    PALF_LOG(INFO, "sync has been disabled", KPC(this));
  } else if (OB_FAIL(state_mgr_.disable_sync())) {
    PALF_LOG(WARN, "disable_sync failed", K(ret), KPC(this));
  } else {
    PALF_EVENT("disable_sync success", palf_id_, K(ret), KPC(this));
    plugins_.record_disable_sync_event(palf_id_);
  }
  return ret;
}

bool PalfHandleImpl::is_vote_enabled() const
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
  } else {
    bool_ret = state_mgr_.is_allow_vote();
  }
  return bool_ret;
}

/*brief:disable_vote(need_check_log_missing), this function is reenterable.
 * step 1: check voting status, if already disabled, just return
 * step 2: for need_check_log_missing situation, double check whether it is really necessary to rebuild
 * step 3: set voting flag as false when necessary
 */
int PalfHandleImpl::disable_vote(const bool need_check_log_missing)
{
  int ret = OB_SUCCESS;
  const PRIORITY_SEED_BIT new_election_inner_priority_seed = PRIORITY_SEED_BIT::SEED_IN_REBUILD_PHASE_BIT;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    //step 1: check vote status.
    bool vote_disabled = false;
    do {
      common::ObMemberList memberlist;
      int64_t replica_num = 0;
      AccessMode access_mode;
      RLockGuard guard(lock_);
      if (OB_FAIL(config_mgr_.get_curr_member_list(memberlist, replica_num))) {
        PALF_LOG(WARN, "get_curr_member_list failed", KPC(this));
      } else if (OB_FAIL(mode_mgr_.get_access_mode(access_mode))) {
        PALF_LOG(WARN, "get_access_mode failed", K(ret), KPC(this));
      } else if (state_mgr_.is_leader_active() &&
          (1 == replica_num || AccessMode::APPEND == access_mode)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "can not disable_vote in leader", KPC(this),
            K(memberlist), K(replica_num), K(access_mode));
      } else if (!state_mgr_.is_allow_vote()) {
        PALF_LOG(INFO, "vote has already been disabled", KPC(this));
        vote_disabled = true;
      }
    } while(0);

    if (OB_SUCC(ret) && !vote_disabled) {
      if (OB_FAIL(election_.add_inner_priority_seed_bit(new_election_inner_priority_seed)) && OB_ENTRY_EXIST != ret) {
        // Because this interface is idempotent, so we need ignore err code OB_ENTRY_EXIST.
        PALF_LOG(WARN, "election add_inner_priority_seed_bit for rebuild failed", KPC(this));
        // Update allow_vote flag
      } else if (OB_FAIL(set_allow_vote_flag_(false, need_check_log_missing))) {
        PALF_LOG(WARN, "set_allow_vote_flag failed", KPC(this));
        // rollback election priority when it encounters failure
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = election_.clear_inner_priority_seed_bit(new_election_inner_priority_seed))) {
          PALF_LOG(WARN, "election clear_inner_priority_seed_bit for rebuild failed", K(tmp_ret), KPC(this));
        }
      } else {
        PALF_EVENT("disable_vote success", palf_id_, KPC(this), K(need_check_log_missing));
        PALF_REPORT_INFO_KV(K(need_check_log_missing));
        plugins_.record_disable_vote_event(palf_id_);
      }
    }
  }
  return ret;
}

int PalfHandleImpl::enable_vote()
{
  int ret = OB_SUCCESS;
  const PRIORITY_SEED_BIT election_inner_priority_seed = PRIORITY_SEED_BIT::SEED_IN_REBUILD_PHASE_BIT;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    // Update allow_vote flag firstly
  } else if (OB_FAIL(set_allow_vote_flag_(true, false/*no need check log misingg*/))) {
    PALF_LOG(WARN, "set_allow_vote_flag failed", KPC(this));
  } else if (OB_FAIL(election_.clear_inner_priority_seed_bit(election_inner_priority_seed))
      && OB_ENTRY_NOT_EXIST != ret) {
    PALF_LOG(WARN, "election clear_inner_priority_seed_bit for rebuild failed", KPC(this));
    // rollback allow_vote flag when it encounters failure
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = set_allow_vote_flag_(false, false/*no need check log misingg*/))) {
      PALF_LOG(WARN, "rollback allow_vote flag failed", K(tmp_ret), KPC(this));
    }
  } else {
    PALF_EVENT("enable_vote success", palf_id_, KPC(this));
    plugins_.record_enable_vote_event(palf_id_);
  }
  return ret;
}

int PalfHandleImpl::set_allow_vote_flag_(const bool allow_vote,
                                         const bool need_check_log_missing)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(replica_meta_lock_);
  if (state_mgr_.is_arb_replica()) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "can not enable_vote/disable_vote in arb_member", K(ret), KPC(this));
  } else {
    WLockGuard guard(lock_);
    if (!allow_vote && need_check_log_missing) {
      //disable_vote and need check whether log is actually missing
      RebuildMetaInfo last_rebuild_meta_info;
      RebuildMetaInfo rebuild_meta_info;
      get_last_rebuild_meta_info_(last_rebuild_meta_info);
      if (last_rebuild_meta_info.is_valid()) {
        //check with local rebuild meta info
        (void)gen_rebuild_meta_info_(rebuild_meta_info);
        ret = (last_rebuild_meta_info == rebuild_meta_info) ? OB_SUCCESS : OB_OP_NOT_ALLOW;
        PALF_LOG(INFO, "double check whether need disable_vote", K(last_rebuild_meta_info),
                 K(rebuild_meta_info), KPC(this));
      } else {
        ret = OB_OP_NOT_ALLOW;
        PALF_LOG(INFO, "maybe restart during rebuild, just return OB_OP_NOT_ALLOW", KPC(this));
      }
    }
    if (OB_SUCC(ret)) {
      FlushMetaCbCtx flush_meta_cb_ctx;
      flush_meta_cb_ctx.type_ = REPLICA_PROPERTY_META;
      flush_meta_cb_ctx.allow_vote_ = allow_vote;
      LogReplicaPropertyMeta replica_property_meta = log_engine_.get_log_meta().get_log_replica_property_meta();
      replica_property_meta.allow_vote_ = allow_vote;
      if (OB_FAIL(log_engine_.submit_flush_replica_property_meta_task(flush_meta_cb_ctx, replica_property_meta))) {
        PALF_LOG(WARN, "submit_flush_replica_property_meta_task failed", K(ret), K(flush_meta_cb_ctx), K(replica_property_meta));
      } else {
        if (!allow_vote) {
          //for disble_vote, modify allow_vote in memory under protection of wlock
          state_mgr_.disable_vote_in_mem();
        }
      }
    }
  }
  // wait until replica_property_meta has been flushed
  if (OB_SUCC(ret)) {
    while(allow_vote != state_mgr_.is_allow_vote_persisted()) {
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
    // Note: can not rebuild while a truncate operation is doing, because group_buffer may be
    //       truncated by LogCallback again after it has been advanced by rebuild operation.
    if (false == sw_.is_allow_rebuild()) {
      ret = OB_EAGAIN;
      PALF_LOG(WARN, "can not advance_base_info for now, try again failed", K(ret), KPC(this), K(palf_base_info), K(is_rebuild));
    } else if (OB_FAIL(check_need_advance_base_info_(new_base_lsn, prev_log_info, is_rebuild))) {
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
  PALF_EVENT("advance_base_info finished", palf_id_, K(ret), KPC(this), K(palf_base_info), K(time_guard));
  plugins_.record_advance_base_info_event(palf_id_, palf_base_info);
  return ret;
}

int PalfHandleImpl::locate_by_scn_coarsely(const SCN &scn, LSN &result_lsn)
{
  int ret = OB_SUCCESS;
  block_id_t result_block_id = LOG_INVALID_BLOCK_ID;
  result_lsn.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(scn));
  } else if (OB_FAIL(get_block_id_by_scn_(scn, result_block_id))) {
    PALF_LOG(WARN, "get_block_id_by_scn_ failed", KR(ret), KPC(this), K(scn));
  } else {
  }
  // 2. convert block_id to lsn
  if (OB_SUCC(ret)) {
    result_lsn = LSN(result_block_id * PALF_BLOCK_SIZE);
    inc_update_last_locate_block_scn_(result_block_id, scn);
  }
  return ret;
}

int PalfHandleImpl::get_block_id_by_scn_(const SCN &scn, block_id_t &result_block_id)
{
  int ret = OB_SUCCESS;
  block_id_t mid_block_id = LOG_INVALID_BLOCK_ID, min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  int64_t mid_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_binary_search_range_(scn, min_block_id, max_block_id, result_block_id))) {
    PALF_LOG(WARN, "get_binary_search_range_ failed", KR(ret), KPC(this), K(scn));
  } else {
    // 1. get lower bound lsn (result_lsn) by binary search
    SCN mid_scn;
    while(OB_SUCC(ret) && min_block_id <= max_block_id) {
      mid_block_id = min_block_id + ((max_block_id - min_block_id) >> 1);
      if (OB_FAIL(log_engine_.get_block_min_scn(mid_block_id, mid_scn))) {
        PALF_LOG(WARN, "get_block_min_scn failed", KR(ret), KPC(this), K(mid_block_id));
        // OB_ERR_OUT_OF_UPPER_BOUND: this block is a empty active block, just return
        // OB_ERR_OUT_OF_LOWER_BOUND: block_id_ is smaller than min_block_id, this block may be recycled
        // OB_ERR_UNEXPECTED: log files lost unexpectedly, just return
        // OB_IO_ERROR: just return
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          // block mid_lsn.block_id_ may be recycled, get_binary_search_range_ again
          if (OB_FAIL(get_binary_search_range_(scn, min_block_id, max_block_id, result_block_id))) {
            PALF_LOG(WARN, "get_binary_search_range_ failed", KR(ret), KPC(this), K(scn));
          }
        } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else if (mid_scn < scn) {
        min_block_id = mid_block_id;
        if (max_block_id == min_block_id) {
          result_block_id = mid_block_id;
          break;
        } else if (max_block_id == min_block_id + 1) {
          SCN next_min_scn;
          if (OB_FAIL(log_engine_.get_block_min_scn(max_block_id, next_min_scn))) {
            // if fail to read next block, just return prev block lsn
            ret = OB_SUCCESS;
            result_block_id = mid_block_id;
          } else if (scn < next_min_scn) {
            result_block_id = mid_block_id;
          } else {
            result_block_id = max_block_id;
          }
          break;
        }
      } else if (mid_scn > scn) {
        // block_id is uint64_t, so check == 0 firstly.
        if (mid_block_id == 0 || mid_block_id - 1 < min_block_id) {
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
          PALF_LOG(WARN, "scn is smaller than min scn of first block", KR(ret), KPC(this), K(min_block_id),
                          K(max_block_id), K(mid_block_id), K(scn), K(mid_scn));
        } else {
          max_block_id = mid_block_id - 1;
        }
      } else {
        result_block_id = mid_block_id;
        break;
      }
    }
  }
  return ret;
}

// @return value
//   OB_SUCCESS
//   OB_ENTRY_NOT_EXIST, when there is no log on disk, return OB_ENTRY_NOT_EXIST
//   others, unexpected error.
int PalfHandleImpl::get_block_id_by_scn_for_flashback_(const SCN &scn, block_id_t &result_block_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_block_id_by_scn_(scn, result_block_id)) && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    PALF_LOG(ERROR, "get_block_id_by_scn_ failed", K(ret), KPC(this), K(scn));
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    block_id_t min_block_id;
    share::SCN min_scn;
    if (OB_FAIL(log_engine_.get_min_block_info(min_block_id, min_scn))
        && OB_ENTRY_NOT_EXIST != ret) {
      PALF_LOG(ERROR, "get_min_block_info failed", K(ret), KPC(this), K(scn));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      PALF_LOG(WARN, "there is no block on disk, set result block id to base block id",
          K(ret), KPC(this), K(scn), K(result_block_id));
    } else {
      ret = OB_SUCCESS;
      result_block_id = min_block_id;
      PALF_LOG(WARN, "scn is smaller than min scn of palf, set result block id to base block id",
          K(ret),  KPC(this), K(scn), K(result_block_id), K(min_block_id), K(min_scn));
    }
  } else {
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
int PalfHandleImpl::get_binary_search_range_(const SCN &scn,
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
    // optimization: cache last_locate_scn_ to shrink binary search range
    SpinLockGuard guard(last_locate_lock_);
   if (is_valid_block_id(last_locate_block_) &&
        min_block_id <= last_locate_block_ &&
        max_block_id >= last_locate_block_) {
      if (scn < last_locate_scn_) {
        max_block_id = last_locate_block_;
      } else if (scn > last_locate_scn_) {
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
        K(result_block_id), K(committed_lsn), K(scn), K_(last_locate_scn), K_(last_locate_block));
  }
  return ret;
}

void PalfHandleImpl::inc_update_last_locate_block_scn_(const block_id_t &block_id, const SCN &scn)
{
  SpinLockGuard guard(last_locate_lock_);
  if (block_id > last_locate_block_) {
    last_locate_block_ = block_id;
    last_locate_scn_ = scn;
  }
}

int PalfHandleImpl::locate_by_lsn_coarsely(const LSN &lsn, SCN &result_scn)
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
    if (OB_FAIL(log_engine_.get_block_min_scn(curr_block_id, result_scn))) {
      // if this block is a empty active block, read prev block if exists
      if (OB_ERR_OUT_OF_UPPER_BOUND == ret &&
          curr_block_id > 0 &&
          OB_FAIL(log_engine_.get_block_min_scn(curr_block_id - 1, result_scn))) {
        PALF_LOG(WARN, "get_block_min_scn failed", KR(ret), KPC(this), K(curr_lsn), K(lsn));
      }
    }
    PALF_LOG(INFO, "locate_by_lsn_coarsely", KR(ret), KPC(this), K(lsn), K(committed_lsn), K(result_scn));
  }
  return ret;
}

int PalfHandleImpl::get_min_block_info_for_gc(block_id_t &min_block_id, SCN &max_scn)
{
  int ret = OB_SUCCESS;
//  if (false == end_lsn.is_valid()) {
//    ret = OB_ENTRY_NOT_EXIST;
//  }
  if (OB_FAIL(log_engine_.get_min_block_info_for_gc(min_block_id, max_scn))) {
    PALF_LOG(WARN, "get_min_block_info_for_gc failed", K(ret), KPC(this));
  } else {
    PALF_LOG(TRACE, "get_min_block_info_for_gc success", K(ret), KPC(this), K(min_block_id), K(max_scn));
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
                                     const SCN &scn)
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
  } else if (OB_FAIL(log_engine_.append_log(lsn, write_buf, scn))) {
    PALF_LOG(ERROR, "LogEngine pwrite failed", K(ret), KPC(this), K(lsn), K(scn));
  } else {
    const int64_t curr_size = write_buf.get_total_size();
    const int64_t accum_size = ATOMIC_AAF(&accum_write_log_size_, curr_size);
    const int64_t now = ObTimeUtility::current_time();
    const int64_t time_cost = now - begin_ts;
    append_cost_stat_.stat(time_cost);
    if (time_cost >= 5 * 1000) {
      PALF_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "write log cost too much time", K(ret), KPC(this),
                   K(lsn), K(scn), "size", write_buf.get_total_size(), K(accum_size), K(time_cost));
    }
    if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, last_accum_write_statistic_time_)) {
      PALF_LOG(INFO, "[PALF STAT INNER APPEND LOG SIZE]", KPC(this), K(accum_size));
      ATOMIC_STORE(&accum_write_log_size_, 0);
    }
  }
  return ret;
}

int PalfHandleImpl::inner_append_log(const LSNArray &lsn_array,
                                     const LogWriteBufArray &write_buf_array,
                                     const SCNArray &scn_array)
{
  int ret = OB_SUCCESS;
  const int64_t begin_ts = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl not inited", K(ret), KPC(this));
  } else if (OB_FAIL(log_engine_.append_log(lsn_array, write_buf_array, scn_array))) {
    PALF_LOG(ERROR, "LogEngine pwrite failed", K(ret), KPC(this), K(lsn_array), K(scn_array));
  } else {
    int64_t count = lsn_array.count();
    int64_t accum_size = 0, curr_size = 0;
    for (int64_t i = 0; i < count; i++) {
      curr_size += write_buf_array[i]->get_total_size();
    }
    accum_size = ATOMIC_AAF(&accum_write_log_size_, curr_size);
    const int64_t now = ObTimeUtility::current_time();
    const int64_t time_cost = now - begin_ts;
    append_cost_stat_.stat(time_cost);
    if (time_cost > 10 * 1000) {
      PALF_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "write log cost too much time", K(ret), KPC(this), K(lsn_array),
               K(scn_array), K(curr_size), K(accum_size), K(time_cost));
    }
    if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, last_accum_write_statistic_time_)) {
      PALF_LOG(INFO, "[PALF STAT INNER APPEND LOG SIZE]", KPC(this), K(accum_size));
      ATOMIC_STORE(&accum_write_log_size_, 0);
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
    PALF_LOG(WARN, "LogEngine truncate_prefix_blocks failed", K(ret), KPC(this), K(lsn));
  } else {
    PALF_LOG(INFO, "LogEngine truncate_prefix_blocks success", K(ret), KPC(this), K(lsn));
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

int PalfHandleImpl::get_access_mode_version(int64_t &mode_version) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(mode_mgr_.get_mode_version(mode_version))) {
    PALF_LOG(WARN, "get_mode_version failed", K(ret), KPC(this));
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

int PalfHandleImpl::get_access_mode_ref_scn(int64_t &mode_version,
                                            AccessMode &access_mode,
                                            SCN &ref_scn) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(mode_mgr_.get_access_mode_ref_scn(mode_version, access_mode, ref_scn))) {
    PALF_LOG(WARN, "get_access_mode_ref_scn failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleImpl::alloc_palf_buffer_iterator(const LSN &offset,
                                               PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  auto get_file_end_lsn = [this]() {
    LSN max_flushed_end_lsn;
    (void)sw_.get_max_flushed_end_lsn(max_flushed_end_lsn);
    LSN committed_end_lsn;
    sw_.get_committed_end_lsn(committed_end_lsn);
    return MIN(committed_end_lsn, max_flushed_end_lsn);
  };
  auto get_mode_version = [this]() -> int64_t {
    int64_t mode_version = INVALID_PROPOSAL_ID;
    int ret = OB_SUCCESS;
    if (OB_FAIL(this->get_access_mode_version(mode_version))) {
      PALF_LOG(WARN, "get_access_mode_version failed", K(ret), KPC(this));
      mode_version = INVALID_PROPOSAL_ID;
    }
    return mode_version;
  };
  if (OB_FAIL(iterator.init(offset, get_file_end_lsn, get_mode_version, log_engine_.get_log_storage()))) {
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
  auto get_mode_version = [this]() -> int64_t {
    int64_t mode_version = INVALID_PROPOSAL_ID;
    int ret = OB_SUCCESS;
    if (OB_FAIL(this->get_access_mode_version(mode_version))) {
      PALF_LOG(WARN, "get_access_mode_version failed", K(ret), KPC(this));
      mode_version = INVALID_PROPOSAL_ID;
    }
    return mode_version;
  };
  if (OB_FAIL(iterator.init(offset, get_file_end_lsn, get_mode_version, log_engine_.get_log_storage()))) {
    PALF_LOG(ERROR, "PalfGroupBufferIterator init failed", K(ret), KPC(this));
  } else {
  }
  return ret;
}

int PalfHandleImpl::alloc_palf_group_buffer_iterator(const SCN &scn,
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
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K_(palf_id), K(scn));
  } else if (OB_FAIL(locate_by_scn_coarsely(scn, start_lsn)) &&
             OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    PALF_LOG(WARN, "locate_by_scn_coarsely failed", KR(ret), KPC(this), K(scn));
  } else if (OB_SUCCESS != ret &&
            !FALSE_IT(start_lsn = log_engine_.get_begin_lsn()) &&
            start_lsn.val_ != PALF_INITIAL_LSN_VAL) {
    PALF_LOG(WARN, "log may have been recycled", KR(ret), KPC(this), K(scn), K(start_lsn));
  } else if (OB_FAIL(local_iter.init(start_lsn, get_file_end_lsn, log_engine_.get_log_storage()))) {
    PALF_LOG(WARN, "PalfGroupBufferIterator init failed", KR(ret), KPC(this), K(start_lsn));
  } else {
    LogGroupEntry curr_group_entry;
    LSN curr_lsn, result_lsn;
    while (OB_SUCC(ret) && OB_SUCC(local_iter.next())) {
      if (OB_FAIL(local_iter.get_entry(curr_group_entry, curr_lsn))) {
        PALF_LOG(ERROR, "PalfGroupBufferIterator get_entry failed", KR(ret), KPC(this),
            K(curr_group_entry), K(curr_lsn), K(local_iter));
      } else if (curr_group_entry.get_scn() >= scn) {
        result_lsn = curr_lsn;
        break;
      } else {
        continue;
      }
    }
    if (OB_SUCC(ret) &&
        result_lsn.is_valid() &&
        OB_FAIL(iterator.init(result_lsn, get_file_end_lsn, log_engine_.get_log_storage()))) {
      PALF_LOG(WARN, "PalfGroupBufferIterator init failed", KR(ret), KPC(this), K(result_lsn));
    } else {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      PALF_LOG(WARN, "locate_by_scn failed", KR(ret), KPC(this), K(scn), K(result_lsn));
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
  } else if (OB_FAIL(plugins_.add_plugin(lc_cb))) {
    PALF_LOG(WARN, "add_plugin failed", KR(ret), KPC(this), KP(lc_cb), K_(plugins));
  } else {
    PALF_LOG(INFO, "set_location_cache_cb success", KPC(this), K_(plugins), KP(lc_cb));
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
  PalfLocationCacheCb *loc_cb = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(plugins_.del_plugin(loc_cb))) {
    PALF_LOG(WARN, "del_plugin failed", KR(ret), KPC(this), K_(plugins));
  }
  return ret;
}

int PalfHandleImpl::set_monitor_cb(PalfMonitorCb *monitor_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "not initted", KR(ret), KPC(this));
  } else if (OB_ISNULL(monitor_cb)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "lc_cb is NULL, can't register", KR(ret), KPC(this));
  } else if (OB_FAIL(plugins_.add_plugin(monitor_cb))) {
    PALF_LOG(WARN, "add_plugin failed", KR(ret), KPC(this), KP(monitor_cb), K_(plugins));
  } else {
    PALF_LOG(INFO, "set_monitor_cb success", KPC(this), K_(plugins), KP(monitor_cb));
  }
  return ret;
}

int PalfHandleImpl::reset_monitor_cb()
{
  int ret = OB_SUCCESS;
  PalfMonitorCb *monitor_cb = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(plugins_.del_plugin(monitor_cb))) {
    PALF_LOG(WARN, "del_plugin failed", KR(ret), KPC(this), K_(plugins));
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

bool PalfHandleImpl::is_in_period_freeze_mode() const
{
  return sw_.is_in_period_freeze_mode();
}

int PalfHandleImpl::period_freeze_last_log()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(true == state_mgr_.is_arb_replica())) {
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
    bool config_state_changed = false;
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
      if (OB_FAIL(config_mgr_.leader_do_loop_work(config_state_changed))) {
        PALF_LOG(WARN, "LogConfigMgr::leader_do_loop_work failed", KR(ret), K_(self), K_(palf_id));
      } else if (OB_FAIL(mode_mgr_.leader_do_loop_work())) {
        PALF_LOG(WARN, "LogModeMgr::leader_do_loop_work failed", KR(ret), K_(self), K_(palf_id));
      }
    } while (0);
    if (OB_UNLIKELY(config_state_changed)) {
      WLockGuard guard(lock_);
      if (OB_FAIL(config_mgr_.switch_state())) {
        PALF_LOG(WARN, "switch_state failed", K(ret));
      }
    }
    if (palf_reach_time_interval(PALF_CHECK_PARENT_CHILD_INTERVAL_US, last_check_parent_child_time_us_)) {
      RLockGuard guard(lock_);
      if (state_mgr_.is_follower_active()) {
        (void) config_mgr_.check_parent_health();
      }
      (void) config_mgr_.check_children_health();
    }
    if (palf_reach_time_interval(PALF_DUMP_DEBUG_INFO_INTERVAL_US, last_dump_info_time_us_)) {
      RLockGuard guard(lock_);
      FLOG_INFO("[PALF_DUMP]", K_(palf_id), K_(self), "[SlidingWindow]", sw_, "[StateMgr]", state_mgr_,
          "[ConfigMgr]", config_mgr_, "[ModeMgr]", mode_mgr_, "[LogEngine]", log_engine_, "[Reconfirm]",
          reconfirm_);
      if (false == state_mgr_.is_arb_replica()) {
        LogMemberAckInfoList ack_info_list;
        sw_.get_ack_info_array(ack_info_list);
        FLOG_INFO("[PALF_DUMP]", K_(palf_id), K_(self), K(ack_info_list));
      }
      (void) sw_.report_log_task_trace(sw_.get_start_id());
    }
  }
  return OB_SUCCESS;
}

int PalfHandleImpl::handle_prepare_request(const common::ObAddr &server,
                                           const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  bool can_handle_prepare_request = false;
  bool can_handle_leader_broadcast = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(server), K(proposal_id));
  } else {
    RLockGuard guard(lock_);
    if (state_mgr_.can_handle_prepare_request(proposal_id)) {
      can_handle_prepare_request = true;
    } else if (state_mgr_.can_handle_leader_broadcast(server, proposal_id)) {
      can_handle_leader_broadcast = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (can_handle_prepare_request) {
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
  } else if (can_handle_leader_broadcast) {
    WLockGuard guard(lock_);
    if (!state_mgr_.can_handle_leader_broadcast(server, proposal_id)) {
      // can not handle leader broadcast
    } else if (OB_FAIL(state_mgr_.handle_leader_broadcast(server, proposal_id))) {
      PALF_LOG(WARN, "handle_leader_broadcast failed", K(ret), KPC(this), K(server), K(proposal_id));
    } else {
      PALF_LOG(TRACE, "handle_leader_broadcast success", K(ret), KPC(this), K(server), K(proposal_id));
    }
  }
  PALF_LOG(TRACE, "handle_prepare_request", K(ret), KPC(this), K(server),
      K(can_handle_prepare_request), K(can_handle_leader_broadcast));
  return ret;
}

int PalfHandleImpl::handle_prepare_response(const common::ObAddr &server,
                                            const int64_t &proposal_id,
                                            const bool vote_granted,
                                            const int64_t &accept_proposal_id,
                                            const LSN &last_lsn,
                                            const LSN &committed_end_lsn,
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
              last_lsn, committed_end_lsn))) {
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
                                      const bool is_applied_mode_meta,
                                      const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  int lock_ret = OB_EAGAIN;
  bool has_accepted = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == server.is_valid() ||
      INVALID_PROPOSAL_ID == proposal_id ||
      false == mode_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), KPC(this), K(server), K(proposal_id), K(mode_meta));
  } else if (OB_FAIL(try_update_proposal_id_(server, proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", KR(ret), KPC(this), K(server), K(proposal_id));
  } else if (OB_SUCCESS != (lock_ret = lock_.wrlock())) {
  } else if (false == mode_mgr_.can_receive_mode_meta(proposal_id, mode_meta, has_accepted)) {
    PALF_LOG(WARN, "can_receive_mode_meta failed", KR(ret), KPC(this), K(proposal_id), K(mode_meta));
  } else if (true == has_accepted) {
    if (OB_FAIL(log_engine_.submit_change_mode_meta_resp(server, proposal_id))) {
      PALF_LOG(WARN, "submit_change_mode_meta_resp failed", KR(ret), KPC(this), K(proposal_id), K(mode_meta));
    }
    if (true == is_applied_mode_meta) {
      // update LogModeMgr::applied_mode_meta requires wlock
      (void) mode_mgr_.after_flush_mode_meta(is_applied_mode_meta, mode_meta);
    }
  } else if (OB_FAIL(mode_mgr_.receive_mode_meta(server, proposal_id, is_applied_mode_meta, mode_meta))) {
    PALF_LOG(WARN, "receive_mode_meta failed", KR(ret), KPC(this), K(server), K(proposal_id),
        K(mode_meta));
  } else { }
  PALF_LOG(INFO, "receive_mode_meta finish", KR(ret), KPC(this), K(server), K(proposal_id),
      K(is_applied_mode_meta), K(mode_meta));
  if (OB_SUCCESS == lock_ret) {
    lock_.wrunlock();
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
    PALF_LOG(TRACE, "ack_mode_meta success", KR(ret), KPC(this), K(server), K(msg_proposal_id));
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
    IPalfEnvImpl *palf_env_impl,
    common::ObOccamTimer *election_timer)
{
  int ret = OB_SUCCESS;
  int pret = -1;
  const bool is_normal_replica = (log_meta.get_log_replica_property_meta().replica_type_ == NORMAL_REPLICA);
  // inner priority seed: smaller means higher priority
  // reserve some bits for future requirements
  uint64_t election_inner_priority_seed = is_normal_replica ?
                                          static_cast<uint64_t>(PRIORITY_SEED_BIT::DEFAULT_SEED) :
                                          0ULL | static_cast<uint64_t>(PRIORITY_SEED_BIT::SEED_NOT_NORMOL_REPLICA_BIT);
  const bool allow_vote = log_meta.get_log_replica_property_meta().allow_vote_;
  if (false == allow_vote) {
    election_inner_priority_seed |= static_cast<uint64_t>(PRIORITY_SEED_BIT::SEED_IN_REBUILD_PHASE_BIT);
  }
  palf::PalfRoleChangeCbWrapper &role_change_cb_wrpper = role_change_cb_wrpper_;
  if ((pret = snprintf(log_dir_, MAX_PATH_SIZE, "%s", log_dir)) && false) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "error unexpected", K(ret), K(palf_id));
  } else if (OB_FAIL(sw_.init(palf_id, self, &state_mgr_, &config_mgr_, &mode_mgr_,
          &log_engine_, &fs_cb_wrapper_, alloc_mgr, &plugins_, palf_base_info, is_normal_replica))) {
    PALF_LOG(WARN, "sw_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(election_.init_and_start(palf_id,
                                              election_timer,
                                              &election_msg_sender_,
                                              self,
                                              election_inner_priority_seed,
                                              1,
                                              [&role_change_cb_wrpper](int64_t id,
                                                                       const ObAddr &dest_addr){
    return role_change_cb_wrpper.on_need_change_leader(id, dest_addr);
  }))) {
    PALF_LOG(WARN, "election_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(hot_cache_.init(palf_id, this))) {
    PALF_LOG(WARN, "hot_cache_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(state_mgr_.init(palf_id, self, log_meta.get_log_prepare_meta(), log_meta.get_log_replica_property_meta(),
          &election_, &sw_, &reconfirm_, &log_engine_, &config_mgr_, &mode_mgr_, &role_change_cb_wrpper_, &plugins_))) {
    PALF_LOG(WARN, "state_mgr_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(config_mgr_.init(palf_id, self, log_meta.get_log_config_meta(), &log_engine_,
          &sw_, &state_mgr_, &election_, &mode_mgr_, &reconfirm_, &plugins_))) {
    PALF_LOG(WARN, "config_mgr_ init failed", K(ret), K(palf_id));
  } else if (is_normal_replica && OB_FAIL(reconfirm_.init(palf_id, self, &sw_, &state_mgr_, &config_mgr_, &mode_mgr_, &log_engine_))) {
    PALF_LOG(WARN, "reconfirm_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(mode_mgr_.init(palf_id, self, log_meta.get_log_mode_meta(), &state_mgr_, &log_engine_, &config_mgr_, &sw_))) {
    PALF_LOG(WARN, "mode_mgr_ init failed", K(ret), K(palf_id));
  } else {
    palf_id_ = palf_id;
    fetch_log_engine_ = fetch_log_engine;
    allocator_ = alloc_mgr;
    self_ = self;
    has_set_deleted_ = false;
    palf_env_impl_ = palf_env_impl;
    is_inited_ = true;
    PALF_LOG(INFO, "PalfHandleImpl do_init_ success", K(ret), K(palf_id), K(self), K(log_dir), K(palf_base_info),
        K(log_meta), K(fetch_log_engine), K(alloc_mgr), K(log_rpc));
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

int PalfHandleImpl::receive_log_(const common::ObAddr &server,
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

int PalfHandleImpl::receive_log(const common::ObAddr &server,
                                const PushLogType push_log_type,
                                const int64_t &msg_proposal_id,
                                const LSN &prev_lsn,
                                const int64_t &prev_log_proposal_id,
                                const LSN &lsn,
                                const char *buf,
                                const int64_t buf_len)
{
  return receive_log_(server, push_log_type, msg_proposal_id, prev_lsn, prev_log_proposal_id, lsn, buf, buf_len);
}

int PalfHandleImpl::receive_batch_log(const common::ObAddr &server,
                                      const int64_t msg_proposal_id,
                                      const int64_t prev_log_proposal_id,
                                      const LSN &prev_lsn,
                                      const LSN &curr_lsn,
                                      const char *buf,
                                      const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  MemPalfGroupBufferIterator iterator;
  MemoryStorage storage;
  auto get_file_end_lsn = [curr_lsn, buf_len] () { return curr_lsn + buf_len; };
  if (OB_FAIL(iterator.init(curr_lsn, get_file_end_lsn, &storage))) {
    PALF_LOG(ERROR, "init iterator failed", K(ret), KPC(this));
  } else if (OB_FAIL(storage.init(curr_lsn))) {
    PALF_LOG(ERROR, "init storage failed", K(ret), KPC(this));
  } else if (OB_FAIL(storage.append(buf, buf_len))) {
    PALF_LOG(ERROR, "storage append failed", K(ret), KPC(this));
  } else {
    LSN prev_lsn_each_round = prev_lsn;
    int64_t prev_log_proposal_id_each_round = prev_log_proposal_id;
    LSN curr_lsn_each_round = curr_lsn;
    int64_t curr_log_proposal_id = 0;
    const char *buf_each_round = NULL;
    int64_t buf_len_each_round = 0;
    int64_t count = 0, success_count = 0;
    while (OB_SUCC(iterator.next())) {
      if (OB_FAIL(iterator.get_entry(buf_each_round, buf_len_each_round, curr_lsn_each_round, curr_log_proposal_id))) {
        PALF_LOG(ERROR, "get_entry failed", K(ret), KPC(this), K(iterator), KP(buf_each_round));
      } else if (OB_FAIL(receive_log_(server, FETCH_LOG_RESP, msg_proposal_id, prev_lsn_each_round,
            prev_log_proposal_id_each_round, curr_lsn_each_round, buf_each_round, buf_len_each_round))) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          PALF_LOG(WARN, "receive_log failed", K(ret), KPC(this), K(iterator), K(server), K(FETCH_LOG_RESP),
              K(msg_proposal_id), K(prev_lsn_each_round), K(prev_log_proposal_id_each_round),
              K(curr_lsn_each_round), KP(buf_each_round), K(buf_len_each_round));
        }
      } else {
        success_count++;
      }
      prev_lsn_each_round = curr_lsn_each_round;
      prev_log_proposal_id_each_round = curr_log_proposal_id;
      count++;
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    int64_t cost_us = ObTimeUtility::current_time() - start_ts;
    if (cost_us > 200 * 1000) {
      PALF_LOG(INFO, "receive_batch_log cost too much time", K(ret), KPC(this), K(server), K(count),
        K(success_count), K(cost_us), K(prev_lsn), K(curr_lsn), K(buf_len), K_(sw), K(iterator));
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
  } else if (!lsn.is_valid() || NULL == buf || buf_len <= 0 || buf_len > MAX_LOG_BUFFER_SIZE) {
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
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(server), K(proposal_id), K(log_end_lsn));
  } else if (!state_mgr_.can_receive_log_ack(proposal_id)) {
    // cannot handle log ack, skip
  } else if (OB_FAIL(sw_.ack_log(server, log_end_lsn))) {
    PALF_LOG(WARN, "ack_log failed", K(ret), KPC(this), K(server), K(proposal_id), K(log_end_lsn));
  } else {
    PALF_LOG(TRACE, "ack_log success", K(ret), KPC(this), K(server), K(proposal_id), K(log_end_lsn));
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
    SpinLockGuard guard(last_rebuild_meta_info_lock_);
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

void PalfHandleImpl::gen_rebuild_meta_info_(RebuildMetaInfo &rebuild_meta) const
{
  int64_t unused_log_id = -1;
  sw_.get_committed_end_lsn(rebuild_meta.committed_end_lsn_);
  sw_.get_last_submit_log_info(rebuild_meta.last_submit_lsn_, unused_log_id, rebuild_meta.last_submit_log_pid_);
}

void PalfHandleImpl::get_last_rebuild_meta_info_(RebuildMetaInfo &rebuild_meta_info) const
{
  SpinLockGuard guard(last_rebuild_meta_info_lock_);
  rebuild_meta_info = last_rebuild_meta_info_;
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
  need_rebuild = false;
  need_fetch_log = false;
  if (!base_lsn.is_valid() || !base_prev_log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(base_lsn), K(base_prev_log_info));
  } else if (state_mgr_.is_leader_active()) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "self is active leader, no need execute rebuild", K(ret), K_(palf_id), K(base_lsn));
  } else if (OB_FAIL(sw_.get_committed_end_lsn(committed_end_lsn))) {
    PALF_LOG(WARN, "get_committed_end_lsn failed", KR(ret), K_(palf_id));
  } else if (base_lsn <= committed_end_lsn) {
    PALF_LOG(INFO, "base_lsn is less than or equal to local committed_end_lsn",
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

int PalfHandleImpl::handle_notify_fetch_log_req(const common::ObAddr &server)
{
  // This req is sent by reconfirming leader.
  // Self is lag behind majority_max_lsn, so it need fetch log immediately.
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_engine_.submit_purge_throttling_task(PurgeThrottlingType::PURGE_BY_NOTIFY_FETCH_LOG))) {
    PALF_LOG(WARN, "failed to submit_purge_throttling_task with notify_fetch_log", KPC(this), K(server));
  } else if (OB_FAIL(sw_.try_fetch_log(FetchTriggerType::RECONFIRM_NOTIFY_FETCH))) {
    PALF_LOG(WARN, "try_fetch_log failed", KR(ret), KPC(this), K(server));
  } else if (OB_FAIL(sw_.submit_push_log_resp(server))) {
    PALF_LOG(WARN, "submit_push_log_resp failed", KR(ret), KPC(this), K(server));
  } else {}
  PALF_LOG(INFO, "handle_notify_fetch_log_req finished", KR(ret), KPC(this), K(server));
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
  RebuildMetaInfo rebuild_meta_info;
  do {
    int tmp_ret = OB_SUCCESS;
    // leader may send multiple notify_rebuild_req, when next req arrives, previous on_rebuild may
    // hold rlock, so try hold wlock and release it after timeout (1ms).
    const int64_t until_timeout_us = common::ObTimeUtility::current_time() + 1000;
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
    } else if (need_rebuild) {
      //set rebuild_meta_info
      gen_rebuild_meta_info_(rebuild_meta_info);
    } else {}
  } while (0);

  if (OB_SUCC(ret)) {
  // can not hold wlock when exec on_rebuild
    if (need_rebuild) {
      if (OB_FAIL(rebuild_cb_wrapper_.on_rebuild(palf_id_, base_lsn))) {
        PALF_LOG(WARN, "on_rebuild failed", K(ret), K(server), K(base_lsn));
      } else {
        PALF_EVENT("on_rebuild success", palf_id_, K(ret), K_(self), K(server), K(base_lsn));
        plugins_.record_rebuild_event(palf_id_, server, base_lsn);
      }
      // Whether on_rebuild returns OB_SUCCESS or not, set value for rebuild_base_lsn_
      SpinLockGuard rebuild_guard(last_rebuild_meta_info_lock_);
      last_rebuild_lsn_ = base_lsn;
      last_rebuild_meta_info_ = rebuild_meta_info;
    } else if (need_fetch_log && OB_FAIL(sw_.try_fetch_log(FetchTriggerType::NOTIFY_REBUILD,
                                                           base_prev_log_info.lsn_, base_lsn, base_prev_log_info.log_id_+1))) {
      PALF_LOG(WARN, "try_fetch_log failed", KR(ret), KPC(this), K(server), K(base_lsn), K(base_prev_log_info));
    }
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
                                           const int64_t accepted_mode_pid,
                                           const SCN &replayable_point,
                                           FetchLogStat &fetch_stat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(fetch_log_from_storage_(server, fetch_type, msg_proposal_id, prev_lsn,
      fetch_start_lsn, fetch_log_size, fetch_log_count, replayable_point, fetch_stat))) {
    PALF_LOG(WARN, "fetch_log_from_storage_ failed", K(ret), K_(palf_id), K_(self),
        K(server), K(fetch_type), K(msg_proposal_id), K(prev_lsn), K(fetch_start_lsn),
        K(fetch_log_size), K(fetch_log_count), K(accepted_mode_pid));
  }
  {
    // try fetch mode_meta when handle every fetch_log_req
    RLockGuard guard(lock_);
    if (OB_FAIL(mode_mgr_.submit_fetch_mode_meta_resp(server, msg_proposal_id, accepted_mode_pid))) {
      PALF_LOG(WARN, "submit_fetch_mode_meta_resp failed", K(ret), K_(palf_id), K_(self),
          K(msg_proposal_id), K(accepted_mode_pid));
    }
    const int64_t accum_size = ATOMIC_AAF(&accum_fetch_log_size_, fetch_log_size);
    if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, last_accum_fetch_statistic_time_)) {
      PALF_LOG(INFO, "[PALF STAT FETCH LOG SIZE]", KPC(this), K(accum_size));
      ATOMIC_STORE(&accum_fetch_log_size_, 0);
    }
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
    PALF_LOG(WARN, "invalid arguments", K(ret), KPC(this), K(server), K(log_lsn), K(log_end_lsn),
        K(log_proposal_id));
  } else if (OB_FAIL(mode_mgr_.get_access_mode(access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret), KPC(this));
  } else if (AccessMode::APPEND == access_mode) {
    // No need send committed_info in APPEND mode, because leader will generate keeapAlive log periodically.
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
                                            const int64_t fetch_log_count,
                                            const SCN &replayable_point,
                                            FetchLogStat &fetch_stat)
{
  int ret = OB_SUCCESS;
  LSN prev_lsn_each_round = prev_lsn;
  LSN prev_end_lsn_each_round = prev_lsn;
  int64_t prev_log_proposal_id_each_round = INVALID_PROPOSAL_ID;
  PalfGroupBufferIterator iterator;
  const LSN fetch_end_lsn = fetch_start_lsn + fetch_log_size;
  int64_t fetched_count = 0;
  const bool need_check_prev_log = (prev_lsn.is_valid() && PALF_INITIAL_LSN_VAL < fetch_start_lsn.val_);
  LSN max_flushed_end_lsn;
  LSN committed_end_lsn;
  bool is_limitted_by_end_lsn = true;
  AccessMode access_mode = AccessMode::INVALID_ACCESS_MODE;
  common::ObMemberList member_list;
  int64_t replica_num = 0;
  GlobalLearnerList learner_list;
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
    int64_t unused_mode_version;
    (void) mode_mgr_.get_access_mode(unused_mode_version, access_mode);
    (void) config_mgr_.get_curr_member_list(member_list, replica_num);
    (void) config_mgr_.get_global_learner_list(learner_list);
  } while(0);

  // max_flushed_end_lsn may be truncated by concurrent truncate, so itreator need handle this
  // case when it try to read some log which is being truncated.
  auto get_file_end_lsn = [&]() { return max_flushed_end_lsn; };
  LogInfo prev_log_info;
  const bool no_need_fetch_log = (prev_lsn >= max_flushed_end_lsn) ||
      (AccessMode::FLASHBACK == access_mode);
  const bool is_dest_in_memberlist = (member_list.contains(server) || learner_list.contains(server));
  // Rpc delay increases enormously when it's size exceeds 2M.
  const int64_t MAX_BATCH_LOG_SIZE_EACH_ROUND = 2 * 1024 * 1024 - 1024;
  char *batch_log_buf = NULL;
  if (no_need_fetch_log) {
    PALF_LOG(INFO, "no need fetch_log_from_storage", K(ret), KPC(this), K(server), K(fetch_start_lsn), K(prev_lsn),
        K(max_flushed_end_lsn), K(access_mode));
  } else if (true == need_check_prev_log
      && OB_FAIL(get_prev_log_info_for_fetch_(prev_lsn, fetch_start_lsn, prev_log_info))) {
    PALF_LOG(WARN, "get_prev_log_info_for_fetch_ failed", K(ret), K_(palf_id), K(prev_lsn), K(fetch_start_lsn));
  } else if (true == need_check_prev_log && prev_log_info.lsn_ != prev_lsn) {
    if (is_dest_in_memberlist) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "the LSN between each replica is not same, unexpected error!!!", K(ret),
          K_(palf_id), K(fetch_start_lsn), K(prev_log_info));
    } else {
      PALF_LOG(INFO, "the LSN between leader and non paxos member is not same, do not fetch log",
          K_(palf_id), K(fetch_start_lsn), K(prev_log_info));
    }
  } else if (check_need_hook_fetch_log_(fetch_type, fetch_start_lsn)) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
  } else if (FALSE_IT(prev_log_proposal_id_each_round = prev_log_info.log_proposal_id_)) {
  } else if (OB_FAIL(iterator.init(fetch_start_lsn, get_file_end_lsn, log_engine_.get_log_storage()))) {
    PALF_LOG(ERROR, "init iterator failed", K(ret), K_(palf_id));
  } else if (FALSE_IT(iterator.set_need_print_error(false))) {
    // NB: Fetch log will be concurrent with truncate, the content on disk will not integrity, need igore
    //     read log error.
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0 &&
      OB_NOT_NULL(batch_log_buf = (char *)mtl_malloc(MAX_BATCH_LOG_SIZE_EACH_ROUND, "BatchLogBuf"))) {
    // When group log size > 10K, we just send it without aggregating.
    const int64_t MAX_NEED_BATCH_LOG_SIZE = 10 * 1024;
    // When batched log count reaches max_batch_log_count_each_round, we end the aggregation.
    const int64_t max_batch_log_count_each_round = PALF_SLIDING_WINDOW_SIZE;
    int64_t remained_count = fetch_log_count;
    bool is_reach_end = false;
    bool skip_next = false;
    BatchFetchParams batch_fetch_params;
    batch_fetch_params.batch_log_buf_ = batch_log_buf;
    batch_fetch_params.can_batch_size_ = MAX_BATCH_LOG_SIZE_EACH_ROUND;
    batch_fetch_params.last_log_lsn_prev_round_ = prev_lsn_each_round;
    batch_fetch_params.last_log_end_lsn_prev_round_ = prev_end_lsn_each_round;
    batch_fetch_params.last_log_proposal_id_prev_round_ = prev_log_proposal_id_each_round;
    while  (OB_SUCC(ret) && remained_count > 0 && !is_reach_end &&
        batch_fetch_params.last_log_end_lsn_prev_round_ < fetch_end_lsn) {
      batch_fetch_params.can_batch_count_ = MIN(remained_count, max_batch_log_count_each_round);
      batch_fetch_params.has_consumed_count_ = 0;
      if (OB_FAIL(batch_fetch_log_each_round_(server, msg_proposal_id, iterator, is_limitted_by_end_lsn,
          is_dest_in_memberlist, replayable_point, fetch_end_lsn, committed_end_lsn, MAX_NEED_BATCH_LOG_SIZE,
          batch_fetch_params, skip_next, is_reach_end, fetch_stat)) && OB_ITER_END != ret) {
        PALF_LOG(WARN, "batch_fetch_log_each_round_ failed", K(ret), KPC(this), K(iterator));
      } else {
        remained_count -= batch_fetch_params.has_consumed_count_;
      }
    }
    prev_lsn_each_round = batch_fetch_params.last_log_lsn_prev_round_;
    prev_end_lsn_each_round = batch_fetch_params.last_log_end_lsn_prev_round_;
    prev_log_proposal_id_each_round = batch_fetch_params.last_log_proposal_id_prev_round_;
    fetched_count = fetch_log_count - remained_count;
  } else {
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0) {
      PALF_LOG(WARN, "allocate batch_log_buf memory failed", KPC(this), K(server), K(prev_lsn));
    }
    LogGroupEntry curr_group_entry;
    LSN curr_lsn;
    LSN curr_log_end_lsn;
    bool is_reach_size_limit = false;  // whether the total fetched size exceeds fetch_log_size
    bool is_reach_count_limit = false;
    bool is_reach_end = false;
    int64_t total_size = 0;
    int64_t send_cost = 0;
    int64_t send_begin_time = ObTimeUtility::current_time();
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
      } else if (false == is_dest_in_memberlist &&
          curr_group_entry.get_header().is_raw_write() &&
          replayable_point.is_valid() &&
          curr_group_entry.get_scn() > replayable_point) {
        is_reach_end = true;
        PALF_LOG(INFO, "non paxos member could not fetch logs which scn is bigger than replayable_point, end fetch",
            K_(palf_id), K(server), K(msg_proposal_id), K(curr_lsn), K(replayable_point));
      } else if (FALSE_IT(send_begin_time = ObTimeUtility::current_time())) {
      } else if (OB_FAIL(submit_fetch_log_resp_(server, msg_proposal_id, prev_log_proposal_id_each_round, \
          prev_lsn_each_round, curr_lsn, curr_group_entry))) {
        PALF_LOG(WARN, "submit_fetch_log_resp_ failed", K(ret), K_(palf_id), K(server),
            K(msg_proposal_id), K(prev_lsn_each_round), K(fetch_start_lsn));
      } else {
        send_cost += ObTimeUtility::current_time() - send_begin_time;
        fetched_count++;
        total_size += curr_group_entry.get_group_entry_size();
        if (fetched_count >= fetch_log_count) {
          is_reach_count_limit = true;
        }
        // check if reach size limit
        if (curr_log_end_lsn >= fetch_end_lsn) {
          is_reach_size_limit = true;
        }
        PALF_LOG(TRACE, "fetch one log success", K(ret), K_(palf_id), K_(self), K(server), K(prev_lsn),
            K(fetch_start_lsn), K(prev_lsn_each_round), K(curr_lsn), K(curr_group_entry),
            K(prev_log_proposal_id_each_round), K(fetch_end_lsn), K(curr_log_end_lsn), K(is_reach_size_limit),
            K(fetch_log_size), K(fetched_count), K(is_reach_count_limit));
        prev_lsn_each_round = curr_lsn;
        prev_end_lsn_each_round = curr_log_end_lsn;
        prev_log_proposal_id_each_round = curr_group_entry.get_header().get_log_proposal_id();
      }
    }
    // update fetch statistic info
    fetch_stat.total_size_ = total_size;
    fetch_stat.group_log_cnt_ = fetched_count;
    fetch_stat.send_cost_ = send_cost;
  }
  if (batch_log_buf != NULL) {
    mtl_free(batch_log_buf);
    batch_log_buf = NULL;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  PALF_LOG(INFO, "fetch_log_from_storage_ finished", K(ret), KPC(this), K(prev_lsn), K(iterator),
      K(fetch_log_count), "has_fetched_log_count", fetched_count);
  // try send committed_info to server
  if (OB_SUCC(ret)) {
    RLockGuard guard(lock_);
    (void) try_send_committed_info_(server, prev_lsn_each_round, prev_end_lsn_each_round,
        prev_log_proposal_id_each_round);
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

int PalfHandleImpl::batch_fetch_log_each_round_(const common::ObAddr &server,
                                                const int64_t msg_proposal_id,
                                                PalfGroupBufferIterator &iterator,
                                                const bool is_limitted_by_end_lsn,
                                                const bool is_dest_in_memberlist,
                                                const share::SCN& replayable_point,
                                                const LSN &fetch_end_lsn,
                                                const LSN &committed_end_lsn,
                                                const int64_t max_need_batch_log_size,
                                                BatchFetchParams &batch_fetch_params,
                                                bool &skip_next,
                                                bool &is_reach_end,
                                                FetchLogStat &fetch_stat)
{
  int ret = OB_SUCCESS;
  int64_t remained_count = batch_fetch_params.can_batch_count_;
  int64_t remained_size = batch_fetch_params.can_batch_size_;
  LSN prev_lsn = batch_fetch_params.last_log_lsn_prev_round_;
  LSN prev_end_lsn = batch_fetch_params.last_log_end_lsn_prev_round_;
  int64_t prev_log_proposal_id = batch_fetch_params.last_log_proposal_id_prev_round_;
  int64_t has_consumed_count = batch_fetch_params.has_consumed_count_;
  LSN curr_lsn;
  LSN curr_end_lsn;
  int64_t curr_log_proposal_id;
  LSN first_log_lsn;
  const char *curr_log_buf = NULL;
  int64_t curr_log_buf_len = 0;
  int64_t log_end_pos = 0;
  bool has_reach_threshold = false;
  share::SCN curr_scn;
  bool is_raw_write;

  int64_t start_ts = ObTimeUtility::current_time();
  while (OB_SUCC(ret) && !is_reach_end && remained_count > 0 && remained_size > 0) {
    if (!skip_next && OB_FAIL(iterator.next())) {
      PALF_LOG(WARN, "iterator next failed", K(ret), KPC(this), K(iterator), K(replayable_point), K(prev_lsn));
    } else if (FALSE_IT(skip_next = false)) {
    } else if (OB_FAIL(iterator.get_entry(curr_log_buf, curr_log_buf_len, curr_scn, curr_lsn, curr_log_proposal_id,
        is_raw_write))){
      PALF_LOG(WARN, "iterator get_entry failed", K(ret), KPC(this), K(iterator), K(log_end_pos));
    } else if (false == is_dest_in_memberlist && is_raw_write && replayable_point.is_valid() &&
        curr_scn > replayable_point) {
      is_reach_end = true;
      PALF_LOG(INFO, "non paxos member could not fetch logs which scn is bigger than replayable_point, end fetch",
            K_(palf_id), K(server), K(msg_proposal_id), K(curr_lsn), K(replayable_point));
    } else if (FALSE_IT(curr_end_lsn = curr_lsn + curr_log_buf_len)) {
    } else if (is_limitted_by_end_lsn && curr_end_lsn > committed_end_lsn){
      // Only leader replica can send uncommitted logs to others,
      // the other replicas just send committed logs to avoid unexpected rewriting.
      is_reach_end = true;
      PALF_LOG(INFO, "reach committed_end_lsn(not leader active replica), end fetch", K(ret), K_(palf_id), K(server),
          K(msg_proposal_id), K(curr_lsn), K(curr_end_lsn), K(committed_end_lsn));
    } else if (curr_log_buf_len >= max_need_batch_log_size ) {
      has_reach_threshold = true;
      PALF_LOG(TRACE, "group log is bigger than batch_log_size_threshold", K(ret), K_(palf_id), K(server),
          K(msg_proposal_id), K(curr_lsn), K(curr_end_lsn), K(max_need_batch_log_size));
      break;
    } else if (remained_size >= curr_log_buf_len ) {
      if (!first_log_lsn.is_valid()) {
        first_log_lsn = curr_lsn;
      }
      MEMCPY(batch_fetch_params.batch_log_buf_ + log_end_pos, curr_log_buf, curr_log_buf_len);
      log_end_pos += curr_log_buf_len;
      prev_lsn = curr_lsn;
      prev_end_lsn = curr_end_lsn;
      prev_log_proposal_id = curr_log_proposal_id;
      has_consumed_count++;
      remained_count--;
      remained_size -= curr_log_buf_len;
      if (curr_end_lsn >= fetch_end_lsn) {
        PALF_LOG(INFO, "fetched log has reached fetch_end_lsn", K(ret), KPC(this), K(curr_lsn), K(curr_end_lsn),
          K(fetch_end_lsn));
        is_reach_end = true;
      }
    } else {
      // batch the group log next round
      skip_next = true;
      PALF_LOG(TRACE, "batched log has exceeded can_batch_size", K(ret), KPC(this), K(batch_fetch_params.can_batch_size_),
          K(remained_size), K(prev_lsn), K(curr_lsn));
      break;
    }
  }
  int64_t batch_end_ts = common::ObTimeUtility::current_time();
  if (OB_ITER_END == ret) {
    is_reach_end = true;
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    PALF_LOG(WARN, "batch log failed", K(ret), KPC(this), K(iterator), K(log_end_pos),
        K(curr_lsn), K(has_consumed_count), K(first_log_lsn));
  } else {
    if (1 < has_consumed_count) {
      ret = submit_batch_fetch_log_resp_(server, msg_proposal_id, batch_fetch_params.last_log_proposal_id_prev_round_,
          batch_fetch_params.last_log_lsn_prev_round_, first_log_lsn, batch_fetch_params.batch_log_buf_, log_end_pos);
    } else if (1 == has_consumed_count) {
      ret = submit_fetch_log_resp_(server, msg_proposal_id, batch_fetch_params.last_log_proposal_id_prev_round_,
          batch_fetch_params.last_log_lsn_prev_round_, first_log_lsn, batch_fetch_params.batch_log_buf_, log_end_pos);
    } else {
      PALF_LOG(TRACE, "no log is aggregated", K(ret), KPC(this), K(log_end_pos), K(curr_lsn),
          K(has_consumed_count), K(first_log_lsn));
    }
  }

  if (OB_SUCC(ret) && has_reach_threshold) {
    if (OB_FAIL(submit_fetch_log_resp_(server, msg_proposal_id, prev_log_proposal_id, prev_lsn,
        curr_lsn, curr_log_buf, curr_log_buf_len))) {
      PALF_LOG(WARN, "submit_fetch_log_resp_ failed", K(ret), KPC(this), K(prev_lsn),
          K(curr_lsn), K(curr_log_buf_len));
    } else {
      log_end_pos += curr_log_buf_len;
      prev_lsn = curr_lsn;
      prev_end_lsn = curr_end_lsn;
      prev_log_proposal_id = curr_log_proposal_id;
      has_consumed_count++;
      remained_count--;
      remained_size -= curr_log_buf_len;
      PALF_LOG(TRACE, "submit_fetch_log_resp_ success", K(ret), KPC(this), K(prev_lsn),
          K(curr_lsn), K(curr_log_buf_len));
    }
  }
  int64_t send_end_ts = common::ObTimeUtility::current_time();
  if (OB_SUCC(ret) && 0 != has_consumed_count) {
    batch_fetch_params.last_log_lsn_prev_round_ = prev_lsn;
    batch_fetch_params.last_log_end_lsn_prev_round_ = prev_end_lsn;
    batch_fetch_params.last_log_proposal_id_prev_round_ = prev_log_proposal_id;
    batch_fetch_params.has_consumed_count_ = has_consumed_count;
    int64_t total_size = log_end_pos;
    int64_t batch_cost = batch_end_ts - start_ts;
    int64_t send_cost = send_end_ts - batch_end_ts;
    int64_t total_cost = send_end_ts - start_ts;
    fetch_stat.total_size_ += total_size;
    fetch_stat.group_log_cnt_ += has_consumed_count;
    fetch_stat.send_cost_ += send_cost;
    int64_t avg_log_size = total_size / has_consumed_count;
    int64_t avg_log_cost = total_cost / has_consumed_count;
    int64_t avg_batch_cost = batch_cost / has_consumed_count;
    int64_t avg_send_cost = send_cost/ has_consumed_count;
    PALF_LOG(TRACE, "batch_fetch_log_one_round_ success", K(ret), KPC(this), K(is_reach_end),
        K(batch_fetch_params.can_batch_size_), K(remained_size), K(batch_fetch_params.can_batch_count_),
        K(has_consumed_count), K(total_size), K(total_cost), K(batch_cost), K(send_cost), K(avg_log_size),
        K(avg_log_cost), K(avg_send_cost), K(avg_batch_cost), K(first_log_lsn),
        K(batch_fetch_params.last_log_lsn_prev_round_), K(iterator));
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

int PalfHandleImpl::submit_fetch_log_resp_(const common::ObAddr &server,
                                          const int64_t &msg_proposal_id,
                                          const int64_t &prev_log_proposal_id,
                                          const LSN &prev_lsn,
                                          const LSN &curr_lsn,
                                          const char *buf,
                                          const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  LogWriteBuf write_buf;
  // NB: 'curr_group_entry' generates by PalfGroupBufferIterator, the memory is safe before next();
  if (OB_FAIL(write_buf.push_back(buf, buf_len))) {
    PALF_LOG(WARN, "push_back buf into LogWriteBuf failed", K(ret));
  } else if (OB_FAIL(log_engine_.submit_push_log_req(server, FETCH_LOG_RESP, msg_proposal_id, prev_log_proposal_id,
        prev_lsn, curr_lsn, write_buf))) {
    PALF_LOG(WARN, "submit_push_log_req failed", K(ret), K(server), K(msg_proposal_id), K(prev_log_proposal_id),
        K(prev_lsn), K(curr_lsn), K(write_buf));
  } else {
    PALF_LOG(TRACE, "submit_fetch_log_resp_ success", K(ret), K(server), K(msg_proposal_id), K(prev_log_proposal_id),
        K(prev_lsn), K(curr_lsn), K(write_buf));
  }
  return ret;
}

int PalfHandleImpl::submit_batch_fetch_log_resp_(const common::ObAddr &server,
                                                 const int64_t msg_proposal_id,
                                                 const int64_t prev_log_proposal_id,
                                                 const LSN &prev_lsn,
                                                 const LSN &curr_lsn,
                                                 const char *buf,
                                                 const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  LogWriteBuf write_buf;
  int64_t pos = 0;
  if (OB_FAIL(write_buf.push_back(buf, buf_len))) {
    PALF_LOG(WARN, "push_back buf into LogWriteBuf failed", K(ret));
  } else if (OB_FAIL(log_engine_.submit_batch_fetch_log_resp(server, msg_proposal_id, prev_log_proposal_id,
        prev_lsn, curr_lsn, write_buf))) {
    PALF_LOG(WARN, "submit_push_log_req failed", K(ret), K(server), K(msg_proposal_id), K(prev_log_proposal_id),
        K(prev_lsn), K(curr_lsn), K(write_buf));
  } else {
    PALF_LOG(TRACE, "submit_batch_fetch_log_resp_ success", K(ret), K(server), KPC(this), K(msg_proposal_id),
        K(prev_log_proposal_id), K(buf_len), K(prev_lsn), K(curr_lsn), K(write_buf));
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
        K(msg_proposal_id), K(prev_lsn), K(prev_mode_pid), K(meta));
  } else if (OB_FAIL(try_update_proposal_id_(server, msg_proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", KR(ret), KPC(this), K(server), K(msg_proposal_id));
  } else {
    TruncateLogInfo truncate_log_info;
    bool need_print_register_log = false;
    // need wlock in case of truncating log and writing log_ms_meta in LogConfigMgr
    WLockGuard guard(lock_);
    // max_scn of multiple replicas may be different in FLASHBACK mode,
    // therefore, skip log barrier for config logs
    const bool skip_log_barrier = mode_mgr_.need_skip_log_barrier();
    if (false == state_mgr_.can_receive_config_log(msg_proposal_id) ||
        false == config_mgr_.can_receive_config_log(server, meta)) {
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
    } else if (!skip_log_barrier && !sw_.pre_check_for_config_log(msg_proposal_id, prev_lsn, prev_log_proposal_id, truncate_log_info)) {
      ret = OB_STATE_NOT_MATCH;
      PALF_LOG(WARN, "pre_check_for_config_log failed, cannot receive config log",
          KR(ret), KPC(this), K(server), K(msg_proposal_id), K(prev_lsn), K(prev_log_proposal_id), K(meta));
    } else if (!skip_log_barrier
        && TRUNCATE_LOG == truncate_log_info.truncate_type_
        && OB_FAIL(sw_.truncate(truncate_log_info, prev_lsn, prev_log_proposal_id))) {
        PALF_LOG(WARN, "sw truncate failed", KR(ret), KPC(this), K(truncate_log_info));
    } else if (OB_FAIL(config_mgr_.receive_config_log(server, meta))) {
      PALF_LOG(WARN, "receive_config_log failed", KR(ret), KPC(this), K(server), K(msg_proposal_id),
          K(prev_log_proposal_id), K(prev_lsn));
    } else if (!meta.curr_.config_.log_sync_memberlist_.contains(self_) &&
               meta.curr_.config_.arbitration_member_.get_server() != self_ &&
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

int PalfHandleImpl::get_total_used_disk_space(int64_t &total_used_disk_space, int64_t &unrecyclable_disk_space) const
{
  int ret = OB_SUCCESS;
  total_used_disk_space = 0;
  unrecyclable_disk_space = 0;
  if (OB_FAIL(log_engine_.get_total_used_disk_space(total_used_disk_space, unrecyclable_disk_space))) {
    PALF_LOG(WARN, "get_total_used_disk_space failed", K(ret), KPC(this));
  }
  return ret;
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
  PALF_LOG(INFO, "inner_after_flush_meta", K(flush_meta_cb_ctx));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (MODE_META == flush_meta_cb_ctx.type_) {
    WLockGuard guard(lock_);
    ret = after_flush_mode_meta_(flush_meta_cb_ctx.proposal_id_,
                                 flush_meta_cb_ctx.is_applied_mode_meta_,
                                 flush_meta_cb_ctx.log_mode_meta_);
  } else {
    RLockGuard guard(lock_);
    switch(flush_meta_cb_ctx.type_) {
      case PREPARE_META:
        ret = after_flush_prepare_meta_(flush_meta_cb_ctx.proposal_id_);
        break;
      case CHANGE_CONFIG_META:
        ret = after_flush_config_change_meta_(flush_meta_cb_ctx.proposal_id_, flush_meta_cb_ctx.config_version_);
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

int PalfHandleImpl::inner_after_flashback(const FlashbackCbCtx &flashback_ctx)
{
  int ret = OB_SUCCESS;
  // do nothing
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

int PalfHandleImpl::after_flush_mode_meta_(const int64_t proposal_id,
                                           const bool is_applied_mode_meta,
                                           const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  const ObAddr &leader = state_mgr_.get_leader();
  if (proposal_id != state_mgr_.get_proposal_id()) {
    PALF_LOG(WARN, "proposal_id has changed during flushing", K(ret), K(proposal_id),
        "curr_proposal_id", state_mgr_.get_proposal_id());
  } else if (OB_FAIL(mode_mgr_.after_flush_mode_meta(is_applied_mode_meta, mode_meta))) {
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

int PalfHandleImpl::get_prev_log_info_for_fetch_(const LSN &prev_lsn,
                                                 const LSN &curr_lsn,
                                                 LogInfo &prev_log_info)
{
  int ret = OB_SUCCESS;
  PalfGroupBufferIterator iterator;
  auto get_file_end_lsn = [&]() { return curr_lsn; };
  auto get_mode_version = [this]() -> int64_t {
    int64_t mode_version = INVALID_PROPOSAL_ID;
    int ret = OB_SUCCESS;
    if (OB_FAIL(this->get_access_mode_version(mode_version))) {
      PALF_LOG(WARN, "get_access_mode_version failed", K(ret), KPC(this));
      mode_version = INVALID_PROPOSAL_ID;
    }
    return mode_version;
  };
  if (OB_FAIL(iterator.init(prev_lsn, get_file_end_lsn, get_mode_version, log_engine_.get_log_storage()))) {
    PALF_LOG(WARN, "LogGroupEntryIterator init failed", K(ret), K(iterator), K(prev_lsn), K(curr_lsn));
  } else if (FALSE_IT(iterator.set_need_print_error(false))) {
  } else {
    LogGroupEntry entry;
    LSN lsn;
    if (OB_SUCC(iterator.next())) {
      if (OB_FAIL(iterator.get_entry(entry, lsn))) {
        PALF_LOG(ERROR, "get_entry failed", K(ret), K(iterator));
      } else {
        const LogGroupEntryHeader &header = entry.get_header();
        prev_log_info.log_id_ = header.get_log_id();
        prev_log_info.scn_ = header.get_max_scn();
        prev_log_info.accum_checksum_ = header.get_accum_checksum();
        prev_log_info.log_proposal_id_ = header.get_log_proposal_id();
        prev_log_info.lsn_ = prev_lsn;
      }
    } else if (OB_FAIL(get_prev_log_info_(curr_lsn, prev_log_info))) {
      PALF_LOG(WARN, "get_prev_log_info_ failed", K(ret), KPC(this));
    }
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
  if (lsn == base_lsn && OB_SUCC(log_snapshot_meta.get_prev_log_info(log_info))) {
    prev_log_info = log_info;
    PALF_LOG(INFO, "lsn is same as base_lsn, and log_snapshot_meta is valid", K(lsn), K(log_snapshot_meta));
  } else if (OB_FAIL(iterator.init(start_lsn, get_file_end_lsn, log_engine_.get_log_storage()))) {
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
        ret = OB_ERR_OUT_OF_UPPER_BOUND;;
        PALF_LOG(WARN, "there is no log before lsn", K(ret), K(lsn), KPC(this), K(iterator));
      // defense code
      } else if (prev_lsn >= lsn) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "prev lsn must be smaller than lsn", K(ret), K(iterator), K(lsn), K(prev_lsn), K(prev_entry_header));
      } else {
        prev_log_info.log_id_ = prev_entry_header.get_log_id();
        prev_log_info.scn_ = prev_entry_header.get_max_scn();
        prev_log_info.accum_checksum_ = prev_entry_header.get_accum_checksum();
        prev_log_info.log_proposal_id_ = prev_entry_header.get_log_proposal_id();
        prev_log_info.lsn_ = prev_lsn;
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
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
  LSN committed_end_lsn;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id));
  } else if (OB_FAIL(sw_.get_max_flushed_log_info(unused_prev_lsn, max_flushed_end_lsn, max_flushed_log_pid))) {
    PALF_LOG(WARN, "get_max_flushed_log_info failed", K(ret), K_(palf_id));
  } else if (OB_FAIL(sw_.get_committed_end_lsn(committed_end_lsn))) {
    PALF_LOG(WARN, "get_committed_end_lsn failed", K(ret), K_(palf_id));
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
            max_flushed_end_lsn, committed_end_lsn, accepted_mode_meta))) {
      PALF_LOG(WARN, "submit_prepare_response failed", K(ret), K_(palf_id));
    } else {
      PALF_LOG(INFO, "submit_prepare_response success", K(ret), K_(palf_id), K_(self), K(server),
          K(vote_granted), K(accept_proposal_id), K(max_flushed_end_lsn), K(committed_end_lsn),
          K(accepted_mode_meta));
    }
  }
  return ret;
}

int PalfHandleImpl::construct_palf_base_info_(const LSN &max_committed_lsn,
                                              PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  LogInfo prev_log_info;
  const LSN base_lsn = log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_;
  if (false == max_committed_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(max_committed_lsn), K(base_lsn));
    // NB:
    // 1. for rebuild, there may be no valid block on disk, however, the 'prev_log_info' has been saved
    //    in LogMeta, if 'max_committed_end_lsn' is same as 'base_lsn', we can construct PalfBaseInfo
    //    as 'prev_log_info'
    // 2. for gc, there is at least two blocks on disk, if 'max_committed_end_lsn' is same as 'base_lsn',
    //    we can construct PalfBaseInfo via iterator.
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

int PalfHandleImpl::construct_palf_base_info_for_flashback_(const LSN &start_lsn,
                                                            const SCN &flashback_scn,
                                                            const LSN &prev_entry_lsn,
                                                            const LogGroupEntryHeader &prev_entry_header,
                                                            PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  LogInfo &prev_log_info = palf_base_info.prev_log_info_;
  const LSN base_lsn = log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_;
  if (false == start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(start_lsn), K(base_lsn));
  } else if (prev_entry_header.is_valid()) {
    prev_log_info.log_id_ = prev_entry_header.get_log_id();
    prev_log_info.scn_ = prev_entry_header.get_max_scn();
    prev_log_info.accum_checksum_ = prev_entry_header.get_accum_checksum();
    prev_log_info.log_proposal_id_ = prev_entry_header.get_log_proposal_id();
    prev_log_info.lsn_ = prev_entry_lsn;
    palf_base_info.curr_lsn_ = prev_entry_lsn + prev_entry_header.get_serialize_size() + prev_entry_header.get_data_len();
    PALF_LOG(INFO, "prev_entry is valid, construct PalfBaseInfo via prev_entry_header", K(prev_entry_header),
        K(palf_base_info));
  } else if (OB_FAIL(get_prev_log_info_(start_lsn, prev_log_info))
             && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    PALF_LOG(WARN, "get_prev_entry_header_before_ failed", K(ret), K(start_lsn), K(prev_log_info));
  // NB: if flashback_scn is smaller than min scn of palf, we need generate PalfBaseInfo by default
  //     and set the scn of prev_log_info with flashback_scn.
  } else if (prev_log_info.scn_ != flashback_scn) {
    PALF_LOG(WARN, "there is no log before flashback_scn, need generate new PalfBaseInfo via flashback_scn",
        K(ret), K(start_lsn), K(palf_base_info), K(prev_log_info));
    prev_log_info.scn_ = flashback_scn;
    palf_base_info.curr_lsn_ = start_lsn;
    if (OB_FAIL(log_engine_.update_log_snapshot_meta_for_flashback(palf_base_info.prev_log_info_))) {
      PALF_LOG(ERROR, "update_log_snapshot_meta_for_flashback failed", K(ret), K(flashback_scn),
          K(palf_base_info));
    }
  } else {
    palf_base_info.prev_log_info_ = prev_log_info;
    palf_base_info.curr_lsn_ = start_lsn;
  }

  PALF_LOG(INFO, "construct_palf_base_info_for_flashback_ finish", K(ret), K(start_lsn),
      K(palf_base_info), K(prev_log_info));

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
  } else if (OB_FAIL(iterator.init(start_lsn, get_file_end_lsn, log_engine_.get_log_storage()))) {
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

int PalfHandleImpl::get_election_leader_without_lock_(ObAddr &addr) const
{
  int ret = OB_SUCCESS;
  int64_t unused_leader_epoch = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret));
  } else if (OB_FAIL(election_.get_current_leader_likely(addr, unused_leader_epoch))) {
    PALF_LOG(WARN, "get_election_leader failed", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_LEADER_NOT_EXIST;
    PALF_LOG(WARN, "election has no leader", K(ret), KPC(this));
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

int PalfHandleImpl::diagnose(PalfDiagnoseInfo &diagnose_info) const
{
  int ret = OB_SUCCESS;
  state_mgr_.get_role_and_state(diagnose_info.palf_role_, diagnose_info.palf_state_);
  diagnose_info.palf_proposal_id_ = state_mgr_.get_proposal_id();
  state_mgr_.get_election_role(diagnose_info.election_role_, diagnose_info.election_epoch_);
  diagnose_info.enable_sync_ = state_mgr_.is_sync_enabled();
  diagnose_info.enable_vote_ = state_mgr_.is_allow_vote();
  return ret;
}

int PalfHandleImpl::flashback(const int64_t mode_version,
                              const share::SCN &flashback_scn,
                              const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int lock_ret = OB_EAGAIN;
  bool is_already_done = false;
  common::ObTimeGuard time_guard("single_replica_flashback", 1 * 1000 * 1000);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl not inited", KPC(this));
  } else if (!flashback_scn.is_valid() || OB_INVALID_TIMESTAMP == timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(ret), KPC(this), K(flashback_scn), K(timeout_us));
  } else if (OB_SUCCESS != (lock_ret = flashback_lock_.trylock())) {
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "another flashback operation is doing, try again",
        KPC(this), K(flashback_scn));
  } else if (OB_FAIL(can_do_flashback_(mode_version, flashback_scn, is_already_done))) {
    PALF_LOG(WARN, "can_do_flashback_ failed", K(ret), KPC(this), K(mode_version), K(flashback_scn));
  } else if (true == is_already_done) {
  } else {
    FlashbackCbCtx flashback_cb_ctx(flashback_scn);
    const LSN max_lsn = get_max_lsn();
    const LSN end_lsn = get_end_lsn();
    flashback_state_ = LogFlashbackState::FLASHBACK_INIT;
    PALF_EVENT("[BEGIN FLASHBACK]", palf_id_, KPC(this), K(mode_version), K(flashback_scn), K(timeout_us),
        K(end_lsn), K(max_lsn));
    do {
      RLockGuard guard(lock_);
      if (OB_FAIL(log_engine_.submit_flashback_task(flashback_cb_ctx))) {
        PALF_LOG(ERROR, "submit_flashback_task failed", K(ret), KPC(this), K(flashback_scn));
      }
    } while (0);
    TimeoutChecker not_timeout(timeout_us);
    while (OB_SUCC(ret) && OB_SUCC(not_timeout())) {
      RLockGuard guard(lock_);
      if (LogFlashbackState::FLASHBACK_FAILED == flashback_state_) {
        ret = OB_EAGAIN;
        PALF_LOG(WARN, "flashback failed", K(ret), KPC(this), K(mode_version), K(flashback_scn));
      } else if (LogFlashbackState::FLASHBACK_RECONFIRM == flashback_state_) {
        ret = OB_EAGAIN;
        PALF_LOG(WARN, "can not flashback a reconfirming leader", K(ret), KPC(this),
            K(mode_version), K(flashback_scn));
      } else if (LogFlashbackState::FLASHBACK_SUCCESS == flashback_state_) {
        const SCN &curr_end_scn = get_end_scn();
        const SCN &curr_max_scn = get_max_scn();
        if (flashback_scn >= curr_max_scn) {
          time_guard.click("flashback_done");
        } else {
          ret = OB_ERR_UNEXPECTED;
          PALF_LOG(ERROR, "flashback finished, but logs haven't been flashed back",
              K(ret), KPC(this), K(mode_version), K(flashback_scn), K(curr_max_scn));
        }
        PALF_EVENT("[END FLASHBACK]", palf_id_, K(ret), KPC(this), K(mode_version),
            K(flashback_scn), K(timeout_us), K(curr_end_scn), K(curr_max_scn), K(time_guard));
        plugins_.record_flashback_event(palf_id_, mode_version, flashback_scn, curr_end_scn, curr_max_scn);
        break;
      } else {
        usleep(100*1000);
        PALF_LOG(INFO, "flashback not finished", K(ret), KPC(this), K(flashback_scn), K(log_engine_));
      }
    }
    FLOG_INFO("[END FLASHBACK PALF_DUMP]", K(ret), K_(palf_id), K_(self), "[SlidingWindow]", sw_,
        "[StateMgr]", state_mgr_, "[ConfigMgr]", config_mgr_, "[ModeMgr]", mode_mgr_,
        "[LogEngine]", log_engine_, "[Reconfirm]", reconfirm_);
  }
  if (OB_SUCCESS == lock_ret) {
    flashback_lock_.unlock();
  }
  return ret;
}

int PalfHandleImpl::can_do_flashback_(const int64_t mode_version,
                                      const share::SCN &flashback_scn,
                                      bool &is_already_done)
{
  int ret = OB_SUCCESS;
  int64_t curr_mode_version = -1;
  AccessMode curr_access_mode = AccessMode::INVALID_ACCESS_MODE;
  block_id_t start_block;
  LSN start_lsn_of_block;
  is_already_done = false;

  RLockGuard guard(lock_);
  (void) mode_mgr_.get_access_mode(curr_mode_version, curr_access_mode);
  const SCN &curr_max_scn = get_max_scn();
  const SCN &curr_end_scn = get_end_scn();
  if (AccessMode::FLASHBACK != curr_access_mode || mode_version != curr_mode_version) {
    (void) sw_.try_fetch_log(FetchTriggerType::MODE_META_BARRIER);
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "access_mode don't match, can't do flashback", K(ret), KPC(this), K(curr_access_mode),
        K(curr_mode_version), K(mode_version), K(flashback_scn));
  } else if (flashback_scn >= curr_max_scn) {
    ret = OB_SUCCESS;
    is_already_done = true;
    PALF_LOG(INFO, "[FLASHBACK] do not need to flashback", K(ret), KPC(this), K(mode_version),
        K(flashback_scn), K(curr_max_scn), K(curr_end_scn));
    // NB: because we have checked whether flashback_scn is greater than or equal to curr_max_scn,
    //     there is no possibility that no log on disk.
  } else if (OB_FAIL(get_block_id_by_scn_for_flashback_(flashback_scn, start_block))) {
    PALF_LOG(ERROR, "get_block_id_by_scn_for_flashback_ failed", K(ret), KPC(this), K(flashback_scn));
  } else if (FALSE_IT(start_lsn_of_block.val_ = start_block * PALF_BLOCK_SIZE)) {
  } else if (start_lsn_of_block < get_base_lsn_used_for_block_gc()) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(ERROR, "flashpoint point is smaller than base_lsn, not support",
        K(ret), KPC(this), K(sw_), K(start_lsn_of_block), "base_lsn:", get_base_lsn_used_for_block_gc());
  }
  return ret;
}

// step1. create tmp block and delete each block after 'flashback_scn';
// step2. execute flashback;
// step3. rename tmp block to normal.
//
// Constraint:
// 1. in process of flashback, prohibit modify 'base_lsn';
// 2. in process of flashback, need keep sw_ is empty;
// 3. in process of flashback, if flashback point is smaller than 'base_lsn', not support.
int PalfHandleImpl::inner_flashback(const share::SCN &flashback_scn)
{
  int ret = OB_SUCCESS;
  block_id_t start_block;
  LSN start_lsn_of_block;
  WLockGuard guard(lock_);
  // In process of flashback, stop submit_log.
  // TODO by runlin: if we care about hold wlock too much time, can optimize.
  const SCN &end_scn = get_end_scn();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl not inited", KPC(this));
  } else if (state_mgr_.is_leader_reconfirm()) {
    PALF_LOG(INFO, "can not do flashback in leader reconfirm state", KPC(this), K(flashback_scn));
    flashback_state_ = LogFlashbackState::FLASHBACK_RECONFIRM;
  } else if (OB_FAIL(get_block_id_by_scn_for_flashback_(flashback_scn, start_block))
             && OB_ENTRY_NOT_EXIST != ret) {
    PALF_LOG(ERROR, "get_block_id_by_scn_for_flashback_ failed", K(ret), KPC(this), K(flashback_scn));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    PALF_LOG(WARN, "there is no log on disk, flashback successfully", K(ret), KPC(this), K(flashback_scn));
    flashback_state_ = LogFlashbackState::FLASHBACK_SUCCESS;
  } else if (FALSE_IT(start_lsn_of_block.val_ = start_block * PALF_BLOCK_SIZE)) {
  } else if (OB_FAIL(log_engine_.begin_flashback(start_lsn_of_block))) {
    PALF_LOG(ERROR, "LogEngine begin_flashback failed", K(ret), K(start_lsn_of_block));
  } else if (OB_FAIL(do_flashback_(start_lsn_of_block, flashback_scn))) {
    PALF_LOG(ERROR, "do_flashback_ failed", K(ret), K(start_lsn_of_block), K(flashback_scn),
				"end_scn", get_end_scn());
  } else if (OB_FAIL(log_engine_.end_flashback(start_lsn_of_block))) {
    PALF_LOG(ERROR, "LogEngine end_flashback failed", K(ret), K(start_lsn_of_block), K(flashback_scn));
  } else {
    flashback_state_ = LogFlashbackState::FLASHBACK_SUCCESS;
    PALF_LOG(INFO, "inner_flashback success", K(ret), KPC(this), K(flashback_scn));
  }
  flashback_state_ = (OB_FAIL(ret))? LogFlashbackState::FLASHBACK_FAILED: flashback_state_;
  return ret;
}

// TODO by yunlong: this function needs refactoring in 4.2.0.0
int PalfHandleImpl::get_ack_info_array(LogMemberAckInfoList &ack_info_array,
                                       common::GlobalLearnerList &degraded_list) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  const bool is_leader = (state_mgr_.is_leader_active() ||
      (state_mgr_.is_leader_reconfirm() && reconfirm_.can_do_degrade()));
  common::ObMember arb_member;
  config_mgr_.get_arbitration_member(arb_member);
  const bool need_degrade_or_upgrade = arb_member.is_valid();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not inited!!!", K(ret));
  } else if (false == is_leader) {
    ret = OB_NOT_MASTER;
  } else if (false == need_degrade_or_upgrade) {
    // do not need degrade or upgrade, skip
  } else if (OB_FAIL(sw_.get_ack_info_array(ack_info_array))) {
    PALF_LOG(WARN, "get_ack_info_array failed", K(ret), KPC(this));
  } else if (OB_FAIL(config_mgr_.get_degraded_learner_list(degraded_list))) {
    PALF_LOG(WARN, "get_degraded_learner_list failed", K(ret), KPC(this));
  }
  return ret;
}

// NB: the timestamp of LogGroupEntry is max timestamp, therefore, if the timestamp of
// LogGroupEntry is greater than 'flashback_scn', we can not ignore it.
//
// step1: read each LogGroupEntry before  'flashback_scn' and append it to tmp_block;
// step2: cut first LogGroupEntry whose timestamp is greater than 'flashback_scn'.
// step3: modify sw_ with new PalfBaseInfo.
int PalfHandleImpl::do_flashback_(const LSN &start_lsn, const share::SCN &flashback_scn)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  char *last_log_buf = NULL;
  int64_t last_log_buf_len = 0;
  LSN last_log_start_lsn;
  if (OB_FAIL(read_and_append_log_group_entry_before_ts_(start_lsn, flashback_scn, last_log_buf,
          last_log_buf_len, last_log_start_lsn, palf_base_info))) {
    PALF_LOG(ERROR, "read_and_append_log_group_entry_before_ts_ failed", K(ret),
        KPC(this), K(start_lsn));
    // when there is no log need be cut, last_log_buf is NULL.
  } else if (NULL != last_log_buf &&
      OB_FAIL(cut_last_log_and_append_it_(last_log_buf, last_log_buf_len, last_log_start_lsn,
          flashback_scn, palf_base_info))) {
    PALF_LOG(ERROR, "cut_last_log_and_append_it_ failed", K(ret), KPC(this), KP(last_log_buf),
        K(last_log_buf_len), K(last_log_start_lsn), K(flashback_scn), K(palf_base_info));
  } else if (OB_FAIL(sw_.flashback(palf_base_info, palf_id_, allocator_))) {
    PALF_LOG(ERROR, "do_flashback_ failed", K(ret), K(start_lsn), K(palf_base_info));
  } else {
    PALF_LOG(INFO, "do_flashback_ success", K(ret), KPC(this), K(start_lsn), K(flashback_scn),
    K(palf_base_info));
  }
  if (NULL != last_log_buf) {
    ob_free(last_log_buf);
  }
  return ret;
}

// NB: if 'flashback_scn' is 10, means that the LogGroupEntry whose timestamp is smaller than
// or equal to 10 need be saved, and the first LogGroupEntry whose timestame is greatet than 10
// need to be cut.
int PalfHandleImpl::read_and_append_log_group_entry_before_ts_(
    const LSN &start_lsn,
    const share::SCN &flashback_scn,
    char*& last_log_buf,
    int64_t &last_log_buf_len,
    LSN &last_log_start_lsn,
    PalfBaseInfo &palf_base_info)
{
  // Each LogGroupEntry can contain many LogEntrys, and each LogEntry has its' own timestamp.
  // Assume the timestamp of one LogEntry is flashback_scn, and it belongs to 'curr_group_entry',
  // if the LogEntry is last LogEntry of 'curr_group_entry', the 'prev_entry_header' is the LogGroupEntryHeader
  // of 'curr_group_entry', and we don't need cut 'curr_group_entry'. otherwise, the 'prev_entry_header'
  // is the prev LogGroupEntryHeader  of 'curr_group_entry', 'curr_group_entry' need be cut.
  int ret = OB_SUCCESS;
  LogGroupEntryHeader prev_entry_header;
  LogGroupEntry curr_group_entry;
  LSN prev_log_lsn, curr_log_lsn;
  PalfGroupBufferIterator iterator;
  auto get_file_end_lsn = [&](){
    LSN max_flushed_end_lsn;
    (void)sw_.get_max_flushed_end_lsn(max_flushed_end_lsn);
    return max_flushed_end_lsn;
  };

  last_log_buf = NULL;
  ReadBufGuard read_buf_guard("Palf", MAX_LOG_BUFFER_SIZE);
  if (!read_buf_guard.read_buf_.is_valid()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "allocate memory failed", KPC(this));
  } else if (OB_FAIL(iterator.init(start_lsn, get_file_end_lsn, log_engine_.get_log_storage()))) {
    PALF_LOG(WARN, "iterator init failed", K(ret), KPC(this), K(start_lsn), K(flashback_scn));
  } else {
    const int64_t read_buf_len = read_buf_guard.read_buf_.buf_len_;
    char *&read_buf = read_buf_guard.read_buf_.buf_;
    const char *buffer = NULL;
    bool last_log_need_be_cut = false;
    bool need_next = true;
    common::ObTimeGuard time_guard("single_flashback", 1 * 1000 * 1000);

    int64_t total_log_num = 0;
    // step1. iterate the block and append log which the scn is smaller than or equal to flashback_scn
    //        to new block.
    while (OB_SUCC(ret)) {
      int64_t log_num = 0;
      int64_t read_buf_pos = 0;
      SCN log_scn;
      LSN lsn;
      // batch append log to storage
      while (OB_SUCC(ret)) {
        if (need_next && OB_FAIL(iterator.next())) {
          PALF_LOG(WARN, "iterator next failed", K(ret), KPC(this), K(iterator));
        } else if (OB_FAIL(iterator.get_entry(buffer, curr_group_entry, curr_log_lsn))) {
          PALF_LOG(ERROR, "get_entry failed", K(ret), KPC(this), K(iterator));
        } else if (flashback_scn < curr_group_entry.get_scn()) {
          last_log_need_be_cut = true;
          ret = OB_ITER_END;
          CLOG_LOG(INFO, "last log need be cut", K(flashback_scn), K(curr_group_entry), K(iterator));
        } else if (FALSE_IT(prev_entry_header = curr_group_entry.get_header())
                   || FALSE_IT(prev_log_lsn = curr_log_lsn)) {
        } else if (curr_group_entry.get_serialize_size() + read_buf_pos > read_buf_len) {
          need_next = false;
          PALF_LOG(WARN, "can batch log because of buffer length", K(ret), KPC(this), K(read_buf_pos), K(read_buf_len), K(curr_group_entry));
          break;
        } else if (FALSE_IT(MEMCPY(read_buf+read_buf_pos, buffer, curr_group_entry.get_serialize_size()))) {
        } else {
          // NB: In first round of while, need init scn and lsn to curr_group_entry
          if (0 == read_buf_pos) {
            if (OB_FAIL(curr_group_entry.get_log_min_scn(log_scn))) {
              PALF_LOG(ERROR, "get_log_min_ts failed", K(ret), KPC(this), K(iterator), K(prev_entry_header));
            } else {
              lsn = curr_log_lsn;
              PALF_LOG(INFO, "this is the first round of while", K(ret), KPC(this), K(iterator), K(prev_entry_header));
            }
          }
          read_buf_pos += curr_group_entry.get_serialize_size();
          need_next = true;
          log_num++;
        }
      }
      total_log_num += log_num;
      LogWriteBuf write_buf;
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        PALF_LOG(ERROR, "unexpected error", K(ret), KPC(this), K(log_num), K(total_log_num), K(iterator));
      } else if (read_buf_pos == 0) {
        PALF_LOG(INFO, "no need append log because read_buf_pos is zero", K(ret), KPC(this), K(iterator));
      } else if (OB_TMP_FAIL(write_buf.push_back(read_buf, read_buf_pos))) {
        PALF_LOG(ERROR, "push_back into write_buf failed", K(ret), KPC(this), K(write_buf), K(iterator), K(read_buf_pos));
      } else if (OB_TMP_FAIL(log_engine_.append_log(lsn, write_buf, log_scn))) {
        PALF_LOG(ERROR, "append_log failed", K(ret), KPC(this), K(write_buf), K(iterator), K(read_buf_pos));
      } else {
        read_buf_pos = 0;
        PALF_LOG(INFO, "append_log success", K(ret), K(curr_log_lsn), K(curr_group_entry), K(write_buf), K(flashback_scn),
            K(log_num));
      }
    }
    time_guard.click("while");
    auto alloc_memory_until_success = [&last_log_buf, &last_log_buf_len]() {
      while (NULL ==
          (last_log_buf = static_cast<char*>(mtl_malloc(last_log_buf_len, "PalfHandleImpl")))) {
        PALF_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "alloc memory for last_log_buf in flashback failed", K(last_log_buf_len));
        usleep(1000);
      };
    };
    // step2. construct new palf base info.
    if (OB_ITER_END == ret) {
      int tmp_ret = OB_SUCCESS;
      int64_t pos = 0;
      if (OB_TMP_FAIL(construct_palf_base_info_for_flashback_(
        start_lsn, flashback_scn, prev_log_lsn, prev_entry_header, palf_base_info))) {
				PALF_LOG(ERROR, "construct_palf_base_info_for_flashback_ failed", K(ret), KPC(this), K(curr_group_entry),
           K(curr_log_lsn));
      } else if (false == last_log_need_be_cut) {
        PALF_LOG(INFO, "last log no need be cut", K(ret), KPC(this), K(iterator), K(curr_group_entry),
            K(prev_entry_header));
      } else if (FALSE_IT(last_log_buf_len = curr_group_entry.get_group_entry_size())
                 || FALSE_IT(last_log_start_lsn = curr_log_lsn)) {
      } else if (FALSE_IT(alloc_memory_until_success())){
      } else if (OB_TMP_FAIL(curr_group_entry.serialize(last_log_buf, last_log_buf_len, pos))) {
        PALF_LOG(ERROR, "curr_group_entry serialize failed", K(ret));
      } else {
        PALF_LOG(INFO, "read_and_append_log_group_entry_before_ts_ success", K(ret), KPC(this),
            K(palf_base_info), K(curr_group_entry), K(curr_log_lsn), K(flashback_scn),
            K(total_log_num), K(time_guard));
      }
      time_guard.click("after while");
      ret = tmp_ret;
    }
  }
  return ret;
}

int PalfHandleImpl::cut_last_log_and_append_it_(char *last_log_buf,
                                                const int64_t last_log_buf_len,
                                                const LSN &last_log_start_lsn,
                                                const share::SCN &flashback_scn,
                                                PalfBaseInfo &in_out_palf_base_info)
{
  int ret = OB_SUCCESS;
  LogGroupEntry new_last_log;
  LogWriteBuf write_buf;
  int64_t pos = 0;
  SCN new_last_scn;
  if (OB_FAIL(new_last_log.deserialize(last_log_buf, last_log_buf_len, pos))) {
    PALF_LOG(ERROR, "new_last_log deserialize failed", K(ret), KPC(this), K(pos));
  } else if (false == new_last_log.check_integrity()) {
    ret = OB_INVALID_DATA;
    PALF_LOG(ERROR, "invalid data", K(ret), K(new_last_log));
  } else if (OB_FAIL(new_last_log.truncate(flashback_scn, in_out_palf_base_info.prev_log_info_.accum_checksum_))) {
    PALF_LOG(ERROR, "new_last_log truncate failed", K(ret), K(flashback_scn), K(in_out_palf_base_info));
  } else if (0 == new_last_log.get_data_len()) {
    PALF_LOG(INFO, "last log no need be cut", K(new_last_log));
  } else if (FALSE_IT(pos = 0)
             || OB_FAIL(new_last_log.serialize(last_log_buf, last_log_buf_len, pos))) {
    PALF_LOG(ERROR, "new_last_log serialize failed", K(ret), KPC(this), K(new_last_log), K(pos));
  } else if (OB_FAIL(write_buf.push_back(last_log_buf, new_last_log.get_group_entry_size()))) {
    PALF_LOG(ERROR, "wite_buf push back failed", K(ret), KPC(this), K(new_last_log));
  } else if (OB_FAIL(new_last_log.get_log_min_scn(new_last_scn))) {
    PALF_LOG(ERROR, "get_log_min_ts failed", K(ret), KPC(this), K(new_last_log));
  } else if (OB_FAIL(log_engine_.append_log(last_log_start_lsn, write_buf, new_last_scn))) {
    PALF_LOG(ERROR, "append_log failed", K(ret), KPC(this), K(last_log_start_lsn));
  } else {
    LogInfo &prev_log_info = in_out_palf_base_info.prev_log_info_;
    const LogGroupEntryHeader &last_log_header = new_last_log.get_header();
    prev_log_info.log_id_ = last_log_header.get_log_id();
    prev_log_info.scn_ = last_log_header.get_max_scn();
    prev_log_info.accum_checksum_ = last_log_header.get_accum_checksum();
    prev_log_info.log_proposal_id_ = last_log_header.get_log_proposal_id();
    prev_log_info.lsn_ = last_log_start_lsn;
    in_out_palf_base_info.curr_lsn_ = last_log_start_lsn + new_last_log.get_group_entry_size();
    PALF_LOG(INFO, "cut_last_log_and_append_it_ success", K(ret), K(in_out_palf_base_info), K(new_last_log), K(write_buf));
  }
  return ret;
}

int PalfHandleImpl::stat(PalfStat &palf_stat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LSN last_rebuild_lsn;
    do {
      SpinLockGuard guard(last_rebuild_meta_info_lock_);
      last_rebuild_lsn = last_rebuild_lsn_;
    } while (0);

    // following members should be protected by rlock_
    RLockGuard guard(lock_);
    block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
    SCN min_block_min_scn;
    ObRole curr_role = INVALID_ROLE;
    ObReplicaState curr_state = INVALID_STATE;
    palf_stat.self_ = self_;
    palf_stat.palf_id_ = palf_id_;
    state_mgr_.get_role_and_state(curr_role, curr_state);
    palf_stat.role_ = (LEADER == curr_role && curr_state == ACTIVE)? LEADER: FOLLOWER;
    palf_stat.log_proposal_id_ = state_mgr_.get_proposal_id();
    (void)config_mgr_.get_config_version(palf_stat.config_version_);
    (void)mode_mgr_.get_access_mode(palf_stat.mode_version_, palf_stat.access_mode_);
    (void)config_mgr_.get_curr_member_list(palf_stat.paxos_member_list_, palf_stat.paxos_replica_num_);
    (void)config_mgr_.get_arbitration_member(palf_stat.arbitration_member_);
    (void)config_mgr_.get_degraded_learner_list(palf_stat.degraded_list_);
    (void)config_mgr_.get_global_learner_list(palf_stat.learner_list_);
    palf_stat.allow_vote_ = state_mgr_.is_allow_vote();
    palf_stat.replica_type_ = state_mgr_.get_replica_type();
    palf_stat.base_lsn_ = log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_;
    (void)log_engine_.get_min_block_info(min_block_id, min_block_min_scn);
    palf_stat.begin_lsn_ = LSN(min_block_id * PALF_BLOCK_SIZE);
    palf_stat.begin_scn_ = min_block_min_scn;
    palf_stat.end_lsn_ = get_end_lsn();
    palf_stat.end_scn_ = get_end_scn();
    palf_stat.max_lsn_ = get_max_lsn();
    palf_stat.max_scn_ = get_max_scn();
    palf_stat.is_need_rebuild_ = is_need_rebuild(palf_stat.end_lsn_, last_rebuild_lsn);
    palf_stat.is_in_sync_ = (LEADER == palf_stat.role_)? true: cached_is_in_sync_;
    PALF_LOG(TRACE, "PalfHandleImpl stat", K(palf_stat));
  }
  return OB_SUCCESS;
}

int PalfHandleImpl::update_palf_stat()
{
  int ret = OB_SUCCESS;
  bool is_in_sync = false;
  bool is_use_sync_cache = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    is_in_sync_(is_in_sync, is_use_sync_cache);
    if (false == is_use_sync_cache) {
      cached_is_in_sync_ = is_in_sync;
    }
  }
  return OB_SUCCESS;
}

int PalfHandleImpl::try_lock_config_change(int64_t lock_owner, int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(lock_owner <= 0 || timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(lock_owner), K(timeout_us));
  } else if (OB_TMP_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_data_version))) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported when data version is invalid", KR(ret), KPC(this),
        K(lock_owner), K(timeout_us));
  } else if (tenant_data_version < DATA_VERSION_4_2_0_0) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported with current data version", KR(ret), K(tenant_data_version),
        KPC(this), K(lock_owner), K(timeout_us));
  } else {
    LogConfigChangeArgs args(lock_owner, ConfigChangeLockType::LOCK_PAXOS_MEMBER_CHANGE, TRY_LOCK_CONFIG_CHANGE);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "try_lock_config_change failed", KR(ret), KPC(this), K(lock_owner));
    } else {
      PALF_EVENT("try_lock_config_change success", palf_id_, KR(ret), KPC(this), K(lock_owner));
    }
  }
  return ret;
}

int PalfHandleImpl::unlock_config_change(int64_t lock_owner, int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(lock_owner <= 0 || timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(lock_owner), K(timeout_us));
  } else if (OB_TMP_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_data_version))) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported when data version is invalid", KR(ret), KPC(this),
        K(lock_owner), K(timeout_us));
  } else if (tenant_data_version < DATA_VERSION_4_2_0_0) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported with current data version", KR(ret), K(tenant_data_version),
        KPC(this), K(lock_owner), K(timeout_us));
  } else {
    LogConfigChangeArgs args(lock_owner, ConfigChangeLockType::LOCK_NOTHING, UNLOCK_CONFIG_CHANGE);
    if (OB_FAIL(one_stage_config_change_(args, timeout_us))) {
      PALF_LOG(WARN, "unlock_config_change failed", KR(ret), KPC(this), K(lock_owner));
    } else {
      PALF_EVENT("unlock_config_change success", palf_id_, KR(ret), KPC(this), K(lock_owner));
    }
  }
  return ret;
}

int PalfHandleImpl::get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl has not inited", K(ret));
  } else if (OB_FAIL(config_mgr_.get_config_change_lock_stat(lock_owner, is_locked))) {
    PALF_LOG(WARN, "get_curr_member_list failed", K(ret), KPC(this));
  } else {}
  return ret;
}

void PalfHandleImpl::is_in_sync_(bool &is_log_sync, bool &is_use_cache)
{
  int ret = OB_SUCCESS;
  SCN leader_max_scn;
  LSN leader_end_lsn;
  is_log_sync = false;
  is_use_cache = false;
  share::SCN local_max_scn = sw_.get_max_scn();
  LSN local_end_lsn;

  if (state_mgr_.get_leader() == self_) {
    is_log_sync = true;
  } else if (false == local_max_scn.is_valid()) {
  } else if (palf_reach_time_interval(PALF_LOG_SYNC_DELAY_THRESHOLD_US, last_check_sync_time_us_)) {
    // if reachs time interval, get max_scn of leader with sync RPC
    if (OB_FAIL(get_leader_max_scn_(leader_max_scn, leader_end_lsn))) {
      CLOG_LOG(WARN, "get_palf_max_scn failed", K(ret), K_(self), K_(palf_id));
      last_check_sync_time_us_ = OB_INVALID_TIMESTAMP;
    } else if (leader_max_scn.is_valid() && leader_end_lsn.is_valid()) {
      sw_.get_committed_end_lsn(local_end_lsn);
      const bool is_scn_sync = (leader_max_scn.convert_to_ts() - local_max_scn.convert_to_ts() <= PALF_LOG_SYNC_DELAY_THRESHOLD_US);
      const bool is_log_size_sync = (leader_end_lsn - local_end_lsn) < 2 * PALF_BLOCK_SIZE;
      // Note: do not consider the gap of LSN (120 MB is too much to catch up for migration dest)
      UNUSED(is_log_size_sync);
      is_log_sync = is_scn_sync;
    }
  } else {
    is_use_cache = true;
  }

  const bool is_in_sync = (is_use_cache) ? cached_is_in_sync_ : is_log_sync;
  const int64_t log_print_interval = (is_in_sync)? 600 * 1000 * 1000: 10 * 1000 * 1000;
  if (palf_reach_time_interval(log_print_interval, last_print_in_sync_time_us_)) {
    CLOG_LOG(INFO, "is_in_sync", K(ret), K_(palf_id), K(is_in_sync), K(is_use_cache),
      "remote_check_is_log_sync", is_log_sync, K(leader_max_scn), K(local_max_scn));
  }
}

int PalfHandleImpl::get_leader_max_scn_(SCN &max_scn, LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader;
  LogGetStatReq req(LogGetStatType::GET_LEADER_MAX_SCN);
  LogGetStatResp resp;
  bool need_renew_leader = false;

  max_scn.reset();
  end_lsn.reset();
  // use lc_cb_ in here without rlock is safe, because we don't reset lc_cb_
  // until this PalfHandleImpl is destoryed.
  if (OB_FAIL(plugins_.nonblock_get_leader(palf_id_, leader))) {
    CLOG_LOG(WARN, "get_leader failed", K(ret), K_(self), K_(palf_id));
    need_renew_leader = true;
  } else if (false == leader.is_valid()) {
    need_renew_leader = true;
  } else if (OB_FAIL(log_engine_.submit_get_stat_req(leader, PALF_SYNC_RPC_TIMEOUT_US, req, resp))) {
    CLOG_LOG(WARN, "get_palf_max_scn failed", K(ret), K_(palf_id));
    need_renew_leader = true;
  } else {
    max_scn = resp.max_scn_;
    end_lsn = resp.end_lsn_;
  }
  if (need_renew_leader && palf_reach_time_interval(500 * 1000, last_renew_loc_time_us_)) {
    (void) plugins_.nonblock_renew_leader(palf_id_);
  }
  return ret;
}

void PalfHandleImpl::report_set_initial_member_list_(const int64_t paxos_replica_num, const common::ObMemberList &member_list)
{
  ObSqlString member_list_buf;
  (void) member_list_to_string(member_list, member_list_buf);
  plugins_.record_set_initial_member_list_event(palf_id_, paxos_replica_num, member_list_buf.ptr());
}
void PalfHandleImpl::report_set_initial_member_list_with_arb_(const int64_t paxos_replica_num, const common::ObMemberList &member_list, const common::ObMember &arb_member)
{
  PALF_REPORT_INFO_KV(K(arb_member));
  ObSqlString member_list_buf;
  (void) member_list_to_string(member_list, member_list_buf);
  plugins_.record_set_initial_member_list_event(palf_id_, paxos_replica_num, member_list_buf.ptr(), EXTRA_INFOS);
}
void PalfHandleImpl::report_force_set_as_single_replica_(const int64_t prev_replica_num, const int64_t curr_replica_num, const ObMember &member)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  ObSqlString member_buf;
  member_to_string(member, member_buf);
  PALF_REPORT_INFO_KV("member", member_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::FORCE_SINGLE_MEMBER),
      palf_id_, config_version, prev_replica_num, curr_replica_num, EXTRA_INFOS);
}

void PalfHandleImpl::report_change_replica_num_(const int64_t prev_replica_num, const int64_t curr_replica_num, const common::ObMemberList &member_list)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  ObSqlString member_list_buf;
  (void) member_list_to_string(member_list, member_list_buf);
  PALF_REPORT_INFO_KV("member_list", member_list_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::CHANGE_REPLICA_NUM),
      palf_id_, config_version, prev_replica_num, curr_replica_num, EXTRA_INFOS);
}
void PalfHandleImpl::report_add_member_(const int64_t prev_replica_num, const int64_t curr_replica_num, const common::ObMember &added_member)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  ObMemberList curr_member_list;
  int64_t replica_num;
  config_mgr_.get_curr_member_list(curr_member_list, replica_num);
  ObSqlString member_list_buf;
  (void) member_list_to_string(curr_member_list, member_list_buf);
  ObSqlString member_buf;
  member_to_string(added_member, member_buf);
  PALF_REPORT_INFO_KV(
      "added_member", member_buf,
      "member_list", member_list_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::ADD_MEMBER),
      palf_id_, config_version, prev_replica_num, curr_replica_num, EXTRA_INFOS);
}
void PalfHandleImpl::report_remove_member_(const int64_t prev_replica_num, const int64_t curr_replica_num, const common::ObMember &removed_member)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  ObMemberList curr_member_list;
  int64_t replica_num;
  config_mgr_.get_curr_member_list(curr_member_list, replica_num);
  ObSqlString member_list_buf;
  (void) member_list_to_string(curr_member_list, member_list_buf);
  ObSqlString member_buf;
  member_to_string(removed_member, member_buf);
  PALF_REPORT_INFO_KV(
      "removed_member", member_buf,
      "member_list", member_list_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::REMOVE_MEMBER),
      palf_id_, config_version, prev_replica_num, curr_replica_num, EXTRA_INFOS);
}
void PalfHandleImpl::report_replace_member_(const common::ObMember &added_member,
                                            const common::ObMember &removed_member,
                                            const common::ObMemberList &member_list,
                                            const char *event_name)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  ObSqlString member_list_buf;
  (void) member_list_to_string(member_list, member_list_buf);
  ObSqlString added_member_buf;
  ObSqlString removed_member_buf;
  member_to_string(added_member, added_member_buf);
  member_to_string(removed_member, removed_member_buf);
  PALF_REPORT_INFO_KV(
      "added_member", added_member_buf,
      "removed_member", removed_member_buf,
      "member_list", member_list_buf);
  int64_t curr_replica_num;
  (void) config_mgr_.get_replica_num(curr_replica_num);
  plugins_.record_reconfiguration_event(event_name, palf_id_, config_version, curr_replica_num, curr_replica_num, EXTRA_INFOS);
}
void PalfHandleImpl::report_add_learner_(const common::ObMember &added_learner)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  common::ObMemberList curr_member_list;
  int64_t curr_replica_num;
  (void) config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num);
  ObSqlString member_list_buf;
  (void) member_list_to_string(curr_member_list, member_list_buf);
  ObSqlString member_buf;
  member_to_string(added_learner, member_buf);
  PALF_REPORT_INFO_KV(
      "added_learner", member_buf,
      "member_list", member_list_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::ADD_LEARNER),
      palf_id_, config_version, curr_replica_num, curr_replica_num, EXTRA_INFOS);
}
void PalfHandleImpl::report_remove_learner_(const common::ObMember &removed_learner)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  common::ObMemberList curr_member_list;
  int64_t curr_replica_num;
  (void) config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num);
  ObSqlString member_list_buf;
  (void) member_list_to_string(curr_member_list, member_list_buf);
  ObSqlString member_buf;
  member_to_string(removed_learner, member_buf);
  PALF_REPORT_INFO_KV(
      "removed_learner", member_buf,
      "member_list", member_list_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::REMOVE_LEARNER),
      palf_id_, config_version, curr_replica_num, curr_replica_num, EXTRA_INFOS);
}
void PalfHandleImpl::report_add_arb_member_(const common::ObMember &added_arb_member)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  common::ObMemberList curr_member_list;
  int64_t curr_replica_num;
  (void) config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num);
  ObSqlString member_list_buf;
  (void) member_list_to_string(curr_member_list, member_list_buf);
  char member_buf_[MAX_SINGLE_MEMBER_LENGTH] = {'\0'};
  ObSqlString member_buf;
  member_to_string(added_arb_member, member_buf);
  PALF_REPORT_INFO_KV(
      "added_arb_member", member_buf,
      "member_list", member_list_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::ADD_ARB_MEMBER),
      palf_id_, config_version, curr_replica_num, curr_replica_num, EXTRA_INFOS);
}
void PalfHandleImpl::report_remove_arb_member_(const common::ObMember &removed_arb_member)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  common::ObMemberList curr_member_list;
  int64_t curr_replica_num;
  (void) config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num);
  ObSqlString member_list_buf;
  (void) member_list_to_string(curr_member_list, member_list_buf);
  ObSqlString member_buf;
  member_to_string(removed_arb_member, member_buf);
  PALF_REPORT_INFO_KV(
      "removed_arb_member", member_buf,
      "member_list", member_list_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::REMOVE_ARB_MEMBER),
      palf_id_, config_version, curr_replica_num, curr_replica_num, EXTRA_INFOS);
}

void PalfHandleImpl::report_switch_learner_to_acceptor_(const common::ObMember &learner)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  ObMemberList curr_member_list;
  int64_t curr_replica_num;
  (void) config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num);
  ObSqlString member_list_buf;
  (void) member_list_to_string(curr_member_list, member_list_buf);
  ObSqlString member_buf;
  member_to_string(learner, member_buf);
  PALF_REPORT_INFO_KV(
      "member", member_buf,
      "curr_member_list", member_list_buf,
      "curr_replica_num", curr_replica_num);
  char replica_readonly_name_[common::MAX_REPLICA_TYPE_LENGTH];
  char replica_full_name_[common::MAX_REPLICA_TYPE_LENGTH];
  replica_type_to_string(ObReplicaType::REPLICA_TYPE_READONLY, replica_readonly_name_, sizeof(replica_readonly_name_));
  replica_type_to_string(ObReplicaType::REPLICA_TYPE_FULL, replica_full_name_, sizeof(replica_full_name_));
  plugins_.record_replica_type_change_event(palf_id_, config_version, replica_readonly_name_, replica_full_name_, EXTRA_INFOS);
}

void PalfHandleImpl::report_switch_acceptor_to_learner_(const common::ObMember &acceptor)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  ObMemberList curr_member_list;
  int64_t curr_replica_num;
  (void) config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num);
  ObSqlString member_list_buf;
  (void) member_list_to_string(curr_member_list, member_list_buf);
  ObSqlString member_buf;
  member_to_string(acceptor, member_buf);
  PALF_REPORT_INFO_KV(
      "member", member_buf,
      "curr_member_list", member_list_buf,
      "curr_replica_num", curr_replica_num);
  char replica_readonly_name_[common::MAX_REPLICA_TYPE_LENGTH];
  char replica_full_name_[common::MAX_REPLICA_TYPE_LENGTH];
  replica_type_to_string(ObReplicaType::REPLICA_TYPE_READONLY, replica_readonly_name_, sizeof(replica_readonly_name_));
  replica_type_to_string(ObReplicaType::REPLICA_TYPE_FULL, replica_full_name_, sizeof(replica_full_name_));
  plugins_.record_replica_type_change_event(palf_id_, config_version, replica_full_name_, replica_readonly_name_, EXTRA_INFOS);
}

void PalfHandleImpl::report_replace_learners_(const common::ObMemberList &added_learners,
                                              const common::ObMemberList &removed_learners)
{
  LogConfigVersion config_version;
  (void) config_mgr_.get_config_version(config_version);
  common::ObMemberList curr_member_list;
  int64_t curr_replica_num;
  (void) config_mgr_.get_curr_member_list(curr_member_list, curr_replica_num);
  ObSqlString added_learners_buf, removed_learners_buf;
  member_list_to_string(added_learners, added_learners_buf);
  member_list_to_string(removed_learners, removed_learners_buf);
  PALF_REPORT_INFO_KV(
      "added_learners", added_learners_buf,
      "removed_learners", removed_learners_buf);
  plugins_.record_reconfiguration_event(LogConfigChangeType2Str(LogConfigChangeType::REPLACE_LEARNERS),
      palf_id_, config_version, curr_replica_num, curr_replica_num, EXTRA_INFOS);
}

bool PalfHandleImpl::check_need_hook_fetch_log_(const FetchLogType fetch_type, const LSN &start_lsn)
{
  bool bool_ret = false;
  const int64_t rebuild_replica_log_lag_threshold = palf_env_impl_->get_rebuild_replica_log_lag_threshold();
  if (rebuild_replica_log_lag_threshold > 0 && (FETCH_LOG_FOLLOWER == fetch_type)) {
    LSN max_lsn = get_max_lsn();
    LSN base_lsn = get_base_lsn_used_for_block_gc();
    bool_ret = (start_lsn < base_lsn) && ((max_lsn - start_lsn) > rebuild_replica_log_lag_threshold);

    if (bool_ret && palf_reach_time_interval(1 * 1000 * 1000L, last_hook_fetch_log_time_us_)) {
      PALF_LOG(INFO, "hook fetch_log because of rebuild_replica_log_lag_threshold", K(palf_id_),
               K(rebuild_replica_log_lag_threshold), K(start_lsn), K(max_lsn), K(base_lsn));
    }
  }
  return bool_ret;
}

PalfStat::PalfStat()
    : self_(),
      palf_id_(INVALID_PALF_ID),
      role_(common::ObRole::INVALID_ROLE),
      log_proposal_id_(INVALID_PROPOSAL_ID),
      config_version_(),
      mode_version_(INVALID_PROPOSAL_ID),
      access_mode_(AccessMode::INVALID_ACCESS_MODE),
      paxos_member_list_(),
      paxos_replica_num_(-1),
      arbitration_member_(),
      degraded_list_(),
      allow_vote_(true),
      replica_type_(LogReplicaType::INVALID_REPLICA),
      begin_lsn_(),
      begin_scn_(),
      base_lsn_(),
      end_lsn_(),
      end_scn_(),
      max_lsn_(),
      max_scn_(),
      is_in_sync_(false),
      is_need_rebuild_(false) { }

bool PalfStat::is_valid() const
{
  return self_.is_valid() &&
          palf_id_ != INVALID_PALF_ID &&
          role_ != common::ObRole::INVALID_ROLE;
}

void PalfStat::reset()
{
  self_.reset();
  palf_id_ = INVALID_PALF_ID;
  role_ = common::ObRole::INVALID_ROLE;
  log_proposal_id_ = INVALID_PROPOSAL_ID;
  config_version_.reset();
  mode_version_ = INVALID_PROPOSAL_ID;
  access_mode_ = AccessMode::INVALID_ACCESS_MODE;
  paxos_member_list_.reset();
  paxos_replica_num_ = -1;
  learner_list_.reset();
  arbitration_member_.reset();
  degraded_list_.reset();
  allow_vote_ = true;
  replica_type_ = LogReplicaType::INVALID_REPLICA;
  begin_lsn_.reset();
  begin_scn_.reset();
  base_lsn_.reset();
  end_lsn_.reset();
  end_scn_.reset();
  max_lsn_.reset();
  max_scn_.reset();
  is_in_sync_ = false;
  is_need_rebuild_ = false;
}

int PalfHandleImpl::read_data_from_buffer(const LSN &read_begin_lsn,
                                          const int64_t in_read_size,
                                          char *buf,
                                          int64_t &out_read_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!read_begin_lsn.is_valid() || in_read_size <= 0 || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K(read_begin_lsn), K(in_read_size),
        KP(buf));
  } else if (OB_FAIL(sw_.read_data_from_buffer(read_begin_lsn, in_read_size, buf, out_read_size))) {
    if (OB_ERR_OUT_OF_LOWER_BOUND != ret) {
      PALF_LOG(WARN, "read_data_from_buffer failed", K(ret), K_(palf_id), K(read_begin_lsn),
          K(in_read_size));
    }
  } else {
    PALF_LOG(TRACE, "read_data_from_buffer success", K(ret), K_(palf_id), K(read_begin_lsn),
        K(in_read_size), K(out_read_size));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(PalfStat, self_, palf_id_, role_, log_proposal_id_, config_version_,
  mode_version_, access_mode_, paxos_member_list_, paxos_replica_num_, allow_vote_,
  replica_type_, begin_lsn_, begin_scn_, base_lsn_, end_lsn_, end_scn_, max_lsn_, max_scn_,
  arbitration_member_, degraded_list_, is_in_sync_, is_need_rebuild_, learner_list_);

} // end namespace palf
} // end namespace oceanbase
