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

#ifndef OCEANBASE_PALF_LOG_SERVICE_
#define OCEANBASE_PALF_LOG_SERVICE_

#include "common/ob_member_list.h"
#include "common/ob_role.h"
#include "lib/hash/ob_link_hashmap_deps.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "election/interface/election_priority.h"
#include "election/interface/election_msg_handler.h"
#include "election/message/election_message.h"
#include "election/algorithm/election_impl.h"
#include "share/scn.h"
#include "palf_callback_wrapper.h"
#include "log_engine.h"                      // LogEngine
#include "log_meta.h"
#include "log_cache.h"
#include "lsn.h"
#include "log_config_mgr.h"
#include "log_mode_mgr.h"
#include "log_reconfirm.h"
#include "log_sliding_window.h"
#include "log_state_mgr.h"
#include "log_io_task_cb_utils.h"
#include "palf_options.h"
#include "palf_iterator.h"

namespace oceanbase
{
namespace common
{
class ObMember;
class ObILogAllocator;
}
namespace palf
{
class FlushLogCbCtx;
class ObMemberChangeCtx;
class LSN;
class FetchLogEngine;
class FlushLogCbCtx;
class TruncateLogCbCtx;
class FlushMetaCbCtx;
class LogIOFlushLogTask;
class LogIOTruncateLogTask;
class LogIOFlushMetaTask;
class ReadBuf;
class LogWriteBuf;
class LogIOWorker;
class LogRpc;
class IPalfEnvImpl;

struct PalfStat {
  OB_UNIS_VERSION(1);
public:
  PalfStat();
  ~PalfStat() { reset(); }
  bool is_valid() const;
  void reset();

  common::ObAddr self_;
  int64_t palf_id_;
  common::ObRole role_;
  int64_t log_proposal_id_;
  LogConfigVersion config_version_;
  int64_t mode_version_;
  AccessMode access_mode_;
  ObMemberList paxos_member_list_;
  int64_t paxos_replica_num_;
  common::ObMember arbitration_member_;
  common::GlobalLearnerList degraded_list_;
  common::GlobalLearnerList learner_list_;
  bool allow_vote_;
  LogReplicaType replica_type_;
  LSN begin_lsn_;
  share::SCN begin_scn_;
  LSN base_lsn_;
  LSN end_lsn_;
  share::SCN end_scn_;
  LSN max_lsn_;
  share::SCN max_scn_;
  bool is_in_sync_;
  bool is_need_rebuild_;
  TO_STRING_KV(K_(self), K_(palf_id), K_(role), K_(log_proposal_id), K_(config_version), K_(mode_version),
      K_(access_mode), K_(paxos_member_list), K_(paxos_replica_num), K_(learner_list), K_(allow_vote), K_(replica_type),
      K_(begin_lsn), K_(begin_scn), K_(base_lsn), K_(end_lsn), K_(end_scn), K_(max_lsn), K_(max_scn),
      K_(is_in_sync), K_(is_need_rebuild));
};

struct PalfDiagnoseInfo {
  PalfDiagnoseInfo() { reset(); }
  ~PalfDiagnoseInfo() { reset(); }
  common::ObRole election_role_;
  int64_t election_epoch_;
  common::ObRole palf_role_;
  palf::ObReplicaState palf_state_;
  int64_t palf_proposal_id_;
  bool enable_sync_;
  bool enable_vote_;
  void reset() {
    election_role_ = FOLLOWER;
    election_epoch_ = 0;
    palf_role_ = FOLLOWER;
    palf_state_ = ObReplicaState::INVALID_STATE;
    palf_proposal_id_ = INVALID_PROPOSAL_ID;
    enable_sync_ = false;
    enable_vote_ = false;
  }
  TO_STRING_KV(K(election_role_),
               K(election_epoch_),
               K(palf_role_),
               K(palf_state_),
               K(palf_proposal_id_),
               K(enable_sync_),
               K(enable_vote_));
};

struct FetchLogStat {
  FetchLogStat() { reset(); }
  ~FetchLogStat() { reset(); }
  int64_t total_size_;
  int64_t group_log_cnt_;
  int64_t read_cost_;  // time cost of reading and deserializing log
  int64_t get_cost_;   // time cost of checking integrity
  int64_t send_cost_;  // time cost of sending logs by rpc
  void reset() {
    total_size_ = 0;
    group_log_cnt_ = 0;
    read_cost_ = 0;
    get_cost_ = 0;
    send_cost_ = 0;
  }
  TO_STRING_KV(K_(total_size),
               K_(group_log_cnt),
               K_(read_cost),
               K_(get_cost),
               K_(send_cost));
};

struct BatchFetchParams {
  BatchFetchParams() { reset(); }
  ~BatchFetchParams() { reset(); }
  void reset() {
    can_batch_count_ = 0;
    can_batch_size_ = 0;
    batch_log_buf_ = NULL;
    last_log_lsn_prev_round_.reset();
    last_log_end_lsn_prev_round_.reset();
    last_log_proposal_id_prev_round_ = 0;
  }
  TO_STRING_KV(K_(can_batch_count),
               K_(can_batch_size),
               KP_(batch_log_buf),
               K_(last_log_lsn_prev_round),
               K_(last_log_end_lsn_prev_round),
               K_(last_log_proposal_id_prev_round),
               K_(has_consumed_count));
  int64_t can_batch_count_;
  int64_t can_batch_size_;
  char *batch_log_buf_;
  LSN last_log_lsn_prev_round_;
  LSN last_log_end_lsn_prev_round_;
  int64_t last_log_proposal_id_prev_round_;
  int64_t has_consumed_count_;
};

struct LSKey {
  LSKey() : id_(-1) {}
  explicit LSKey(const int64_t id) : id_(id) {}
  ~LSKey() {id_ = -1;}
  LSKey(const LSKey &key) { this->id_ = key.id_; }
  LSKey &operator=(const LSKey &other)
  {
    this->id_ = other.id_;
    return *this;
  }

  bool operator==(const LSKey &palf_id) const
  {
    return this->compare(palf_id) == 0;
  }
  bool operator!=(const LSKey &palf_id) const
  {
    return this->compare(palf_id) != 0;
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&hash_val, sizeof(id_), id_);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int compare(const LSKey &palf_id) const
  {
    if (palf_id.id_ < id_) {
      return 1;
    } else if (palf_id.id_ == id_) {
      return 0;
    } else {
      return -1;
    }
  }
  void reset() {id_ = -1;}
  bool is_valid() const {return -1 != id_;}
  int64_t id_;
  TO_STRING_KV(K_(id));
};

struct RebuildMetaInfo
{
public:
  RebuildMetaInfo() {reset();}
  ~ RebuildMetaInfo() {reset();}
  void reset()
  {
    committed_end_lsn_.reset();
    last_submit_lsn_.reset();
    last_submit_log_pid_ = INVALID_PROPOSAL_ID;
  }
  bool is_valid() const
  {
    return (committed_end_lsn_.is_valid()
            && last_submit_lsn_.is_valid()
            && INVALID_PROPOSAL_ID != last_submit_log_pid_);
  }
  bool operator==(const RebuildMetaInfo &other) const
  {
    return (committed_end_lsn_ == other.committed_end_lsn_
            && last_submit_lsn_ == other.last_submit_lsn_
            && last_submit_log_pid_ == other.last_submit_log_pid_);
  }
  TO_STRING_KV(K_(committed_end_lsn),
               K_(last_submit_lsn),
               K_(last_submit_log_pid));
public:
  LSN committed_end_lsn_;
  LSN last_submit_lsn_;
  int64_t last_submit_log_pid_;
};

// 日志服务的接口类，logservice以外的模块使用日志服务，只允许调用IPalfHandleImpl的接口
class IPalfHandleImpl : public common::LinkHashValue<LSKey>
{
public:
  IPalfHandleImpl() {};
  virtual ~IPalfHandleImpl() {};
public:
  virtual bool check_can_be_used() const = 0;

  // after creating palf successfully, set initial memberlist(can only be called once)
  //
  // @param [in] member_list, paxos memberlist
  // @param [in] paxos_replica_num, number of paxos replicas
  // @param [in] learner_list, learner_list
  //
  // @return :TODO
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const int64_t paxos_replica_num,
                                      const common::GlobalLearnerList &learner_list) = 0;
#ifdef OB_BUILD_ARBITRATION
  // after creating palf which includes arbitration replica successfully,
  // set initial memberlist(can only be called once)
  // @param [in] ObMemberList, the initial member list, do not include arbitration replica
  // @param [in] arb_member, arbitration replica
  // @param [in] paxos_replica_num, number of paxos replicas
  // @param [in] learner_list, learner_list
  // @return :TODO
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const common::ObMember &arb_member,
                                      const int64_t paxos_replica_num,
                                      const common::GlobalLearnerList &learner_list) = 0;
  virtual int get_remote_arb_member_info(ArbMemberInfo &arb_member_info) = 0;
  virtual int get_arb_member_info(ArbMemberInfo &arb_member_info) const = 0;
  virtual int get_arbitration_member(common::ObMember &arb_member) const = 0;
#endif
  // set region for self
  // @param [common::ObRegion] region
  virtual int set_region(const common::ObRegion &region) = 0;
  // set region info for paxos members
  // @param [common::ObArrayHashMap<common::ObAddr, common::ObRegion>] regions of Paxos members
  virtual int set_paxos_member_region_map(const LogMemberRegionMap &region_map) = 0;
  // 提交需要持久化的内容，提交内容以日志形式在本地持久化并同步给Paxos成员列表中的其他副本
  // 同时，若存在只读副本或物理备库，日志会在满足条件时同步给对应节点
  // 在Paxos成员列表中的多数派副本持久化成功后，会调用log_ctx->on_success()通知调用者
  //
  // 若执行过程中，发生了主备切换（Leader->Follower），且该日志最终形成了多数派，
  // 则仍会调用log_ctx->on_success()而非replay log
  //
  // 函数返回成功的场景下，在任一副本上log_ctx->on_success()/log_ctx->on_failure()/replay log
  // 其中之一保证最终调用且只调用一次
  //
  // @param [in] opts, 提交日志的一些可选项参数，具体参见PalfAppendOptions的定义
  // @param [in] buf, 待持久化内容的起点指针，::submit_log函数返回后buf即可被释放
  // @param [in] buf_len, 待持久化内容的长度，size的有效范围是[0, 2M]
  // @param [in] ref_scn, 日志对应的时间，满足弱读需求
  //
  // 下述两个值通过submit_log出参，而不是on_success()和上层交互，好处是类似于lock_for_read逻辑可以更早
  // 的拿到准确的版本号信息
  // @param [out] lsn, 日志的唯一标识符
  //                          主要使用场景是prepare日志中记录redo日志的lsn，用于数据链路回拉历史日志时定位使用
  // @param [out] scn, 日志对应的submit_scn，主要用于事务版本号，比如lock_for_read场景使用
  //
  // @return :TODO
  virtual int submit_log(const PalfAppendOptions &opts,
                         const char *buf,
                         const int64_t buf_len,
                         const share::SCN &ref_scn,
                         LSN &lsn,
                         share::SCN &scn) = 0;
  // 提交group_log到palf
  // 使用场景：备库leader处理从主库收到的日志
  // @param [in] opts, 提交日志的一些可选项参数，具体参见PalfAppendOptions的定义
  // @param [in] lsn, 日志对应的lsn
  // @param [in] buf, 待持久化内容的起点指针，::submit_group_log函数返回后buf即可被释放
  // @param [in] buf_len, 待持久化内容的长度
  virtual int submit_group_log(const PalfAppendOptions &opts,
                               const LSN &lsn,
                               const char *buf,
                               const int64_t buf_len) = 0;
  // 返回当前副本的角色，只存在Leader/StandbyLeader/Follower三种角色
  //
  // @param [out] role, 当前副本的角色
  // @param [out] proposal_id, leader的唯一标识符，跨机单调递增.
  //
  // @return :TODO
  virtual int get_role(common::ObRole &role,
                       int64_t &proposal_id,
                       bool &is_pending_state) const = 0;
  // 获取 palf_id
  virtual int get_palf_id(int64_t &palf_id) const = 0;
  // 切主接口，用于内部调试用，任何正式功能不应依赖此接口
  // 正式功能中的切主动作应当由优先级策略来描述，统一到一套规则中
  // 该接口是异步接口，在该接口调用返回成功后，Leader可能会经历若干秒才完成切换，理论上也存在极小概率切换失败
  // 在系统正常的情况下，预期在若干ms内，leader就会完成切换
  //
  // @param [in] dest_addr, 切主的目的副本，必须在当前的成员列表中
  //
  // @return :
  //  OB_ENTRY_NOT_EXIST : 切主的目标副本不在palf当前的成员列表中
  //  OB_NOT_MASTER : 本副本当前不是leader，无法接受切主请求
  virtual int change_leader_to(const common::ObAddr &dest_addr) = 0;

  virtual int get_global_learner_list(common::GlobalLearnerList &learner_list) const = 0;
  virtual int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const = 0;
  virtual int get_config_version(LogConfigVersion &config_version) const = 0;
  virtual int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                                     int64_t &paxos_replica_num,
                                                     common::GlobalLearnerList &learner_list) const = 0;
  virtual int get_election_leader(common::ObAddr &addr) const = 0;

  // @brief: a special config change interface, change replica number of paxos group
  // @param[in] common::ObMemberList: current memberlist, for pre-check
  // @param[in] const int64_t curr_replica_num: current replica num, for pre-check
  // @param[in] const int64_t new_replica_num: new replica num
  // @param[in] const int64_t timeout_us: timeout, us
  // @return
  // - OB_SUCCESS: change_replica_num successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: change_replica_num timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int change_replica_num(const common::ObMemberList &member_list,
                                 const int64_t curr_replica_num,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us) = 0;
  // @brief: force set self as single replica.
  virtual int force_set_as_single_replica() = 0;

  // @brief, add a member into paxos group
  // @param[in] common::ObMember &member: member which will be added
  // @param[in] const int64_t new_replica_num: replica number of paxos group after adding 'member'
  // @param[in] const int64_t timeout_us: add member timeout, us
  // @return
  // - OB_SUCCESS: add member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: add member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int add_member(const common::ObMember &member,
                        const int64_t new_replica_num,
                        const LogConfigVersion &config_version,
                        const int64_t timeout_us) = 0;

  // @brief, remove a member from paxos group
  // @param[in] common::ObMember &member: member which will be removed
  // @param[in] const int64_t new_replica_num: replica number of paxos group after removing 'member'
  // @param[in] const int64_t timeout_us: remove member timeout, us
  // @return
  // - OB_SUCCESS: remove member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int remove_member(const common::ObMember &member,
                            const int64_t new_replica_num,
                            const int64_t timeout_us) = 0;

  // @brief, replace old_member with new_member
  // @param[in] const common::ObMember &added_member: member will be added
  // @param[in] const common::ObMember &removed_member: member will be removed
  // @param[in] const LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int replace_member(const common::ObMember &added_member,
                             const common::ObMember &removed_member,
                             const LogConfigVersion &config_version,
                             const int64_t timeout_us) = 0;

  // @brief: add a learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &added_learner: learner will be added
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: add_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int add_learner(const common::ObMember &added_learner, const int64_t timeout_us) = 0;

  // @brief: remove a learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &removed_learner: learner will be removed
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: remove_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int remove_learner(const common::ObMember &removed_learner, const int64_t timeout_us) = 0;

  // @brief: switch a learner(read only replica) to acceptor(full replica) in this clsuter
  // @param[in] const common::ObMember &learner: learner will be switched to acceptor
  // @param[in] const int64_t new_replica_num: replica number of paxos group after switching
  //            learner to acceptor (similar to add_member)
  // @param[in] const LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: switch_learner_to_acceptor timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int switch_learner_to_acceptor(const common::ObMember &learner,
                                         const int64_t new_replica_num,
                                         const LogConfigVersion &config_version,
                                         const int64_t timeout_us) = 0;

  // @brief: switch an acceptor(full replica) to learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &member: acceptor will be switched to learner
  // @param[in] const int64_t new_replica_num: replica number of paxos group after switching
  //            acceptor to learner (similar to remove_member)
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: switch_acceptor_to_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int switch_acceptor_to_learner(const common::ObMember &member,
                                         const int64_t new_replica_num,
                                         const int64_t timeout_us) = 0;

  // @brief, replace removed_learners with added_learners
  // @param[in] const common::ObMemberList &added_learners: learners will be added
  // @param[in] const common::ObMemberList &removed_learners: learners will be removed
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace learner successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int replace_learners(const common::ObMemberList &added_learners,
                               const common::ObMemberList &removed_learners,
                               const int64_t timeout_us) = 0;

  // @brief, replace removed_member with learner
  // @param[in] const common::ObMember &added_member: member will be added
  // @param[in] const common::ObMember &removed_member: member will be removed
  // @param[in] const LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int replace_member_with_learner(const common::ObMember &added_member,
                                          const common::ObMember &removed_member,
                                          const LogConfigVersion &config_version,
                                          const int64_t timeout_us) = 0;
#ifdef OB_BUILD_ARBITRATION
  // @brief, add an arbitration member to paxos group
  // @param[in] common::ObMember &member: arbitration member which will be added
  // @param[in] const int64_t timeout_us: add member timeout, us
  // @return
  // - OB_SUCCESS: add arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: add arbitration member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int add_arb_member(const common::ObMember &added_member,
                             const int64_t timeout_us) = 0;
  // @brief, remove an arbitration member from paxos group
  // @param[in] common::ObMember &member: arbitration member which will be removed
  // @param[in] const int64_t timeout_us: remove member timeout, us
  // @return
  // - OB_SUCCESS: remove arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove arbitration member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int remove_arb_member(const common::ObMember &arb_member,
                                const int64_t timeout_us) = 0;
  // @brief: degrade an acceptor(full replica) to learner(special read only replica) in this cluster
  // @param[in] const common::ObMemberList &member_list: acceptors will be degraded to learner
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: timeout
  // - OB_NOT_MASTER: not leader
  virtual int degrade_acceptor_to_learner(const LogMemberAckInfoList &degrade_servers,
                                          const int64_t timeout_us) = 0;
  // @brief: upgrade a learner(special read only replica) to acceptor(full replica) in this cluster
  // @param[in] const common::ObMemberList &learner_list: learners will be upgraded to acceptors
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: timeout
  // - OB_NOT_MASTER: not leader
  virtual int upgrade_learner_to_acceptor(const LogMemberAckInfoList &upgrade_servers,
                                          const int64_t timeout_us) = 0;
#endif

  // 设置日志文件的可回收位点，小于等于lsn的日志文件均可以安全回收
  //
  // @param [in] lsn，可回收的日志文件位点
  //
  // @return :TODO
  virtual int set_base_lsn(const LSN &lsn) = 0;

  // 允许palf收拉日志
  virtual int enable_sync() = 0;
  // 禁止palf收拉日志，在rebuild/migrate场景下，防止日志盘爆
  virtual int disable_sync() = 0;
  // 标记palf实例已经删除
  virtual void set_deleted() = 0;
  virtual bool is_sync_enabled() const = 0;

  // 迁移/rebuild场景目的端推进base_lsn
  //
  // @param [in] palf_base_info，可回收的日志文件位点
  virtual int advance_base_info(const PalfBaseInfo &palf_base_info, const bool is_rebuild) = 0;

  // @desc: query coarse lsn by scn, that means there is a LogGroupEntry in disk,
  // its lsn and scn are result_lsn and result_scn, and result_scn <= scn.
  //        result_lsn   result_scn
  //                 \   /
  //      [log 1]     [log 2][log 3] ... [log n]  [log n+1]
  //  -------------------------------------------|-------------> time
  //                                           scn
  // Note that this function may be time-consuming
  // Note that result_lsn always points to head of log file
  // @params [in] scn:
  // @params [out] result_lsn: the lower bound lsn which includes scn
  // @return
  // - OB_SUCCESS: locate_by_scn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ENTRY_NOT_EXIST: there is no log in disk
  // - OB_ERR_OUT_OF_LOWER_BOUND: scn is too old, log files may have been recycled
  // - OB_NEED_RETRY: the block is being flashback, need retry.
  // - others: bug
  virtual int locate_by_scn_coarsely(const share::SCN &scn, LSN &result_lsn) = 0;

  // @desc: query coarse scn by lsn, that means there is a LogGroupEntry in disk,
  // its lsn and scn are result_lsn and result_scn, and result_lsn <= lsn.
  //  result_lsn    result_scn
  //           \    /
  //    [log 1][log 2][log 3][log 4][log 5]...[log n][log n+1]
  //  --------------------------------------------|-------------> lsn
  //                                             lsn
  // Note that this function may be time-consuming
  // @params [in] lsn: lsn
  // @params [out] result_scn: the lower bound scn which includes lsn
  // - OB_SUCCESS; locate_by_lsn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ERR_OUT_OF_LOWER_BOUND: lsn is too small, log files may have been recycled
  // - OB_NEED_RETRY: the block is being flashback, need retry.
  // - others: bug
  virtual int locate_by_lsn_coarsely(const LSN &lsn, share::SCN &result_scn) = 0;
  virtual int get_begin_lsn(LSN &lsn) const = 0;
  virtual int get_begin_scn(share::SCN &scn) = 0;
  virtual int get_base_lsn(LSN &lsn) const = 0;
  virtual int get_base_info(const LSN &base_lsn, PalfBaseInfo &base_info) = 0;

  virtual int get_min_block_info_for_gc(block_id_t &min_block_id, share::SCN &max_scn) = 0;
  //begin lsn                          base lsn                                end lsn
  //   │                                │                                         │
  //   │                                │                                         │
  //   │                                │                                         │
  //   │                                │                                         │
  //   ▼                                ▼                                         ▼
  //   ┌─────────────────────────────────────────────────────────────────────────────────┐
  //   │                                                                                 │
  //   │                                                                                 │
  //   │                                                                                 │
  //   └─────────────────────────────────────────────────────────────────────────────────┘
  //
  // return the block length which the previous data was committed
  virtual const LSN get_end_lsn() const = 0;
  virtual LSN get_max_lsn() const = 0;
  virtual const share::SCN get_max_scn() const = 0;
  virtual const share::SCN get_end_scn() const = 0;
  virtual int get_last_rebuild_lsn(LSN &last_rebuild_lsn) const = 0;
  virtual int get_total_used_disk_space(int64_t &total_used_disk_space, int64_t &unrecyclable_disk_space) const = 0;
  virtual const LSN &get_base_lsn_used_for_block_gc() const = 0;
  // @desc: get ack_info_array and degraded_list for judging to degrade/upgrade
  // @params [in] member_ts_array: ack info array of all paxos members
  // @params [in] degraded_list: members which have been degraded
  // @return:
  virtual int get_ack_info_array(LogMemberAckInfoList &ack_info_array,
                                 common::GlobalLearnerList &degraded_list) const = 0;

  virtual int delete_block(const block_id_t &block_id) = 0;
  virtual int read_log(const LSN &lsn,
                       const int64_t in_read_size,
                       ReadBuf &read_buf,
                       int64_t &out_read_size) = 0;
  virtual int inner_after_flush_log(const FlushLogCbCtx &flush_log_cb_ctx) = 0;
  virtual int inner_after_truncate_log(const TruncateLogCbCtx &truncate_log_cb_ctx) = 0;
  virtual int inner_after_flush_meta(const FlushMetaCbCtx &flush_meta_cb_ctx) = 0;
  virtual int inner_after_truncate_prefix_blocks(const TruncatePrefixBlocksCbCtx &truncate_prefix_cb_ctx) = 0;
  virtual int advance_reuse_lsn(const LSN &flush_log_end_lsn) = 0;
  virtual int inner_after_flashback(const FlashbackCbCtx &flashback_ctx) = 0;
  virtual int inner_append_log(const LSN &lsn,
                               const LogWriteBuf &write_buf,
                               const share::SCN &scn) = 0;
  virtual int inner_append_log(const LSNArray &lsn_array,
                               const LogWriteBufArray &write_buf_array,
                               const SCNArray &scn_array) = 0;
  virtual int inner_append_meta(const char *buf,
                                const int64_t buf_len) = 0;
  virtual int inner_truncate_log(const LSN &lsn) = 0;
  virtual int inner_truncate_prefix_blocks(const LSN &lsn) = 0;
  virtual int inner_flashback(const share::SCN &flashback_scn) = 0;
  virtual int check_and_switch_state() = 0;
  virtual int check_and_switch_freeze_mode() = 0;
  virtual bool is_in_period_freeze_mode() const = 0;
  virtual int period_freeze_last_log() = 0;
  virtual int handle_prepare_request(const common::ObAddr &server,
                                     const int64_t &proposal_id) = 0;
  virtual int handle_prepare_response(const common::ObAddr &server,
                                      const int64_t &proposal_id,
                                      const bool vote_granted,
                                      const int64_t &accept_proposal_id,
                                      const LSN &last_lsn,
                                      const LSN &committed_end_lsn,
                                      const LogModeMeta &log_mode_meta) = 0;
  virtual int handle_election_message(const election::ElectionPrepareRequestMsg &msg) = 0;
  virtual int handle_election_message(const election::ElectionPrepareResponseMsg &msg) = 0;
  virtual int handle_election_message(const election::ElectionAcceptRequestMsg &msg) = 0;
  virtual int handle_election_message(const election::ElectionAcceptResponseMsg &msg) = 0;
  virtual int handle_election_message(const election::ElectionChangeLeaderMsg &msg) = 0;
  virtual int receive_log(const common::ObAddr &server,
                          const PushLogType push_log_type,
                          const int64_t &proposal_id,
                          const LSN &prev_lsn,
                          const int64_t &prev_proposal_id,
                          const LSN &lsn,
                          const char *buf,
                          const int64_t buf_len) = 0;
  virtual int receive_batch_log(const common::ObAddr &server,
                                const int64_t msg_proposal_id,
                                const int64_t prev_log_proposal_id,
                                const LSN &prev_lsn,
                                const LSN &curr_lsn,
                                const char *buf,
                                const int64_t buf_len) = 0;
  virtual int ack_log(const common::ObAddr &server,
                      const int64_t &proposal_id,
                      const LSN &log_end_lsn) = 0;
  virtual int get_log(const common::ObAddr &server,
                      const FetchLogType fetch_type,
                      const int64_t msg_proposal_id,
                      const LSN &prev_lsn,
                      const LSN &start_lsn,
                      const int64_t fetch_log_size,
                      const int64_t fetch_log_count,
                      const int64_t accepted_mode_pid) = 0;
  virtual int fetch_log_from_storage(const common::ObAddr &server,
                                     const FetchLogType fetch_type,
                                     const int64_t &req_proposal_id,
                                     const LSN &prev_log_offset,
                                     const LSN &log_offset,
                                     const int64_t fetch_log_size,
                                     const int64_t fetch_log_count,
                                     const int64_t accepted_mode_pid,
                                     const SCN &replayable_point,
                                     FetchLogStat &fetch_stat) = 0;
  virtual int receive_config_log(const common::ObAddr &server,
                                 const int64_t &msg_proposal_id,
                                 const int64_t &prev_log_proposal_id,
                                 const LSN &prev_lsn,
                                 const int64_t &prev_mode_pid,
                                 const LogConfigMeta &meta) = 0;
  virtual int ack_config_log(const common::ObAddr &server,
                             const int64_t proposal_id,
                             const LogConfigVersion &config_version) = 0;
  virtual int receive_mode_meta(const common::ObAddr &server,
                                const int64_t msg_proposal_id,
                                const bool is_applied_mode_meta,
                                const LogModeMeta &meta) = 0;
  virtual int ack_mode_meta(const common::ObAddr &server,
                            const int64_t proposal_id) = 0;
  virtual int handle_notify_fetch_log_req(const common::ObAddr &server) = 0;
  virtual int handle_notify_rebuild_req(const common::ObAddr &server,
                                        const LSN &base_lsn,
                                        const LogInfo &base_prev_log_info) = 0;
  virtual int handle_config_change_pre_check(const ObAddr &server,
                                             const LogGetMCStReq &req,
                                             LogGetMCStResp &resp) = 0;
  virtual int handle_register_parent_req(const LogLearner &child,
                                         const bool is_to_leader) = 0;
  virtual int handle_register_parent_resp(const LogLearner &server,
                                          const LogCandidateList &candidate_list,
                                          const RegisterReturn reg_ret) = 0;
  virtual int handle_learner_req(const LogLearner &server, const LogLearnerReqType req_type) = 0;
  virtual int set_scan_disk_log_finished() = 0;
  virtual int change_access_mode(const int64_t proposal_id,
                                 const int64_t mode_version,
                                 const AccessMode &access_mode,
                                 const share::SCN &ref_scn) = 0;
  virtual int get_access_mode(int64_t &mode_version, AccessMode &access_mode) const = 0;
  virtual int get_access_mode(AccessMode &access_mode) const = 0;
  virtual int get_access_mode_ref_scn(int64_t &mode_version,
                                      AccessMode &access_mode,
                                      SCN &ref_scn) const = 0;
  virtual int handle_committed_info(const common::ObAddr &server,
                            const int64_t &msg_proposal_id,
                            const int64_t prev_log_id,
                            const int64_t &prev_log_proposal_id,
                            const LSN &committed_end_lsn) = 0;

  // @brief: check whether the palf instance is allowed to vote for logs
  // By default, return true;
  // After calling disable_vote(), return false.
  virtual bool is_vote_enabled() const = 0;
  // @brief: store a persistent flag which means this paxos replica
  // can not reply ack when receiving logs.
  // By default, paxos replica can reply ack.
  // This interface is idempotent.
  // @param[in] need_check_log_missing: reason for rebuilding. True means log missing, False means data
  // missing
  // @return:
  // OB_NOT_INIT: not inited
  // OB_NOT_RUNNING: in stop state
  // OB_OP_NOT_ALLOWED: no need to rebuilds. rebuilding should be abandoned.
  // OB_LEADER_NOT_EXIST: no leader when double checking. rebuilding should retry.
  virtual int disable_vote(const bool need_check_log_missing) = 0;
  // @brief: store a persistent flag which means this paxos replica
  // can reply ack when receiving logs.
  // By default, paxos replica can reply ack.
  // This interface is idempotent.
  virtual int enable_vote() = 0;

  // ===================== Iterator start =======================
  virtual int alloc_palf_buffer_iterator(const LSN &offset,
                                         PalfBufferIterator &iterator) = 0;
  virtual int alloc_palf_group_buffer_iterator(const LSN &offset,
                                               PalfGroupBufferIterator &iterator) = 0;
  virtual int alloc_palf_group_buffer_iterator(const share::SCN &scn,
                                               PalfGroupBufferIterator &iterator) = 0;
  // ===================== Iterator end =======================

  // ==================== Callback start ======================
  virtual int register_file_size_cb(palf::PalfFSCbNode *fs_cb) = 0;
  virtual int unregister_file_size_cb(palf::PalfFSCbNode *fs_cb) = 0;
  virtual int register_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb) = 0;
  virtual int unregister_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb) = 0;
  virtual int register_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb) = 0;
  virtual int unregister_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb) = 0;
  virtual int set_location_cache_cb(PalfLocationCacheCb *lc_cb) = 0;
  virtual int reset_location_cache_cb() = 0;
  virtual int set_election_priority(election::ElectionPriority *priority) = 0;
  virtual int reset_election_priority() = 0;
  // ==================== Callback end ========================
  virtual int revoke_leader(const int64_t proposal_id) = 0;
  virtual int flashback(const int64_t mode_version,
                        const share::SCN &flashback_scn,
                        const int64_t timeout_us) = 0;

  virtual int stat(PalfStat &palf_stat) = 0;
  virtual int get_palf_epoch(int64_t &palf_epoch) const = 0;
  virtual int try_lock_config_change(int64_t lock_owner, int64_t timeout_us) = 0;
  virtual int unlock_config_change(int64_t lock_owner, int64_t timeout_us) = 0;
  virtual int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked) = 0;
  virtual int diagnose(PalfDiagnoseInfo &diagnose_info) const = 0;
  virtual int update_palf_stat() = 0;
  virtual int read_data_from_buffer(const LSN &read_begin_lsn,
                                    const int64_t in_read_size,
                                    char *buf,
                                    int64_t &out_read_size) const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class PalfHandleImpl : public IPalfHandleImpl
{
public:
  PalfHandleImpl();
  ~PalfHandleImpl() override;
  int init(const int64_t palf_id,
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
           const int64_t palf_epoch);
  bool check_can_be_used() const override final;
  // 重启接口
  // 1. 生成迭代器，定位meta_storage和log_storage的终点;
  // 2. 从meta storage中读最新数据，初始化dio_aligned_buf;
  // 3. 初始化log_storage中的dio_aligned_buf;
  // 4. 初始化palf_handle_impl的其他字段.
  int load(const int64_t palf_id,
           FetchLogEngine *fetch_log_engine,
           const char *log_dir,
           ObILogAllocator *alloc_mgr,
           ILogBlockPool *log_block_pool,
           LogRpc *log_rpc,
           LogIOWorker*log_io_worker,
           IPalfEnvImpl *palf_env_impl,
           const common::ObAddr &self,
           common::ObOccamTimer *election_timer,
           const int64_t palf_epoch,
           bool &is_integrity);
  void destroy();
  int start();
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list) override final;
#ifdef OB_BUILD_ARBITRATION
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const common::ObMember &arb_member,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list) override final;
#endif
  int set_region(const common::ObRegion &region) override final;
  int set_paxos_member_region_map(const LogMemberRegionMap &region_map) override final;
  int submit_log(const PalfAppendOptions &opts,
                 const char *buf,
                 const int64_t buf_len,
                 const share::SCN &ref_scn,
                 LSN &lsn,
                 share::SCN &scn) override final;

  int submit_group_log(const PalfAppendOptions &opts,
                       const LSN &lsn,
                       const char *buf,
                       const int64_t buf_len) override final;
  int get_role(common::ObRole &role,
               int64_t &proposal_id,
               bool &is_pending_state) const override final;
  int get_palf_id(int64_t &palf_id) const override final;
  int change_leader_to(const common::ObAddr &dest_addr) override final;
  int get_global_learner_list(common::GlobalLearnerList &learner_list) const override final;
  int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const override final;
  int get_config_version(LogConfigVersion &config_version) const;
  int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                             int64_t &paxos_replica_num,
                                             common::GlobalLearnerList &learner_list) const override final;
  int get_election_leader(common::ObAddr &addr) const;
  int force_set_as_single_replica() override final;
  int change_replica_num(const common::ObMemberList &member_list,
                         const int64_t curr_replica_num,
                         const int64_t new_replica_num,
                         const int64_t timeout_us) override final;
  int add_member(const common::ObMember &member,
                const int64_t new_replica_num,
                const LogConfigVersion &config_version,
                const int64_t timeout_us) override final;
  int remove_member(const common::ObMember &member,
                    const int64_t new_replica_num,
                    const int64_t timeout_us) override final;
  int replace_member(const common::ObMember &added_member,
                     const common::ObMember &removed_member,
                     const LogConfigVersion &config_version,
                     const int64_t timeout_us) override final;
  int add_learner(const common::ObMember &added_learner,
                  const int64_t timeout_us) override final;
  int remove_learner(const common::ObMember &removed_learner,
                  const int64_t timeout_us) override final;
  int switch_learner_to_acceptor(const common::ObMember &learner,
                                 const int64_t new_replica_num,
                                 const LogConfigVersion &config_version,
                                 const int64_t timeout_us) override final;
  int switch_acceptor_to_learner(const common::ObMember &member,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us) override final;
  int replace_learners(const common::ObMemberList &added_learners,
                       const common::ObMemberList &removed_learners,
                       const int64_t timeout_us) override final;
  int replace_member_with_learner(const common::ObMember &added_member,
                                  const common::ObMember &removed_member,
                                  const LogConfigVersion &config_version,
                                  const int64_t timeout_us) override final;
#ifdef OB_BUILD_ARBITRATION
  int add_arb_member(const common::ObMember &added_member,
                     const int64_t timeout_us) override final;
  int remove_arb_member(const common::ObMember &arb_member,
                        const int64_t timeout_us) override final;
  int degrade_acceptor_to_learner(const LogMemberAckInfoList &degrade_servers,
                                  const int64_t timeout_us) override final;
  int upgrade_learner_to_acceptor(const LogMemberAckInfoList &upgrade_servers,
                                  const int64_t timeout_us) override final;
  int get_remote_arb_member_info(ArbMemberInfo &arb_member_info) override final;
  int get_arb_member_info(ArbMemberInfo &arb_member_info) const override final;
  int get_arbitration_member(common::ObMember &arb_member) const override final;
#endif
  int set_base_lsn(const LSN &lsn) override final;
  int enable_sync() override final;
  int disable_sync() override final;
  void set_deleted() override final;
  bool is_sync_enabled() const override final;
  int advance_base_info(const PalfBaseInfo &palf_base_info, const bool is_rebuild) override final;
  int locate_by_scn_coarsely(const share::SCN &scn, LSN &result_lsn) override final;
  int locate_by_lsn_coarsely(const LSN &lsn, share::SCN &result_scn) override final;
  bool is_vote_enabled() const override final;
  int disable_vote(const bool need_check_log_missing) override final;
  int enable_vote() override final;
  int read_data_from_buffer(const LSN &read_begin_lsn,
                            const int64_t in_read_size,
                            char *buf,
                            int64_t &out_read_size) const;
public:
  int delete_block(const block_id_t &block_id) override final;
  int read_log(const LSN &lsn,
               const int64_t in_read_size,
               ReadBuf &read_buf,
               int64_t &out_read_size) override final;
  int set_scan_disk_log_finished() override;
  int change_access_mode(const int64_t proposal_id,
                         const int64_t mode_version,
                         const AccessMode &access_mode,
                         const share::SCN &ref_scn) override final;
  int get_access_mode(int64_t &mode_version, AccessMode &access_mode) const override final;
  int get_access_mode(AccessMode &access_mode) const override final;
  int get_access_mode_version(int64_t &mode_version) const;
  int get_access_mode_ref_scn(int64_t &mode_version,
                              AccessMode &access_mode,
                              SCN &ref_scn) const override final;
  // =========================== Iterator start ============================
  int alloc_palf_buffer_iterator(const LSN &offset, PalfBufferIterator &iterator) override final;
  int alloc_palf_group_buffer_iterator(const LSN &offset, PalfGroupBufferIterator &iterator) override final;
  int alloc_palf_group_buffer_iterator(const share::SCN &scn, PalfGroupBufferIterator &iterator) override final;
  // =========================== Iterator end ============================

  // ==================== Callback start ======================
  int register_file_size_cb(palf::PalfFSCbNode *fs_cb) override final;
  int unregister_file_size_cb(palf::PalfFSCbNode *fs_cb) override final;
  int register_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb) override final;
  int unregister_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb) override final;
  int register_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb) override final;
  int unregister_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb) override final;
  int set_location_cache_cb(PalfLocationCacheCb *lc_cb) override final;
  int reset_location_cache_cb() override final;
  int set_monitor_cb(PalfMonitorCb *monitor_cb);
  int reset_monitor_cb();
  int set_election_priority(election::ElectionPriority *priority) override final;
  int reset_election_priority() override final;
  // ==================== Callback end ========================
public:
  int get_begin_lsn(LSN &lsn) const override final;
  int get_begin_scn(share::SCN &scn)  override final;
  int get_base_lsn(LSN &lsn) const override final;
  int get_base_info(const LSN &base_lsn, PalfBaseInfo &base_info) override final;
  int get_min_block_info_for_gc(block_id_t &min_block_id, share::SCN &max_scn) override final;
  // return the block length which the previous data was committed
  const LSN get_end_lsn() const override final
  {
    LSN committed_end_lsn;
    sw_.get_committed_end_lsn(committed_end_lsn);
    return committed_end_lsn;
  }

  LSN get_max_lsn() const override final
  {
    return sw_.get_max_lsn();
  }

  const share::SCN get_max_scn() const override final
  {
    return sw_.get_max_scn();
  }

  const share::SCN get_end_scn() const override final
  {
    // 基于实现复杂度考虑，直接用last_slide_scn作为end_scn
    // 否则需要在match_lsn_map中额外维护scn
    return sw_.get_last_slide_scn();
  }
  int get_last_rebuild_lsn(LSN &last_rebuild_lsn) const override final;
  int get_total_used_disk_space(int64_t &total_used_disk_space, int64_t &unrecyclable_disk_space) const;
  // return the smallest recycable lsn
  const LSN &get_base_lsn_used_for_block_gc() const override final
  {
    return log_engine_.get_base_lsn_used_for_block_gc();
  }
  int get_ack_info_array(LogMemberAckInfoList &ack_info_array,
                         common::GlobalLearnerList &degraded_list) const override final;
  // =====================  LogIOTask start ==========================
  int inner_after_flush_log(const FlushLogCbCtx &flush_log_cb_ctx) override final;
  int inner_after_truncate_log(const TruncateLogCbCtx &truncate_log_cb_ctx) override final;
  int inner_after_flush_meta(const FlushMetaCbCtx &flush_meta_cb_ctx) override final;
  int inner_after_truncate_prefix_blocks(const TruncatePrefixBlocksCbCtx &truncate_prefix_cb_ctx) override final;
  int advance_reuse_lsn(const LSN &flush_log_end_lsn);
  int inner_after_flashback(const FlashbackCbCtx &flashback_ctx) override final;
  int inner_append_log(const LSN &lsn,
                       const LogWriteBuf &write_buf,
                       const share::SCN &scn) override final;
  int inner_append_log(const LSNArray &lsn_array,
                       const LogWriteBufArray &write_buf_array,
                       const SCNArray &scn_array);
  int inner_append_meta(const char *buf,
                        const int64_t buf_len) override final;
  int inner_truncate_log(const LSN &lsn) override final;
  int inner_truncate_prefix_blocks(const LSN &lsn) override final;
  int inner_flashback(const share::SCN &flashback_scn) override final;
  // ==================================================================
  int check_and_switch_state() override final;
  int check_and_switch_freeze_mode() override final;
  bool is_in_period_freeze_mode() const override final;
  int period_freeze_last_log() override final;
  int handle_prepare_request(const common::ObAddr &server,
                             const int64_t &proposal_id) override final;
  int handle_prepare_response(const common::ObAddr &server,
                              const int64_t &proposal_id,
                              const bool vote_granted,
                              const int64_t &accept_proposal_id,
                              const LSN &last_lsn,
                              const LSN &committed_end_lsn,
                              const LogModeMeta &log_mode_meta) override final;
  int handle_election_message(const election::ElectionPrepareRequestMsg &msg) override final;
  int handle_election_message(const election::ElectionPrepareResponseMsg &msg) override final;
  int handle_election_message(const election::ElectionAcceptRequestMsg &msg) override final;
  int handle_election_message(const election::ElectionAcceptResponseMsg &msg) override final;
  int handle_election_message(const election::ElectionChangeLeaderMsg &msg) override final;
  int receive_log(const common::ObAddr &server,
                  const PushLogType push_log_type,
                  const int64_t &msg_proposal_id,
                  const LSN &prev_lsn,
                  const int64_t &prev_log_proposal_id,
                  const LSN &lsn,
                  const char *buf,
                  const int64_t buf_len) override final;
  int receive_batch_log(const common::ObAddr &server,
                        const int64_t msg_proposal_id,
                        const int64_t prev_log_proposal_id,
                        const LSN &prev_lsn,
                        const LSN &curr_lsn,
                        const char *buf,
                        const int64_t buf_len) override final;
  int ack_log(const common::ObAddr &server,
              const int64_t &proposal_id,
              const LSN &log_end_lsn) override final;
  int get_log(const common::ObAddr &server,
              const FetchLogType fetch_type,
              const int64_t msg_proposal_id,
              const LSN &prev_lsn,
              const LSN &start_lsn,
              const int64_t fetch_log_size,
              const int64_t fetch_log_count,
              const int64_t accepted_mode_pid) override final;
  int fetch_log_from_storage(const common::ObAddr &server,
                             const FetchLogType fetch_type,
                             const int64_t &msg_proposal_id,
                             const LSN &prev_lsn,
                             const LSN &fetch_start_lsn,
                             const int64_t fetch_log_size,
                             const int64_t fetch_log_count,
                             const int64_t accepted_mode_pid,
                             const SCN &replayable_point,
                             FetchLogStat &fetch_stat) override final;
  int receive_config_log(const common::ObAddr &server,
                         const int64_t &msg_proposal_id,
                         const int64_t &prev_log_proposal_id,
                         const LSN &prev_lsn,
                         const int64_t &prev_mode_pid,
                         const LogConfigMeta &meta) override final;
  int ack_config_log(const common::ObAddr &server,
                     const int64_t proposal_id,
                     const LogConfigVersion &config_version) override final;
  int receive_mode_meta(const common::ObAddr &server,
                        const int64_t proposal_id,
                        const bool is_applied_mode_meta,
                        const LogModeMeta &meta) override final;
  int ack_mode_meta(const common::ObAddr &server,
                     const int64_t proposal_id) override final;
  int handle_notify_fetch_log_req(const common::ObAddr &server) override final;
  int handle_notify_rebuild_req(const common::ObAddr &server,
                                const LSN &base_lsn,
                                const LogInfo &base_prev_log_info) override final;
  int handle_committed_info(const common::ObAddr &server,
                            const int64_t &msg_proposal_id,
                            const int64_t prev_log_id,
                            const int64_t &prev_log_proposal_id,
                            const LSN &committed_end_lsn) override final;
  int handle_config_change_pre_check(const ObAddr &server,
                                     const LogGetMCStReq &req,
                                     LogGetMCStResp &resp) override final;
  int revoke_leader(const int64_t proposal_id) override final;
  int stat(PalfStat &palf_stat) override final;
  int handle_register_parent_req(const LogLearner &child,
                                 const bool is_to_leader) override final;
  int handle_register_parent_resp(const LogLearner &server,
                                  const LogCandidateList &candidate_list,
                                  const RegisterReturn reg_ret) override final;
  int handle_learner_req(const LogLearner &server, const LogLearnerReqType req_type) override final;
  int get_palf_epoch(int64_t &palf_epoch) const override final;
  int flashback(const int64_t mode_version,
                const share::SCN &flashback_scn,
                const int64_t timeout_us) override final;

  //config change lock related function
  int try_lock_config_change(int64_t lock_owner, int64_t timeout_us);

  int unlock_config_change(int64_t lock_owner, int64_t timeout_us);
  int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked);

  int diagnose(PalfDiagnoseInfo &diagnose_info) const;
  int update_palf_stat() override final;
  TO_STRING_KV(K_(palf_id), K_(self), K_(has_set_deleted));
private:
  int do_init_mem_(const int64_t palf_id,
                   const PalfBaseInfo &palf_base_info,
                   const LogMeta &log_meta,
                   const char *log_dir,
                   const common::ObAddr &self,
                   FetchLogEngine *fetch_log_engine,
                   ObILogAllocator *alloc_mgr,
                   LogRpc *log_rpc,
                   IPalfEnvImpl *palf_env_impl,
                   common::ObOccamTimer *election_timer);
  int after_flush_prepare_meta_(const int64_t &proposal_id);
  int after_flush_config_change_meta_(const int64_t proposal_id, const LogConfigVersion &config_version);
  int after_flush_mode_meta_(const int64_t proposal_id,
                             const bool is_applied_mode_meta,
                             const LogModeMeta &mode_meta);
  int after_flush_snapshot_meta_(const LSN &lsn);
  int after_flush_replica_property_meta_(const bool allow_vote);
  /*
   *param[in] need_check_log_missing: for disable_vote invoke by rebuilding,
   true means need double check whether log is actually missing
   * */
  int set_allow_vote_flag_(const bool allow_vote, const bool need_check_log_missing);
  int get_prev_log_info_(const LSN &lsn, LogInfo &log_info);
  int get_prev_log_info_for_fetch_(const LSN &prev_lsn,
                                   const LSN &curr_lsn,
                                   LogInfo &prev_log_info);
  int submit_prepare_response_(const common::ObAddr &server,
                               const int64_t &proposal_id);
  int construct_palf_base_info_(const LSN &max_committed_lsn,
                                PalfBaseInfo &palf_base_info);
  int construct_palf_base_info_for_flashback_(const LSN &start_lsn,
                                              const share::SCN &flashback_scn,
                                              const LSN &prev_entry_lsn,
                                              const LogGroupEntryHeader &prev_entry_header,
                                              PalfBaseInfo &palf_base_info);
  int append_disk_log_to_sw_(const LSN &start_lsn);
  int try_send_committed_info_(const common::ObAddr &server,
                               const LSN &log_lsn,
                               const LSN &log_end_lsn,
                               const int64_t &log_proposal_id);
  int receive_log_(const common::ObAddr &server,
                  const PushLogType push_log_type,
                  const int64_t &msg_proposal_id,
                  const LSN &prev_lsn,
                  const int64_t &prev_log_proposal_id,
                  const LSN &lsn,
                  const char *buf,
                  const int64_t buf_len);
  int fetch_log_from_storage_(const common::ObAddr &server,
                              const FetchLogType fetch_type,
                              const int64_t &msg_proposal_id,
                              const LSN &prev_lsn,
                              const LSN &fetch_start_lsn,
                              const int64_t fetch_log_size,
                              const int64_t fetch_log_count,
                              const SCN &replayable_point,
                              FetchLogStat &fetch_stat);
  int batch_fetch_log_each_round_(const common::ObAddr &server,
                                  const int64_t msg_proposal_id,
                                  PalfGroupBufferIterator &iterator,
                                  const bool is_limitted_by_end_lsn,
                                  const bool is_dest_in_memberlist,
                                  const share::SCN& replayable_point,
                                  const LSN &fetch_end_lsn,
                                  const LSN &committed_end_lsn,
                                  const int64_t batch_log_size_threshold,
                                  BatchFetchParams &batch_fetch_params,
                                  bool &skip_next,
                                  bool &is_reach_end,
                                  FetchLogStat &fetch_stat);
  int submit_fetch_log_resp_(const common::ObAddr &server,
                             const int64_t &msg_proposal_id,
                             const int64_t &prev_log_proposal_id,
                             const LSN &prev_lsn,
                             const LSN &curr_lsn,
                             const LogGroupEntry &curr_group_entry);
  int submit_fetch_log_resp_(const common::ObAddr &server,
                             const int64_t &msg_proposal_id,
                             const int64_t &prev_log_proposal_id,
                             const LSN &prev_lsn,
                             const LSN &curr_lsn,
                             const char *buf,
                             const int64_t buf_len);
  int submit_batch_fetch_log_resp_(const common::ObAddr &server,
                                   const int64_t msg_proposal_id,
                                   const int64_t prev_log_proposal_id,
                                   const LSN &prev_lsn,
                                   const LSN &curr_lsn,
                                   const char *buf,
                                   const int64_t buf_len);
  int try_update_proposal_id_(const common::ObAddr &server,
                              const int64_t &proposal_id);
  int get_binary_search_range_(const share::SCN &scn,
                               block_id_t &min_block_id,
                               block_id_t &max_block_id,
                               block_id_t &result_block_id);
  int get_block_id_by_scn_(const share::SCN &scn, block_id_t &result_block_id);
  int get_block_id_by_scn_for_flashback_(const share::SCN &scn, block_id_t &result_block_id);
  void inc_update_last_locate_block_scn_(const block_id_t &block_id, const share::SCN &scn);
  int pre_check_before_degrade_upgrade_(const LogMemberAckInfoList &servers,
                                        const LogConfigChangeType &type);
  int can_change_config_(const LogConfigChangeArgs &args, int64_t &proposal_id);
  int check_args_and_generate_config_(const LogConfigChangeArgs &args,
                                      const int64_t proposal_id,
                                      const int64_t election_epoch,
                                      bool &is_already_finished,
                                      LogConfigInfoV2 &new_config_info) const;
  int wait_log_barrier_(const LogConfigChangeArgs &args,
                        const LogConfigInfoV2 &new_config_info,
                        TimeoutChecker &not_timeout);
  int one_stage_config_change_(const LogConfigChangeArgs &args, const int64_t timeout_us);
  int check_need_rebuild_(const LSN &base_lsn,
                          const LogInfo &base_prev_log_info,
                          bool &need_rebuild,
                          bool &need_fetch_log);
  int check_need_advance_base_info_(const LSN &base_lsn,
                                    const LogInfo &base_prev_log_info,
                                    const bool is_rebuild);
  int get_election_leader_without_lock_(ObAddr &addr) const;
  // ========================= flashback ==============================
  int can_do_flashback_(const int64_t mode_version,
                        const share::SCN &flashback_scn,
                        bool &is_already_done);
  int do_flashback_(const LSN &start_lsn,
                    const share::SCN &flashback_scn);
  int read_and_append_log_group_entry_before_ts_(const LSN &start_lsn,
                                                 const share::SCN &flashback_scn,
                                                 char *&last_log_buf,
                                                 int64_t &last_log_buf_len,
                                                 LSN &last_log_start_lsn,
                                                 PalfBaseInfo &palf_base_info);
  int cut_last_log_and_append_it_(char *last_log_buf,
                                  const int64_t last_log_buf_len,
                                  const LSN &last_log_start_lsn,
                                  const share::SCN &flashback_scn,
                                  PalfBaseInfo &in_out_palf_base_info);
  // =================================================================
  int leader_sync_mode_meta_to_arb_member_();
  void is_in_sync_(bool &is_log_sync, bool &is_use_cache);
  int get_leader_max_scn_(SCN &max_scn, LSN &end_lsn);
  void gen_rebuild_meta_info_(RebuildMetaInfo &rebuild_meta) const;
  void get_last_rebuild_meta_info_(RebuildMetaInfo &rebuild_meta_info) const;
  // ======================= report event begin =======================================
  void report_set_initial_member_list_(const int64_t paxos_replica_num, const common::ObMemberList &member_list);
  void report_set_initial_member_list_with_arb_(const int64_t paxos_replica_num, const common::ObMemberList &member_list, const common::ObMember &arb_member);
  void report_force_set_as_single_replica_(const int64_t prev_replica_num, const int64_t curr_replica_num, const ObMember &member);
  void report_change_replica_num_(const int64_t prev_replica_num, const int64_t curr_replica_num, const common::ObMemberList &member_list);
  void report_add_member_(const int64_t prev_replica_num, const int64_t curr_replica_num, const common::ObMember &added_member);
  void report_remove_member_(const int64_t prev_replica_num, const int64_t curr_replica_num, const common::ObMember &removed_member);
  void report_replace_member_(const common::ObMember &added_member,
                              const common::ObMember &removed_member,
                              const common::ObMemberList &member_list,
                              const char *event_name);
  void report_add_learner_(const common::ObMember &added_learner);
  void report_remove_learner_(const common::ObMember &removed_learner);
  void report_add_arb_member_(const common::ObMember &added_arb_member);
  void report_remove_arb_member_(const common::ObMember &removed_arb_member);
  void report_switch_learner_to_acceptor_(const common::ObMember &learner);
  void report_switch_acceptor_to_learner_(const common::ObMember &acceptor);
  void report_replace_learners_(const common::ObMemberList &added_learners,
                                const common::ObMemberList &removed_learners);
  // ======================= report event end =======================================
  bool check_need_hook_fetch_log_(const FetchLogType fetch_type, const LSN &start_lsn);
private:
  class ElectionMsgSender : public election::ElectionMsgSender
  {
  public:
    ElectionMsgSender(LogNetService &net_service) : net_service_(net_service) {};
    virtual int broadcast(const election::ElectionPrepareRequestMsg &msg,
                          const ObIArray<ObAddr> &list) const override final
    {
      int tmp_ret = common::OB_SUCCESS;
      for (int64_t idx = 0; idx < list.count(); ++idx) {
        const_cast<election::ElectionPrepareRequestMsg *>(&msg)->set_receiver(list.at(idx));
        if (OB_SUCCESS != (tmp_ret = net_service_.post_request_to_server_(list.at(idx), msg))) {
          PALF_LOG(INFO, "post prepare request msg failed", K(tmp_ret), "server", list.at(idx),
              K(msg));
        }
      }
      return common::OB_SUCCESS;
    }
    virtual int broadcast(const election::ElectionAcceptRequestMsg &msg,
                          const ObIArray<ObAddr> &list) const override final
    {
      int tmp_ret = common::OB_SUCCESS;
      for (int64_t idx = 0; idx < list.count(); ++idx) {
        const_cast<election::ElectionAcceptRequestMsg *>(&msg)->set_receiver(list.at(idx));
        if (OB_SUCCESS != (tmp_ret = net_service_.post_request_to_server_(list.at(idx), msg))) {
          PALF_LOG(INFO, "post accept request msg failed", K(tmp_ret), "server", list.at(idx),
              K(msg));
        }
      }
      return common::OB_SUCCESS;
    }
    virtual int send(const election::ElectionPrepareResponseMsg &msg) const override final
    {
      return net_service_.post_request_to_server_(msg.get_receiver(), msg);
    }
    virtual int send(const election::ElectionAcceptResponseMsg &msg) const override final
    {
      return net_service_.post_request_to_server_(msg.get_receiver(), msg);
    }
    virtual int send(const election::ElectionChangeLeaderMsg &msg) const override final
    {
      return net_service_.post_request_to_server_(msg.get_receiver(), msg);
    }
  private:
    LogNetService &net_service_;
  };
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef common::ObSpinLock SpinLock;
  typedef common::ObSpinLockGuard SpinLockGuard;
  typedef common::RWLock::WLockGuardWithTimeout WLockGuardWithTimeout;
  enum LogFlashbackState
  {
    FLASHBACK_INIT = 0,
    FLASHBACK_SUCCESS = 1,
    FLASHBACK_FAILED = 2,
    FLASHBACK_RECONFIRM = 3,
  };
private:
  mutable RWLock lock_;
  char log_dir_[common::MAX_PATH_SIZE];
  LogSlidingWindow sw_;
  LogConfigMgr config_mgr_;
  LogModeMgr mode_mgr_;
  LogStateMgr state_mgr_;
  LogReconfirm reconfirm_;
  LogEngine log_engine_;
  ElectionMsgSender election_msg_sender_;
  election::ElectionImpl election_;
  LogHotCache hot_cache_;
  FetchLogEngine *fetch_log_engine_;
  common::ObILogAllocator *allocator_;
  int64_t palf_id_;
  common::ObAddr self_;
  palf::PalfFSCbWrapper fs_cb_wrapper_;
  palf::PalfRoleChangeCbWrapper role_change_cb_wrpper_;
  palf::PalfRebuildCbWrapper rebuild_cb_wrapper_;
  LogPlugins plugins_;
  // ======optimization for locate_by_scn_coarsely=========
  mutable SpinLock last_locate_lock_;
  share::SCN last_locate_scn_;
  block_id_t last_locate_block_;
  // ======optimization for locate_by_scn_coarsely=========
  int64_t cannot_recv_log_warn_time_;
  int64_t cannot_handle_committed_info_time_;
  int64_t log_disk_full_warn_time_;
  int64_t last_check_parent_child_time_us_;
  int64_t wait_slide_print_time_us_;
  int64_t append_size_stat_time_us_;
  int64_t replace_member_print_time_us_;
  mutable int64_t config_change_print_time_us_;
  mutable SpinLock last_rebuild_meta_info_lock_;//protect last_rebuild_lsn_ and last_rebuild_meta_info_
  LSN last_rebuild_lsn_;
  RebuildMetaInfo last_rebuild_meta_info_;//used for double checking whether it is necessary to rebuild
  LSN last_record_append_lsn_;
  // NB: only set has_set_deleted_ to true when this palf_handle has been deleted.
  bool has_set_deleted_;
  IPalfEnvImpl *palf_env_impl_;
  bool diskspace_enough_;
  ObMiniStat::ObStatItem append_cost_stat_;
  ObMiniStat::ObStatItem flush_cb_cost_stat_;
  int64_t last_accum_write_statistic_time_;
  int64_t accum_write_log_size_;  // the accum size of written logs
  int64_t last_accum_fetch_statistic_time_;
  int64_t accum_fetch_log_size_;
  // a spin lock for read/write replica_meta mutex
  SpinLock replica_meta_lock_;
  SpinLock rebuilding_lock_;
  SpinLock config_change_lock_;
  SpinLock mode_change_lock_;
  // a spin lock for single replica mutex
  SpinLock flashback_lock_;
  int64_t last_dump_info_time_us_;
  LogFlashbackState flashback_state_;
  int64_t last_check_sync_time_us_;
  int64_t last_renew_loc_time_us_;
  int64_t last_print_in_sync_time_us_;
  int64_t last_hook_fetch_log_time_us_;
  int64_t chaning_config_warn_time_;
  bool cached_is_in_sync_;
  bool has_higher_prio_config_change_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase
#endif // OCEANBASE_LOGSERVICE_LOG_SERVICE_
