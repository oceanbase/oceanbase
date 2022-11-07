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
#include "palf_callback_wrapper.h"
#include "log_engine.h"                      // LogEngine
#include "log_meta.h"
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
class PalfEnvImpl;

struct PalfStat {
  common::ObAddr self_;
  int64_t palf_id_;
  common::ObRole role_;
  int64_t log_proposal_id_;
  LogConfigVersion config_version_;
  AccessMode access_mode_;
  ObMemberList paxos_member_list_;
  int64_t paxos_replica_num_;
  bool allow_vote_;
  LogReplicaType replica_type_;
  LSN begin_lsn_;
  int64_t begin_ts_ns_;
  LSN base_lsn_;
  LSN end_lsn_;
  int64_t end_ts_ns_;
  LSN max_lsn_;
  int64_t max_ts_ns_;
  TO_STRING_KV(K_(self), K_(palf_id), K_(role), K_(log_proposal_id), K_(config_version),
      K_(access_mode), K_(paxos_member_list), K_(paxos_replica_num), K_(allow_vote),
      K_(replica_type), K_(base_lsn), K_(end_lsn), K_(end_ts_ns), K_(max_lsn));
};

struct PalfDiagnoseInfo {
  common::ObRole election_role_;
  int64_t election_epoch_;
  common::ObRole palf_role_;
  palf::ObReplicaState palf_state_;
  int64_t palf_proposal_id_;
  TO_STRING_KV(K(election_role_),
               K(election_epoch_),
               K(palf_role_),
               K(palf_state_),
               K(palf_proposal_id_));
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
    return this->compare(palf_id);
  }
  bool operator!=(const LSKey &palf_id) const
  {
    return this->compare(palf_id);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&hash_val, sizeof(id_), id_);
    return hash_val;
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


// 日志服务的接口类，logservice以外的模块使用日志服务，只允许调用IPalfHandleImpl的接口
class IPalfHandleImpl : public common::LinkHashValue<LSKey>
{
public:
  IPalfHandleImpl() {}
  virtual ~IPalfHandleImpl() {}
public:
  // after creating palf successfully, set initial memberlist(can only be called once)
  //
  // @param [in] member_list, paxos memberlist
  // @param [in] paxos_replica_num, number of paxos replicas
  //
  // @return :TODO
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const int64_t paxos_replica_num) = 0;
  // after creating palf which includes arbitration replica successfully,
  // set initial memberlist(can only be called once)
  // @param [in] ObMemberList, the initial member list, do not include arbitration replica
  // @param [in] arb_member, arbitration replica
  // @param [in] paxos_replica_num, number of paxos replicas
  // @return :TODO
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const common::ObMember &arb_member,
                                      const int64_t paxos_replica_num) = 0;
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
  // @param [in] ref_ts_ns, 日志对应的时间，满足弱读需求
  //
  // 下述两个值通过submit_log出参，而不是on_success()和上层交互，好处是类似于lock_for_read逻辑可以更早
  // 的拿到准确的版本号信息
  // @param [out] lsn, 日志的唯一标识符
  //                          主要使用场景是prepare日志中记录redo日志的lsn，用于数据链路回拉历史日志时定位使用
  // @param [out] log_timestamp, 日志对应的submit_timestamp，主要用于事务版本号，比如lock_for_read场景使用
  //
  // @return :TODO
  virtual int submit_log(const PalfAppendOptions &opts,
                         const char *buf,
                         const int64_t buf_len,
                         const int64_t ref_ts_ns,
                         LSN &lsn,
                         int64_t &log_timestamp) = 0;
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

  // @brief: a special config change interface, change replica number of paxos group
  // @param[in] common::ObMemberList: current memberlist, for pre-check
  // @param[in] const int64_t curr_replica_num: current replica num, for pre-check
  // @param[in] const int64_t new_replica_num: new replica num
  // @param[in] const int64_t timeout_ns: timeout, ns
  // @return
  // - OB_SUCCESS: change_replica_num successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: change_replica_num timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int change_replica_num(const common::ObMemberList &member_list,
                                 const int64_t curr_replica_num,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_ns) = 0;

  // @brief, add a member into paxos group
  // @param[in] common::ObMember &member: member which will be added
  // @param[in] const int64_t new_replica_num: replica number of paxos group after adding 'member'
  // @param[in] const int64_t timeout_ns: add member timeout, ns
  // @return
  // - OB_SUCCESS: add member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: add member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int add_member(const common::ObMember &member,
                        const int64_t new_replica_num,
                        const int64_t timeout_ns) = 0;

  // @brief, remove a member from paxos group
  // @param[in] common::ObMember &member: member which will be removed
  // @param[in] const int64_t new_replica_num: replica number of paxos group after removing 'member'
  // @param[in] const int64_t timeout_ns: remove member timeout, ns
  // @return
  // - OB_SUCCESS: remove member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int remove_member(const common::ObMember &member,
                            const int64_t new_replica_num,
                            const int64_t timeout_ns) = 0;

  // @brief, replace old_member with new_member
  // @param[in] const common::ObMember &added_member: member wil be added
  // @param[in] const common::ObMember &removed_member: member will be removed
  // @param[in] const int64_t timeout_ns
  // @return
  // - OB_SUCCESS: replace member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int replace_member(const common::ObMember &added_member,
                             const common::ObMember &removed_member,
                             const int64_t timeout_ns) = 0;

  // @brief: add a learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &added_learner: learner will be added
  // @param[in] const int64_t timeout_ns
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: add_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int add_learner(const common::ObMember &added_learner, const int64_t timeout_ns) = 0;

  // @brief: remove a learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &removed_learner: learner will be removed
  // @param[in] const int64_t timeout_ns
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: remove_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int remove_learner(const common::ObMember &removed_learner, const int64_t timeout_ns) = 0;

  // @brief: switch a learner(read only replica) to acceptor(full replica) in this clsuter
  // @param[in] const common::ObMember &learner: learner will be switched to acceptor
  // @param[in] const int64_t timeout_ns
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: switch_learner_to_acceptor timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int switch_learner_to_acceptor(const common::ObMember &learner, const int64_t timeout_ns) = 0;

  // @brief: switch an acceptor(full replica) to learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &member: acceptor will be switched to learner
  // @param[in] const int64_t timeout_ns
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: switch_acceptor_to_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int switch_acceptor_to_learner(const common::ObMember &member, const int64_t timeout_ns) = 0;

  // @brief, add an arbitration member to paxos group
  // @param[in] common::ObMember &member: arbitration member which will be added
  // @param[in] const int64_t paxos_replica_num: replica number of paxos group after adding 'member'
  // @param[in] const int64_t timeout_us: add member timeout, us
  // @return
  // - OB_SUCCESS: add arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: add arbitration member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int add_arb_member(const common::ObMember &added_member,
                             const int64_t paxos_replica_num,
                             const int64_t timeout_us) = 0;
  // @brief, remove an arbitration member from paxos group
  // @param[in] common::ObMember &member: arbitration member which will be removed
  // @param[in] const int64_t paxos_replica_num: replica number of paxos group after removing 'member'
  // @param[in] const int64_t timeout_us: remove member timeout, us
  // @return
  // - OB_SUCCESS: remove arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove arbitration member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int remove_arb_member(const common::ObMember &arb_member,
                                const int64_t paxos_replica_num,
                                const int64_t timeout_us) = 0;
  // @brief, replace old arbitration member with new arbitration member, can be called in any member
  // @param[in] const common::ObMember &added_member: arbitration member wil be added
  // @param[in] const common::ObMember &removed_member: arbitration member will be removed
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace arbitration member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  virtual int replace_arb_member(const common::ObMember &added_arb_member,
                                 const common::ObMember &removed_arb_member,
                                 const int64_t timeout_us) = 0;
  // @brief: degrade an acceptor(full replica) to learner(special read only replica) in this cluster
  // @param[in] const common::ObMemberList &member_list: acceptors will be degraded to learner
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: timeout
  // - OB_NOT_MASTER: not leader
  virtual int degrade_acceptor_to_learner(const common::ObMemberList &member_list, const int64_t timeout_ns) = 0;
  // @brief: upgrade a learner(special read only replica) to acceptor(full replica) in this cluster
  // @param[in] const common::ObMemberList &learner_list: learners will be upgraded to acceptors
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: timeout
  // - OB_NOT_MASTER: not leader
  virtual int upgrade_learner_to_acceptor(const common::ObMemberList &learner_list, const int64_t timeout_ns) = 0;

  // 设置日志文件的可回收位点，小于等于lsn的日志文件均可以安全回收
  //
  // @param [in] lsn，可回收的日志文件位点
  //
  // @return :TODO
  virtual int set_base_lsn(const LSN &lsn) = 0;
  // 迁移/rebuild场景目的端推进base_lsn
  //
  // @param [in] palf_base_info，可回收的日志文件位点
  virtual int advance_base_info(const PalfBaseInfo &palf_base_info, const bool is_rebuild) = 0;

  // @desc: query coarse lsn by ts(ns), that means there is a LogGroupEntry in disk,
  // its lsn and log_ts are result_lsn and result_ts_ns, and result_ts_ns <= ts_ns.
  //        result_lsn   result_ts_ns
  //                 \   /
  //      [log 1]     [log 2][log 3] ... [log n]  [log n+1]
  //  -------------------------------------------|-------------> time
  //                                           ts_ns
  // Note that this function may be time-consuming
  // Note that result_lsn always points to head of log file
  // @params [in] ts_ns: timestamp(nano second)
  // @params [out] result_lsn: the lower bound lsn which includes ts_ns
  // @return
  // - OB_SUCCESS: locate_by_ts_ns_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ENTRY_NOT_EXIST: there is no log in disk
  // - OB_ERR_OUT_OF_LOWER_BOUND: ts_ns is too old, log files may have been recycled
  // - others: bug
  virtual int locate_by_ts_ns_coarsely(const int64_t ts_ns, LSN &result_lsn) = 0;

  // @desc: query coarse ts by lsn, that means there is a LogGroupEntry in disk,
  // its lsn and log_ts are result_lsn and result_ts_ns, and result_lsn <= lsn.
  //  result_lsn    result_ts_ns
  //           \    /
  //    [log 1][log 2][log 3][log 4][log 5]...[log n][log n+1]
  //  --------------------------------------------|-------------> lsn
  //                                             lsn
  // Note that this function may be time-consuming
  // @params [in] lsn: lsn
  // @params [out] result_ts_ns: the lower bound timestamp which includes lsn
  // - OB_SUCCESS; locate_by_lsn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ERR_OUT_OF_LOWER_BOUND: lsn is too small, log files may have been recycled
  // - others: bug
  virtual int locate_by_lsn_coarsely(const LSN &lsn, int64_t &result_ts_ns) = 0;
  virtual int get_begin_lsn(LSN &lsn) const = 0;
  virtual int get_begin_ts_ns(int64_t &ts) const = 0;
  virtual int get_base_info(const LSN &base_lsn, PalfBaseInfo &base_info) = 0;

  virtual int get_min_block_info_for_gc(block_id_t &min_block_id, int64_t &max_ts_ns) = 0;
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
  virtual int64_t get_max_ts_ns() const = 0;
  virtual int64_t get_end_ts_ns() const = 0;
  virtual int get_last_rebuild_lsn(LSN &last_rebuild_lsn) const = 0;
  virtual int64_t get_total_used_disk_space() const = 0;
  virtual const LSN &get_base_lsn_used_for_block_gc() const = 0;

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
  virtual int inner_append_log(const LSN &lsn,
                               const LogWriteBuf &write_buf,
                               const int64_t log_ts) = 0;
  virtual int inner_append_log(const LSNArray &lsn_array,
                               const LogWriteBufArray &write_buf_array,
                               const LogTsArray &log_ts_array) = 0;
  virtual int inner_append_meta(const char *buf,
                                const int64_t buf_len) = 0;
  virtual int inner_truncate_log(const LSN &lsn) = 0;
  virtual int inner_truncate_prefix_blocks(const LSN &lsn) = 0;
  virtual int handle_prepare_request(const common::ObAddr &server,
                                     const int64_t &proposal_id) = 0;
  virtual int handle_prepare_response(const common::ObAddr &server,
                                      const int64_t &proposal_id,
                                      const bool vote_granted,
                                      const int64_t &accept_proposal_id,
                                      const LSN &last_lsn,
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
                                     const int64_t accepted_mode_pid) = 0;
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
                                const LogModeMeta &meta) = 0;
  virtual int ack_mode_meta(const common::ObAddr &server,
                            const int64_t proposal_id) = 0;
  virtual int handle_notify_rebuild_req(const common::ObAddr &server,
                                        const LSN &base_lsn,
                                        const LogInfo &base_prev_log_info) = 0;
  virtual int get_memberchange_status(const ObAddr &server,
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
                                 const int64_t ref_ts_ns) = 0;
  virtual int get_access_mode(int64_t &mode_version, AccessMode &access_mode) const = 0;
  virtual int get_access_mode(AccessMode &access_mode) const = 0;
  virtual int handle_committed_info(const common::ObAddr &server,
                            const int64_t &msg_proposal_id,
                            const int64_t prev_log_id,
                            const int64_t &prev_log_proposal_id,
                            const LSN &committed_end_lsn) = 0;
  // @brief: store a persistent flag which means this paxos replica
  // can not reply ack when receiving logs.
  // By default, paxos replica can reply ack.
  virtual int disable_vote() = 0;
  // @brief: store a persistent flag which means this paxos replica
  // can reply ack when receiving logs.
  // By default, paxos replica can reply ack.
  virtual int enable_vote() = 0;

  // ===================== Iterator start =======================
  virtual int alloc_palf_buffer_iterator(const LSN &offset,
                                         PalfBufferIterator &iterator) = 0;
  virtual int alloc_palf_group_buffer_iterator(const LSN &offset,
                                               PalfGroupBufferIterator &iterator) = 0;
  virtual int alloc_palf_group_buffer_iterator(const int64_t ts_ns,
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
  virtual int stat(PalfStat &palf_stat) = 0;
  virtual int get_palf_epoch(int64_t &palf_epoch) const = 0;
  virtual int diagnose(PalfDiagnoseInfo &diagnose_info) const = 0;
};

class PalfHandleImpl : public IPalfHandleImpl
{
public:
  PalfHandleImpl();
  ~PalfHandleImpl() override;
  int init(const int64_t palf_id,
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
           const int64_t palf_epoch);
  bool check_can_be_used() const;
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
           PalfEnvImpl *palf_env_impl,
           const common::ObAddr &self,
           common::ObOccamTimer *election_timer,
           const int64_t palf_epoch);
  void destroy();
  int start();
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const int64_t paxos_replica_num) override final;
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const common::ObMember &arb_member,
                              const int64_t paxos_replica_num) override final;
  int set_region(const common::ObRegion &region) override final;
  int set_paxos_member_region_map(const LogMemberRegionMap &region_map) override final;
  int submit_log(const PalfAppendOptions &opts,
                 const char *buf,
                 const int64_t buf_len,
                 const int64_t ref_ts_ns,
                 LSN &lsn,
                 int64_t &log_timestamp) override final;

  int submit_group_log(const PalfAppendOptions &opts,
                       const LSN &lsn,
                       const char *buf,
                       const int64_t buf_len) override final;
  int get_role(common::ObRole &role,
               int64_t &proposal_id,
               bool &is_pending_state) const override final;
  int change_leader_to(const common::ObAddr &dest_addr) override final;
  int get_global_learner_list(common::GlobalLearnerList &learner_list) const override final;
  int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const override final;
  int change_replica_num(const common::ObMemberList &member_list,
                         const int64_t curr_replica_num,
                         const int64_t new_replica_num,
                         const int64_t timeout_ns) override final;
  int add_member(const common::ObMember &member,
                const int64_t new_replica_num,
                const int64_t timeout_ns) override final;
  int remove_member(const common::ObMember &member,
                    const int64_t new_replica_num,
                    const int64_t timeout_ns) override final;
  int replace_member(const common::ObMember &added_member,
                     const common::ObMember &removed_member,
                     const int64_t timeout_ns) override final;
  int add_learner(const common::ObMember &added_learner,
                  const int64_t timeout_ns) override final;
  int remove_learner(const common::ObMember &removed_learner,
                  const int64_t timeout_ns) override final;
  int switch_learner_to_acceptor(const common::ObMember &learner,
                                 const int64_t timeout_ns) override final;
  int switch_acceptor_to_learner(const common::ObMember &member,
                                 const int64_t timeout_ns) override final;
  int add_arb_member(const common::ObMember &added_member,
                     const int64_t paxos_replica_num,
                     const int64_t timeout_us) override final;
  int remove_arb_member(const common::ObMember &arb_member,
                        const int64_t paxos_replica_num,
                        const int64_t timeout_us) override final;
  int replace_arb_member(const common::ObMember &added_arb_member,
                         const common::ObMember &removed_arb_member,
                         const int64_t timeout_us) override final;
  int degrade_acceptor_to_learner(const common::ObMemberList &member_list, const int64_t timeout_ns) override final;
  int upgrade_learner_to_acceptor(const common::ObMemberList &learner_list, const int64_t timeout_ns) override final;
  int set_base_lsn(const LSN &lsn) override final;
  int enable_sync();
  int disable_sync();
  bool is_sync_enabled() const;
  int advance_base_info(const PalfBaseInfo &palf_base_info, const bool is_rebuild) override final;
  int locate_by_ts_ns_coarsely(const int64_t ts_ns, LSN &result_lsn) override final;
  int locate_by_lsn_coarsely(const LSN &lsn, int64_t &result_ts_ns) override final;
  void set_deleted();
  int disable_vote() override final;
  int enable_vote() override final;
public:
  int delete_block(const block_id_t &block_id);
  int read_log(const LSN &lsn,
               const int64_t in_read_size,
               ReadBuf &read_buf,
               int64_t &out_read_size) override final;
  int set_scan_disk_log_finished() override;
  int change_access_mode(const int64_t proposal_id,
                         const int64_t mode_version,
                         const AccessMode &access_mode,
                         const int64_t ref_ts_ns) override final;
  int get_access_mode(int64_t &mode_version, AccessMode &access_mode) const override final;
  int get_access_mode(AccessMode &access_mode) const override final;
  // =========================== Iterator start ============================
  int alloc_palf_buffer_iterator(const LSN &offset, PalfBufferIterator &iterator) override final;
  int alloc_palf_group_buffer_iterator(const LSN &offset, PalfGroupBufferIterator &iterator) override final;
  int alloc_palf_group_buffer_iterator(const int64_t ts_ns, PalfGroupBufferIterator &iterator) override final;
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
  int set_election_priority(election::ElectionPriority *priority) override final;
  int reset_election_priority() override final;
  // ==================== Callback end ========================
public:
  int get_begin_lsn(LSN &lsn) const;
  int get_begin_ts_ns(int64_t &ts) const;
  int get_base_info(const LSN &base_lsn, PalfBaseInfo &base_info);
  int get_min_block_info_for_gc(block_id_t &min_block_id, int64_t &max_ts_ns) override final;
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

  int64_t get_max_ts_ns() const override final
  {
    return sw_.get_max_log_ts();
  }

  int64_t get_end_ts_ns() const override final
  {
    // 基于实现复杂度考虑，直接用last_slide_log_ts作为end_ts
    // 否则需要在match_lsn_map中额外维护log_ts
    return sw_.get_last_slide_log_ts();
  }
  int get_last_rebuild_lsn(LSN &last_rebuild_lsn) const override final;
  int64_t get_total_used_disk_space() const;
  // return the smallest recycable lsn
  const LSN &get_base_lsn_used_for_block_gc() const
  {
    return log_engine_.get_base_lsn_used_for_block_gc();
  }
  // =====================  LogIOTask start ==========================
  int inner_after_flush_log(const FlushLogCbCtx &flush_log_cb_ctx) override final;
  int inner_after_truncate_log(const TruncateLogCbCtx &truncate_log_cb_ctx) override final;
  int inner_after_flush_meta(const FlushMetaCbCtx &flush_meta_cb_ctx) override final;
  int inner_after_truncate_prefix_blocks(const TruncatePrefixBlocksCbCtx &truncate_prefix_cb_ctx) override final;
  int advance_reuse_lsn(const LSN &flush_log_end_lsn);
  int inner_append_log(const LSN &lsn,
                       const LogWriteBuf &write_buf,
                       const int64_t log_ts) override final;
  int inner_append_log(const LSNArray &lsn_array,
                       const LogWriteBufArray &write_buf_array,
                       const LogTsArray &log_ts_array);
  int inner_append_meta(const char *buf,
                        const int64_t buf_len) override final;
  int inner_truncate_log(const LSN &lsn) override final;
  int inner_truncate_prefix_blocks(const LSN &lsn);
  // ==================================================================
  int check_and_switch_state();
  int check_and_switch_freeze_mode();
  int period_freeze_last_log();
  int handle_prepare_request(const common::ObAddr &server,
                             const int64_t &proposal_id) override final;
  int handle_prepare_response(const common::ObAddr &server,
                              const int64_t &proposal_id,
                              const bool vote_granted,
                              const int64_t &accept_proposal_id,
                              const LSN &last_lsn,
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
                             const int64_t accepted_mode_pid);
  int receive_config_log(const common::ObAddr &server,
                         const int64_t &msg_proposal_id,
                         const int64_t &prev_log_proposal_id,
                         const LSN &prev_lsn,
                         const int64_t &prev_mode_pid,
                         const LogConfigMeta &meta);
  int ack_config_log(const common::ObAddr &server,
                     const int64_t proposal_id,
                     const LogConfigVersion &config_version);
  int receive_mode_meta(const common::ObAddr &server,
                        const int64_t proposal_id,
                        const LogModeMeta &meta);
  int ack_mode_meta(const common::ObAddr &server,
                     const int64_t proposal_id);
  int handle_notify_rebuild_req(const common::ObAddr &server,
                                const LSN &base_lsn,
                                const LogInfo &base_prev_log_info) override final;
  int handle_committed_info(const common::ObAddr &server,
                            const int64_t &msg_proposal_id,
                            const int64_t prev_log_id,
                            const int64_t &prev_log_proposal_id,
                            const LSN &committed_end_lsn) override;
  int get_memberchange_status(const ObAddr &server,
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
  int get_palf_epoch(int64_t &palf_epoch) const;
  int diagnose(PalfDiagnoseInfo &diagnose_info) const;
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
                   LogIOWorker *log_io_worker,
                   PalfEnvImpl *palf_env_impl,
                   common::ObOccamTimer *election_timer);
  int check_req_proposal_id_(const int64_t &proposal_id);
  int after_flush_prepare_meta_(const int64_t &proposal_id);
  int after_flush_config_change_meta_(const int64_t proposal_id, const LogConfigVersion &config_version);
  int after_flush_mode_meta_(const int64_t proposal_id, const LogModeMeta &mode_meta);
  int after_flush_snapshot_meta_(const LSN &lsn);
  int after_flush_replica_property_meta_(const bool allow_vote);
  int set_allow_vote_flag_(const bool allow_vote);
  int get_prev_log_info_(const LSN &lsn, LogInfo &log_info);
  int submit_prepare_response_(const common::ObAddr &server,
                               const int64_t &proposal_id);
  int construct_palf_base_info_(const LSN &max_committed_lsn,
                                  PalfBaseInfo &palf_base_info);
  int append_disk_log_to_sw_(const LSN &start_lsn);
  int try_send_committed_info_(const common::ObAddr &server,
                               const LSN &log_lsn,
                               const LSN &log_end_lsn,
                               const int64_t &log_proposal_id);
  int fetch_log_from_storage_(const common::ObAddr &server,
                              const FetchLogType fetch_type,
                              const int64_t &msg_proposal_id,
                              const LSN &prev_lsn,
                              const LSN &fetch_start_lsn,
                              const int64_t fetch_log_size,
                              const int64_t fetch_log_count);
  int submit_fetch_log_resp_(const common::ObAddr &server,
                             const int64_t &msg_proposal_id,
                             const int64_t &prev_log_proposal_id,
                             const LSN &prev_lsn,
                             const LSN &curr_lsn,
                             const LogGroupEntry &curr_group_entry);
  int submit_fetch_mode_meta_resp_(const common::ObAddr &server,
                                   const int64_t msg_proposal_id,
                                   const int64_t accepted_mode_pid);
  int try_update_proposal_id_(const common::ObAddr &server,
                              const int64_t &proposal_id);
  //int get_binary_search_range_(const int64_t ts_ns, LSN &min_lsn, LSN &max_lsn, LSN &result_lsn);
  int get_binary_search_range_(const int64_t ts_ns,
                               block_id_t &min_block_id,
                               block_id_t &max_block_id,
                               block_id_t &result_block_id);
  void inc_update_last_locate_block_ts_(const block_id_t &block_id, const int64_t ts);
  int can_change_config_(const LogConfigChangeArgs &args, int64_t &proposal_id);
  int check_args_and_generate_config_(const LogConfigChangeArgs &args,
                                      bool &is_already_finished,
                                      common::ObMemberList &log_sync_memberlist,
                                      int64_t &log_sync_repclia_num) const;
  int sync_get_committed_end_lsn_(const LogConfigChangeArgs &args,
                                  const ObMemberList &new_member_list,
                                  const int64_t new_replica_num,
                                  const int64_t conn_timeout_ns,
                                  LSN &committed_end_lsn,
                                  bool &added_member_has_new_version);
  bool check_follower_sync_status_(const LogConfigChangeArgs &args,
                                   const ObMemberList &new_member_list,
                                   const int64_t new_replica_num,
                                   const int64_t half_timeout_us,
                                   bool &added_member_has_new_version);
  int one_stage_config_change_(const LogConfigChangeArgs &args, const int64_t timeout_ns);
  int check_need_rebuild_(const LSN &base_lsn,
                          const LogInfo &base_prev_log_info,
                          bool &need_rebuild,
                          bool &need_fetch_log);
  int check_need_advance_base_info_(const LSN &base_lsn,
                                    const LogInfo &base_prev_log_info,
                                    const bool is_rebuild);
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
  FetchLogEngine *fetch_log_engine_;
  common::ObILogAllocator *allocator_;
  int64_t palf_id_;
  common::ObAddr self_;
  palf::PalfFSCbWrapper fs_cb_wrapper_;
  palf::PalfRoleChangeCbWrapper role_change_cb_wrpper_;
  palf::PalfRebuildCbWrapper rebuild_cb_wrapper_;
  PalfLocationCacheCb *lc_cb_;
  // ======optimization for locate_by_ts_ns_coarsely=========
  mutable SpinLock last_locate_lock_;
  int64_t last_locate_ts_ns_;
  block_id_t last_locate_block_;
  // ======optimization for locate_by_ts_ns_coarsely=========
  int64_t cannot_recv_log_warn_time_;
  int64_t cannot_handle_committed_info_time_;
  int64_t log_disk_full_warn_time_;
  int64_t last_check_parent_child_ts_us_;
  int64_t wait_slide_print_time_us_;
  int64_t append_size_stat_time_us_;
  int64_t replace_member_print_time_us_;
  int64_t config_change_print_time_us_;
  mutable SpinLock last_rebuild_lsn_lock_;
  LSN last_rebuild_lsn_;
  LSN last_record_append_lsn_;
  // NB: only set has_set_deleted_ to true when this palf_handle has been deleted.
  bool has_set_deleted_;
  PalfEnvImpl *palf_env_impl_;
  ObMiniStat::ObStatItem append_cost_stat_;
  ObMiniStat::ObStatItem flush_cb_cost_stat_;
  // a spin lock for read/write replica_meta mutex
  SpinLock replica_meta_lock_;
  SpinLock rebuilding_lock_;
  SpinLock config_change_lock_;
  SpinLock mode_change_lock_;
  int64_t last_dump_info_ts_us_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_SERVICE_
