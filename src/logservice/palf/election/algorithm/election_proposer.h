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

#ifndef LOGSERVICE_PALF_ELECTION_ALGORITHM_OB_ELECTION_PROPOSER_H
#define LOGSERVICE_PALF_ELECTION_ALGORITHM_OB_ELECTION_PROPOSER_H

#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/election/interface/election_msg_handler.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/utils/election_utils.h"
#include "logservice/palf/election/message/election_message.h"
#include "ob_clock_generator.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

class ResponseChecker;
class ElectionPrepareRequestMsg;
class ElectionAcceptRequestMsg;
class ElectionChangeLeaderMsg;
class ElectionImpl;
class DefaultRoleChangeCallBack;

class ElectionProposer
{
  friend class ResponseChecker;
  friend class ElectionPrepareRequestMsg;
  friend class ElectionAcceptRequestMsg;
  friend class ElectionChangeLeaderMsg;
  friend class ElectionImpl;
  friend class DefaultRoleChangeCallBack;
public:
  ElectionProposer(ElectionImpl *election);
  int init(const int64_t restart_counter);
  /**
   * @description: 为选举设置新的成员列表
   * @param {MemberList} &new_member_list
   * @return {int} OB_INVALID_ARGUMENT : 新成员列表的版本号比当前成员列表的版本号更小
                   OB_OP_NOT_ALLOWED : 该副本是leader，且当前成员版本号未推给多数派，不允许成员变更操作
                   others : from MemberListWithStates
   * @Date: 2021-12-23 11:33:50
   */
  int set_member_list(const MemberList &new_member_list);
  int change_leader_to(const ObAddr &dest_addr);
  int start();
  void stop();
  /**
   * @description: 该接口检查自己是不是leader，如果检查自己是leader
   *               输出一个epoch，调用端认为该epoch就是调用此接口时的epoch即可
   * @param {int64_t} *leader_elected_ballot 输出的epoch
   * @return {bool} true：自己是leader，false：自己不是leader
   * @Date: 2021-12-22 15:11:14
   */
  bool check_leader(int64_t *epoch = nullptr) const
  {
    int ret = false;
    int64_t current_ts = get_monotonic_ts();
    int64_t lease;
    int64_t exposed_epoch;
    leader_lease_and_epoch_.get(lease, exposed_epoch);
    if (OB_LIKELY(current_ts < lease)) {
      ret = true;
      if (OB_NOT_NULL(epoch)) {
        *epoch = exposed_epoch;
      }
    }
    return ret;
  }
  int revoke(const RoleChangeReason &reason);
public:
  // 发prepare请求
  void prepare(const common::ObRole role);
  // 收到其他proposer发出的prepare请求，一呼百应
  void on_prepare_request(const ElectionPrepareRequestMsg &prepare_req,
                          bool *need_register_devote_task);
  // 收到来自acceptor接收prepare请求后的响应
  void on_prepare_response(const ElectionPrepareResponseMsg &prepare_res);
  // 发accept请求
  void propose();
  // 收到来自acceptor接收accept请求后的响应
  void on_accept_response(const ElectionAcceptResponseMsg &accept_res);
  void inner_change_leader_to(const common::ObAddr &dst);
  void on_change_leader(const ElectionChangeLeaderMsg &change_leader_msg);
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  bool leader_revoke_if_lease_expired_(RoleChangeReason reason);
  bool leader_takeover_if_lease_valid_(RoleChangeReason reason);
  void advance_ballot_number_and_reset_related_states_(const int64_t new_ballot_number,
                                                       const char *reason);
  int reschedule_or_register_prepare_task_after_(const int64_t delay_us);
  int register_renew_lease_task_();
  bool is_self_in_memberlist_() const;
  int prepare_change_leader_to_(const ObAddr &dest_addr, const ObStringHolder &reason);
private:
  // 抽象该数据结构以达成以下目的:
  // lease与epoch是关联的变量值，需要原子的被外界获取以用于区分leader切换的ABA场景（更严格的讲，为保证正确性，外界至少要读取到一个lease值以及一个不会比该lease值更旧的epoch值）
  // 这里使用seq lock实现了对两个变量的原子读
  // leader使用续约成功的ballot number更新epoch，通常ballot number是稳定不变的，但是在个别场景下频繁发生变化时可能出现PALF频繁卸任的情况，这里且举两例：
  // 1. 某副本与leader断连，它无法被正常续约，但仍与其他副本保持联系，它将不断的使用更大的ballot number进行prepare动作推高其他副本的ballot number从而迫使leader也抬高ballot number
  // 2. 成员变更加副本时，新加副本尚不在集群其他副本与leader的成员列表中，但是新加副本的成员列表已更新且周期性进行prepare动作抬高集群和leader的ballot number
  // 当发生以上情况时，理论上大部分场景下可以避免暴露给外界的epoch被推大，只要：
  // 【Leader的lease在续约过程中从未中断过，即，确认不可能有其他的leader产生】
  // 精准判断lease不曾中断必须要依赖于double check机制，更新的过程如下：
  // 1. 当前lease仍然有效。
  // 2. 更新lease值但不更新epoch（此时不可被外界读取，否则若lease在更新过程中失效，外界将获取到新的lease与旧的epoch）。
  // 3. 再次检查仍然在旧lease的有效范围内。
  // 4. 可被外界读取。
  // 该数据结构抽象set_lease_and_epoch_if_lease_expired_or_just_set_lease()接口配合seq lock实现上述语义。
  struct LeaderLeaseAndEpoch  {
    LeaderLeaseAndEpoch() : lease_(INVALID_VALUE), epoch_(INVALID_VALUE), seq_(INVALID_VALUE) {}
    void set_lease_and_epoch_if_lease_expired_or_just_set_lease(const int64_t lease, const int64_t epoch)
    {
      int64_t old_lease;
      ++seq_;
      MEM_BARRIER();
      old_lease = lease_;
      if (OB_LIKELY(get_monotonic_ts() < old_lease)) {// lease没过期的时候，试图只set lease，不推进epoch
        lease_ = lease;
        if (OB_UNLIKELY(get_monotonic_ts() >= old_lease)) {// double check, 此时lease过期了，但该状态不会被外界读取到，必须要推进epoch
          epoch_ = epoch;
        }
      } else {// lease过期的时候，两个值一起修改
        lease_ = lease;
        epoch_ = epoch;
      }
      MEM_BARRIER();
      ++seq_;
    }
    void get(int64_t &lease, int64_t &epoch) const
    {
      int64_t seq = INVALID_VALUE;
      do {
        if (OB_UNLIKELY(seq != INVALID_VALUE)) {
          PAUSE();
        }
        seq = seq_;
        MEM_BARRIER();
        if (OB_LIKELY((seq & 1) != 0)) {
          lease = lease_;
          epoch = epoch_;
        }
        MEM_BARRIER();
      } while (OB_UNLIKELY(seq != seq_) || ((seq & 1) == 0));
    }
    bool is_valid() const
    {
      int64_t lease = INVALID_VALUE;
      int64_t epoch = INVALID_VALUE;
      get(lease, epoch);
      return lease != INVALID_VALUE && epoch != INVALID_VALUE;
    }
    void reset()
    {
      ++seq_;
      MEM_BARRIER();
      lease_ = INVALID_VALUE;
      epoch_ = INVALID_VALUE;
      MEM_BARRIER();
      ++seq_;
    }
    #define LEASE "leader_lease", TimeSpanWrapper(lease_)
    TO_STRING_KV(LEASE, K_(epoch));
    #undef LEASE
    int64_t lease_;
    int64_t epoch_;
    mutable int64_t seq_;// 与memory barrier配合，实现sequence lock，避免原子操作的开销
  };
  common::ObRole role_;
  // for follower
  int64_t ballot_number_;// proposer感知到的集群中存在的最大的选举轮次
  MemberListWithStates memberlist_with_states_;// 记录已知的acceptor的状态
  // for leader
  int64_t prepare_success_ballot_;// 被多数派prepare成功的选举轮次，该值将用于续约以及在lease第一次生效的场景下转化为对外的epoch
  LeaderLeaseAndEpoch leader_lease_and_epoch_;// Lease是在本机上的有效截止时间点，epoch是当选leader第一次lease生效时的ballot值
  // for change leader phase
  int64_t switch_source_leader_ballot_;// 旧主的ballot number
  common::ObAddr switch_source_leader_addr_;// 旧主的addr
  // for filter old message
  int64_t restart_counter_;// 日志流初始化的次数，从持久化meta中读取，为了识别宕机前的旧消息
  // 实现需要
  // ElectionImpl的指针，可以通过该指针调用外部回调、获取选举优先级等
  ElectionImpl * const p_election_;
  // 周期性进行无主选举和有主连任的定时任务的RAII句柄，在析构的时候会自动取消定时任务
  common::ObOccamTimerTaskRAIIHandle devote_task_handle_;
  common::ObOccamTimerTaskRAIIHandle renew_lease_task_handle_;
  // 通常情况下，所有副本的prepare定时任务都是被注册到相同的时间点执行的
  // 即便一开始所有副本的定时任务的执行时间点不完全相同，“一呼百应”也会将这些定时任务同步到同一个时间点
  //（因为进行一呼百应或者续lease成功后，应该重新计算下一次执行prepare定时任务的时间，这个计算和调整定时任务执行时间点的行为将会同步所有副本的prepare定时任务）
  // 上述行为是在设计里是妥当的，但会带来实现上的一个额外的并发问题：
  // 假如一个副本在同时收到了prepare消息，进行“百应”，同时它的prepare定时任务也被调度到了，要进行”一呼“
  // 由于所有的“一呼”定时任务都是同步的，因此有很大概率碰到这个并发调度的问题
  // 由于临界区的存在，两个操作是被串行化的，如果调度的顺序恰好是：先进行了“百应”，再进行“一呼”
  // 那么由于“一呼”要推大ballot number，这会中断当前的选举轮次
  // 即便在“百应”动作后会重新调整下一次“一呼”动作的调度时间，但是这次重调整操作与当前正要执行的“一呼”动作仍然是并发的，无法保证调度顺序
  // 所以这个逻辑仅靠定时任务是实现不了的，需要在用户执行定时任务的逻辑里作判断
  // 这里引入last_do_prepare_ts_变量，来记录一个proposer上次执行“一呼”或者“百应”的时间点
  // 下一次执行“一呼”或者“百应”动作的时间点距离上一次执行的时间点不应小于一个并发阈值
  int64_t last_do_prepare_ts_;
  int64_t last_dump_proposer_info_ts_;
  int64_t last_dump_election_msg_count_state_ts_;
  int64_t record_lease_interval_;// 用于感知lease是否发生了变化
  struct HighestPriorityMsgCache {
    HighestPriorityMsgCache() : cached_ts_(INVALID_VALUE) {}
    void set(const ElectionAcceptResponseMsg &rhs, const ObStringHolder &reason) {
      cached_msg_ = rhs;
      (void) cached_reason_.assign(reason);
      cached_ts_ = ObClockGenerator::getRealClock();
    }
    void check_expired() {
      if (cached_ts_ != INVALID_VALUE && ObClockGenerator::getRealClock() - cached_ts_ > CACHE_EXPIRATION_TIME) {
        reset();
      }
    }
    void reset() {
      cached_ts_ = INVALID_VALUE;
      cached_msg_.reset();
      cached_reason_.reset();
    }
    TO_STRING_KV(K_(cached_msg), K_(cached_reason), KTIME_(cached_ts));
    ElectionAcceptResponseMsg cached_msg_;// 在Leader上缓存优先级最高的副本的响应，在第二次收到该响应时触发切主，避免多次切主
    ObStringHolder cached_reason_;
    int64_t cached_ts_;// 缓存消息的时间点，缓存的消息具有时效性，需要在缓存失效前完成与优先级最高副本的第二次交互，超时清空
  } highest_priority_cache_;
};

}// namespace election
}// namespace palf
}// namesapce oceanbase
#endif
