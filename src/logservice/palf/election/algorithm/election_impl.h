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

#ifndef LOGSERVICE_PALF_ELECTION_ALGORITHM_OB_ELECTION_IMPL_H
#define LOGSERVICE_PALF_ELECTION_ALGORITHM_OB_ELECTION_IMPL_H

#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string_holder.h"
#include "common/ob_role.h"
#include "election_acceptor.h"
#include "election_proposer.h"
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/election/interface/election_msg_handler.h"
#include "logservice/palf/election/interface/election_priority.h"
#include "logservice/palf/election/utils/election_utils.h"
#include "logservice/palf/election/utils/election_args_checker.h"
#include "logservice/palf/election/message/election_message.h"
#include "logservice/palf/election/utils/election_event_recorder.h"

namespace oceanbase
{
namespace unittest
{
class TestElection;
}
namespace palf
{
namespace election
{

struct DefaultRoleChangeCallBack
{
  void operator()(ElectionImpl *, common::ObRole before, common::ObRole after, RoleChangeReason reason);
};

class RequestChecker;
class ElectionImpl : public Election
{
  friend class unittest::TestElection;
  friend class ElectionProposer;
  friend class ElectionAcceptor;
  friend class RequestChecker;
  friend class DefaultRoleChangeCallBack;
public:
  ElectionImpl();
  ~ElectionImpl();
  int init_and_start(const int64_t id,
                     common::ObOccamTimer *election_timer,
                     ElectionMsgSender *msg_handler,
                     const common::ObAddr &self_addr,
                     const uint64_t inner_priority_seed,
                     const int64_t restart_counter,
                     const ObFunction<int(const int64_t, const ObAddr &)> &prepare_change_leader_cb,
                     const ObFunction<void(ElectionImpl *, common::ObRole, common::ObRole, RoleChangeReason)> &cb = DefaultRoleChangeCallBack());
  virtual void stop() override final;
  virtual int can_set_memberlist(const palf::LogConfigVersion &new_config_version) const override final;
  virtual int set_memberlist(const MemberList &new_memberlist) override final;
  virtual int change_leader_to(const common::ObAddr &dest_addr) override final;
  virtual int temporarily_downgrade_protocol_priority(const int64_t time_us, const char *reason) override final;
  /**
   * @description: 返回选举对象当前的角色和epoch
   * @param {ObRole} &role 当前的角色，取LEADER或者FOLLOWER
   * @param {int64_t} &epoch 若当前角色为LEADER，则置为当前的epoch，否则置为0
   * @return {int} OB_SUCCESS always
   * @Date: 2021-12-23 15:46:06
   */
  virtual int get_role(common::ObRole &role, int64_t &epoch) const final
  {
    if (OB_LIKELY(proposer_.check_leader(&epoch))) {
      role = common::ObRole::LEADER;
    } else {
      role = common::ObRole::FOLLOWER;
      epoch = 0;
    }
    return common::OB_SUCCESS;
  }
  /**
   * @description: 获取当前的leader及其epoch，如果自己不是leader，则给出最可能是leader的对象及其epoch
   * @param {common::ObAddr} &addr 输出的leader ip，如果是自己，则是精准语义，如果不是自己，是likely语义
   * @param {int64_t} &cur_leader_epoch 输出的leader epoch，如果leader是自己，则是精准语义，如果不是自己，是likely语义
   * @return {int} OB_SUCCESS always
   * @Date: 2021-12-22 15:34:45
   */  
  virtual int get_current_leader_likely(common::ObAddr &addr,
                                        int64_t &cur_leader_epoch) const override final
  {
    int ret = common::OB_SUCCESS;
    bool get_addr_from_proposer = false;
    // 先默认自己是leader，校验lease，校验成功直接给出本机地址
    if (OB_LIKELY(proposer_.check_leader(&cur_leader_epoch))) {
      addr = self_addr_;
      get_addr_from_proposer = true;
    }
    // proposer上读取失败时，从acceptor上读取lease的owner
    if (OB_UNLIKELY(!get_addr_from_proposer)) {
      if (OB_LIKELY(!acceptor_.lease_.is_expired())) {
        acceptor_.lease_.get_owner_and_ballot(addr, cur_leader_epoch);
        if (OB_UNLIKELY(addr == self_addr_ || !addr.is_valid())) {
          addr.reset();
          cur_leader_epoch = 0;
        }
      } else {
        addr.reset();
        cur_leader_epoch = 0;
      }
    }
    return ret;
  }
  virtual int set_priority(ElectionPriority *priority) override final;
  virtual int reset_priority() override final;
  virtual int handle_message(const ElectionPrepareRequestMsg &msg) override final;
  virtual int handle_message(const ElectionAcceptRequestMsg &msg) override final;
  virtual int handle_message(const ElectionPrepareResponseMsg &msg) override final;
  virtual int handle_message(const ElectionAcceptResponseMsg &msg) override final;
  virtual int handle_message(const ElectionChangeLeaderMsg &msg) override final;
  virtual const common::ObAddr &get_self_addr() const override;
  int add_inner_priority_seed_bit(const PRIORITY_SEED_BIT new_bit);
  int clear_inner_priority_seed_bit(const PRIORITY_SEED_BIT old_bit);
  int set_inner_priority_seed(const uint64_t seed);
  TO_STRING_KV(K_(is_inited), K_(is_running), K_(proposer), K_(acceptor),
               K_(ls_biggest_min_cluster_version_ever_seen), KPC_(priority), K_(temporarily_downgrade_priority_info));
private:// 定向暴露给友元类
  void handle_message_base_(const ElectionMsgBase &message_base);
  void refresh_priority_();
  /**
   * @description: 比较两个消息的优先级哪个更高
   * @param {MSG} &lhs 第一个消息
   * @param {MSG} &rhs 第二个消息
   * @param {ObStringHolder} &reason 得到比较结果的原因
   * @return {bool} true:可以确定第二个消息的优先级高于第一个消息
                    false:无法确定第二个消息的优先级高于第一个消息
   * @Date: 2022-02-15 15:45:41
   */
  template <typename MSG>
  bool is_rhs_message_higher_(const MSG &lhs,
                              const MSG &rhs,
                              ObStringHolder &reason,
                              const bool decentralized_voting,
                              const LogPhase phase) const
  {
    ELECT_TIME_GUARD(500_ms);
    #define PRINT_WRAPPER KR(ret), K(rhs_is_higher), K(compare_result), K(reason), K(lhs), K(rhs),\
                          K(decentralized_voting), KPC(self_priority), KPC(lhs_priority),\
                          KPC(rhs_priority), K(*this)
    int ret = OB_SUCCESS;
    bool rhs_is_higher = false;
    int compare_result = 0;
    ElectionPriority *self_priority = priority_;
    ElectionPriority *lhs_priority = nullptr;
    ElectionPriority *rhs_priority = nullptr;
    if (!rhs.is_valid()) {// 编码时保证此种情况不会发生，此处为防御性检查
      ret = OB_INVALID_ARGUMENT;
      (void) reason.assign("INVALID MSG");
      LOG_PHASE(ERROR, phase, "new message is invalid");
    } else if (!lhs.is_valid()) {
      rhs_is_higher = true;
      (void) reason.assign("INVALID MSG");
    } else if (lhs.get_membership_version() > rhs.get_membership_version()) {
      (void) reason.assign("MEMBERSHIP VERSION");
    } else if (lhs.get_membership_version() < rhs.get_membership_version()) {
      rhs_is_higher = true;
      (void) reason.assign("MEMBERSHIP VERSION");
    } else if (lhs.get_inner_priority_seed() < rhs.get_inner_priority_seed()) {
      (void) reason.assign("PRIORITY SEED");
    } else if (lhs.get_inner_priority_seed() > rhs.get_inner_priority_seed()) {
      rhs_is_higher = true;
      (void) reason.assign("PRIORITY SEED");
    } else {
      if (!lhs.is_buffer_valid()) {// lhs的优先级为空，此时lhs消息具有最低优先级
        /* 这里解释一下，为什么空优先级要比非空优先级更低，而不是认为空优先级与所有优先级一样，举一个反例如下：
        * 考虑三副本([IP:priority]):{[127.0.0.1:1], [127.0.0.2:NULL], [127.0.0.3:3]}
        * （优先级越高权重越大，优先级相同的情况下，IP越小权重越大）
        * 假设三个副本接收消息的顺序如下：
        * 127.0.0.1: 先接收[127.0.0.2:NULL]，再接收[127.0.0.3:3]，此时认为[127.0.0.2:NULL]优先级更高，最后接收[127.0.0.1:1]，投票给127.0.0.1
        * 127.0.0.2: 先接收[127.0.0.1:1]，再接收[127.0.0.3:3]，此时认为[127.0.0.3:3]优先级更高，最后接收[127.0.0.2:NULL]，投票给127.0.0.2
        * 127.0.0.3: 先接收[127.0.0.1:1]，再接收[127.0.0.2:NULL]，此时认为[127.0.0.1:1]优先级更高，最后接收[127.0.0.3:3]，投票给127.0.0.3
        * 三个副本按照以上的既定顺序接收消息的时候将导致分票，而此时只要消息的接收顺序不发生变化，分票无主将持续下去
        * 因此有必要认为空优先级要比非空优先级更低
        */
        if (rhs.is_buffer_valid()) {// 如果rhs消息的优先级非空，rhs消息高于lhs消息
          rhs_is_higher = true;
          (void) reason.assign("priority is valid");
        } else if (decentralized_voting && rhs.get_sender() < lhs.get_sender()) {// 如果lhs的消息和rhs消息的优先级都是空的，那么比较IP
          rhs_is_higher = true;
          (void) reason.assign("IP-PORT(priority invalid)");
        } else {
          (void) reason.assign("IP-PORT(priority invalid)");
        }
      } else {// lhs优先级非空
        if (!rhs.is_buffer_valid()) {// rhs优先级是空的，判定rhs优先级更低
          (void) reason.assign("old message priority is valid and new message priority is invalid");
        } else {// rhs优先级非空，具备可比较的基础
          bool can_only_compare_ip_port = false;
          uint64_t compare_version = get_ls_biggest_min_cluster_version_ever_seen_();
          if (OB_ISNULL(self_priority)) {// 本机的优先级还没有设置，无法感知子类类型，此时没办法进行比较，只能比较IP大小
            can_only_compare_ip_port = true;
            LOG_PHASE(WARN, phase, "self priority not setted, can only compare IP-PORT");
          } else if (compare_version == 0) {
            can_only_compare_ip_port = true;
            LOG_PHASE(WARN, phase, "self ever seen min_cluster_version is 0, can only compare IP-PORT");
          }
          if (can_only_compare_ip_port) {
            if (decentralized_voting && rhs.get_sender() < lhs.get_sender()) {
              rhs_is_higher = true;
              (void) reason.assign("IP-PORT(priority invalid)");
            } else {
              (void) reason.assign("IP-PORT(priority invalid)");
            }
          } else {// 此时具备所有可比较的条件，进行真正的优先级比较
            char buffer1[self_priority->get_size_of_impl_type()];
            char buffer2[self_priority->get_size_of_impl_type()];
            self_priority->placement_new_impl(buffer1);
            self_priority->placement_new_impl(buffer2);
            lhs_priority = reinterpret_cast<ElectionPriority *>(buffer1);
            rhs_priority = reinterpret_cast<ElectionPriority *>(buffer2);
            int64_t pos1 = 0;
            int64_t pos2 = 0;
            if (CLICK_FAIL(lhs_priority->deserialize(lhs.get_priority_buffer(),
                                                     PRIORITY_BUFFER_SIZE,
                                                     pos1))) {
              LOG_PHASE(WARN, phase, "deserialize old message priority failed");
              (void) reason.assign("DESERIALIZE FAIL");
            } else if (CLICK_FAIL(rhs_priority->deserialize(rhs.get_priority_buffer(),
                                                            PRIORITY_BUFFER_SIZE,
                                                            pos2))) {
              LOG_PHASE(WARN, phase, "deserialize new message priority failed");
              (void) reason.assign("DESERIALIZE FAIL");
            } else if (CLICK_FAIL(lhs_priority->compare_with(*rhs_priority,
                                                             compare_version,
                                                             decentralized_voting,
                                                             compare_result,
                                                             reason))) {
              LOG_PHASE(WARN, phase, "compare priority failed");
              (void) reason.assign("COMPARE FAIL");
            } else {
              if (compare_result < 0) {
                rhs_is_higher = true;
              } else if (compare_result == 0 && decentralized_voting) {
                if (rhs.get_sender() < lhs.get_sender()) {
                  rhs_is_higher = true;
                  (void) reason.assign("IP-PORT(priority equal)");
                } else {
                  (void) reason.assign("IP-PORT(priority equal)");
                }
              }
              LOG_PHASE(TRACE, phase, "compare priority done");
            }
            lhs_priority->~ElectionPriority();
            rhs_priority->~ElectionPriority();
          }
        }
      }
    }
    return rhs_is_higher;
    #undef PRINT_WRAPPER
  }
  ElectionPriority *get_priority_() const;
  LogConfigVersion get_membership_version_() const;
  bool is_member_list_valid_() const;
  int broadcast_(const ElectionPrepareRequestMsg &msg,
                 const common::ObArray<common::ObAddr> &list) const;
  int broadcast_(const ElectionAcceptRequestMsg &msg,
                 const common::ObArray<common::ObAddr> &list) const;
  int send_(const ElectionPrepareResponseMsg &msg) const;
  int send_(const ElectionAcceptResponseMsg &msg) const;
  int send_(const ElectionChangeLeaderMsg &msg) const;
  uint64_t get_ls_biggest_min_cluster_version_ever_seen_() const;
  const char *print_version_pretty_(const uint64_t version) const;
  uint64_t generate_inner_priority_seed_() const;
private:
  bool is_inited_;
  bool is_running_;
  mutable common::ObSpinLock lock_;
  int64_t id_;
  ElectionProposer proposer_;// 对应PAXOS算法中的proposer
  ElectionAcceptor acceptor_;// 对应PAXOS算法中的acceptor
  ElectionPriority *priority_;// 由外部实现的选举优先级模块
  ElectionMsgSender *msg_handler_;// 由外部实现的消息收发模块
  common::ObAddr self_addr_;
  ObFunction<int(const int64_t, const ObAddr &)> prepare_change_leader_cb_;// 切主回调
  ObFunction<void(ElectionImpl *,common::ObRole,common::ObRole,RoleChangeReason)> role_change_cb_;// 角色状态变更回调
  uint64_t inner_priority_seed_;// 协议内选举优先级
  struct TemporarilyDowngradePriorityInfo {
    TemporarilyDowngradePriorityInfo()
    : downgrade_expire_ts_(0),
    interval_(0),
    reason_(nullptr) {}
    int64_t downgrade_expire_ts_;// 触发临时降低选举优先级的结束时间
    int64_t interval_;// 临时降低选举优先级的持续时长
    const char *reason_; // 临时降低选举优先级的原因
    TO_STRING_KV("downgrade_expire_ts", TimeSpanWrapper(downgrade_expire_ts_),
                 "interval", ObTimeLiteralPrettyPrinter(interval_),
                 K_(reason));
  } temporarily_downgrade_priority_info_;
  common::ObOccamTimer *timer_;// 选举定时任务的定时器
  LsBiggestMinClusterVersionEverSeen ls_biggest_min_cluster_version_ever_seen_;// 为仲裁副本维护的日志流级别的min_cluster_version值，用于处理选举兼容性升级相关问题
  EventRecorder event_recorder_;// 事件汇报模块
  mutable ElectionMsgCounter msg_counter_;// 监控模块
};

}// namespace election
}// namespace palf
}// namesapce oceanbase
#endif
