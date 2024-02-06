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
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/palf_handle_impl.h"

namespace oceanbase
{
using namespace palf::election;
namespace unittest
{
class MockElection : public Election, public common::LinkHashValue<palf::LSKey>
{
public:
  MockElection();
  MockElection(const int64_t id, const common::ObAddr &self);
  virtual ~MockElection() { }
  int init(const int64_t id, const common::ObAddr &self);
  void stop() override final;
  // 设置成员列表
  int set_memberlist(const MemberList &new_member_list) override final;
  // 获取选举当前的角色
  int get_role(common::ObRole &role, int64_t &epoch) const override final;
  // 如果自己是leader，那么拿到的就是准确的leader，如果自己不是leader，那么拿到lease的owner
  int get_current_leader_likely(common::ObAddr &addr,
                                        int64_t &cur_leader_epoch) const override final;
  // 供role change service使用
  int change_leader_to(const common::ObAddr &dest_addr) override final;
  int revoke(const RoleChangeReason &reason) override final;
  // 拿本机地址
  const common::ObAddr &get_self_addr() const override final;
  // 打印日志
  int64_t to_string(char *buf, const int64_t buf_len) const override final;
  // 设置选举优先级
  int set_priority(ElectionPriority *priority) override final;
  int reset_priority() override final;
  // 处理消息
  int handle_message(const ElectionPrepareRequestMsg &msg) override final;
  int handle_message(const ElectionAcceptRequestMsg &msg) override final;
  int handle_message(const ElectionPrepareResponseMsg &msg) override final;
  int handle_message(const ElectionAcceptResponseMsg &msg) override final;
  int handle_message(const ElectionChangeLeaderMsg &msg) override final;
  int set_leader(const common::ObAddr &leader, const int64_t new_epoch);
private:
  int64_t id_;
  common::ObAddr self_;
  common::ObRole role_;
  int64_t epoch_;
  common::ObAddr leader_;
  bool is_inited_;
};
}// unittest
}// oceanbase