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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_ELECTION_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_ELECTION_

#include "logservice/palf/election/interface/election.h"

namespace oceanbase
{
namespace palf
{
using namespace election;

namespace mockelection
{
class MockElection : public palf::election::Election
{
public:
  MockElection() {}
  ~MockElection() {}

  int start()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  void stop() override final
  {}
  int can_set_memberlist(const palf::LogConfigVersion &new_config_version) const override final
  {
    int ret = OB_SUCCESS;
    UNUSED(new_config_version);
    return ret;
  }
  // 设置成员列表
  int set_memberlist(const MemberList &new_member_list) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(new_member_list);
    return ret;
  }
  int revoke(const RoleChangeReason &reason)
  {
    UNUSED(reason);
    int ret = OB_SUCCESS;
    return ret;
  }
  int set_priority(ElectionPriority *) override final { return  OB_SUCCESS; }
  int reset_priority() override final { return  OB_SUCCESS; }
  // 获取选举当前的角色
  int get_role(common::ObRole &role, int64_t &epoch) const override final
  {
    int ret = OB_SUCCESS;
    role = role_;
    epoch = leader_epoch_;
    return ret;
  }
  // 如果自己是leader，那么拿到的就是准确的leader，如果自己不是leader，那么拿到lease的owner
  int get_current_leader_likely(common::ObAddr &p_addr,
                                int64_t &p_cur_leader_epoch) const override final
  {
    int ret = OB_SUCCESS;
    p_addr = leader_;
    p_cur_leader_epoch = leader_epoch_;
    return ret;
  }
  // 供内部测试的时候切主使用
  virtual int change_leader_to(const common::ObAddr &dest_addr) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(dest_addr);
    return ret;
  }
  virtual int temporarily_downgrade_protocol_priority(const int64_t time_us, const char *reason) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(time_us);
    UNUSED(reason);
    return OB_SUCCESS;
  }
  // 拿本机地址
  const common::ObAddr &get_self_addr() const override final
  {
    return self_;
  }
  // 打印日志
  virtual int64_t to_string(char *buf, const int64_t buf_len) const override final
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return 0;
  }
  // 处理消息
  virtual int handle_message(const ElectionPrepareRequestMsg &msg) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(msg);
    return ret;
  }
  virtual int handle_message(const ElectionAcceptRequestMsg &msg) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(msg);
    return ret;
  }
  virtual int handle_message(const ElectionPrepareResponseMsg &msg) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(msg);
    return ret;
  }
  virtual int handle_message(const ElectionAcceptResponseMsg &msg) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(msg);
    return ret;
  }
  virtual int handle_message(const ElectionChangeLeaderMsg &msg) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(msg);
    return ret;
  }
public:
  common::ObAddr self_;
  common::ObAddr leader_;
  int64_t leader_epoch_;
  common::ObRole role_;
};
} // end of election
} // end of palf
} // end of oceanbase

#endif
