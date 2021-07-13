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

#ifndef OCEANBASE_CLOG_MOCK_OB_ROOT_SERVER_H_
#define OCEANBASE_CLOG_MOCK_OB_ROOT_SERVER_H_

#include "clog/ob_i_membership_callback.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase {
namespace clog {
class MockObRootServer : public ObIMembershipCallback {
public:
  struct MockRootTableNode {
    int64_t mc_timestamp_;
    common::ObMemberList pre_member_list_;
    common::ObMemberList cur_member_list_;

    MockRootTableNode() : mc_timestamp_(0)
    {}
    ~MockRootTableNode()
    {}

    MockRootTableNode(const MockRootTableNode& node)
    {
      deep_copy(node);
    }

    void operator=(const MockRootTableNode& node)
    {
      deep_copy(node);
    }

    void reset()
    {
      pre_member_list_.reset();
      cur_member_list_.reset();
      mc_timestamp_ = 0;
    }

    int deep_copy(const MockRootTableNode& node)
    {
      int ret = common::OB_SUCCESS;
      if (common::OB_SUCCESS != (ret = pre_member_list_.deep_copy(node.pre_member_list_))) {
        CLOG_LOG(WARN, "pre_member_list deep copy fail", "ret", ret);
      } else if (common::OB_SUCCESS != (ret = cur_member_list_.deep_copy(node.cur_member_list_))) {
        CLOG_LOG(WARN, "cur_member_list deep copy fail", "ret", ret);
      } else {
        mc_timestamp_ = node.mc_timestamp_;
      }
      return ret;
    }
  };

public:
  MockObRootServer();
  ~MockObRootServer();
  int init();
  void destroy();

  // TODO: used to write a log
  int prepare_major_freeze();

  int on_member_change_success(const common::ObPartitionKey& partition_key, const int64_t mc_timestamp,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list);

private:
  static const int64_t BUCKET_NUM = 1000;
  static const int32_t MOD_ID = 789789;

  bool is_inited_;
  common::hash::ObHashMap<common::ObPartitionKey, MockRootTableNode> maps_;
  DISALLOW_COPY_AND_ASSIGN(MockObRootServer);
};

}  // namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_MOCK_OB_ROOT_SERVER_H_
