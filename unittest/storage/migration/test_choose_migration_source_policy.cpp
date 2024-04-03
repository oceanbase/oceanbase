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
#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "storage/high_availability/ob_storage_ha_src_provider.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_meta_package.h"
#include "test_migration.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "storage/ob_locality_manager.h"
#include "lib/ob_errno.h"
#include "logservice/palf/palf_handle_impl.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace testing::internal;
using ::testing::_;
using ::testing::Invoke;
namespace storage
{
class MockStorageRpc : public ObStorageRpc
{
public:
  MockStorageRpc()
    : ObStorageRpc()
  {
  }
  virtual ~MockStorageRpc() {}

  MOCK_METHOD4(post_ls_meta_info_request, int(const uint64_t, const ObStorageHASrcInfo &,
      const share::ObLSID &, obrpc::ObFetchLSMetaInfoResp &));
};

class MockGetMemberHelper : public ObStorageHAGetMemberHelper
{
public:
  MockGetMemberHelper()
    : ObStorageHAGetMemberHelper()
  {
  }
  virtual ~MockGetMemberHelper() {}

  MOCK_METHOD6(get_ls_member_list_and_learner_list_, int(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &, common::GlobalLearnerList &, common::ObIArray<common::ObAddr> &));
  MOCK_METHOD3(get_ls_leader, int(const uint64_t, const share::ObLSID &, common::ObAddr &));
  MOCK_METHOD2(get_ls, int(const share::ObLSID &, ObLSHandle &));
  MOCK_METHOD0(check_tenant_primary, bool());
};

class MockMemberList
{
public:
  MockMemberList() {}
  virtual ~MockMemberList() {}

  int get_ls_member_list_for_checkpoint(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(4/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_rs_recommand(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    common::ObAddr rs_recommand_addr;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(3/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(mock_rs_recommand_addr(rs_recommand_addr))) {
      LOG_WARN("failed to mock rs recommand addr", K(ret));
    } else if (OB_FAIL(mock_learner_list(rs_recommand_addr, learner_list))) {
      LOG_WARN("failed to mock learner list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_idc_mode_idc_leader(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(5/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_idc_mode_idc_follower(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(5/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_idc_mode_region_leader(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(3/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_idc_mode_region_follower(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(3/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_idc_mode_diff_region_leader(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(1/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_idc_mode_diff_region_follower(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(2/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_region_mode_region_follower(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(4/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_region_mode_region_leader(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(3/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_region_mode_diff_region_follower(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(2/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_region_mode_diff_region_leader(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(1/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_member_helper(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_addr_list(5/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_rebuild_mode(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_addr_list(5/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    }
    return ret;
  }

  int get_ls_member_list_for_replica_type_failed(const uint64_t, const share::ObLSID &,
      const bool, common::ObAddr &leader, common::GlobalLearnerList &learner_list, common::ObIArray<common::ObAddr> &addr_list)
  {
    int ret = OB_SUCCESS;
    common::ObAddr addr;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock addr", K(ret));
    } else if (OB_FAIL(mock_addr_list(3/*addr_count*/, addr_list))) {
      LOG_WARN("failed to mock addr list", K(ret));
    } else if (OB_FAIL(mock_check_replica_type_addr(addr))) {
      LOG_WARN("failed to mock rs recommand addr", K(ret));
    } else if (OB_FAIL(mock_learner_list(addr, learner_list))) {
      LOG_WARN("failed to mock learner list", K(ret));
    }
    return ret;
  }

  int get_ls_leader_succ(const uint64_t, const share::ObLSID &, common::ObAddr &leader)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_leader_addr(leader))) {
      LOG_WARN("failed to mock leader addr", K(ret));
    }
    return ret;
  }

  int get_ls_succ(const share::ObLSID &, ObLSHandle &ls_handle)
  {
    int ret = OB_SUCCESS;
    ls_handle.ls_ = &mock_ls_;
    return ret;
  }

  int get_ls_succ_with_palf(const share::ObLSID &, ObLSHandle &ls_handle)
  {
    int ret = OB_SUCCESS;
    common::ObAddr parent;
    if (OB_FAIL(mock_addr("192.168.1.1:1234", parent))) {
      LOG_WARN("failed to mock addr", K(ret));
    } else {
      mock_ls_.log_handler_.palf_handle_.palf_handle_impl_ = &mock_palf_handle_impl_;
      mock_palf_handle_impl_.is_inited_ = true;
      mock_palf_handle_impl_.config_mgr_.parent_ = parent;
      ls_handle.ls_ = &mock_ls_;
    }
    return ret;
  }

  int get_ls_fail(const share::ObLSID &, ObLSHandle &ls_handle)
  {
    int ret = OB_ERR_UNEXPECTED;
    ls_handle.ls_ = &mock_ls_;
    return ret;
  }

  bool check_tenant_primary_true()
  {
    return true;
  }
  bool check_tenant_primary_false()
  {
    return false;
  }

public:
  palf::PalfHandleImpl mock_palf_handle_impl_;
  ObLS mock_ls_;
};

class MockLsMetaInfo
{
public:
  MockLsMetaInfo() {}
  virtual ~MockLsMetaInfo() {}
  int post_ls_meta_info_request_succ(const uint64_t, const ObStorageHASrcInfo &,
      const share::ObLSID &, obrpc::ObFetchLSMetaInfoResp &res)
  {
    int ret = OB_SUCCESS;
    ret = mock_valid_ls_meta(res);
    return ret;
  }

  int post_ls_meta_info_request_min_checkpoint(const uint64_t, const ObStorageHASrcInfo &,
      const share::ObLSID &, obrpc::ObFetchLSMetaInfoResp &res)
  {
    int ret = OB_SUCCESS;
    ret = mock_valid_ls_meta(res);
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_.set_min();
    return ret;
  }

  int post_ls_meta_info_request_max_checkpoint(const uint64_t, const ObStorageHASrcInfo &,
      const share::ObLSID &, obrpc::ObFetchLSMetaInfoResp &res)
  {
    int ret = OB_SUCCESS;
    ret = mock_valid_ls_meta(res);
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_.set_max();
    return ret;
  }

  int post_ls_meta_info_request_invalid_type_checkpoint(const uint64_t, const ObStorageHASrcInfo &,
      const share::ObLSID &, obrpc::ObFetchLSMetaInfoResp &res)
  {
    int ret = OB_SUCCESS;
    ret = mock_valid_ls_meta(res);
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_.set_max();
    return ret;
  }

  int post_ls_meta_info_request_base_checkpoint(const uint64_t, const ObStorageHASrcInfo &,
      const share::ObLSID &, obrpc::ObFetchLSMetaInfoResp &res)
  {
    int ret = OB_SUCCESS;
    ret = mock_valid_ls_meta(res);
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_.set_base();
    return ret;
  }

  int post_ls_meta_info_request_parent_checkpoint(const uint64_t, const ObStorageHASrcInfo &,
      const share::ObLSID &, obrpc::ObFetchLSMetaInfoResp &res)
  {
    int ret = OB_SUCCESS;
    ret = mock_valid_ls_meta(res);
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_.set_base();
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_ = mock_ckpt_inc(res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_);
    return ret;
  }

  int post_ls_meta_info_request_large_checkpoint(const uint64_t, const ObStorageHASrcInfo &,
      const share::ObLSID &, obrpc::ObFetchLSMetaInfoResp &res)
  {
    int ret = OB_SUCCESS;
    ret = mock_valid_ls_meta(res);
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_.set_base();
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_ = mock_ckpt_inc(res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_);
    res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_ = mock_ckpt_inc(res.ls_meta_package_.ls_meta_.clog_checkpoint_scn_);
    return ret;
  }
};

class TestChooseMigrationSourcePolicy : public ::testing::Test
{
public:
  TestChooseMigrationSourcePolicy();
  virtual ~TestChooseMigrationSourcePolicy();
  virtual void SetUp();
  virtual void TearDown();
private:
  ObStorageHAChooseSrcHelper choose_src_helper_;
  MockStorageRpc storage_rpc_;
  ObStorageRpcProxy storage_rpc_proxy_;
  ObCommonRpcProxy common_rpc_proxy_;
  MockLocalityManager locality_manager_;
  MockGetMemberHelper member_helper_;
};

TestChooseMigrationSourcePolicy::TestChooseMigrationSourcePolicy()
{}

TestChooseMigrationSourcePolicy::~TestChooseMigrationSourcePolicy()
{}

void TestChooseMigrationSourcePolicy::SetUp()
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_base();
  common::ObAddr addr;
  EXPECT_EQ(OB_SUCCESS, mock_dst_addr(addr));
  EXPECT_EQ(OB_SUCCESS, storage_rpc_.init(&storage_rpc_proxy_, addr, &common_rpc_proxy_));
  EXPECT_EQ(OB_SUCCESS, member_helper_.init(&storage_rpc_));
  EXPECT_EQ(OB_SUCCESS, locality_manager_.init_manager(addr));
}

void TestChooseMigrationSourcePolicy::TearDown()
{
  locality_manager_.destroy();
  storage_rpc_.destroy();
}
// test checkpoint policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234"]
// mock condition:
// 192.168.1.1:1234: checkpoint -> OB_MAX_SCN_TS_NS, type -> F
// 192.168.1.2:1234, 192.168.1.3:1234, 192.168.1.4:1234: checkpoint -> OB_MIN_SCN_TS_NS, status -> F
// output addr:192.168.1.1:1234
TEST_F(TestChooseMigrationSourcePolicy, get_available_src_with_checkpoint_policy)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_min_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_max_checkpoint))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_min_checkpoint));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_checkpoint));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, get_ls(_, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_succ));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_base();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_checkpoint(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_checkpoint_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.1:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test rs recommand policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234"]
// mock condition:
// 192.168.1.1:1234, 192.168.1.2:1234, 192.168.1.3:1234: checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.4:1234: checkpoint -> OB_BASE_SCN_TS_NS, type -> R
// recommand addr: 192.168.1.4:1234
// output addr:192.168.1.4:1234
TEST_F(TestChooseMigrationSourcePolicy, get_available_src_with_rs_recommend)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_rs_recommand));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, get_ls(_, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_fail));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_base();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_rs_recommand(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_recommand_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.4:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test idc policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234", "192.168.1.5:1234"]
// 192.168.1.1:1234 : idc -> idc1, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.4:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.5:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.1:1234
TEST_F(TestChooseMigrationSourcePolicy, idc_mode_idc_leader)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_idc_mode_idc_leader));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_IDC_LEADER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.1:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test idc policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234", "192.168.1.5:1234"]
// 192.168.1.1:1234 : idc -> idc1, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc1, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.4:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.5:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.2:1234
TEST_F(TestChooseMigrationSourcePolicy, idc_mode_idc_follower)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_idc_mode_idc_follower));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_IDC_FOLLOWER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.2:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test idc policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234"]
// 192.168.1.1:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.1:1234
TEST_F(TestChooseMigrationSourcePolicy, idc_mode_region_leader)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_idc_mode_region_leader));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_REGION_LEADER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.1:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test idc policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234"]
// 192.168.1.1:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.2:1234
TEST_F(TestChooseMigrationSourcePolicy, idc_mode_region_follower)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_idc_mode_region_follower));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_REGION_FOLLOWER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.2:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test idc policy
// candidate addr: ["192.168.1.1:1234"]
// 192.168.1.1:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// output addr:192.168.1.1:1234
TEST_F(TestChooseMigrationSourcePolicy, idc_mode_diff_region_leader)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_idc_mode_diff_region_leader));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_DIFF_REGION_LEADER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.1:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test idc policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234"]
// 192.168.1.1:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.2:1234
TEST_F(TestChooseMigrationSourcePolicy, idc_mode_diff_region_follower)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_idc_mode_diff_region_follower));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_DIFF_REGION_FOLLOWER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.2:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test region policy
// 192.168.1.1:1234 : idc -> idc1, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.4:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.2:1234
TEST_F(TestChooseMigrationSourcePolicy, region_mode_region_follower)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_region_mode_region_follower));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_region_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::REGION_MODE_REGION_FOLLOWER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::REGION, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.2:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test region policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234"]
// 192.168.1.1:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.1:1234
TEST_F(TestChooseMigrationSourcePolicy, region_mode_region_leader)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_region_mode_region_leader));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_region_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::REGION_MODE_REGION_LEADER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::REGION, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.1:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test region policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234"]
// 192.168.1.1:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.2:1234
TEST_F(TestChooseMigrationSourcePolicy, region_mode_diff_region_follower)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_region_mode_diff_region_follower));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_region_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::REGION_MODE_DIFF_REGION_FOLLOWER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::REGION, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.2:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test region policy
// candidate addr: ["192.168.1.1:1234"]
// 192.168.1.1:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// output addr:192.168.1.1:1234
TEST_F(TestChooseMigrationSourcePolicy, region_mode_diff_region_leader)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_region_mode_diff_region_leader));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_region_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::REGION_MODE_DIFF_REGION_LEADER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::REGION, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.1:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test rebuild policy
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234", "192.168.1.5:1234"]
// 192.168.1.1:1234 : idc -> idc1, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.4:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.5:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// output addr:192.168.1.1:1234
TEST_F(TestChooseMigrationSourcePolicy, get_available_src_with_rebuild)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_succ));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_rebuild_mode));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_rebuild(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_IDC_LEADER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.1:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}

TEST_F(TestChooseMigrationSourcePolicy, member_helper_get_member_list)
{
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_member_helper));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  common::ObArray<common::ObAddr> addr_list;
  EXPECT_EQ(OB_SUCCESS, member_helper_.get_ls_member_list(tenant_id, ls_id, addr_list));
}
// test ObMigrationSrcByLocationProvider init fail
TEST_F(TestChooseMigrationSourcePolicy, src_provider_init_idc_fail)
{
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_init_fail(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
}
// test ObMigrationSrcByLocationProvider init fail
TEST_F(TestChooseMigrationSourcePolicy, src_provider_init_region_fail)
{
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_init_fail(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_region_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
}
// test ObRSRecommendSrcProvider init fail
TEST_F(TestChooseMigrationSourcePolicy, src_provider_init_recommand_fail)
{
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_init_fail(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_recommand_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
}
// test ObMigrationSrcByCheckpointProvider init fail
TEST_F(TestChooseMigrationSourcePolicy, src_provider_init_checkpoint_fail)
{
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_min();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_init_fail(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_checkpoint_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
}
// test check replica valid fail
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234", "192.168.1.5:1234"]
// local checkpoint -> OB_BASE_SCN_TS_NS
// parent checkpoint -> OB_BASE_SCN_TS_NS + 1
// dst type -> F
// 192.168.1.1:1234 : checkpoint -> OB_BASE_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : checkpoint -> OB_MIN_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.4:1234 : checkpoint -> OB_BASE_SCN_TS_NS + 2, type -> F
// 192.168.1.5:1234 : checkpoint -> OB_MAX_SCN_TS_NS, type -> R
// output addr:192.168.1.3:1234
TEST_F(TestChooseMigrationSourcePolicy, get_available_src_condition_fail)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_parent_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_min_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_base_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_large_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_invalid_type_checkpoint))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_min_checkpoint));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_replica_type_failed));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, get_ls(_, _))
      .WillOnce(Invoke(&member_list, &MockMemberList::get_ls_fail))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_base();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_checkpoint(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_checkpoint_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.3:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test check replica valid fail
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234", "192.168.1.5:1234"]
// local checkpoint -> OB_BASE_SCN_TS_NS
// parent checkpoint -> OB_BASE_SCN_TS_NS + 1
// dst type -> F
// 192.168.1.1:1234 : idc -> idc1, region -> region1, checkpoint -> OB_MIN_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.4:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS + 2, type -> F
// 192.168.1.5:1234 : idc -> idc1, region -> region2, checkpoint -> OB_MIN_SCN_TS_NS, type -> F
// output addr:192.168.1.4:1234
TEST_F(TestChooseMigrationSourcePolicy, idc_mode_check_replica_fail)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_parent_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_min_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_base_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_base_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_large_checkpoint))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_min_checkpoint));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_idc_mode_idc_leader));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, get_ls(_, _))
      .WillOnce(Invoke(&member_list, &MockMemberList::get_ls_fail))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_succ));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_base();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_location(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_IDC_LEADER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.4:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}
// test check replica valid fail
// candidate addr: ["192.168.1.1:1234", "192.168.1.2:1234", "192.168.1.3:1234", "192.168.1.4:1234", "192.168.1.5:1234"]
// local checkpoint -> OB_BASE_SCN_TS_NS
// parent checkpoint -> OB_BASE_SCN_TS_NS + 1
// dst type -> F
// 192.168.1.1:1234 : idc -> idc1, region -> region1, checkpoint -> OB_MIN_SCN_TS_NS, type -> F, leader
// 192.168.1.2:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.3:1234 : idc -> idc2, region -> region1, checkpoint -> OB_BASE_SCN_TS_NS, type -> F
// 192.168.1.4:1234 : idc -> idc1, region -> region2, checkpoint -> OB_BASE_SCN_TS_NS + 2, type -> F
// 192.168.1.5:1234 : idc -> idc1, region -> region2, checkpoint -> OB_MIN_SCN_TS_NS, type -> F
// output addr:192.168.1.4:1234
TEST_F(TestChooseMigrationSourcePolicy, idc_mode_r_replica_init)
{
  MockLsMetaInfo ls_meta;
  EXPECT_CALL(storage_rpc_, post_ls_meta_info_request(_, _, _, _))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_parent_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_min_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_base_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_base_checkpoint))
      .WillOnce(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_large_checkpoint))
      .WillRepeatedly(Invoke(&ls_meta, &MockLsMetaInfo::post_ls_meta_info_request_min_checkpoint));
  MockMemberList member_list;
  EXPECT_CALL(member_helper_, get_ls_member_list_and_learner_list_(_, _, _, _, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_member_list_for_idc_mode_idc_leader));
  EXPECT_CALL(member_helper_, get_ls_leader(_, _, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_leader_succ));
  EXPECT_CALL(member_helper_, get_ls(_, _))
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::get_ls_succ_with_palf));
  EXPECT_CALL(member_helper_, check_tenant_primary())
      .WillRepeatedly(Invoke(&member_list, &MockMemberList::check_tenant_primary_true));
  const uint64_t tenant_id = 1001;
  const share::ObLSID ls_id(1);
  share::SCN local_ls_checkpoint_scn;
  local_ls_checkpoint_scn.set_base();
  ObMigrationOpArg mock_arg;
  EXPECT_EQ(OB_SUCCESS, mock_migrate_arg_for_r_type(mock_arg));
  ObStorageHASrcProvider::ChooseSourcePolicy policy;
  EXPECT_EQ(OB_SUCCESS, get_idc_policy(mock_arg, tenant_id, policy));
  ObStorageHASrcInfo src_info;
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.init(tenant_id, ls_id, local_ls_checkpoint_scn, mock_arg, policy, &storage_rpc_, &member_helper_));
  EXPECT_EQ(OB_SUCCESS, mock_locality_manager(MOCKLOCALITY::IDC_MODE_IDC_LEADER, locality_manager_));
  EXPECT_EQ(ObStorageHASrcProvider::ChooseSourcePolicy::IDC, choose_src_helper_.get_provider()->get_policy_type());
  static_cast<ObMigrationSrcByLocationProvider *>(choose_src_helper_.get_provider())->set_locality_manager_(&locality_manager_);
  EXPECT_EQ(OB_SUCCESS, choose_src_helper_.get_available_src(mock_arg, src_info));
  common::ObAddr expect_addr;
  EXPECT_EQ(OB_SUCCESS, mock_addr("192.168.1.4:1234", expect_addr));
  EXPECT_EQ(expect_addr, src_info.src_addr_);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_choose_migration_source_policy.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_choose_migration_source_policy.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
