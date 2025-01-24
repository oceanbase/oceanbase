// owner: msy164651
// owner group: rs

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

#define USING_LOG_PREFIX RS

#include <gmock/gmock.h>
#define  private public
#define  protected public

#include "env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
using namespace share;
using namespace common;
namespace rootserver
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
class TestLSRecoveryGuard : public unittest::ObSimpleClusterTestBase
{
public:
  TestLSRecoveryGuard() : unittest::ObSimpleClusterTestBase("test_ls_recovery_stat") {}
protected:

  uint64_t tenant_id_;
};

TEST_F(TestLSRecoveryGuard, sys_recovery_guard)
{
  int ret = OB_SUCCESS;
  {
    //不init直接析构
    ObLSRecoveryGuard guard;
  }
  {
    //init 系统租户，析构
    ObLSRecoveryGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.init(OB_SYS_TENANT_ID, SYS_LS));
  }
  {
    //init不存在的租户，或者日志流
    ObLSRecoveryGuard guard;
    ASSERT_EQ(OB_TENANT_NOT_IN_SERVER, guard.init(1002, SYS_LS));
  }
}

TEST_F(TestLSRecoveryGuard, user_recovery_guard)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  tenant_id_ = 1002;
  SCN readable_scn;
  palf::LogConfigVersion config_version;
  {
    ObLSRecoveryGuard guard;
    ObLSID ls_id(10000000);
    ASSERT_EQ(OB_LS_NOT_EXIST, guard.init(tenant_id_, ls_id));
  }
  {
    ObLSRecoveryGuard guard;
    //加锁成功后，不在汇报，但是可以在统计
    ASSERT_EQ(OB_SUCCESS, guard.init(tenant_id_, SYS_LS, 300 * 1000));
    readable_scn = guard.ls_recovery_stat_->readable_scn_upper_limit_;
    //内存中的scn还是可以推高的
    SCN readable_scn_memory = guard.ls_recovery_stat_->replicas_scn_.at(0).get_readable_scn();
    config_version = guard.ls_recovery_stat_->config_version_in_inner_;
    ASSERT_EQ(1, guard.ls_recovery_stat_->ref_cnt_);
    ObLSRecoveryGuard guard1;
    //不能加锁成功
    ASSERT_EQ(OB_EAGAIN, guard1.init(tenant_id_, SYS_LS, 2 * 1000 * 1000));
    ASSERT_EQ(OB_INIT_TWICE, guard.init(tenant_id_, SYS_LS));
    ASSERT_EQ(1, guard.ls_recovery_stat_->ref_cnt_);
    ASSERT_EQ(OB_SUCCESS, guard.ls_recovery_stat_->reset_inner_readable_scn());
    usleep(3000 * 1000);//sleep 300ms，应该设置成最新
    ASSERT_EQ(readable_scn.get_val_for_sql(), guard.ls_recovery_stat_->readable_scn_upper_limit_.get_val_for_sql());
    ASSERT_NE(readable_scn_memory.get_val_for_sql(), guard.ls_recovery_stat_->replicas_scn_.at(0).readable_scn_.get_val_for_sql());
  }
  //释放后，可以推过
  usleep(3000 * 1000);
  {
    ObLSRecoveryGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.init(tenant_id_, SYS_LS, 300 * 1000));
    ASSERT_NE(readable_scn.get_val_for_sql(), guard.ls_recovery_stat_->readable_scn_upper_limit_.get_val_for_sql());
    readable_scn = guard.ls_recovery_stat_->readable_scn_upper_limit_;
    ASSERT_EQ(OB_SUCCESS, guard.ls_recovery_stat_->reset_inner_readable_scn());
    ASSERT_EQ(OB_NEED_RETRY, guard.ls_recovery_stat_->set_inner_readable_scn(config_version,readable_scn, true));
    ASSERT_EQ(OB_SUCCESS, guard.ls_recovery_stat_->set_inner_readable_scn(config_version, readable_scn, false));

  }
}
} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
