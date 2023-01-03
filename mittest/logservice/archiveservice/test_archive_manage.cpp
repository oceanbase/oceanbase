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

#include "basic_archive.h"
#include "lib/ob_errno.h"
#include <cstdint>
#include "cluster/logservice/env/ob_simple_log_cluster_env.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace unittest
{
class MySimpleArchiveInstance : public ObSimpleArchive
{
public:
  MySimpleArchiveInstance() : ObSimpleArchive() {}
};
static const int64_t ONE_MINUTE = 60L * 1000 * 1000 * 1000;
TEST_F(MySimpleArchiveInstance, test_archive_mgr)
{
  int ret = OB_SUCCESS;
  // 创建普通租户以及用户表
  ret = prepare();
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = prepare_dest();
  EXPECT_EQ(OB_SUCCESS, ret);
  const uint64_t tenant_id = tenant_ids_[0];
  int64_t round_id = 0;

  // =============== 首次开启归档 ================ //
  // 开启归档
  round_id = 1;  // 第一轮开启, round_id == 1
  ret = run_archive(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 检查rs归档状态为BEGINNING
  ret = check_rs_beginning(tenant_id, round_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 检查rs归档状态为DOING
  ret = check_rs_doing(tenant_id, round_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 检查rs归档进度
  ret = check_rs_archive_progress(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 检查日志流归档状态推进
  ret = check_archive_progress(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 检查日志流归档任务
  ret = check_ls_archive_task(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // fake关闭归档组件
  ret = fake_stop_component(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  /*
   * TODO 暂时关闭该部分单测内容
   * 需要补齐功能:
   * 1. INTERRUPT持久化内部表在没有piece记录场景没有覆盖到
   * 2. 切piece与切archive server目前可能造成归档进度回退, rs无法推进进度
   *
  // 检查归档任务全部处理完成
  ret = check_task_finish(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // fake修改piece相关信息, 将piece interval修改为秒级别
  ret = fake_piece_info_after_fake_stop(tenant_id, ONE_MINUTE);
  EXPECT_EQ(OB_SUCCESS, ret);

  // fake删除日志流归档任务
  ret = fake_remove_ls(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // fake重启归档组件
  ret = fake_restart_component(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 检查重启归档组件后, rs归档进度
  ret = check_rs_archive_progress(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 检查日志流归档状态推进
  ret = check_archive_progress(tenant_id, true);
  EXPECT_EQ(OB_SUCCESS, ret);
  */
  // =============== 关闭归档 ================ //
  ret = stop_archive();
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = check_rs_stop(tenant_id, round_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // =============== 重新开启归档 ================ //
  round_id = 2;
  ret = run_archive(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 检查归档组件处于doing
  ret = check_rs_doing(tenant_id, round_id);
  EXPECT_EQ(OB_SUCCESS, ret);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_archive_mgr.log", true, false, "test_archive_mgr_rs.log", "test_archive_election.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
