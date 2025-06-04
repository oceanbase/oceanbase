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
#define USING_LOG_PREFIX STORAGETEST

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include <vector>
#include "mock_object_storage.h"

#define private public
#define protected public
#include "test_ss_atomic_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_overwrite_op.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "shared_storage/clean_residual_data.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

class TestAtomicErrSim : public ::testing::Test
{
public:
  TestAtomicErrSim() = default;
  virtual ~TestAtomicErrSim() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  static int tablet_cur_op_id;
};

int TestAtomicErrSim::tablet_cur_op_id = 0;

ObSSTableInfoList write_ss_list;

void TestAtomicErrSim::SetUpTestCase()
{
  ObAtomicFile::WRITE_FILE_TIMEOUT_MAX_RETRY = 0;
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  OB_MOCK_OBJ_SRORAGE.tenant_ = ObTenantEnv::get_tenant();
}

void TestAtomicErrSim::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  OB_MOCK_OBJ_SRORAGE.stop();
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

std::vector<int> point_array_total =
  {1,2,3,4,5,6,7,8,9,10,11,12,13,14};

std::vector<int> write_point_array_total =
  {4, 8, 5, 9, 10, 11, 12, 13, 7, 14};
std::vector<std::vector<int>> write_point_array_in_total =
  {{4, 8}, {5, 9}, {10, 11, 12, 13}, {7, 14}};

std::vector<std::vector<int>> write_point_array_in_mini =
  {{1, 4}, {2, 5}, {6, 7, 8, 9}, {3, 10}};

TEST_F(TestAtomicErrSim, test_sswriter_change)
{
  std::cout << "--------test sswriter change--------" << std::endl;
  // test sswriter change occurs at each write step
  {
    // type: SHARE_MINI_OP_ID
    // step 2.A.a.iv 2.B.a.iv total two writes
    TEST_INJECT_SSWRITER_CHANGE(write_point_array_in_mini[MINI_OP_ID], OB_NOT_MASTER)
  }

  {
    // type: SHARE_MINI_CURRENT
    // step 2.A.a.v 2.B.a.v total two writes
    TEST_INJECT_SSWRITER_CHANGE(write_point_array_in_mini[MINI_CURRENT], OB_NOT_MASTER)
  }

  {
    // type: SHARE_MINI_SSTABLE_TASK
    // step 3.B 4.B 5.B 6.B  total four writes
    TEST_INJECT_SSWRITER_CHANGE(write_point_array_in_mini[MINI_SSTABLE_TASK], OB_NOT_MASTER)
  }

  {
    // type: SHARE_MINI_SSTABLE_LIST
    // step 2.A.c 4.D total two write
    {
      TEST_INJECT_SSWRITER_CHANGE(std::vector<int>({3}), OB_NOT_MASTER)
    }

    {
      TEST_INJECT_SSWRITER_CHANGE(std::vector<int>({10}), OB_SUCCESS)
    }
  }
  tablet_cur_op_id += 1;
}

TEST_F(TestAtomicErrSim, test_request_timeout_without_reconfirm)
{
  // test requests to oss time out occurs at each step
  {
    // request timeout but the execution is successful
    std::cout << "--------test request timeout without confirm but successfully executes--------" << std::endl;
  for (int idx = 0; idx < write_point_array_in_total.size(); idx++)
    {
      TEST_TIMEOUT_SUCC_REQUEST_WITHOUT_RECONFIRM(write_point_array_in_total[idx], point_array_total.size()-7, OB_TIMEOUT)
    }
  }

  {
    // request timeout but the execution is unsuccessful
    std::cout << "--------test request timeout but unsuccessfully executes--------" << std::endl;
    for (int idx = 0; idx < write_point_array_in_total.size(); idx++)
    {
      TEST_TIMEOUT_FAIL_REQUEST_WITHOUT_RECONFIRM(write_point_array_in_total[idx], point_array_total.size()-7, OB_TIMEOUT)
    }
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_atomic_errsim.log3*");
  OB_LOGGER.set_file_name("test_atomic_errsim3.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
