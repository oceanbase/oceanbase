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

/*
 * normal process for sstable list doing one op
 * #W means doing write to oss
 * #R means doing read to oss
 * #X means no need to do because of not doing sswriter reconfirm
 * step.1 get file handle
 * step.2 create op
 *                    A. check lease, cache lease epoch
 *                      if lease changed do sswriter ressume
 *                      a. generate op_id
 * #OP1  #R1 #X              i. read "current" obj and get it's value as $op_id
 * #OP2  #R2 #X             ii. find op_id obj that exists
 * #OP3  #R3 #X            iii. find MAX op_id obj whose op_id is $max_op_id
 *                        iii. set $next_op_id = $max_op_id + 1
 * #OP4  #W1               iv. write $next_op_id obj to oss
 * #OP5  #W2                v. write $next_op_id to current obj to oss
 *                      b. generate sstable list file content
 * #OP6  #R4               i. read from next_op_id - 1 to 1, find max sstable.list.op_id or sstable.task.op_id whose finish field is true
 *                        ii. generate new sstable list content
 * #OP7  #W3            c. write new sstable list file to oss
 *                    B. generate op_id
 *                     a. generate op_id
//  * #OP7  #R #X            i. read "current" obj and get it's value as $op_id
//  * #OP8  #R #X           ii. find MAX op_id obj whose op_id is $max_op_id
 *                      iii. set $next_op_id = $max_op_id + 1
 * #OP8  #W4             iv. write $next_op_id obj to oss
 * #OP9  #W5              v. write $next_op_id to current obj to oss
 *                    C. do init op
 * step.3 write task file to oss
 *                    A. check lease
 * #OP10 #w6          B. write task file to oss
 * step.4 update max upload data seq
 *                    A. check lease
 * #OP11 #w7          B. write task file to oss
 * step.5 update max upload meta seq
 *                    A. check lease
 * #OP12 #w8          B. write task file to oss
 * step.6 finish op
 *                    A. check lease
 * #OP13 #w9          B. write task file finish field to true to oss
 *                    C. check lease
 * #OP14 #w10         E. flush file content to share storage
 */

std::vector<int> point_array_total =
  {1,2,3,4,5,6,7,8,9,10,11,12,13,14};

std::vector<int> write_point_array_total =
  {4, 8, 5, 9, 10, 11, 12, 13, 7, 14};
std::vector<std::vector<int>> write_point_array_in_total =
  {{4, 8}, {5, 9}, {10, 11, 12, 13}, {7, 14}};

std::vector<std::vector<int>> write_point_array_in_mini =
  {{1, 4}, {2, 5}, {6, 7, 8, 9}, {3, 10}};

TEST_F(TestAtomicErrSim, test_request_timeout_with_reconfirm)
{
  // test requests to oss time out occurs at each step
  {
    // request timeout but the execution is successful
    std::cout << "--------test request timeout but successfully executes--------" << std::endl;
    {
      // type: SHARE_MINI_OP_ID
      // step 2.A.a.iv 2.B.a.iv total two writes
      TEST_TIMEOUT_SUCC_REQUEST_WITH_RECONFIRM(write_point_array_in_total[MINI_OP_ID], point_array_total.size(), OB_TIMEOUT)

    }

    {
      // type: SHARE_MINI_CURRENT
      // step 2.A.a.v 2.B.a.v total two writes
      TEST_TIMEOUT_SUCC_REQUEST_WITH_RECONFIRM(write_point_array_in_total[MINI_CURRENT], point_array_total.size(), OB_TIMEOUT)
    }

    {
      // type: SHARE_MINI_SSTABLE_TASK
      // step 3.B 4.B 5.B 6.B  total four writes
      TEST_TIMEOUT_SUCC_REQUEST_WITH_RECONFIRM(write_point_array_in_total[MINI_SSTABLE_TASK], point_array_total.size(), OB_TIMEOUT)
    }

    {
      // type: SHARE_MINI_SSTABLE_LIST
      // step 2.A.c 4.D total two writes
      TEST_TIMEOUT_SUCC_REQUEST_WITH_RECONFIRM(write_point_array_in_total[SHARE_MINI_SSTABLE_LIST], point_array_total.size(), OB_TIMEOUT)
    }
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_atomic_errsim1.log*");
  OB_LOGGER.set_file_name("test_atomic_errsim1.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
