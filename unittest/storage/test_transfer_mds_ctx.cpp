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

#include <gtest/gtest.h>

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "share/config/ob_server_config.h"
#include "unittest/storage/init_basic_struct.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"
#include "src/storage/compaction/ob_medium_list_checker.h"
#include "src/storage/multi_data_source/ob_abort_transfer_in_mds_ctx.h"
#include "src/storage/multi_data_source/ob_finish_transfer_in_mds_ctx.h"
#include "src/storage/multi_data_source/ob_start_transfer_in_mds_ctx.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;
using namespace oceanbase::compaction;
using namespace oceanbase::storage::mds;

#define USING_LOG_PREFIX STORAGE


namespace oceanbase
{
namespace unittest
{
class TestTransferMdsCtx : public ::testing::Test
{
public:
  TestTransferMdsCtx() = default;
  virtual ~TestTransferMdsCtx() = default;
};

TEST_F(TestTransferMdsCtx, start_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObStartTransferInMdsCtx start_ctx;
  start_ctx.version_ = ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = start_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStartTransferInMdsCtx de_start_ctx1;
  pos = 0;
  ret = de_start_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1, de_start_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStartTransferInMdsCtx de_start_ctx2;
  pos = 0;
  ret = de_start_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1, de_start_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObStartTransferInMdsCtx start_ctx2;
  ret = start_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStartTransferInMdsCtx de_start_ctx3;
  pos = 0;
  ret = de_start_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3, de_start_ctx3.version_);
}


TEST_F(TestTransferMdsCtx, finish_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObFinishTransferInMdsCtx finish_ctx;
  finish_ctx.version_ = ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = finish_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObFinishTransferInMdsCtx de_finish_ctx1;
  pos = 0;
  ret = de_finish_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1, de_finish_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObFinishTransferInMdsCtx de_finish_ctx2;
  pos = 0;
  ret = de_finish_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1, de_finish_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObFinishTransferInMdsCtx finish_ctx2;
  ret = finish_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObFinishTransferInMdsCtx de_finish_ctx3;
  pos = 0;
  ret = de_finish_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V2, de_finish_ctx3.version_);
}

TEST_F(TestTransferMdsCtx, abort_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObAbortTransferInMdsCtx abort_ctx;
  abort_ctx.version_ = ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = abort_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObAbortTransferInMdsCtx de_abort_ctx1;
  pos = 0;
  ret = de_abort_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1, de_abort_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObAbortTransferInMdsCtx de_abort_ctx2;
  pos = 0;
  ret = de_abort_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1, de_abort_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObAbortTransferInMdsCtx abort_ctx2;
  ret = abort_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObAbortTransferInMdsCtx de_abort_ctx3;
  pos = 0;
  ret = de_abort_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V2, de_abort_ctx3.version_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_transfer_mds_ctx.log*");
  OB_LOGGER.set_file_name("test_transfer_mds_ctx.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
