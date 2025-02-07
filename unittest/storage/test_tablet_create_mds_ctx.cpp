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
#include "storage/multi_data_source/ob_tablet_create_mds_ctx.h"
#include "storage/tx/ob_trans_define.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace unittest
{
class TestTabletCreateMdsCtx : public ::testing::Test
{
public:
  TestTabletCreateMdsCtx() = default;
  virtual ~TestTabletCreateMdsCtx() = default;
};

TEST_F(TestTabletCreateMdsCtx, start_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;

  mds::ObTabletCreateMdsCtx mds_ctx{mds::MdsWriter{transaction::ObTransID{123}}};
  mds_ctx.set_ls_id(share::ObLSID(1001));

  // serialize
  const int64_t serialize_size = mds_ctx.get_serialize_size();
  char *buffer = new char[serialize_size]();
  int64_t pos = 0;
  ret = mds_ctx.serialize(buffer, serialize_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  // deserialize
  mds::ObTabletCreateMdsCtx ctx;
  pos = 0;
  ret = ctx.deserialize(buffer, serialize_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, serialize_size);
  ASSERT_EQ(ctx.ls_id_, mds_ctx.ls_id_);
  ASSERT_EQ(ctx.writer_.writer_id_, mds_ctx.writer_.writer_id_);

  delete [] buffer;
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tablet_create_mds_ctx.log*");
  OB_LOGGER.set_file_name("test_tablet_create_mds_ctx.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
