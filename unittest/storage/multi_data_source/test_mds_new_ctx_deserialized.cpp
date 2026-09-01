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
#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#define private public
#define protected public

#include "src/storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "storage/multi_data_source/ob_tablet_truncate_mds_ctx.h"
namespace oceanbase {
namespace storage
{
namespace mds
{
void *MdsAllocator::alloc(const int64_t size)
{
  void *ptr = ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  ob_free(ptr);
}
}
}
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;

class TestMdsNewCtxDeserialized: public ::testing::Test
{
public:
  TestMdsNewCtxDeserialized() {};
  virtual ~TestMdsNewCtxDeserialized() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsNewCtxDeserialized);
};

TEST_F(TestMdsNewCtxDeserialized, deserialized_from_mds_ctx) {
  MdsCtx old_ctx;
  old_ctx.set_writer(MdsWriter(transaction::ObTransID(1)));
  old_ctx.set_binding_type_id(TupleTypeIdx<mds::BufferCtxTupleHelper, MdsCtx>::value);
  char buffer[1024];
  for (auto &ch : buffer)
    ch = 0xff;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_ctx.serialize(buffer, 1024, pos));
  int64_t buffer_len = pos;
  pos = 0;
  ObTabletCreateMdsCtx new_ctx1;
  ASSERT_EQ(OB_SUCCESS, new_ctx1.deserialize(buffer, buffer_len, pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_ctx1.deserialize(buffer, buffer_len + 1, pos));
  pos = 0;
  for (int idx = buffer_len; idx < 1024; ++idx)
    buffer[idx] = 0;
  ASSERT_EQ(OB_SUCCESS, new_ctx1.deserialize(buffer, buffer_len + 10, pos));
  pos = 0;
  ObStartTransferInMdsCtx new_ctx2;
  ASSERT_EQ(OB_SUCCESS, new_ctx2.deserialize(buffer, buffer_len, pos));
  pos = 0;
  ObFinishTransferInMdsCtx new_ctx3;
  ASSERT_EQ(OB_SUCCESS, new_ctx3.deserialize(buffer, buffer_len, pos));
}

TEST_F(TestMdsNewCtxDeserialized, truncate_ctx_assign_keeps_cleanup_ownership_local) {
  const transaction::ObTransID tx_id(1);
  ObTabletTruncateMdsCtx source_ctx{MdsWriter(tx_id)};
  source_ctx.set_ls_id(ObLSID(1001));
  source_ctx.set_tablet_id(ObTabletID(200001));
  source_ctx.state_ = TwoPhaseCommitState::ON_PREPARE;
  source_ctx.set_nop(false);

  ObTabletTruncateMdsCtx copied_ctx;
  EXPECT_EQ(OB_SUCCESS, copied_ctx.assign(source_ctx));
  EXPECT_EQ(source_ctx.get_writer(), copied_ctx.get_writer());
  EXPECT_EQ(source_ctx.state_, copied_ctx.state_);
  EXPECT_EQ(source_ctx.ls_id_, copied_ctx.ls_id_);
  EXPECT_EQ(source_ctx.tablet_id_, copied_ctx.tablet_id_);
  EXPECT_TRUE(copied_ctx.nop_);

  // Keep teardown side-effect free even when this test runs against the buggy implementation.
  source_ctx.set_nop(true);
  copied_ctx.set_nop(true);
}

TEST_F(TestMdsNewCtxDeserialized, truncate_ctx_rejects_missing_magic) {
  MdsCtx base_ctx;
  base_ctx.set_writer(MdsWriter(transaction::ObTransID(1)));
  base_ctx.set_binding_type_id(TupleTypeIdx<mds::BufferCtxTupleHelper, ObTabletTruncateMdsCtx>::value);

  char buffer[1024];
  for (auto &ch : buffer) {
    ch = 0;
  }
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, base_ctx.serialize(buffer, sizeof(buffer), pos));
  const int64_t base_ctx_len = pos;

  ObTabletTruncateMdsCtx truncate_ctx;
  pos = 0;
  EXPECT_NE(OB_SUCCESS, truncate_ctx.deserialize(buffer, base_ctx_len, pos));
}

TEST_F(TestMdsNewCtxDeserialized, truncate_ctx_rejects_bad_magic_before_next_ctx) {
  MdsCtx base_ctx;
  base_ctx.set_writer(MdsWriter(transaction::ObTransID(1)));
  base_ctx.set_binding_type_id(TupleTypeIdx<mds::BufferCtxTupleHelper, ObTabletTruncateMdsCtx>::value);
  ObTabletTruncateMdsCtx next_ctx{MdsWriter(transaction::ObTransID(2))};
  next_ctx.set_ls_id(ObLSID(1001));
  next_ctx.set_tablet_id(ObTabletID(200001));

  char buffer[1024];
  for (auto &ch : buffer) {
    ch = 0;
  }
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, base_ctx.serialize(buffer, sizeof(buffer), pos));
  const int64_t base_ctx_len = pos;
  ASSERT_EQ(OB_SUCCESS, next_ctx.serialize(buffer, sizeof(buffer), pos));
  const int64_t total_len = pos;

  int64_t magic_pos = base_ctx_len;
  int32_t decoded_magic = -1;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer, total_len, magic_pos, decoded_magic));
  ASSERT_NE(ObTabletTruncateMdsCtx::MAGIC, decoded_magic);

  ObTabletTruncateMdsCtx truncate_ctx;
  pos = 0;
  EXPECT_NE(OB_SUCCESS, truncate_ctx.deserialize(buffer, total_len, pos));
}

TEST_F(TestMdsNewCtxDeserialized, truncate_ctx_rejects_corrupted_new_extension) {
  MdsCtx base_ctx;
  base_ctx.set_writer(MdsWriter(transaction::ObTransID(1)));
  base_ctx.set_binding_type_id(TupleTypeIdx<mds::BufferCtxTupleHelper, ObTabletTruncateMdsCtx>::value);
  char buffer[1024];
  for (auto &ch : buffer) {
    ch = 0;
  }
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, base_ctx.serialize(buffer, sizeof(buffer), pos));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(buffer, sizeof(buffer), pos, ObTabletTruncateMdsCtx::MAGIC));
  const int64_t corrupted_len = pos;

  ObTabletTruncateMdsCtx truncate_ctx;
  pos = 0;
  EXPECT_NE(OB_SUCCESS, truncate_ctx.deserialize(buffer, corrupted_len, pos));
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_new_ctx_deserialized.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_new_ctx_deserialized.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  int64_t alloc_times = oceanbase::storage::mds::MdsAllocator::get_alloc_times();
  int64_t free_times = oceanbase::storage::mds::MdsAllocator::get_free_times();
  if (alloc_times != free_times) {
    MDS_LOG(ERROR, "memory may leak", K(free_times), K(alloc_times));
    ret = -1;
  } else {
    MDS_LOG(INFO, "all memory released", K(free_times), K(alloc_times));
  }
  return ret;
}
