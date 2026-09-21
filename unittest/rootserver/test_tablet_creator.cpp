/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <vector>
#include "lib/alloc/ob_malloc_callback.h"
#include "lib/allocator/ob_malloc.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;
using obrpc::ObCreateTabletInfo;
using obrpc::ObBatchCreateTabletArg;

// The pre-fix layout: nested ObSArray members that always allocate a heap block on
// deserialize. Kept here to cross-check that switching to ObSEArray does not change
// the wire format.
struct LegacyCreateTabletInfo
{
  OB_UNIS_VERSION(1);
public:
  ObSArray<ObTabletID> tablet_ids_;
  ObTabletID data_tablet_id_;
  ObSArray<int64_t> table_schema_index_;
  lib::Worker::CompatMode compat_mode_ = lib::Worker::CompatMode::MYSQL;
  bool is_create_bind_hidden_tablets_ = false;
  ObSArray<int64_t> create_commit_versions_;
  bool has_cs_replica_ = false;
};

OB_SERIALIZE_MEMBER(LegacyCreateTabletInfo, tablet_ids_, data_tablet_id_, table_schema_index_,
                    compat_mode_, is_create_bind_hidden_tablets_, create_commit_versions_, has_cs_replica_);

// Counts how many normal-size (7936B) heap blocks were requested while alive.
class BlockCounter : public lib::ObMallocCallback
{
public:
  void operator()(const lib::ObMemAttr &, int64_t used, const lib::AObject &) override
  {
    if (used == OB_MALLOC_NORMAL_BLOCK_SIZE) {
      ++normal_block_count_;
    }
  }
  int64_t normal_block_count_ = 0;
};

template <typename Info>
void fill_info(Info &info, const int64_t count, const uint64_t first_id = 200001)
{
  info.data_tablet_id_ = ObTabletID(first_id);
  info.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, info.tablet_ids_.push_back(ObTabletID(first_id + i)));
    ASSERT_EQ(OB_SUCCESS, info.table_schema_index_.push_back(i));
  }
}

// The ObSArray -> ObSEArray<_,4> switch must not change the serialized bytes, so that
// mixed-version RPC/replay stays compatible. Check the inline boundary (<=4) and beyond.
TEST(TestCreateTabletInfo, wire_compatibility)
{
  for (const int64_t count : {1, 4, 5}) {
    SCOPED_TRACE(count);
    LegacyCreateTabletInfo old_info;
    ObCreateTabletInfo new_info;
    fill_info(old_info, count);
    fill_info(new_info, count);
    ASSERT_TRUE(new_info.is_valid());
    ASSERT_EQ(old_info.get_serialize_size(), new_info.get_serialize_size());

    std::vector<char> old_buf(old_info.get_serialize_size());
    std::vector<char> new_buf(new_info.get_serialize_size());
    int64_t old_pos = 0;
    int64_t new_pos = 0;
    ASSERT_EQ(OB_SUCCESS, old_info.serialize(old_buf.data(), old_buf.size(), old_pos));
    ASSERT_EQ(OB_SUCCESS, new_info.serialize(new_buf.data(), new_buf.size(), new_pos));
    ASSERT_EQ(old_buf, new_buf);

    // New reader must accept legacy bytes and produce identical content.
    ObCreateTabletInfo reader;
    new_pos = 0;
    ASSERT_EQ(OB_SUCCESS, reader.deserialize(old_buf.data(), old_buf.size(), new_pos));
    ASSERT_EQ(count, reader.tablet_ids_.count());
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(ObTabletID(200001 + i), reader.tablet_ids_.at(i));
      EXPECT_EQ(i, reader.table_schema_index_.at(i));
    }
  }
}

// Core of this change: replaying many small tablet infos must not blow up memory.
// The legacy layout allocated two 7936B blocks per info (tablet_ids_ + index_);
// with ObSEArray<_,4> those small arrays stay inline, so no per-info block is taken.
TEST(TestCreateTabletInfo, replay_does_not_bloat_memory)
{
  // A single legacy one-tablet info takes two normal blocks on deserialize.
  LegacyCreateTabletInfo old_info;
  fill_info(old_info, 1);
  std::vector<char> old_buf(old_info.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_info.serialize(old_buf.data(), old_buf.size(), pos));
  {
    BlockCounter old_counter;
    lib::ObMallocCallbackGuard guard(old_counter);
    LegacyCreateTabletInfo old_reader;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, old_reader.deserialize(old_buf.data(), old_buf.size(), pos));
#ifndef OB_USE_ASAN
    EXPECT_EQ(2, old_counter.normal_block_count_);
#endif
  }

  // Serialize 50000 single-tablet infos, then replay them.
  const int64_t info_count = 50000;
  ObBatchCreateTabletArg batch;
  ObTableSchema schema;
  ASSERT_EQ(OB_SUCCESS, ObInnerTableSchema::all_dummy_schema(schema));
  ASSERT_EQ(OB_SUCCESS, batch.init_create_tablet(ObLSID(1001), SCN::base_scn(), false));
  ASSERT_EQ(OB_SUCCESS, batch.table_schemas_.push_back(schema));
  ASSERT_EQ(OB_SUCCESS, batch.tablets_.prepare_allocate(info_count));
  for (int64_t i = 0; i < info_count; ++i) {
    fill_info(batch.tablets_.at(i), 1, 200001 + i);
  }
  ASSERT_TRUE(batch.is_valid());
  std::vector<char> buf(batch.get_serialize_size());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, batch.serialize(buf.data(), buf.size(), pos));
  batch.reset();

  BlockCounter counter;
  {
    lib::ObMallocCallbackGuard guard(counter);
    ObBatchCreateTabletArg replay_arg;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, replay_arg.deserialize(buf.data(), buf.size(), pos));
    ASSERT_EQ(info_count, replay_arg.get_tablet_count());
    ASSERT_TRUE(replay_arg.is_valid());
    EXPECT_EQ(ObTabletID(200001), replay_arg.tablets_.at(0).tablet_ids_.at(0));
#if !defined(DISABLE_SE_ARRAY) && !defined(OB_USE_ASAN)
    // Legacy would take 2 * 50000 blocks (~760MB); inline arrays take almost none.
    EXPECT_LT(counter.normal_block_count_, 32);
#endif
    replay_arg.reset();
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_tablet_creator.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
