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

#define protected public
#define private public
#define UNITTEST
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx_table/ob_tx_table.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace storage;
using namespace blocksstable;
using namespace share;

static int turn = 0;
static ObTxData src_tx_data;
static ObTxData dst_tx_data;

namespace storage {
int ObTxTable::check_with_tx_data(ObReadTxDataArg &read_tx_data_arg,
                                  ObITxDataCheckFunctor &fn)
{
  TRANS_LOG(INFO, "turn is", K(turn));
  if (turn == 0) {
    // use dst
    turn++;
    return fn(dst_tx_data, NULL);
  } else if (turn == 1) {
    // use src
    turn++;
    return fn(src_tx_data, NULL);
  } else {
    return OB_SUCCESS;
  }
}


}

namespace unittest
{

class TestTxTableGuards : public ::testing::Test
{
public:
  TestTxTableGuards() {}
  virtual void SetUp() override
  {
    turn = 0;
    src_tx_data.reset();
    dst_tx_data.reset();
    TRANS_LOG(INFO, "setup success");
  }

  virtual void TearDown() override
  {
    turn = 0;
    src_tx_data.reset();
    dst_tx_data.reset();
    TRANS_LOG(INFO, "teardown success");
  }

  static void SetUpTestCase()
  {
    turn = 0;
    src_tx_data.reset();
    dst_tx_data.reset();

    TRANS_LOG(INFO, "SetUpTestCase");
  }
  static void TearDownTestCase()
  {
    turn = 0;
    src_tx_data.reset();
    dst_tx_data.reset();

    TRANS_LOG(INFO, "TearDownTestCase");
  }

};

TEST_F(TestTxTableGuards, check_on_single_dest_1) {
  ObTxTable dst_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(100);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(90);
  dst_tx_data.state_ = ObTxData::COMMIT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(dst_tx_data.commit_version_, trans_version);
  EXPECT_EQ(dst_tx_data.state_, ObTxData::COMMIT);
}

TEST_F(TestTxTableGuards, check_on_single_dest_2) {
  ObTxTable dst_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(80);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(90);
  dst_tx_data.state_ = ObTxData::COMMIT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(trans_version, SCN::max_scn());
  EXPECT_EQ(state, ObTxData::RUNNING);
}

TEST_F(TestTxTableGuards, check_on_dest_and_src_1) {
  ObTxTable dst_tx_table;
  ObTxTable src_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(100);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  guards.src_tx_table_guard_.tx_table_ = &src_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(90);
  dst_tx_data.state_ = ObTxData::COMMIT;

  src_tx_data.commit_version_.convert_from_ts(2);
  src_tx_data.end_scn_.convert_from_ts(90);
  src_tx_data.state_ = ObTxData::COMMIT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(dst_tx_data.commit_version_, trans_version);
  EXPECT_EQ(dst_tx_data.state_, ObTxData::COMMIT);
}

TEST_F(TestTxTableGuards, check_on_dest_and_src_2) {
  ObTxTable dst_tx_table;
  ObTxTable src_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(100);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  guards.src_tx_table_guard_.tx_table_ = &src_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(110);
  dst_tx_data.state_ = ObTxData::ABORT;

  src_tx_data.commit_version_.convert_from_ts(2);
  src_tx_data.end_scn_.convert_from_ts(90);
  src_tx_data.state_ = ObTxData::COMMIT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(SCN::max_scn(), trans_version);
  EXPECT_EQ(state, ObTxData::RUNNING);
}

TEST_F(TestTxTableGuards, check_on_dest_and_src_3) {
  ObTxTable dst_tx_table;
  ObTxTable src_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(100);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  guards.src_tx_table_guard_.tx_table_ = &src_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(110);
  dst_tx_data.state_ = ObTxData::COMMIT;

  src_tx_data.commit_version_.convert_from_ts(2);
  src_tx_data.end_scn_.convert_from_ts(90);
  src_tx_data.state_ = ObTxData::ABORT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(SCN::min_scn(), trans_version);
  EXPECT_EQ(src_tx_data.state_, state);
}


} // namespace unittest
} // namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_tx_table_guards.log*");
  OB_LOGGER.set_file_name("test_tx_table_guards.log");
  OB_LOGGER.set_log_level("DEBUG");
  STORAGE_LOG(INFO, "begin unittest: test tx table guards");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
