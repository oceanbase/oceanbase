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
#include "storage/memtable/ob_memtable_context.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{

class TestObTxMisc : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestObTxMisc, multiple_checksum_collapse_for_commit_log)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // only one checksum
  {
    uint64_t checksum0 = 12323221;
    ObArrayHelper<uint64_t> arr(1, &checksum0, 1);
    uint8_t signature0 = 0;
    ObArrayHelper<uint8_t> sig(1, &signature0, 1);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 12323221);
    EXPECT_EQ(0, sig.count());
  }
  // multiple, but only one is valid
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 12323221);
    EXPECT_EQ(0, sig.count());
    // valid is in middle
    checksum[0] = 1;
    checksum[13] = 34443;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 34443);
    EXPECT_EQ(0, sig.count());
  }
  // multiple, multiple valid: 1, 13
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1;
    checksum[13] = 34443;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_GT(result, 1);
    EXPECT_NE(result, 12323221);
    EXPECT_NE(result, 34443);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(12323221 & 0xFF, sig.at(0));
    EXPECT_EQ(34443 & 0xFF, sig.at(13));
  }
  // multiple, multiple valid, 18,21
  {
    uint64_t checksum[64];
    for (int i = 0; i < 64; i++) checksum[i] = 1;
    checksum[18] = 34443;
    checksum[21] = 34444;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_GT(result, 1);
    EXPECT_NE(result, 34443);
    EXPECT_NE(result, 34444);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(34443 & 0xFF, sig.at(18));
    EXPECT_EQ(34444 & 0xFF, sig.at(21));
  }
  // multiple, all is valid
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1 + i;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_NE(result, 12323221);
    EXPECT_GT(result, 1);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(12323221 & 0xFF, sig.at(0));
  }
}

TEST_F(TestObTxMisc, TxDesc_add_mofied_tables)
{
  ObTxDesc txdesc;
  {
    uint64_t tids[] = {1,2,3,4,5,6,7};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 9; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9, 10};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9, 10, 1, 5, 7};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {11,12,13,14,15,16,17,18,19,20};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 20; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {20, 19, 17, 14, 18, 20, 1, 7, 5, 4, 9};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 20; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_tx_misc.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
