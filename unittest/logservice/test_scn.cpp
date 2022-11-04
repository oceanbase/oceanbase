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

#include "logservice/palf/scn.h"
#include <gtest/gtest.h>

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{

TEST(TestSCN, test_log_pos)
{
  const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  SCN scn_invalid;
  SCN scn1;
  SCN scn2;
  SCN scn3;
  SCN scn4;
  // Test invalid argument
  EXPECT_FALSE(scn_invalid.is_valid());

  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_gts(-1));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_gts(0));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_gts(4611686018427387904));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_gts(4611686018427387903));
  EXPECT_TRUE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_gts(100000));
  EXPECT_TRUE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn3.convert_for_gts(100000));
  EXPECT_TRUE(scn3.is_valid());

  scn1.reset();
  scn2.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_lsn_allocator(0));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_lsn_allocator(4611686018427387904));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_lsn_allocator(-1));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_lsn_allocator(4611686018427387903));
  EXPECT_TRUE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_lsn_allocator(100000));
  EXPECT_TRUE(scn1.is_valid());

  scn1.reset();
  scn2.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_inner_table_field(0));
  EXPECT_FALSE(scn_invalid.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_inner_table_field(-1));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_inner_table_field(4611686018427387904));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_inner_table_field(4611686018427387903));
  EXPECT_TRUE(scn1.is_valid());
  EXPECT_EQ(4611686018427387903, scn1.get_val_for_inner_table_field());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_inner_table_field(100000));
  EXPECT_TRUE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn2.convert_for_inner_table_field(90000));
  EXPECT_TRUE(scn2.is_valid());

  EXPECT_EQ(100, scn1.convert_to_ts());
  EXPECT_EQ(90, scn2.convert_to_ts());
  EXPECT_EQ(0, scn_invalid.convert_to_ts());
  EXPECT_EQ(0, scn4.convert_to_ts());

  EXPECT_EQ(100000, scn1.get_val_for_inner_table_field());
  EXPECT_EQ(90000, scn2.get_val_for_inner_table_field());
  EXPECT_EQ(0, scn4.get_val_for_inner_table_field());

  // Test operator <
  EXPECT_FALSE(scn1 == scn2);
  EXPECT_TRUE(scn1 == scn3);

  EXPECT_FALSE(scn1 != scn3);
  EXPECT_TRUE(scn1 != scn2);

  EXPECT_FALSE(scn1 < scn2);
  EXPECT_TRUE(scn2 < scn1);

  EXPECT_FALSE(scn1 <= scn2);
  EXPECT_TRUE(scn2 <= scn1);
  EXPECT_TRUE(scn1 <= scn3);

  EXPECT_FALSE(scn2 > scn1);
  EXPECT_TRUE(scn1 > scn2);

  EXPECT_FALSE(scn2 >= scn1);
  EXPECT_TRUE(scn1 >= scn2);
  EXPECT_TRUE(scn1 >= scn3);


  // Test serialization and deserialization
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, scn1.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, scn1.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, scn4.deserialize(buf, BUFSIZE, pos));
  EXPECT_EQ(scn1, scn4);
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_scn.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_scn");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
