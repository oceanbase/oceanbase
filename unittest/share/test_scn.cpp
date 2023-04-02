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

#include "share/scn.h"
#include <gtest/gtest.h>
#include <cstdint>

namespace oceanbase
{
using namespace common;
using namespace share;

namespace unittest
{

TEST(TestSCN, test_scn)
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
  EXPECT_EQ(UINT64_MAX, scn_invalid.get_val_for_inner_table_field());

  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_gts(-1));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_gts(0));
  EXPECT_TRUE(scn1.is_valid());
  scn1.reset();
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
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_logservice(0));
  EXPECT_TRUE(scn1.is_valid());
  scn1.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_logservice(4611686018427387904));
  EXPECT_FALSE(scn1.is_valid());
  scn1.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_logservice(-1));
  EXPECT_FALSE(scn1.is_valid());
  scn1.reset();
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_logservice(4611686018427387903));
  EXPECT_TRUE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_logservice(100000));
  EXPECT_TRUE(scn1.is_valid());

  scn1.reset();
  scn2.reset();
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_inner_table_field(0));
  EXPECT_TRUE(scn1.is_valid());
  scn1.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_inner_table_field(-1));
  EXPECT_FALSE(scn1.is_valid());
  scn1.reset();
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
  SCN scn_convert;
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn_convert.convert_from_ts(46116860184273880));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn_convert.convert_from_ts(4611686018427388));
  EXPECT_EQ(OB_SUCCESS, scn_convert.convert_from_ts(4611686018427387));
  EXPECT_EQ(4611686018427387, scn_convert.convert_to_ts());
  EXPECT_EQ(OB_SUCCESS, scn_convert.convert_from_ts(0));
  EXPECT_EQ(0, scn_convert.convert_to_ts());
  EXPECT_EQ(OB_SUCCESS, scn_convert.convert_from_ts(1));
  EXPECT_EQ(1, scn_convert.convert_to_ts());

  EXPECT_EQ(100000, scn1.get_val_for_inner_table_field());
  EXPECT_EQ(90000, scn2.get_val_for_inner_table_field());
  EXPECT_EQ(UINT64_MAX, scn4.get_val_for_inner_table_field());

  SCN scn_min=SCN::min_scn();
  SCN scn_max=SCN::max_scn();
  SCN scn_base=SCN::base_scn();

  SCN scn_invalid1;

  EXPECT_EQ(0, scn_min.get_val_for_inner_table_field());
  EXPECT_EQ(4611686018427387903, scn_max.get_val_for_inner_table_field());
  EXPECT_EQ(1, scn_base.get_val_for_inner_table_field());

  //test get_val_for_tx() and get_val_for_tx()
  //
  scn1.reset();
  scn2.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_tx(-1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_tx(INT64_MAX-1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_tx(4611686018427387904));
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_tx(4611686018427387903));
  EXPECT_TRUE(scn1.is_max());
  EXPECT_EQ(INT64_MAX, scn1.get_val_for_tx());
  scn1.reset();
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_tx(INT64_MAX));
  EXPECT_TRUE(scn1.is_max());
  EXPECT_EQ(INT64_MAX, scn1.get_val_for_tx());

  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_tx(0));
  EXPECT_TRUE(scn1.is_min());
  EXPECT_EQ(0, scn1.get_val_for_tx());

  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_tx(1));
  EXPECT_TRUE(scn1 == SCN::base_scn());
  EXPECT_EQ(1, scn1.get_val_for_tx());

  EXPECT_TRUE(scn1.is_valid());
  scn1.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_inner_table_field(-1));
  EXPECT_FALSE(scn1.is_valid());
  scn1.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, scn1.convert_for_inner_table_field(4611686018427387904));
  EXPECT_FALSE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_inner_table_field(4611686018427387903));
  EXPECT_TRUE(scn1.is_valid());
  EXPECT_EQ(4611686018427387903, scn1.get_val_for_inner_table_field());
  EXPECT_EQ(OB_SUCCESS, scn1.convert_for_inner_table_field(100000));
  EXPECT_TRUE(scn1.is_valid());
  EXPECT_EQ(OB_SUCCESS, scn2.convert_for_inner_table_field(90000));
  EXPECT_TRUE(scn2.is_valid());



  // Test operator <
  EXPECT_FALSE(scn1 == scn2);
  EXPECT_TRUE(scn1 == scn3);
  EXPECT_FALSE(scn_invalid == scn2);
  EXPECT_TRUE(scn_invalid == scn_invalid1);

  EXPECT_FALSE(scn1 != scn3);
  EXPECT_TRUE(scn1 != scn2);
  EXPECT_FALSE(scn_invalid != scn_invalid1);

  EXPECT_FALSE(scn1 < scn2);
  EXPECT_TRUE(scn2 < scn1);
  EXPECT_TRUE(scn_invalid < scn2);
  EXPECT_FALSE(scn_invalid < scn_invalid1);

  EXPECT_FALSE(scn1 <= scn2);
  EXPECT_TRUE(scn2 <= scn1);
  EXPECT_TRUE(scn1 <= scn3);
  EXPECT_TRUE(scn_invalid <=scn2);
  EXPECT_TRUE(scn_invalid <= scn_invalid1);

  EXPECT_FALSE(scn2 > scn1);
  EXPECT_TRUE(scn1 > scn2);
  EXPECT_FALSE(scn_invalid > scn2);
  EXPECT_FALSE(scn_invalid > scn_invalid1);

  EXPECT_FALSE(scn2 >= scn1);
  EXPECT_TRUE(scn1 >= scn2);
  EXPECT_TRUE(scn1 >= scn3);
  EXPECT_FALSE(scn_invalid >= scn2);
  EXPECT_TRUE(scn_invalid >= scn_invalid1);


  // Test serialization and deserialization
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, scn1.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, scn1.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, scn4.deserialize(buf, BUFSIZE, pos));
  EXPECT_EQ(scn1, scn4);

}

TEST(TestSCN, test_update)
{
  SCN self;
  SCN result;
  SCN bigger;
  SCN smaller;
  SCN scn_invalid;

  //test inc_update and dec_update
  EXPECT_EQ(OB_SUCCESS, self.convert_for_logservice(100));
  EXPECT_EQ(OB_SUCCESS, bigger.convert_for_logservice(200));
  EXPECT_EQ(OB_SUCCESS, smaller.convert_for_logservice(1));

  result = self.inc_update(scn_invalid);
  EXPECT_EQ(result, self);
  EXPECT_FALSE(self == scn_invalid);

  result = self.inc_update(smaller);
  EXPECT_EQ(result, self);
  EXPECT_FALSE(self == smaller);
  result = self.inc_update(bigger);
  EXPECT_EQ(self, bigger);
  EXPECT_EQ(result, bigger);

  result = self.dec_update(smaller);
  EXPECT_EQ(result, smaller);
  EXPECT_EQ(self, smaller);
  result = self.dec_update(bigger);
  EXPECT_EQ(result, smaller);
  EXPECT_EQ(self, smaller);

  result = self.dec_update(scn_invalid);
  EXPECT_EQ(result, scn_invalid);
  EXPECT_EQ(self, scn_invalid);
  result = scn_invalid.inc_update(bigger);
  EXPECT_EQ(scn_invalid, bigger);
  EXPECT_EQ(result, bigger);

}

TEST(TestSCN, test_serialize)
{
  SCN scn;
  SCN max;

  max.set_max();
  const int64_t BUF_LEN = 128;
  char buf1[BUF_LEN] = {0};
  char buf2[BUF_LEN] = {0};
  char buf3[BUF_LEN] = {0};
  char buf4[BUF_LEN] = {0};
  int64_t pos1 = 0;
  int64_t pos2 = 0;
  int64_t pos3 = 0;
  int64_t pos4 = 0;

  EXPECT_EQ(OB_SUCCESS, scn.convert_for_logservice(100));
  EXPECT_EQ(scn.get_fixed_serialize_size(), max.get_fixed_serialize_size());
  EXPECT_EQ(OB_SUCCESS, scn.fixed_serialize(buf1, BUF_LEN, pos1));
  EXPECT_EQ(OB_SUCCESS, max.fixed_serialize(buf2, BUF_LEN, pos2));

  EXPECT_EQ(OB_SUCCESS, scn.serialize(buf3, BUF_LEN, pos3));
  EXPECT_EQ(OB_SUCCESS, max.serialize(buf4, BUF_LEN, pos4));

  SCN de_scn;
  SCN de_max;
  int64_t de_pos1 = 0;
  int64_t de_pos2 = 0;
  int64_t de_pos3 = 0;
  int64_t de_pos4 = 0;
  EXPECT_EQ(OB_SUCCESS, de_scn.fixed_deserialize(buf1, pos1, de_pos1));
  EXPECT_EQ(OB_SUCCESS, de_max.fixed_deserialize(buf2, pos2, de_pos2));
  EXPECT_EQ(scn, de_scn);
  EXPECT_EQ(max, de_max);

  EXPECT_EQ(pos1, de_pos1);
  EXPECT_EQ(pos2, de_pos2);

  de_scn.reset();
  de_max.reset();
  EXPECT_EQ(OB_SUCCESS, de_scn.deserialize(buf3, pos3, de_pos3));
  EXPECT_EQ(OB_SUCCESS, de_max.deserialize(buf4, pos4, de_pos4));

  EXPECT_EQ(scn, de_scn);
  EXPECT_EQ(max, de_max);
  EXPECT_EQ(pos3, de_pos3);
  EXPECT_EQ(pos4, de_pos4);


  EXPECT_EQ(false, de_scn == SCN::max_scn());

  pos1 = 0;
  int64_t val = INT64_MAX;
  EXPECT_EQ(OB_SUCCESS, oceanbase::serialization::encode(buf1, BUF_LEN, pos1, val));
  de_pos1 = 0;
  EXPECT_EQ(OB_SUCCESS, de_scn.deserialize(buf1, pos1, de_pos1));
  EXPECT_EQ(pos1, de_pos1);
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
