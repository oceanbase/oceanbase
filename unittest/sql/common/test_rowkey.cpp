/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "sql/ob_sql_init.h"

using namespace oceanbase;
//using namespace oceanbase::sql;
using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace test
{
const int64_t SER_BUF_LEN = 10001;
const int64_t ITEM_CNT = 5;
class TestRowkey: public ::testing::Test
{
public:
  TestRowkey() {}
  ~TestRowkey() {}
};

TEST_F(TestRowkey, test_serialize)
{
  char buf[ITEM_CNT][SER_BUF_LEN];
  ObRowkey row_key[ITEM_CNT];
  int64_t pos = 0;
  // serialize
  ObObj objs[ITEM_CNT];
  for (int64_t i = 0; i < ITEM_CNT ; i++) {
    objs[i].set_int(i);
    row_key[i].assign(&objs[i], 1);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, row_key[i].serialize(buf[i], SER_BUF_LEN, pos));
  }

  //deserialize
  ObRowkey de_row_key;
  for (int i = 0 ; i < ITEM_CNT; ++i) {
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, de_row_key.deserialize(buf[i], SER_BUF_LEN, pos));
    ASSERT_EQ(true, de_row_key == row_key[i]);
  }
}
}

int main(int argc, char **argv)
{
  init_sql_factories();
  OB_LOGGER.set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
