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

#include "share/ob_scanner.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
using namespace oceanbase::common;
class TestScanner: public ::testing::Test
{
public:
  TestScanner();
  virtual ~TestScanner();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestScanner);
protected:
  // function members
protected:
  // data members
};

TestScanner::TestScanner()
{
}

TestScanner::~TestScanner()
{
}

void TestScanner::SetUp()
{
}

void TestScanner::TearDown()
{
}

TEST_F(TestScanner, basic_test)
{
  ObScanner scanner;
  ObObj objs[3];
  ObNewRow row;
  row.cells_ = objs;
  row.count_ = 3;
  row.cells_[0].set_int(1);
  row.cells_[1].set_int(2);
  row.cells_[2].set_int(3);

  for (int i = 0; i < 1024; ++i) {
    OK(scanner.add_row(row));
  }
  ASSERT_EQ(1024, scanner.get_row_count());
  ASSERT_EQ(3, scanner.get_col_count());

  ObScanner::Iterator it = scanner.begin();
  ObNewRow row2;
  row2.cells_ = objs;
  row2.count_ = 2;
  BAD(it.get_next_row(row2));
  row2.count_ = 3;
  for (int i = 0; i < 1024; ++i) {
    OK(it.get_next_row(row2));
    ASSERT_EQ(3, row2.count_);
    _OB_LOG(INFO, "row=%s", S(row2));
  }
}

TEST_F(TestScanner, serialization)
{
  ObScanner scanner;
  scanner.set_mem_size_limit(1024);
  scanner.set_affected_rows(10);
  scanner.set_last_insert_id_to_client(111);
  scanner.set_last_insert_id_session(121);
  scanner.set_last_insert_id_changed(true);
  scanner.set_found_rows(100);
  scanner.set_err_code(OB_ERR_UNEXPECTED);
  scanner.store_err_msg("how are you");
  scanner.set_row_matched_count(1000);
  scanner.set_row_duplicated_count(2000);
  scanner.set_extend_info("fine,thank you, and you");
  scanner.set_is_result_accurate(false);
  scanner.init();

  ObObj objs[3];
  ObNewRow row;
  row.cells_ = objs;
  row.count_ = 3;
  row.cells_[0].set_int(1);
  row.cells_[1].set_int(2);
  row.cells_[2].set_int(3);

  for (int i = 0; i < 10; ++i) {
    OK(scanner.add_row(row));
  }

  ModuleArena allocator;
  int64_t buf_size = scanner.get_serialize_size();
  char *buf = (char *)allocator.alloc(buf_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.serialize(buf, buf_size, pos));
  ASSERT_EQ(buf_size, pos);

  ObScanner out_scanner;
  pos = 0;
  ObString err_msg(scanner.get_err_msg());
  ObString err_msg_1("how are you");
  ASSERT_EQ(OB_SUCCESS, out_scanner.deserialize(buf, buf_size, pos));
  ASSERT_EQ(scanner.get_mem_size_limit(), 1024);
  ASSERT_EQ(scanner.get_affected_rows(), 10);
  ASSERT_EQ(scanner.get_last_insert_id_to_client(), 111);
  ASSERT_EQ(scanner.get_last_insert_id_session(), 121);
  ASSERT_EQ(scanner.get_last_insert_id_changed(), true);
  ASSERT_EQ(scanner.get_found_rows(), 100);
  ASSERT_EQ(scanner.get_err_code(), OB_ERR_UNEXPECTED);
  ASSERT_EQ(err_msg.compare(err_msg_1), 0);
  ASSERT_EQ(scanner.get_row_matched_count(), 1000);
  ASSERT_EQ(scanner.get_row_duplicated_count(), 2000);
  ASSERT_EQ(scanner.get_extend_info(), "fine,thank you, and you");
  ASSERT_EQ(scanner.is_result_accurate(), false);

  ObScanner::Iterator it = out_scanner.begin();
  ObNewRow row2;
  row2.cells_ = objs;
  row2.count_ = 2;
  BAD(it.get_next_row(row2));
  row2.count_ = 3;
  for (int i = 0; i < 10; ++i) {
    OK(it.get_next_row(row2));
    ASSERT_EQ(3, row2.count_);
    _OB_LOG(INFO, "row=%s", S(row2));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
