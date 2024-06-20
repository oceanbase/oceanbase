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

// #define USING_LOG_PREFIX SQL_ENGINE
#define USING_LOG_PREFIX COMMON
#include <iterator>
#include <gtest/gtest.h>
#include "../test_op_engine.h"
#include "../ob_test_config.h"
#include <vector>
#include <string>

using namespace ::oceanbase::sql;

namespace test
{
class TestHashGroupByVec : public TestOpEngine
{
public:
  TestHashGroupByVec();
  virtual ~TestHashGroupByVec();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestHashGroupByVec);

protected:
  // function members
protected:
  // data members
};

TestHashGroupByVec::TestHashGroupByVec()
{
  std::string schema_filename = ObTestOpConfig::get_instance().test_filename_prefix_ + ".schema";
  strcpy(schema_file_path_, schema_filename.c_str());
}

TestHashGroupByVec::~TestHashGroupByVec()
{}

void TestHashGroupByVec::SetUp()
{
  TestOpEngine::SetUp();
}

void TestHashGroupByVec::TearDown()
{
  destroy();
}

TEST_F(TestHashGroupByVec, basic_test)
{
  std::string test_file_path = ObTestOpConfig::get_instance().test_filename_prefix_ + ".test";
  int ret = basic_random_test(test_file_path);
  EXPECT_EQ(ret, 0);
}

// TEST_F(TestHashGroupByVec, basic_test2)
// {
//   int ret = OB_SUCCESS;
//   std::string test_file_path = ObTestOpConfig::get_instance().test_filename_prefix_ + ".test";
//   if(OB_FAIL(basic_random_test_output_to_file(test_file_path, true))) {
//     LOG_ERROR("Some error occur in running vectorization 2.0 operator", K(ret));
//   } else if (OB_FAIL(basic_random_test_output_to_file(test_file_path, false))) {
//     LOG_ERROR("Some error occur in running original operator", K(ret));
//   }
//   EXPECT_EQ(ret, 0);
// }

// TEST_F(TestHashGroupByVec, your_own_test)
// {
//   std::string test_file_path = ObTestOpConfig::get_instance().test_filename_prefix_ + ".test";
//   std::ifstream if_tests(test_file_path);
//   if (if_tests.is_open() == false) { return; }
//   std::string line;

//   while (std::getline(if_tests, line)) {
//     // handle query
//     if (line.size() <= 0) continue;
//     if (line.at(0) == '#') continue;

//     ObOperator *root = NULL;
//     ObExecutor exector;
//     if (OB_FAIL(get_tested_op_from_string(line, false, root, exector))) {
//       LOG_WARN("generate tested op fail, sql: ", K(line.data()));
//     } else {
//       int round = 1;
//       const int64_t max_row_cnt = 256;
//       const ObBatchRows *child_brs = nullptr;

//       LOG_INFO("============== Final output ===============", K(round));
//       while (!root->brs_.end_) {
//         if (OB_FAIL(root->get_next_batch(max_row_cnt, child_brs))) {
//           LOG_ERROR("root op fail to get_next_batch data", K(original_root));
//           break;
//         }
//       }
//     }
//   }
// }
} // namespace test

int main(int argc, char **argv)
{
  ObTestOpConfig::get_instance().test_filename_prefix_ = "test_hash_groupby2";
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-bg") == 0) {
      ObTestOpConfig::get_instance().test_filename_prefix_ += "_bg";
      ObTestOpConfig::get_instance().run_in_background_ = true;
    }
  }
  ObTestOpConfig::get_instance().init();

  system(("rm -f " + ObTestOpConfig::get_instance().test_filename_prefix_ + ".log").data());
  system(("rm -f " + ObTestOpConfig::get_instance().test_filename_prefix_ + ".log.*").data());
  oceanbase::common::ObClockGenerator::init();
  observer::ObReqTimeGuard req_timeinfo_guard;
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name((ObTestOpConfig::get_instance().test_filename_prefix_ + ".log").data(), true);
  init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}