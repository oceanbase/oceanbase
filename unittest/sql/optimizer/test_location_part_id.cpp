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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "observer/ob_server.h"
#include "observer/ob_req_time_service.h"
#include <test_optimizer_utils.h>
#define BUF_LEN 102400 // 100K
using std::cout;
using namespace oceanbase::json;
using oceanbase::sql::ObTableLocation;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
namespace test
{
class TestLocationPartitionId :public TestOptimizerUtils
{
public:
  TestLocationPartitionId(){}
  ~TestLocationPartitionId(){}
};

TEST_F(TestLocationPartitionId, calc_part_id_by_rowkeys)
{
  ObSQLSessionInfo session_info;
  ObTZInfoMap tz_info;
  ASSERT_EQ(OB_SUCCESS, tz_info.init(ObModIds::OB_HASH_BUCKET_TIME_ZONE_INFO_MAP));
  ASSERT_EQ(OB_SUCCESS, session_info.init(0, 0, &allocator_, &tz_info));
  ASSERT_EQ(OB_SUCCESS, session_info.load_default_sys_variable(false, false));
  const char *result_file = "./test_location_part_id.result";
  const char *tmp_file = "./test_location_part_id.tmp";
  const char *test_file = "./test_location_part_id.test";
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  ObString database_name = "calc_partition";
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  char buf[BUF_LEN];
  int64_t case_num = 0;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0) continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#') continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';') continue;
    else {
      ++case_num;
      ObString json_str(total_line.length() - 1, total_line.c_str());
      ObString table_name;
      ObSEArray<ObSEArray<ObObj, 3>, 1> row_array;
      ASSERT_EQ(OB_SUCCESS, parse_row_from_json(json_str, table_name, row_array));
      ObSEArray<ObRowkey, 3> rowkeys;
      ObTableLocation table_location;
      ObSEArray<int64_t, 3> part_ids;
      ObSEArray<RowkeyArray, 3> rowkey_lists;
      const ObTableSchema *table_schema = NULL;
      ASSERT_EQ(OB_SUCCESS, schema_guard_.get_table_schema(sys_tenant_id_, database_name, table_name, false, table_schema));
      ASSERT_EQ(OB_SUCCESS, convert_row_array_to_rowkeys(row_array, rowkeys, table_schema->get_rowkey_info(), session_info_.get_timezone_info()));
      ASSERT_EQ(OB_SUCCESS, table_location.calculate_partition_ids_by_rowkey(session_info_, schema_guard_, table_schema->get_table_id(), rowkeys, part_ids, rowkey_lists));
      memset(buf, 0, BUF_LEN);
      of_result << "----------------Case" << case_num <<" start----------------" << std::endl;
      of_result << "rowkey json"<< ": " << total_line << std::endl;
      of_result << "part_ids: " << to_cstring(part_ids) << std::endl;
      rowkey_lists.to_string(buf, BUF_LEN);
      of_result << "rowkey_lists: " << buf << std::endl;
      of_result << "----------------Case" << case_num <<" end----------------" << std::endl;
      total_line = "";
    }
  }
  if_tests.close();
  of_result.close();
  formalize_tmp_file(tmp_file);
  TestSqlUtils::is_equal_content(tmp_file, result_file);
}
}
int main(int argc, char **argv)
{
  //char c;
  //std::cout<<"Press any key to continue..."<<std::endl;
  //std::cin>>c;
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  system("rm -rf test_location_part_id.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_location_part_id.log", true);
  oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  if(argc >= 2)
  {
    if (strcmp("DEBUG", argv[1]) == 0
        || strcmp("WARN", argv[1]) == 0)
    OB_LOGGER.set_log_level(argv[1]);
  }
  test::parse_cmd_line_param(argc, argv, test::clp);
  ret = RUN_ALL_TESTS();
  return ret;
}
