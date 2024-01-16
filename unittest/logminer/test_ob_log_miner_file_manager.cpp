/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define private public
#include "ob_log_miner_file_manager.h"
#undef private
#include "gtest/gtest.h"

namespace oceanbase
{
namespace oblogminer
{

TEST(test_ob_log_miner_file_manager, GenFileHeader)
{
  ObLogMinerFileManager manager;
  const int64_t path_size = 100;
  char cwd_buf[path_size];
  ObArenaAllocator alloc;
  char *header = nullptr;
  int64_t data_len = 0;
  EXPECT_NE(nullptr, getcwd(cwd_buf, path_size));
  char path[OB_MAX_URI_LENGTH];
  char rm_dir_command[OB_MAX_URI_LENGTH];
  EXPECT_EQ(OB_SUCCESS, databuff_printf(path, sizeof(path), "file://%s/logminer_output", cwd_buf));
  EXPECT_EQ(OB_SUCCESS, databuff_printf(rm_dir_command, sizeof(rm_dir_command), "rm -rf %s/logminer_output", cwd_buf));
  EXPECT_EQ(OB_SUCCESS,
      manager.init(path, RecordFileFormat::CSV, ObLogMinerFileManager::FileMgrMode::ANALYZE));
  EXPECT_EQ(OB_SUCCESS, manager.generate_data_file_header_(alloc, header, data_len));
  EXPECT_STREQ(header, "TENANT_ID,TRANS_ID,PRIMARY_KEY,TENANT_NAME,DATABASE_NAME,"
  "TABLE_NAME,OPERATION,OPERATION_CODE,COMMIT_SCN,COMMIT_TIMESTAMP,SQL_REDO,SQL_UNDO,ORG_CLUSTER_ID\n");

  system(rm_dir_command);
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_log_miner_file_manager.log");
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_miner_file_manager.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}