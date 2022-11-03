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

#define USING_LOG_PREFIX PL
#include <gtest/gtest.h>
#include "observer/ob_server.h"
#include "test_pl_utils.h"
#include "sql/ob_sql_init.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace observer;
using namespace test;

class ObPLTest : public TestPLUtils, public ::testing::Test
{
public:
  ObPLTest() {}
  virtual ~ObPLTest() {}
  virtual void SetUp() { init(); }
  virtual void TearDown() {}
  virtual void init() { TestPLUtils::init(); }

public:
  // data members

};

TEST_F(ObPLTest, test_resolve)
{
  const char* test_file = "./test_pl.sql";
  const char* result_file = "./test_resolve.result";
  const char* tmp_file = "./test_resolve.tmp";
  ASSERT_NO_FATAL_FAILURE(resolve_test(test_file, result_file, tmp_file));
}

TEST_F(ObPLTest, test_compile)
{
  const char* test_file = "./test_pl.sql";
  const char* result_file = "./test_compile.result";
  const char* tmp_file = "./test_compile.tmp";
  ASSERT_NO_FATAL_FAILURE(compile_test(test_file, result_file, tmp_file));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  ObScannerPool::build_instance();
  ObIntermResultPool::build_instance();
  ObServerOptions opts;
  opts.cluster_id_ = 1;
  opts.rs_list_ = "127.0.0.1:1";
  opts.zone_ = "test1";
  opts.data_dir_ = "/tmp";
  opts.mysql_port_ = 23000 + getpid() % 1000;
  opts.rpc_port_ = 24000 + getpid() % 1000;
  system("mkdir -p /tmp/sstable /tmp/clog /tmp/slog");
  GCONF.datafile_size = 41943040;
//  OBSERVER.init(opts);
  int ret = 0;//RUN_ALL_TESTS();
  OBSERVER.destroy();
  return ret;
}

