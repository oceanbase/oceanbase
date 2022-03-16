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

#include <cstdlib>
#include <gtest/gtest.h>

#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_array.h"

#include "liboblog/src/ob_i_log_fetcher.h"
#include "liboblog/src/ob_log_fetcher_utils.h"
#include "liboblog/src/ob_log_fetcher.h"

#include "test_log_fetcher_common_utils.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;
using namespace storage;
using namespace transaction;
using namespace clog;
using namespace fetcher;

namespace oceanbase
{
namespace unittest
{


/*
 * Manual:
 *  - This test allows you to fetch log data from
 *    a single observer.
 *  - Partitions and data are set up by this test.
 */
/*
 * Fetch Log Test.
 * Use schema 1.
 */
TEST(DISABLED_ObLogFetcherEnhanced, FetchLogTest1)
//TEST(ObLogFetcherEnhanced, FetchLogTest1)
{
  ObClockGenerator::init();

  // Prepare svr.
  SvrCfg svr_cfg;
  svr_cfg.svr_addr_ = "10.210.177.162";
  svr_cfg.internal_port_ = 43000;
  svr_cfg.mysql_port_ = 43001;
  svr_cfg.mysql_db_ = "oceanbase";
  svr_cfg.mysql_password_ = "";
  svr_cfg.mysql_user_ = "root";
  svr_cfg.mysql_timeout_ = 1 * _SEC_;

  // Prepare table.
  ObArray<ObPartitionKey> pkeys;
  const int64_t table_cnt = 3;
  prepare_table_1(svr_cfg,
                  prepare_table_name_1(),
                  table_cnt,
                  prepare_table_schema_1(),
                  pkeys);

  // Print them.
  for (int64_t idx = 0; idx < pkeys.count(); ++idx) {
    ObPartitionKey &key = pkeys.at(idx);
    _I_(">>> add partition key", K(key));
  }

  // Prepare svr provider.
  MockSvrProvider1 svr_provider;
  ObAddr addr(ObAddr::IPV4, svr_cfg.svr_addr_, svr_cfg.mysql_port_);
  svr_provider.add_svr(addr);

  // Prepare err handler.
  MockLiboblogErrHandler1 err_handler;

  // Prepare parser.
  MockParser1 mock_parser;

  // Fetcher config.
  FetcherConfig fcfg;
  fcfg.reset();

  ObConcurrentFIFOAllocator fifo;
  int64_t G = 1024 * 1024 * 1024;
  EXPECT_EQ(OB_SUCCESS, fifo.init(1 * G, 1 * G, OB_MALLOC_BIG_BLOCK_SIZE));

  // Task Pool
  ObLogTransTaskPool<PartTransTask> task_pool;
  EXPECT_EQ(OB_SUCCESS, task_pool.init(&fifo, 10240, 1024, 4 * 1024L * 1024L, true));

  // Prepare fetcher.
  ObLogFetcherEnhanced fetcher;
  int ret = fetcher.init(&mock_parser,
                         &err_handler,
                         &svr_provider,
                         &task_pool,
                         fcfg);
  EXPECT_EQ(OB_SUCCESS, ret);

  // Add partitions.
  for (int64_t idx = 0; idx < pkeys.count(); ++idx) {
    ret = fetcher.start_fetch(pkeys.at(idx), 1);
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  // Start worker.
  ret = fetcher.start();
  EXPECT_EQ(OB_SUCCESS, ret);
  _I_(">>> Start fetch");

  // Generate data.
  _I_(">>> Generate data");
  const int64_t trans_cnt_per_part = 100;
  const int64_t part_cnt = table_cnt; // pcnt == table cnt.
  const int64_t trans_cnt = part_cnt * trans_cnt_per_part;
  ConnectorConfig cfg = prepare_cfg_1(svr_cfg);
  for (int64_t idx = 0; idx < table_cnt; ++idx) {
    DataGenerator1 gen(cfg);
    gen.insert(prepare_table_name_1()[idx], 0, trans_cnt_per_part);
    gen.join();
  }

  // Wait.
  while (mock_parser.get_trans_cnt() < trans_cnt) {
    usec_sleep(1 * _SEC_);
    _I_(">>> Waiting...");
  }

  // Stop everything.
  _I_(">>> Stop fetch");
  for (int64_t idx = 0; idx < pkeys.count(); ++idx) {
    ret = fetcher.stop_fetch(pkeys.at(idx));
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  fetcher.stop();
  ret = fetcher.destroy();
  EXPECT_EQ(OB_SUCCESS, ret);
}

}
}



int main(int argc, char **argv)
{
  ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}

