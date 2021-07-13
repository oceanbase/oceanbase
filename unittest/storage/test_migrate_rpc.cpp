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
#include "lib/net/ob_addr.h"
#include "storage/ob_partition_service_rpc.h"
#include "rpc/testing.h"
#include "mockcontainer/mock_ob_partition_service.h"
#include "mockcontainer/mock_ob_iterator.h"
#include "storage/ob_partition_base_data_ob_reader.h"
#include "storage/ob_i_store.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace storage;
using namespace blocksstable;
using namespace memtable;

namespace obrpc {
class TestMigrateRpc : public ::testing::Test {
public:
  static void SetUpTestCase();
  void SetUp()
  {}
  void TearDown()
  {}
  static rpctesting::Service service_;
  static ObPartitionServiceRpcProxy rpc_proxy_;
  static ObInOutBandwidthThrottle bandwidth_throttle_;
  static ObAddr self_addr_;
};

rpctesting::Service TestMigrateRpc::service_;
ObPartitionServiceRpcProxy TestMigrateRpc::rpc_proxy_;
ObInOutBandwidthThrottle TestMigrateRpc::bandwidth_throttle_;
ObAddr TestMigrateRpc::self_addr_;

void TestMigrateRpc::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, service_.init());
  self_addr_ = ObAddr(ObAddr::IPV4, "127.0.0.1", service_.get_listen_port());

  ASSERT_EQ(OB_SUCCESS, service_.get_proxy(rpc_proxy_));
  ASSERT_EQ(OB_SUCCESS, bandwidth_throttle_.init(INT64_MAX));
}

class ObMockFetchPartitionInfoP : public ObFetchPartitionInfoP {
public:
  ObMockFetchPartitionInfoP() : ObFetchPartitionInfoP(NULL)
  {}
  ~ObMockFetchPartitionInfoP()
  {}

protected:
  int process()
  {
    result_.meta_.pkey_ = arg_.pkey_;
    result_.major_version_ = arg_.pkey_.table_id_ % 10;
    return OB_SUCCESS;
  }
};

class ObMockFetchLogicRowP : public ObFetchLogicRowP {
public:
  ObMockFetchLogicRowP() : ObFetchLogicRowP(NULL, NULL)
  {}
  ~ObMockFetchLogicRowP()
  {}

  ObMockIterator iter_;

protected:
  int process()
  {
    int ret = OB_SUCCESS;
    ObStoreRow* store_row = NULL;
    iter_.reset_iter();

    char* buf = NULL;
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter_.get_next_row(store_row))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next row", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (NULL == store_row) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "store row must not be NULL", K(ret), KP(store_row));
      } else if (OB_FAIL(fill_data(*store_row))) {
        STORAGE_LOG(WARN, "failed to fill data", K(ret));
      } else {
        STORAGE_LOG(INFO, "succ to fill data", K(*store_row));
      }
    }
    iter_.reset_iter();
    return ret;
  }
};

TEST_F(TestMigrateRpc, FetchPartitionInfo)
{
  ObMockFetchPartitionInfoP* p = new ObMockFetchPartitionInfoP();
  service_.reg_processor(p);

  ObFetchPartitionInfoArg arg;
  arg.pkey_.table_id_ = 123456789;

  ObFetchPartitionInfoResult result;
  ASSERT_EQ(OB_SUCCESS, rpc_proxy_.to(self_addr_).fetch_partition_info(arg, result));
  ASSERT_EQ(result.meta_.pkey_, arg.pkey_);
  ASSERT_EQ(result.major_version_, 9);
}

TEST_F(TestMigrateRpc, FetchLogicRowP)
{
  ObMockFetchLogicRowP* p = new ObMockFetchLogicRowP();
  service_.reg_processor(p);

  const char* input = "int    varchar number                     \n"
                      "1      feiji   999999999999999999999999999\n"
                      "2      tank    1234567898765431           \n"
                      "3      lehaha  100000                     \n";
  ASSERT_EQ(OB_SUCCESS, p->iter_.from(input));

  ObLogicRowReader reader;
  ObFetchLogicRowArg arg_;
  arg_.key_range_.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, reader.init(rpc_proxy_, bandwidth_throttle_, self_addr_, arg_));

  ASSERT_TRUE(p->iter_.equals(reader));
}

}  // namespace obrpc
}  // namespace oceanbase

int main(int argc, char* argv[])
{
  system("rm -f test_migrate_rpc.log");
  OB_LOGGER.set_file_name("test_migrate_rpc.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
