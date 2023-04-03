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
#include <pthread.h>
#include "share/ob_rpc_struct.h"


namespace oceanbase
{
using namespace obrpc;
using namespace common;
namespace share
{

class TestRPCStruct: public ::testing::Test
{
public:
  TestRPCStruct() {}
  virtual ~TestRPCStruct() {}

};

#define PRINT_SIZE(T) \
do  \
{ \
  const int64_t size = sizeof(T); \
  const int64_t count = OB_MALLOC_BIG_BLOCK_SIZE / size; \
  STORAGE_LOG(INFO, "print size", K(#T), K(size), K(count)); \
} while(0); 

TEST(TestRPCStruct, print_size)
{
  PRINT_SIZE(ObMigrateReplicaArg);
  PRINT_SIZE(ObMigrateReplicaRes);
  PRINT_SIZE(ObChangeReplicaArg);
  PRINT_SIZE(ObChangeReplicaRes);
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
