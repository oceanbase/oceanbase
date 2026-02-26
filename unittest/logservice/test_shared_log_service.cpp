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
#include "logservice/ob_log_service.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;
using namespace logservice;
namespace unittest
{

TEST(TestLogSharedStorageService, test_is_valid)
{
  ObSharedLogService shared_log;
  logservice::ObLogService log_service;
  const uint64_t tenant_id = 1002;
  ObSharedLogUploadHandler *handler = NULL;

  ASSERT_EQ(OB_NOT_INIT, shared_log.add_ls(ObLSID(1)));
  ASSERT_EQ(OB_NOT_INIT, shared_log.remove_ls(ObLSID(1)));
  ASSERT_EQ(OB_NOT_INIT, shared_log.get_log_ss_handler(ObLSID(1), handler));

  const ObAddr addr(ObAddr::IPV4, "127.0.0.1", 1000);
  common::ObMySQLProxy sql_proxy;
  obrpc::ObLogServiceRpcProxy rpc_proxy;
  ObLocationAdapter location_adapter;
  palf::LogSharedQueueTh log_shared_queue_th;
  ObTenantMutilAllocator allocator(tenant_id);
  ASSERT_EQ(OB_SUCCESS, shared_log.init(tenant_id, addr, &log_service,
      &sql_proxy, &rpc_proxy, &location_adapter, &allocator, &log_shared_queue_th));

  ASSERT_EQ(OB_INVALID_ARGUMENT, shared_log.add_ls(ObLSID()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, shared_log.remove_ls(ObLSID()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, shared_log.get_log_ss_handler(ObLSID(), handler));

}

} //end of namespace logservice
}//end of namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_ss_service.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_ss_service");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
