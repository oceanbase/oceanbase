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
#include <iostream>
#include "lib/time/ob_time_utility.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/queue/ob_link.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/queue/ob_lighty_queue.h"
#include "logservice/applyservice/ob_log_apply_service.h"
#include "logservice/palf/palf_env_impl.h"
#include "logservice/palf/palf_handle_impl_guard.h"
#include "mock_log_ctx.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"

using namespace std;
using namespace oceanbase::common;
namespace oceanbase
{
using namespace share;
using namespace common;
using namespace palf;
using namespace logservice;
namespace unittest
{
const int TEST_CB_COUNT = 5000 * 1000;
const int TRANS_COUNT = 100;
MockLogCtx *log_ctx_array[TEST_CB_COUNT];
int64_t  stream_id(1);
ObLogApplyService apply_service;
PalfEnvImpl palf_env_impl;
char base_dir;
ObAddr mock_server(1,1);
rpc::frame::ObReqTransport mock_tp(NULL, NULL);
ObTenantMutilAllocator mock_alloc_mgr(500);

TEST(TestApplyEngine, test_ob_apply_service)
{
  PalfHandleImplGuard guard;
  ObLightyQueue lighty_queue;
  ObLinkQueue link_queue;
  //palf_env_impl.init(&base_dir, mock_server, &mock_tp, &mock_alloc_mgr);
  //palf_env_impl.create_palf_handle_impl(stream_id, guard);
  lighty_queue.init(TEST_CB_COUNT);
  apply_service.init();
  apply_service.start();
  int64_t begin_ts = ObTimeUtility::current_time();
  // new logctx
  for (int i = 0; i < TEST_CB_COUNT; i++)
  {
    log_ctx_array[i] = new MockLogCtx();
    log_ctx_array[i]->__palf_set_id(1);
  }
  int64_t create_finished_ts = ObTimeUtility::current_time();
  REPLAY_LOG(WARN, "create cost", K(TEST_CB_COUNT), K(TRANS_COUNT),
             K(create_finished_ts - begin_ts));
  // link queue push
  for (int i = 0; i < TEST_CB_COUNT; i++)
  {
    link_queue.push(log_ctx_array[i]);
  }
  int64_t link_queue_push_finished_ts = ObTimeUtility::current_time();
  REPLAY_LOG(WARN, "link queue push cost", K(TEST_CB_COUNT), K(TRANS_COUNT),
             K(link_queue_push_finished_ts - create_finished_ts));
  // lighty queue push
  for (int i = 0; i < TEST_CB_COUNT; i++)
  {
    lighty_queue.push(log_ctx_array[i]);
  }
  int64_t lighty_queue_push_finished_ts = ObTimeUtility::current_time();
  REPLAY_LOG(WARN, "lighty queue push cost", K(TEST_CB_COUNT), K(TRANS_COUNT),
             K(lighty_queue_push_finished_ts - link_queue_push_finished_ts));
  // ap_eg push
  for (int i = 0; i < TEST_CB_COUNT; i++)
  {
    apply_service.push_append_cb(1, log_ctx_array[i], LSN(i * 10));
  }
  int64_t ag_eg_push_finished_ts = ObTimeUtility::current_time();
  REPLAY_LOG(WARN, "ap_eg queue push cost", K(TEST_CB_COUNT), K(TRANS_COUNT),
             K(ag_eg_push_finished_ts - lighty_queue_push_finished_ts));
  sleep(10);

}
} // end of unittest
} // end of oceanbase
int main(int argc, char **argv)
{
  UNUSED(argc);
  UNUSED(argv);
  OB_LOGGER.set_file_name("test_ob_apply_service.log", true);
  OB_LOGGER.set_log_level("WARN");
  PALF_LOG(INFO, "begin unittest::test_ob_apply_service");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
