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

#define USING_LOG_PREFIX SERVER

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/detect/ob_detect_manager.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/thread/thread_mgr.h"
#include "lib/alloc/memory_dump.h"

// simple server
#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace std;

namespace oceanbase
{
namespace unittest
{

static const uint64_t ACTIVATE_DELAY_TIME_SEC = 5;
static uint64_t s_tenant_id = OB_SYS_TENANT_ID;
ObDetectManager *dm = nullptr;
ObDetectManagerThread &dm_thr = ObDetectManagerThread::instance();

class ObMockResource
{
public:
  static int free_cnt;
public:
  ObMockResource() {}
  void destroy()
  {
    int ret = OB_SUCCESS;
    ATOMIC_FAA(&ObMockResource::free_cnt, 1);
    LOG_WARN("ObMockResource destoryed");
  }
};

int ObMockResource::free_cnt = 0;

class ObMockDetectCB : public ObIDetectCallback
{
public:
  ObMockDetectCB(uint64_t tenant_id, const ObArray<ObPeerTaskState> &peer_states, ObMockResource *resource)
    : ObIDetectCallback(tenant_id, peer_states), resouce_(resource) {}
  int do_callback() override
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(resouce_)) {
      resouce_->destroy();
      ob_free(resouce_);
      resouce_ = nullptr;
    }
    return ret;
  }
  int64_t get_detect_callback_type() const override { return (int64_t)DetectCallBackType::VIRTUAL; }
private:
  ObMockResource* resouce_;
};

ObMockResource *allocate_mock_resource()
{
  void *buf = nullptr;
  buf = ob_malloc(sizeof(ObMockResource), "mock resource");
  ObMockResource *rsc = new(buf) ObMockResource;
  return rsc;
}

static int init_dm()
{
  int ret = OB_SUCCESS;
  // hack an local addr to dm
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  MTL_SWITCH(s_tenant_id) {
    dm = MTL(ObDetectManager*);
    dm->self_ = self;
    dm_thr.self_ = self;
  }
  return ret;
}

// check memory具有延迟性，从测试的结果来看，大致阈值是5s
// 调用 check_memory_leak 前需要前置sleep
int check_memory_leak()
{
  sleep(15);
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  for (int ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
    auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, ctx_id);
    if (nullptr == ta) {
      ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(tenant_id, ctx_id);
    }
    if (nullptr == ta) {
      continue;
    }
    if (OB_SUCC(ret)) {
      ret = ta->iter_label([&](lib::ObLabel &label, LabelItem *l_item)
        {
          if (strcmp(label.str_, "DetectManager") == 0) {
            LOG_WARN("dm leak memory:", K(tenant_id), K(ctx_id), K(label.str_), K(l_item->hold_), K(l_item->used_), K(l_item->count_));
          }
          return OB_SUCCESS;
        });
    }
  }
  return ret;
}

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class ObSimpleClusterExampleTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSimpleClusterExampleTest() : ObSimpleClusterTestBase("test_ob_simple_cluster_") {}
};

TEST_F(ObSimpleClusterExampleTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObSimpleClusterExampleTest, init_dm)
{
  ASSERT_EQ(OB_SUCCESS, init_dm());
}

TEST_F(ObSimpleClusterExampleTest, test_detect_local)
{
  // mock an local addr
  ObAddr local_addr;
  local_addr.set_ip_addr("127.0.0.1", 8086);
  ObPeerTaskState peer_state(local_addr);
  ObArray<ObPeerTaskState> peer_states;
  peer_states.push_back(peer_state);

  // cb1 managerd by dm
  ObDetectableId id1;
  ObDetectManagerUtils::generate_detectable_id(id1, s_tenant_id);
  ObMockDetectCB *cb1 = nullptr;
  ObMockResource *rsc1 = allocate_mock_resource();
  uint64_t node_sequence_id1 = 0;

  // cb2 managerd by dm
  ObDetectableId id2;
  ObDetectManagerUtils::generate_detectable_id(id2, s_tenant_id);
  ObMockDetectCB *cb2 = nullptr;
  ObMockResource *rsc2 = allocate_mock_resource();
  uint64_t node_sequence_id2 = 0;

  // register id
  ASSERT_EQ(OB_SUCCESS, dm->register_detectable_id(id1));

  // register cb
  ASSERT_EQ(OB_SUCCESS, dm->register_check_item(id1, cb1, node_sequence_id1, true/* need_ref */, peer_states, rsc1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, dm->register_check_item(id1, cb1, node_sequence_id1, true/* need_ref */, peer_states, rsc1));
  ASSERT_EQ(OB_SUCCESS, dm->register_check_item(id2, cb2, node_sequence_id2, false/* need_ref */, peer_states, rsc2));

  // unregister id1 after 2s
  sleep(ACTIVATE_DELAY_TIME_SEC);
  ASSERT_EQ(OB_SUCCESS, dm->unregister_detectable_id(id1));

  // wait detect loop done
  sleep(15);
  // cb1 and cb2 are executed;
  ASSERT_EQ(2, ATOMIC_LOAD(&ObMockResource::free_cnt));
  ATOMIC_SAF(&ObMockResource::free_cnt, 2);

  // unregister cb1
  cb1->dec_ref_count();
  ASSERT_EQ(OB_SUCCESS, dm->unregister_check_item(id1, node_sequence_id1));
  check_memory_leak();
}

TEST_F(ObSimpleClusterExampleTest, test_detect_remote)
{
  // get rpc addr
  ObAddr remote_addr = get_curr_simple_server().get_addr();
  ObPeerTaskState peer_state(remote_addr);
  ObArray<ObPeerTaskState> peer_states;
  peer_states.push_back(peer_state);

  ObDetectableId id1;
  ObDetectManagerUtils::generate_detectable_id(id1, s_tenant_id);
  // cb1 managerd by user
  ObMockDetectCB *cb1 = nullptr;
  ObMockResource *rsc1 = allocate_mock_resource();
  uint64_t node_sequence_id1 = 0;

  // cb2 managerd by dm
  ObDetectableId id2;
  ObDetectManagerUtils::generate_detectable_id(id2, s_tenant_id);
  ObMockDetectCB *cb2 = nullptr;
  ObMockResource *rsc2 = allocate_mock_resource();
  uint64_t node_sequence_id2 = 0;

  // register
  ASSERT_EQ(OB_SUCCESS, dm->register_detectable_id(id1));
  ASSERT_EQ(OB_SUCCESS, dm->register_check_item(id1, cb1, node_sequence_id1, true/* need_ref */, peer_states, rsc1));
  ASSERT_EQ(OB_SUCCESS, dm->register_check_item(id2, cb2, node_sequence_id2, false/* need_ref */, peer_states, rsc2));

  // unregister id1 after 2s
  sleep(ACTIVATE_DELAY_TIME_SEC);
  ASSERT_EQ(OB_SUCCESS, dm->unregister_detectable_id(id1));

  // wait detect loop done
  sleep(15);
  ASSERT_EQ(2, ATOMIC_LOAD(&ObMockResource::free_cnt));

  // unregister cb1.
  cb1->dec_ref_count();
  ASSERT_EQ(OB_SUCCESS, dm->unregister_check_item(id1, node_sequence_id1));
  check_memory_leak();
}

TEST_F(ObSimpleClusterExampleTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char*)"INFO";
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);

  int ret = OB_SUCCESS;
  system("rm -f test_ob_detect_manager_in_simple_server.log*");
  OB_LOGGER.set_file_name("test_ob_detect_manager_in_simple_server.log", true, true);
  OB_LOGGER.set_log_level("INFO");

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
