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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/detect/ob_detect_manager.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/thread/thread_mgr.h"
#include "lib/alloc/memory_dump.h"
#include "share/detect/ob_detect_manager_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace std;

static const uint64_t ACTIVATE_DELAY_TIME_SEC = 5;

static uint64_t s_tenant_id = 500;
ObDetectManager *dm = new ObDetectManager(s_tenant_id);
ObDetectManagerThread &dm_thr = ObDetectManagerThread::instance();

// only detect local, mock an invalid transport;
rpc::frame::ObReqTransport mock_transport(nullptr, nullptr);

static int init_dm()
{
  int ret = OB_SUCCESS;
  GCTX.server_id_ = 10086;
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  if (OB_FAIL(dm->init(self, 1))) {
    LOG_WARN("failed to init dm");
  } else if (OB_FAIL(dm_thr.init(self, &mock_transport))) {
    LOG_WARN("failed to init dm thr");
  }
  return ret;
}

// check memory具有延迟性，从测试的结果来看，大致阈值是5s
// 调用 check_memory_leak 前需要前置sleep
int check_memory_leak()
{
  sleep(15);
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_SERVER_TENANT_ID;
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
          if (strcmp(label.str_, "ReqMapTempContext") == 0 || strcmp(label.str_, "DetectManager") == 0) {
            LOG_WARN("dm leak memory:", K(tenant_id), K(ctx_id), K(label.str_), K(l_item->hold_), K(l_item->used_), K(l_item->count_));
          }
          return OB_SUCCESS;
        });
    }
  }
  return ret;
}

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

class ObDetectManagerTest : public ::testing::Test
{
};

TEST_F(ObDetectManagerTest, init_dm)
{
  ASSERT_EQ(OB_SUCCESS, init_dm());
}

TEST_F(ObDetectManagerTest, test_register_detectable_id)
{
  // register
  ObDetectableId id1;
  ObDetectManagerUtils::generate_detectable_id(id1, s_tenant_id);
  ObDetectableId id2;
  ObDetectManagerUtils::generate_detectable_id(id2, s_tenant_id);
  ObDetectableId id3;
  ObDetectManagerUtils::generate_detectable_id(id3, s_tenant_id);
  ObDetectableId id4;
  ASSERT_EQ(OB_SUCCESS, dm->register_detectable_id(id1));
  ASSERT_EQ(OB_HASH_EXIST, dm->register_detectable_id(id1));
  ASSERT_EQ(OB_SUCCESS, dm->register_detectable_id(id2));
  ASSERT_EQ(OB_SUCCESS, dm->register_detectable_id(id3));
  ASSERT_EQ(OB_INVALID_ARGUMENT, dm->register_detectable_id(id4));
  ASSERT_EQ(3, dm->detectable_ids_.size());

  // seek task alive
  ASSERT_TRUE(dm->is_task_alive(id1));
  ASSERT_TRUE(dm->is_task_alive(id2));
  ASSERT_TRUE(dm->is_task_alive(id3));

  // unregister
  ASSERT_EQ(OB_SUCCESS, dm->unregister_detectable_id(id1));
  ASSERT_EQ(OB_HASH_NOT_EXIST, dm->unregister_detectable_id(id1));
  ASSERT_EQ(OB_SUCCESS, dm->unregister_detectable_id(id2));
  ASSERT_EQ(OB_SUCCESS, dm->unregister_detectable_id(id3));
  ASSERT_EQ(OB_INVALID_ARGUMENT, dm->unregister_detectable_id(id4));
  ASSERT_EQ(0, dm->detectable_ids_.size());

  ASSERT_FALSE(dm->is_task_alive(id1));
  ASSERT_FALSE(dm->is_task_alive(id2));
  ASSERT_FALSE(dm->is_task_alive(id3));
  ASSERT_FALSE(dm->is_task_alive(id4));
}

TEST_F(ObDetectManagerTest, test_register_check_item)
{
  ObArray<ObPeerTaskState> peer_states;
  // one id with one callback
  ObDetectableId id1;
  ObDetectManagerUtils::generate_detectable_id(id1, s_tenant_id);
  ObMockDetectCB *cb1 = nullptr;
  uint64_t node_sequence_id1 = 0;
  auto rsc1 = allocate_mock_resource();
  ASSERT_EQ(OB_SUCCESS, dm->register_check_item(id1, cb1, node_sequence_id1, true/* need_ref */, peer_states, rsc1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, dm->register_check_item(id1, cb1, node_sequence_id1, true/* need_ref */, peer_states, rsc1));

  // one id with multiple callbacks
  ObDetectableId id2;
  ObDetectManagerUtils::generate_detectable_id(id2, s_tenant_id);
  auto rsc2_1 = allocate_mock_resource();
  auto rsc2_2 = allocate_mock_resource();
  ObMockDetectCB *cb2_1 = nullptr;
  ObMockDetectCB *cb2_2 = nullptr;
  uint64_t node_sequence_id2_1 = 0;
  uint64_t node_sequence_id2_2 = 0;
  ASSERT_EQ(OB_SUCCESS, dm->register_check_item(id2, cb2_1, node_sequence_id2_1, true/* need_ref */, peer_states, rsc2_1));
  ASSERT_EQ(OB_SUCCESS, dm->register_check_item(id2, cb2_2, node_sequence_id2_2, true/* need_ref */, peer_states, rsc2_2));
  ASSERT_EQ(2, dm->all_check_items_.size());

  // unregister
  cb1->dec_ref_count();
  cb2_1->dec_ref_count();
  cb2_2->dec_ref_count();
  ASSERT_EQ(OB_SUCCESS, dm->unregister_check_item(id1, node_sequence_id1));
  ASSERT_EQ(OB_HASH_NOT_EXIST, dm->unregister_check_item(id1, node_sequence_id1));
  ASSERT_EQ(OB_SUCCESS, dm->unregister_check_item(id2, node_sequence_id2_2));
  ASSERT_EQ(OB_SUCCESS, dm->unregister_check_item(id2, node_sequence_id2_1));
  ASSERT_EQ(0, dm->all_check_items_.size());
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  ObMemoryDump::get_instance().init();
  system("rm -f test_ob_detect_manager.log*");
  OB_LOGGER.set_file_name("test_ob_detect_manager.log", true, true);
  OB_LOGGER.set_log_level("TRACE", "TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  TG_STOP(lib::TGDefIDs::DetectManager);
  return ret;
}
