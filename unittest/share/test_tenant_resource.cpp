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

#define USING_LOG_PREFIX SHARE

#include <unordered_map>
#include <gtest/gtest.h>

#define private public
#define protected public
#include "lib/thread/thread_mgr.h"
#include "share/ob_thread_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_mtl_helper.h"
#include "storage/tx/ob_trans_service.h"


namespace oceanbase
{
namespace share
{
using namespace lib;

#define TEST_TENANT_ID 1
#define TEST_TENANT_ID2 1001
#define TEST_TENANT_ID3 1002

class MockObTenant
{
public:
  MockObTenant(uint64_t id) : tenant_base_(id)
  {}
  ~MockObTenant() {
    stop();
  }
  int init()
  {
    ObTenantSwitchGuard guard(&tenant_base_);
    int ret = OB_SUCCESS;
    if (OB_FAIL(tenant_base_.init())) {
    } else if (OB_FAIL(tenant_base_.create_mtl_module())) {
    } else if (OB_FAIL(tenant_base_.init_mtl_module())) {
    } else if (OB_FAIL(tenant_base_.start_mtl_module())) {
    } else {
    }
    return ret;
  }
  ObTenantBase *get_res()
  {
    return &tenant_base_;
  }
  void stop()
  {
    tenant_base_.stop_mtl_module();
    tenant_base_.wait_mtl_module();
    tenant_base_.destroy();
  }
public:
  ObTenantBase tenant_base_;
};

class MyRunnable : public TGRunnable
{
public:
  void run1() override
  {
    run_count_++;
    uint64_t tenant_id = MTL_ID();
    LOG_INFO("run", K(run_count_), K(tenant_id));
    while (!has_set_stop() && !(OB_NOT_NULL(&Thread::current()) ? Thread::current().has_set_stop() : false)) {
      ::usleep(50000);
    }
  }
  int64_t run_count_=0;
};

class MyHandler : public TGTaskHandler
{
public:
 void handle(void* t) {}
};

class MyThreadPool : public Threads
{
public:
  void run(int64_t idx) override {
    while (!has_set_stop() && ! (OB_NOT_NULL(&Thread::current()) ? Thread::current().has_set_stop() : false)) {
      ::usleep(50000);
    }
  }
};

class ObTestModule
{
public:
  ObTestModule() {}
  virtual ~ObTestModule() {}

  static int mtl_init(ObTestModule *&mod)
  {
    return mod->init();
  }
  int init()
  {
    LOG_INFO("init", KP(this));
    return TG_CREATE_TENANT(TGDefIDs::COMMON_THREAD_POOL, tg_id_);
  }
  int start()
  {
    LOG_INFO("start", KP(this));
    // set runnable
    int ret = TG_SET_RUNNABLE_AND_START(tg_id_, runnable_);
    return ret;
  }
  void stop()
  {
    LOG_INFO("stop", KP(this));
    TG_STOP(tg_id_);
  }
  void wait()
  {
    LOG_INFO("wait", KP(this));
    TG_WAIT(tg_id_);
  }
  void destroy()
  {
    LOG_INFO("destroy", KP(this));
    TG_DESTROY(tg_id_);
  }
private:
  int tg_id_;
  MyRunnable runnable_;
};

class TestTenantResource : public ::testing::Test
{
public:
  virtual void SetUp() {
     MTL_BIND2(mtl_new_default, ObTestModule::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
  }
  virtual void TearDown()
  {
    tenants_.clear();
    ObTenantEnv::set_tenant(nullptr);
  }
public:
  static std::unordered_map<uint64_t, MockObTenant*> tenants_;
};

std::unordered_map<uint64_t, MockObTenant*> TestTenantResource::tenants_;

int get_tenant_base_with_lock(uint64_t tenant_id, common::ObLDHandle &handle, ObTenantBase *&tenant, ReleaseCbFunc &release_cb)
{
  UNUSED(handle);
  UNUSED(release_cb);
  int ret = OB_SUCCESS;
  auto it = TestTenantResource::tenants_.find(tenant_id);
  if (it != TestTenantResource::tenants_.end()) {
    tenant = it->second->get_res();
  } else {
    ret = OB_TENANT_NOT_EXIST;
  }
  return ret;
}



TEST_F(TestTenantResource, basic)
{
  // create tenant
  MockObTenant tenant(TEST_TENANT_ID);
  // set thread_local
  ObTenantEnv::set_tenant(tenant.get_res());
  // check tenant id
  ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
}

TEST_F(TestTenantResource, start_thread)
{
  MockObTenant tenant(TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, tenant.init());
  ObTenantEnv::set_tenant(tenant.get_res());

  int tg_id;
  // create tenant isolation thread pool
  ASSERT_EQ(OB_SUCCESS, TG_CREATE_TENANT(TGDefIDs::COMMON_THREAD_POOL, tg_id));
  MyRunnable runnable;
  // set runnable
  ASSERT_EQ(OB_SUCCESS, TG_SET_RUNNABLE_AND_START(tg_id, runnable));

  // check exist
  ASSERT_EQ(OB_HASH_EXIST, tenant.get_res()->get_tg_set().exist_refactored(tg_id));

  int thread_cnt = TG_GET_THREAD_CNT(tg_id);
  thread_cnt++;
  // set thread_cnt
  ASSERT_EQ(OB_SUCCESS, TG_SET_THREAD_CNT(tg_id, thread_cnt));

  ASSERT_EQ(thread_cnt, TG_GET_THREAD_CNT(tg_id));

  TG_STOP(tg_id);
  TG_WAIT(tg_id);
  // destroy thread
  TG_DESTROY(tg_id);
}

TEST_F(TestTenantResource, switch_tenant_guard)
{
  int ret = OB_SUCCESS;
  MockObTenant tenant(TEST_TENANT_ID);
  tenants_.emplace(TEST_TENANT_ID, &tenant);
  MockObTenant tenant2(TEST_TENANT_ID2);
  tenants_.emplace(TEST_TENANT_ID2, &tenant2);
  MockObTenant tenant3(TEST_TENANT_ID3);
  tenants_.emplace(TEST_TENANT_ID3, &tenant3);

  {
    ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(TEST_TENANT_ID));
    ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
    guard.release();
    ASSERT_EQ(OB_INVALID_TENANT_ID, MTL_ID());
  }

  {
    ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(TEST_TENANT_ID));
    ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
  }
  ASSERT_EQ(OB_INVALID_TENANT_ID, MTL_ID());

  {
    ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(TEST_TENANT_ID));
    ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
    {
      ObTenantSwitchGuard guard;
      ASSERT_EQ(OB_SUCCESS, guard.switch_to(TEST_TENANT_ID2));
      ASSERT_EQ(TEST_TENANT_ID2, MTL_ID());
    }
    ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
    guard.release();
    ASSERT_EQ(OB_INVALID_TENANT_ID, MTL_ID());
  }
}

TEST_F(TestTenantResource, mtl_switch)
{
  ObTenantEnv::set_tenant(nullptr);
  int ret = OB_SUCCESS;
  MockObTenant tenant(TEST_TENANT_ID);
  tenants_.emplace(TEST_TENANT_ID, &tenant);
  MockObTenant tenant2(TEST_TENANT_ID2);
  tenants_.emplace(TEST_TENANT_ID2, &tenant2);

  MTL_SWITCH(TEST_TENANT_ID) {
    ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
  }
  ASSERT_EQ(0, MTL_ID());

  MTL_SWITCH(TEST_TENANT_ID) {
    ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
    MTL_SWITCH(TEST_TENANT_ID2) {
      ASSERT_EQ(TEST_TENANT_ID2, MTL_ID());
    }
    ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
  }
  ASSERT_EQ(0, MTL_ID());
}

TEST_F(TestTenantResource, tenant_base_set)
{
  transaction::ObTransService* trans_service = (transaction::ObTransService*)ob_malloc(sizeof(transaction::ObTransService),
                                                                                      ObNewModIds::TEST);
  ObTenantBase tenant_base(1);
  tenant_base.set(trans_service);
  ObTenantEnv::set_tenant(&tenant_base);
  ASSERT_EQ(trans_service, MTL(transaction::ObTransService*));
  ob_free(trans_service);
  ObTenantEnv::set_tenant(nullptr);
  ASSERT_EQ(nullptr, MTL(transaction::ObTransService*));
}

TEST_F(TestTenantResource, tenant_thread_dynamic)
{
  ObTenantBase tenant_base(1);
  ASSERT_EQ(OB_SUCCESS, tenant_base.init());
  ObTenantEnv::set_tenant(&tenant_base);
  // common thread pool
  int tg_id;
  ASSERT_EQ(OB_SUCCESS, TG_CREATE_TENANT(TGDefIDs::COMMON_THREAD_POOL, tg_id));
  MyRunnable runnable;
  ASSERT_EQ(OB_SUCCESS, TG_SET_RUNNABLE_AND_START(tg_id, runnable));
  ASSERT_EQ(1, TG_GET_THREAD_CNT(tg_id));
  ASSERT_EQ(OB_SUCCESS, MTL_REGISTER_THREAD_DYNAMIC(1.5, tg_id));

  // queue thread
  int tg_id2;
  ASSERT_EQ(OB_SUCCESS, TG_CREATE_TENANT(TGDefIDs::COMMON_QUEUE_THREAD, tg_id2));
  MyHandler handler;
  ASSERT_EQ(OB_SUCCESS, TG_SET_HANDLER_AND_START(tg_id2, handler));
  ASSERT_EQ(1, TG_GET_THREAD_CNT(tg_id2));
  ASSERT_EQ(OB_SUCCESS, MTL_REGISTER_THREAD_DYNAMIC(0.5, tg_id2));


  // user thread
  MyThreadPool my_thread;
  ASSERT_EQ(OB_SUCCESS, my_thread.init());
  ASSERT_EQ(OB_SUCCESS, MTL_REGISTER_THREAD_DYNAMIC(2.5, &my_thread));
  ASSERT_EQ(OB_SUCCESS, my_thread.start());
  ASSERT_EQ(1, my_thread.get_thread_count());

  // tenant config change
  ASSERT_EQ(OB_SUCCESS, tenant_base.update_thread_cnt(4));

  ASSERT_EQ(6, TG_GET_THREAD_CNT(tg_id));
  ASSERT_EQ(2, TG_GET_THREAD_CNT(tg_id2));
  ASSERT_EQ(10, my_thread.get_thread_count());

  // tenant config change
  ASSERT_EQ(OB_SUCCESS, tenant_base.update_thread_cnt(2));
  ASSERT_EQ(3, TG_GET_THREAD_CNT(tg_id));
  ASSERT_EQ(1, TG_GET_THREAD_CNT(tg_id2));
  ASSERT_EQ(5, my_thread.get_thread_count());
  TG_STOP(tg_id);
  TG_WAIT(tg_id);
  TG_DESTROY(tg_id);
  TG_STOP(tg_id2);
  TG_WAIT(tg_id2);
  TG_DESTROY(tg_id2);
  my_thread.stop();
  my_thread.wait();
  my_thread.destroy();
}




} // end share
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



