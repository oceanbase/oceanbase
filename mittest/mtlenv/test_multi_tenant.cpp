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

#define USING_LOG_PREFIX STORAGETEST

#include <string>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define private public
#include "lib/alloc/ob_tenant_ctx_allocator.h"
#undef private

#include "lib/random/ob_random.h"
#include "lib/oblog/ob_log.h"
#include "share/rc/ob_context.h"
#include "lib/allocator/ob_malloc.h"
#include "share/io/ob_io_manager.h"

#include "observer/omt/ob_tenant_mtl_helper.h"
#include "mtlenv/mock_tenant_module_env.h"

using namespace std;
//using namespace share;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::omt;
using namespace oceanbase::observer;
using namespace oceanbase::storage;
using namespace oceanbase::share;

#define OMT_UNITTEST

class MockObPxPools : public ObPxPools
{
public:
  MockObPxPools() : ObPxPools()
  {
    this_seq_= MockObPxPools::seq_++;
  }
  ~MockObPxPools()
  {
    MockObPxPools::seq_--;
  }
  static int mock_mtl_init(ObPxPools *&pool)
  {
    int ret = OB_SUCCESS;
    pool = OB_NEW(MockObPxPools, common::ObModIds::OMT_TENANT);
    if (OB_ISNULL(pool)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }
  static void mock_mtl_destroy(ObPxPools *&pool)
  {
    MockObPxPools *ptr = static_cast<MockObPxPools*>(pool);
    OB_DELETE(MockObPxPools, common::ObModIds::OMT_TENANT, ptr);
    pool = nullptr;
  }
  int64_t get_seq() { return this_seq_; }
private:
  static int64_t seq_;
  int64_t this_seq_;
};
int64_t MockObPxPools::seq_ = 0;

class TestMultiTenant : public ::testing::Test
{
public:
  TestMultiTenant()
  {
  }

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    //EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().remove_sys_tenant());
    // partition_service not init
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp();
  virtual void TearDown();

  static int fake_init_compat_mode(lib::Worker::CompatMode &compat_mode)
  {
    compat_mode = lib::Worker::CompatMode::ORACLE;
    return OB_SUCCESS;
  }

protected:
  int add_tenant(uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    if (is_virtual_tenant_id(tenant_id)) {
      if (OB_FAIL(GCTX.omt_->create_tenant_without_unit(tenant_id, 1, 1))) {
        LOG_ERROR("fail to create virtual tenant", K(ret), K(tenant_id));
      }
    } else {
      if (OB_FAIL(create_normal_tenant(tenant_id))) {
        LOG_ERROR("fail to create normal tenant", K(ret), K(tenant_id));
      }
    }
    return ret;
  }

  int create_normal_tenant(const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;

    ObTenantMeta tenant_meta;
    if (OB_FAIL(MockTenantModuleEnv::construct_default_tenant_meta(tenant_id, tenant_meta))) {
      LOG_ERROR("fail to create tenant", K(ret));
    } else if (OB_FAIL(GCTX.omt_->create_tenant(tenant_meta, false))) {
      LOG_ERROR("fail to create tenant", K(ret));
    }

    return ret;
  }

};

void TestMultiTenant::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  MTL_BIND(fake_init_compat_mode, nullptr);
}

void TestMultiTenant::TearDown()
{
  // remove all tenants and ensure that resources are released
  TenantIdList ids;
  GCTX.omt_->get_tenant_ids(ids);
  bool lock_succ = false;
  for (int64_t index = 0; index < ids.size(); index++) {
    while (OB_EAGAIN == GCTX.omt_->remove_tenant(ids[index], lock_succ));
  }
}

TEST_F(TestMultiTenant, convert_between_hidden_and_sys_tenant)
{
  int ret = OB_SUCCESS;
  //release tenant rlock in MockTenantModuleEnv first, otherwise try wlock will fail
  MockTenantModuleEnv::get_instance().release_guard();
  ret = GCTX.omt_->convert_real_to_hidden_sys_tenant();
  ASSERT_EQ(ret, OB_SUCCESS);

  ObTenantMeta meta;
  ret = MockTenantModuleEnv::construct_default_tenant_meta(OB_SYS_TENANT_ID, meta);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = GCTX.omt_->convert_hidden_to_real_sys_tenant(meta.unit_);
  ASSERT_EQ(ret, OB_SUCCESS);
}


TEST_F(TestMultiTenant, create_and_remove_tenant)
{
  uint64_t tenants[] = {100, 24, 45, 94, 23, 9, 17, 999, 1009};
  int cnt = sizeof(tenants)/sizeof(uint64_t);
  int ret = OB_SUCCESS;

  for (int i = 0; i < cnt; i++) {
    ret = add_tenant(tenants[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  TenantList &list = GCTX.omt_->get_tenant_list();
  ASSERT_EQ(cnt, list.size());

  uint64_t last = 0;
  for (auto it = list.begin(); it != list.end(); it++) {
    ASSERT_GT((*it)->id(), last);
    last = (*it)->id();
  }
  ret = add_tenant(tenants[0]);
  ASSERT_EQ(OB_TENANT_EXIST, ret);
  bool lock_succ = false;
  while (OB_EAGAIN == GCTX.omt_->remove_tenant(tenants[0], lock_succ));
  //ASSERT_EQ(OB_SUCCESS, ret); // partition_service should init
  ret = add_tenant(tenants[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
}

//int get_tenant_with_tenant_lock(const uint64_t tenant_id, ObTenant *&tenant) const;
TEST_F(TestMultiTenant, tenant_lock)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 100;
  add_tenant(tenant_id);
  ObLDHandle handle;
  // new
  ObTenant *tenant = nullptr;
  ret = GCTX.omt_->get_tenant_with_tenant_lock(tenant_id, handle, tenant);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(tenant, nullptr);
  ASSERT_TRUE(tenant->lock_.is_locked());
  tenant->unlock(handle);
  ASSERT_FALSE(tenant->lock_.is_locked());

  // old
  tenant = nullptr;
  ret = GCTX.omt_->get_tenant(tenant_id, tenant);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(tenant, nullptr);
  ASSERT_FALSE(tenant->lock_.is_locked());
}

TEST_F(TestMultiTenant, get_tenant_context)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 100;
  add_tenant(tenant_id);
  share::ObTenantSpace *ctx = nullptr;
  ObLDHandle handle;
  ret = share::get_tenant_ctx_with_tenant_lock(tenant_id, handle, ctx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->get_tenant(), nullptr);
  ASSERT_TRUE(static_cast<ObTenant *>(ctx->get_tenant())->lock_.is_locked());
  static_cast<ObTenant *>(ctx->get_tenant())->unlock(handle);
}

class CtxMemConfigGetter : public ObICtxMemConfigGetter
{
public:
  virtual int get(int64_t tenant_id, int64_t tenant_limit, common::ObIArray<ObCtxMemConfig> &configs)
  {
    int ret = OB_SUCCESS;
    {
      ObCtxMemConfig cfg;
      cfg.ctx_id_ = 10;
      cfg.idle_size_ = 10 * INTACT_ACHUNK_SIZE;
      cfg.limit_ = 5 * tenant_limit / 100;
      ret = configs.push_back(cfg);
    }
    {
      ObCtxMemConfig cfg;
      cfg.ctx_id_ = 11;
      cfg.idle_size_ = 20 * INTACT_ACHUNK_SIZE;
      cfg.limit_ = 10 * tenant_limit / 100;
      ret = configs.push_back(cfg);
    }
    return ret;
  }
};

static CtxMemConfigGetter mcg;

TEST_F(TestMultiTenant, idle)
{
  omt::ObMultiTenant::mcg_ = &mcg;
  uint64_t tenant_id = 100;
  int ret = add_tenant(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  int64_t tenant_limit = malloc_allocator->get_tenant_limit(tenant_id);
  common::ObArray<ObCtxMemConfig> configs;
  mcg.get(tenant_id, tenant_limit, configs);
  ASSERT_TRUE(2 == configs.size());
  for (int i = 0 ; i < configs.size(); i++) {
    ObCtxMemConfig &cfg = configs.at(i);
    auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, cfg.ctx_id_);
    ASSERT_TRUE(NULL != ta);
    int64_t chunk_cnt = cfg.idle_size_/INTACT_ACHUNK_SIZE;
    ASSERT_EQ(ta->chunk_cnt_, chunk_cnt);
    ASSERT_EQ(ta->idle_size_, cfg.idle_size_);
    ASSERT_EQ(ta->get_limit(), cfg.limit_);
    ASSERT_NE(0, malloc_allocator->get_tenant_ctx_hold(tenant_id, cfg.ctx_id_));
  }
  // remove
  bool lock_succ = false;
  GCTX.omt_->remove_tenant(tenant_id, lock_succ);
  for (int i = 0 ; i < configs.size(); i++) {
    ObCtxMemConfig &cfg = configs.at(i);
    auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, cfg.ctx_id_);
    ASSERT_TRUE(NULL != ta);
    ta->set_idle(0);
    ASSERT_EQ(0, malloc_allocator->get_tenant_ctx_hold(tenant_id, cfg.ctx_id_));
  }
}

TEST_F(TestMultiTenant, tenant_local)
{
  MTL_BIND(MockObPxPools::mock_mtl_init, MockObPxPools::mock_mtl_destroy);
  uint64_t tenant_id[] = {1, 1001};
  int64_t &seq = MockObPxPools::seq_;
  ASSERT_EQ(seq, 0);
  add_tenant(tenant_id[0]);
  ASSERT_EQ(seq, 1);
  add_tenant(tenant_id[1]);
  ASSERT_EQ(seq, 2);
  int ret = OB_SUCCESS;

  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_SUCC(guard.switch_to(tenant_id[0]))) {
    ObPxPools *px_pool = MTL(ObPxPools*);
    ASSERT_NE(px_pool, nullptr);
    MockObPxPools *mock_pool = static_cast<MockObPxPools*>(px_pool);
    ASSERT_EQ(mock_pool->get_seq(), 0);
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard2);
    if (OB_SUCC(guard2.switch_to(tenant_id[1]))) {
      ObPxPools *px_pool2 = MTL(ObPxPools*);
      ASSERT_NE(px_pool2, nullptr);
      ASSERT_NE(px_pool2, px_pool);
      MockObPxPools *mock_pool2 = static_cast<MockObPxPools*>(px_pool2);
      ASSERT_EQ(mock_pool2->get_seq(), 1);
    } else {
      ASSERT_TRUE(false);
    }
    guard2.release();
    px_pool = MTL(ObPxPools*);
    ASSERT_NE(px_pool, nullptr);
    mock_pool = static_cast<MockObPxPools*>(px_pool);
    ASSERT_EQ(mock_pool->get_seq(), 0);
  } else {
    ASSERT_TRUE(false);
  }
  guard.release();
  bool lock_succ = false;
  while (OB_EAGAIN == GCTX.omt_->remove_tenant(tenant_id[0], lock_succ));
  ASSERT_EQ(seq, 1);
  while (OB_EAGAIN == GCTX.omt_->remove_tenant(tenant_id[1], lock_succ));
  ASSERT_EQ(seq, 0);
}

int main(int argc, char *argv[])
{
  system("rm -f ./test_multi_tenant.log");

  ::testing::InitGoogleMock(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_multi_tenant.log", true);
  return RUN_ALL_TESTS();
}
