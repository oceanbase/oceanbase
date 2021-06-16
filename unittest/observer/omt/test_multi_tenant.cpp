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
#include <gmock/gmock.h>

#include "all_mock.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_token_calcer.h"
#define protected public
#include "lib/alloc/ob_tenant_ctx_allocator.h"
#undef protected
#define protected public
#include "observer/omt/ob_tenant.h"
#undef protected
#include "observer/omt/ob_worker_processor.h"
#include "lib/random/ob_random.h"
#include "lib/oblog/ob_log.h"
#include "share/rc/ob_context.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/allocator/ob_malloc.h"

using namespace std;
// using namespace share;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::omt;
using namespace oceanbase::observer;

#define OMT_UNITTEST

class MockObPxPools : public ObPxPools {
public:
  MockObPxPools() : ObPxPools()
  {
    this_seq_ = MockObPxPools::seq_++;
  }
  ~MockObPxPools()
  {
    MockObPxPools::seq_--;
  }
  static int mock_mtl_init(ObPxPools*& pool)
  {
    int ret = OB_SUCCESS;
    pool = OB_NEW(MockObPxPools, common::ObModIds::OMT_TENANT);
    if (OB_ISNULL(pool)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }
  static void mock_mtl_destroy(ObPxPools*& pool)
  {
    MockObPxPools* ptr = static_cast<MockObPxPools*>(pool);
    OB_DELETE(MockObPxPools, common::ObModIds::OMT_TENANT, ptr);
    pool = nullptr;
  }
  int64_t get_seq()
  {
    return this_seq_;
  }

private:
  static int64_t seq_;
  int64_t this_seq_;
};
int64_t MockObPxPools::seq_ = 0;

class MockOMT : public ObMultiTenant {
  static constexpr auto TIMES_OF_WORKERS = 10;

public:
  MockOMT() : ObMultiTenant(procor_)
  {}
  int add_tenant(uint64_t tenant_id)
  {
    return ObMultiTenant::add_tenant(tenant_id, 1, 1);
  }

private:
  ObFakeWorkerProcessor procor_;
};
class TestMultiTenant : public ::testing::Test {
public:
  TestMultiTenant()
  {
    all_mock_init();
  }

  static int fake_init_compat_mode(ObWorker::CompatMode& compat_mode)
  {
    compat_mode = ObWorker::CompatMode::ORACLE;
    return OB_SUCCESS;
  }

  static int fake_trans_audit_recmgr_mtl_init(transaction::ObTransAuditRecordMgr*& mgr)
  {
    mgr = nullptr;
    return OB_SUCCESS;
  }

  static int fake_dfc_mtl_init(sql::dtl::ObTenantDfc*& dfc)
  {
    dfc = nullptr;
    return OB_SUCCESS;
  }

  virtual void SetUp()
  {
    static constexpr auto NODE_QUOTA = 10;
    EXPECT_EQ(OB_SUCCESS, omt_.init(ObAddr(), NODE_QUOTA));
    MTL_BIND(fake_init_compat_mode, nullptr);
    MTL_BIND(fake_trans_audit_recmgr_mtl_init, nullptr);
    MTL_BIND(fake_dfc_mtl_init, nullptr);
  }

  virtual void TearDown()
  {
    // Delete all tenants and ensure that resources are released
    TenantIdList ids;
    omt_.get_tenant_ids(ids);
    for (int64_t index = 0; index < ids.size(); index++) {
      omt_.del_tenant(ids[index], true);
    }
    // omt_.clear();
    omt_.destroy();
  }

  // void clear() { omt_.clear(); }

protected:
  MockOMT omt_;
};

TEST_F(TestMultiTenant, basic)
{
  // sorted
  uint64_t tenants[] = {100, 24, 45, 1, 94, 23, 9, 17, 10000};
  int cnt = sizeof(tenants) / sizeof(uint64_t);
  int ret = OB_SUCCESS;
  for (int i = 0; i < cnt; i++) {
    ret = omt_.add_tenant(tenants[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  TenantList& list = omt_.get_tenant_list();
  ASSERT_EQ(cnt, list.size());
  uint64_t last = 0;
  for (auto it = list.begin(); it != list.end(); it++) {
    ASSERT_GT((*it)->id(), last);
    last = (*it)->id();
  }
  ret = omt_.add_tenant(tenants[0]);
  ASSERT_EQ(OB_TENANT_EXIST, ret);
  omt_.del_tenant(tenants[0]);
  // ASSERT_EQ(OB_SUCCESS, ret); // partition_service should init
  ret = omt_.add_tenant(tenants[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMultiTenant, RS_TENANT)
{
  uint64_t tenant_id = OB_RS_TENANT_ID;
  int ret = omt_.add_tenant(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  lib::ObMallocAllocator* ma = lib::ObMallocAllocator::get_instance();
  ASSERT_EQ(OB_RS_MEMORY, ma->get_tenant_limit(OB_RS_TENANT_ID));
}

// int get_tenant_with_tenant_lock(const uint64_t tenant_id, ObTenant *&tenant) const;
TEST_F(TestMultiTenant, tenant_lock)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 100;
  omt_.add_tenant(tenant_id);
  ObLDHandle handle;
  // new
  ObTenant* tenant = nullptr;
  ret = omt_.get_tenant_with_tenant_lock(tenant_id, handle, tenant);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(tenant, nullptr);
  ASSERT_TRUE(tenant->lock_.is_locked());
  tenant->unlock(handle);
  ASSERT_FALSE(tenant->lock_.is_locked());

  // old
  tenant = nullptr;
  ret = omt_.get_tenant(tenant_id, tenant);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(tenant, nullptr);
  ASSERT_FALSE(tenant->lock_.is_locked());
}

TEST_F(TestMultiTenant, get_tenant_context)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 100;
  omt_.add_tenant(tenant_id);
  GCTX.omt_ = &omt_;
  share::ObTenantSpace* ctx = nullptr;
  ObLDHandle handle;
  ret = share::get_tenant_ctx_with_tenant_lock(tenant_id, handle, ctx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->get_tenant(), nullptr);
  ASSERT_TRUE(static_cast<ObTenant*>(ctx->get_tenant())->lock_.is_locked());
  static_cast<ObTenant*>(ctx->get_tenant())->unlock(handle);
}

class CtxMemConfigGetter : public ObICtxMemConfigGetter {
public:
  virtual int get(common::ObIArray<ObCtxMemConfig>& configs)
  {
    int ret = OB_SUCCESS;
    {
      ObCtxMemConfig cfg;
      cfg.ctx_id_ = 10;
      cfg.idle_size_ = 10 * INTACT_ACHUNK_SIZE;
      ret = configs.push_back(cfg);
    }
    {
      ObCtxMemConfig cfg;
      cfg.ctx_id_ = 11;
      cfg.idle_size_ = 20 * INTACT_ACHUNK_SIZE;
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
  int ret = omt_.add_tenant(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  common::ObArray<ObCtxMemConfig> configs;
  mcg.get(configs);
  ASSERT_TRUE(2 == configs.size());
  ObMallocAllocator* malloc_allocator = ObMallocAllocator::get_instance();
  for (int i = 0; i < configs.size(); i++) {
    ObCtxMemConfig& cfg = configs.at(i);
    ObTenantCtxAllocator* ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, cfg.ctx_id_);
    ASSERT_TRUE(NULL != ta);
    int64_t chunk_cnt = cfg.idle_size_ / INTACT_ACHUNK_SIZE;
    ASSERT_EQ(ta->chunk_cnt_, chunk_cnt);
    ASSERT_EQ(ta->idle_size_, cfg.idle_size_);
    ASSERT_NE(0, malloc_allocator->get_tenant_ctx_hold(tenant_id, cfg.ctx_id_));
  }
  // delete
  omt_.del_tenant(tenant_id);
  for (int i = 0; i < configs.size(); i++) {
    ObCtxMemConfig& cfg = configs.at(i);
    ObTenantCtxAllocator* ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, cfg.ctx_id_);
    ASSERT_TRUE(NULL != ta);
    ASSERT_EQ(0, malloc_allocator->get_tenant_ctx_hold(tenant_id, cfg.ctx_id_));
  }
}

TEST_F(TestMultiTenant, tenant_local)
{
  MTL_BIND(MockObPxPools::mock_mtl_init, MockObPxPools::mock_mtl_destroy);
  uint64_t tenant_id[] = {1010, 1011};
  int64_t& seq = MockObPxPools::seq_;
  ASSERT_EQ(seq, 0);
  omt_.add_tenant(tenant_id[0]);
  ASSERT_EQ(seq, 1);
  omt_.add_tenant(tenant_id[1]);
  ASSERT_EQ(seq, 2);
  int ret = OB_SUCCESS;

  FETCH_ENTITY(TENANT_SPACE, tenant_id[0])
  {
    ObPxPools* px_pool = MTL_GET(ObPxPools*);
    ASSERT_NE(px_pool, nullptr);
    MockObPxPools* mock_pool = static_cast<MockObPxPools*>(px_pool);
    ASSERT_EQ(mock_pool->get_seq(), 0);
    FETCH_ENTITY(TENANT_SPACE, tenant_id[1])
    {
      ObPxPools* px_pool2 = MTL_GET(ObPxPools*);
      ASSERT_NE(px_pool2, nullptr);
      ASSERT_NE(px_pool2, px_pool);
      MockObPxPools* mock_pool2 = static_cast<MockObPxPools*>(px_pool2);
      ASSERT_EQ(mock_pool2->get_seq(), 1);
    }
    else
    {
      ASSERT_TRUE(false);
    }
    px_pool = MTL_GET(ObPxPools*);
    ASSERT_NE(px_pool, nullptr);
    mock_pool = static_cast<MockObPxPools*>(px_pool);
    ASSERT_EQ(mock_pool->get_seq(), 0);
  }
  else
  {
    ASSERT_TRUE(false);
  }
  omt_.del_tenant(tenant_id[0]);
  ASSERT_EQ(seq, 1);
  omt_.del_tenant(tenant_id[1]);
  ASSERT_EQ(seq, 0);
}

TEST_F(TestMultiTenant, table_context)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1001;
  ret = omt_.add_tenant(tenant_id);  // fake compat mode is oracle
  ASSERT_EQ(OB_SUCCESS, ret);
  GCTX.omt_ = &omt_;
  uint64_t table_id = combine_id(1001, OB_MAX_SYS_TABLE_ID);
  THIS_WORKER.set_compatibility_mode(ObWorker::CompatMode::INVALID);
  ObWorker::CompatMode orig_compat_mode = ObWorker::CompatMode::INVALID;

  bool flag = false;
  CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, table_id)
  {
    flag = true;
    ASSERT_EQ(ObWorker::CompatMode::MYSQL, THIS_WORKER.get_compatibility_mode());
  }
  ASSERT_TRUE(flag);
  ASSERT_EQ(orig_compat_mode, THIS_WORKER.get_compatibility_mode());

  flag = false;
  table_id = combine_id(1001, OB_MAX_SYS_TABLE_ID + 1);
  CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, table_id)
  {
    flag = true;
    ASSERT_EQ(ObWorker::CompatMode::ORACLE, THIS_WORKER.get_compatibility_mode());
  }
  ASSERT_TRUE(flag);
  ASSERT_EQ(orig_compat_mode, THIS_WORKER.get_compatibility_mode());
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleMock(&argc, argv);
  if (argc > 1) {
    OB_LOGGER.set_log_level(3);
  }
  return RUN_ALL_TESTS();
}
