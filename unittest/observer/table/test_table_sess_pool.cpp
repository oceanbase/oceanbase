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

#define private public  // 获取私有成员
#define protected public  // 获取私有成员
#include "observer/table/ob_table_session_pool.h"
#include "mtlenv/mock_tenant_module_env.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::storage;

class TestTableSessPool: public ::testing::Test
{
public:
  TestTableSessPool() {}
  virtual ~TestTableSessPool() {}
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
    TABLEAPI_SESS_POOL_MGR->init();
    create_credential(1, mock_cred_);
  }
  void TearDown()
  {
    TABLEAPI_SESS_POOL_MGR->destroy();
  }
private:
  void create_credential(uint64_t user_id, ObTableApiCredential *&cred);
  int create_and_add_node(ObTableApiCredential &cred);
private:
  ObArenaAllocator allocator_;
  ObTableApiCredential *mock_cred_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTableSessPool);
};

void TestTableSessPool::create_credential(uint64_t user_id, ObTableApiCredential *&cred)
{
  void *buf = nullptr;
  buf = allocator_.alloc(sizeof(ObTableApiCredential));
  cred = new (buf) ObTableApiCredential();
  cred->cluster_id_ = 0;
  cred->tenant_id_ = 1;
  cred->user_id_ = user_id;
  cred->database_id_ = 1;
  cred->expire_ts_ = 0;
  cred->hash(cred->hash_val_);
}

int TestTableSessPool::create_and_add_node(ObTableApiCredential &cred)
{
  int ret = OB_SUCCESS;
  ObTableApiSessNode *tmp_node = nullptr;
  void *buf = nullptr;

  if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->create_session_pool_safe())) {
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableApiSessNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    tmp_node = new (buf) ObTableApiSessNode(cred);
    MemoryContext tmp_mem_ctx = nullptr;
    ContextParam param;
    param.set_mem_attr(MTL_ID(), "TbSessNod", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::ALLOC_THREAD_SAFE);
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(tmp_mem_ctx, param))) {
    } else if (OB_ISNULL(tmp_mem_ctx)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObMemAttr attr(cred.tenant_id_, "TbSessQueue");
      int64_t max_sess_num = 100;
      tmp_node->mem_ctx_ = tmp_mem_ctx;
      tmp_node->last_active_ts_ = ObTimeUtility::fast_current_time();
      tmp_node->is_inited_ = true;
      if (OB_FAIL(tmp_node->sess_queue_.init(max_sess_num, &tmp_node->queue_allocator_, attr))) {
      } else if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->pool_->key_node_map_.set_refactored(cred.hash_val_, tmp_node))) {
        if (OB_HASH_EXIST != ret) {
        } else {
          ret = OB_SUCCESS; // replace error code
        }
        tmp_node->~ObTableApiSessNode();
        allocator_.free(tmp_node);
        tmp_node = nullptr;
      }
    }

  }
  return ret;
}

TEST_F(TestTableSessPool, test_mgr_init)
{
  ObTableApiSessPoolMgr mgr;
  ASSERT_FALSE(mgr.is_inited_);
  ASSERT_EQ(nullptr, mgr.pool_);
  ASSERT_EQ(nullptr, mgr.elimination_task_.sess_pool_mgr_);

  ASSERT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ASSERT_TRUE(TABLEAPI_SESS_POOL_MGR->is_inited_);
  ASSERT_EQ(nullptr, TABLEAPI_SESS_POOL_MGR->pool_);
  ASSERT_EQ(TABLEAPI_SESS_POOL_MGR, TABLEAPI_SESS_POOL_MGR->elimination_task_.sess_pool_mgr_);
}

TEST_F(TestTableSessPool, test_pool_init)
{
  ObTableApiSessPool pool;
  ASSERT_FALSE(pool.is_inited_);
  ASSERT_TRUE(pool.key_node_map_.empty());
  ASSERT_TRUE(pool.retired_nodes_.is_empty());
}

TEST_F(TestTableSessPool, test_node_init)
{
  ObTableApiSessNode node(*mock_cred_);
  ASSERT_EQ(0, node.last_active_ts_);
}

TEST_F(TestTableSessPool, test_node_val_init)
{
  ObTableApiSessNode node(*mock_cred_);
  ObTableApiSessNodeVal val(&node, MTL_ID());
  ASSERT_FALSE(val.is_inited_);
  ASSERT_EQ(&node, val.owner_node_);
}

TEST_F(TestTableSessPool, test_sess_guard_init)
{
  ObTableApiSessGuard guard;
  ASSERT_EQ(nullptr, guard.sess_node_val_);
}

TEST_F(TestTableSessPool, mgr_get_session)
{
  ASSERT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTableApiSessPoolMgr *mgr = TABLEAPI_SESS_POOL_MGR;

  // first time will create a new node
  ASSERT_EQ(OB_SUCCESS, create_and_add_node(*mock_cred_));
  ASSERT_NE(nullptr, mgr->pool_);
  ASSERT_TRUE(mgr->pool_->is_inited_);
  ASSERT_EQ(1, mgr->pool_->key_node_map_.size());
  ASSERT_EQ(0, mgr->pool_->retired_nodes_.size_);
  ObTableApiSessNode *node;
  ASSERT_EQ(OB_SUCCESS, mgr->pool_->get_sess_node(mock_cred_->hash_val_, node));
  ASSERT_NE(nullptr, node);
  ASSERT_NE(0, node->last_active_ts_);

  // add mock val to node
  void *buf = nullptr;
  ObMemAttr attr(MTL_ID(), "TbSessNodVal", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_NE(nullptr, buf = node->mem_ctx_->allocf(sizeof(ObTableApiSessNodeVal), attr));
  ObTableApiSessNodeVal *val = new (buf) ObTableApiSessNodeVal(node, MTL_ID());
  val->is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, node->sess_queue_.push(val));

  ObTableApiSessGuard guard;
  mgr->sys_vars_.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, mgr->get_sess_info(*mock_cred_, guard));
  ASSERT_NE(nullptr, guard.sess_node_val_);
  ASSERT_NE(nullptr, guard.get_sess_node_val());
  const ObTableApiCredential *cred = nullptr;
  ASSERT_EQ(OB_SUCCESS, guard.get_credential(cred));
  ASSERT_NE(nullptr, cred);
  mgr->destroy();
}

TEST_F(TestTableSessPool, mgr_destroy)
{
  ASSERT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTableApiSessPoolMgr *mgr = TABLEAPI_SESS_POOL_MGR;
  ASSERT_EQ(OB_SUCCESS, create_and_add_node(*mock_cred_));
  ASSERT_NE(nullptr, mgr->pool_);
  ObTableApiSessNode *node;
  ASSERT_EQ(OB_SUCCESS, mgr->pool_->get_sess_node(mock_cred_->hash_val_, node));
  mgr->destroy();
  ASSERT_FALSE(mgr->is_inited_);
  ASSERT_EQ(nullptr, mgr->pool_);
  ASSERT_EQ(0, mgr->allocator_.total());
  ASSERT_EQ(0, mgr->allocator_.used());
}

TEST_F(TestTableSessPool, mgr_sess_recycle)
{
  ASSERT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTableApiSessPoolMgr *mgr = TABLEAPI_SESS_POOL_MGR;
  ASSERT_EQ(OB_SUCCESS, create_and_add_node(*mock_cred_));
  ASSERT_NE(nullptr, mgr->pool_);

  // add mock val to node
  ObTableApiSessNode *node;
  ASSERT_EQ(OB_SUCCESS, mgr->pool_->get_sess_node(mock_cred_->hash_val_, node));
  void *buf = nullptr;
  ObMemAttr attr(MTL_ID(), "TbSessNodVal", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_NE(nullptr, buf = node->mem_ctx_->allocf(sizeof(ObTableApiSessNodeVal), attr));
  ObTableApiSessNodeVal *val = new (buf) ObTableApiSessNodeVal(node, MTL_ID());
  val->is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, node->sess_queue_.push(val));

  ObTableApiSessGuard guard;
  mgr->sys_vars_.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, mgr->get_sess_info(*mock_cred_, guard));
  mgr->elimination_task_.runTimerTask();
  ASSERT_EQ(1, mgr->pool_->key_node_map_.size());
  ASSERT_EQ(0, mgr->pool_->retired_nodes_.size_);
  guard.~ObTableApiSessGuard();

  // 3min not access
  ASSERT_EQ(OB_SUCCESS, mgr->pool_->get_sess_node(mock_cred_->hash_val_, node));
  node->last_active_ts_ = node->last_active_ts_ - ObTableApiSessPool::SESS_RETIRE_TIME;
  mgr->elimination_task_.run_retire_sess_task();
  ASSERT_EQ(0, mgr->pool_->key_node_map_.size());
  ASSERT_EQ(1, mgr->pool_->retired_nodes_.size_);
  mgr->elimination_task_.run_recycle_retired_sess_task();
  ASSERT_EQ(0, mgr->pool_->key_node_map_.size());
  ASSERT_EQ(0, mgr->pool_->retired_nodes_.size_);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("TestTableSessPool.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
