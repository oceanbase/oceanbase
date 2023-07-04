#include <gtest/gtest.h>
#define private public  // 获取私有成员
#include "observer/table/ob_table_session_pool.h"

using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::sql;

class TestTableSessPool: public ::testing::Test
{
public:
  const int64_t TENANT_CNT = 10;
  const int64_t USER_CNT = 100;
  const int64_t NODE_CNT = 100;
  const int64_t SESS_CNT = 10;
public:
  TestTableSessPool() {}
  virtual ~TestTableSessPool() {}
  void prepare_sess_pool(ObTableApiSessPoolMgr &mgr);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTableSessPool);
};

// 10租户，100用户，每个用户下面挂10个session
void TestTableSessPool::prepare_sess_pool(ObTableApiSessPoolMgr &mgr)
{
  ASSERT_EQ(OB_SUCCESS, mgr.init());
  uint64_t tenant_ids[TENANT_CNT];
  uint64_t user_ids[USER_CNT];
  ObTableApiSessPoolGuard pool_guards[TENANT_CNT];
  for (uint64_t i = 0; i < TENANT_CNT; i++) {
    tenant_ids[i] = i + 1;
    ASSERT_EQ(OB_SUCCESS, mgr.extend_sess_pool(tenant_ids[i], pool_guards[i]));
    ObTableApiSessPool *tmp_pool = pool_guards[i].get_sess_pool();
    ASSERT_NE(nullptr, tmp_pool);
    for (uint64_t j = 0; j < USER_CNT; j++) {
      user_ids[j] = j;
      ASSERT_EQ(OB_SUCCESS, mgr.get_session_pool(tenant_ids[i], pool_guards[i]));
      ObTableApiSessPool *pool = pool_guards[i].get_sess_pool();
      ASSERT_NE(nullptr, pool);
      ObTableApiCredential credential;
      credential.tenant_id_ = tenant_ids[i];
      credential.user_id_ = user_ids[j];
      ASSERT_EQ(OB_SUCCESS, pool->update_sess(credential));
      ObTableApiSessNode *node = nullptr;
      ASSERT_EQ(OB_SUCCESS, tmp_pool->get_sess_node(user_ids[j], node));
      ASSERT_NE(nullptr, node);
      for (int64_t k = 0; k < SESS_CNT; k++) {
        void *buf = node->allocator_.alloc(sizeof(ObTableApiSessNodeVal));
        ASSERT_NE(nullptr, buf);
        ObTableApiSessNodeVal *val = new (buf) ObTableApiSessNodeVal(tenant_ids[i], node);
        val->is_inited_ = true;
        ASSERT_EQ(true, node->sess_lists_.free_list_.add_last(val));
      }
    }
  }
}

TEST_F(TestTableSessPool, get_session)
{
  uint64_t tenant_id = 1;
  uint64_t user_id = 0;
  uint64_t key = user_id;
  ObTableApiSessPoolMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init());
  ObTableApiSessPoolGuard pool_guard;
  ASSERT_EQ(OB_HASH_NOT_EXIST, mgr.get_session_pool(tenant_id, pool_guard));
  ASSERT_EQ(nullptr, pool_guard.get_sess_pool());
  ASSERT_EQ(OB_SUCCESS, mgr.extend_sess_pool(tenant_id, pool_guard));
  ObTableApiSessPool *pool = pool_guard.get_sess_pool();
  ASSERT_TRUE(nullptr != pool);
  ASSERT_TRUE(pool->is_inited_);
  ASSERT_EQ(1, pool->ref_count_);
  ASSERT_EQ(false, pool->is_deleted_);
  ASSERT_EQ(tenant_id, pool->tenant_id_);
  ObTableApiSessGuard sess_guard;
  ObTableApiCredential credential;
  credential.tenant_id_ = tenant_id;
  credential.user_id_ = user_id;
  ASSERT_EQ(OB_SUCCESS, pool->update_sess(credential));
  ObTableApiSessNode *node = nullptr;
  ASSERT_EQ(OB_SUCCESS, pool->get_sess_node(key, node));
  ASSERT_TRUE(nullptr != node);
  ASSERT_TRUE(node->is_empty());
  void *buf = node->allocator_.alloc(sizeof(ObTableApiSessNodeVal));
  ASSERT_NE(nullptr, buf);
  ObTableApiSessNodeVal *new_val = new (buf) ObTableApiSessNodeVal(tenant_id, node);
  new_val->is_inited_ = true;
  ASSERT_EQ(true, node->sess_lists_.free_list_.add_last(new_val));
}

TEST_F(TestTableSessPool, remove_session)
{
  int64_t tenant_id = 1;
  uint64_t user_id = 0;
  ObTableApiCredential credential;
  credential.tenant_id_ = tenant_id;
  credential.user_id_ = user_id;
  ObTableApiSessNode node(credential);
  for (int64_t i = 0; i < SESS_CNT; i++) {
    void *buf = node.allocator_.alloc(sizeof(ObTableApiSessNodeVal));
    ASSERT_NE(nullptr, buf);
    ObTableApiSessNodeVal *val = new (buf) ObTableApiSessNodeVal(tenant_id, &node);
    val->is_inited_ = true;
    ASSERT_EQ(true, node.sess_lists_.free_list_.add_last(val));
  }
  ASSERT_EQ(false, node.is_empty());
  ASSERT_EQ(SESS_CNT, node.sess_lists_.free_list_.get_size());
  node.remove_unused_sess();
  ASSERT_EQ(0, node.sess_lists_.free_list_.get_size());
}

TEST_F(TestTableSessPool, retire_session)
{
  int ret = 0;
  ObTableApiSessPoolMgr mgr;
  prepare_sess_pool(mgr);
  ObTableApiSessPoolForeachOp op;
  ASSERT_EQ(OB_SUCCESS, mgr.sess_pool_map_.foreach_refactored(op));
  const ObTableApiSessPoolForeachOp::TelantIdArray &tenant_ids = op.get_telant_id_array();
  ASSERT_EQ(TENANT_CNT, tenant_ids.count());
  const int64_t N = tenant_ids.count();
  // 1. 标记淘汰
  for (int64_t i = 0; i < N; i++) {
    uint64_t tenant_id = tenant_ids.at(i);
    ObTableApiSessPoolGuard pool_guard;
    ASSERT_EQ(OB_SUCCESS, mgr.get_session_pool(tenant_id, pool_guard));
    ObTableApiSessPool *pool = pool_guard.get_sess_pool();
    ASSERT_NE(nullptr, pool);
    ObTableApiSessForeachOp op;
    ASSERT_EQ(OB_SUCCESS, pool->key_node_map_.foreach_refactored(op));
    const ObTableApiSessForeachOp::SessKvArray &kvs = op.get_key_value_array();
    ASSERT_EQ(NODE_CNT, kvs.count());
    for (int64_t j = 0; j < kvs.count(); j++) {
      if (j % 2 == 0) {
        const ObTableApiSessForeachOp::ObTableApiSessKV &kv = kvs.at(j);
        ASSERT_EQ(OB_SUCCESS, pool->move_sess_to_retired_list(kv.key_));
      }
    }
  }
  // 2. 触发淘汰
  ASSERT_EQ(OB_SUCCESS, mgr.elimination_task_.run_recycle_retired_sess_task());
  // 3. 检查
  for (int64_t i = 0; i < N; i++) {
    uint64_t tenant_id = tenant_ids.at(i);
    ObTableApiSessPoolGuard pool_guard;
    ASSERT_EQ(OB_SUCCESS, mgr.get_session_pool(tenant_id, pool_guard));
    ObTableApiSessPool *pool = pool_guard.get_sess_pool();
    ASSERT_NE(nullptr, pool);
    ObTableApiSessForeachOp op;
    ASSERT_EQ(OB_SUCCESS, pool->key_node_map_.foreach_refactored(op));
    const ObTableApiSessForeachOp::SessKvArray &kvs = op.get_key_value_array();
    ASSERT_EQ(NODE_CNT/2, kvs.count());
    for (int64_t j = 0; j < kvs.count(); j++) {
      const ObTableApiSessForeachOp::ObTableApiSessKV &kv = kvs.at(j);
      ASSERT_EQ(SESS_CNT, kv.node_->sess_lists_.free_list_.get_size());
    }
  }
}

TEST_F(TestTableSessPool, reference_session)
{
  // prepare
  ObTableApiSessPoolMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init());
  uint64_t tenant_id = 1;
  uint64_t user_id = 0;
  ObTableApiSessPoolGuard pool_guard;
  ASSERT_EQ(OB_SUCCESS, mgr.extend_sess_pool(tenant_id, pool_guard));
  ObTableApiSessPool *pool = pool_guard.get_sess_pool();
  ASSERT_NE(nullptr, pool);
  ObTableApiCredential credential;
  credential.tenant_id_ = tenant_id;
  credential.user_id_ = user_id;
  ASSERT_EQ(OB_SUCCESS, pool->update_sess(credential));
  ObTableApiSessNode *node = nullptr;
  ASSERT_EQ(OB_SUCCESS, pool->get_sess_node(user_id, node));
  ASSERT_NE(nullptr, node);
  void *buf = node->allocator_.alloc(sizeof(ObTableApiSessNodeVal));
  ASSERT_NE(nullptr, buf);
  ObTableApiSessNodeVal *val = new (buf) ObTableApiSessNodeVal(tenant_id, node);
  val->is_inited_ = true;
  ASSERT_EQ(true, node->sess_lists_.free_list_.add_last(val));
  // get and retire
  {
    // get
    ObTableApiSessGuard guard;
    ASSERT_EQ(OB_SUCCESS, pool->get_sess_info(credential, guard));
    ASSERT_NE(nullptr, guard.get_sess_node_val());
    // mark retire
    ObTableApiSessForeachOp op;
    ASSERT_EQ(OB_SUCCESS, pool->key_node_map_.foreach_refactored(op));
    const ObTableApiSessForeachOp::SessKvArray &kvs = op.get_key_value_array();
    ASSERT_EQ(1, kvs.count());
    const ObTableApiSessForeachOp::ObTableApiSessKV &kv = kvs.at(0);
    // run retire task
    ASSERT_EQ(OB_SUCCESS, mgr.elimination_task_.run_recycle_retired_sess_task());
    // check
    op.reset();
    ASSERT_EQ(OB_SUCCESS, pool->key_node_map_.foreach_refactored(op));
    const ObTableApiSessForeachOp::SessKvArray &new_kvs = op.get_key_value_array();
    ASSERT_EQ(1, new_kvs.count());
  }
  // retire after def ref
  ASSERT_EQ(OB_SUCCESS, mgr.elimination_task_.run_recycle_retired_sess_task());
  // check
  ObTableApiSessPoolForeachOp op;
  ASSERT_EQ(OB_SUCCESS, mgr.sess_pool_map_.foreach_refactored(op));
  const ObTableApiSessPoolForeachOp::TelantIdArray &arr = op.get_telant_id_array();
  ASSERT_EQ(1, arr.count());
}

TEST_F(TestTableSessPool, retire_session_then_get_session)
{
  uint64_t tenant_id = 1;
  uint64_t user_id = 0;
  ObTableApiSessPoolMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init());
  ObTableApiSessPoolGuard pool_guard;
  ASSERT_EQ(OB_SUCCESS, mgr.extend_sess_pool(tenant_id, pool_guard));
  ObTableApiCredential credential;
  credential.tenant_id_ = tenant_id;
  credential.user_id_ = user_id;
  ASSERT_EQ(OB_SUCCESS, mgr.update_sess(credential));

  // 塞一个ObTableApiSessNodeVal
  ObTableApiSessPool *pool = pool_guard.get_sess_pool();
  ObTableApiSessNode *node = nullptr;
  ASSERT_EQ(OB_SUCCESS, pool->get_sess_node(user_id, node));
  void *buf = node->allocator_.alloc(sizeof(ObTableApiSessNodeVal));
  ASSERT_NE(nullptr, buf);
  ObTableApiSessNodeVal *val = new (buf) ObTableApiSessNodeVal(tenant_id, node);
  val->is_inited_ = true;
  ASSERT_EQ(true, node->sess_lists_.free_list_.add_last(val));

  // 第一次获取了session
  ObTableApiSessGuard sess_guard;
  ASSERT_EQ(OB_SUCCESS, mgr.get_sess_info(credential, sess_guard));
  sess_guard.~ObTableApiSessGuard(); // 模仿访问结束，析构

  // 长时间没有访问ob，session被放到淘汰链表，后台定时回收
  ASSERT_EQ(OB_SUCCESS, pool->get_sess_node(user_id, node));
  ASSERT_EQ(OB_SUCCESS, pool->move_sess_to_retired_list(user_id));
  ASSERT_EQ(1, pool->retired_nodes_.size_);
  ASSERT_EQ(OB_SUCCESS, mgr.elimination_task_.run_recycle_retired_sess_task());
  ASSERT_EQ(0, pool->retired_nodes_.size_);

  // 连接隔了很长时间，突然又访问db
  ASSERT_EQ(OB_HASH_NOT_EXIST, pool->get_sess_node(user_id, node));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("TestTableSessPool.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
