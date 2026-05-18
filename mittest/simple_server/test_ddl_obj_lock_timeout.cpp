/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX RS
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "share/ob_share_util.h"
#include "share/ob_debug_sync.h"
#include "lib/worker.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::rootserver;
using namespace oceanbase::transaction;
using namespace oceanbase::transaction::tablelock;
using namespace oceanbase::share;
using namespace oceanbase::common;

class ObDDLObjLockTimeoutTest : public ObSimpleClusterTestBase
{
public:
  ObDDLObjLockTimeoutTest() : ObSimpleClusterTestBase("test_ddl_obj_lock_timeout_") {}

protected:
  int acquire_obj_lock(ObTxDesc *&tx_desc,
                       ObTransService *txs,
                       const uint64_t obj_id,
                       const ObTableLockMode lock_mode);

  // Mirrors obj_lock_with_lock_id_ timeout/error-conversion logic
  // using a plain ObMySQLTransaction instead of ObDDLSQLTransaction
  int try_lock_obj_via_inner_conn(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t obj_id,
      const ObTableLockMode lock_mode);

  void cleanup_tx(ObTransService *txs, ObTxDesc *tx_desc);
};

int ObDDLObjLockTimeoutTest::acquire_obj_lock(
    ObTxDesc *&tx_desc,
    ObTransService *txs,
    const uint64_t obj_id,
    const ObTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  ObTxParam tx_param;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 60 * 1000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  if (OB_FAIL(txs->acquire_tx(tx_desc))) {
    LOG_WARN("acquire tx failed", KR(ret));
  } else {
    ObLockObjsRequest lock_arg;
    lock_arg.lock_mode_ = lock_mode;
    lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
    lock_arg.timeout_us_ = 0;
    lock_arg.is_from_sql_ = true;
    ObLockID lock_id;
    lock_id.set(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, obj_id);
    if (OB_FAIL(lock_arg.objs_.push_back(lock_id))) {
      LOG_WARN("push back lock id failed", KR(ret));
    } else if (OB_FAIL(MTL(ObTableLockService*)->lock(*tx_desc, tx_param, lock_arg))) {
      LOG_WARN("lock obj failed", KR(ret), K(obj_id), K(lock_mode));
    }
  }
  return ret;
}

int ObDDLObjLockTimeoutTest::try_lock_obj_via_inner_conn(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t obj_id,
    const ObTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = nullptr;
  if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>(
                    trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans conn is NULL", KR(ret));
  } else {
    ObTimeoutCtx ctx;
    ObLockObjRequest lock_arg;
    lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_COMMON_OBJ;
    lock_arg.owner_id_ = ObTableLockOwnerID::default_owner();
    lock_arg.obj_id_ = obj_id;
    lock_arg.lock_mode_ = lock_mode;
    lock_arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    const int64_t rpc_timeout = GCONF.rpc_timeout;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else if (FALSE_IT(lock_arg.timeout_us_ = min(rpc_timeout, ctx.get_timeout()))) {
    } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, lock_arg, conn))) {
      if (OB_EAGAIN == ret && ctx.get_timeout() > 0) {
        ret = OB_ERR_PARALLEL_DDL_CONFLICT;
        LOG_WARN("lock obj eagain, overwrite to parallel ddl conflict",
                 KR(ret), K(tenant_id), K(lock_arg));
      } else {
        LOG_WARN("lock obj failed", KR(ret), K(tenant_id), K(lock_arg));
      }
    } else {
      DEBUG_SYNC(AFTER_DDL_OBJ_LOCK);
    }
  }
  return ret;
}

void ObDDLObjLockTimeoutTest::cleanup_tx(ObTransService *txs, ObTxDesc *tx_desc)
{
  if (OB_NOT_NULL(txs) && OB_NOT_NULL(tx_desc)) {
    txs->rollback_tx(*tx_desc);
    txs->release_tx(*tx_desc);
  }
}

// Lock times out due to rpc_timeout while overall DDL timeout is still valid
// → OB_ERR_PARALLEL_DDL_CONFLICT
TEST_F(ObDDLObjLockTimeoutTest, test_lock_timeout_to_parallel_ddl_conflict)
{
  LOG_INFO("test_lock_timeout_to_parallel_ddl_conflict begin");
  const uint64_t test_obj_id = 99999;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObTxDesc *tx_desc = nullptr;
  ObTransService *txs = nullptr;

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  // Step 1: tx1 holds exclusive lock on test_obj_id
  ASSERT_EQ(OB_SUCCESS, acquire_obj_lock(tx_desc, txs, test_obj_id, EXCLUSIVE));
  LOG_INFO("tx1 acquired exclusive lock", K(test_obj_id));

  // Step 2: Set worker timeout to 30s (simulating long ddl_timeout)
  const int64_t saved_timeout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30 * 1000 * 1000L);

  // Step 3: Start a regular transaction and try to lock the same object
  {
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, tenant_id));

    ret = try_lock_obj_via_inner_conn(trans, tenant_id, test_obj_id, EXCLUSIVE);
    LOG_INFO("try_lock_obj result", KR(ret));
    ASSERT_EQ(OB_ERR_PARALLEL_DDL_CONFLICT, ret);

    trans.end(false);
  }

  // Cleanup
  THIS_WORKER.set_timeout_ts(saved_timeout_ts);
  cleanup_tx(txs, tx_desc);
  LOG_INFO("test_lock_timeout_to_parallel_ddl_conflict end");
}

// Worker timeout not set → ctx uses rpc_timeout as fallback →
// after lock timeout, ctx also expired → stays OB_TIMEOUT
TEST_F(ObDDLObjLockTimeoutTest, test_lock_timeout_when_overall_expired)
{
  LOG_INFO("test_lock_timeout_when_overall_expired begin");
  const uint64_t test_obj_id = 99998;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObTxDesc *tx_desc = nullptr;
  ObTransService *txs = nullptr;

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);

  // Step 1: tx1 holds exclusive lock (use different obj_id to avoid residual from previous test)
  ASSERT_EQ(OB_SUCCESS, acquire_obj_lock(tx_desc, txs, test_obj_id, EXCLUSIVE));
  LOG_INFO("tx1 acquired exclusive lock", K(test_obj_id));

  // Step 2: Worker timeout = INT64_MAX (not set)
  // set_default_timeout_ctx fallback → ctx timeout = rpc_timeout
  // lock timeout = min(rpc_timeout, rpc_timeout) = rpc_timeout
  // After lock times out, lock_obj returns OB_EAGAIN (rewritten from OB_TIMEOUT),
  // ctx also expired → stays OB_EAGAIN (not converted to PARALLEL_DDL_CONFLICT)
  const int64_t saved_timeout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(INT64_MAX);

  // Step 3
  {
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, tenant_id));

    ret = try_lock_obj_via_inner_conn(trans, tenant_id, test_obj_id, EXCLUSIVE);
    LOG_INFO("try_lock_obj result", KR(ret));
    ASSERT_EQ(OB_EAGAIN, ret);

    trans.end(false);
  }

  // Cleanup
  THIS_WORKER.set_timeout_ts(saved_timeout_ts);
  cleanup_tx(txs, tx_desc);
  LOG_INFO("test_lock_timeout_when_overall_expired end");
}

// No lock conflict → lock succeeds
TEST_F(ObDDLObjLockTimeoutTest, test_lock_success_no_conflict)
{
  LOG_INFO("test_lock_success_no_conflict begin");
  const uint64_t test_obj_id_no_conflict = 88888;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  const int64_t saved_timeout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30 * 1000 * 1000L);

  {
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, tenant_id));

    ret = try_lock_obj_via_inner_conn(trans, tenant_id, test_obj_id_no_conflict, EXCLUSIVE);
    LOG_INFO("try_lock_obj result", KR(ret));
    ASSERT_EQ(OB_SUCCESS, ret);

    trans.end(false);
  }

  THIS_WORKER.set_timeout_ts(saved_timeout_ts);
  LOG_INFO("test_lock_success_no_conflict end");
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(OB_LOG_LEVEL_INFO);
  OB_LOGGER.set_mod_log_levels("RS:DEBUG");
  OB_LOGGER.set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
