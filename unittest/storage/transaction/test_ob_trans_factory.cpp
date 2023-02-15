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

#include "storage/tx/ob_trans_factory.h"
#include "storage/tx/ob_trans_log.h"
#include "storage/tx/ob_trans_ctx.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{
class TestObTransFactory : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static void *alloc_ls_tx_ctx_mgr(void *args);
  static void *alloc_clog_buf(void *args);
  //static void *alloc_trans_rpc(void *args);
  static void *alloc_mutator_buf(void *args);

public :
  static const char *LOCAL_IP;
  static const int32_t PORT = 8080;
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;

  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VClogBufALID_PARTITION_COUNT = 100;
};
const char *TestObTransFactory::LOCAL_IP = "127.0.0.1";

void *TestObTransFactory::alloc_ls_tx_ctx_mgr(void *args)
{
  ObLSTxCtxMgr *ls_tx_ctx_mgr = static_cast<ObLSTxCtxMgr *>(args);
  if (NULL != ls_tx_ctx_mgr) {
    ObLSTxCtxMgrFactory::release(ls_tx_ctx_mgr);
    EXPECT_EQ(1, ObLSTxCtxMgrFactory::get_release_count());
  }

  pthread_exit(NULL);
}

void *TestObTransFactory::alloc_clog_buf(void *args)
{
  ClogBuf *clog_buf = static_cast<ClogBuf *>(args);
  if (NULL != clog_buf) {
    ClogBufFactory::release(clog_buf);
    EXPECT_EQ(1, ClogBufFactory::get_release_count());
  }
  pthread_exit(NULL);
}

/*
void *TestObTransFactory::alloc_trans_rpc(void *args)
{

  TransRpcTask *trans_rpc_task = static_cast<TransRpcTask *>(args);
  if (NULL != trans_rpc_task) {
    TransRpcTaskFactory::release(trans_rpc_task);
    EXPECT_EQ(1, TransRpcTaskFactory::get_release_count());
  }

  pthread_exit(NULL);
}*/

void *TestObTransFactory::alloc_mutator_buf(void *args)
{
  MutatorBuf *mutator_buf = static_cast<MutatorBuf *>(args);
  if (NULL != mutator_buf) {
    MutatorBufFactory::release(mutator_buf);
    EXPECT_EQ(1, MutatorBufFactory::get_release_count());
  }

  pthread_exit(NULL);
}

TEST_F(TestObTransFactory, init_reset)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ClogBuf *clog_buf = NULL;
  //TransRpcTask *trans_rpc_task = NULL;
  MutatorBuf *mutator_buf = NULL;

  // alloc for ObLSTxCtxMgr object
  uint64_t tenant_id = 1001;
  ls_tx_ctx_mgr = ObLSTxCtxMgrFactory::alloc(tenant_id);
  if (NULL == ls_tx_ctx_mgr) {
    TRANS_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "ObLSTxCtxMgr memory alloc error");
  } else {
    EXPECT_EQ(1, ObLSTxCtxMgrFactory::get_alloc_count());
  }
  // free
  pthread_t tid1;
  EXPECT_TRUE(0 == pthread_create(&tid1, &attr, TestObTransFactory::alloc_ls_tx_ctx_mgr,
      static_cast<void *>(ls_tx_ctx_mgr)));
  pthread_join(tid1, NULL);

  // alloc for ClogBuf object
  clog_buf = ClogBufFactory::alloc();
  if (NULL == clog_buf) {
    TRANS_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "ClogBuf memory alloc error");
  } else {
    EXPECT_EQ(1, ClogBufFactory::get_alloc_count());
  }
  // free
  pthread_t tid2;
  EXPECT_TRUE(0 == pthread_create(&tid2, &attr, TestObTransFactory::alloc_clog_buf,
      static_cast<void *>(clog_buf)));
  pthread_join(tid2, NULL);

  /*
  // alloc for TransRpcTask object
  trans_rpc_task = TransRpcTaskFactory::alloc();
  if (NULL == trans_rpc_task) {
    TRANS_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "TransRpcTaskFactory memory alloc error");
  } else {
    EXPECT_EQ(1, TransRpcTaskFactory::get_alloc_count());
  }
  // free
  pthread_t tid3;
  EXPECT_TRUE(0 == pthread_create(&tid3, &attr, TestObTransFactory::alloc_trans_rpc,
      static_cast<void *>(trans_rpc_task)));
  pthread_join(tid3, NULL);
  */

  // alloc for MutatorBuf object
  mutator_buf = MutatorBufFactory::alloc();
  if (NULL == mutator_buf) {
    TRANS_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "MutatorBufFactory memory alloc error");
  } else {
    EXPECT_EQ(1, MutatorBufFactory::get_alloc_count());
  }
  // free
  pthread_t tid5;
  EXPECT_TRUE(0 == pthread_create(&tid5, &attr, TestObTransFactory::alloc_mutator_buf,
      static_cast<void *>(mutator_buf)));
  pthread_join(tid5, NULL);
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_factory.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
