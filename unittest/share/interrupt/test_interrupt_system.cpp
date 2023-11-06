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

#define private public

#include "share/interrupt/ob_interrupt_rpc_proxy.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "sql/engine/px/ob_px_interruption.h"
#include "rpc/testing.h"
#include <thread>

using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace common
{

rpctesting::Service g_service;
ObInterruptProcessor g_processor;
ObInterruptRpcProxy g_proxy;
ObGlobalInterruptManager *g_manager(nullptr);

void init_rpc()
{
  int ret = OB_SUCCESS;
  ret = g_service.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = g_service.reg_processor(&g_processor);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = g_service.get_proxy(g_proxy);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST(ObGlobalInterruptManager, init_manager)
{
  int ret = OB_SUCCESS;
  init_rpc();
  ObAddr local(ObAddr::IPV4, "127.0.0.1", 10086);
  ObAddr dst = g_service.get_dst();

  // prepare mocked ObInterruptibleTaskID, ObInterruptCode,
  ObInterruptibleTaskID interrupt_id(1, 1);
  ObInterruptStackInfo interrupt_stack_info;
  interrupt_stack_info.set_info(1 /*from tid*/, local /*from_svr_addr*/,
                                "mock error" /*extra_msg*/);
  ObInterruptCode interrupt_code(OB_RPC_CONNECT_ERROR, interrupt_stack_info);
  ObInterruptChecker *checker = nullptr;

  // Singleton acquisition
  g_manager = ObGlobalInterruptManager::getInstance();
  ASSERT_EQ(false, nullptr == g_manager);

  // test init ObGlobalInterruptManager
  ret = g_manager->interrupt(interrupt_id, interrupt_code);
  ASSERT_EQ(OB_NOT_INIT, ret);

  ret = g_manager->interrupt(local, interrupt_id, interrupt_code);
  ASSERT_EQ(OB_NOT_INIT, ret);

  ret = g_manager->register_checker(checker, interrupt_id);
  ASSERT_EQ(OB_NOT_INIT, ret);

  ret = g_manager->unregister_checker(checker, interrupt_id);
  ASSERT_EQ(OB_NOT_INIT, ret);

  // No RPC initialization is illegal
  ret = g_manager->init(local, nullptr);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // Regular initialization
  ret = g_manager->init(local, &g_proxy);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Initialize correctly and feedback error many times
  ret = g_manager->init(local, &g_proxy);
  ASSERT_EQ(OB_INIT_TWICE, ret);
}

TEST(ObGlobalInterruptManager, register_and_unregister)
{
  int ret = OB_SUCCESS;
  ObAddr local(ObAddr::IPV4, "127.0.0.1", 10086);
  ObAddr dst = g_service.get_dst();

  // prepare mocked ObInterruptibleTaskID, ObInterruptCode,
  ObInterruptibleTaskID interrupt_id(1, 1);
  ObInterruptStackInfo interrupt_stack_info;
  interrupt_stack_info.set_info(1 /*from tid*/, local /*from_svr_addr*/,
                                "mock error" /*extra_msg*/);
  ObInterruptCode interrupt_code(OB_RPC_CONNECT_ERROR, interrupt_stack_info);

  // prepare checker
  ObInterruptChecker *checker = nullptr;
  checker = new ObInterruptChecker(false, 1); // no coroutine again

  // Test self-registration
  ret = checker->register_checker(interrupt_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Test duplicate registration
  ret = checker->register_checker(interrupt_id);
  ASSERT_EQ(OB_HASH_EXIST, ret);
  ret = g_manager->register_checker(checker, interrupt_id);
  ASSERT_EQ(OB_HASH_EXIST, ret);

  // unregister checker
  ret = g_manager->unregister_checker(checker, interrupt_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // duplicate unregister checker
  ret = g_manager->unregister_checker(checker, interrupt_id);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);
  EXPECT_EQ(0, g_manager->map_.size());

  delete checker;

  // Test group registration and deregistration
  ObInterruptChecker *checker0 = new ObInterruptChecker(false, 1);
  ObInterruptChecker *checker1 = new ObInterruptChecker(false, 1);
  ObInterruptChecker *checker2 = new ObInterruptChecker(false, 1);
  ObInterruptChecker *checker3 = new ObInterruptChecker(false, 1);
  ret = g_manager->register_checker(checker0, interrupt_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = g_manager->register_checker(checker1, interrupt_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = g_manager->register_checker(checker2, interrupt_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = g_manager->register_checker(checker3, interrupt_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = g_manager->unregister_checker(checker0, interrupt_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = g_manager->unregister_checker(checker1, interrupt_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = g_manager->unregister_checker(checker2, interrupt_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = g_manager->unregister_checker(checker3, interrupt_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  delete checker0;
  delete checker1;
  delete checker2;
  delete checker3;

  EXPECT_EQ(0, g_manager->map_.size());
}

TEST(ObGlobalInterruptManager, interrupt)
{
  int ret = OB_SUCCESS;
  ObAddr local(ObAddr::IPV4, "127.0.0.1", 10086);
  ObAddr dst = g_service.get_dst();

  // prepare mocked ObInterruptibleTaskID, ObInterruptCode,
  ObInterruptibleTaskID interrupt_id(1, 1);
  ObInterruptStackInfo interrupt_stack_info;
  interrupt_stack_info.set_info(1 /*from tid*/, local /*from_svr_addr*/,
                                "mock error" /*extra_msg*/);
  ObInterruptCode interrupt_code(OB_RPC_CONNECT_ERROR, interrupt_stack_info);

  // prepare checker && register checker
  ObInterruptChecker *checker = nullptr;
  checker = new ObInterruptChecker(false, 1); // no coroutine again
  ret = checker->register_checker(interrupt_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Semaphore detection without interruption
  bool r = checker->is_interrupted();
  ASSERT_EQ(false, r);

  // Local execution interrupt check semaphore
  ret = g_manager->interrupt(interrupt_id, interrupt_code);
  ASSERT_EQ(OB_SUCCESS, ret);

  r = checker->is_interrupted();
  ASSERT_EQ(true, r);

  // unregister checker
  ret = g_manager->unregister_checker(checker, interrupt_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // The interrupt can still be called after the checker is unregistered
  // but can not actually do interrupt, the return code is OB_HASH_NOT_EXIST BUT overwrite by
  // OB_SUCCESS
  ret = g_manager->interrupt(local, interrupt_id, interrupt_code);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Downgrade to local interrupt test when remote interrupt is passed to local address
  g_manager->register_checker(checker, interrupt_id);
  ret = g_manager->interrupt(local, interrupt_id, interrupt_code);
  ASSERT_EQ(OB_SUCCESS, ret);

  r = checker->is_interrupted();
  ASSERT_EQ(true, r);

  // Test status clear
  checker->clear_status();
  r = checker->is_interrupted();
  ASSERT_EQ(false, r);

  g_manager->interrupt(interrupt_id, interrupt_code);
  r = checker->is_interrupted();
  ASSERT_EQ(true, r);

  // rpc call test
  checker->clear_status();
  ret = g_manager->interrupt(dst, interrupt_id, interrupt_code);
  ASSERT_EQ(OB_SUCCESS, ret);

  sleep(1);
  r = checker->is_interrupted();
  ASSERT_EQ(true, r);

  // Automatic logout of test destruction
  // unregister checker
  ret = g_manager->unregister_checker(checker, interrupt_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObInterruptCheckerNode *checker_node = nullptr;
  ret = g_manager->map_.get_refactored(interrupt_id, checker_node);
  EXPECT_EQ(OB_HASH_NOT_EXIST, ret);

  EXPECT_EQ(0, g_manager->map_.size());

  delete checker;
}

enum RegMode
{
  ManagerInterface = 0,
  CheckerInterface = 1,
  Guard = 2,
  SetInterruptable = 3,
};

static void mulit_reg_unreg(RegMode reg_mod) {
  int ret = OB_SUCCESS;
  ObRandom random;
  uint64_t slp_time = abs(random.get_int32()) % 100 + 1;
  ObInterruptibleTaskID interrupt_id(1, 1);
  ObInterruptChecker *checker = new ObInterruptChecker(false, 1);
  switch (reg_mod) {
    case RegMode::ManagerInterface: {
      ret = g_manager->register_checker(checker, interrupt_id);
      EXPECT_EQ(OB_SUCCESS, ret);
      ob_usleep(slp_time);
      ret = g_manager->unregister_checker(checker, interrupt_id);
      EXPECT_EQ(OB_SUCCESS, ret);
      break;
    }
    case RegMode::CheckerInterface: {
      ret = checker->register_checker(interrupt_id);
      ASSERT_EQ(OB_SUCCESS, ret);
      ob_usleep(slp_time);
      checker->unregister_checker(interrupt_id);
      break;
    }
    case RegMode::Guard: {
      sql::ObPxInterruptGuard px_int_guard(interrupt_id);
      break;
    }
    case RegMode::SetInterruptable: {
      ret = SET_INTERRUPTABLE(interrupt_id);
      ASSERT_EQ(OB_SUCCESS, ret);
      ob_usleep(slp_time);
      UNSET_INTERRUPTABLE(interrupt_id);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }

  delete checker;
}

TEST(ObGlobalInterruptManager, multi_thread_register_unregister)
{
  const int64_t thread_cnt = 2048;
  std::thread threads[thread_cnt];
  for (int64_t i = 0; i < thread_cnt; ++i) {
    RegMode reg_mod = static_cast<RegMode>(i % 4);
    threads[i] = std::thread(mulit_reg_unreg, reg_mod);
  }

  for (int i = 0; i < thread_cnt; ++i) {
    threads[i].join();
  }

  EXPECT_EQ(0, g_manager->map_.size());
}

static void thread_wait_interrupt(void *ready_thr_cnt)
{
  ObInterruptibleTaskID interrupt_id(2, 2);
  SET_INTERRUPTABLE(interrupt_id);
  std::atomic<int> *ready_cnt = static_cast<std::atomic<int>*> (ready_thr_cnt);
  ready_cnt->fetch_add(1);
  while (!IS_INTERRUPTED()) {
    ob_usleep(100);
  }
  UNSET_INTERRUPTABLE(interrupt_id);
}

TEST(ObGlobalInterruptManager, interrupt_all_threads)
{
  const int64_t thread_cnt = 64;
  std::thread threads[thread_cnt];
  std::atomic<int> ready_thr_cnt{0};
  for (int64_t i = 0; i < thread_cnt; ++i) {
    threads[i] = std::thread(thread_wait_interrupt, &ready_thr_cnt);
  }

  // waiting all threads ready
  while (ready_thr_cnt.load() != thread_cnt) {
    ob_usleep(100);
  }

  ObInterruptibleTaskID interrupt_id(2, 2);
  ObInterruptStackInfo interrupt_stack_info;
  ObAddr dst = g_service.get_dst();
  interrupt_stack_info.set_info(1 /*from tid*/, dst /*from_svr_addr*/,
                                "mock error" /*extra_msg*/);
  ObInterruptCode interrupt_code(OB_RPC_CONNECT_ERROR, interrupt_stack_info);

  g_manager->interrupt(dst, interrupt_id, interrupt_code);

  for (int i = 0; i < thread_cnt; ++i) {
    threads[i].join();
  }

  EXPECT_EQ(0, g_manager->map_.size());
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_interrupt_manager.log", true, true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
