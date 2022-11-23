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

#include "lib/coro/testing.h"
#include "share/interrupt/ob_interrupt_rpc_proxy.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "rpc/testing.h"

using namespace oceanbase::obrpc;

namespace oceanbase {
namespace common {

TEST(ObGlobalInterruptManager, normal)
{

  rpctesting::Service service;
  ObInterruptProcessor p;
  service.init();
  service.reg_processor(&p);
  ObInterruptRpcProxy proxy;
  service.get_proxy(proxy);

  int ret = OB_SUCCESS;
  int temp_id = 1;
  int icode = 1;
  ObAddr local(ObAddr::IPV4, "127.0.0.1", 10086);
  ObAddr dst = service.get_dst();

  /// Singleton acquisition
  ObGlobalInterruptManager *manager = ObGlobalInterruptManager::getInstance();
  ASSERT_EQ(false, nullptr == manager);

  ObInterruptChecker *checker = nullptr;
  /// Perform various operations without initialization, correct feedback

  ret = manager->interrupt(temp_id, icode);
  ASSERT_EQ(OB_NOT_INIT, ret);

  ret = manager->interrupt(local, temp_id, icode);
  ASSERT_EQ(OB_NOT_INIT, ret);

  ret = manager->register_checker(checker);
  ASSERT_EQ(OB_NOT_INIT, ret);

  ret = manager->unregister_checker(checker);
  ASSERT_EQ(OB_NOT_INIT, ret);

  /// No RPC initialization is illegal
  ret = manager->init(local, nullptr);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  /// Regular initialization
  ret = manager->init(local, &proxy);
  ASSERT_EQ(OB_SUCCESS, ret);

  /// Initialize correctly and feedback error many times
  ret = manager->init(local, &proxy);
  ASSERT_EQ(OB_INIT_TWICE, ret);

  ret = manager->register_checker(checker);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = manager->unregister_checker(checker);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  /// Get the inspector
  checker = new ObInterruptChecker(true, temp_id);
  /// Test self-registration
  ret = checker->register_checker(temp_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  /// Test duplicate registration
  ret = checker->register_checker(temp_id);
  ASSERT_EQ(OB_INIT_TWICE, ret);
  ret = checker->register_checker(temp_id + 1);
  ASSERT_EQ(OB_INIT_TWICE, ret);
  ret = manager->register_checker(checker);
  ASSERT_EQ(OB_INIT_TWICE, ret);

  /// Semaphore detection without interruption
  bool r = checker->is_interrupted();
  ASSERT_EQ(false, r);

  /// Local execution interrupt check semaphore
  ret = manager->interrupt(temp_id, icode);
  ASSERT_EQ(OB_SUCCESS, ret);

  r = checker->is_interrupted();
  ASSERT_EQ(true, r);

  /// Release checker
  ret = manager->unregister_checker(checker);
  ASSERT_EQ(OB_SUCCESS, ret);

  /// The interrupt can still be called after the checker is released
  ret = manager->interrupt(local, temp_id, icode);
  ASSERT_EQ(OB_SUCCESS, ret);

  /// Downgrade to local interrupt test when remote interrupt is passed to local address
  manager->register_checker(checker);
  ret = manager->interrupt(local, temp_id, icode);
  ASSERT_EQ(OB_SUCCESS, ret);

  r = checker->is_interrupted();
  ASSERT_EQ(true, r);

  // Test status clear
  checker->clear_status();
  r = checker->is_interrupted();
  ASSERT_EQ(false, r);

  manager->interrupt(temp_id, icode);
  r = checker->is_interrupted();
  ASSERT_EQ(true, r);

  /// rpc call test
  checker->clear_status();
  ret = manager->interrupt(dst, temp_id, icode);
  ASSERT_EQ(OB_SUCCESS, ret);

  r = checker->is_interrupted();
  ASSERT_EQ(true, r);



  //Automatic logout of test destruction
  delete checker;

  ret = manager->map_.get_refactored(temp_id, checker);
  EXPECT_EQ(OB_HASH_NOT_EXIST, ret);

  // Test group registration, interruption and deregistration
  ObInterruptChecker *checker0 = new ObInterruptChecker(true, 1);
  ObInterruptChecker *checker1 = new ObInterruptChecker(true, 1);
  ObInterruptChecker *checker2 = new ObInterruptChecker(true, 1);
  ObInterruptChecker *checker3 = new ObInterruptChecker(true, 1);
  ret = manager->register_checker(checker0);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = manager->register_checker(checker1);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = manager->register_checker(checker2);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = manager->register_checker(checker3);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = manager->unregister_checker(checker0);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = manager->unregister_checker(checker1);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = manager->unregister_checker(checker2);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = manager->unregister_checker(checker3);
  EXPECT_EQ(OB_SUCCESS, ret);
  delete checker0;
  delete checker1;
  delete checker2;
  delete checker3;


  // The following is in coroutine mode
  // Simulate regular usage scenarios
  // And test the use of several global functions

  int total = -1;
  int fail = 0;
  int ready = 0;
  uint64_t n[40];

  auto apool = cotesting::FlexPool {
      [&] {
        int idx = ATOMIC_AAF(&total, 1);
        uint64_t cid = CO_ID();
        n[idx] = cid;
        SET_INTERRUPTABLE(cid);
        /// The first 39 coroutines are waiting to be terminated
        if (idx < 39) {
          while (ready == 0) {
            ::usleep(1000);
          }
          if (IS_INTERRUPTED()) {
            ASSERT_EQ(idx + 10, GET_INTERRUPT_CODE());
            return;
          }
          fail++;
        }
        /// The 40th coroutine is responsible for terminating them with three scenarios respectively
        else {
          for (int i = 0; i < 39; i++) {
            int k = i % 3;
            if (k == 0)
              manager->interrupt(n[i], i + 10);
            else if (k == 1)
              manager->interrupt(local, n[i], i + 10);
            else
              manager->interrupt(dst, n[i], i + 10);
          }
          ASSERT_EQ(false, IS_INTERRUPTED());
          ready++;
        }
      },
      4, 10};
  apool.start(true);
  ASSERT_EQ(0, fail);

  int xcnt = 0;
  // Test group interrupted
  SET_INTERRUPTABLE(10000);

  auto bpool = cotesting::FlexPool{
      [&] {
        SET_INTERRUPTABLE(10000);
        ATOMIC_INC(&xcnt);
        while (!IS_INTERRUPTED()) {
          CO_YIELD();
        }
        EXPECT_EQ(1, GET_INTERRUPT_CODE());
        ATOMIC_DEC(&xcnt);
      },
      10, 10};
  bpool.start(false);
  // Wait for all coroutines to execute set_interruptable
  while (xcnt != 100) {};

  // Execute interrupt for taskid = 10000
  manager->interrupt(10000, 1);
  bpool.wait();
  // Coroutine interrupt count is cleared to 0
  ASSERT_EQ(0, xcnt);
  // The current thread also receives an interrupt signal
  ASSERT_EQ(true, IS_INTERRUPTED());
  ASSERT_EQ(1, GET_INTERRUPT_CODE());
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
