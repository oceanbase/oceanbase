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

#ifndef OCEANBASE_COMMON_TEST_UTIL_
#define OCEANBASE_COMMON_TEST_UTIL_

#include "lib/thread/thread_pool.h"
/*
 * for testing code clips in multithread env. sample:
 *========================================
 * BEGIN_THREAD_CODE(MyClass, 10)
 * {
 *     static int i = 0;
 *     ASSERT_TRUE(__sync_fetch_and_add(i) > 0);
 * }
 * END_THREAD_CODE(MyClass);
 *=========================================
 */
#define BEGIN_THREAD_CODE(class_name, thread_count)             \
  class _##class_name : public oceanbase::lib::ThreadPool       \
  {                                                             \
public:                                                         \
_##class_name() { set_thread_count(thread_count); }             \
void run1() final {                                             \
                                                                \

#define END_THREAD_CODE(class_name) \
  }};\
  _##class_name my_##class_name; \
  my_##class_name.start();  my_##class_name.wait();

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define BAD(ass) ASSERT_NE(OB_SUCCESS, (ass))

#endif
