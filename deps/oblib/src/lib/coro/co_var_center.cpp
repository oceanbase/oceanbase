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

#include "co_var_center.h"
#include "lib/coro/co_routine.h"
#include "lib/coro/co_sched.h"

namespace oceanbase {
namespace lib {

#ifndef __OB_C_LIBC_FREE__
EXTERN_C_BEGIN
extern void* __libc_free(void*);
EXTERN_C_END
#endif

const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
__thread char* DEFAULT_CRLS = nullptr;
pthread_key_t default_crls_key = INVALID_THREAD_KEY;

void destroy_default_crls(void* ptr)
{
  __libc_free(ptr);
}

CoVarCenter::CoVarCenter()
{
  pthread_key_create(&default_crls_key, destroy_default_crls);
}

CoVarCenter::~CoVarCenter()
{
  auto& key = default_crls_key;
  if (key != INVALID_THREAD_KEY) {
    void* ptr = pthread_getspecific(key);
    destroy_default_crls(ptr);
    pthread_key_delete(key);
  }
}

/////////////////////////////////////////////////////
// Co-Routine Location Storage memory arrangement
//
// | CV   |
// | CV   |
// | ...  |
// | ...  |
// | HOOK |
// | HOOK |
// | NUM  |
//
// NUM is the number of hooks.
int CoVarCenter::register_hook(CoVarHook* hook)
{
  char* buffer = DEFAULT_CRLS;
  if (OB_LIKELY(CoSched::get_active_routine() != nullptr)) {
    buffer = CoSched::get_active_routine()->get_crls_buffer();
  }
  const auto MAX_CRLS_SIZE = coro::CoConfig::MAX_CRLS_SIZE;
  int64_t& num = reinterpret_cast<int64_t&>(*(&buffer[MAX_CRLS_SIZE] - 8));
  CoVarHook** last = reinterpret_cast<CoVarHook**>(&buffer[MAX_CRLS_SIZE] - 16);
  last[-num++] = hook;
  return static_cast<int>(num);
}

void CoVarCenter::at_routine_create()
{
  char* buffer = nullptr;
  if (OB_LIKELY(CoSched::get_active_routine() != nullptr)) {
    buffer = CoSched::get_active_routine()->get_crls_buffer();
    assert(buffer != nullptr);
  } else {
    ob_abort();
  }

  int64_t& num = reinterpret_cast<int64_t&>(*(&buffer[coro::CoConfig::MAX_CRLS_SIZE] - 8));
  num = 0;
}

void CoVarCenter::at_routine_exit()
{
  if (CoSched::get_active_routine() != CoSched::get_instance()) {
    char* buffer = nullptr;
    if (OB_LIKELY(CoSched::get_active_routine() != nullptr)) {
      buffer = CoSched::get_active_routine()->get_crls_buffer();
    }
    const auto MAX_CRLS_SIZE = coro::CoConfig::MAX_CRLS_SIZE;
    int64_t& num = reinterpret_cast<int64_t&>(*(&buffer[MAX_CRLS_SIZE] - 8));
    CoVarHook** last = reinterpret_cast<CoVarHook**>(&buffer[MAX_CRLS_SIZE] - 16);
    for (int64_t i = num - 1; i >= 0; i--) {
      last[-i]->deinit_();
    }
    num = 0;
  }
}

}  // namespace lib
}  // namespace oceanbase
