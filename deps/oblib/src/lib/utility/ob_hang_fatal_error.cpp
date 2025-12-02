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

#define USING_LOG_PREFIX LIB
#include "lib/utility/ob_hang_fatal_error.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_common_utility.h"
#include "lib/oblog/ob_log.h"

extern "C" {
void right_to_die_or_duty_to_live_c()
{
  ::oceanbase::common::right_to_die_or_duty_to_live();
}
}

namespace oceanbase
{
namespace common
{
_RLOCAL(bool, in_try_stmt);
_RLOCAL(bool, in_exception_state) = false;
int64_t g_fatal_error_thread_id = -1;

class ObCaptureMemoryArray
{
public:
  struct Info
  {
    void *mem_ptr;
    int64_t mem_len;
    int64_t thread_id;
    TO_STRING_KV(KP(mem_ptr), K(mem_len), K(thread_id));
  };

  void add(void *mem_ptr, int64_t mem_len, int64_t thread_id)
  {
    array_[current_index_].mem_ptr = mem_ptr;
    array_[current_index_].mem_len = mem_len;
    array_[current_index_].thread_id = thread_id;
    current_index_ = (current_index_ + 1) % ARRAY_SIZE;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_ARRAY_START();
    bool first = true;
    for (int64_t i = 0; i < ARRAY_SIZE; ++i) {
      const Info &info = array_[i];
      if (info.mem_ptr != NULL || info.mem_len != 0 || info.thread_id != 0) {
        if (!first) {
          J_COMMA();
        }
        J_OBJ_START();
        J_KV(K(i), K(info));
        J_OBJ_END();
        first = false;
      }
    }
    J_ARRAY_END();
    return pos;
  }

  uint64_t current_index() const
  {
    return current_index_;
  }

private:
  static const int64_t ARRAY_SIZE = 64;
  Info array_[ARRAY_SIZE];
  uint64_t current_index_ = 0;
};

static ObCaptureMemoryArray g_capture_memory_array;

int64_t get_fatal_error_thread_id()
{
  return g_fatal_error_thread_id;
}
void set_fatal_error_thread_id(int64_t thread_id)
{
  g_fatal_error_thread_id = thread_id;
}

void add_capture_memory_info(void *mem_ptr, int64_t mem_len)
{
  g_capture_memory_array.add(mem_ptr, mem_len, GETTID());
}

void dump_capture_memory_info()
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_ERROR("dump capture memory array:", K(g_capture_memory_array));
}

void print_capture_memory_info()
{
  int ret = OB_SUCCESS;
  if (g_capture_memory_array.current_index() > 0) {
    LOG_INFO("dump capture memory array:", K(g_capture_memory_array));
  }
}
// To die or to live, it's a problem.
void right_to_die_or_duty_to_live()
{
  const ObFatalErrExtraInfoGuard *extra_info = ObFatalErrExtraInfoGuard::get_thd_local_val_ptr();
  set_fatal_error_thread_id(GETTID());
  while (true) {
    const char *info = (NULL == extra_info) ? NULL : to_cstring(*extra_info);
    LOG_DBA_ERROR(OB_ERR_THREAD_PANIC, "msg", "Trying so hard to die", KCSTRING(info), KCSTRING(lbt()));
  #ifndef FATAL_ERROR_HANG
    if (in_try_stmt) {
      in_exception_state = true;
      throw OB_EXCEPTION<OB_ERR_UNEXPECTED>();
    }
  #endif
    sleep(60);
  }
}

} //common
} //oceanbase
