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

#include "lib/queue/ob_lighty_queue.h"
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "lib/utility/utility.h"

namespace oceanbase {
namespace common {
static int64_t get_us()
{
  return ::oceanbase::common::ObTimeUtility::current_time();
}

int LightyQueue::init(const uint64_t capacity, const lib::ObLabel& label)
{
  return queue_.init(capacity, global_default_allocator, label);
}

int LightyQueue::push(void* data, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout = (timeout > 0 ? (get_us() + timeout) : 0);
  int64_t wait_timeout = 0;
  while (true) {  // WHITESCAN: OB_CUSTOM_ERROR_COVERED
    uint32_t seq = cond_.get_seq();
    if (OB_SUCCESS == (ret = queue_.push(data))) {
      break;
    } else if (timeout <= 0 || (wait_timeout = abs_timeout - get_us()) <= 0) {
      break;
    } else {
      cond_.wait(seq, wait_timeout);
    }
  }
  if (OB_SUCCESS == ret) {
    cond_.signal();
  }
  return ret;
}

int LightyQueue::pop(void*& data, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout = (timeout > 0 ? (get_us() + timeout) : 0);
  int64_t wait_timeout = 0;
  while (true) {  // WHITESCAN: OB_CUSTOM_ERROR_COVERED
    uint32_t seq = cond_.get_seq();
    if (OB_SUCCESS == (ret = queue_.pop(data))) {
      break;
    } else if (timeout <= 0 || (wait_timeout = abs_timeout - get_us()) <= 0) {
      break;
    } else {
      cond_.wait(seq, wait_timeout);
    }
  }
  if (OB_SUCCESS == ret) {
    cond_.signal();
  }
  return ret;
}

int LightyQueue::multi_pop(void** data, const int64_t data_count, int64_t& avail_count, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  avail_count = 0;
  if (data_count > 0) {
    void* curr_data = NULL;
    int64_t abs_timeout = (timeout > 0 ? (get_us() + timeout) : 0);
    int64_t wait_timeout = 0;
    while (true) {  // WHITESCAN: OB_CUSTOM_ERROR_COVERED
      uint32_t seq = cond_.get_seq();
      if (OB_SUCCESS == (ret = queue_.pop(curr_data))) {
        data[avail_count++] = curr_data;
        curr_data = NULL;
        cond_.signal();
        if (avail_count >= data_count) {
          // finish then break
          break;
        }
      } else if (avail_count > 0) {
        // not finish, but has already got one, break
        ret = OB_SUCCESS;
        break;
      } else if (timeout <= 0 || (wait_timeout = abs_timeout - get_us()) <= 0) {
        break;
      } else {
        cond_.wait(seq, wait_timeout);
      }
    }
  }
  return ret;
}

void LightyQueue::reset()
{
  void* p = NULL;
  while (0 == pop(p, 0))
    ;
}
};  // end namespace common
};  // end namespace oceanbase
