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

#ifndef SRC_LIB_UTILITY_OB_HANG_FATAL_ERROR_H_
#define SRC_LIB_UTILITY_OB_HANG_FATAL_ERROR_H_

#include <exception>
#include "lib/coro/co_var.h"

namespace oceanbase
{
namespace common
{
extern void right_to_die_or_duty_to_live();
int64_t get_fatal_error_thread_id();
void set_fatal_error_thread_id(int64_t thread_id);


RLOCAL_EXTERN(bool, in_try_stmt);

struct OB_BASE_EXCEPTION : public std::exception
{
  virtual const char *what() const throw() override { return nullptr; }
  virtual int get_errno() { return 0; }
};

template <int ERRNO>
struct OB_EXCEPTION : public OB_BASE_EXCEPTION
{
  virtual int get_errno() { return ERRNO; }
};

}
}

#endif /* SRC_LIB_UTILITY_OB_HANG_FATAL_ERROR_H_ */
