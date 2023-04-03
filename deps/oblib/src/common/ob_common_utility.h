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

#ifndef _OCEABASE_COMMON_OB_COMMON_UTILITY_H_
#define _OCEABASE_COMMON_OB_COMMON_UTILITY_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
extern const char *print_server_role(const common::ObServerRole server_role);

//@brief recursive function call should use this function to check if recursion is too deep
//to avoid stack overflow, default reserved statck size is 1M
extern int64_t get_reserved_stack_size();
extern void set_reserved_stack_size(int64_t reserved_size);
extern int check_stack_overflow(
    bool &is_overflow,
    int64_t reserved_stack_size = get_reserved_stack_size(),
    int64_t *used_size = nullptr);
extern int get_stackattr(void *&stackaddr, size_t &stacksize);
extern void set_stackattr(void *stackaddr, size_t stacksize);

// return OB_SIZE_OVERFLOW if stack overflow
inline int check_stack_overflow(void)
{
  bool overflow = false;
  int ret = check_stack_overflow(overflow);
  return OB_LIKELY(OB_SUCCESS == ret) && OB_UNLIKELY(overflow) ? OB_SIZE_OVERFLOW : ret;
}

/**
 * @brief The ObFatalErrExtraInfoGuard class is used for printing extra info, when fatal error happens.
 *        The of pointer of the class is maintained on thread local.
 */
class ObFatalErrExtraInfoGuard
{
public:
  explicit ObFatalErrExtraInfoGuard();
  virtual ~ObFatalErrExtraInfoGuard();
  static const ObFatalErrExtraInfoGuard *get_thd_local_val_ptr();
  DEFINE_VIRTUAL_TO_STRING();
private:
  static ObFatalErrExtraInfoGuard *&get_val();
  ObFatalErrExtraInfoGuard *last_;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEABASE_COMMON_OB_COMMON_UTILITY_H_ */
