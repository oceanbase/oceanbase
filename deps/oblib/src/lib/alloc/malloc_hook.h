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

#ifndef MALLOC_HOOK_H
#define MALLOC_HOOK_H
#include "lib/alloc/alloc_struct.h"
extern void init_malloc_hook();

inline bool& in_hook()
{
  thread_local bool in_hook = false;
  return in_hook;
}
namespace oceanbase
{
namespace lib
{
class ObMallocHookAttrGuard
{
public:
  ObMallocHookAttrGuard(ObMemAttr& attr)
   : old_attr_(tl_mem_attr)
  {
    tl_mem_attr = attr;
  }
  ~ObMallocHookAttrGuard()
  {
    tl_mem_attr = old_attr_;
  }
  static ObMemAttr get_tl_mem_attr()
  {
    return tl_mem_attr;
  }
private:
  static thread_local ObMemAttr tl_mem_attr;
  ObMemAttr old_attr_;
};
}
}

#endif /* MALLOC_HOOK_H */
