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

#ifndef OCEANBASE_COMMON_RECURSION_H_
#define OCEANBASE_COMMON_RECURSION_H_

namespace oceanbase
{
namespace common
{

#ifdef DETECT_RECURSION
class RecursionCheckerGuard
{
public:
  RecursionCheckerGuard();
  ~RecursionCheckerGuard();
public:
  constexpr static int max_cap = 256;
  static thread_local void** tl_func;
  static thread_local int    tl_n_func;
private:
  void**           func_bak_;
  int              n_func_bak_;
  void*            func_[max_cap];
};
#define RECURSION_CHECKER_GUARD RecursionCheckerGuard guard
#else
#define RECURSION_CHECKER_GUARD
#endif

} // end namespace common
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_RECURSION_H_
