/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
