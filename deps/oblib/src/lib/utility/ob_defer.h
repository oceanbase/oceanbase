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

#ifndef OCEANBASE_COMMON_OB_DEFER_H_
#define OCEANBASE_COMMON_OB_DEFER_H_

#include <functional>

namespace oceanbase
{
namespace common
{
// C++ implementation of golang defer keyword, the concept of delayed execution, often used for resource release
template <typename FnType>
class ScopedLambda {
 public:
  explicit ScopedLambda(FnType fn) : fn_(std::move(fn)), active_(true) { }
  // Default movable.
  ScopedLambda(ScopedLambda&&) = default;
  ScopedLambda& operator=(ScopedLambda&&) = default;
  // Non-copyable. In particular, there is no good reasoning about which copy
  // remains active.
  ScopedLambda(const ScopedLambda&) = delete;
  ScopedLambda& operator=(const ScopedLambda&) = delete;
  ~ScopedLambda() {
    if (active_) fn_();
  }
  void run_and_expire() {
    if (active_) fn_();
    active_ = false;
  }
  void activate() { active_ = true; }
  void deactivate() { active_ = false; }

 private:
  FnType fn_;
  bool active_ = true;
};

template <typename FnType>
ScopedLambda<FnType> MakeScopedLambda(FnType fn) {
  return ScopedLambda<FnType>(std::move(fn));
}

#define TOKEN_PASTE(x, y) x ## y
#define TOKEN_PASTE2(x, y) TOKEN_PASTE(x, y)
#define SCOPE_UNIQUE_NAME(name) TOKEN_PASTE2(name, __LINE__)
#define NAMED_DEFER_X(name, C , ...) \
    auto name = common::MakeScopedLambda([C]() mutable { __VA_ARGS__; })
#define NAMED_DEFER(name, ...) NAMED_DEFER_X(name, &, __VA_ARGS__)
#define DEFER(...) \
    NAMED_DEFER_X(SCOPE_UNIQUE_NAME(defer_varname), &, __VA_ARGS__)
#define DEFER_C(...) \
    NAMED_DEFER_X(SCOPE_UNIQUE_NAME(defer_varname), =, __VA_ARGS__)
} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_DEFER_H_
