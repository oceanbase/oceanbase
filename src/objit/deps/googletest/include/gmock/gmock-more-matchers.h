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

#ifndef GMOCK_GMOCK_MORE_MATCHERS_H_
#define GMOCK_GMOCK_MORE_MATCHERS_H_

#include "gmock/gmock-generated-matchers.h"

namespace testing {

// Defines a matcher that matches an empty container. The container must
// support both size() and empty(), which all STL-like containers provide.
MATCHER(IsEmpty, negation ? "isn't empty" : "is empty") {
  if (arg.empty()) {
    return true;
  }
  *result_listener << "whose size is " << arg.size();
  return false;
}

}  // namespace testing

#endif  // GMOCK_GMOCK_MORE_MATCHERS_H_
