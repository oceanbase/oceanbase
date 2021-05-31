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

#ifndef OCEANBASE_OB_LOG_UTILS_H__
#define OCEANBASE_OB_LOG_UTILS_H__

#include "lib/container/ob_array.h"  // ObArray
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
using namespace common;
using namespace common::array;
namespace clog {

// bool cmp(const Type1 &a, const Type2 &b);
// -  if it returns true, means a < b
template <class T, class CompareFunc>
int top_k(
    const common::ObArray<T>& in_array, const int64_t k_num, common::ObArray<T>& out_array, CompareFunc& compare_func)
{
  int ret = common::OB_SUCCESS;
  int64_t array_cnt = in_array.count();
  int64_t cnt = std::min(k_num, array_cnt);

  for (int64_t idx = 0; common::OB_SUCCESS == ret && idx < cnt; ++idx) {
    if (OB_FAIL(out_array.push_back(in_array.at(idx)))) {
      CLOG_LOG(ERROR, "push back into slow array fail", K(ret), K(idx));
    } else {
      // do nothing
    }
  }

  if (common::OB_SUCCESS == ret && array_cnt > 0) {
    if (array_cnt <= k_num) {
      std::make_heap(out_array.begin(), out_array.end(), compare_func);
    } else {
      std::make_heap(out_array.begin(), out_array.end(), compare_func);

      for (int64_t idx = k_num; common::OB_SUCCESS == ret && idx < array_cnt; ++idx) {
        if (compare_func(in_array.at(idx), out_array.at(0))) {
          out_array[0] = in_array.at(idx);
          std::make_heap(out_array.begin(), out_array.end(), compare_func);
        } else {
          // do nothing
        }
      }  // for
    }

    if (common::OB_SUCCESS == ret) {
      std::sort_heap(out_array.begin(), out_array.end(), compare_func);
    }
  }

  return ret;
}

}  // namespace clog
}  // namespace oceanbase

#endif
