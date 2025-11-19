/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_TYPE_OB_KMEANS_MONITOR_H
#define SRC_SHARE_VECTOR_TYPE_OB_KMEANS_MONITOR_H

#include <cstring>
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
namespace share {

// Union structure for packing kmeans iteration info into a single int64_t
struct ObKmeansIterInfo {
  union {
    int64_t packed_value;
    struct {
      int64_t now_iter_ : 10;       // 10 bits, can represent 0-1023
      int64_t finish_diff_int_ : 17; // 17 bits, can represent 0-131071
      int64_t now_diff_int_ : 17;    // 17 bits, can represent 0-131071
      int64_t reserved : 20;         // reserved bits, total 64 bits
    } fields;
  };

  ObKmeansIterInfo() : packed_value(0) {}

  void set_kmeans_iter_info(int64_t now_iter, int64_t finish_diff, int64_t now_diff) {
    fields.now_iter_ = now_iter & 0x3FF;             // 10 bits mask
    fields.finish_diff_int_ = finish_diff & 0x1FFFF; // 17 bits mask
    fields.now_diff_int_ = now_diff & 0x1FFFF;       // 17 bits mask
  }

  static void get_kmeans_iter_info(const int64_t &packed_value, int64_t &now_iter, int64_t &finish_diff, int64_t &now_diff) {
    ObKmeansIterInfo info;
    info.packed_value = packed_value;
    now_iter = info.fields.now_iter_;
    finish_diff = info.fields.finish_diff_int_;
    now_diff = info.fields.now_diff_int_;
  }
};

struct ObKmeansMonitor {
  ObKmeansMonitor() {
    memset(this, 0, sizeof(*this));
  }
  static int64_t diff_float_to_int(float diff) { return static_cast<int64_t>(diff * DIFF_FIXED_POINT_FACTOR); }
  static float diff_int_to_float(int64_t diff_int) { return static_cast<float>(diff_int / DIFF_FIXED_POINT_FACTOR); }
  static int64_t imbalance_float_to_int(float imbalance_factor)
  {
    return static_cast<int64_t>(imbalance_factor * IMPALANCE_FIXED_POINT_FACTOR);
  }
  static float imbalance_int_to_float(int64_t imbalance_int) { return static_cast<float>(imbalance_int / IMPALANCE_FIXED_POINT_FACTOR); }
  void set_kmeams_monitor(int64_t now_iter, float finish_diff, float now_diff, float imbalance_factor) {
    if (OB_UNLIKELY(OB_NOT_NULL(kmeans_iter_info_value_))) {
      kmeans_iter_info_->set_kmeans_iter_info(now_iter, diff_float_to_int(finish_diff), diff_float_to_int(now_diff));
      (void)ATOMIC_SET(kmeans_iter_info_value_, kmeans_iter_info_->packed_value);
    }
    if (OB_UNLIKELY(OB_NOT_NULL(imbalance_factor_int_))) {
      (void)ATOMIC_SET(imbalance_factor_int_, imbalance_float_to_int(imbalance_factor));
    }
  }
  void add_finish_tablet_cnt() {
    if (OB_UNLIKELY(OB_NOT_NULL(finish_tablet_cnt_))) {
      (void)ATOMIC_AAF(finish_tablet_cnt_, 1);
    }
  }
  int64_t *finish_tablet_cnt_;
  union {
    ObKmeansIterInfo *kmeans_iter_info_; // for now_iter_,finish_diff_int_,now_diff_int_
    int64_t *kmeans_iter_info_value_;
    int64_t *vec_index_task_thread_pool_cnt_;
  };
  union {
    int64_t *imbalance_factor_int_;
    int64_t *vec_index_task_total_cnt_;
  };

  union {
    int64_t *vec_index_task_finish_cnt_;
  };

  static constexpr float DIFF_FIXED_POINT_FACTOR = 1e5F;
  static constexpr float IMPALANCE_FIXED_POINT_FACTOR = 1e2F;
};

} // namespace share
} // namespace oceanbase

#endif // SRC_SHARE_VECTOR_TYPE_OB_KMEANS_MONITOR_H
