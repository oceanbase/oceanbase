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

#ifndef OCENABASE_LIB_OB_RESOURCE_LIMITER_
#define OCENABASE_LIB_OB_RESOURCE_LIMITER_

#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace lib
{
class ObResourceLimiter
{
public:
  ObResourceLimiter(int64_t max, int64_t min);
  ~ObResourceLimiter();
  ObResourceLimiter* get_parent() const { return parent_; }
  void add_child(ObResourceLimiter *node);
  void del_child(ObResourceLimiter *node);
  bool has_child() const { return NULL != child_; }
  int64_t get_hold() const { return hold_; }
  int64_t get_used() const { return hold_ - cache_; }
  int64_t get_max() const { return max_; }
  int64_t get_min() const { return min_; }
  void set_max(int64_t max) { max_ = max; }
  void set_min(int64_t min)
  { 
    min_ = min;
    recalculate();
  }
  bool acquire(int64_t quota);
private:
  void recalculate();
private:
  int64_t max_;
  int64_t min_;
  int64_t hold_;
  int64_t cache_;
  ObMutex mutex_;
  ObResourceLimiter *parent_;
  ObResourceLimiter *child_;
  ObResourceLimiter *next_;
};

struct ObShareTenantLimiter
{
  ObShareTenantLimiter(int64_t tenant_id)
    : tenant_id_(tenant_id),
      limiter_(INT64_MAX, 0),
      next_(NULL)
  {}
  bool has_child() const { return limiter_.has_child(); }
  void set_max(int64_t max) { limiter_.set_max(max); }
  void add_child(ObResourceLimiter* node)
  {
    limiter_.add_child(node);
  }
  int64_t tenant_id_;
  ObResourceLimiter limiter_;
  ObShareTenantLimiter *next_;
};

} // end of namespace lib 
} // end of namespace oceanbase

#endif