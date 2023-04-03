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

#ifndef _OB_COUNTER_H
#define _OB_COUNTER_H 1
#include <stdint.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/thread_local/ob_tsi_utils.h"
namespace oceanbase
{
namespace common
{
class ObSimpleCounter
{
public:
  ObSimpleCounter()
      :value_(0)
  {}
  ~ObSimpleCounter() = default;

  void inc(int64_t delta = 1) { ATOMIC_AAF(&value_, delta); }
  void dec(int64_t delta = 1) { ATOMIC_SAF(&value_, delta); }
  void set(int64_t v) { ATOMIC_STORE(&value_, v); }
  int64_t value() const { return ATOMIC_LOAD(&value_); }
private:
  int64_t value_;
};

template <int64_t SLOT_NUM, typename SlotPicker>
class ObCounter
{
public:
  ObCounter() { memset(items_, 0, sizeof(items_)); }
  ~ObCounter() {}

  void reset() { memset(items_, 0, sizeof(items_)); }

  void inc(int64_t delta = 1)
  {
    SlotPicker::add_value(&items_[SlotPicker::get_my_id() % SLOT_NUM].value_, delta);
  }
  void dec(int64_t delta = 1)
  {
    SlotPicker::add_value(&items_[SlotPicker::get_my_id() % SLOT_NUM].value_, -delta);
  }
  void set(int64_t v) { ATOMIC_STORE(&items_[SlotPicker::get_my_id() % SLOT_NUM].value_, v); }
  int64_t value() const
  {
    int64_t sum = 0;
    int64_t real_max = SlotPicker::get_max_id();
    int64_t valid_count = (real_max < SLOT_NUM) ? real_max : SLOT_NUM;
    for (int64_t i = 0; i != valid_count; ++i) {
      sum += ATOMIC_LOAD(&items_[i].value_);
    }
    return sum;
  }
private:
  struct Item
  {
    int64_t value_;
  } __attribute__ ((aligned (16)));
  Item items_[SLOT_NUM];
};

struct ObCounterSlotPickerByCPU
{
  static inline int64_t get_my_id() { return ::oceanbase::common::icpu_id(); }
  static inline int64_t get_max_id() { return ::oceanbase::common::get_max_icpu_id(); }
  static inline void add_value(int64_t *v, int64_t d) { (void) ATOMIC_FAA(v, d); }
};

struct ObCounterSlotPickerByThread
{
  static inline int64_t get_my_id() { return ::oceanbase::common::get_itid(); }
  static inline int64_t get_max_id() { return ::oceanbase::common::get_max_itid(); }
  static inline void add_value(int64_t *v, int64_t d) { (void) ATOMIC_FAA(v, d); }
};

struct ObCounterSlotPickerByCPUNonAtomic
{
  static inline int64_t get_my_id() { return ::oceanbase::common::icpu_id(); }
  static inline int64_t get_max_id() { return ::oceanbase::common::get_max_icpu_id(); }
  static inline void add_value(int64_t *v, int64_t d) { *v += d; }
};
static const int64_t OB_COUNTER_MAX_THREAD_NUM = 4096;
static const int64_t OB_COUNTER_MAX_CPU_NUM = 128;
typedef ObCounter<OB_COUNTER_MAX_THREAD_NUM, ObCounterSlotPickerByThread> ObTCCounter;
typedef ObCounter<OB_COUNTER_MAX_CPU_NUM, ObCounterSlotPickerByCPU> ObPCCounter;
typedef ObCounter<OB_COUNTER_MAX_CPU_NUM/4, ObCounterSlotPickerByCPUNonAtomic> ObPCNonAtomicCounter;

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_COUNTER_H */
