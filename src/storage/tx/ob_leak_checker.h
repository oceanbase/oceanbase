/**
 * Copyright (c) 2021, 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TRANSACTION_OB_LEAK_CHECKER_
#define OCEANBASE_TRANSACTION_OB_LEAK_CHECKER_

#include <stdint.h>
#include <sys/types.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace transaction
{

class ObLeakChecker
{
private:
  class Item
  {
  public:
    Item() : lock_(0), tenant_id_(0), key_(0), arg1_(0), arg2_(0), tid_(0),
             start_ts_(0), max_dur_ts_(INT64_MAX), is_concerned_(false) { mod_name_[0] = '\0'; }
    void set(const uint64_t key, const uint64_t arg1, const uint64_t arg2, const char *mod_name,
        const int64_t max_dur_ts)
    {
      if (0 == key_) {
        tenant_id_ = MTL_ID();
        key_ = key;
        arg1_ = arg1;
        arg2_ = arg2;
        if (NULL != mod_name) {
          strncpy(mod_name_, mod_name, sizeof(mod_name_));
          mod_name_[sizeof(mod_name_) - 1] = '\0';
        }
        tid_ = gettid();
        start_ts_ = ObTimeUtility::current_time();
        max_dur_ts_ = max_dur_ts;
        is_concerned_ = false;
        const ObCurTraceId::TraceId trace_id = *(ObCurTraceId::get_trace_id());
        trace_id_.set(trace_id);
      }
    }
    void unset(const uint64_t key)
    {
      if (key == key_) {
        if (is_concerned()) {
          TRANS_LOG(INFO, "unset leak checker item which is concerned", K(*this));
        }
        key_ = 0;
      }
    }
    Item &operator=(const Item &item)
    {
      if (this != &item) {
        lock_ = 0;
        tenant_id_ = item.tenant_id_;
        key_ = item.key_;
        arg1_ = item.arg1_;
        arg2_ = item.arg2_;
        tid_ = item.tid_;
        start_ts_ = item.start_ts_;
        max_dur_ts_ = item.max_dur_ts_;
        strncpy(mod_name_, item.mod_name_, sizeof(mod_name_));
        is_concerned_ = item.is_concerned_;
        trace_id_ = item.trace_id_;
      }
      return *this;
    }
    bool is_equal(const Item &item) const
    {
      return tenant_id_ == item.tenant_id_ && key_ == item.key_;
    }
    void set_concerned() { is_concerned_ = true; }
    bool is_concerned() const { return is_concerned_; }
    bool is_valid() const { return key_ != 0; }
    int64_t get_dur_ts() const { return is_valid() ? (ObTimeUtility::current_time() - start_ts_) : 0; }
    int64_t get_max_dur_ts() const { return max_dur_ts_; }
    TO_STRING_KV(K_(tenant_id), K_(key), K_(arg1), K_(arg2), K_(tid), K_(start_ts), "dur_ts", get_dur_ts(),
        K_(max_dur_ts), K_(mod_name), K_(trace_id), K_(is_concerned));
  public:
    void lock()
    {
      while (true) {
        if (ATOMIC_BCAS(&lock_, 0, 1)) {
          break;
        }
        PAUSE();
      }
    }
    void unlock()
    {
      ATOMIC_SET(&lock_, 0);
    }
  private:
    int64_t lock_;
    int64_t tenant_id_;
    uint64_t key_;
    uint64_t arg1_;
    uint64_t arg2_;
    int64_t tid_;
    int64_t start_ts_;
    int64_t max_dur_ts_;
    char mod_name_[15];
    bool is_concerned_;
    ObCurTraceId::TraceId trace_id_;
  } CACHE_ALIGNED;
public:
  static ObLeakChecker &get_instance()
  {
    static ObLeakChecker instance_;
    return instance_;
  }
  static void reg(const uint64_t key, const uint64_t arg1, const uint64_t arg2,
      const char *mod_name, const int64_t max_dur_ts = 60 * 1000 * 1000)
  {
#ifdef ENABLE_DEBUG_LOG
    ObLeakChecker &checker = ObLeakChecker::get_instance();
    checker.do_reg(key, arg1, arg2, mod_name, max_dur_ts);
#else
    UNUSED(key);
    UNUSED(arg1);
    UNUSED(arg2);
    UNUSED(mod_name);
    UNUSED(max_dur_ts);
#endif
  }
  static void unreg(const uint64_t key)
  {
#ifdef ENABLE_DEBUG_LOG
    ObLeakChecker &checker = ObLeakChecker::get_instance();
    checker.do_unreg(key);
#else
    UNUSED(key);
#endif
  }
  static void dump()
  {
#ifdef ENABLE_DEBUG_LOG
    ObLeakChecker &checker = ObLeakChecker::get_instance();
    checker.do_dump();
#endif
  }
public:
  void do_reg(const uint64_t key, const uint64_t arg1, const uint64_t arg2,
      const char *mod_name, const int64_t max_dur_ts)
  {
    const uint64_t idx = get_hash_value(key) % MAX_ITEM;
    Item &item = items_[idx];
    item.lock();
    item.set(key, arg1, arg2, mod_name, max_dur_ts);
    item.unlock();
  }
  void do_unreg(const uint64_t key)
  {
    const uint64_t idx = get_hash_value(key) % MAX_ITEM;
    Item &item = items_[idx];
    item.lock();
    item.unset(key);
    item.unlock();
  }
  void do_dump()
  {
    ObTimeGuard tg("dump_leak_checker");
    int64_t suspect_item_cnt = 0;
    Item oldest_items[MAX_DUMP_CNT];
    Item newest_items[MAX_DUMP_CNT];
    for (int64_t i = 0; i < MAX_ITEM; i++) {
      Item &item = items_[i];
      item.lock();
      if (item.get_dur_ts() > item.get_max_dur_ts()) {
        const int64_t oldest_idx = get_old_item_idx(oldest_items, MAX_DUMP_CNT);
        const int64_t newest_idx = get_new_item_idx(newest_items, MAX_DUMP_CNT);
        if ((!oldest_items[oldest_idx].is_valid()) || oldest_items[oldest_idx].get_dur_ts() < item.get_dur_ts()) {
          item.set_concerned();
          oldest_items[oldest_idx] = item;
        }
        if ((!newest_items[newest_idx].is_valid()) || newest_items[newest_idx].get_dur_ts() > item.get_dur_ts()) {
          item.set_concerned();
          newest_items[newest_idx] = item;
        }
        suspect_item_cnt++;
      }
      item.unlock();
    }
    for (int64_t i = 0; i < MAX_DUMP_CNT; i++) {
      if (!oldest_items[i].is_valid()) {
        break;
      } else {
        TRANS_LOG(INFO, "leak checker, dump oldest item", K(oldest_items[i]));
      }
    }
    for (int64_t i = 0; i < MAX_DUMP_CNT; i++) {
      if (!newest_items[i].is_valid()) {
        break;
      } else {
        bool need_dump = true;
        for (int64_t j = 0; need_dump && j < MAX_DUMP_CNT; j++) {
          if (newest_items[i].is_equal(oldest_items[j])) {
            need_dump = false;
          }
        }
        if (need_dump) {
          TRANS_LOG(INFO, "leak checker, dump newest item", K(newest_items[i]));
        }
      }
    }
    TRANS_LOG(INFO, "finish dump leak checker", K(suspect_item_cnt), K(tg));
  }
private:
  int64_t get_new_item_idx(const Item *items, const int64_t item_cnt)
  {
    int64_t idx = 0;
    for (int64_t i = 0; i < item_cnt; i++) {
      if (!items[i].is_valid()) {
        idx = i;
        break;
      } else {
        if (items[idx].get_dur_ts() < items[i].get_dur_ts()) {
          idx = i;
        }
      }
    }
    return idx;
  }
  int64_t get_old_item_idx(const Item *items, const int64_t item_cnt)
  {
    int64_t idx = 0;
    for (int64_t i = 0; i < item_cnt; i++) {
      if (!items[i].is_valid()) {
        idx = i;
        break;
      } else {
        if (items[idx].get_dur_ts() > items[i].get_dur_ts()) {
          idx = i;
        }
      }
    }
    return idx;
  }
  uint64_t get_hash_value(const uint64_t key)
  {
    return murmurhash(&key, sizeof(key), 0);
  }
private:
#ifdef ENABLE_DEBUG_LOG
  static const int64_t MAX_ITEM = 100000;
#else
  static const int64_t MAX_ITEM = 1;
#endif
  static const int64_t MAX_DUMP_CNT = 8;
  Item items_[MAX_ITEM];
};

}
}//end of namespace oceanbase

#endif // OCEANBASE_TRANSACTION_OB_LEAK_CHECKER_
