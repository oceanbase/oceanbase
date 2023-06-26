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

#ifndef OCEANBASE_OB_REF_MGR_
#define OCEANBASE_OB_REF_MGR_

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace common
{

class ObRef
{
public:
  ObRef() : ref_(0), lock_(0) {}
  void inc() {
    ATOMIC_INC(&ref_);
  }
  void dec() {
    ATOMIC_DEC(&ref_);
  }
  int32_t get_ref_cnt() const {
    return ATOMIC_LOAD(&ref_);
  }
  bool try_lock() {
    return ATOMIC_BCAS(&lock_, 0, 1);
  }
  void lock() {
    while (!ATOMIC_BCAS(&lock_, 0, 1));
  }
  void unlock() {
    ATOMIC_STORE(&lock_, 0);
  }
private:
  int32_t ref_;
  int32 lock_;
};

template<typename T>
class ObMultiModRefMgr
{
  static const int MAX_ENUM_VALUE = 64;
  static_assert(std::is_enum<T>::value, "typename must be a enum type");
  static_assert(static_cast<int>(T::TOTAL_MAX_MOD) < MAX_ENUM_VALUE,
                "Please add TOTAL_MAX_MOD in enum class");
public:
  ObMultiModRefMgr() : is_delete_(false) {}
public:
  struct ObMultiModRefInfo
  {
    ObRef ref_info_[static_cast<int>(T::TOTAL_MAX_MOD)];
  };
  // inc 需要在应用层保证安全，虽然这里检查了is_delete但是和inc操作并不原子
  // 依然会有is_delete之后，增加引用计数的可能，需要应用保证inc一定是在引用计数不为0下执行
  int inc(const T t) {
    int ret = OB_SUCCESS;
    const int mod = static_cast<int>(t);
    if (mod < 0 || mod >= static_cast<int>(T::TOTAL_MAX_MOD)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "inc ref", "type", typeid(T).name(), K(this), K(mod), K(ret));
    } else if (ATOMIC_LOAD(&is_delete_)) {
      ret = OB_NOT_RUNNING;
    } else {
      int32_t idx = (int32_t)(get_itid() % MAX_CPU_NUM);
      ref_mgr_[idx].ref_info_[mod].inc();
    }
    return ret;
  }

  bool dec(const T t)
  {
    bool can_release = false;
    const int mod = static_cast<int>(t);
    if (mod < 0 || mod >= static_cast<int>(T::TOTAL_MAX_MOD)) {
      COMMON_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "dec ref", "type", typeid(T).name(), K(this), K(mod));
    } else {
      int32_t idx = (int32_t)(get_itid() % MAX_CPU_NUM);
      // lock one slot
      ref_mgr_[idx].ref_info_[mod].lock();
      // dec one slot ref
      ref_mgr_[idx].ref_info_[mod].dec();
      if (ATOMIC_LOAD(&is_delete_) && get_total_ref_cnt() == 0) {
        ref_mgr_[idx].ref_info_[mod].inc();
        ref_mgr_[idx].ref_info_[mod].unlock();
        // lock all slot for confirm this dec is the last or not
        lock_all_slot();
        ref_mgr_[idx].ref_info_[mod].dec();
        if (get_total_ref_cnt() == 0) {
          can_release = true;
        } else {
          unlock_all_slot();
        }
      } else {
        ref_mgr_[idx].ref_info_[mod].unlock();
      }
    }
    return can_release;
  }
  int32_t get_total_ref_cnt() {
    int32_t ref_cnt = 0;
    for (int idx = 0; idx < MAX_CPU_NUM; idx++) {
      for (int mod = 0; mod < static_cast<int>(T::TOTAL_MAX_MOD); mod++) {
        ref_cnt += ref_mgr_[idx].ref_info_[mod].get_ref_cnt();
      }
    }
    return ref_cnt;
  }

  void set_delete() {
    ATOMIC_STORE(&is_delete_, true);
  }
  void print() {
    int tmp_ret = OB_SUCCESS;
    ObSqlString msg;
    for (int mod = 0; mod < static_cast<int>(T::TOTAL_MAX_MOD); mod++) {
      int32_t ref_cnt = 0;
      for (int idx = 0; idx < MAX_CPU_NUM; idx++) {
        ref_cnt += ref_mgr_[idx].ref_info_[mod].get_ref_cnt();
      }
      if (OB_SUCCESS != (tmp_ret = msg.append_fmt("%s%d:%d", mod>0?",":"", mod, ref_cnt))) {
        COMMON_LOG_RET(WARN, tmp_ret, "failed to add msg", K(mod), K(ref_cnt), K(msg));
      }
    }
    COMMON_LOG(INFO, "RefMgr mod ref", "type", typeid(T).name(), K(this), K(msg));
  }
private:
  void lock_all_slot() {
    for (int idx = 0; idx < MAX_CPU_NUM; idx++) {
      for (int mod = 0; mod < static_cast<int>(T::TOTAL_MAX_MOD); mod++) {
        while (!ref_mgr_[idx].ref_info_[mod].try_lock()) {
          sched_yield();
        }
      }
    }
  }
  void unlock_all_slot() {
    for (int idx = 0; idx < MAX_CPU_NUM; idx++) {
      for (int mod = 0; mod < static_cast<int>(T::TOTAL_MAX_MOD); mod++) {
        ref_mgr_[idx].ref_info_[mod].unlock();
      }
    }
  }
private:
  static const int64_t MAX_CPU_NUM = 64;
  ObMultiModRefInfo ref_mgr_[MAX_CPU_NUM];
  bool is_delete_;
};

} // end common
} // end oceanbase

#endif
