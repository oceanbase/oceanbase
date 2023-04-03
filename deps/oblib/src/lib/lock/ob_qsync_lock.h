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

#ifndef OCEANBASE_SHARE_LOCK_QSYNC_LOCK_H
#define OCEANBASE_SHARE_LOCK_QSYNC_LOCK_H

#include "lib/allocator/ob_qsync.h"

namespace oceanbase
{
namespace common
{

class ObQSyncLock
{
public:
  ObQSyncLock() : write_flag_(0) {}
  ~ObQSyncLock() {}
  int init(const lib::ObMemAttr &mem_attr);
  bool is_inited() const { return qsync_.is_inited(); }
  void destroy();
  int rdlock();
  void rdunlock();
  int wrlock();
  void wrunlock();
  int try_rdlock();
private:
  static const int64_t TRY_SYNC_COUNT = 16;
private:
  int64_t write_flag_ CACHE_ALIGNED;
  common::ObDynamicQSync qsync_;
};

class ObQSyncLockWriteGuard
{
public:
  ObQSyncLockWriteGuard(ObQSyncLock &lock) : lock_(lock) {
    lock_.wrlock();
  }
  ~ObQSyncLockWriteGuard() {
    lock_.wrunlock();
  }
private:
  ObQSyncLock &lock_;
};

class ObQSyncLockReadGuard
{
public:
  ObQSyncLockReadGuard(ObQSyncLock &lock) : lock_(lock) {
    lock_.rdlock();
  }
  ~ObQSyncLockReadGuard() {
    lock_.rdunlock();
  }
private:
  ObQSyncLock &lock_;
};


}
}

#endif
