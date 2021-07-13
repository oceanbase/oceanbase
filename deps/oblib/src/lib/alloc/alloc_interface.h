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

#ifndef _ALLOC_INTERFACE_H_
#define _ALLOC_INTERFACE_H_

#include <stdint.h>
#include <cstdlib>
#include <cstddef>
#include "lib/lock/ob_mutex.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
namespace lib {
class ObTenantCtxAllocator;
class IBlockMgr {
public:
  virtual ABlock* alloc_block(uint64_t size, const ObMemAttr& attr) = 0;
  virtual void free_block(ABlock* block) = 0;
  virtual ObTenantCtxAllocator& get_tenant_ctx_allocator() = 0;
};  // end of class IBlockMgr

class ISetLocker {
public:
  virtual void lock() = 0;
  virtual void unlock() = 0;
  virtual bool trylock() = 0;
};

class SetDoNothingLocker : public ISetLocker {
public:
  void lock() override
  {}
  void unlock() override
  {}
  bool trylock() override
  {
    return true;
  }
};

class SetLocker : public ISetLocker {
public:
  SetLocker(lib::ObMutex& mutex) : mutex_(mutex)
  {}
  void lock() override
  {
    mutex_.lock();
  }
  void unlock() override
  {
    mutex_.unlock();
  }
  bool trylock() override
  {
    return 0 == mutex_.trylock();
  }

private:
  lib::ObMutex& mutex_;
};

class SetLockerForLogger : public ISetLocker {
public:
  SetLockerForLogger(lib::ObMutex& mutex) : mutex_(mutex), is_disable_(false)
  {}
  void lock() override
  {
    mutex_.lock();
    is_disable_ = !OB_LOGGER.is_enable_logging();
    OB_LOGGER.set_disable_logging(true);
  }
  void unlock() override
  {
    OB_LOGGER.set_disable_logging(is_disable_);
    mutex_.unlock();
  }
  bool trylock() override
  {
    bool succ = 0 == mutex_.trylock();
    if (succ) {
      is_disable_ = !OB_LOGGER.is_enable_logging();
      OB_LOGGER.set_disable_logging(true);
    }
    return succ;
  }

private:
  lib::ObMutex& mutex_;
  bool is_disable_;
};

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* _ALLOC_INTERFACE_H_ */
