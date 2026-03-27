/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_CACHE_OB_CACHE_WASHER_H_
#define OCEANBASE_CACHE_OB_CACHE_WASHER_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace lib
{

class ObICacheWasher
{
public:
  struct ObCacheMemBlock
  {
    ObCacheMemBlock() : next_(NULL) {}
    ObCacheMemBlock *next_;
  };

  virtual int sync_wash_mbs(const uint64_t tenant_id, const int64_t wash_size,
                            ObCacheMemBlock *&wash_blocks) = 0;
  virtual int erase_cache(const uint64_t tenant_id) = 0;
};

class ObDefaultCacheWasher : public ObICacheWasher
{
  ObDefaultCacheWasher() {};
  virtual ~ObDefaultCacheWasher() {};

  virtual int sync_wash_mbs(const uint64_t tenant_id, const int64_t wash_size,
                            ObCacheMemBlock *&wash_blocks) override
  {
    UNUSED(tenant_id);
    UNUSED(wash_size);
    UNUSED(wash_blocks);
    return common::OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
  }
  virtual int erase_cache(const uint64_t tenant_id) override
  {
    UNUSED(tenant_id);
    return OB_SUCCESS;
  }
};

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_CACHE_WASHER_H_
