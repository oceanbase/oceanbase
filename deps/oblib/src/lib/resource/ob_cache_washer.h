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
