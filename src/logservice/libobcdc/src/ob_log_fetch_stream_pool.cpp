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
 *
 * FetchStream Pool
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetch_stream_pool.h"

#include "share/ob_define.h"                  // OB_SERVER_TENANT_ID
#include "lib/allocator/ob_mod_define.h"    // ObModIds
#include "lib/utility/ob_macro_utils.h"     // OB_FAIL
#include "lib/oblog/ob_log_module.h"        // LOG_ERROR

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

FetchStreamPool::FetchStreamPool() : pool_()
{}

FetchStreamPool::~FetchStreamPool()
{
  destroy();
}

int FetchStreamPool::init(const int64_t cached_fs_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pool_.init(cached_fs_count,
      ObModIds::OB_LOG_FETCH_STREAM_POOL,
      OB_SERVER_TENANT_ID,
      DEFAULT_BLOCK_SIZE))) {
    LOG_ERROR("init fetch stream obj pool fail", KR(ret), K(cached_fs_count));
  } else {
    // succ
  }
  return ret;
}

void FetchStreamPool::destroy()
{
  pool_.destroy();
}

int FetchStreamPool::alloc(FetchStream *&fs)
{
  return pool_.alloc(fs);
}

int FetchStreamPool::free(FetchStream *fs)
{
  return pool_.free(fs);
}

void FetchStreamPool::print_stat()
{
  int64_t alloc_count = pool_.get_alloc_count();
  int64_t free_count = pool_.get_free_count();
  int64_t fixed_count = pool_.get_fixed_count();
  int64_t used_count = alloc_count - free_count;
  int64_t dynamic_count = (alloc_count > fixed_count) ? alloc_count - fixed_count : 0;

  _LOG_INFO("[STAT] [FETCH_STREAM_POOL] USED=%ld FREE=%ld FIXED=%ld DYNAMIC=%ld",
      used_count, free_count, fixed_count, dynamic_count);
}

}
}
