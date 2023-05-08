/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX OBLOG

#include "share/ob_errno.h"                     // OB_SUCCESS, ..
#include "lib/oblog/ob_log_module.h"            // LOG_*
#include "ob_log_fetcher_ls_ctx_default_factory.h"

namespace oceanbase
{
namespace logfetcher
{
ObLogFetcherLSCtxDefaultFactory::ObLogFetcherLSCtxDefaultFactory() :
    is_inited_(false),
    ctx_pool_()
{}

int ObLogFetcherLSCtxDefaultFactory::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_FAIL(ctx_pool_.init(LS_CTX_MAX_CACHED_COUNT,
      ObModIds::OB_LOG_PART_FETCH_CTX_POOL,
      tenant_id,
      LS_CTX_POOL_BLOCK_SIZE))) {
    LOG_ERROR("init LSFetchCtxPool fail", KR(ret), LITERAL_K(LS_CTX_MAX_CACHED_COUNT),
        LITERAL_K(LS_CTX_POOL_BLOCK_SIZE));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObLogFetcherLSCtxDefaultFactory::destroy()
{
  if (is_inited_) {
    ctx_pool_.destroy();
    is_inited_ = false;
  }
}

int ObLogFetcherLSCtxDefaultFactory::alloc(LSFetchCtx *&ptr)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcherLSCtxDefaultFactory is not inited", KR(ret));
  } else if (OB_FAIL(ctx_pool_.alloc(ptr))) {
    LOG_ERROR("alloc LSFetchCtx fail", KR(ret));
  } else {}

  return ret;
}

int ObLogFetcherLSCtxDefaultFactory::free(LSFetchCtx *ptr)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcherLSCtxDefaultFactory is not inited", KR(ret));
  } else if (OB_FAIL(ctx_pool_.free(ptr))) {
    LOG_ERROR("free LSFetchCtx fail", KR(ret));
  } else {}

  return ret;
}

} // namespace logfetcher
} // namespace oceanbase
