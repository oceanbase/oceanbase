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
 *
 * Oceanbase LogFecher LS Ctx factory
 */

#ifndef OCEANBASE_LOG_FECCHER_LS_CTX_DEFAULT_FACTORY_H_
#define OCEANBASE_LOG_FECCHER_LS_CTX_DEFAULT_FACTORY_H_

#include "ob_log_fetcher_ls_ctx_factory.h"  // ObILogFetcherLSCtxFactory
#include "lib/objectpool/ob_small_obj_pool.h"  // ObSmallObjPool

namespace oceanbase
{
namespace logfetcher
{
class ObLogFetcherLSCtxDefaultFactory : public ObILogFetcherLSCtxFactory
{
public:
  ObLogFetcherLSCtxDefaultFactory();
  virtual ~ObLogFetcherLSCtxDefaultFactory() { destroy(); }
  int init(const uint64_t tenant_id);
  virtual void destroy();

public:
  virtual int alloc(LSFetchCtx *&ptr);
  virtual int free(LSFetchCtx *ptr);

private:
  typedef common::ObSmallObjPool<LSFetchCtx> LSFetchCtxPool;
  static const int64_t LS_CTX_MAX_CACHED_COUNT = 200;
  static const int64_t LS_CTX_POOL_BLOCK_SIZE = 1L << 24;

  bool is_inited_;
  LSFetchCtxPool ctx_pool_;
};

}; // end namespace logfetcher
}; // end namespace oceanbase
#endif
