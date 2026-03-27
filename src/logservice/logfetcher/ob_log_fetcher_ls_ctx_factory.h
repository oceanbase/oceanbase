/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_FECCHER_LS_CTX_FACTORY_H_
#define OCEANBASE_LOG_FECCHER_LS_CTX_FACTORY_H_

#include "ob_log_ls_fetch_ctx.h"  // LSFetchCtx

namespace oceanbase
{
namespace logfetcher
{
class ObILogFetcherLSCtxFactory
{
public:
  virtual ~ObILogFetcherLSCtxFactory() {}
  virtual void destroy() = 0;

public:
  virtual int alloc(LSFetchCtx *&ptr) = 0;
  virtual int free(LSFetchCtx *ptr) = 0;
};

}; // end namespace logfetcher
}; // end namespace oceanbase
#endif
