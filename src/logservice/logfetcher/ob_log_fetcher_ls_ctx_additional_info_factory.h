/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_FETCHER_LS_CTX_ADDITIONAL_INFO_FACTORY_H_
#define OCEANBASE_LOG_FETCHER_LS_CTX_ADDITIONAL_INFO_FACTORY_H_

#include "ob_log_fetcher_ls_ctx_additional_info.h"  // ObILogFetcherLSCtxAddInfo

namespace oceanbase
{
namespace logfetcher
{
class ObILogFetcherLSCtxAddInfoFactory
{
public:
  virtual ~ObILogFetcherLSCtxAddInfoFactory() {}
  virtual void destroy() = 0;

public:
  virtual int alloc(const char *str, ObILogFetcherLSCtxAddInfo *&ptr) = 0;
  virtual void free(ObILogFetcherLSCtxAddInfo *ptr) = 0;
};

} // namespace logfetcher
} // namespace oceanbase

#endif
