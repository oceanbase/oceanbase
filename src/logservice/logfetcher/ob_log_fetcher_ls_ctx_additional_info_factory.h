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
