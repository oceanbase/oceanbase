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
