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

#ifndef OCEANBASE_LOG_LS_CALLBACK_H_
#define OCEANBASE_LOG_LS_CALLBACK_H_

#include "lib/container/ob_se_array.h"        // ObSEArray
#include "logservice/palf/lsn.h"              // LSN
#include "logservice/common_util/ob_log_ls_define.h"                 // logservice::TenantLSID
#include "logservice/logfetcher/ob_log_fetcher_start_parameters.h"  // logfetcher::ObLogFetcherStartParameters

namespace oceanbase
{
namespace libobcdc
{
struct LSAddCallback
{
public:
  virtual ~LSAddCallback() {}

public:
  // Add LS
  virtual int add_ls(const logservice::TenantLSID &tls_id,
      const logfetcher::ObLogFetcherStartParameters &start_parameters) = 0;
};

struct LSRecycleCallback
{
public:
  virtual ~LSRecycleCallback() {}

public:
  // Recycling LS
  virtual int recycle_ls(const logservice::TenantLSID &tls_id) = 0;
};

typedef common::ObSEArray<int64_t, 4> LSCBArray;

} // namespace libobcdc
} // namespace oceanbase

#endif
