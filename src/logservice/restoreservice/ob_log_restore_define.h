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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DEFINE_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DEFINE_H_

#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"                  // ObAddr
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/palf/lsn.h"              // LSN
#include "share/ob_define.h"
namespace oceanbase
{
namespace logservice
{
using oceanbase::palf::LSN;
using oceanbase::common::ObAddr;
using oceanbase::common::ObString;
const int64_t MAX_FETCH_LOG_BUF_LEN = 4 * 1024 * 1024L;

struct ObLogRestoreErrorContext
{
  int ret_code_;
  share::ObTaskId trace_id_;
  ObLogRestoreErrorContext() { reset(); }
  virtual ~ObLogRestoreErrorContext() { reset(); }
  void reset();
  ObLogRestoreErrorContext &operator=(const ObLogRestoreErrorContext &other);
  TO_STRING_KV(K_(ret_code), K_(trace_id));
};

// The fetch log context of one ls,
// if the ls is scheduled with fetch log task,
// it is marked issued.
struct ObRemoteFetchContext
{
  bool issued_;
  int64_t last_fetch_ts_;
  LSN max_submit_lsn_;
  LSN max_fetch_lsn_;
  ObLogRestoreErrorContext error_context_;          // 记录该日志流遇到错误信息, 仅leader有效

  ObRemoteFetchContext() { reset(); }
  ~ObRemoteFetchContext() { reset(); }
  void reset();
  TO_STRING_KV(K_(issued), K_(last_fetch_ts), K_(max_submit_lsn), K_(max_fetch_lsn), K_(error_context));
};

} // namespace logservice
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DEFINE_H_ */
