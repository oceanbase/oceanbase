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

#include "lib/utility/ob_macro_utils.h"
#include "logservice/palf/lsn.h"              // LSN
#include "share/ob_define.h"
namespace oceanbase
{
namespace logservice
{
const int64_t MAX_FETCH_LOG_BUF_LEN = 4 * 1024 * 1024L;
const int64_t MIN_FETCH_LOG_WORKER_THREAD_COUNT = 1;
const int64_t MAX_FETCH_LOG_WORKER_THREAD_COUNT = 10;
const int64_t MAX_LS_FETCH_LOG_TASK_CONCURRENCY = 4;

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

struct ObRestoreLogContext
{
  bool seek_done_;
  palf::LSN lsn_;

  ObRestoreLogContext() { reset(); }
  ~ObRestoreLogContext() { reset(); }
  void reset();
  TO_STRING_KV(K_(seek_done), K_(lsn));
};

int64_t get_restore_concurrency_by_max_cpu(const uint64_t tenant_id);
} // namespace logservice
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DEFINE_H_ */
