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

#ifndef OCEANBASE_OB_LOCK_FUNC_EXECUTOR_H_
#define OCEANBASE_OB_LOCK_FUNC_EXECUTOR_H_

#include "storage/tablelock/ob_lock_executor.h"
#include "sql/session/ob_basic_session_info.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObExecContext;
}

namespace pl
{
class PlPackageLock;
}

namespace transaction
{
namespace tablelock
{
class ObGetLockExecutor : public ObLockExecutor
{
public:
  friend class pl::PlPackageLock;
  int execute(sql::ObExecContext &ctx,
              const ObString &lock_name,
              const int64_t timeout_us);
private:
  int generate_lock_id_(ObLockContext &ctx,
                        const ObString &lock_name,
                        const int64_t timeout_us,
                        uint64_t &lock_id);
  int lock_obj_(sql::ObSQLSessionInfo *session,
                const transaction::ObTxParam &tx_param,
                const uint32_t client_session_id,
                const uint64_t client_session_create_ts,
                const int64_t obj_id,
                const int64_t timeout_us);
  int generate_lock_id_(const ObString &lock_name,
                        uint64_t &lock_id,
                        char *lock_handle);
  int write_lock_id_(ObLockContext &ctx,
                     const ObString &lock_name,
                     const int64_t timeout_us,
                     const uint64_t &lock_id,
                     const char *lock_handle_buf);
};

class ObReleaseLockExecutor : public ObUnLockExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              const ObString &lock_name,
              int64_t &release_cnt);
};

class ObReleaseAllLockExecutor : public ObUnLockExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              int64_t &release_cnt);
};

class ObISFreeLockExecutor : public ObLockExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              const ObString &lock_name);
};

class ObISUsedLockExecutor : public ObLockExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              const ObString &lock_name,
              uint32_t &sess_id);
};

} // tablelock
} // transaction
} // oceanbase
#endif
