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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_OWNER_STATE_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_OWNER_STATE_H_

#include <cstdint>

#include "lib/allocator/ob_malloc.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"

namespace oceanbase
{
namespace logservice
{

enum class ObLogTransportTaskOwnerState : uint8_t
{
  BORROWED = 0, // points to external memory, such as RPC decode buffer.
  OWNED     // owned by holder, released on destruction.
};

class ObLogTransportTaskHolder
{
public:
  ObLogTransportTaskHolder();
  // Build a BORROWED holder from |task|.
  // Contract:
  // 1) success: |holder| is updated to point to |task|;
  // 2) failure: |holder| remains unchanged (no side effect).
  static int borrowed(const ObLogTransportReq *task, ObLogTransportTaskHolder &holder);
  // Build an OWNED holder from |task|.
  // Contract:
  // 1) success: |holder| takes ownership of |task|;
  // 2) failure: |holder| remains unchanged (no side effect).
  static int owned(ObLogTransportReq *task, ObLogTransportTaskHolder &holder);
  int ensure_owned(const common::ObMemAttr &attr);
  bool is_owned() const;
  bool is_empty() const;
  const ObLogTransportReq *ptr() const;
  int64_t size() const;
  void reset();

  ObLogTransportTaskHolder(ObLogTransportTaskHolder &&other) noexcept;
  ObLogTransportTaskHolder &operator=(ObLogTransportTaskHolder &&other) noexcept;
  ~ObLogTransportTaskHolder();
  TO_STRING_KV(K(state_), KP(ptr_), KPC(ptr_));

private:
  ObLogTransportTaskHolder(const ObLogTransportTaskHolder &) = delete;
  ObLogTransportTaskHolder &operator=(const ObLogTransportTaskHolder &) = delete;
  static void free_owned_(const ObLogTransportReq *task);

private:
  const ObLogTransportReq *ptr_;
  ObLogTransportTaskOwnerState state_;
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_OWNER_STATE_H_
