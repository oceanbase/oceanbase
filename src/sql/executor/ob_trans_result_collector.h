/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_EXECUTOR_OB_TRANS_RESULT_COLLECTOR_H_
#define SRC_SQL_EXECUTOR_OB_TRANS_RESULT_COLLECTOR_H_

#include "sql/engine/ob_exec_context.h"
#include "storage/tx/ob_trans_define.h"
#include "lib/allocator/ob_safe_arena.h"
#include "share/rpc/ob_batch_rpc.h"

namespace oceanbase
{
namespace sql
{

//

/*
 *      /-->-- FORBIDDEN
 *     /
 *    /---------->----------\
 *   /                       \
 * SENT -->-- RUNNING -->-- FINISHED -->-- RETURNED
 *   \             \                        /
 *    \------>------\----------->----------/
 *
 */
enum ObTaskStatus
{
  TS_INVALID,
  TS_SENT,
  TS_RUNNING,
  TS_FINISHED,
  TS_FORBIDDEN,
  TS_NEED_WAIT_ABOVE,
  TS_RETURNED,
};

}  // namespace sql
}  // namespace oceanbase
#endif /* SRC_SQL_EXECUTOR_OB_TRANS_RESULT_COLLECTOR_H_ */
