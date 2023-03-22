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
