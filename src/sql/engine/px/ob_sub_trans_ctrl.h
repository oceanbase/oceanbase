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

#ifndef __SQL_ENGINE_PX_SUB_TRANS_UTIL_H__
#define __SQL_ENGINE_PX_SUB_TRANS_UTIL_H__

#include "common/ob_partition_key.h"
#include "sql/ob_sql_trans_control.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObPxSqcMeta;
class ObSubTransCtrl {
public:
  ObSubTransCtrl() = default;
  ~ObSubTransCtrl() = default;
  int start_participants(ObExecContext& ctx, ObPxSqcMeta& sqc);
  int end_participants(ObExecContext& ctx, bool is_rb);

private:
  /* functions */
  int get_participants(ObPxSqcMeta& sqc, common::ObPartitionArray& participants) const;
  /* variables */
  TransState trans_state_;  // Mark whether start_part has been called to determine whether to call end_part
  common::ObPartitionArray participants_;
  DISALLOW_COPY_AND_ASSIGN(ObSubTransCtrl);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* __SQL_ENGINE_PX_SUB_TRANS_UTIL_H__ */
//// end of header file
