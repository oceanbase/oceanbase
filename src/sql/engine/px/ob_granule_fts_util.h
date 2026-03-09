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

#ifndef OCEANBASE_ENGINE_PX_OB_GRANULE_FTS_UTIL_H_
#define OCEANBASE_ENGINE_PX_OB_GRANULE_FTS_UTIL_H_

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace sql
{

class ObGranuleTaskInfo;
class ObPxTabletRange;

class ObGranuleFtsUtil
{
public:
  static int get_fts_forward_range(
      ObExecContext &ctx,
      const sql::ObPxTabletRange *&forward_range);
  static int calculate_fts_slice_idx_for_task(
      ObExecContext &ctx,
      const ObGranuleTaskInfo &gi_task_info,
      int64_t &current_slice_idx,
      int64_t &total_slice_count);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_OB_GRANULE_FTS_UTIL_H_
