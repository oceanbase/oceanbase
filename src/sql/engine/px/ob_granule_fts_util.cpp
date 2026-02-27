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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_granule_fts_util.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace sql
{

int ObGranuleFtsUtil::get_fts_forward_range(
    ObExecContext &ctx,
    const sql::ObPxTabletRange *&forward_range)
{
  int ret = OB_SUCCESS;
  forward_range = nullptr;
  storage::ObColumnClusteredDag *dag = nullptr;
  storage::ObDDLTabletContext *tablet_ctx = nullptr;

  if (OB_ISNULL(ctx.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc handler is null", K(ret));
  } else if (OB_ISNULL(dag = static_cast<storage::ObColumnClusteredDag*>(
      ctx.get_sqc_handler()->get_sub_coord().get_ddl_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else {
    const ObIArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablet_id = dag->get_ls_tablet_ids();
    if (ls_tablet_id.count() != 1) { // fts doc word table complement inner sql only support one tablet right now
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_tablet_id count is not 1", K(ret), K(ls_tablet_id.count()));
    } else {
      const ObTabletID &tablet_id = ls_tablet_id.at(0).second;
      if (OB_FAIL(dag->get_tablet_context(tablet_id, tablet_ctx))) {
        LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
      } else if (OB_ISNULL(tablet_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet context is null", K(ret), K(tablet_id));
      } else {
        const sql::ObPxTabletRange &src_range = tablet_ctx->get_forward_final_sample_range();
        forward_range = &src_range;
      }
    }
  }
  return ret;
}

int ObGranuleFtsUtil::calculate_fts_slice_idx_for_task(
    ObExecContext &ctx,
    const ObGranuleTaskInfo &gi_task_info,
    int64_t &current_slice_idx,
    int64_t &total_slice_count)
{
  int ret = OB_SUCCESS;
  const sql::ObPxTabletRange *forward_range = nullptr;

  if (OB_UNLIKELY(gi_task_info.ranges_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gi_task_info has no ranges", K(ret), K(gi_task_info));
  } else if (OB_UNLIKELY(gi_task_info.ranges_.count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gi_task_info has more than one range", K(ret), K(gi_task_info));
  } else {
    if (OB_FAIL(get_fts_forward_range(ctx, forward_range))) {
      LOG_WARN("failed to get fts forward range", K(ret));
    } else if (!forward_range->is_valid()) { // parallelism = 1, no need to calculate slice idx
      current_slice_idx = 0;
      total_slice_count = 1;
    } else {
      current_slice_idx = gi_task_info.slice_idx_;
      total_slice_count = forward_range->range_cut_.count() + 1;
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
