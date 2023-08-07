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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_task_spliter.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObTaskInfo::ObTaskInfo(common::ObIAllocator &allocator)
  : range_location_(allocator),
    task_split_type_(ObTaskSpliter::INVALID_SPLIT),
    task_location_(),
    pull_slice_id_(OB_INVALID_ID),
    force_save_interm_result_(false),
    slice_events_(ObModIds::OB_SQL_EXECUTOR_TASK_INFO, OB_MALLOC_NORMAL_BLOCK_SIZE),
    location_idx_(OB_INVALID_ID),
    location_idx_list_(allocator),
    root_op_(NULL),
    state_(OB_TASK_STATE_NOT_INIT),
    slice_count_pos_(allocator),
    background_(false),
    retry_times_(0),
    ts_task_send_begin_(INT64_MIN),
    ts_task_recv_done_(INT64_MIN),
    ts_result_send_begin_(INT64_MIN),
    ts_result_recv_done_(INT64_MIN),
    root_spec_(NULL)
{
}

ObTaskInfo::~ObTaskInfo()
{
  slice_events_.reset();
}

int ObTaskInfo::deep_copy_slice_events(ObIAllocator &allocator,
                                       const ObIArray<ObSliceEvent> &slice_events)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < slice_events.count(); ++i) {
    const ObSliceEvent &src_sr = slice_events.at(i);
    if (OB_FAIL(slice_events_.push_back(src_sr))) {
      LOG_WARN("fail to push back dest slice event", K(ret), K(src_sr));
    }
//    void *dest_sr_ptr = NULL;
//    ObSliceEvent *dest_sr = NULL;
//    if (OB_ISNULL(dest_sr_ptr = allocator.alloc(sizeof(ObSliceEvent)))) {
//      ret = OB_ALLOCATE_MEMORY_FAILED;
//      LOG_WARN("fail to alloc dest slice event", K(ret));
//    } else if (OB_ISNULL(dest_sr = new(dest_sr_ptr)ObSliceEvent())) {
//      ret = OB_ALLOCATE_MEMORY_FAILED;
//      LOG_WARN("fail to new dest slice evetnt", K(ret));
//    } else if (OB_FAIL(dest_sr->assign(src_sr))) {
//      LOG_WARN("fail to assign dest slice event", K(ret), K(src_sr));
//      dest_sr->~ObSliceEvent();
//      dest_sr = NULL;
//    } else if (OB_FAIL(slice_events_.push_back(dest_sr))) {
//      LOG_WARN("fail to push back dest slice event", K(ret), K(*dest_sr));
//      dest_sr->~ObSliceEvent();
//      dest_sr = NULL;
//    }
  }
  return ret;
}

int ObTaskInfo::ObRangeLocation::assign(const ObTaskInfo::ObRangeLocation &range_loc)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(part_locs_.assign(range_loc.part_locs_))) {
    SQL_EXE_LOG(WARN, "copy part locs failed", K(ret), K(range_loc));
  } else {
    server_ = range_loc.server_;
  }
  return ret;
}

int ObGranuleTaskInfo::assign(const ObGranuleTaskInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ranges_.assign(other.ranges_))) {
      LOG_WARN("assign ranges_ failed", K(ret));
    } else if (OB_FAIL(ss_ranges_.assign(other.ss_ranges_))) {
      LOG_WARN("assign ss_ranges_ failed", K(ret));
    } else {
      tablet_loc_ = other.tablet_loc_;
      task_id_ = other.task_id_;
    }
  }
  return ret;
}

int ObTaskInfo::ObPartLoc::assign(ObTaskInfo::ObPartLoc &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scan_ranges_.assign(other.scan_ranges_))) {
    LOG_WARN("fail to assign scan range", K(ret), K(other.scan_ranges_.count()));
  } else {
    part_key_ref_id_ = other.part_key_ref_id_;
    value_ref_id_ = other.value_ref_id_;
    renew_time_ = other.renew_time_;
    row_store_ = other.row_store_;
    datum_store_ = other.datum_store_;
  }
  return ret;
}

}
}