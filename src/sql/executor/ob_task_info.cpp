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

ObTaskInfo::ObTaskInfo(common::ObIAllocator& allocator)
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
      child_task_results_(),
      slice_count_pos_(allocator),
      background_(false),
      retry_times_(0),
      ts_task_send_begin_(INT64_MIN),
      ts_task_recv_done_(INT64_MIN),
      ts_result_send_begin_(INT64_MIN),
      ts_result_recv_done_(INT64_MIN)
{}

ObTaskInfo::~ObTaskInfo()
{
  slice_events_.reset();
}

int ObTaskInfo::deep_copy_slice_events(ObIAllocator& allocator, const ObIArray<ObSliceEvent>& slice_events)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < slice_events.count(); ++i) {
    const ObSliceEvent& src_sr = slice_events.at(i);
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

int ObTaskInfo::ObRangeLocation::assign(const ObTaskInfo::ObRangeLocation& range_loc)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(part_locs_.assign(range_loc.part_locs_))) {
    SQL_EXE_LOG(WARN, "copy part locs failed", K(ret), K(range_loc));
  } else {
    server_ = range_loc.server_;
  }
  return ret;
}

int ObTaskInfo::get_task_participants(ObPartitionIArray& participants)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_location_.part_locs_.count(); ++i) {
    const ObPartitionKey& key = range_location_.part_locs_.at(i).partition_key_;
    if (is_virtual_table(key.table_id_)) {
      // do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(participants, key))) {
      LOG_WARN("add var to array no dup failed", K(ret));
    }
  }
  return ret;
}
int64_t ObTaskInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_TASK_LOC, task_location_, K_(range_location), K_(location_idx), K_(location_idx_list), K_(state),K_(child_task_results), K_(slice_count_pos), K_(background), K_(retry_times), K_(location_idx_list));
  J_OBJ_END();
  return pos;
}
int64_t ObTaskInfo::ObRangeLocation::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(part_locs), K_(server));
  J_OBJ_END();
  return pos;
}
int64_t ObTaskInfo::ObPartLoc::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(partition_key), K_(depend_table_keys), K_(scan_ranges), K_(part_key_ref_id), K_(value_ref_id),K_(renew_time), KPC_(row_store));
  J_OBJ_END();
  return pos;
}
int64_t ObGranuleTaskInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ranges), K_(partition_id), K_(task_id));
  J_OBJ_END();
  return pos;
}
