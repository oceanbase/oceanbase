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
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "share/external_table/ob_external_table_file_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

OB_SERIALIZE_MEMBER(ObPxDetectableIds,
                    qc_detectable_id_,
                    sqc_detectable_id_);

OB_SERIALIZE_MEMBER(ObP2PDhMapInfo,
                    p2p_sequence_ids_,
                    target_addrs_);

OB_SERIALIZE_MEMBER(ObQCMonitoringInfo, cur_sql_, qc_tid_);

OB_SERIALIZE_MEMBER(ObPxSqcMeta,
                    execution_id_,
                    qc_id_,
                    dfo_id_,
                    sqc_id_,
                    transmit_channel_,
                    qc_ch_info_,
                    sqc_ch_info_,
                    exec_addr_,
                    qc_addr_,
                    max_task_count_,
                    min_task_count_,
                    px_int_id_,
                    receive_channel_,
                    is_fulltree_,
                    is_rpc_worker_,
                    qc_server_id_,
                    parent_dfo_id_,
                    px_sequence_id_,
                    total_task_count_,
                    total_part_count_,
                    transmit_use_interm_result_,
                    recieve_use_interm_result_,
                    serial_receive_channels_,
                    rescan_batch_params_,
                    partition_pruning_table_locations_,
                    ignore_vtable_error_,
                    temp_table_ctx_,
                    access_table_location_keys_,
                    adjoining_root_dfo_,
                    is_single_tsc_leaf_dfo_,
                    access_external_table_files_,
                    px_detectable_ids_,
                    p2p_dh_map_info_,
                    sqc_count_,
                    monitoring_info_);
OB_SERIALIZE_MEMBER(ObPxTask,
                    qc_id_,
                    dfo_id_,
                    sqc_id_,
                    task_id_,
                    sqc_ch_info_,
                    task_ch_info_,
                    qc_addr_,
                    sqc_addr_,
                    exec_addr_,
                    execution_id_,
                    px_int_id_,
                    is_fulltree_);
OB_SERIALIZE_MEMBER(ObPxRpcInitTaskResponse,
                    task_co_id_);

OB_SERIALIZE_MEMBER(ObPxRpcInitSqcResponse,
                    rc_,
                    reserved_thread_count_,
                    partitions_info_,
                    sqc_order_gi_tasks_);
OB_SERIALIZE_MEMBER(ObSqcTableLocationKey,
                    table_location_key_,
                    ref_table_id_,
                    tablet_id_,
                    is_dml_,
                    is_loc_uncertain_);
OB_SERIALIZE_MEMBER(ObPxCleanDtlIntermResInfo, ch_total_info_, sqc_id_, task_count_);
OB_SERIALIZE_MEMBER(ObPxCleanDtlIntermResArgs, info_, batch_size_);

int ObQCMonitoringInfo::init(const ObExecContext &exec_ctx) {
  int ret = OB_SUCCESS;
  qc_tid_ = GETTID();
  if (OB_NOT_NULL(exec_ctx.get_my_session())) {
    cur_sql_ = exec_ctx.get_my_session()->get_current_query_string();
  }
  if (cur_sql_.length() > ObQCMonitoringInfo::LIMIT_LENGTH) {
    cur_sql_.assign(cur_sql_.ptr(), ObQCMonitoringInfo::LIMIT_LENGTH);
  }
  return ret;
}

int ObQCMonitoringInfo::assign(const ObQCMonitoringInfo &other) {
  int ret = OB_SUCCESS;
  cur_sql_ = other.cur_sql_;
  qc_tid_ = other.qc_tid_;
  return ret;
}

void ObQCMonitoringInfo::reset() {
  cur_sql_.reset();
}

int ObPxSqcMeta::assign(const ObPxSqcMeta &other)
{
  int ret = OB_SUCCESS;
  // 注意：非通用函数，不能用于保存已经初始化过的 ObPxSqcMeta
  //       仅用于 assign 执行地址
  if (NULL != qc_channel_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only add a new sqc. you are adding an inited one", K(ret));
  } else if (OB_FAIL(access_table_locations_.assign(other.access_table_locations_))) {
    LOG_WARN("fail assign tscs locations", K(ret));
  } else if (OB_FAIL(transmit_channel_.assign(other.transmit_channel_))) {
    LOG_WARN("fail assign data channel", K(ret));
  } else if (OB_FAIL(receive_channel_.assign(other.receive_channel_))) {
    LOG_WARN("fail assign data channel", K(ret));
  } else if (OB_FAIL(serial_receive_channels_.assign(other.serial_receive_channels_))) {
    LOG_WARN("fail assign serial_receive_channels_", K(ret));
  } else if (OB_FAIL(rescan_batch_params_.assign(other.rescan_batch_params_))) {
    LOG_WARN("fail to assign batch rescan params", K(ret));
  } else if (OB_FAIL(partition_pruning_table_locations_.assign(other.partition_pruning_table_locations_))) {
    LOG_WARN("fail to assign table location", K(ret));
  } else if (OB_FAIL(temp_table_ctx_.assign(other.temp_table_ctx_))) {
    LOG_WARN("failed to assgin to interm result ids.", K(ret));
  } else if (OB_FAIL(access_table_location_keys_.assign(other.access_table_location_keys_))) {
    LOG_WARN("failed to assgin to table location keys.", K(ret));
  } else if (OB_FAIL(access_table_location_indexes_.assign(other.access_table_location_indexes_))) {
    LOG_WARN("failed to assgin to table location keys.", K(ret));
  } else if (OB_FAIL(p2p_dh_map_info_.assign(other.p2p_dh_map_info_))) {
    LOG_WARN("fail to assign p2p dh map info", K(ret));
  } else if (OB_FAIL(monitoring_info_.assign(other.monitoring_info_))) {
    LOG_WARN("fail to assign qc monitoring info", K(ret));
  } else {
    execution_id_ = other.execution_id_;
    qc_id_ = other.qc_id_;
    dfo_id_ = other.dfo_id_;
    sqc_id_ = other.sqc_id_;
    thread_inited_ = other.thread_inited_;
    thread_finish_ = other.thread_finish_;
    exec_addr_ = other.exec_addr_;
    qc_ch_info_ = other.qc_ch_info_;
    sqc_ch_info_ = other.sqc_ch_info_;
    exec_addr_ = other.exec_addr_;
    qc_addr_ = other.qc_addr_;
    task_count_ = other.task_count_;
    max_task_count_ = other.max_task_count_;
    min_task_count_ = other.min_task_count_;
    qc_channel_ = NULL;
    px_int_id_ = other.px_int_id_;
    is_fulltree_ = other.is_fulltree_;
    is_rpc_worker_ = other.is_rpc_worker_;
    qc_server_id_ = other.qc_server_id_;
    parent_dfo_id_ = other.parent_dfo_id_;
    total_task_count_ = other.total_task_count_;
    total_part_count_ = other.total_part_count_;
    px_sequence_id_ = other.px_sequence_id_;
    transmit_use_interm_result_ = other.transmit_use_interm_result_;
    recieve_use_interm_result_ = other.recieve_use_interm_result_;
    ignore_vtable_error_ = other.ignore_vtable_error_;
    server_not_alive_ = other.server_not_alive_;
    adjoining_root_dfo_ = other.adjoining_root_dfo_;
    is_single_tsc_leaf_dfo_ = other.is_single_tsc_leaf_dfo_;
    px_detectable_ids_ = other.px_detectable_ids_;
    interrupt_by_dm_ = other.interrupt_by_dm_;
    sqc_count_ = other.sqc_count_;
  }
  access_external_table_files_.reuse();
  for (int i = 0; OB_SUCC(ret) && i < other.access_external_table_files_.count(); i++) {
    const ObExternalFileInfo &other_file = other.access_external_table_files_.at(i);
    ObExternalFileInfo temp_file;
    temp_file.file_id_ = other_file.file_id_;
    temp_file.file_addr_ = other_file.file_addr_;
    if (OB_FAIL(ob_write_string(allocator_, other_file.file_url_, temp_file.file_url_))) {
      LOG_WARN("fail to write string", K(ret));
    } else if (OB_FAIL(access_external_table_files_.push_back(temp_file))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObPxSqcMeta::add_serial_recieve_channel(const ObPxReceiveDataChannelMsg &channel)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serial_receive_channels_.push_back(channel))) {
    LOG_WARN("fail to push back msg", K(ret));
  }
  return ret;
}

int ObDfo::get_sqc(int64_t idx, ObPxSqcMeta *&sqc)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= sqcs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid idx", K(idx), K(sqcs_.count()), K(ret));
  } else {
    sqc = &sqcs_.at(idx);
    if (OB_ISNULL(sqc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(sqc), K(ret));
    } else if (idx != sqc->get_sqc_id()) {
      // 按照设计预期，sqc 加入 sqcs_ 的顺序和 id 应该一致
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("idx and sqc id mismatch", K(idx), "id", sqc->get_sqc_id(), K(ret));
    }
  }
  return ret;
}

int ObDfo::get_sqcs(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sqcs_.count(); ++i) {
    ret = sqcs.push_back(&sqcs_.at(i));
  }
  return ret;
}

int ObDfo::get_sqcs(common::ObIArray<const ObPxSqcMeta *> &sqcs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sqcs_.count(); ++i) {
    ret = sqcs.push_back(&sqcs_.at(i));
  }
  return ret;
}

int ObDfo::get_addrs(common::ObIArray<common::ObAddr> &addrs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sqcs_.count(); ++i) {
    ret = addrs.push_back(sqcs_.at(i).get_exec_addr());
  }
  return ret;
}

int ObDfo::add_sqc(const ObPxSqcMeta &sqc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqcs_.push_back(sqc))) {
    LOG_WARN("fail add sqc to dfo", K(sqc), K(ret));
  }
  return ret;
}

int ObDfo::check_dfo_pair(ObDfo &parent, ObDfo &child, int64_t &child_dfo_idx)
{
  int ret = OB_SUCCESS;
  child_dfo_idx = -1;
  ObIArray<ObDfo*> &child_dfos = parent.get_child_dfos();
  for (int64_t i = 0; i < child_dfos.count() && OB_SUCC(ret); ++i) {
    if (&child == child_dfos.at(i)) {
      child_dfo_idx = i;
      break;
    }
  }
  if (-1 == child_dfo_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: child dfo not found",
      K(child.get_dfo_id()),
      K(parent.get_dfo_id()));
  }
  return ret;
}

int ObDfo::fill_channel_info_by_sqc(
  ObDtlExecServer &ch_servers,
  ObPxSqcMeta &sqc)
{
  int ret = OB_SUCCESS;
  ch_servers.total_task_cnt_ = 0;
  OZ(ch_servers.prefix_task_counts_.push_back(ch_servers.total_task_cnt_));
  OZ(ch_servers.add_exec_addr(sqc.get_exec_addr()));
  ch_servers.total_task_cnt_ = 1;
  return ret;
}

int ObDfo::fill_channel_info_by_sqc(
  ObDtlExecServer &ch_servers,
  common::ObIArray<ObPxSqcMeta> &sqcs)
{
  int ret = OB_SUCCESS;
  ch_servers.total_task_cnt_ = 0;
  for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); ++i) {
    ObPxSqcMeta &sqc = sqcs.at(i);
    OZ(ch_servers.prefix_task_counts_.push_back(ch_servers.total_task_cnt_));
    OZ(ch_servers.add_exec_addr(sqc.get_exec_addr()));
    ch_servers.total_task_cnt_ += sqc.get_task_count();
  }
  return ret;
}

int ObDfo::calc_total_task_count()
{
  int ret = OB_SUCCESS;
  if (sqcs_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should at least have one sqc", "count", sqcs_.count(), K(ret));
  }
  int64_t total_task_cnt = 0;
  for (int64_t i = 0; i < sqcs_.count() && OB_SUCC(ret); ++i) {
    total_task_cnt += sqcs_.at(i).get_task_count();
  }
  total_task_cnt_ = total_task_cnt;
  return ret;
}

int ObDfo::prepare_channel_info()
{
  int ret = OB_SUCCESS;
  if (sqcs_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should at least have one sqc", "count", sqcs_.count(), K(ret));
  } else if (OB_FAIL(calc_total_task_count())) {
    LOG_WARN("failed to calc total task count", K(ret));
  }
  return ret;
}

int ObDfo::build_tasks()
{
  int ret = OB_SUCCESS;
  if (sqcs_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should at least have one sqc", "count", sqcs_.count(), K(ret));
  }
  total_task_cnt_ = 0;
  for (int64_t i = 0; i < sqcs_.count() && OB_SUCC(ret); ++i) {
    ObPxSqcMeta &sqc = sqcs_.at(i);
    for (int64_t j = 0; j < sqc.get_task_count() && OB_SUCC(ret); ++j) {
      ObPxTaskMeta task;
      task.set_exec_addr(sqc.get_exec_addr());
      task.set_sqc_id(sqc.get_sqc_id());
      task.set_task_id(j);
      ++total_task_cnt_;
      if (OB_FAIL(tasks_.push_back(task))) {
        LOG_WARN("fail save task to array",
                 "sqc_cnt", sqcs_.count(), "sqc_task_cnt", sqc.get_task_count(),
                 K(i), K(j), K(task), K(ret));
      }
    }
  }
  return ret;
}

int ObDfo::alloc_data_xchg_ch()
{
  int ret = OB_SUCCESS;

  if (is_root_dfo()) {
    // root dfo 无需设置 data transmit channels
  } else {
    for (int64_t t = 0; t < tasks_.count() && OB_SUCC(ret); ++t) {
      ObPxTaskChSet ch_set;
      ch_set.set_exec_addr(tasks_.at(t).get_exec_addr());
      ch_set.set_sqc_id(tasks_.at(t).get_sqc_id());
      ch_set.set_task_id(tasks_.at(t).get_task_id());
      ret = transmit_ch_sets_.push_back(ch_set);
    }
  }

  LOG_TRACE("init receive_ch_sets_map_", KP(this), K_(dfo_id), K(lbt()));

  for (int64_t i = 0; OB_SUCC(ret) && i < child_dfos_.count(); ++i) {
    int64_t child_dfo_id = child_dfos_.at(i)->get_dfo_id();
    if (child_dfo_id < 0 || child_dfo_id >= MAX_DFO_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("child dfo id is invalid", K(child_dfo_id), K(ret));
    } else if (child_dfo_id >= receive_ch_sets_map_.count() &&
               OB_FAIL(receive_ch_sets_map_.prepare_allocate(child_dfo_id * 2 + 1))) {
      LOG_WARN("fail to reserve receive_ch_sets_map", K(ret));
    } else if (NULL != receive_ch_sets_map_.at(child_dfo_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL map entry expected. can not init twice", K(ret));
    } else {
      void *ptr = NULL;
      ObPxTaskChSets *receive_ch_sets = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObPxTaskChSets)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail alloc mem", "size", sizeof(ObPxTaskChSets), K(ret));
      } else if (OB_ISNULL(receive_ch_sets = new (ptr) ObPxTaskChSets())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        receive_ch_sets_map_.at(child_dfo_id) = receive_ch_sets;
        for (int64_t t = 0; t < tasks_.count() && OB_SUCC(ret); ++t) {
          ObPxTaskChSet ch_set;
          ch_set.set_exec_addr(tasks_.at(t).get_exec_addr());
          ch_set.set_sqc_id(tasks_.at(t).get_sqc_id());
          ch_set.set_task_id(tasks_.at(t).get_task_id());
          if (OB_FAIL(receive_ch_sets->push_back(ch_set))) {
            LOG_WARN("fail push back ch_set", K(ch_set), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDfo::get_task_transmit_chs_for_update(ObIArray<ObPxTaskChSet *> &ch_sets)
{
  int ret = OB_SUCCESS;
  ch_sets.reuse();
  for (int64_t i = 0; i < transmit_ch_sets_.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(ch_sets.push_back(&transmit_ch_sets_.at(i)))) {
      LOG_WARN("fail push back channel set", K(ret));
    }
  }
  return ret;
}

int ObDfo::get_task_receive_chs_for_update(int64_t child_dfo_id, ObIArray<ObPxTaskChSet *> &ch_sets)
{
  int ret = OB_SUCCESS;

  LOG_TRACE("use gfu receive_ch_sets_map_", KP(this), K_(dfo_id), K(lbt()));
  ch_sets.reuse();

  if (child_dfo_id < 0 || child_dfo_id >= MAX_DFO_ID) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == receive_ch_sets_map_.at(child_dfo_id)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can't find any entry for child dfo id", K(child_dfo_id), K(ret));
  } else {
    ObPxTaskChSets &receive_ch_sets = *receive_ch_sets_map_.at(child_dfo_id);
    for (int64_t i = 0; i < receive_ch_sets.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(ch_sets.push_back(&receive_ch_sets.at(i)))) {
        LOG_WARN("fail push back info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (receive_ch_sets.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array size unexpected.should not be zero",
                 K(child_dfo_id), "ch_sets", receive_ch_sets.count(), K(ret));
      }
    }
  }
  return ret;
}

int ObDfo::get_task_transmit_chs(ObPxTaskChSets &ch_sets,
                                 TaskFilterFunc filter) const
{
  int ret = OB_SUCCESS;
  ch_sets.reuse();
  for (int64_t i = 0; i < transmit_ch_sets_.count() && OB_SUCC(ret); ++i) {
    if (filter(transmit_ch_sets_.at(i))) {
      if (OB_FAIL(ch_sets.push_back(transmit_ch_sets_.at(i)))) {
        LOG_WARN("fail push back channel set", K(ret));
      }
    }
  }
  return ret;
}

int ObDfo::get_task_receive_chs(int64_t child_dfo_id, ObPxTaskChSets &ch_sets) const
{
  return get_task_receive_chs(child_dfo_id,
                              ch_sets,
                              [](const ObPxTaskChSet &){return true;});

}

int ObDfo::get_task_receive_chs(int64_t child_dfo_id,
                                ObPxTaskChSets &ch_sets,
                                TaskFilterFunc filter) const
{
  int ret = OB_SUCCESS;

  LOG_TRACE("use receive_ch_sets_map_", KP(this), K_(dfo_id), K(lbt()));
  ch_sets.reuse();

  if (child_dfo_id < 0 || child_dfo_id >= MAX_DFO_ID) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == receive_ch_sets_map_.at(child_dfo_id)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can't find any entry for child dfo id", K(child_dfo_id), K(ret));
  } else {
    ObPxTaskChSets &receive_ch_sets = *receive_ch_sets_map_.at(child_dfo_id);
    for (int64_t i = 0; i < receive_ch_sets.count() && OB_SUCC(ret); ++i) {
      if (filter(receive_ch_sets.at(i))) {
        if (OB_FAIL(ch_sets.push_back(receive_ch_sets.at(i)))) {
          LOG_WARN("fail push back info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (receive_ch_sets.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array size unexpected.should not be zero",
                 K(child_dfo_id), "ch_sets", receive_ch_sets.count(), K(ret));
      }
    }
  }
  return ret;
}

// child dfo保存是child和parent的sqc信息，parent直接从child拿就可以
int ObDfo::get_dfo_ch_info(int64_t sqc_idx, ObDtlChTotalInfo *&ch_info)
{
  int ret = OB_SUCCESS;
  ch_info = nullptr;
  if (1 == dfo_ch_infos_.count()) {
    // only m * n
    ch_info = &dfo_ch_infos_.at(0);
  } else {
    if (0 >= dfo_ch_infos_.count() || sqc_idx >= dfo_ch_infos_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: receive ch info is error", K(ret), K(sqc_idx),
        K(dfo_ch_infos_.count()));
    } else {
      ch_info = &dfo_ch_infos_.at(sqc_idx);
    }
  }
  return ret;
}

int ObDfo::get_qc_channels(ObIArray<ObDtlChannel *> &sqc_chs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sqcs_.count(); ++i) {
    ObDtlChannel *ch = sqcs_.at(i).get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL unexpected", K(ret));
    } else if (OB_FAIL(sqc_chs.push_back(ch))) {
      LOG_WARN("fail push back ch", K(ret));
    }
  }
  return ret;
}

////// ObPxRpcInitSqcArgs ////////
int ObPxRpcInitSqcArgs::serialize_common_parts_1(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObPhyOpSeriCtx seri_ctx;
  seri_ctx.exec_ctx_ = exec_ctx_;
  ObSqcSerializeCache &ser_cache = const_cast<ObSqcSerializeCache &>(ser_cache_);
  if (ser_cache.cache_serialized_) {
    if (pos + ser_cache.len1_ > buf_len) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      MEMCPY(buf + pos, ser_cache.buf1_, ser_cache.len1_);
      pos += ser_cache.len1_;
    }
  } else {
    int64_t old_pos = pos;

    LST_DO_CODE(OB_UNIS_ENCODE, *ser_phy_plan_);
    LST_DO_CODE(OB_UNIS_ENCODE, *exec_ctx_);

    if (OB_SUCC(ret) && ser_cache.enable_serialize_cache_) {
      ser_cache.len1_ = pos - old_pos;
      ser_cache.buf1_ = ser_cache.allocator_.alloc(ser_cache.len1_);
      if (OB_ISNULL(ser_cache.buf1_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(ser_cache.buf1_, buf + old_pos, ser_cache.len1_);
      }
    }
  }
  return ret;
}

int ObPxRpcInitSqcArgs::serialize_common_parts_2(
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObPhyOpSeriCtx seri_ctx;
  seri_ctx.exec_ctx_ = exec_ctx_;
  ObSqcSerializeCache &ser_cache = const_cast<ObSqcSerializeCache &>(ser_cache_);
  if (ser_cache.cache_serialized_) {
    if (pos + ser_cache.len2_ > buf_len) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      MEMCPY(buf + pos, ser_cache.buf2_, ser_cache.len2_);
      pos += ser_cache.len2_;
    }
  } else {
    int64_t old_pos = pos;
    // 序列化Task的核心部分到远端：sub plan tree
    const ObExprFrameInfo *frame_info = &ser_phy_plan_->get_expr_frame_info();
    if (OB_ISNULL(op_spec_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: op root is null", K(ret));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_expr_frame_info(
                buf, buf_len, pos, *exec_ctx_, *const_cast<ObExprFrameInfo *>(frame_info)))) {
      LOG_WARN("failed to serialize rt expr", K(ret));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_tree(
                buf, buf_len, pos, *op_spec_root_, sqc_.is_fulltree(), sqc_.get_exec_addr(), &seri_ctx))) {
      LOG_WARN("fail serialize root_op", K(ret), K(buf_len), K(pos));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_op_input(
                buf, buf_len, pos, *op_spec_root_, exec_ctx_->get_kit_store(), sqc_.is_fulltree()))) {
      LOG_WARN("failed to deserialize kit store", K(ret));
    }

    if (OB_SUCC(ret) && ser_cache.enable_serialize_cache_) {
      ser_cache.len2_ = pos - old_pos;
      ser_cache.buf2_ = ser_cache.allocator_.alloc(ser_cache.len2_);
      if (OB_ISNULL(ser_cache.buf2_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(ser_cache.buf2_, buf + old_pos, ser_cache.len2_);
      }
    }
  }
  return ret;
}


OB_DEF_SERIALIZE(ObPxRpcInitSqcArgs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_spec_root_) || OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(op_spec_root_), K_(exec_ctx), K_(ser_phy_plan));
  }

  if (OB_SUCC(ret) && OB_FAIL(serialize_common_parts_1(buf, buf_len, pos))) {
      LOG_WARN("fail serialize common parts 1", K(ret));
  }
  LST_DO_CODE(OB_UNIS_ENCODE, sqc_);
  if (OB_SUCC(ret) && OB_FAIL(serialize_common_parts_2(buf, buf_len, pos))) {
      LOG_WARN("fail serialize common parts 2", K(ret));
  }
  // can reuse cache from now on
  (const_cast<ObSqcSerializeCache &>(ser_cache_)).cache_serialized_ = ser_cache_.enable_serialize_cache_;
  LST_DO_CODE(OB_UNIS_ENCODE, qc_order_gi_tasks_);
  LOG_TRACE("serialize sqc", K_(sqc));
  LOG_DEBUG("end trace sqc args", K(pos), K(buf_len), K(this->get_serialize_size()));
  return ret;
}


OB_DEF_DESERIALIZE(ObPxRpcInitSqcArgs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sqc_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sqc handler cann't be null", K(ret));
  } else if (OB_FAIL(sqc_handler_->copy_sqc_init_arg(pos, buf, data_len))) {
    LOG_WARN("Failed to assign sqc", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxRpcInitSqcArgs)
{

  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_) || OB_ISNULL(op_spec_root_)) {
    LOG_ERROR("task not init", K_(exec_ctx), K_(ser_phy_plan), K(ret));
  } else {
    ObPhyOpSeriCtx seri_ctx;
    seri_ctx.exec_ctx_ = exec_ctx_;
    ObSqcSerializeCache &ser_cache = const_cast<ObSqcSerializeCache &>(ser_cache_);
    if (ser_cache.cache_serialized_) {
      len += ser_cache.slen_;
    } else {
      LST_DO_CODE(OB_UNIS_ADD_LEN, *ser_phy_plan_);
      LST_DO_CODE(OB_UNIS_ADD_LEN, *exec_ctx_);
      if (OB_ISNULL(op_spec_root_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected status: op root is null", K(ret));
      } else {
        const ObExprFrameInfo *frame_info = &ser_phy_plan_->get_expr_frame_info();
        len += ObPxTreeSerializer::get_serialize_expr_frame_info_size(*exec_ctx_, *const_cast<ObExprFrameInfo *>(frame_info));
        len += ObPxTreeSerializer::get_tree_serialize_size(*op_spec_root_, sqc_.is_fulltree(), &seri_ctx);
        len += ObPxTreeSerializer::get_serialize_op_input_size(
            *op_spec_root_, exec_ctx_->get_kit_store(), sqc_.is_fulltree());
      }
      LOG_TRACE("trace get ser rpc init sqc args size", K(len));
      ser_cache.slen_ = len;
    }
    // always serialize
    LST_DO_CODE(OB_UNIS_ADD_LEN, sqc_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, qc_order_gi_tasks_);
  }
  return len;
}

void ObPxRpcInitSqcArgs::set_serialize_param(ObExecContext &exec_ctx,
                                             ObOpSpec &op_spec_root,
                                             const ObPhysicalPlan &ser_phy_plan)
{
  exec_ctx_ = &exec_ctx;
  op_spec_root_ = &op_spec_root;
  ser_phy_plan_ = &ser_phy_plan;
}

void ObPxRpcInitSqcArgs::set_deserialize_param(ObExecContext &exec_ctx,
                                               ObPhysicalPlan &des_phy_plan,
                                               ObIAllocator *des_allocator)
{
  exec_ctx_ = &exec_ctx;
  des_phy_plan_ = &des_phy_plan;
  des_allocator_ = des_allocator;
}

int ObPxRpcInitSqcArgs::do_deserialize(int64_t &pos, const char *net_buf, int64_t data_len)
{
  int ret = OB_SUCCESS;
  /**
   * 不需要记录des allocator分配出来这段buf，因为sqc handler回收的时候，一定
   * 会进行reset，从而释放掉该内存。
   */
  char *buf = nullptr;
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(des_phy_plan_) || OB_ISNULL(des_allocator_)
      || OB_ISNULL(net_buf) || (data_len <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret), K_(exec_ctx), K_(des_phy_plan), K_(des_allocator),
        K(net_buf), K(data_len));
  } else if (OB_ISNULL(buf = (char *)des_allocator_->alloc(sizeof(char) * data_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory", K(ret));
  } else if (OB_UNLIKELY(NULL != op_spec_root_)) {
    op_spec_root_ = nullptr;
  }

  if (OB_SUCC(ret)) {
    MEMCPY(buf, net_buf, data_len);

    LST_DO_CODE(OB_UNIS_DECODE, *des_phy_plan_);
    LST_DO_CODE(OB_UNIS_DECODE, *exec_ctx_);
    if (OB_SUCC(ret) && OB_ISNULL(exec_ctx_->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    }
    if (OB_SUCC(ret)) {
      // Compact mode may not set while rpc argument deserialize, set it manually.
      // See:
      lib::CompatModeGuard g(ORACLE_MODE == exec_ctx_->get_my_session()->get_compatibility_mode()
          ? lib::Worker::CompatMode::ORACLE
          : lib::Worker::CompatMode::MYSQL);

      LST_DO_CODE(OB_UNIS_DECODE, sqc_);

      LOG_TRACE("deserialize sqc", K_(sqc));

      if (OB_SUCC(ret)) {
        const ObExprFrameInfo *frame_info = &des_phy_plan_->get_expr_frame_info();
        if (OB_FAIL(ObPxTreeSerializer::deserialize_expr_frame_info(
            buf, data_len, pos, *exec_ctx_, *const_cast<ObExprFrameInfo *>(frame_info)))) {
          LOG_WARN("failed to serialize rt expr", K(ret));
        } else if (OB_FAIL(ObPxTreeSerializer::deserialize_tree(
            buf, data_len, pos, *des_phy_plan_, op_spec_root_, scan_spec_ops_))) {
          LOG_WARN("fail deserialize tree", K(ret));
        } else if (OB_FAIL(op_spec_root_->create_op_input(*exec_ctx_))) {
          LOG_WARN("create operator from spec failed", K(ret));
        } else if (OB_FAIL(ObPxTreeSerializer::deserialize_op_input(buf, data_len, pos, exec_ctx_->get_kit_store()))) {
          LOG_WARN("failed to deserialize kit store", K(ret));
        } else {
          des_phy_plan_->set_root_op_spec(op_spec_root_);
          exec_ctx_->reference_my_plan(des_phy_plan_);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // if version of qc is old, qc_order_gi_tasks_ will not be serialized and the value will be false.
    qc_order_gi_tasks_ = false;
    LST_DO_CODE(OB_UNIS_DECODE, qc_order_gi_tasks_);
    LOG_TRACE("deserialize qc order gi tasks", K(qc_order_gi_tasks_), K(sqc_), K(this));
  }
  return ret;
}

int ObPxRpcInitSqcArgs::assign(ObPxRpcInitSqcArgs &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqc_.assign(other.sqc_))) {
    LOG_WARN("Failed to assign sqc", K(ret));
  } else {
    des_phy_plan_ = other.des_phy_plan_;
    exec_ctx_ = other.exec_ctx_;
    op_spec_root_ = other.op_spec_root_;
    static_engine_root_ = other.static_engine_root_;
    des_allocator_ = other.des_allocator_;
  }
  return ret;
}


////// ObPxRpcInitTaskArgs ////////

OB_DEF_SERIALIZE(ObPxRpcInitTaskArgs)
{
  int ret = OB_SUCCESS;


  if (OB_ISNULL(op_spec_root_) || OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K_(exec_ctx), K_(ser_phy_plan));
  }
  uint64_t sqc_task_ptr_val = reinterpret_cast<uint64_t>(sqc_task_ptr_);
  uint64_t sqc_handler_ptr_val = reinterpret_cast<uint64_t>(sqc_handler_);

  LST_DO_CODE(OB_UNIS_ENCODE, *ser_phy_plan_);
  LST_DO_CODE(OB_UNIS_ENCODE, *exec_ctx_);
  LST_DO_CODE(OB_UNIS_ENCODE, task_);
  LST_DO_CODE(OB_UNIS_ENCODE, sqc_task_ptr_val);
  LST_DO_CODE(OB_UNIS_ENCODE, sqc_handler_ptr_val);

  LOG_TRACE("serialize task", KP_(sqc_task_ptr), KP_(sqc_handler), K_(task));
  // 序列化Task的核心部分到执行端：sub plan tree
  if (OB_SUCC(ret)) {
    const ObExprFrameInfo *frame_info = &ser_phy_plan_->get_expr_frame_info();
    if (OB_ISNULL(op_spec_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: op root is null", K(ret));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_expr_frame_info(
        buf, buf_len, pos, *exec_ctx_, *const_cast<ObExprFrameInfo *>(frame_info)))) {
      LOG_WARN("failed to serialize rt expr", K(ret));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_tree(
                buf, buf_len, pos, *op_spec_root_, task_.is_fulltree(), task_.get_exec_addr()))) {
      LOG_WARN("fail serialize root_op", K(ret), K(buf_len), K(pos));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_op_input(
        buf, buf_len, pos, *op_spec_root_, exec_ctx_->get_kit_store(), task_.is_fulltree()))) {
      LOG_WARN("failed to deserialize kit store", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPxRpcInitTaskArgs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(des_phy_plan_) || OB_ISNULL(des_allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret), K_(exec_ctx), K_(des_phy_plan), K_(des_allocator));
  } else if (OB_UNLIKELY(NULL != op_spec_root_)) {
    // if op_root_ is not NULL, just reset it
    op_spec_root_ = NULL;
  }

  // PX framework do the work at stage named after-process,
  // and in this stage we can not ensure the memory to be valid (may be release
  // by the network framework). So we deep copy these memory here.

  pos = 0;
  char *tmp_buf = (char *)des_allocator_->alloc(data_len);

  if (OB_ISNULL(tmp_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(tmp_buf));
  } else {
    MEMCPY(tmp_buf, buf, data_len);
    buf = tmp_buf;
  }

  LST_DO_CODE(OB_UNIS_DECODE, *des_phy_plan_);
  LST_DO_CODE(OB_UNIS_DECODE, *exec_ctx_);
  LST_DO_CODE(OB_UNIS_DECODE, task_);

  uint64_t sqc_task_ptr_val = 0;
  uint64_t sqc_handler_ptr_val = 0;
  LST_DO_CODE(OB_UNIS_DECODE, sqc_task_ptr_val);
  LST_DO_CODE(OB_UNIS_DECODE, sqc_handler_ptr_val);
  sqc_task_ptr_ = reinterpret_cast<ObPxTask *>(sqc_task_ptr_val);
  sqc_handler_ = reinterpret_cast<ObPxSqcHandler *>(sqc_handler_ptr_val);

  LOG_TRACE("deserialized task", KP_(sqc_task_ptr), KP_(sqc_handler), K_(task));

  if (OB_SUCC(ret)) {
    const ObExprFrameInfo *expr_frame_info = &des_phy_plan_->get_expr_frame_info();
    if (OB_FAIL(ObPxTreeSerializer::deserialize_expr_frame_info(
        buf, data_len, pos, *exec_ctx_, *const_cast<ObExprFrameInfo*>(expr_frame_info)))) {
      LOG_WARN("failed to serialize rt expr", K(ret));
    } else if (OB_FAIL(ObPxTreeSerializer::deserialize_tree(
                buf, data_len, pos, *des_phy_plan_, op_spec_root_))) {
      LOG_WARN("fail deserialize tree", K(ret));
    } else if (OB_FAIL(op_spec_root_->create_op_input(*exec_ctx_))) {
      LOG_WARN("create operator from spec failed", K(ret));
    } else if (OB_FAIL(ObPxTreeSerializer::deserialize_op_input(buf, data_len, pos, exec_ctx_->get_kit_store()))) {
          LOG_WARN("failed to deserialize kit store", K(ret));
    } else {
      des_phy_plan_->set_root_op_spec(op_spec_root_);
      exec_ctx_->reference_my_plan(des_phy_plan_);
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxRpcInitTaskArgs)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_) || OB_ISNULL(op_spec_root_)) {
    LOG_ERROR("task not init", K_(exec_ctx), K_(ser_phy_plan));
  } else {
    uint64_t sqc_task_ptr_val = reinterpret_cast<uint64_t>(sqc_task_ptr_);
    uint64_t sqc_handler_ptr_val = reinterpret_cast<uint64_t>(sqc_handler_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, *ser_phy_plan_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, *exec_ctx_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, task_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, sqc_task_ptr_val);
    LST_DO_CODE(OB_UNIS_ADD_LEN, sqc_handler_ptr_val);
    if (OB_ISNULL(op_spec_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected status: op root is null", K(ret));
    } else {
      const ObExprFrameInfo *frame_info = &ser_phy_plan_->get_expr_frame_info();
      len += ObPxTreeSerializer::get_serialize_expr_frame_info_size(*exec_ctx_, *const_cast<ObExprFrameInfo *>(frame_info));
      len += ObPxTreeSerializer::get_tree_serialize_size(*op_spec_root_, task_.is_fulltree());
      len += ObPxTreeSerializer::get_serialize_op_input_size(
        *op_spec_root_, exec_ctx_->get_kit_store(), task_.is_fulltree()
      );
    }
  }
  return len;
}


void ObPxRpcInitTaskArgs::set_serialize_param(ObExecContext &exec_ctx,
                                              ObOpSpec &op_spec_root,
                                              const ObPhysicalPlan &ser_phy_plan)
{
  exec_ctx_ = &exec_ctx;
  op_spec_root_ = &op_spec_root;
  ser_phy_plan_ = &ser_phy_plan;
}

void ObPxRpcInitTaskArgs::set_deserialize_param(ObExecContext &exec_ctx,
                                                ObPhysicalPlan &des_phy_plan,
                                                ObIAllocator *des_allocator)
{
  exec_ctx_ = &exec_ctx;
  des_phy_plan_ = &des_phy_plan;
  des_allocator_ = des_allocator;
}

int ObPxRpcInitTaskArgs::init_deserialize_param(lib::MemoryContext &mem_context, const observer::ObGlobalContext &gctx)
{
  int ret = OB_SUCCESS;
  void *plan_buf = NULL;
  void *ctx_buf = NULL;
  if (OB_ISNULL(plan_buf = mem_context->get_arena_allocator().alloc(sizeof(ObPhysicalPlan)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_ISNULL(ctx_buf = mem_context->get_arena_allocator().alloc(sizeof(ObDesExecContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    des_phy_plan_ = new (plan_buf) ObPhysicalPlan(mem_context);
    exec_ctx_ = new (ctx_buf) ObDesExecContext(mem_context->get_arena_allocator(), gctx.session_mgr_);
    des_allocator_ = &mem_context->get_arena_allocator();
  }
  return ret;
}


int ObPxRpcInitTaskArgs::deep_copy_assign(ObPxRpcInitTaskArgs &src,
                                          ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNIS_VERSION_GUARD(lib::get_unis_global_compat_version());
  // 深拷贝 arg 中所有元素，入session、op tree 等
  // 暂时通过序列化+反序列化完成
  int64_t ser_pos = 0;
  int64_t des_pos = 0;
  void *ser_ptr = NULL;
  int64_t ser_arg_len = src.get_serialize_size();
  if (OB_ISNULL(ser_ptr = alloc.alloc(ser_arg_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc memory", K(ser_arg_len), KP(ser_ptr), K(ret));
  } else if (OB_ISNULL(des_allocator_)
             || OB_ISNULL(des_phy_plan_)
             || OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialize args not init", K(ret));
  } else if (OB_FAIL(src.serialize(static_cast<char *>(ser_ptr), ser_arg_len, ser_pos))) {
    LOG_WARN("fail serialzie init task arg", KP(ser_ptr), K(ser_arg_len), K(ser_pos), K(ret));
  } else if (OB_FAIL(deserialize(static_cast<const char *>(ser_ptr), ser_pos, des_pos))) {
    LOG_WARN("fail des task arg", KP(ser_ptr), K(ser_pos), K(des_pos), K(ret));
  } else if (ser_pos != des_pos) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("data_len and pos mismatch", K(ser_arg_len), K(ser_pos), K(des_pos), K(ret));
  } else {
    // PLACE_HOLDER: if want multiple px worker share trans_desc
    // set exec_ctx_->session->set_effective_trans_desc(src.exec_ctx_->session->get_effective_trans_desc());
  }

  return ret;
}


void ObDfo::reset_resource(ObDfo *dfo)
{
  if (nullptr != dfo) {
    dfo->sqcs_.reset();
    dfo->child_dfos_.reset();
    dfo->tasks_.reset();
    for (int64_t j = 0; j < dfo->receive_ch_sets_map_.count(); ++j) {
      if (OB_NOT_NULL(dfo->receive_ch_sets_map_.at(j))) {
        dfo->receive_ch_sets_map_.at(j)->reset();
      }
    }
    dfo->transmit_ch_sets_.reset();
    dfo->p2p_dh_map_info_.destroy();
    dfo->~ObDfo();
    dfo = nullptr;
  }
}

bool ObDfo::check_root_valid()
{
  bool invalid = false;
  if (nullptr != root_op_spec_) {
    invalid = IS_PX_COORD(root_op_spec_->type_);
  }
  return invalid;
}

const ObPhysicalPlan* ObDfo::get_plan_by_root()
{
  const ObPhysicalPlan *plan = nullptr;
  if (nullptr != root_op_spec_) {
    plan = root_op_spec_->get_phy_plan();
  }
  return plan;
}
