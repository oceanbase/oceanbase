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

#ifndef _OB_SQL_PX_DTL_MSG_H_
#define _OB_SQL_PX_DTL_MSG_H_

#include "share/interrupt/ob_global_interrupt_call.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/dtl/ob_dtl_processor.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/engine/px/ob_px_row_store.h"
#include "common/row/ob_row.h"
#include "lib/compress/ob_compress_util.h"

namespace oceanbase {
namespace sql {

struct ObPxPartitionInfo {
  OB_UNIS_VERSION(1);

public:
  ObPxPartitionInfo() : partition_key_(), logical_row_count_(0), physical_row_count_(0)
  {}
  virtual ~ObPxPartitionInfo() = default;
  void assign(const ObPxPartitionInfo& partition_info)
  {
    partition_key_ = partition_info.partition_key_;
    logical_row_count_ = partition_info.logical_row_count_;
    physical_row_count_ = partition_info.physical_row_count_;
  }
  TO_STRING_KV(K_(partition_key), K_(logical_row_count), K_(physical_row_count));
  common::ObPartitionKey partition_key_;
  int64_t logical_row_count_;
  int64_t physical_row_count_;
};
struct ObPxDmlRowInfo {
  OB_UNIS_VERSION(1);

public:
  ObPxDmlRowInfo() : row_match_count_(0), row_duplicated_count_(0), row_deleted_count_(0)
  {}
  ~ObPxDmlRowInfo() = default;
  void reset()
  {
    row_match_count_ = 0;
    row_duplicated_count_ = 0;
    row_deleted_count_ = 0;
  }
  void set_px_dml_row_info(const ObPhysicalPlanCtx& plan_ctx);
  void add_px_dml_row_info(ObPxDmlRowInfo& row_info)
  {
    row_match_count_ += row_info.row_match_count_;
    row_duplicated_count_ += row_info.row_duplicated_count_;
    row_deleted_count_ += row_info.row_deleted_count_;
  }
  TO_STRING_KV(K_(row_match_count), K_(row_duplicated_count), K_(row_deleted_count))
public:
  int64_t row_match_count_;
  int64_t row_duplicated_count_;
  int64_t row_deleted_count_;
};

class ObPxTaskMonitorInfo {
  OB_UNIS_VERSION(1);

public:
  ObPxTaskMonitorInfo() : sched_exec_time_start_(0), sched_exec_time_end_(0), exec_time_start_(0), exec_time_end_(0)
  {}
  ObPxTaskMonitorInfo& operator=(const ObPxTaskMonitorInfo& other)
  {
    sched_exec_time_start_ = other.sched_exec_time_start_;
    sched_exec_time_end_ = other.sched_exec_time_end_;
    exec_time_start_ = other.exec_time_start_;
    exec_time_end_ = other.exec_time_end_;
    metrics_.assign(other.metrics_);
    return *this;
  }
  inline void record_sched_exec_time_begin()
  {
    sched_exec_time_start_ = ObTimeUtility::current_time();
  }
  inline void record_sched_exec_time_end()
  {
    sched_exec_time_end_ = ObTimeUtility::current_time();
  }
  inline void record_exec_time_begin()
  {
    exec_time_start_ = ObTimeUtility::current_time();
  }
  inline void record_exec_time_end()
  {
    exec_time_end_ = ObTimeUtility::current_time();
  }
  inline int64_t get_task_start_timestamp() const
  {
    return sched_exec_time_start_;
  }
  inline int64_t get_sched_exec_time() const
  {
    return sched_exec_time_end_ - sched_exec_time_start_;
  }
  inline int64_t get_exec_time() const
  {
    return exec_time_end_ - exec_time_start_;
  }

  int add_op_metric(sql::ObOpMetric& metric);
  common::ObIArray<sql::ObOpMetric>& get_op_metrics()
  {
    return metrics_;
  }

  TO_STRING_KV(
      K_(sched_exec_time_start), K_(sched_exec_time_end), K_(exec_time_start), K_(exec_time_end), K(metrics_.count()));

private:
  int64_t sched_exec_time_start_;
  int64_t sched_exec_time_end_;
  int64_t exec_time_start_;
  int64_t exec_time_end_;
  common::ObSEArray<sql::ObOpMetric, 4> metrics_;  // operator metric
};

typedef common::ObSEArray<ObPxTaskMonitorInfo, 32> ObPxTaskMonitorInfoArray;

class ObPxTaskChSet : public dtl::ObDtlChSet {
  OB_UNIS_VERSION(1);

public:
  ObPxTaskChSet()
      : sqc_id_(common::OB_INVALID_INDEX), task_id_(common::OB_INVALID_INDEX), sm_group_id_(common::OB_INVALID_INDEX)
  {}
  ~ObPxTaskChSet() = default;
  void set_sqc_id(int64_t sqc_id)
  {
    sqc_id_ = sqc_id;
  }
  int64_t get_sqc_id() const
  {
    return sqc_id_;
  }
  void set_task_id(int64_t task_id)
  {
    task_id_ = task_id;
  }
  int64_t get_task_id() const
  {
    return task_id_;
  }
  void set_sm_group_id(int64_t sm_group_id)
  {
    sm_group_id_ = sm_group_id;
  }
  int64_t get_sm_group_id() const
  {
    return sm_group_id_;
  }
  int assign(const ObPxTaskChSet& ch_set);

private:
  int64_t sqc_id_;
  int64_t task_id_;
  int64_t sm_group_id_;
};

typedef common::ObArray<ObPxTaskChSet, common::ModulePageAllocator, false, /*auto free*/
    common::ObArrayDefaultCallBack<ObPxTaskChSet>, common::DefaultItemEncode<ObPxTaskChSet> >
    ObPxTaskChSets;
typedef common::ObArray<dtl::ObDtlChTotalInfo, common::ModulePageAllocator, false, /*auto free*/
    common::ObArrayDefaultCallBack<dtl::ObDtlChTotalInfo>, common::DefaultItemEncode<dtl::ObDtlChTotalInfo> >
    ObPxChTotalInfos;

struct ObPxPartChMapItem {
  OB_UNIS_VERSION(1);

public:
  ObPxPartChMapItem(int64_t first, int64_t second) : first_(first), second_(second), third_(INT64_MAX)
  {}
  ObPxPartChMapItem(int64_t first, int64_t second, int64_t third) : first_(first), second_(second), third_(third)
  {}
  ObPxPartChMapItem() : first_(0), second_(0), third_(INT64_MAX)
  {}
  int assign(const ObPxPartChMapItem& other)
  {
    first_ = other.first_;
    second_ = other.second_;
    third_ = other.third_;
    return common::OB_SUCCESS;
  }
  int64_t first_;
  int64_t second_;
  int64_t third_;
  TO_STRING_KV(K_(first), K_(second), K_(third));
};

typedef common::ObArray<ObPxPartChMapItem, common::ModulePageAllocator, false, /*auto free*/
    common::ObArrayDefaultCallBack<ObPxPartChMapItem>, common::DefaultItemEncode<ObPxPartChMapItem> >
    ObPxPartChMapArray;
typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> ObPxPartChMap;

struct ObPxPartChInfo {
  ObPxPartChInfo() : part_ch_array_()
  {}
  ~ObPxPartChInfo() = default;
  ObPxPartChMapArray part_ch_array_;
};

class ObPxReceiveDataChannelMsg : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::PX_RECEIVE_DATA_CHANNEL> {
  OB_UNIS_VERSION_V(1);

public:
  ObPxReceiveDataChannelMsg() : child_dfo_id_(-1), ch_sets_(), ch_map_opt_(false), ch_total_info_()
  {}
  virtual ~ObPxReceiveDataChannelMsg() = default;
  int set_payload(int64_t child_dfo_id, ObPxTaskChSets& ch_sets)
  {
    child_dfo_id_ = child_dfo_id;
    return ch_sets_.assign(ch_sets);
  }
  int set_payload(int64_t child_dfo_id, dtl::ObDtlChTotalInfo& ch_total_infos)
  {
    set_ch_map_opt();
    child_dfo_id_ = child_dfo_id;
    return ch_total_info_.assign(ch_total_infos);
  }
  int assign(const ObPxReceiveDataChannelMsg& other)
  {
    int ret = common::OB_SUCCESS;
    child_dfo_id_ = other.child_dfo_id_;
    ch_map_opt_ = other.ch_map_opt_;
    if (OB_FAIL(ch_sets_.assign(other.ch_sets_))) {
    } else if (OB_FAIL(ch_total_info_.assign(other.ch_total_info_))) {
    }
    return ret;
  }
  void reset()
  {
    ch_sets_.reset();
    ch_total_info_.reset();
    ch_map_opt_ = false;
  }
  int64_t get_child_dfo_id() const
  {
    return child_dfo_id_;
  }
  ObPxTaskChSets& get_ch_sets()
  {
    return ch_sets_;
  }
  const ObPxTaskChSets& get_ch_sets() const
  {
    return ch_sets_;
  }
  dtl::ObDtlChTotalInfo& get_ch_total_info()
  {
    return ch_total_info_;
  }

  bool has_filled_channel() const
  {
    return 0 < ch_sets_.count() || ch_map_opt_;
  }
  void set_ch_map_opt()
  {
    ch_map_opt_ = true;
  }
  bool get_ch_map_opt()
  {
    return ch_map_opt_;
  }
  TO_STRING_KV(K_(child_dfo_id), K_(ch_sets), K_(ch_map_opt), K_(ch_total_info));

private:
  int64_t child_dfo_id_;
  ObPxTaskChSets ch_sets_;
  bool ch_map_opt_;
  dtl::ObDtlChTotalInfo ch_total_info_;
};

class ObPxTransmitDataChannelMsg : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::PX_TRANSMIT_DATA_CHANNEL> {
  OB_UNIS_VERSION_V(1);

public:
  ObPxTransmitDataChannelMsg() : ch_sets_(), ch_total_info_(), part_affinity_map_(), ch_map_opt_(false)
  {}
  virtual ~ObPxTransmitDataChannelMsg() = default;
  void reset()
  {
    ch_sets_.reset();
    ch_total_info_.reset();
    part_affinity_map_.reset();
    ch_map_opt_ = false;
  }
  int set_payload(const ObPxTaskChSets& ch_sets, const ObPxPartChMapArray& map)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ch_sets_.assign(ch_sets))) {
      // fail
    } else if (OB_FAIL(part_affinity_map_.assign(map))) {
      // fail
    }
    return ret;
  }
  int set_payload(const dtl::ObDtlChTotalInfo& ch_total_info, const ObPxPartChMapArray& map)
  {
    int ret = common::OB_SUCCESS;
    set_ch_map_opt();
    if (OB_FAIL(ch_total_info_.assign(ch_total_info))) {
    } else if (OB_FAIL(part_affinity_map_.assign(map))) {
    }
    return ret;
  }
  int set_payload(
      const ObPxTaskChSets& ch_sets, const dtl::ObDtlChTotalInfo& ch_total_infos, const ObPxPartChMapArray& map)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ch_total_info_.assign(ch_total_infos))) {
    } else if (OB_FAIL(part_affinity_map_.assign(map))) {
    } else if (OB_FAIL(ch_sets_.assign(ch_sets))) {
    }
    return ret;
  }
  int assign(const ObPxTransmitDataChannelMsg& other)
  {
    ch_map_opt_ = other.ch_map_opt_;
    return set_payload(other.ch_sets_, other.ch_total_info_, other.part_affinity_map_);
  }
  ObPxTaskChSets& get_ch_sets()
  {
    return ch_sets_;
  }
  const ObPxTaskChSets& get_ch_sets() const
  {
    return ch_sets_;
  }
  ObPxPartChMapArray& get_part_affinity_map()
  {
    return part_affinity_map_;
  }
  dtl::ObDtlChTotalInfo& get_ch_total_info()
  {
    return ch_total_info_;
  }

  void set_ch_map_opt()
  {
    ch_map_opt_ = true;
  }
  bool get_ch_map_opt()
  {
    return ch_map_opt_;
  }
  bool has_filled_channel() const
  {
    return 0 < ch_sets_.count() || ch_map_opt_;
  }
  TO_STRING_KV(K_(ch_sets), K_(part_affinity_map), K_(ch_total_info), K_(ch_map_opt));

private:
  ObPxTaskChSets ch_sets_;
  dtl::ObDtlChTotalInfo ch_total_info_;
  ObPxPartChMapArray part_affinity_map_;  // addressing channels in partition wise join
  bool ch_map_opt_;
};

class ObPxInitSqcResultMsg : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::INIT_SQC_RESULT> {
  OB_UNIS_VERSION_V(1);

public:
  ObPxInitSqcResultMsg()
      : dfo_id_(common::OB_INVALID_ID), sqc_id_(common::OB_INVALID_ID), rc_(common::OB_SUCCESS), task_count_(0)
  {}
  virtual ~ObPxInitSqcResultMsg() = default;
  void reset()
  {}
  TO_STRING_KV(K_(dfo_id), K_(sqc_id), K_(rc), K_(task_count));

public:
  int64_t dfo_id_;
  int64_t sqc_id_;
  int rc_;
  int64_t task_count_;
  // No need to serialize
  ObSEArray<ObPxPartitionInfo, 8> partitions_info_;
};

class ObPxFinishSqcResultMsg : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::FINISH_SQC_RESULT> {
  OB_UNIS_VERSION_V(1);

public:
  ObPxFinishSqcResultMsg()
      : dfo_id_(common::OB_INVALID_ID),
        sqc_id_(common::OB_INVALID_ID),
        rc_(common::OB_SUCCESS),
        task_monitor_info_array_(),
        sqc_affected_rows_(0),
        dml_row_info_(),
        temp_table_id_(common::OB_INVALID_ID),
        interm_result_ids_()
  {}
  virtual ~ObPxFinishSqcResultMsg() = default;
  const sql::TransResult& get_trans_result() const
  {
    return trans_result_;
  }
  sql::TransResult& get_trans_result()
  {
    return trans_result_;
  }
  void reset()
  {
    dfo_id_ = OB_INVALID_ID;
    sqc_id_ = OB_INVALID_ID;
    rc_ = OB_SUCCESS;
    trans_result_.reset();
    task_monitor_info_array_.reset();
    dml_row_info_.reset();
  }
  TO_STRING_KV(K_(dfo_id), K_(sqc_id), K_(rc), K_(sqc_affected_rows));

public:
  int64_t dfo_id_;
  int64_t sqc_id_;
  int rc_;
  sql::TransResult trans_result_;
  ObPxTaskMonitorInfoArray task_monitor_info_array_;
  int64_t sqc_affected_rows_;
  ObPxDmlRowInfo dml_row_info_;
  int64_t temp_table_id_;
  ObSEArray<uint64_t, 8> interm_result_ids_;
};

class ObPxFinishTaskResultMsg : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::FINISH_TASK_RESULT> {
  OB_UNIS_VERSION_V(1);

public:
  ObPxFinishTaskResultMsg()
      : dfo_id_(common::OB_INVALID_ID),
        sqc_id_(common::OB_INVALID_ID),
        task_id_(common::OB_INVALID_ID),
        rc_(common::OB_ERR_UNEXPECTED)
  {}
  virtual ~ObPxFinishTaskResultMsg() = default;
  int assign(const ObPxFinishTaskResultMsg& other)
  {
    dfo_id_ = other.dfo_id_;
    sqc_id_ = other.sqc_id_;
    task_id_ = other.task_id_;
    rc_ = other.rc_;
    return common::OB_SUCCESS;
  }
  void reset()
  {}
  TO_STRING_KV(K_(dfo_id), K_(sqc_id), K_(task_id), K_(rc));

public:
  int64_t dfo_id_;
  int64_t sqc_id_;
  int64_t task_id_;
  int rc_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /*_OB_SQL_PX_DTL_MSG_H_ */
