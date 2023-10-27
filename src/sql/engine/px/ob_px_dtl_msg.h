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
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "common/row/ob_row.h"
#include "lib/compress/ob_compress_util.h"
#include "storage/tx/ob_trans_define.h"
#include "sql/engine/ob_exec_feedback_info.h"
#include "sql/ob_sql_define.h"

namespace oceanbase
{
namespace sql
{

typedef obrpc::ObRpcResultCode ObPxUserErrorMsg;

struct ObPxTabletInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPxTabletInfo() : tablet_id_(), logical_row_count_(0), physical_row_count_(0) {}
  virtual ~ObPxTabletInfo() = default;
  void assign(const ObPxTabletInfo &partition_info) {
    tablet_id_ = partition_info.tablet_id_;
    logical_row_count_ = partition_info.logical_row_count_;
    physical_row_count_ = partition_info.physical_row_count_;
  }
  TO_STRING_KV(K_(tablet_id), K_(logical_row_count), K_(physical_row_count));
  int64_t tablet_id_;
  int64_t logical_row_count_;
  int64_t physical_row_count_;
};
struct ObPxDmlRowInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPxDmlRowInfo() : row_match_count_(0), row_duplicated_count_(0), row_deleted_count_(0) {}
  ~ObPxDmlRowInfo() = default;
  void reset()
  {
    row_match_count_ = 0;
    row_duplicated_count_ = 0;
    row_deleted_count_ = 0;
  }
  void set_px_dml_row_info(const ObPhysicalPlanCtx &plan_ctx);
  void add_px_dml_row_info(ObPxDmlRowInfo &row_info)
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

// keep for compatiblity. never should be used anymore
class ObPxTaskMonitorInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPxTaskMonitorInfo() : sched_exec_time_start_(0), sched_exec_time_end_(0),
                          exec_time_start_(0), exec_time_end_(0) {}
  ObPxTaskMonitorInfo &operator = (const ObPxTaskMonitorInfo &other)
  {
    sched_exec_time_start_ = other.sched_exec_time_start_;
    sched_exec_time_end_ = other.sched_exec_time_end_;
    exec_time_start_ = other.exec_time_start_;
    exec_time_end_ = other.exec_time_end_;
    metrics_.assign(other.metrics_);
    return *this;
  }
  TO_STRING_KV(K_(sched_exec_time_start), K_(sched_exec_time_end), K_(exec_time_start), K_(exec_time_end), K(metrics_.count()));
private:
  int64_t sched_exec_time_start_;          //sqc观察到的task执行开始时间戳
  int64_t sched_exec_time_end_;            //sqc观察到的task执行结束时间戳
  int64_t exec_time_start_;                //task执行开始时间
  int64_t exec_time_end_;                  //task执行结束时间
  common::ObSEArray<sql::ObOpMetric, 1> metrics_;     // operator metric
};

typedef common::ObSEArray<ObPxTaskMonitorInfo, 1> ObPxTaskMonitorInfoArray;

// 每个 Task 都有一组输出 channel，对接 consumer DFO 的所有 Task
class ObPxTaskChSet : public dtl::ObDtlChSet
{
  OB_UNIS_VERSION(1);
public:
  ObPxTaskChSet() : sqc_id_(common::OB_INVALID_INDEX), task_id_(common::OB_INVALID_INDEX),
  sm_group_id_(common::OB_INVALID_INDEX) {}
  ~ObPxTaskChSet() = default;
  void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  int64_t get_sqc_id() const { return sqc_id_; }
  void set_task_id(int64_t task_id) { task_id_ = task_id; }
  int64_t get_task_id() const { return task_id_; }
  void set_sm_group_id(int64_t sm_group_id) { sm_group_id_ = sm_group_id; }
  int64_t get_sm_group_id() const { return sm_group_id_; }
  int assign(const ObPxTaskChSet &ch_set);
private:
  int64_t sqc_id_;
  int64_t task_id_;
  int64_t sm_group_id_;
};

// 每个 SQC 都包含多个 Task，这个结构用于记录它们的全部 channel
typedef common::ObArray<ObPxTaskChSet,
                        common::ModulePageAllocator,
                        false, /*auto free*/
                        common::ObArrayDefaultCallBack<ObPxTaskChSet>,
                        common::DefaultItemEncode<ObPxTaskChSet> > ObPxTaskChSets;
typedef common::ObArray<dtl::ObDtlChTotalInfo,
                        common::ModulePageAllocator,
                        false, /*auto free*/
                        common::ObArrayDefaultCallBack<dtl::ObDtlChTotalInfo>,
                        common::DefaultItemEncode<dtl::ObDtlChTotalInfo> > ObPxChTotalInfos;

class ObPxBloomFilterChInfo : public dtl::ObDtlChTotalInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPxBloomFilterChInfo() : filter_id_(common::OB_INVALID_INDEX) {}

  void set_filter_id(int64_t filter_id) { filter_id_ = filter_id; }
  int64_t get_filter_id() const { return filter_id_; }
  int assign(const ObPxBloomFilterChInfo &other)
  {
    filter_id_ = other.filter_id_;
    return dtl::ObDtlChTotalInfo::assign(other);
  }
private:
  int64_t filter_id_;
};

class ObPxBloomFilterChSet : public dtl::ObDtlChSet
{
  OB_UNIS_VERSION(1);
public:
  ObPxBloomFilterChSet() : filter_id_(common::OB_INVALID_INDEX),
      sqc_id_(common::OB_INVALID_INDEX) {};
  ~ObPxBloomFilterChSet() = default;
  void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  int64_t get_sqc_id() const { return sqc_id_; }
  void set_filter_id(int64_t filter_id) { filter_id_ = filter_id; }
  int64_t get_filter_id() const { return filter_id_; }
  int assign(const ObPxBloomFilterChSet &ch_set);
private:
  int64_t filter_id_;
  int64_t sqc_id_;
};
typedef common::ObArray<ObPxBloomFilterChSet,
                        common::ModulePageAllocator,
                        false, /*auto free*/
                        common::ObArrayDefaultCallBack<ObPxBloomFilterChSet>,
                        common::DefaultItemEncode<ObPxBloomFilterChSet> > ObPxBloomFilterChSets;

// partition map格式：
// 老的形式： first: tablet_id second: 全局task_idx
// 新的形式： 2种情况
//        case1: first：tablet_id, second: prefix_task_count, third: sqc_task_idx
//               case1是普通partition wise join下的场景，之前为了获取task_idx重新遍历了所有task，代价比较高
//               新的方式则仅仅记录 prefix_task_count 和 sqc的task_idx，只要两者之和就可以得到真正的task_idx
//        case2: first：tablet_id, second: prefix_task_count
//               在slave mapping等场景，partition是发送到sqc所有worker，所以这里只记录了sqc的sqc_id
//               worker在后续拿的时候直接构建整个sqc所有worker之间的映射即可
struct ObPxPartChMapItem
{
  OB_UNIS_VERSION(1);
public:
  ObPxPartChMapItem(int64_t first, int64_t second) : first_(first), second_(second), third_(INT64_MAX) {}
  ObPxPartChMapItem(int64_t first, int64_t second, int64_t third)
    : first_(first), second_(second), third_(third)
  {}
  ObPxPartChMapItem() : first_(0), second_(0), third_(INT64_MAX) {}
  int assign(const ObPxPartChMapItem &other) {
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

typedef common::ObArray<ObPxPartChMapItem,
                        common::ModulePageAllocator,
                        false, /*auto free*/
                        common::ObArrayDefaultCallBack<ObPxPartChMapItem>,
                        common::DefaultItemEncode<ObPxPartChMapItem> > ObPxPartChMapArray;
typedef sql::ObTMArray<ObPxPartChMapItem,
                       common::ModulePageAllocator,
                       false, /*auto free*/
                       common::ObArrayDefaultCallBack<ObPxPartChMapItem>,
                       common::DefaultItemEncode<ObPxPartChMapItem> > ObPxPartChMapTMArray;
typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> ObPxPartChMap;


struct ObPxPartChInfo
{
  ObPxPartChInfo() : part_ch_array_() {}
  ~ObPxPartChInfo() = default;
  ObPxPartChMapTMArray part_ch_array_;
};

class ObPxReceiveDataChannelMsg
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::PX_RECEIVE_DATA_CHANNEL>
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxReceiveDataChannelMsg() :  child_dfo_id_(-1), ch_sets_(),
    ch_total_info_(), has_filled_channel_(false) {}
  virtual ~ObPxReceiveDataChannelMsg() = default;
  int set_payload(int64_t child_dfo_id, ObPxTaskChSets &ch_sets)
  {
    child_dfo_id_ = child_dfo_id;
    return ch_sets_.assign(ch_sets);
  }
  int set_payload(int64_t child_dfo_id, dtl::ObDtlChTotalInfo &ch_total_infos)
  {
    ch_total_info_.reset();
    has_filled_channel_ = true;
    child_dfo_id_ = child_dfo_id;
    return ch_total_info_.assign(ch_total_infos);
  }
  int assign(const ObPxReceiveDataChannelMsg &other)
  {
    int ret = common::OB_SUCCESS;
    child_dfo_id_ = other.child_dfo_id_;
    has_filled_channel_ = other.has_filled_channel_;
    if (OB_FAIL(ch_sets_.assign(other.ch_sets_))) {
    } else if (OB_FAIL(ch_total_info_.assign(other.ch_total_info_))) {
    }
    return ret;
  }
  void reset()
  {
    ch_sets_.reset();
    ch_total_info_.reset();
    has_filled_channel_ = false;
  }
  int64_t get_child_dfo_id() const { return child_dfo_id_; }
  ObPxTaskChSets &get_ch_sets() { return ch_sets_; }
  const ObPxTaskChSets &get_ch_sets() const { return ch_sets_; }
  dtl::ObDtlChTotalInfo &get_ch_total_info() { return ch_total_info_; }

  bool has_filled_channel() const { return 0 < ch_sets_.count() || has_filled_channel_;}
  bool is_valid() const
  {
    return ch_total_info_.is_valid();
  }
  TO_STRING_KV(K_(child_dfo_id), K_(ch_sets), K_(ch_total_info));
private:
  // 通过 child_dfo_id 来判断本 ch_sets_
  // 属于哪个 ReceiveOp (一个DFO可以有多个ReceiveOp)
  int64_t child_dfo_id_;
  ObPxTaskChSets ch_sets_;
  dtl::ObDtlChTotalInfo ch_total_info_;
  bool has_filled_channel_;
};


class ObPxTransmitDataChannelMsg
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::PX_TRANSMIT_DATA_CHANNEL>
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxTransmitDataChannelMsg()
    : ch_sets_(), ch_total_info_(), part_affinity_map_(), has_filled_channel_(false)
  {
  }
  virtual ~ObPxTransmitDataChannelMsg() = default;
  void reset()
  {
    ch_sets_.reset();
    ch_total_info_.reset();
    part_affinity_map_.reset();
    has_filled_channel_ = false;
  }
  int set_payload(const ObPxTaskChSets &ch_sets, const ObPxPartChMapArray &map)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ch_sets_.assign(ch_sets))) {
      // fail
    } else if (OB_FAIL(part_affinity_map_.assign(map))) {
      // fail
    }
    return ret;
  }
  int set_payload(const dtl::ObDtlChTotalInfo &ch_total_info, const ObPxPartChMapArray &map)
  {
    int ret = common::OB_SUCCESS;
    has_filled_channel_ = true;
    if (OB_FAIL(ch_total_info_.assign(ch_total_info))) {
    } else if (OB_FAIL(part_affinity_map_.assign(map))) {
    }
    return ret;
  }
  int set_payload(
    const ObPxTaskChSets &ch_sets,
    const dtl::ObDtlChTotalInfo &ch_total_infos,
    const ObPxPartChMapArray &map)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ch_total_info_.assign(ch_total_infos))) {
    } else if (OB_FAIL(part_affinity_map_.assign(map))) {
    } else if (OB_FAIL(ch_sets_.assign(ch_sets))) {
    }
    return ret;
  }
  int assign(const ObPxTransmitDataChannelMsg &other)
  {
    has_filled_channel_ = other.has_filled_channel_;
    return set_payload(other.ch_sets_, other.ch_total_info_, other.part_affinity_map_);
  }
  ObPxTaskChSets &get_ch_sets() { return ch_sets_; }
  const ObPxTaskChSets &get_ch_sets() const { return ch_sets_; }
  ObPxPartChMapArray &get_part_affinity_map() { return part_affinity_map_; }
  dtl::ObDtlChTotalInfo &get_ch_total_info() { return ch_total_info_; }
  bool has_filled_channel() const { return ch_sets_.count() > 0 || has_filled_channel_; }

  TO_STRING_KV(K_(ch_sets), K_(part_affinity_map), K_(ch_total_info));
private:
  ObPxTaskChSets ch_sets_;
  dtl::ObDtlChTotalInfo ch_total_info_;
  ObPxPartChMapArray part_affinity_map_; // partition wise join 时寻址 channel
  bool has_filled_channel_;
};

class ObPxInitSqcResultMsg
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::INIT_SQC_RESULT>
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxInitSqcResultMsg()
      : dfo_id_(common::OB_INVALID_ID),
        sqc_id_(common::OB_INVALID_ID),
        rc_(common::OB_SUCCESS),
        task_count_(0),
        err_msg_(),
        sqc_order_gi_tasks_(false) {}
  virtual ~ObPxInitSqcResultMsg() = default;
  void reset() {}
  TO_STRING_KV(K_(dfo_id), K_(sqc_id), K_(rc), K_(task_count));
public:
  int64_t dfo_id_;
  int64_t sqc_id_;
  int rc_; // 错误码
  int64_t task_count_;
  ObPxUserErrorMsg err_msg_; // for error msg & warning msg
  // No need to serialize
  ObSEArray<ObPxTabletInfo, 8> tablets_info_;
  bool sqc_order_gi_tasks_;
};



class ObPxFinishSqcResultMsg
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::FINISH_SQC_RESULT>
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxFinishSqcResultMsg()
      : dfo_id_(common::OB_INVALID_ID),
        sqc_id_(common::OB_INVALID_ID),
        rc_(common::OB_SUCCESS),
        das_retry_rc_(common::OB_SUCCESS),
        task_monitor_info_array_(),
        sqc_affected_rows_(0),
        dml_row_info_(),
        temp_table_id_(common::OB_INVALID_ID),
        interm_result_ids_(),
        fb_info_(),
        err_msg_() {}
  virtual ~ObPxFinishSqcResultMsg() = default;
  const transaction::ObTxExecResult &get_trans_result() const { return trans_result_; }
  transaction::ObTxExecResult &get_trans_result() { return trans_result_; }
  void reset()
  {
    dfo_id_ = common::OB_INVALID_ID;
    sqc_id_ = common::OB_INVALID_ID;
    rc_ = common::OB_SUCCESS;
    das_retry_rc_ = common::OB_SUCCESS;
    trans_result_.reset();
    task_monitor_info_array_.reset();
    dml_row_info_.reset();
    fb_info_.reset();
    err_msg_.reset();
  }
  TO_STRING_KV(K_(dfo_id), K_(sqc_id), K_(rc), K_(das_retry_rc), K_(sqc_affected_rows));
public:
  int64_t dfo_id_;
  int64_t sqc_id_;
  int rc_; // 错误码
  int das_retry_rc_; //record the error code that cause DAS to retry
  transaction::ObTxExecResult trans_result_;
  ObPxTaskMonitorInfoArray task_monitor_info_array_; // deprecated, keep for compatiblity
  int64_t sqc_affected_rows_; // pdml情况下，一个sqc 影响的行数
  ObPxDmlRowInfo dml_row_info_; // SQC存在DML算子时, 需要统计行 信息
  uint64_t temp_table_id_;
  ObSEArray<uint64_t, 8> interm_result_ids_;
  ObExecFeedbackInfo fb_info_;
  ObPxUserErrorMsg err_msg_; // for error msg & warning msg
};

class ObPxFinishTaskResultMsg
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::FINISH_TASK_RESULT>
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxFinishTaskResultMsg()
      : dfo_id_(common::OB_INVALID_ID),
        sqc_id_(common::OB_INVALID_ID),
        task_id_(common::OB_INVALID_ID),
        rc_(common::OB_ERR_UNEXPECTED) {}
  virtual ~ObPxFinishTaskResultMsg() = default;
  int assign(const ObPxFinishTaskResultMsg &other)
  {
    dfo_id_ = other.dfo_id_;
    sqc_id_ = other.sqc_id_;
    task_id_ = other.task_id_;
    rc_ = other.rc_;
    return common::OB_SUCCESS;
  }
  void reset() {}
  TO_STRING_KV(K_(dfo_id), K_(sqc_id), K_(task_id), K_(rc));
public:
  int64_t dfo_id_;
  int64_t sqc_id_;
  int64_t task_id_;
  int rc_;
};

class ObPxCreateBloomFilterChannelMsg
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::PX_BLOOM_FILTER_CHANNEL>
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxCreateBloomFilterChannelMsg() : ch_set_info_(), sqc_id_(INT64_MAX), sqc_count_(0) {}
  virtual ~ObPxCreateBloomFilterChannelMsg() = default;
  int assign(const ObPxCreateBloomFilterChannelMsg &other)
  {
    int ret = OB_SUCCESS;
    sqc_count_ = other.sqc_count_;
    sqc_id_ = other.sqc_id_;
    if (OB_FAIL(ch_set_info_.assign(other.ch_set_info_))) {
    }
    return ret;
  }
  void reset()
  {
    ch_set_info_.reset();
    sqc_count_ = 0;
    sqc_id_ = INT64_MAX;
  }
  TO_STRING_KV(K_(ch_set_info), K_(sqc_id), K_(sqc_count));
public:
  ObPxBloomFilterChInfo ch_set_info_;
  int64_t sqc_id_;
  int64_t sqc_count_;
};

class ObPxBloomFilterData: public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::PX_BLOOM_FILTER_DATA>
{
  OB_UNIS_VERSION_V(1);
public:
   ObPxBloomFilterData() : filter_(), tenant_id_(common::OB_INVALID_TENANT_ID),
       filter_id_(common::OB_INVALID_ID), server_id_(common::OB_INVALID_ID),
       px_sequence_id_(common::OB_INVALID_ID), bloom_filter_count_(0) {}
  virtual ~ObPxBloomFilterData() = default;
  void reset()
  {
    filter_.reset_filter();
    tenant_id_ = OB_INVALID_TENANT_ID;
    filter_id_ = OB_INVALID_ID;
    server_id_ = OB_INVALID_ID;
    px_sequence_id_ = OB_INVALID_ID;
    bloom_filter_count_ = 0;
  }
  TO_STRING_KV(K_(filter), K_(server_id), K_(px_sequence_id));
public:
  ObPxBloomFilter filter_;
  int64_t tenant_id_;
  int64_t filter_id_;
  int64_t server_id_;
  int64_t px_sequence_id_;
  int64_t bloom_filter_count_;
};

class ObJoinFilterDataCtx
{
public:
  ObJoinFilterDataCtx()
    : ch_set_(), ch_set_info_(), filter_ready_(false), filter_data_(NULL), ch_provider_ptr_(0), filter_id_(common::OB_INVALID_ID),
      bf_idx_at_sqc_proxy_(-1),
      compressor_type_(common::ObCompressorType::NONE_COMPRESSOR) {}
  ~ObJoinFilterDataCtx() = default;
  TO_STRING_KV(K_(filter_ready));
public:
  ObPxBloomFilterChSet ch_set_;
  ObPxBloomFilterChInfo ch_set_info_;
  bool filter_ready_;
  ObPxBloomFilterData *filter_data_;
  uint64_t ch_provider_ptr_;
  int64_t filter_id_;
  int64_t bf_idx_at_sqc_proxy_;
  common::ObCompressorType compressor_type_;
};

class ObPxTabletRange final
{
  OB_UNIS_VERSION(1);
public:
  ObPxTabletRange();
  ~ObPxTabletRange() = default;
  void reset();
  bool is_valid() const;
  template <bool use_allocator>
  int deep_copy_from(const ObPxTabletRange &other, common::ObIAllocator &allocator,
                     char *buf, int64_t size, int64_t &pos);
  int assign(const ObPxTabletRange &other);
  int64_t get_range_col_cnt() const { return range_cut_.empty() ? 0 :
      range_cut_.at(0).count(); }
  TO_STRING_KV(K_(tablet_id), K_(range_cut));
public:
  static const int64_t DEFAULT_RANGE_COUNT = 8;
  typedef common::ObSEArray<common::ObRowkey, DEFAULT_RANGE_COUNT> EndKeys;
  typedef ObTMSegmentArray<ObDatum> DatumKey;
  typedef ObSEArray<int64_t, 2> RangeWeight;
  typedef ObTMArray<DatumKey> RangeCut; // not include MAX at last nor MIN at first
  typedef ObSEArray<RangeWeight, DEFAULT_RANGE_COUNT> RangeWeights;

  int64_t tablet_id_;
  int64_t range_weights_;
  RangeCut range_cut_;
};

template <bool use_allocator>
int ObPxTabletRange::deep_copy_from(const ObPxTabletRange &other, common::ObIAllocator &allocator,
                                    char *buf, int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  reset();
  tablet_id_ = other.tablet_id_;
  range_weights_ = other.range_weights_;
  if (OB_FAIL(range_cut_.reserve(other.range_cut_.count()))) {
    SQL_LOG(WARN, "reserve end keys failed", K(ret), K(other.range_cut_.count()));
  }
  DatumKey copied_key;
  RangeWeight range_weight;
  ObDatum tmp_datum;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.range_cut_.count(); ++i) {
    const DatumKey &cur_key = other.range_cut_.at(i);
    copied_key.reuse();
    range_weight.reuse();
    for (int64_t j = 0; OB_SUCC(ret) && j < cur_key.count(); ++j) {
      if (use_allocator && OB_FAIL(tmp_datum.deep_copy(cur_key.at(j), allocator))) {
        SQL_LOG(WARN, "deep copy datum failed", K(ret), K(i), K(j), K(cur_key.at(j)));
      } else if (!use_allocator && OB_FAIL(tmp_datum.deep_copy(cur_key.at(j), buf, size, pos))) {
        SQL_LOG(WARN, "deep copy datum failed", K(ret), K(i), K(j), K(cur_key.at(j)), K(size), K(pos));
      } else if (OB_FAIL(copied_key.push_back(tmp_datum))) {
        SQL_LOG(WARN, "push back datum failed", K(ret), K(i), K(j), K(tmp_datum));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(range_cut_.push_back(copied_key))) {
      SQL_LOG(WARN, "push back rowkey failed", K(ret), K(copied_key), K(i));
    }
  }
  return ret;
}

}
}
#endif /*_OB_SQL_PX_DTL_MSG_H_ */
