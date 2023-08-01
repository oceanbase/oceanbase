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

#ifndef OCEANBASE_SQL_EXECUTOR_TASK_INFO_
#define OCEANBASE_SQL_EXECUTOR_TASK_INFO_

#include "sql/engine/ob_operator.h"
#include "sql/executor/ob_task_location.h"
#include "sql/executor/ob_task_id.h"
#include "sql/executor/ob_slice_id.h"
#include "sql/executor/ob_task_event.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace sql
{

//class ObTaskResultV2
//{
//public:
//  ObTaskResultV2() {}
//  virtual ~ObTaskResultV2() {}
//  virtual int64_t get_slice_count() = 0;
//  virtual int get_slice_event(int64_t idx, ObSliceEvent &slice_event) = 0;
//public:
//  void set_task_location(const ObTaskLocation &task_location) { task_location_ = task_location; }
//  ObTaskLocation &get_task_location() { return task_location_; }
//protected:
//  ObTaskLocation task_location_;
//};

//class ObSingleSliceResult : public ObTaskResultV2
//{
//  OB_UNIS_VERSION(1);
//public:
//  ObSingleSliceResult() {}
//  virtual ~ObSingleSliceResult() {}
//  virtual int64_t get_slice_count();
//  virtual int get_slice_event(int64_t idx, ObSliceEvent &slice_event);
//public:
//  void set_slice_event(const ObSliceEvent &slice_event) { slice_event_.assign(slice_event); }
//  int assign(const ObSingleSliceResult &other);
//  TO_STRING_KV(K_(task_location),
//               K_(slice_event));
//private:
//  ObSliceEvent slice_event_;
//};

//class ObMultiSliceResult : public ObTaskResultV2
//{
//public:
//  ObMultiSliceResult() {}
//  virtual ~ObMultiSliceResult() {}
//  virtual int64_t get_slice_count();
//  virtual int get_slice_event(int64_t idx, ObSliceEvent &slice_event);
//public:
//  void set_slice_events(const common::ObIArray<ObSliceEvent> &slice_events) { slice_events_ = &slice_events; }
//private:
//  const common::ObIArray<ObSliceEvent> *slice_events_;
//};

enum ObTaskState
{
  OB_TASK_STATE_NOT_INIT,
  OB_TASK_STATE_INITED,
  OB_TASK_STATE_RUNNING,
  OB_TASK_STATE_FINISHED,
  OB_TASK_STATE_SKIPPED,
  OB_TASK_STATE_FAILED,
};

class ObPhyOperator;
class ObDASTabletLoc;

class ObGranuleTaskInfo
{
public:
	ObGranuleTaskInfo()
	  : ranges_(),
      ss_ranges_(),
	    tablet_loc_(nullptr),
	    task_id_(0)
	{ }
	virtual ~ObGranuleTaskInfo() { }
  int assign(const ObGranuleTaskInfo &other);
	TO_STRING_KV(K_(ranges), K_(ss_ranges), K_(task_id));
public:
  common::ObSEArray<common::ObNewRange, 1> ranges_;
  common::ObSEArray<common::ObNewRange, 1> ss_ranges_;
  ObDASTabletLoc *tablet_loc_;
  //just for print
  int64_t task_id_;
};

// 用于 NLJ 场景下对右侧分区表扫描做 partition pruning
class ObGIPruningInfo
{
public:
  ObGIPruningInfo() : part_id_(common::OB_INVALID_ID) {}

  int64_t get_part_id() const
  {
    return part_id_;
  }
  void set_part_id(int64_t part_id)
  {
    part_id_ = part_id;
  }
private:
  // 不裁剪的分区id，除此之外其它id全部裁剪
  // NLJ 从左侧每读出一行都会更新一次本 part_id
  int64_t part_id_;
};

typedef common::hash::ObHashMap<uint64_t, ObGranuleTaskInfo, common::hash::NoPthreadDefendMode> GIPrepareTaskMap;

class ObTaskInfo
{
public:
  class ObPartLoc
  {
  public:
    ObPartLoc()
      : scan_ranges_(common::ObModIds::OB_SQL_EXECUTOR_TASK_INFO, OB_MALLOC_NORMAL_BLOCK_SIZE),
        part_key_ref_id_(common::OB_INVALID_ID),
        value_ref_id_(common::OB_INVALID_ID),
        renew_time_(common::OB_INVALID_TIMESTAMP),
        row_store_(NULL),
        datum_store_(NULL)
    {
    }
    virtual ~ObPartLoc() {}
    inline void reset()
    {
      scan_ranges_.reset();
      renew_time_ = common::OB_INVALID_TIMESTAMP;
      part_key_ref_id_ = common::OB_INVALID_ID;
      value_ref_id_ = common::OB_INVALID_ID;
      row_store_ = NULL;
      datum_store_ = NULL;
    }
    inline bool is_valid() const
    {
      return common::OB_INVALID_TIMESTAMP != renew_time_;
    }
    int assign(ObPartLoc &other);
    TO_STRING_KV(K_(scan_ranges),
                 K_(part_key_ref_id),
                 K_(value_ref_id),
                 K_(renew_time),
                 KPC_(row_store),
                 KPC_(datum_store));
    common::ObSEArray<common::ObNewRange, 1> scan_ranges_; // scan query ranges
    uint64_t part_key_ref_id_; //用来标识range location中的partition info属于plan中的哪个physical operator
    uint64_t value_ref_id_;
    int64_t renew_time_;
    ObRowStore *row_store_; //该partition对应的row缓存，用于dml语句
    ObChunkDatumStore *datum_store_;
  };
  class ObRangeLocation
  {
  public:
    ObRangeLocation()
      : inner_alloc_("RangeLocation"),
        part_locs_(&inner_alloc_)
    {
    }
    explicit ObRangeLocation(common::ObIAllocator &allocator)
      : part_locs_(allocator),
        server_()
    {
    }
    virtual ~ObRangeLocation() {}
    inline void reset()
    {
      part_locs_.reset();
      server_.reset();
    }
    inline bool is_valid() const
    {
      bool bool_ret = true;
      if (!server_.is_valid() || part_locs_.count() <= 0) {
        bool_ret = false;
      }
      for (int64_t i = 0; true == bool_ret && i < part_locs_.count(); ++i) {
        if (!part_locs_.at(i).is_valid()) {
          bool_ret = false;
        }
      }
      return bool_ret;
    }
    int assign(const ObRangeLocation &location);
    TO_STRING_KV(K_(part_locs), K_(server));
    // 因为一个 task 中可能包含多个 scan 算子，每个 scan 算子对应一个 ObPartLoc
    // 所以 part_locs_ 是一个数组
    common::ModulePageAllocator inner_alloc_;
    common::ObFixedArray<ObTaskInfo::ObPartLoc, common::ObIAllocator> part_locs_;
    common::ObAddr server_;
  };

public:
  explicit ObTaskInfo(common::ObIAllocator &allocator);
  virtual ~ObTaskInfo();
  void set_root_spec(ObOpSpec *root_spec) { root_spec_ = root_spec; }
  ObOpSpec *get_root_spec() const { return root_spec_; }
  // 在远端Task执行完成后给控制节点汇报执行状态
  ObTaskState get_state() const { return state_; }
  void set_state(ObTaskState state) { state_ = state; }
  // 最底层读数据的task
  inline int set_range_location(const ObTaskInfo::ObRangeLocation &range_loc);
  inline const ObTaskInfo::ObRangeLocation &get_range_location() const;
  inline ObTaskInfo::ObRangeLocation &get_range_location();
  inline void set_task_split_type(int64_t task_split_type) { task_split_type_ = task_split_type; }
  inline int64_t get_task_split_type() { return task_split_type_; }
  // 中间结果处理的task
  void set_task_location(const ObTaskLocation &task_loc) { task_location_ = task_loc; }
  const ObTaskLocation &get_task_location() const { return task_location_; }
  ObTaskLocation &get_task_location() { return task_location_; }
  // 获取中间结果所需的数据结构
  inline void set_pull_slice_id(uint64_t pull_slice_id) { pull_slice_id_ = pull_slice_id; }
  inline uint64_t get_pull_slice_id() const { return pull_slice_id_; }
  inline void set_force_save_interm_result(bool force_save_interm_result)
  {
    force_save_interm_result_ = force_save_interm_result;
  }
  inline bool is_force_save_interm_result() const { return force_save_interm_result_; }
  int deep_copy_slice_events(common::ObIAllocator &allocator,
                             const common::ObIArray<ObSliceEvent> &slice_events);
  // 获取位置信息，在LocationList中得下标
  inline void set_location_idx(uint64_t location_idx) { location_idx_ = location_idx; }
  inline uint64_t get_location_idx() const { return location_idx_; }
  inline int init_location_idx_array(int64_t loc_idx_cnt)
  { return location_idx_list_.init(loc_idx_cnt); }
  inline int add_location_idx(uint64_t location_idx)
  { return location_idx_list_.push_back(location_idx); }
  inline const ObIArray<uint64_t> &get_location_idx_list() const
  { return location_idx_list_; }

  int init_sclie_count_array(int64_t array_count) { return slice_count_pos_.init(array_count); }
  int add_slice_count_pos(int64_t slice_count) { return slice_count_pos_.push_back(slice_count); }

  void set_background(const bool v) { background_ = v; }
  bool is_background() const { return background_; }
  inline const ObTaskID &get_ob_task_id() const { return task_location_.get_ob_task_id(); }
  inline uint64_t get_job_id() const { return task_location_.get_job_id(); }
  inline uint64_t get_task_id() const { return task_location_.get_task_id(); }
  inline void set_task_send_begin(int64_t ts)   { ts_task_send_begin_   = ts; }
  inline void set_task_recv_done(int64_t ts)    { ts_task_recv_done_    = ts; }
  inline void set_result_send_begin(int64_t ts) { ts_result_send_begin_ = ts; }
  inline void set_result_recv_done(int64_t ts)  { ts_result_recv_done_  = ts; }
  inline int64_t get_task_send_begin() const   { return ts_task_send_begin_; }
  inline int64_t get_task_recv_done() const    { return ts_task_recv_done_; }
  inline int64_t get_result_send_begin() const { return ts_result_send_begin_; }
  inline int64_t get_result_recv_done() const  { return ts_result_recv_done_; }
  inline int32_t retry_times() const { return retry_times_; }
  inline void inc_retry_times() { retry_times_++; }

  TO_STRING_KV(N_TASK_LOC, task_location_,
               K_(range_location),
               K_(location_idx),
               K_(location_idx_list),
               K_(state),
               K_(slice_count_pos),
               K_(background),
               K_(retry_times),
               K_(location_idx_list));

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTaskInfo);

private:
  /* TODO: 将下面的成员归结为三类INFO更易于理解：
   * 1. TaskInputInfo
   *    > ObRangeLocation range_location_;
   *    > uint64_t pull_slice_id_;
   * 2. TaskActionInfo
   *    > ObPhyOperator *root_op_;
   *    > ObTaskState state_;
   * 3. TaskOutputInfo
   *    > ObTaskLocation task_location_;
   *
   * TaskInputInfo负责记录本Task要消费的数据从哪里来
   * TaskActionInfo负责记录本Task要如何处理数据
   * TsakOutputInfo负责记录Task处理数据的结果保存到了哪里
   * 其中TaskOutputInfo存储的信息对于后继执行的Job很有用，可以作为他们的输入参数，
   * 用于构造他们的TaskInfoInput(ObSliceID)
   */

  /*** 扫描物理表Task必须的数据结构 ****/
  // 记录本Task映射到哪些Partition，以及这些partition&query range对应的server
  ObRangeLocation range_location_;
  int64_t task_split_type_;

  // 记录本Task中的中间结果的位置（ObTaskLocation中的server同时也表示本task要发送到哪台机器上执行）
  // 汇报后提供给上层Task读取
  ObTaskLocation task_location_;

  /*** 读取中间结果Task必须的数据结构 ***/
  // 要拉取的slice id
  uint64_t pull_slice_id_;
  // 中间结果所在的位置在ObReceiveInput::init函数中计算，因此此处不用保存

  // 强制在远端保存中间结果，不在task event中带回来
  bool force_save_interm_result_;
  // task执行的汇报结果
  common::ObSEArray<ObSliceEvent, 1> slice_events_;

  // 获取位置信息，在LocationList中得下标
  uint64_t location_idx_;
  ObFixedArray<uint64_t, common::ObIAllocator> location_idx_list_;

  /*** 通用数据结构 ***/
  ObPhyOperator *root_op_;
  // 本Task的运行状态
  ObTaskState state_;

  ObFixedArray<uint64_t, common::ObIAllocator> slice_count_pos_;
  // run task in background threads.
  bool background_;

  // task 粒度重试，记录重试次数，避免无限重试
  int32_t retry_times_;
private:
  int64_t ts_task_send_begin_;
  int64_t ts_task_recv_done_;
  int64_t ts_result_send_begin_;
  int64_t ts_result_recv_done_;
  ObOpSpec *root_spec_; // for static engine
};

inline int ObTaskInfo::set_range_location(const ObTaskInfo::ObRangeLocation &range_loc)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!range_loc.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_EXE_LOG(WARN, "invalid range location", K(ret), K(range_loc));
  } else {
    ret = range_location_.assign(range_loc);
  }
  return ret;
}

inline const ObTaskInfo::ObRangeLocation &ObTaskInfo::get_range_location() const
{
  return range_location_;
}

inline ObTaskInfo::ObRangeLocation &ObTaskInfo::get_range_location()
{
  return range_location_;
}

}
}
#endif /* OCEANBASE_SQL_EXECUTOR_TASK_INFO_ */
//// end of header file
