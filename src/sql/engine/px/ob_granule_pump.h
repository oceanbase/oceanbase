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

#ifndef OB_GRANULE_PUMP_H_
#define OB_GRANULE_PUMP_H_
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_granule_util.h"
#include "sql/engine/ob_engine_op_traits.h"
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"


namespace oceanbase
{
namespace share {
  struct ObExternalFileInfo;
}
namespace sql
{

class ObTableModifySpec;
class ObPxSqcMeta;

class ObGranulePumpArgs
{
public:
  enum PruningStatus{
    READY_PRUNING = 0,
    FINISH_PRUNING = 1
  };
public:
  class ObGranulePumpOpInfo
  {
  public:
    ObGranulePumpOpInfo() : scan_ops_(), modify_op_(NULL), gi_op_(nullptr) {}
    ~ObGranulePumpOpInfo() { reset(); }
    void reset()
    {
      scan_ops_.reset();
      modify_op_ = NULL;
      gi_op_ = nullptr;
    }
    int push_back_scan_ops(const ObTableScanSpec *tsc)
    { return scan_ops_.push_back(tsc); }
    void init_modify_op(const ObTableModifySpec *modify_op)
    { modify_op_ = modify_op; }

    ObIArray<const ObTableScanSpec *> &get_scan_ops() { return (ObIArray<const ObTableScanSpec *> &)scan_ops_; }
    ObTableModifySpec *get_modify_op() { return (ObTableModifySpec *)modify_op_; }
    int assign(const ObGranulePumpOpInfo &rhs);
    common::ObArray<const ObTableScanSpec*> scan_ops_;
    const ObTableModifySpec* modify_op_;
    const ObGranuleIteratorSpec *gi_op_;
  };
public :
  ObGranulePumpArgs() : ctx_(NULL), op_info_(),
      tablet_arrays_(), run_time_pruning_flags_(),
      cur_tablet_idx_(0), finish_pruning_tablet_idx_(0),
      sharing_iter_end_(false),
      pruning_status_(READY_PRUNING),
      pruning_ret_(OB_SUCCESS),
      px_tablets_info_(), parallelism_(0),
      tablet_size_(0), gi_attri_flag_(0),
      lucky_one_(true),
      query_range_by_runtime_filter_(),
      extract_finished_(false),
      gi_op_id_(common::OB_INVALID_ID),
      pump_version_(0) {}
  virtual ~ObGranulePumpArgs() { reset(); };

  TO_STRING_KV(K(px_tablets_info_),
               K(parallelism_),
               K(tablet_size_),
               K(gi_attri_flag_),
               K_(gi_op_id))

  bool need_partition_granule();
  bool is_finish_pruning() { return pruning_status_ == FINISH_PRUNING; }
  void set_finish_pruning() { pruning_status_ = FINISH_PRUNING; }
  int get_pruning_ret() { return pruning_ret_; }
  void set_pruning_ret(int v) { pruning_ret_ = v; }
  void reset() {
    op_info_.reset();
    tablet_arrays_.reset();
    run_time_pruning_flags_.reset();
    external_table_files_.reset();
    query_range_by_runtime_filter_.reset();
    locations_order_.reset();
  }

  int assign(const ObGranulePumpArgs &rhs);

  int64_t get_pump_version() const { return ATOMIC_LOAD(&pump_version_); }
  int64_t inc_pump_version() { return ATOMIC_FAA(&pump_version_, 1); }

  int64_t try_inc_pump_version(int64_t old_version)
  {
    return ATOMIC_CAS(&pump_version_, old_version, old_version + 1);
  }

  ObExecContext *ctx_;
  ObGranulePumpOpInfo op_info_;
  common::ObArray<DASTabletLocArray> tablet_arrays_;
  //-----for runtime filter pruning granule
  common::ObArray<bool> run_time_pruning_flags_;
  int64_t cur_tablet_idx_;
  int64_t finish_pruning_tablet_idx_;
  bool sharing_iter_end_;
  PruningStatus pruning_status_;
  int pruning_ret_;
  //-----end
  common::ObArray<ObPxTabletInfo> px_tablets_info_;
  common::ObArray<share::ObExternalFileInfo> external_table_files_;
  int64_t parallelism_;
  int64_t tablet_size_;
  uint64_t gi_attri_flag_;
  // -----for runtime filter extract query range
  bool lucky_one_; // atomic, indicatee which thread is luckly to do extract query range
  ObSEArray<ObNewRange, 16> query_range_by_runtime_filter_;
  bool extract_finished_;
  ObSEArray<std::pair<int64_t, bool>, 18> locations_order_;
  int64_t gi_op_id_;
  // %task_version_ is increased when task regenerated.
  // Used to help detecting taskset change in GI.
  int64_t pump_version_;
  //-----end
};

// 引入 TaskSet 的概念，是为了处理一个 GI 下管多张表的场景。
//
// 对于单表扫描来说，ObGITaskSet 中 partition_keys_ 等几个数组里，都只有一个元素
// 对于 Partition Wise 的 N 表扫描（一个 GI 下挂多个 table）场景，ObGITaskSet 中 partition_keys_
// 等几个数组里，有 N 个元素。
class ObGITaskSet {
public:
  struct ObGITaskInfo
  {
    ObGITaskInfo() : tablet_loc_(nullptr), range_(), ss_range_(), idx_(0), hash_value_(0),
                     is_false_range_(false),  scan_task_() {}
    ObGITaskInfo(ObDASTabletLoc *tablet_loc,
                 const common::ObNewRange &range,
                 const common::ObNewRange &ss_range,
                 int64_t idx,
                 bool is_false_range,
                 sql::ObIExtTblScanTask* scan_task) :
        tablet_loc_(tablet_loc), range_(range), ss_range_(ss_range), idx_(idx), hash_value_(0),
        is_false_range_(is_false_range), scan_task_(scan_task) {}
    TO_STRING_KV(KPC(tablet_loc_),
                 KP(tablet_loc_),
                 K(range_),
                 K(ss_range_),
                 K(idx_),
                 K(hash_value_),
                 K(is_false_range_),
                 KPC(scan_task_));
    ObDASTabletLoc *tablet_loc_;
    common::ObNewRange range_;
    common::ObNewRange ss_range_;
    int64_t idx_;
    uint64_t hash_value_;
    bool is_false_range_;
    sql::ObIExtTblScanTask* scan_task_;
  };

  enum ObGIRandomType
  {
    GI_RANDOM_NONE = 0,
    GI_RANDOM_TASK,     // a task may contains many query range witch from the same partition
    GI_RANDOM_RANGE,    // a task have only one query range, it can get the best randomness, but it speed more in rescan
  };

  ObGITaskSet() : gi_task_set_(), cur_pos_(0), task_count_(0) {}
  TO_STRING_KV(K(gi_task_set_), K(cur_pos_), K(task_count_));
  int get_task_at_pos(ObGranuleTaskInfo &info, const int64_t &pos) const;
  int get_task_tablet_id_at_pos(const int64_t &pos, uint64_t &tablet_id) const;

  int get_next_gi_task_pos(int64_t &pos);
  int get_next_gi_task(ObGranuleTaskInfo &info);
  int assign(const ObGITaskSet &other);
  int set_pw_affi_partition_order(bool asc, bool force_reverse);
  int set_block_order(bool asc);
  int construct_taskset(const common::ObIArray<ObDASTabletLoc*> &taskset_tablets,
                        const common::ObIArray<ObNewRange> &taskset_ranges,
                        const common::ObIArray<ObNewRange> &ss_ranges,
                        const common::ObIArray<int64_t> &taskset_idxs,
                        common::ObIArray<sql::ObIExtTblScanTask*> &scan_tasks,
                        ObGIRandomType random_type);
  int append_taskset(ObGITaskSet &other);
public:
  common::ObArray<ObGITaskInfo> gi_task_set_;
  int64_t cur_pos_;
  int64_t task_count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGITaskSet);
};

static const int64_t OB_DEFAULT_GI_TASK_COUNT = 1;
typedef common::ObSEArray<ObGITaskSet, OB_DEFAULT_GI_TASK_COUNT> ObGITaskArray;
typedef common::ObIArray<ObGITaskSet> GITaskIArray;

struct GITaskArrayItem
{
  GITaskArrayItem() :
    tsc_op_id_(common::OB_INVALID_ID),
    no_more_task_from_shared_pool_(false)
  {}

  TO_STRING_KV(K(tsc_op_id_), K(taskset_array_));
  // table scan operator id or insert op id
  // TODO: jiangting.lk 先不修改变量名字，后期统一调整
  uint64_t tsc_op_id_;
  // gi task set array
  ObGITaskArray taskset_array_;
  volatile bool no_more_task_from_shared_pool_;
};

typedef common::ObArray<GITaskArrayItem> GITaskArrayMap;

/*
 * in most cases, the partition wise join has about 2 or 3 table scan below.
 * so eight hash bucket is large enough.
 * unfortunately, if we got more table scan, the hash map still work in a
 * inefficient way.
 * */
static const int64_t PARTITION_WISE_JOIN_TSC_HASH_BUCKET_NUM = 8;

//TODO muhang.zb refactor
typedef common::hash::ObHashMap<uint64_t, ObGITaskSet, common::hash::NoPthreadDefendMode> TaskSetMap;

class ObGranuleSplitter
{
public :
  ObGranuleSplitter() = default;
  virtual ~ObGranuleSplitter() = default;

  static int get_query_range(ObExecContext &ctx,
                             const ObQueryRangeProvider &tsc_pre_query_range,
                             ObIArray<ObNewRange> &ranges,
                             ObIArray<ObNewRange> &ss_ranges,
                             int64_t table_id,
                             int64_t op_id,
                             bool partition_granule,
                             bool with_param_down = false);


protected :
  int split_gi_task(ObGranulePumpArgs &args,
                    const ObTableScanSpec *tsc,
                    int64_t table_id,
                    int64_t op_id,
                    const common::ObIArray<ObDASTabletLoc*> &tablets,
                    bool partition_granule,
                    ObGITaskSet &task_set,
                    ObGITaskSet::ObGIRandomType random_type);

public :
  ObSEArray<ObPxTabletInfo, 8> px_tablets_info_;
};

class ObRandomGranuleSplitter : public ObGranuleSplitter
{
public :
  ObRandomGranuleSplitter() = default;
  virtual ~ObRandomGranuleSplitter() = default;
  int split_granule(ObGranulePumpArgs &args,
                    common::ObIArray<const ObTableScanSpec *> &scan_ops,
                    GITaskArrayMap &gi_task_array_result,
                    bool check_task_exist,
                    ObGITaskSet::ObGIRandomType random_type,
                    bool partition_granule = true);
  int split_granule(ObIAllocator &allocator, ObGranuleIteratorOp *gi_op, ObGranulePumpArgs &args,
                    const ObTableScanSpec *tsc, int64_t op_id, ObGITaskSet &task_set,
                    ObGITaskSet::ObGIRandomType random_type, ObDASTabletLoc *tablet_loc,
                    const ObIArray<ObNewRange> &ranges);

private:
};

class ObAccessAllGranuleSplitter : public ObGranuleSplitter
{
public :
  ObAccessAllGranuleSplitter() = default;
  virtual ~ObAccessAllGranuleSplitter() = default;
  int split_granule(ObGranulePumpArgs &args,
                    common::ObIArray<const ObTableScanSpec *> &scan_ops,
                    GITaskArrayMap &gi_task_array_result,
                    bool check_task_exist,
                    ObGITaskSet::ObGIRandomType random_type,
                    bool partition_granule = true);
private :
  int split_tasks_access_all(ObGITaskSet &taskset, int64_t parallelism, ObGITaskArray &taskset_array);
};

class ObPartitionWiseGranuleSplitter : public ObGranuleSplitter
{
public:
  ObPartitionWiseGranuleSplitter() = default;
  virtual ~ObPartitionWiseGranuleSplitter() = default;
  int split_granule(ObGranulePumpArgs &args,
                    ObIArray<const ObTableScanSpec *> &scan_ops,
                    GITaskArrayMap &gi_task_array_result,
                    ObGITaskSet::ObGIRandomType random_type,
                    bool partition_granule = true);
  // FULL PARITION WISE情况下的任务划分与其他类型的`spliter`有非常大的不同；普通的spliter仅仅需要考虑TSC，
  // 但是PARTITION WISE情况下，有可能需要考虑DML（目前仅仅是INSERT)
  int split_granule(ObGranulePumpArgs &args,
                    ObIArray<const ObTableScanSpec *> &scan_ops,
                    const ObTableModifySpec *modify_op,
                    GITaskArrayMap &gi_task_array_result,
                    ObGITaskSet::ObGIRandomType random_type,
                    bool partition_granule = true);

private:
//  FULL PARTITION WISE划分任务的情况下，有可能需要对INSERT进行划分
//  TSC的任务划分，直接使用`split_gi_task`方法
int split_insert_gi_task(ObGranulePumpArgs &args,
                        const uint64_t insert_table_id,
                        const int64_t row_key_count,
                        const common::ObIArray<ObDASTabletLoc*> &tablets,
                        bool partition_granule,
                        ObGITaskSet &task_set,
                        ObGITaskSet::ObGIRandomType random_type);

int split_tsc_gi_task(ObGranulePumpArgs &args,
                      common::ObIArray<const ObTableScanSpec *> &scan_ops,
                      const common::ObIArray<DASTabletLocArray> &tablet_arrays,
                      int64_t tsc_begin_idx,
                      int64_t task_begin_idxm,
                      GITaskArrayMap &gi_task_array_result,
                      bool partition_granule,
                      ObGITaskSet::ObGIRandomType random_type);

};

class ObAffinitizeGranuleSplitter : public ObGranuleSplitter
{
public :
  ObAffinitizeGranuleSplitter() = default;
  virtual ~ObAffinitizeGranuleSplitter() = default;
protected :
  int split_tasks_affinity(ObExecContext &ctx,
                           ObGITaskSet &taskset,
                           int64_t parallelism,
                           ObGITaskArray &taskset_array);
};

class ObNormalAffinitizeGranuleSplitter : public ObAffinitizeGranuleSplitter
{
public :
  ObNormalAffinitizeGranuleSplitter() = default;
  virtual ~ObNormalAffinitizeGranuleSplitter() = default;
  int split_granule(ObGranulePumpArgs &args,
                    ObIArray<const ObTableScanSpec *> &scan_ops,
                    GITaskArrayMap &gi_task_array_result,
                    ObGITaskSet::ObGIRandomType random_type,
                    bool partition_granule = true);
};

class ObPWAffinitizeGranuleSplitter : public ObAffinitizeGranuleSplitter
{
public:
  ObPWAffinitizeGranuleSplitter() = default;
  virtual ~ObPWAffinitizeGranuleSplitter() = default;
  int split_granule(ObGranulePumpArgs &args,
                    ObIArray<const ObTableScanSpec *> &scan_ops,
                    GITaskArrayMap &gi_task_array_result,
                    ObGITaskSet::ObGIRandomType random_type,
                    bool partition_granule = true);
  int adjust_task_order(bool asc, ObGITaskArray &taskset_array, int64_t tsc_op_id,
                        const ObIArray<std::pair<int64_t, bool>> &locations_order);
};

//A task will be send to many DFO and we use many threads to execute the DFO.
//the ObGranulePump object belong to a specified DFO, and many threads will
//use it to get granule task.
//the worker who revice the DFO will genrate a ObGranulePump object,
//and the worker who end last destroy this object.
class ObGranulePump
{
private:
  static const int64_t OB_GRANULE_SHARED_POOL_POS = 0;

public:
  ObGranulePump() :
  lock_(common::ObLatchIds::SQL_GI_SHARE_POOL_LOCK),
  parallelism_(-1),
  tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
  partition_wise_join_(false),
  gi_task_array_map_(),
  pump_args_(),
  need_partition_pruning_(false),
  pruning_table_locations_(),
  is_taskset_reset_(false),
  fetch_task_ret_(OB_SUCCESS),
  finished_cnt_(0)
  {
  }

  virtual ~ObGranulePump() {
    destroy();
  }

  int init_pump_args(ObExecContext *ctx,
                     ObPxSqcMeta &sqc,
                     const ObGranuleIteratorSpec *gi_op,
                     const ObIArray<const ObTableScanSpec*> &scan_ops,
                     const ObTableModifySpec *modify_op,
                     const common::ObIArray<DASTabletLocArray> &tablet_arrays,
                     common::ObIArray<ObPxTabletInfo> &partitions_info);

  ObGranulePumpArgs *get_granule_pump_arg(const int64_t gi_op_id);
  int add_new_gi_task(ObGranulePumpArgs &args, bool check_task_exist);

  void destroy();

  void reset_task_array();

  int fetch_granule_task(const ObGITaskSet *&task_set,
                         int64_t &pos,
                         ObGranuleTaskInfo &info,
                         int64_t worker_id,
                         uint64_t tsc_op_id,
                         uint64_t fetched_task_cnt,
                         ObGranuleSplitterType splitter_type);
  // 通过phy op ids获得其对应的gi tasks
  int try_fetch_pwj_tasks(ObIArray<ObGranuleTaskInfo> &infos,
                          const ObIArray<int64_t> &op_ids,
                          int64_t worker_id,
                          ObGranuleSplitterType splitter_type);
  bool is_taskset_reset() const { return is_taskset_reset_; }
  void set_fetch_task_ret(int ret) { ATOMIC_STORE(&fetch_task_ret_, ret); }
  int get_fetch_task_ret() const { return ATOMIC_LOAD(&fetch_task_ret_); }
  DECLARE_TO_STRING;
public:

  int regenerate_gi_task();
  int regenerate_gi_task(ObGranulePumpArgs &args);

  int split_granule(ObGranuleIteratorOp *gi_op, int64_t scan_op_id, ObDASTabletLoc *tablet_loc,
                    const ObIArray<ObNewRange> &ranges);

  int reset_gi_task();

  common::ObIArray<ObGranulePumpArgs> &get_pump_args() { return pump_args_; }
  void set_parallelism(int64_t parallelism) { parallelism_ = parallelism; }
  int64_t get_parallelism() const { return parallelism_; }

  inline void set_need_partition_pruning(bool flag) { need_partition_pruning_ = flag; };
  inline bool need_partition_pruning() { return need_partition_pruning_; }
  int set_pruning_table_location(common::ObIArray<ObTableLocation> &table_locations)
  { return pruning_table_locations_.assign(table_locations); }
  common::ObIArray<ObTableLocation> *get_pruning_table_location() { return &pruning_table_locations_; }
  int get_first_tsc_range_cnt(int64_t &cnt);
  const GITaskArrayMap &get_task_array_map() const { return gi_task_array_map_; }
  static int find_task_array_item(GITaskArrayMap &gi_task_map,
                              common::ObIArray<const ObTableScanSpec *> &scan_ops,
                              const bool check_task_exist, int64_t &idx);

#ifdef OB_BUILD_CPP_ODPS
  inline int get_odps_downloader(int64_t part_id, apsara::odps::sdk::IDownloadPtr &downloader) {
    int ret = OB_SUCCESS;
    downloader = NULL;
    ret = odps_partition_downloader_mgr_.get_odps_downloader(part_id, downloader);
    return ret;
  }
  inline ObOdpsPartitionDownloaderMgr::OdpsMgrMap& get_odps_map() {
    return odps_partition_downloader_mgr_.get_odps_map();
  }
  inline bool is_odps_downloader_inited() {  return odps_partition_downloader_mgr_.is_download_mgr_inited(); }
  ObOdpsPartitionDownloaderMgr &get_odps_downloader_mgr() { return odps_partition_downloader_mgr_; }
  ObOdpsPartitionUploaderMgr &get_odps_uploader_mgr() { return odps_partition_uploader_mgr_; }
#endif
#ifdef OB_BUILD_JNI_ODPS
  inline bool is_odps_scanner_mgr_inited() {
    return odps_partition_jni_scanner_mgr_.is_jni_scanner_mgr_inited();
  }
  ObOdpsPartitionJNIDownloaderMgr &get_odps_jni_scanner_mgr() {
    return odps_partition_jni_scanner_mgr_;
  }
  ObOdpsPartitionJNIUploaderMgr &get_odps_jni_uploader_mgr() {
    return odps_jni_uploader_mgr_;
  }
#endif
private:
  int init_external_odps_table_downloader(ObGranulePumpArgs &args);
  int fetch_granule_by_worker_id(const ObGITaskSet *&task_set,
                                 int64_t &pos,
                                 ObGranuleTaskInfo &info,
                                 int64_t thread_id,
                                 uint64_t tsc_op_id);

  int fetch_granule_from_shared_pool(const ObGITaskSet *&task_set,
                                     int64_t &pos,
                                     ObGranuleTaskInfo &info,
                                     uint64_t tsc_op_id,
                                     uint64_t fetched_task_cnt);

  int fetch_pw_granule_by_worker_id(ObIArray<ObGranuleTaskInfo> &infos,
                                    const ObIArray<int64_t> &op_ids,
                                    int64_t thread_id,
                                    ObGranuleSplitterType splitter_type);

  int fetch_pw_granule_from_shared_pool(ObIArray<ObGranuleTaskInfo> &infos,
                                        const ObIArray<int64_t> &op_ids,
                                        ObGranuleSplitterType splitter_type);
  int check_pw_end(int64_t end_tsc_count, int64_t op_count, int64_t task_count);

  int find_taskset_by_tsc_id(uint64_t op_id, GITaskArrayItem *&taskset_array_item);

  int init_arg(ObGranulePumpArgs &arg,
               ObExecContext *ctx,
               const ObGranuleIteratorSpec *gi_op,
               const ObIArray<const ObTableScanSpec*> &scan_ops,
               const common::ObIArray<DASTabletLocArray> &tablet_arrays,
               common::ObIArray<ObPxTabletInfo> &tablets_info,
               const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
               const ObTableModifySpec* modify_op,
               int64_t parallelism,
               int64_t tablet_size,
               uint64_t gi_attri_flag,
               const ObIArray<std::pair<int64_t, bool>> &locations_order);

  int check_can_randomize(ObGranulePumpArgs &args, bool &can_randomize);

  int fill_shared_pool(ObGITaskSet &new_task_set, GITaskArrayItem &taskset_array_item);

private:
  //TODO::muhang 自旋锁还是阻塞锁，又或者按静态划分任务避免锁竞争？
  common::ObSpinLock lock_;
  int64_t parallelism_;
  int64_t tablet_size_;
  bool partition_wise_join_;
  GITaskArrayMap gi_task_array_map_;
#ifdef OB_BUILD_CPP_ODPS
  ObOdpsPartitionDownloaderMgr odps_partition_downloader_mgr_;
  ObOdpsPartitionUploaderMgr odps_partition_uploader_mgr_;
#endif
#ifdef OB_BUILD_JNI_ODPS
  ObOdpsPartitionJNIDownloaderMgr odps_partition_jni_scanner_mgr_;
  ObOdpsPartitionJNIUploaderMgr         odps_jni_uploader_mgr_;
#endif
  common::ObArray<ObGranulePumpArgs> pump_args_;
  bool need_partition_pruning_;
  common::ObArray<ObTableLocation> pruning_table_locations_;


  bool is_taskset_reset_;
  // when granule tasks are fetched concurrently, if one thread failed to fetch task,
  // others should not fetch tasks any more.
  int fetch_task_ret_;
  uint64_t finished_cnt_;
};

}//sql
}//namespace

#endif
