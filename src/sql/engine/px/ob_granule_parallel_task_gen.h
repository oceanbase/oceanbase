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

#ifndef OB_GRANULE_PARALLEL_TASK_GEN_H_
#define OB_GRANULE_PARALLEL_TASK_GEN_H_

#include <utility>
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/ob_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/das/ob_das_define.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/executor/ob_task_info.h"

namespace oceanbase
{
namespace sql
{
class ObGranuleIteratorOp;
class ObGranulePump;
struct ObGranuleTaskInfo;
struct ObCsvGamblingResult {
  bool is_success_;
  bool is_gambling_end_with_escaped_;  // 传递给 full scan 阶段
  int64_t file_id_;
  int64_t chunk_idx_;
  int64_t chunk_cnt_;
  int64_t bounded_start_pos_;
  int64_t end_pos_;
  int64_t enclosed_cnt_;
  int64_t even_pos_;
  int64_t odd_pos_;
  int64_t next_start_pos_;
  ObDASTabletLoc *tablet_loc_;
  sql::ObIExtTblScanTask *scan_task_;

  ObCsvGamblingResult()
    : is_success_(false), is_gambling_end_with_escaped_(false),
      file_id_(0), chunk_idx_(0), chunk_cnt_(0),
      bounded_start_pos_(0), end_pos_(0),
      enclosed_cnt_(0), even_pos_(OB_INVALID_INDEX),
      odd_pos_(OB_INVALID_INDEX), next_start_pos_(0),
      tablet_loc_(nullptr), scan_task_(nullptr)
  {}
  TO_STRING_KV(K_(is_success), K_(is_gambling_end_with_escaped), K_(file_id),
              K_(chunk_idx), K_(chunk_cnt),
              K_(bounded_start_pos), K_(end_pos),
              K_(enclosed_cnt), K_(even_pos),
              K_(odd_pos), K_(next_start_pos));
};

struct ObCsvFullScanResult {
  int64_t file_id_;
  int64_t chunk_idx_;
  int64_t chunk_cnt_;
  int64_t enclosed_cnt_;
  int64_t even_pos_;
  int64_t odd_pos_;
  ObDASTabletLoc *tablet_loc_;
  sql::ObIExtTblScanTask *scan_task_;
  ObCsvFullScanResult()
    : file_id_(0), chunk_idx_(0),
      chunk_cnt_(0), enclosed_cnt_(0),
      even_pos_(OB_INVALID_INDEX), odd_pos_(OB_INVALID_INDEX),
      tablet_loc_(nullptr), scan_task_(nullptr)
  {}
  TO_STRING_KV(K_(file_id), K_(chunk_idx), K_(chunk_cnt),
               K_(enclosed_cnt), K_(even_pos), K_(odd_pos));
};

// 公共的CSV任务结果分组处理辅助函数
// 对结果按 file_id 和 chunk_idx 排序，然后按 file_id 分组，对每组调用回调函数
template<typename ResultType>
struct CsvTaskResultCmp {
  bool operator()(const ResultType &a, const ResultType &b) {
    if (a.file_id_ != b.file_id_) {
      return a.file_id_ < b.file_id_;
    } else {
      return a.chunk_idx_ < b.chunk_idx_;
    }
  }
};
template<typename ResultType, typename Processor>
int process_csv_tasks_by_file_id(ObIArray<ResultType> &all_task_results,
                                 Processor &processor) {
  int ret = OB_SUCCESS;
  // 按 file_id 和 chunk_idx 排序
  if (all_task_results.count() > 0) {
    lib::ob_sort(&all_task_results.at(0), &all_task_results.at(0) + all_task_results.count(), CsvTaskResultCmp<ResultType>());
  }

  // 按 file_id 分组处理
  int64_t task_cnt = all_task_results.count();
  int64_t task_idx = 0;
  while (task_idx < task_cnt && OB_SUCC(ret)) {
    int64_t start_idx = task_idx;
    int64_t cur_file_id = all_task_results.at(task_idx).file_id_;

    // 找出所有属于同一个 file_id 的任务
    while (task_idx < task_cnt && all_task_results.at(task_idx).file_id_ == cur_file_id) {
      ++task_idx;
    }

    int64_t end_idx = task_idx - 1;
    if (OB_FAIL(processor(all_task_results, start_idx, end_idx))) {
      SQL_LOG(WARN, "failed to process file group", K(ret), K(start_idx), K(end_idx), K(cur_file_id));
    }
  }

  return ret;
}

// 公共的CSV边界扫描辅助结构，抽取出全量扫描的逻辑，避免代码重复
struct CsvFullScanBoundHelper {
  CsvFullScanBoundHelper()
    : is_escaped_(false),
      enclosed_cnt_(0),
      even_pos_(OB_INVALID_INDEX),
      odd_pos_(OB_INVALID_INDEX),
      already_read_size_(0)
  {}

  inline void process_char(char ch, int64_t relative_pos,
                           const ObExternalFileFormat &file_format) {
    if (is_escaped_) {
      is_escaped_ = false;
    } else if (ch == file_format.csv_format_.field_enclosed_char_) {
      ++enclosed_cnt_;
    } else if (ch == file_format.csv_format_.field_escaped_char_) {
      is_escaped_ = true;
    } else if (relative_pos != 0  // never choose line term as the first char of a chunk
               && !file_format.csv_format_.line_term_str_.empty()
               && ch == file_format.csv_format_.line_term_str_.ptr()[0]) {
      if (enclosed_cnt_ % 2 == 0 && even_pos_ == OB_INVALID_INDEX) {
        even_pos_ = relative_pos;
      } else if (enclosed_cnt_ % 2 == 1 && odd_pos_ == OB_INVALID_INDEX) {
        odd_pos_ = relative_pos;
      }
    }
  }

  int do_full_scan_boundary(const ObExternalFileFormat &file_format,
                            char *buf, int64_t content_len) {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < content_len; ++i) {
      process_char(buf[i], already_read_size_ + i, file_format);
    }
    already_read_size_ += content_len;
    return ret;
  }

  bool is_escaped_;
  int64_t enclosed_cnt_;
  int64_t even_pos_;
  int64_t odd_pos_;
  int64_t already_read_size_;
};

struct GamblingFunctor {
  GamblingFunctor()
    : is_inited_(false),
      has_enclosed_char_(true),
      is_finished_(false),
      is_success_(false),
      finish_cnt_(0),
      target_finish_cnt_(START_STATE_CNT),
      succ_idx_(OB_INVALID_INDEX),
      bound_helper_()
  {}
  enum struct CSVState {
    INVALID = -1,
    RECORD_START,
    FIELD_START,
    UNQUOTED_FIELD,
    QUOTED_FIELD,
    QUOTE_END,
    ESCAPED,
    SPECIAL,
    MAX_STATE,
  };

  enum struct CSVCharCategory {
    INVALID = -1,
    ENCLOSING_CHAR,
    FIELD_DELIMITER,
    LINE_DELIMITER,
    ESCAPE_CHAR,
    OTHER,
    MAX_TYPE,
  };

  static constexpr int64_t START_UNQUOTED_FIELD_IDX = 0;
  static constexpr int64_t START_QUOTED_FIELD_IDX = 1;
  static constexpr int64_t MAX_VALID_STATE_CNT = 6;
  static constexpr int64_t MAX_INPUT_CNT = 5;
  static constexpr int64_t START_STATE_CNT = 2;
    /*
  * 状态转移矩阵
  * i行j列表示在第i行状态遇到第j类字符应该转移到的状态
  *                   enclosed field_delimiter line_delimiter escape_char other
  * RECORD_START(R)     Q           F               R             S         U
  * FIELD_START(F)      Q           F               R             S         U
  * UNQUOTED_FIELD(U)   -           F               R             S         U
  * QUOTED_FIELD(Q)     E           Q               Q             S         Q
  * QUOTE_END(E)        Q           F               R             -         -
  * ESCAPED(S)          P           P               P             P         P
  */
  static constexpr CSVState state_transition_matrix_[MAX_VALID_STATE_CNT][MAX_INPUT_CNT] = {
    {CSVState::QUOTED_FIELD, CSVState::FIELD_START, CSVState::RECORD_START, CSVState::ESCAPED, CSVState::UNQUOTED_FIELD},
    {CSVState::QUOTED_FIELD, CSVState::FIELD_START, CSVState::RECORD_START, CSVState::ESCAPED, CSVState::UNQUOTED_FIELD},
    {CSVState::INVALID, CSVState::FIELD_START, CSVState::RECORD_START, CSVState::ESCAPED, CSVState::UNQUOTED_FIELD},
    {CSVState::QUOTE_END, CSVState::QUOTED_FIELD, CSVState::QUOTED_FIELD, CSVState::ESCAPED, CSVState::QUOTED_FIELD},
    {CSVState::QUOTED_FIELD, CSVState::FIELD_START, CSVState::RECORD_START, CSVState::INVALID, CSVState::INVALID},
    {CSVState::SPECIAL, CSVState::SPECIAL, CSVState::SPECIAL, CSVState::SPECIAL, CSVState::SPECIAL},
  };
  int init(const ObExternalFileFormat &file_format);

  int update_state(const CSVCharCategory category, const int64_t idx);

  int verify_gambling_success();

  CSVCharCategory get_char_category(char ch, const ObExternalFileFormat &file_format);

  void print_state() const;

  int operator()(const ObExternalFileFormat &file_format, char *buf, int64_t content_len);
  bool is_inited_;
  bool has_enclosed_char_;
  bool is_finished_;
  bool is_success_;
  int64_t finish_cnt_;
  int64_t target_finish_cnt_;
  int64_t succ_idx_;
  int64_t potential_start_pos_[START_STATE_CNT];
  CSVState prev_states_[START_STATE_CNT];
  CSVState cur_states_[START_STATE_CNT];
  CsvFullScanBoundHelper bound_helper_;
};

const static char * MEM_ATTR_EXT_TASK_GEN = "ExtTaskGen";
struct GIExternalTaskArgs {
  ObSEArray<ObDASTabletLoc *, 16> taskset_tablets_;
  ObSEArray<sql::ObIExtTblScanTask *, 16> scan_tasks_;
  ObSEArray<int64_t, 16> taskset_idxs_;

  GIExternalTaskArgs() : taskset_tablets_(), scan_tasks_(), taskset_idxs_() {
    taskset_tablets_.set_attr(common::ObMemAttr(MTL_ID(), MEM_ATTR_EXT_TASK_GEN));
    scan_tasks_.set_attr(common::ObMemAttr(MTL_ID(), MEM_ATTR_EXT_TASK_GEN));
    taskset_idxs_.set_attr(common::ObMemAttr(MTL_ID(), MEM_ATTR_EXT_TASK_GEN));
  }
};

// ODPS 任务结果类型
struct GIOdpsTaskResult {
  ObDASTabletLoc *tablet_loc_;
  sql::ObIExtTblScanTask *scan_task_;

  TO_STRING_KV(KP(tablet_loc_), KP(scan_task_));
};

typedef GIOdpsTaskResult GICsvTaskResult;

// TaskGen 共享上下文
struct ObTaskGenContext {
  ObTaskGenContext() : final_task_args_() {}
  virtual ~ObTaskGenContext() {}
  GIExternalTaskArgs final_task_args_;
};

struct ObOdpsTaskGenContext : public ObTaskGenContext {
  ObOdpsTaskGenContext() : ObTaskGenContext() {}
  virtual ~ObOdpsTaskGenContext() {}
};

struct ObCsvTaskGenContext : public ObTaskGenContext {
  ObCsvTaskGenContext()
    : ObTaskGenContext(), data_scan_tasks_(),
      gambling_results_()
  {
    data_scan_tasks_.set_attr(common::ObMemAttr(MTL_ID(), MEM_ATTR_EXT_TASK_GEN));
    gambling_results_.set_attr(common::ObMemAttr(MTL_ID(), MEM_ATTR_EXT_TASK_GEN));
  }
  virtual ~ObCsvTaskGenContext() {}
  ObSEArray<GICsvTaskResult, 16> data_scan_tasks_;
  ObSEArray<ObCsvGamblingResult, 16> gambling_results_;
};

struct GIOneStageParallelTaskGenBase {
  virtual int gen_task_parallel(ObExecContext &exec_ctx, ObGranuleIteratorOp *gi_op, ObTaskGenContext *ctx) = 0;
  struct IStage {
    virtual bool is_finished() const = 0;
    virtual bool is_succeed() const = 0;
  };

  virtual bool is_finished() const = 0;
  virtual bool is_succeed() const = 0;
  virtual int64_t get_tsc_op_id() = 0;
  // 在所有线程完成后调用，将生成的任务填充到 pump
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
  virtual ~GIOneStageParallelTaskGenBase() {}
};

template<typename TaskResult>
class GIOneStageParallelTaskGen : public GIOneStageParallelTaskGenBase {
public:

  struct OneStage : public IStage {
    enum class stage {
      INITIALIZED,
      GEN_TASK_WAITING,
      GEN_TASK_FINISHED,
      GEN_TASK_FAILED
    } stage_;
    OneStage() : stage_(stage::INITIALIZED) {}
    bool is_finished() const override {
      return get_stage() == stage::GEN_TASK_FINISHED || get_stage() == stage::GEN_TASK_FAILED;
    }
    bool is_succeed() const override {
      return get_stage() == stage::GEN_TASK_FINISHED;
    }
    void set_stage(stage stage) {
      ATOMIC_STORE(reinterpret_cast<volatile int32_t*>(&stage_), static_cast<int32_t>(stage));
    }
    stage get_stage() const {
      return static_cast<stage>(ATOMIC_LOAD(reinterpret_cast<const volatile int32_t*>(&stage_)));
    }
  };

  GIOneStageParallelTaskGen()
      : readers_parallelism_(0), gi_pump_(nullptr),
        tsc_op_id_(common::OB_INVALID_ID), gi_attri_flag_(0),
        generating_thread_count_(0),
        lock_for_global_tasks_collect_(common::ObLatchIds::GI_ONE_STAGE_PARALLEL_TASK_GEN_LOCK),
        global_task_results_(), stage_cond_() {
          global_task_results_.set_attr(common::ObMemAttr(MTL_ID(), MEM_ATTR_EXT_TASK_GEN));
          stage_cond_.init(common::ObWaitEventIds::DEFAULT_COND_WAIT);
        }

  GIOneStageParallelTaskGen(int64_t tsc_op_id, uint64_t gi_attri_flag,
                            ObGranulePump *pump, int64_t readers_parallelism)
      : readers_parallelism_(readers_parallelism), gi_pump_(pump),
        tsc_op_id_(tsc_op_id), gi_attri_flag_(gi_attri_flag),
        generating_thread_count_(0), lock_for_global_tasks_collect_(common::ObLatchIds::GI_ONE_STAGE_PARALLEL_TASK_GEN_LOCK), global_task_results_(),
        stage_cond_() {
          global_task_results_.set_attr(common::ObMemAttr(MTL_ID(), MEM_ATTR_EXT_TASK_GEN));
          stage_cond_.init(common::ObWaitEventIds::DEFAULT_COND_WAIT);
        }
  virtual ~GIOneStageParallelTaskGen();
  virtual int gen_task_parallel(ObExecContext &exec_ctx, ObGranuleIteratorOp *gi_op, ObTaskGenContext *ctx) override;
  bool is_finished() const override {
    return one_stage_.is_finished();
  }
  bool is_succeed() const override {
    return one_stage_.is_succeed();
  }
  // 实现父类纯虚函数
  virtual int64_t get_tsc_op_id() override {
    return tsc_op_id_;
  }

  // 将单个扫描任务转换为单阶段扫描任务（纯虚函数，由子类实现）
  // 结果数组允许一个输入任务产生多个输出任务
  // 处理任务信息并创建扫描任务（通用方法，不特定于 ODPS）
  virtual int process_one_task(
      ObGranuleTaskInfo &task_info,
      ObSEArray<TaskResult, 16> &local_task_results) = 0;
  virtual int join_task_results(
      ObIArray<TaskResult> &all_task_results,
      ObTaskGenContext *ctx) = 0;

  // 在所有线程完成后调用，将生成的任务填充到 pump
  TO_STRING_KV(K(tsc_op_id_));
protected:

  // 等待所有线程完成任务生成
  int wait_for_stage_completion(ObExecContext &exec_ctx);
protected:
  int64_t readers_parallelism_;
  ObGranulePump *gi_pump_;
private:
  OneStage one_stage_;
  int64_t tsc_op_id_;
  uint64_t gi_attri_flag_;
  int64_t generating_thread_count_;  // 正在获取任务的线程计数 <= parallelism_
  common::ObSpinLock lock_for_global_tasks_collect_;
  ObSEArray<TaskResult, 16> global_task_results_;
  common::ObThreadCond stage_cond_;  // 用于等待 stage 完成的条件变量
  DISALLOW_COPY_AND_ASSIGN(GIOneStageParallelTaskGen);
};


class GIOdpsParallelTaskGen : public GIOneStageParallelTaskGen<GIOdpsTaskResult> {
public:
  GIOdpsParallelTaskGen(const ObString &property_str, int64_t tsc_op_id,
                        uint64_t gi_attri_flag, ObGranulePump *pump,
                        ObExecContext *exec_ctx, int64_t parallelism)
      : GIOneStageParallelTaskGen(tsc_op_id, gi_attri_flag, pump, parallelism),
        init_(false), exec_ctx_(exec_ctx), property_str_(property_str), arena_alloc_(MEM_ATTR_EXT_TASK_GEN) {}
  virtual ~GIOdpsParallelTaskGen();
  int init();
private:
  bool init_;
  ObExecContext *exec_ctx_; // allocator
  ObString property_str_;
  sql::ObExternalFileFormat external_odps_format_;
  common::ObArenaAllocator arena_alloc_; // allocator
protected:
  // ODPS 特定的任务转换实现
  virtual int process_one_task(
      ObGranuleTaskInfo &task_info,
      ObSEArray<GIOdpsTaskResult, 16> &local_task_results) override;

  // ODPS 特定的最终任务构建实现
  virtual int join_task_results(
      ObIArray<GIOdpsTaskResult> &all_task_results,
      ObTaskGenContext *ctx) override;
private:
  int odps_one_task_processing(
      sql::ObIExtTblScanTask *one_scan_task, ObDASTabletLoc *tablet_loc,
      ObIArray<GIOdpsTaskResult> &one_stage_task_results);
};

class GICsvGamblingParallelTaskGen : public GIOneStageParallelTaskGen<ObCsvGamblingResult> {
public:
  GICsvGamblingParallelTaskGen(const ObString &format, int64_t tsc_op_id, uint64_t gi_attri_flag,
                               ObGranulePump *pump, ObExecContext *exec_ctx, int64_t parallelism,
                               const ObString &location, const ObString &access_info)
      : GIOneStageParallelTaskGen<ObCsvGamblingResult>(tsc_op_id, gi_attri_flag, pump, parallelism),
        init_(false), exec_ctx_(exec_ctx),
        external_location_(location), external_access_info_(access_info),
        external_file_format_str_(format), external_file_format_(),
        arena_alloc_(MEM_ATTR_EXT_TASK_GEN) {}
  virtual ~GICsvGamblingParallelTaskGen();
  int init();
protected:
  // CSV Gambling 特定的任务转换实现
  virtual int process_one_task(
      ObGranuleTaskInfo &task_info,
      ObSEArray<ObCsvGamblingResult, 16> &local_task_results) override;

  // CSV Gambling 特定的最终任务构建实现
  virtual int join_task_results(
      ObIArray<ObCsvGamblingResult> &all_task_results,
      ObTaskGenContext *ctx) override;
private:
  int csv_gambling_one_task_processing(
      sql::ObIExtTblScanTask *one_scan_task, ObDASTabletLoc *tablet_loc,
      ObIArray<ObCsvGamblingResult> &one_stage_task_results);
  bool init_;
  ObExecContext *exec_ctx_;
  ObString external_location_;
  ObString external_access_info_;
  ObString external_file_format_str_;
  sql::ObExternalFileFormat external_file_format_;
  common::ObArenaAllocator arena_alloc_; // allocator
};

class GICsvFullScanParallelTaskGen : public GIOneStageParallelTaskGen<ObCsvFullScanResult> {
public:
  GICsvFullScanParallelTaskGen(const ObString &format, int64_t tsc_op_id, uint64_t gi_attri_flag,
                               ObGranulePump *pump, ObExecContext *exec_ctx,
                               int64_t parallelism, const ObString &location,
                               const ObString &access_info)
      : GIOneStageParallelTaskGen<ObCsvFullScanResult>(tsc_op_id, gi_attri_flag, pump, parallelism),
      init_(false), exec_ctx_(exec_ctx),
      external_location_(location), external_access_info_(access_info),
      external_file_format_str_(format), external_file_format_(),
      arena_alloc_(MEM_ATTR_EXT_TASK_GEN) {}
  virtual ~GICsvFullScanParallelTaskGen();
  int init();
protected:
  // CSV Full Scan 特定的任务转换实现
  virtual int process_one_task(
      ObGranuleTaskInfo &task_info,
      ObSEArray<ObCsvFullScanResult, 16> &local_task_results) override;

  // CSV Full Scan 特定的最终任务构建实现
  virtual int join_task_results(
      ObIArray<ObCsvFullScanResult> &all_task_results,
      ObTaskGenContext *ctx) override;
private:
int csv_full_scan_one_task_processing(
              sql::ObIExtTblScanTask *one_scan_task, ObDASTabletLoc *tablet_loc,
              ObIArray<ObCsvFullScanResult> &one_stage_task_results);
  bool init_;
  ObExecContext *exec_ctx_;
  ObString external_location_;
  ObString external_access_info_;
  ObString external_file_format_str_;
  sql::ObExternalFileFormat external_file_format_;
  common::ObArenaAllocator arena_alloc_; // allocator
};


class GITaskGenRunner;
class GITaskGenRunnerBuilder {
public:
  explicit GITaskGenRunnerBuilder(int64_t tsc_op_id, ObExecContext &exec_ctx, ObGranulePump *pump)
    : tsc_op_id_(tsc_op_id),
      exec_ctx_(exec_ctx),
      pump_(pump),
      runner_(nullptr),
      ret_(OB_SUCCESS) {}

  template<typename GeneratorType, typename ContextType, typename... Args>
  GITaskGenRunnerBuilder& add(GeneratorType *&out_generator, Args&&... args);

  int build(GITaskGenRunner *&runner);

  bool has_error() const { return OB_SUCCESS != ret_; }
  int get_error() const { return ret_; }
private:
  int ensure_runner_created();
  int64_t tsc_op_id_;
  ObExecContext &exec_ctx_;
  ObGranulePump *pump_;
  GITaskGenRunner *runner_;
  int ret_;
};


class GITaskGenRunner {
public:
  explicit GITaskGenRunner(int64_t tsc_op_id, ObExecContext &exec_ctx)
      : tsc_op_id_(tsc_op_id), exec_ctx_(exec_ctx), ctx_(nullptr), one_stage_task_gens_() {
        one_stage_task_gens_.set_attr(common::ObMemAttr(MTL_ID(), MEM_ATTR_EXT_TASK_GEN));
      }

  int get_tsc_op_id() const { return tsc_op_id_; }
  int is_finished(bool &finished) const;
  int is_succeed(bool &succeed) const;
  const GIExternalTaskArgs &get_final_task_args() const {
    return ctx_->final_task_args_;
  }

  ObTaskGenContext *get_ctx() const {
    return ctx_;
  }

  void set_ctx(ObTaskGenContext *ctx) {
    ctx_ = ctx;
  }

  int add_task_gen(GIOneStageParallelTaskGenBase *task_gen) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(one_stage_task_gens_.push_back(task_gen))) {
      SQL_LOG(WARN, "failed to push back task gen", K(ret));
    } else {
      tsc_op_id_ = task_gen->get_tsc_op_id();
    }
    return ret;
  }

  int run_task(ObGranuleIteratorOp *gi_op);
  ObExecContext &get_exec_ctx() const {
    return exec_ctx_;
  }
  virtual ~GITaskGenRunner() {
    OB_DELETEx(ObTaskGenContext, &exec_ctx_.get_allocator(), ctx_);
    ctx_ = nullptr;
    for (int i = 0; i < one_stage_task_gens_.count(); i++) {
      OB_DELETEx(GIOneStageParallelTaskGenBase, &exec_ctx_.get_allocator(), one_stage_task_gens_.at(i));
    }
    one_stage_task_gens_.reset();
  }

  TO_STRING_KV(K(tsc_op_id_), K(one_stage_task_gens_));
private:
  int64_t tsc_op_id_;
  ObExecContext &exec_ctx_;
  ObTaskGenContext* ctx_;
  ObSEArray<GIOneStageParallelTaskGenBase *, 2> one_stage_task_gens_;
};



template<typename GeneratorType, typename ContextType, typename... Args>
GITaskGenRunnerBuilder& GITaskGenRunnerBuilder::add(GeneratorType *&out_generator, Args&&... args) {
  int ret = ret_;
  out_generator = nullptr;

  if (has_error()) {
    SQL_LOG(WARN, "skip adding generator due to previous error", K(ret), K(tsc_op_id_));
    return *this;
  }

  ret_ = ensure_runner_created();
  if (OB_SUCCESS != ret_) {
    ret = ret_;
    SQL_LOG(WARN, "failed to ensure runner created", K(ret), K(tsc_op_id_));
    return *this;
  }

  if (OB_ISNULL(runner_->get_ctx())) {
    ContextType *ctx = OB_NEWx(ContextType, &exec_ctx_.get_allocator());
    if (OB_ISNULL(ctx)) {
      ret_ = OB_ALLOCATE_MEMORY_FAILED;
      ret = ret_;
      SQL_LOG(WARN, "failed to allocate context", K(ret), K(tsc_op_id_));
      return *this;
    } else {
      runner_->set_ctx(ctx);
    }
  }

  GeneratorType *task_gen = OB_NEWx(GeneratorType, &exec_ctx_.get_allocator(), std::forward<Args>(args)...);
  if (OB_ISNULL(task_gen)) {
    ret_ = OB_ALLOCATE_MEMORY_FAILED;
    ret = ret_;
    SQL_LOG(WARN, "failed to allocate generator", K(ret), K(tsc_op_id_));
  } else if (OB_FAIL(ret_ = task_gen->init())) {
    SQL_LOG(WARN, "failed to init generator", K(ret), K(tsc_op_id_));
  } else if (OB_FAIL(ret_ = runner_->add_task_gen(task_gen))) {
    SQL_LOG(WARN, "failed to add task gen to runner", K(ret), K(tsc_op_id_));
  } else {
    out_generator = task_gen;
    task_gen = nullptr;
  }

  if (OB_FAIL(ret_) && OB_NOT_NULL(task_gen)) {
    OB_DELETEx(GeneratorType, &exec_ctx_.get_allocator(), task_gen);
  }

  return *this;
}

} // namespace sql
} // namespace oceanbase

#endif /* OB_GRANULE_PARALLEL_TASK_GEN_H_ */
