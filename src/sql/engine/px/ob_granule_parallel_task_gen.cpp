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

#include "ob_granule_parallel_task_gen.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "share/external_table/ob_external_table_utils.h"
#include "lib/lock/ob_lock_guard.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/config/ob_server_config.h"
#ifdef OB_BUILD_CPP_ODPS
#include "sql/engine/table/ob_odps_table_row_iter.h"
#endif
#ifdef OB_BUILD_JNI_ODPS
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"
#endif

namespace oceanbase
{
namespace sql
{

template<typename TaskResult>
GIOneStageParallelTaskGen<TaskResult>::~GIOneStageParallelTaskGen() {
}


/*
 * 有多个线程完成
 */
template<typename TaskResult>
int GIOneStageParallelTaskGen<TaskResult>::gen_task_parallel(ObExecContext &exec_ctx, ObGranuleIteratorOp *gi_op, ObTaskGenContext *ctx)
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  if (OB_ISNULL(gi_op) || OB_ISNULL(gi_pump_) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gi_op or gi_pump_ or odps_ctx is null", K(ret), KP(gi_op), KP(gi_pump_), KP(ctx));
  } else {
    ObSEArray<TaskResult, 16> local_task_results;
    bool finished = false;
    while (OB_SUCC(ret) && !finished) {
      // 获取下一个任务
      if (OB_FAIL(exec_ctx.check_status())) {
        LOG_WARN("failed to fast check status", K(ret));
      } else if (one_stage_.get_stage() != OneStage::stage::INITIALIZED) {
        finished = true;
      } else if (OB_FAIL(gi_op->get_next_granule_task_map_for_range_gen_parallel())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next granule task for range gen parallel", K(ret));
        } else {
          ret = OB_SUCCESS;
          finished = true;
        }
      } else {
        // 生成任务
        ObGranuleTaskInfo task_info;
        if (OB_FAIL(gi_op->fetch_task_for_range_gen_parallel(tsc_op_id_, task_info))) {
          LOG_WARN("failed to generate granule task", K(ret));
        } else if (OB_FAIL(this->process_one_task(
                      task_info, local_task_results))) {
          LOG_WARN("failed to process task info and create scan tasks", K(ret));
        } else {
          // 所有线程都需要添加到 global_task_results_
          // 只能让一个线程执行这个操作，因为 global_task_results_ 是共享的
          ObLockGuard<ObSpinLock> lock_guard(lock_for_global_tasks_collect_);
          for (int64_t i = 0; OB_SUCC(ret) && i < local_task_results.count(); ++i) {
            if (OB_FAIL(global_task_results_.push_back(local_task_results.at(i)))) {
              LOG_WARN("failed to push back task result", K(ret));
            }
          }
          local_task_results.reuse();
        }
      }
    }
  }

  int64_t generating_thread_count = ATOMIC_AAF(&generating_thread_count_, 1);
  int paralleism = gi_op->get_parallelism();

  if (generating_thread_count == paralleism) {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(exec_ctx.check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(this->join_task_results(global_task_results_, ctx))) {
      LOG_WARN("failed to build final task parameters", K(ret));
    } else if (ctx->final_task_args_.scan_tasks_.count() > 0 && OB_FAIL(gi_pump_->refill_pump_with_new_gen_tasks(
                tsc_op_id_, gi_attri_flag_, ctx->final_task_args_))) {
      LOG_WARN("failed to regenerate gi task", K(ret));
    }

    // 通知等待的线程
    {
      ObThreadCondGuard guard(stage_cond_);
      // 无论成功失败都设置状态，让其他线程能退出等待
      if (OB_FAIL(ret)) {
        one_stage_.set_stage(OneStage::stage::GEN_TASK_FAILED);
      } else {
        one_stage_.set_stage(OneStage::stage::GEN_TASK_FINISHED);
      }
      int tmp_ret = 0;
      tmp_ret = stage_cond_.broadcast();
      if (tmp_ret != OB_SUCCESS) { // do not cover ret
        LOG_WARN("failed to broadcast condition", K(tmp_ret));
      }
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  } else {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(wait_for_stage_completion(exec_ctx))) {
      LOG_WARN("failed to wait for stage completion", K(ret));
    }
  }
  return ret;
}


template<typename TaskResult>
int GIOneStageParallelTaskGen<TaskResult>::wait_for_stage_completion(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(stage_cond_);

  // 等待 stage 完成（FINISHED 或 FAILED）
  // one stage是原子的 无需保护
  while (OB_SUCC(ret) && !one_stage_.is_finished()) {
    if (OB_FAIL(exec_ctx.check_status())) {
      LOG_WARN("fail to check status", K(ret));
    } else {
      // 使用条件变量等待，超时时间 100ms，以便定期检查状态
      ret = stage_cond_.wait_us(100 * 1000);
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;  // 超时是正常的，继续循环检查
      }
    }
  }
  return ret;
}


GIOdpsParallelTaskGen::~GIOdpsParallelTaskGen()
{}

int GIOdpsParallelTaskGen::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(external_odps_format_.load_from_string(property_str_,
                                                    arena_alloc_))) {
    LOG_WARN("failed to load odps format from string", K(ret));
  }
  if (OB_SUCC(ret)) {
    init_ = true;
  }
  return ret;
}

int GIOdpsParallelTaskGen::odps_one_task_processing(
    sql::ObIExtTblScanTask *one_scan_task,
    ObDASTabletLoc *tablet_loc,
    ObIArray<GIOdpsTaskResult> &one_stage_task_results)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!init_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(one_scan_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one scan task is null", K(ret));
  } else {
    ObOdpsScanTask *odps_scan_task = static_cast<ObOdpsScanTask *>(one_scan_task);
    if (OB_ISNULL(odps_scan_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("odps scan task is null", K(ret));
    } else {
      ObString session_id = odps_scan_task->session_id_;
      if (session_id.empty()) {
        if (GCONF._use_odps_jni_connector) {
#if defined(OB_BUILD_JNI_ODPS)
          // JniRowTableIter::fetch_odps_row_count_and_session_id
          sql::ObODPSJNITableRowIterator odps_driver;
          int64_t row_count = 0;
          if (OB_FAIL(odps_driver.init_jni_meta_scanner(
                  external_odps_format_.odps_format_,
                  exec_ctx_->get_my_session()))) {
            LOG_WARN("failed to init jni scanner", K(ret));
          } else if (OB_FAIL(sql::ObOdpsPartitionJNIDownloaderMgr::
                                 fetch_odps_partition_row_count_and_session_id(
                                     odps_driver, odps_scan_task->file_url_,
                                     row_count, exec_ctx_->get_allocator(),
                                     session_id))) {
            LOG_WARN("failed to fetch row count and session id", K(ret));
          } else {
            if (row_count == -1) {
              row_count = 0;
            }
            odps_scan_task->first_lineno_ = 0;
            odps_scan_task->last_lineno_ = row_count;
            odps_scan_task->session_id_ = session_id;
          }
#else
          ret = OB_NOT_SUPPORTED;
#endif
        } else {
#if defined(OB_BUILD_CPP_ODPS)
          // 解析 odps_format 并创建 downloader
          // 这里非常特殊
          apsara::odps::sdk::IDownloadPtr download_handle;
          if (OB_FAIL(gi_pump_->get_odps_downloader_mgr()
                          .get_or_create_odps_downloader(
                              odps_scan_task->part_id_,
                              odps_scan_task->file_url_, session_id,
                              external_odps_format_.odps_format_, true,
                              download_handle))) {
            LOG_WARN("failed to get or create odps downloader", K(ret),
                     K(odps_scan_task->part_id_));
          } else if (OB_ISNULL(download_handle)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret), KP(download_handle.get()));
          } else {
            int64_t row_count = download_handle->GetRecordCount();
            std::string download_id = download_handle->GetDownloadId();
            ObString download_id_obstr(download_id.length(),
                                       download_id.c_str());
            ObString session_id_obstr;
            // 这个session id要用很久和 ObOdpsScanTask 一样要用exec_ctex
            OZ(ob_write_string(exec_ctx_->get_allocator(), download_id_obstr,
                               session_id_obstr, true));
            odps_scan_task->first_lineno_ = 0;
            odps_scan_task->last_lineno_ = row_count;
            odps_scan_task->session_id_ = session_id_obstr;
          }
#else
          ret = OB_NOT_SUPPORTED;
#endif
        }
      } else { // session id not empty
        if (GCONF._use_odps_jni_connector) {
#if defined(OB_BUILD_JNI_ODPS)
          // do nothing
#else
          ret = OB_NOT_SUPPORTED;
#endif
        } else {
#if defined(OB_BUILD_CPP_ODPS)
          // 解析 odps_format 并创建 downloader
          apsara::odps::sdk::IDownloadPtr download_handle;
          if (OB_FAIL(gi_pump_->get_odps_downloader_mgr()
                                 .get_or_create_odps_downloader(
                                     odps_scan_task->part_id_,
                                     odps_scan_task->file_url_, session_id,
                                     external_odps_format_.odps_format_, true,
                                     download_handle))) {
            LOG_WARN("failed to get or create odps downloader", K(ret),
                     K(odps_scan_task->part_id_));
          } else if (OB_ISNULL(download_handle)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret), KP(download_handle.get()));
          }
#else
          ret = OB_NOT_SUPPORTED;
#endif
        }
      } // session id
    }   // load odps format
    if (OB_SUCC(ret)) {
      GIOdpsTaskResult one_result;
      one_result.scan_task_ = odps_scan_task;
      one_result.tablet_loc_ = tablet_loc;
      if (OB_FAIL(one_stage_task_results.push_back(one_result))) {
        LOG_WARN("failed to push back task result", K(ret));
      }
    }
  }
  return ret;
}

int GIOdpsParallelTaskGen::process_one_task(
        ObGranuleTaskInfo &task_info,
        ObSEArray<GIOdpsTaskResult, 16> &local_task_results)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gi_pump_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("gi_pump_ is null", K(ret));
  }
  LOG_TRACE("process task info and create scan tasks", K(task_info.scan_tasks_.count()));

  for (int64_t i = 0; OB_SUCC(ret) && i < task_info.scan_tasks_.count(); ++i) {
    sql::ObIExtTblScanTask *scan_task = task_info.scan_tasks_.at(i);
    ObSEArray<GIOdpsTaskResult, 16> one_stage_task_results;
    if (OB_FAIL(odps_one_task_processing(
            scan_task, task_info.tablet_loc_, one_stage_task_results))) {
      LOG_WARN("failed to transform one scan task to one stage scan task",
               K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < one_stage_task_results.count();
           ++j) {
        if (OB_FAIL(
                local_task_results.push_back(one_stage_task_results.at(j)))) {
          LOG_WARN("failed to push back task result", K(ret));
        }
      }
    }
  }
  return ret;
}

/*
 * from all_task_results to build final task parameters
 * final_task_args
 */
int GIOdpsParallelTaskGen::join_task_results(
                           ObIArray<GIOdpsTaskResult> &all_task_results,
                           ObTaskGenContext *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!init_) || OB_ISNULL(exec_ctx_) || OB_ISNULL(ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init or ctx is null", K(ret), KP(exec_ctx_), KP(ctx));
  } else {
    ObOdpsTaskGenContext *odps_ctx = static_cast<ObOdpsTaskGenContext *>(ctx);
    oceanbase::sql::GIExternalTaskArgs &final_task_args = odps_ctx->final_task_args_;
    int64_t total_line_number = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_task_results.count(); ++i) {
      sql::ObIExtTblScanTask *scan_task = all_task_results.at(i).scan_task_;
      if (OB_ISNULL(scan_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scan task is null", K(ret));
      } else {
        total_line_number += scan_task->last_lineno_ - scan_task->first_lineno_;
      }
    }

    int64_t each_task_line_number = total_line_number / (3 * readers_parallelism_);
    if (each_task_line_number <= 10000) {
      each_task_line_number = 10000; // 不少于1k行
    }
    int64_t task_idx = 0;
    int64_t current_task_line_count = 0;
    int64_t current_task_start_lineno = 0;
    ObOdpsScanTask *current_new_task = NULL;
    // 遍历所有原始 scan tasks，每个至少创建一个任务，大于 each_task_line_number 则切分
    for (int64_t i = 0; OB_SUCC(ret) && i < all_task_results.count(); ++i) {
      sql::ObIExtTblScanTask *original_task = all_task_results.at(i).scan_task_;
      ObDASTabletLoc *tablet_loc = all_task_results.at(i).tablet_loc_;
      if (OB_ISNULL(original_task) || OB_ISNULL(tablet_loc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("original scan task is null", K(ret), K(i));
      } else {
        ObOdpsScanTask *odps_original_task = static_cast<ObOdpsScanTask *>(original_task);
        int64_t task_start = original_task->first_lineno_;
        int64_t task_end = original_task->last_lineno_;
        int64_t task_total_lines = task_end - task_start;
        // 如果任务行数 <= each_task_line_number，直接创建一个任务
        if (task_total_lines <= each_task_line_number) {
          ObIAllocator &allocator = exec_ctx_->get_allocator();
          ObOdpsScanTask *new_task = NULL;
          // pump args's ctx is exec_ctx_
          if (OB_ISNULL(new_task = OB_NEWx(ObOdpsScanTask, (&allocator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate ObOdpsScanTask", K(ret));
          } else {
            if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(
                odps_original_task->file_url_,
                odps_original_task->part_id_,
                task_start,
                task_end,
                odps_original_task->session_id_,
                odps_original_task->first_split_idx_,
                odps_original_task->last_split_idx_,
                *new_task))) {
              LOG_WARN("failed to make odps scan task", K(ret));
            } else if (OB_FAIL(final_task_args.scan_tasks_.push_back(new_task))) {
              LOG_WARN("failed to push back scan task", K(ret));
            } else if (OB_FAIL(final_task_args.taskset_tablets_.push_back(tablet_loc))) {
              LOG_WARN("failed to push back tablet", K(ret));
            } else if (OB_FAIL(final_task_args.taskset_idxs_.push_back(i))) {
              LOG_WARN("failed to push back task idx", K(ret));
            }
          }
        } else {
          // 任务行数 > each_task_line_number，需要切分
          int64_t remaining_lines = task_total_lines;
          int64_t current_start = task_start;
          ObOdpsScanTask *last_task = NULL;
          int64_t split_file_count = each_task_line_number * 0.3;
          while (OB_SUCC(ret) && remaining_lines > 0) {
            int64_t lines_to_assign = each_task_line_number;
            bool is_last_chunk = false;
            // 判断是否是最后一片
            if (remaining_lines <= each_task_line_number) {
              lines_to_assign = remaining_lines;
              is_last_chunk = true;
            }
            // 如果最后一片太小（小于 each_task_line_number 的 30%），且不是唯一的一片，则合并到前一个任务
            if (is_last_chunk && lines_to_assign < split_file_count && NULL != last_task) {
              // 合并到前一个任务
              last_task->last_lineno_ = task_end;
              remaining_lines = 0;
              break;
            }
            // 创建新任务
            ObIAllocator &allocator = exec_ctx_->get_allocator();
            ObOdpsScanTask *new_task = NULL;
            if (OB_ISNULL(new_task = OB_NEWx(ObOdpsScanTask, (&allocator)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate ObOdpsScanTask", K(ret));
            } else {
              int64_t current_end = current_start + lines_to_assign;
              if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(
                  odps_original_task->file_url_,
                  odps_original_task->part_id_,
                  current_start,
                  current_end,
                  odps_original_task->session_id_,
                  odps_original_task->first_split_idx_,
                  odps_original_task->last_split_idx_,
                  *new_task))) {
                LOG_WARN("failed to make odps scan task", K(ret));
              } else if (OB_FAIL(final_task_args.scan_tasks_.push_back(new_task))) {
                LOG_WARN("failed to push back scan task", K(ret));
              } else if (OB_FAIL(final_task_args.taskset_tablets_.push_back(tablet_loc))) {
                LOG_WARN("failed to push back tablet", K(ret));
              } else if (OB_FAIL(final_task_args.taskset_idxs_.push_back(task_idx++))) {
                LOG_WARN("failed to push back task idx", K(ret));
              } else {
                last_task = new_task;
                current_start += lines_to_assign;
                remaining_lines -= lines_to_assign;
              }
            }
          } // while remaining_lines > 0
        } // task_total_lines <= each_task_line_number
      } // OB_ISNULL(original_task)
    }

    if (OB_UNLIKELY(OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_TRACE)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < final_task_args.scan_tasks_.count(); ++i) {
        sql::ObIExtTblScanTask *scan_task = final_task_args.scan_tasks_.at(i);
        if (OB_ISNULL(scan_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("scan task is null", K(ret));
        } else {
          ObOdpsScanTask *odps_scan_task = static_cast<ObOdpsScanTask *>(scan_task);
          LOG_TRACE("odps new scan task in palrallel stage",
                    K(odps_scan_task->file_url_), K(odps_scan_task->part_id_),
                    K(odps_scan_task->session_id_),
                    K(odps_scan_task->first_lineno_),
                    K(odps_scan_task->last_lineno_));
        }
      }
    }
    LOG_TRACE("odps build final task from scan tasks", K(total_line_number), K(each_task_line_number),
              K(final_task_args.scan_tasks_.count()));
  }
  return ret;
}

GICsvGamblingParallelTaskGen::~GICsvGamblingParallelTaskGen()
{
}

int GICsvGamblingParallelTaskGen::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(init_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", K(ret));
  } else if (OB_FAIL(external_file_format_.load_from_string(external_file_format_str_, arena_alloc_))) {
    LOG_WARN("failed to load odps format from string", K(ret));
  } else {
    init_ = true;
  }
  return ret;
}

int GICsvGamblingParallelTaskGen::csv_gambling_one_task_processing(
    sql::ObIExtTblScanTask *one_scan_task, ObDASTabletLoc *tablet_loc,
    ObIArray<ObCsvGamblingResult> &one_stage_task_results)
{
  int ret = OB_SUCCESS;
  const ObExtTableScanTask *scan_task = static_cast<const ObExtTableScanTask*>(one_scan_task);
  const ObCsvParallelInfo *csv_parallel_info = OB_NOT_NULL(scan_task) ? scan_task->parallel_parse_csv_info_ : nullptr;
  if (OB_ISNULL(scan_task) || OB_ISNULL(csv_parallel_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan task or csv parallel info is null", K(ret), K(scan_task), K(csv_parallel_info));
  } else if (csv_parallel_info->csv_task_type_ != CsvTaskType::GAMBLING_BOUND) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is not csv gambling task", K(ret), K(csv_parallel_info->csv_task_type_));
  } else {
    ObString url = scan_task->file_url_;
    int64_t start_pos = csv_parallel_info->start_pos_;
    int64_t end_pos = csv_parallel_info->end_pos_;
    int64_t chunk_idx = csv_parallel_info->chunk_idx_;
    int64_t chunk_cnt = csv_parallel_info->chunk_cnt_;
    int64_t file_id = scan_task->file_id_;
    GamblingFunctor func;
    if (chunk_idx != 0 && OB_FAIL(share::ObExternalTableUtils::read_data_for_bound(
                                  external_location_, external_access_info_,
                                  external_file_format_, url, start_pos,
                                  end_pos, func))) {
      LOG_WARN("failed to do gambling bound", K(ret), K(url), K(start_pos), K(end_pos));
    } else {
      ObCsvGamblingResult result;
      result.file_id_ = file_id;
      result.chunk_idx_ = chunk_idx;
      result.chunk_cnt_ = chunk_cnt;
      result.enclosed_cnt_ = func.bound_helper_.enclosed_cnt_;
      result.is_gambling_end_with_escaped_ = func.bound_helper_.is_escaped_;  // 需要把转义状态传递下去, 否则该chunk的余下数据在全量扫描阶段统计相关信息时可能会出错
      if (func.bound_helper_.even_pos_ != OB_INVALID_INDEX) {
        result.even_pos_ = func.bound_helper_.even_pos_ + start_pos;
      }
      if (func.bound_helper_.odd_pos_ != OB_INVALID_INDEX) {
        result.odd_pos_ = func.bound_helper_.odd_pos_ + start_pos;
      }
      result.next_start_pos_ = chunk_idx == 0 ? 0 : func.bound_helper_.already_read_size_ + start_pos + 1;
      result.end_pos_ = end_pos;
      result.tablet_loc_ = tablet_loc;
      result.scan_task_ = one_scan_task;
      if (chunk_idx == 0 || func.is_success_) {
        result.is_success_ = true;
        if (chunk_idx != 0 && (func.succ_idx_ < 0 || func.succ_idx_ >= GamblingFunctor::START_STATE_CNT)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, succ_idx is out of range", K(ret), K(func.succ_idx_));
        } else {
          result.bounded_start_pos_ = (chunk_idx == 0) ? 0 : func.potential_start_pos_[func.succ_idx_] + start_pos;
        }
      }
      OZ (one_stage_task_results.push_back(result));
    }
  }
  return ret;
}

int GICsvGamblingParallelTaskGen::process_one_task(
        ObGranuleTaskInfo &task_info,
        ObSEArray<ObCsvGamblingResult, 16> &local_task_results)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gi_pump_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("gi_pump_ is null", K(ret));
  }
  LOG_TRACE("process task info and create scan tasks", K(task_info.scan_tasks_.count()));

  for (int64_t i = 0; OB_SUCC(ret) && i < task_info.scan_tasks_.count(); ++i) {
    sql::ObIExtTblScanTask *scan_task = task_info.scan_tasks_.at(i);
    ObSEArray<ObCsvGamblingResult, 16> one_stage_task_results;
    if (OB_FAIL(csv_gambling_one_task_processing(
            scan_task, task_info.tablet_loc_, one_stage_task_results))) {
      LOG_WARN("failed to transform one scan task to one stage scan task",
               K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < one_stage_task_results.count();
           ++j) {
        if (OB_FAIL(
                local_task_results.push_back(one_stage_task_results.at(j)))) {
          LOG_WARN("failed to push back task result", K(ret));
        }
      }
    }
  }
  return ret;
}

int GICsvGamblingParallelTaskGen::join_task_results(
                                  ObIArray<ObCsvGamblingResult> &all_task_results,
                                  ObTaskGenContext *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!init_) || OB_ISNULL(exec_ctx_) || OB_ISNULL(ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init or ctx is null", K(ret), KP(exec_ctx_), KP(ctx));
  } else {
    ObCsvTaskGenContext *csv_ctx = static_cast<ObCsvTaskGenContext *>(ctx);
    oceanbase::sql::GIExternalTaskArgs &final_task_args = csv_ctx->final_task_args_;
    final_task_args.scan_tasks_.reset();
    final_task_args.taskset_idxs_.reset();
    final_task_args.taskset_tablets_.reset();
    int64_t scan_task_idx = 0;

    struct CsvGamblingResultProcessor {
      ObArenaAllocator &arena_alloc_;
      ObCsvTaskGenContext *csv_ctx_;
      oceanbase::sql::GIExternalTaskArgs &final_task_args_;
      int64_t &scan_task_idx_;

      CsvGamblingResultProcessor(ObArenaAllocator &arena_alloc,
                                 ObCsvTaskGenContext *csv_ctx,
                                 oceanbase::sql::GIExternalTaskArgs &final_task_args,
                                 int64_t &scan_task_idx)
        : arena_alloc_(arena_alloc),
          csv_ctx_(csv_ctx),
          final_task_args_(final_task_args),
          scan_task_idx_(scan_task_idx) {}

      int operator()(ObIArray<ObCsvGamblingResult> &results,
                     int64_t start_idx, int64_t end_idx) {
        int ret = OB_SUCCESS;
        LOG_TRACE("join gambling results, the range of one file's results", K(start_idx), K(end_idx));
        bool is_success = true;
        for (int64_t i = start_idx; is_success && i <= end_idx; ++i) {
          if (!results.at(i).is_success_) {
            is_success = false;
          }
        }

        int gambling_ret = OB_E(EventTable::EN_PARALLEL_PARSE_CSV_GAMBLING_FAILED) OB_SUCCESS;
        if (gambling_ret != OB_SUCCESS) {
          is_success = false;
        }

        sql::ObIExtTblScanTask *original_task = results.at(start_idx).scan_task_;
        sql::ObExtTableScanTask *ext_task = static_cast<sql::ObExtTableScanTask *>(original_task);
        ObDASTabletLoc *tablet_loc = results.at(start_idx).tablet_loc_;
        if (OB_ISNULL(original_task) || OB_ISNULL(tablet_loc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("original scan task is null", K(ret));
        } else {
          for (int64_t i = start_idx; i <= end_idx && OB_SUCC(ret); ++i) {
            const ObCsvGamblingResult &tmp = results.at(i);
            int64_t start_pos = 0;
            int64_t end_pos = 0;
            if (is_success) {
              start_pos = tmp.bounded_start_pos_;
              end_pos = (i + 1 <= end_idx) ? results.at(i+1).bounded_start_pos_ : tmp.end_pos_;
            } else {
              start_pos = tmp.next_start_pos_;
              end_pos = tmp.end_pos_;
            }
            ObExtTableScanTask *scan_task = OB_NEWx(ObExtTableScanTask, (&arena_alloc_));
            if (OB_ISNULL(scan_task)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate scan task", K(ret));
            } else if (OB_FAIL(scan_task->init_parallel_parse_csv_info(arena_alloc_))) {
              LOG_WARN("failed to init parallel parse csv info", K(ret));
            } else if (OB_FAIL(ObExternalTableUtils::make_parallel_parse_csv_task(*ext_task,
                                                                                 1, INT64_MAX,
                                                                                 start_pos, end_pos,
                                                                                 i - start_idx, end_idx - start_idx + 1,
                                                                                 scan_task))) {
              LOG_WARN("failed to make parallel parse csv task for state boundary", K(ret));
            } else {
              if (OB_ISNULL(scan_task->parallel_parse_csv_info_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("parallel parse csv info is null", K(ret));
              } else if (is_success) {
                GICsvTaskResult csv_task_result;
                scan_task->parallel_parse_csv_info_->csv_task_type_ = CsvTaskType::PARSE_DATA;
                csv_task_result.scan_task_ = scan_task;
                csv_task_result.tablet_loc_ = tablet_loc;
                OZ (csv_ctx_->data_scan_tasks_.push_back(csv_task_result));
              } else {
                scan_task->parallel_parse_csv_info_->csv_task_type_ = CsvTaskType::FULL_SCAN_BOUND;
                scan_task->parallel_parse_csv_info_->is_gambling_end_with_escaped_ = tmp.is_gambling_end_with_escaped_;
                OZ (final_task_args_.scan_tasks_.push_back(scan_task));
                OZ (final_task_args_.taskset_idxs_.push_back(scan_task_idx_++));
                OZ (final_task_args_.taskset_tablets_.push_back(tablet_loc));
                OZ (csv_ctx_->gambling_results_.push_back(tmp));
              }
            }
          }
        }
        return ret;
      }
    };

    CsvGamblingResultProcessor processor(arena_alloc_, csv_ctx, final_task_args, scan_task_idx);
    if (OB_LOGGER.need_to_print(OB_LOG_LEVEL_TRACE)) {
      for (int64_t i = 0; i < all_task_results.count(); i++) {
        LOG_TRACE("gambling task result detail", K(i), K(all_task_results.at(i)));
      }
    }
    if (OB_FAIL(process_csv_tasks_by_file_id(all_task_results, processor))) {
      LOG_WARN("failed to process csv tasks by file id", K(ret));
    }
    LOG_INFO("join gambling results, first empty means all files gambling succeeded",
              K(ret), K(final_task_args.scan_tasks_.count()), K(csv_ctx->data_scan_tasks_.count()));
  }
  return ret;
}

GICsvFullScanParallelTaskGen::~GICsvFullScanParallelTaskGen()
{
}

int GICsvFullScanParallelTaskGen::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(init_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", K(ret));
  } else if (OB_FAIL(external_file_format_.load_from_string(external_file_format_str_,
                                                    arena_alloc_))) {
    LOG_WARN("failed to load external file format from string", K(ret));
  } else {
    init_ = true;
  }
  return ret;
}

int GICsvFullScanParallelTaskGen::csv_full_scan_one_task_processing(
                                  sql::ObIExtTblScanTask *one_scan_task, ObDASTabletLoc *tablet_loc,
                                  ObIArray<ObCsvFullScanResult> &one_stage_task_results)
{
  int ret = OB_SUCCESS;
  const ObExtTableScanTask *scan_task = static_cast<const ObExtTableScanTask*>(one_scan_task);
  const ObCsvParallelInfo *csv_parallel_info = OB_NOT_NULL(scan_task) ? scan_task->parallel_parse_csv_info_ : nullptr;
  if (OB_ISNULL(scan_task) || OB_ISNULL(csv_parallel_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan task or csv parallel info is null", K(ret), K(scan_task), K(csv_parallel_info));
  } else if (csv_parallel_info->csv_task_type_ != CsvTaskType::FULL_SCAN_BOUND) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parallel parse csv type", K(ret), K(csv_parallel_info->csv_task_type_));
  } else {
    ObString url = scan_task->file_url_;
    int64_t start_pos = csv_parallel_info->start_pos_;
    int64_t end_pos = csv_parallel_info->end_pos_;
    int64_t chunk_idx = csv_parallel_info->chunk_idx_;
    int64_t chunk_cnt = csv_parallel_info->chunk_cnt_;
    int64_t file_id = scan_task->file_id_;
    struct FullScanFunctor {
      FullScanFunctor() : is_finished_(false), bound_helper_() {}

      int operator()(const ObExternalFileFormat &file_format,
                     char *buf, int64_t content_len) {
        return bound_helper_.do_full_scan_boundary(file_format, buf, content_len);
      }

      bool is_finished_;  // full scan, don't set
      CsvFullScanBoundHelper bound_helper_;
    };
    FullScanFunctor func;
    func.bound_helper_.is_escaped_ = csv_parallel_info->is_gambling_end_with_escaped_;
    if (OB_FAIL(share::ObExternalTableUtils::read_data_for_bound(
                                             external_location_, external_access_info_,
                                             external_file_format_, url, start_pos,
                                             end_pos, func))) {
      LOG_WARN("failed to do full scan bound", K(ret), K(url), K(start_pos), K(end_pos));
    } else {
      ObCsvFullScanResult result;
      result.file_id_ = file_id;
      result.chunk_idx_ = chunk_idx;
      result.chunk_cnt_ = chunk_cnt;
      result.enclosed_cnt_ = func.bound_helper_.enclosed_cnt_;
      if (func.bound_helper_.even_pos_ != OB_INVALID_INDEX) {
        result.even_pos_ = start_pos + func.bound_helper_.even_pos_;
      }
      if (func.bound_helper_.odd_pos_ != OB_INVALID_INDEX) {
        result.odd_pos_ = start_pos + func.bound_helper_.odd_pos_;
      }
      result.tablet_loc_ = tablet_loc;
      result.scan_task_ = one_scan_task;
      OZ (one_stage_task_results.push_back(result));
    }
  }
  return ret;
}

int GICsvFullScanParallelTaskGen::process_one_task(
        ObGranuleTaskInfo &task_info,
        ObSEArray<ObCsvFullScanResult, 16> &local_task_results)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gi_pump_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("gi_pump_ is null", K(ret));
  }
  LOG_TRACE("process task info and create scan tasks", K(task_info.scan_tasks_.count()));

  for (int64_t i = 0; OB_SUCC(ret) && i < task_info.scan_tasks_.count(); ++i) {
    sql::ObIExtTblScanTask *scan_task = task_info.scan_tasks_.at(i);
    ObSEArray<ObCsvFullScanResult, 16> one_stage_task_results;
    if (OB_FAIL(csv_full_scan_one_task_processing(scan_task, task_info.tablet_loc_, one_stage_task_results))) {
      LOG_WARN("failed to transform one scan task to one stage scan task", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < one_stage_task_results.count(); ++j) {
        if (OB_FAIL(local_task_results.push_back(one_stage_task_results.at(j)))) {
          LOG_WARN("failed to push back task result", K(ret));
        }
      }
    }
  }
  return ret;
}
int GICsvFullScanParallelTaskGen::join_task_results(
                                  ObIArray<ObCsvFullScanResult> &all_task_results,
                                  ObTaskGenContext *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!init_) || OB_ISNULL(exec_ctx_) || OB_ISNULL(ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init or ctx is null", K(ret), KP(exec_ctx_), KP(ctx));
  } else {
    ObCsvTaskGenContext *csv_ctx = static_cast<ObCsvTaskGenContext *>(ctx);
    oceanbase::sql::GIExternalTaskArgs &final_task_args = csv_ctx->final_task_args_;
    final_task_args.scan_tasks_.reset();
    final_task_args.taskset_idxs_.reset();
    final_task_args.taskset_tablets_.reset();
    int64_t scan_task_idx = 0;

    struct CsvFullScanResultProcessor {
      ObArenaAllocator &arena_alloc_;
      ObCsvTaskGenContext *csv_ctx_;
      oceanbase::sql::GIExternalTaskArgs &final_task_args_;
      int64_t &scan_task_idx_;

      CsvFullScanResultProcessor(ObArenaAllocator &arena_alloc,
                                 ObCsvTaskGenContext *csv_ctx,
                                 oceanbase::sql::GIExternalTaskArgs &final_task_args,
                                 int64_t &scan_task_idx)
        : arena_alloc_(arena_alloc),
          csv_ctx_(csv_ctx),
          final_task_args_(final_task_args),
          scan_task_idx_(scan_task_idx) {}

      int operator()(ObIArray<ObCsvFullScanResult> &results,
                     int64_t start_idx, int64_t end_idx) {
        int ret = OB_SUCCESS;
        sql::ObIExtTblScanTask *original_task = results.at(start_idx).scan_task_;
        sql::ObExtTableScanTask *ext_task = static_cast<sql::ObExtTableScanTask *>(original_task);
        ObDASTabletLoc *tablet_loc = results.at(start_idx).tablet_loc_;
        ObArray<int64_t> bounded_start_pos;
        int64_t enclosed_cnt = 0;
        LOG_TRACE("join full scan results, the range of one file's results", K(start_idx), K(end_idx));
        if (OB_ISNULL(original_task) || OB_ISNULL(tablet_loc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("original scan task is null", K(ret));
        } else {
          for (int64_t i = start_idx; i <= end_idx && OB_SUCC(ret); ++i) {
            ObCsvGamblingResult &gambling_result = csv_ctx_->gambling_results_.at(i);
            ObCsvFullScanResult &full_scan_result = results.at(i);
            bool is_find_split_pos = false;
            // handle the part of the chunk scanned in gambling
            if (i == start_idx) {
              OZ (bounded_start_pos.push_back(0));
              is_find_split_pos = true;
            } else if (enclosed_cnt % 2 == 0 && gambling_result.even_pos_ != OB_INVALID_INDEX) {
              OZ (bounded_start_pos.push_back(gambling_result.even_pos_ + 1));
              is_find_split_pos = true;
            } else if (enclosed_cnt % 2 == 1 && gambling_result.odd_pos_ != OB_INVALID_INDEX) {
              OZ (bounded_start_pos.push_back(gambling_result.odd_pos_ + 1));
              is_find_split_pos = true;
            }
            enclosed_cnt += gambling_result.enclosed_cnt_;

            // handle the part of the chunk scanned in full scan
            if (OB_SUCC(ret) && !is_find_split_pos) {
              if (enclosed_cnt % 2 == 0 && full_scan_result.even_pos_ != OB_INVALID_INDEX) {
                OZ (bounded_start_pos.push_back(full_scan_result.even_pos_ + 1));
              } else if (enclosed_cnt % 2 == 1 && full_scan_result.odd_pos_ != OB_INVALID_INDEX) {
                OZ (bounded_start_pos.push_back(full_scan_result.odd_pos_ + 1));
              }
            }
            enclosed_cnt += full_scan_result.enclosed_cnt_;
          }

          for (int64_t i = 0; OB_SUCC(ret) && i < bounded_start_pos.count(); ++i) {
            int64_t start_pos = bounded_start_pos.at(i);
            int64_t end_pos = (i + 1 < bounded_start_pos.count()) ? bounded_start_pos.at(i+1) : ext_task->file_size_;
            ObExtTableScanTask *scan_task = OB_NEWx(ObExtTableScanTask, (&arena_alloc_));
            if (OB_ISNULL(scan_task)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate scan task", K(ret));
            } else if (OB_FAIL(scan_task->init_parallel_parse_csv_info(arena_alloc_))) {
              LOG_WARN("failed to init parallel parse csv info", K(ret));
            } else if (OB_FAIL(ObExternalTableUtils::make_parallel_parse_csv_task(*ext_task,
                                                                                  1, INT64_MAX,
                                                                                  start_pos, end_pos,
                                                                                  i, bounded_start_pos.count(),
                                                                                  scan_task))) {
              LOG_WARN("failed to make table scan task", K(ret));
            } else {
              if (OB_ISNULL(scan_task->parallel_parse_csv_info_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("parallel parse csv info is null", K(ret));
              } else {
                scan_task->parallel_parse_csv_info_->csv_task_type_ = CsvTaskType::PARSE_DATA;
              }
              OZ (final_task_args_.scan_tasks_.push_back(scan_task));
              OZ (final_task_args_.taskset_idxs_.push_back(scan_task_idx_++));
              OZ (final_task_args_.taskset_tablets_.push_back(tablet_loc));
            }
          }
        }
        return ret;
      }
    };

    if (all_task_results.count() > 0) {
      if (all_task_results.count() != csv_ctx->gambling_results_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task results count not equal to gambling results count", K(ret),
                 K(all_task_results.count()), K(csv_ctx->gambling_results_.count()));
      } else {
        if (OB_LOGGER.need_to_print(OB_LOG_LEVEL_TRACE)) {
          for (int64_t i = 0; i < all_task_results.count(); i++) {
            LOG_TRACE("full scan task result detail", K(i), K(all_task_results.at(i)));
          }
        }
        CsvFullScanResultProcessor processor(arena_alloc_, csv_ctx, final_task_args, scan_task_idx);
        if (OB_FAIL(process_csv_tasks_by_file_id(all_task_results, processor))) {
          LOG_WARN("failed to process csv tasks by file id", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < csv_ctx->data_scan_tasks_.count(); ++i) {
        OZ (final_task_args.scan_tasks_.push_back(csv_ctx->data_scan_tasks_.at(i).scan_task_));
        OZ (final_task_args.taskset_idxs_.push_back(scan_task_idx++));
        OZ (final_task_args.taskset_tablets_.push_back(csv_ctx->data_scan_tasks_.at(i).tablet_loc_));
      }
    }
    LOG_INFO("join full scan results, the amount of scan tasks for parsing data", K(ret), K(final_task_args.scan_tasks_.count()));
    if (OB_LOGGER.need_to_print(OB_LOG_LEVEL_TRACE)) {
      for (int64_t i = 0; i < final_task_args.scan_tasks_.count(); i++) {
        LOG_TRACE("scan task detail", K(ret), K(i), KPC(final_task_args.scan_tasks_.at(i)));
      }
    }
  }
  return ret;
}

int GITaskGenRunnerBuilder::ensure_runner_created() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(runner_)) {
    if (OB_FAIL(pump_->create_and_add_runner(tsc_op_id_, exec_ctx_, runner_))) {
      LOG_WARN("failed to create and add runner", K(ret), K(tsc_op_id_));
    }
  }
  return ret;
}

int GITaskGenRunnerBuilder::build(GITaskGenRunner *&runner) {
  runner = nullptr;

  if (has_error()) {
  } else if (OB_ISNULL(runner_)) {
    ret_ = OB_ERR_UNEXPECTED;
  } else {
    runner = runner_;
    runner_ = nullptr;
  }

  return ret_;
}

int GITaskGenRunner::is_finished(bool &finished) const
{
  int ret = OB_SUCCESS;
  finished = false;
  if (one_stage_task_gens_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no task gen", K(ret));
  } else {
    int64_t idx = one_stage_task_gens_.count() - 1;
    if (OB_ISNULL(one_stage_task_gens_.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task gen is null", K(ret), K(idx));
    } else {
      finished = one_stage_task_gens_.at(idx)->is_finished();
    }
  }
  return ret;
}

int GITaskGenRunner::is_succeed(bool &succeed) const
{
  int ret = OB_SUCCESS;
  succeed = false;
  if (one_stage_task_gens_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no task gen", K(ret));
  } else {
    int64_t idx = one_stage_task_gens_.count() - 1;
    if (OB_ISNULL(one_stage_task_gens_.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task gen is null", K(ret), K(idx));
    } else {
      succeed = one_stage_task_gens_.at(idx)->is_succeed();
    }
  }
  return ret;
}

int GITaskGenRunner::run_task(ObGranuleIteratorOp *gi_op) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gi_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gi_op is null", K(ret));
  } else if (one_stage_task_gens_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no task gen to run", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < one_stage_task_gens_.count(); i++) {
      if (OB_ISNULL(one_stage_task_gens_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task gen is null", K(ret), K(i));
      } else if (OB_FAIL(one_stage_task_gens_.at(i)->gen_task_parallel(
                  exec_ctx_, gi_op, ctx_))) {
        LOG_WARN("failed to gen task", K(ret), K(i));
      }
    }
  }
  return ret;
}

int GamblingFunctor::init(const ObExternalFileFormat &file_format) {
  int ret = OB_SUCCESS;
  if (file_format.csv_format_.field_enclosed_char_ == INT64_MAX) {
    --target_finish_cnt_;
    has_enclosed_char_ = false;
  }
  for (int64_t i = 0; i < START_STATE_CNT; ++i) {
    potential_start_pos_[i] = OB_INVALID_INDEX;
  }
  cur_states_[START_UNQUOTED_FIELD_IDX] = CSVState::UNQUOTED_FIELD;
  cur_states_[START_QUOTED_FIELD_IDX] = CSVState::QUOTED_FIELD;
  prev_states_[START_UNQUOTED_FIELD_IDX] = CSVState::INVALID;
  prev_states_[START_QUOTED_FIELD_IDX] = CSVState::INVALID;
  is_inited_ = true;
  return ret;
}

int GamblingFunctor::update_state(const CSVCharCategory category, const int64_t idx) {
  int ret = OB_SUCCESS;
  int64_t input_idx = static_cast<int64_t>(category);
  if (input_idx < 0 || input_idx >= MAX_INPUT_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, input_idx is out of range", K(ret), K(input_idx));
  }

  for (int64_t k = 0; k < START_STATE_CNT && OB_SUCC(ret); ++k) {
    if (k == START_QUOTED_FIELD_IDX && !has_enclosed_char_) {
      // do nothing
    } else {
      CSVState &cur_state = cur_states_[k];
      CSVState next_state = CSVState::INVALID;
      if (cur_state != CSVState::INVALID && cur_state != CSVState::RECORD_START) {
        int64_t state_idx = static_cast<int64_t>(cur_state);
        if (state_idx < 0 || state_idx >= MAX_VALID_STATE_CNT) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, state_idx is out of range", K(ret), K(state_idx));
        } else {
          next_state = state_transition_matrix_[state_idx][input_idx];
          if (next_state == CSVState::SPECIAL) {  // 转义状态的转换比较特殊
            if (prev_states_[k] == CSVState::QUOTED_FIELD) {
              next_state = CSVState::QUOTED_FIELD;
            } else {
              next_state = CSVState::UNQUOTED_FIELD;
            }
          }
          prev_states_[k] = cur_state;
          cur_state = next_state;
        }

        if (OB_FAIL(ret)) {
        } else if (cur_state == CSVState::INVALID || cur_state == CSVState::RECORD_START) {  // terminate condition
          if (cur_state == CSVState::RECORD_START) {
            potential_start_pos_[k] = bound_helper_.already_read_size_ + idx + 1;
          }
          ++finish_cnt_;
        }
      }
    }
  }
  return ret;
}

int GamblingFunctor::verify_gambling_success()
{
  int ret = OB_SUCCESS;
  // verify if only one record start is found, or multiple record have the same start pos
  if (finish_cnt_ > target_finish_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, finish_cnt_ is greater than target_finish_cnt_", K(ret), K(finish_cnt_), K(target_finish_cnt_));
  } else if (finish_cnt_ == target_finish_cnt_) {
    is_finished_ = true;
    bool is_same = true;
    succ_idx_ = OB_INVALID_INDEX;
    for (int64_t k = 0; k < START_STATE_CNT && is_same; ++k) {
      if (k == START_QUOTED_FIELD_IDX && !has_enclosed_char_) {
        // do nothing
      } else if (cur_states_[k] == CSVState::RECORD_START) {
        if (succ_idx_ == OB_INVALID_INDEX) {
          succ_idx_ = k;
        } else if (potential_start_pos_[k] != potential_start_pos_[succ_idx_]) {
          is_same = false;
        }
      } else if (cur_states_[k] != CSVState::INVALID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, state neither record start nor invalid", K(ret), K(k), K(cur_states_[k]));
      }
    }
    if (succ_idx_ != OB_INVALID_INDEX && is_same) {
      is_success_ = true;
    }
  }
  return ret;
}

GamblingFunctor::CSVCharCategory GamblingFunctor::get_char_category(char ch, const ObExternalFileFormat &file_format)
{
  CSVCharCategory category = CSVCharCategory::INVALID;
  if (ch == file_format.csv_format_.field_enclosed_char_) {
    category = CSVCharCategory::ENCLOSING_CHAR;
  } else if (!file_format.csv_format_.field_term_str_.empty()
              && ch == file_format.csv_format_.field_term_str_.ptr()[0]) {
    category = CSVCharCategory::FIELD_DELIMITER;
  } else if (!file_format.csv_format_.line_term_str_.empty()
              && ch == file_format.csv_format_.line_term_str_.ptr()[0]) {
    category = CSVCharCategory::LINE_DELIMITER;
  } else if (ch == file_format.csv_format_.field_escaped_char_) {
    category = CSVCharCategory::ESCAPE_CHAR;
  } else {
    category = CSVCharCategory::OTHER;
  }
  return category;
}

void GamblingFunctor::print_state() const
{
  LOG_INFO("gambling state", K(has_enclosed_char_), K(is_finished_), K(is_success_), K(finish_cnt_),
     K(target_finish_cnt_), K(succ_idx_), K(potential_start_pos_[0]), K(potential_start_pos_[1]), K(cur_states_[0]), K(cur_states_[1]));
}

int GamblingFunctor::operator()(const ObExternalFileFormat &file_format, char *buf, int64_t content_len) {
  int ret = OB_SUCCESS;

  if (!is_inited_ && OB_FAIL(init(file_format))) {
    LOG_WARN("failed to init", K(ret));
  }

  for (int64_t i = 0; i < content_len && !is_finished_ && OB_SUCC(ret); ++i) {
    char ch = buf[i];
    // 1. gambling bound
    CSVCharCategory category = get_char_category(ch, file_format);
    if (category == CSVCharCategory::INVALID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, char category is invalid", K(ret), K(ch));
    } else if (OB_FAIL(update_state(category, i))) {
      LOG_WARN("failed to update state", K(ret));
    } else if (OB_FAIL(verify_gambling_success())) {
      LOG_WARN("failed to verify gambling success", K(ret));
    } else {
      // 2. full scan bound
      bound_helper_.process_char(ch, bound_helper_.already_read_size_ + i, file_format);
    }
  }

  if (OB_SUCC(ret)) {
    bound_helper_.already_read_size_ += content_len;
    if (bound_helper_.already_read_size_ > file_format.csv_format_.max_row_length_) {  // early stop
      is_finished_ = true;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
