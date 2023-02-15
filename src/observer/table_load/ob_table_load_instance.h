// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#pragma once

#include "share/table/ob_table_load_row_array.h"
#include "share/table/ob_table_load_define.h"
#include "sql/engine/cmd/ob_load_data_utils.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadParam;
class ObTableLoadTableCtx;
class ObTableLoadCoordinator;
class ObTableLoadExecCtx;

class ObTableLoadInstance
{
  static const int64_t WAIT_INTERVAL_US = 1LL * 1000 * 1000; // 1s
public:
  ObTableLoadInstance();
  ~ObTableLoadInstance();
  int init(observer::ObTableLoadParam &param,
      const oceanbase::common::ObIArray<int64_t> &idx_array,
      observer::ObTableLoadExecCtx *execute_ctx);
  void destroy();
  int commit(table::ObTableLoadResultInfo &result_info);
  int px_commit_data();
  int px_commit_ddl();
  int write(int32_t session_id, const table::ObTableLoadObjRowArray &obj_rows);
  sql::ObLoadDataStat *get_job_stat() const { return job_stat_; }
  void update_job_stat_parsed_rows(int64_t parsed_rows)
  {
    ATOMIC_AAF(&job_stat_->parsed_rows_, parsed_rows);
  }
  void update_job_stat_parsed_bytes(int64_t parsed_bytes)
  {
    ATOMIC_AAF(&job_stat_->parsed_bytes_, parsed_bytes);
  }
private:
  int check_trans_committed();
  int check_merged();
private:
  static const int64_t DEFAULT_SEGMENT_ID = 1;
  common::ObIAllocator *allocator_;
  ObTableLoadExecCtx *execute_ctx_;
  ObTableLoadTableCtx *table_ctx_;
  ObTableLoadCoordinator *coordinator_;
  table::ObTableLoadTransId trans_id_;
  table::ObTableLoadArray<uint64_t> next_sequence_no_array_;
  sql::ObLoadDataStat *job_stat_;
  bool px_mode_;
  bool online_opt_stat_gather_;
  uint64_t sql_mode_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadInstance);
};

} // namespace observer
} // namespace oceanbase