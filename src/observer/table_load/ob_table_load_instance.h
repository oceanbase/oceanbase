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
class ObTableLoadExecCtx;

class ObTableLoadInstance
{
  static const int64_t WAIT_INTERVAL_US = 1LL * 1000 * 1000; // 1s
public:
  ObTableLoadInstance();
  ~ObTableLoadInstance();
  void destroy();
  int init(ObTableLoadParam &param, const common::ObIArray<int64_t> &idx_array,
           ObTableLoadExecCtx *execute_ctx);
  int write(int32_t session_id, const table::ObTableLoadObjRowArray &obj_rows);
  int commit(table::ObTableLoadResultInfo &result_info);
  int px_commit_data();
  int px_commit_ddl();
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
  int create_table_ctx(ObTableLoadParam &param, const common::ObIArray<int64_t> &idx_array);
  int begin();
  int start_trans();
  int check_trans_committed();
  int check_merged();
private:
  struct TransCtx
  {
  public:
    void reset()
    {
      trans_id_.reset();
      next_sequence_no_array_.reset();
    }
  public:
    table::ObTableLoadTransId trans_id_;
    table::ObTableLoadArray<uint64_t> next_sequence_no_array_;
  };
private:
  static const int64_t DEFAULT_SEGMENT_ID = 1;
  ObTableLoadExecCtx *execute_ctx_;
  common::ObIAllocator *allocator_;
  ObTableLoadTableCtx *table_ctx_;
  sql::ObLoadDataStat *job_stat_;
  TransCtx trans_ctx_;
  bool is_committed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadInstance);
};

} // namespace observer
} // namespace oceanbase