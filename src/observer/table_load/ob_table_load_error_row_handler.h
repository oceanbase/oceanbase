/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "sql/engine/cmd/ob_load_data_utils.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadResultInfo;
} // namespace table
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadCoordinatorCtx;
class ObTableLoadParam;

class ObTableLoadErrorRowHandler
{
public:
  ObTableLoadErrorRowHandler();
  virtual ~ObTableLoadErrorRowHandler();
  int init(const ObTableLoadParam &param, table::ObTableLoadResultInfo &result_info,
           sql::ObLoadDataStat *job_stat);
  int handle_error_row(int error_code);
  int handle_error_row(int error_code, int64_t duplicate_row_count);
  uint64_t get_error_row_count() const;
  TO_STRING_KV(K_(dup_action), K_(max_error_row_count), K_(error_row_count));
private:
  sql::ObLoadDupActionType dup_action_;
  uint64_t max_error_row_count_;
  table::ObTableLoadResultInfo *result_info_;
  sql::ObLoadDataStat *job_stat_;
  mutable lib::ObMutex mutex_;
  uint64_t error_row_count_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
