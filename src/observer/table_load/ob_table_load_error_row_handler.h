// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

#pragma once

#include "common/row/ob_row.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadResultInfo;
} // namespace table
namespace observer
{
class ObTableLoadStoreCtx;

class ObTableLoadErrorRowHandler : public ObDirectLoadDMLRowHandler
{
public:
  ObTableLoadErrorRowHandler();
  virtual ~ObTableLoadErrorRowHandler();
  int init(ObTableLoadStoreCtx *store_ctx);
  int handle_insert_row(const blocksstable::ObDatumRow &row) override;
  int handle_update_row(const blocksstable::ObDatumRow &row) override;
  int handle_update_row(const blocksstable::ObDatumRow &old_row,
                        const blocksstable::ObDatumRow &new_row,
                        const blocksstable::ObDatumRow *&result_row) override;
  int handle_error_row(int error_code, const common::ObNewRow &row);
  int handle_error_row(int error_code, const blocksstable::ObDatumRow &row);
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
