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
class ObTableLoadCoordinatorCtx;
class ObTableLoadParam;

class ObTableLoadErrorRowHandler : public ObDirectLoadDMLRowHandler
{
public:
  ObTableLoadErrorRowHandler();
  virtual ~ObTableLoadErrorRowHandler();
  int init(const ObTableLoadParam &param, table::ObTableLoadResultInfo &result_info,
           sql::ObLoadDataStat *job_stat);
  int handle_insert_row(const blocksstable::ObDatumRow &row) override;
  int handle_update_row(const blocksstable::ObDatumRow &row) override;
  int handle_update_row(common::ObArray<const ObDirectLoadExternalRow *> &rows,
                        const ObDirectLoadExternalRow *&row) override;
  int handle_update_row(common::ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
                        const ObDirectLoadMultipleDatumRow *&row) override;
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
