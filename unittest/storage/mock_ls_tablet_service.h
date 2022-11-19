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

#ifndef OCEANBASE_STORAGE_MOCK_LS_TABLET_SERVICE
#define OCEANBASE_STORAGE_MOCK_LS_TABLET_SERVICE

#include "storage/ls/ob_ls_tablet_service.h"

namespace oceanbase
{
namespace storage
{
// mock different ls tablet service for dml interfaces(table_scan, insert_rows...)
// just override prepare_dml_running_ctx

class MockInsertRowsLSTabletService : public ObLSTabletService
{
public:
  MockInsertRowsLSTabletService() = default;
  virtual ~MockInsertRowsLSTabletService() = default;
protected:
  virtual int prepare_dml_running_ctx(
      const common::ObIArray<uint64_t> *column_ids,
      const common::ObIArray<uint64_t> *upd_col_ids,
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx) override;
private:
  int prepare_relative_tables(
      const int64_t schema_version,
      const common::ObIArray<uint64_t> *upd_col_ids,
      common::ObIArray<ObTabletHandle> &tablet_handles,
      ObDMLRunningCtx &run_ctx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MOCK_LS_TABLET_SERVICE
