/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MOCK_ACCESS_SERVICE
#define OCEANBASE_STORAGE_MOCK_ACCESS_SERVICE

#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace storage
{
class ObLSTabletService;

class MockObAccessService : public ObAccessService
{
public:
  MockObAccessService(ObLSTabletService *tablet_service = nullptr);
  virtual ~MockObAccessService() = default;
public:
  int insert_rows(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows);
public:
  ObLSTabletService *tablet_service_; // different kinds of mock ls tablet service
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MOCK_ACCESS_SERVICE
