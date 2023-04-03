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
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
public:
  ObLSTabletService *tablet_service_; // different kinds of mock ls tablet service
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MOCK_ACCESS_SERVICE
