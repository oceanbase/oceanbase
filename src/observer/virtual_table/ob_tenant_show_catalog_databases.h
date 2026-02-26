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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_CATALOG_DATABASES_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_CATALOG_DATABASES_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObTenantShowCatalogDatabases final : public common::ObVirtualTableScannerIterator
{
public:
  ObTenantShowCatalogDatabases() = default;
  virtual ~ObTenantShowCatalogDatabases() override = default;
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

private:
  enum class ALL_COLUMNS
  {
    CATALOG_ID = common::OB_APP_MIN_COLUMN_ID,
    DATABASE_NAME,
  };
  int fill_scanner();
  uint64_t tenant_id_ = OB_INVALID_ID;
  uint64_t catalog_id_ = OB_INVALID_ID;
  DISALLOW_COPY_AND_ASSIGN(ObTenantShowCatalogDatabases);
};
} // namespace observer
} // namespace oceanbase

#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_CATALOG_DATABASES_ */
