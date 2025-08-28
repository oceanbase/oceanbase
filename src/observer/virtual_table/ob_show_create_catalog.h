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

#ifndef OCEANBASE_OBSERVER_OB_SHOW_CREATE_CATALOG_
#define OCEANBASE_OBSERVER_OB_SHOW_CREATE_CATALOG_
#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "src/share/catalog/ob_catalog_properties.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace observer
{
class ObShowCreateCatalog : public common::ObVirtualTableScannerIterator
{
public:
  ObShowCreateCatalog();
  virtual ~ObShowCreateCatalog();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

private:
  int calc_show_catalog_id(uint64_t &show_catalog_id);
  int fill_row_cells(uint64_t show_catalog_id, const common::ObString &catalog_name);
  int print_catalog_definition(const uint64_t tenant_id,
                               const uint64_t catalog_id,
                               char *buf,
                               const int64_t &buf_len,
                               int64_t &pos) const;
  int print_odps_catalog_definition(const share::ObODPSCatalogProperties &odps,
                                    char *buf,
                                    const int64_t &buf_len,
                                    int64_t &pos) const;
  int print_hive_catalog_definition(const share::ObHMSCatalogProperties &odps,
                                    char *buf, const int64_t &buf_len,
                                    int64_t &pos) const;


  int print_filesystem_catalog_definition(const share::ObFilesystemCatalogProperties &properties,
                                          char *buf,
                                          const int64_t &buf_len,
                                          int64_t &pos) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObShowCreateCatalog);
};
} // namespace observer
} // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_SHOW_CREATE_CATALOG_ */
