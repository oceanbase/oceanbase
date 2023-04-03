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

#ifndef OCEANBASE_OB_VIRTUAL_DATA_ACCESS_SERVICE_H_
#define OCEANBASE_OB_VIRTUAL_DATA_ACCESS_SERVICE_H_

#include "share/ob_i_tablet_scan.h"
#include "ob_virtual_table_iterator_factory.h"

namespace oceanbase
{
namespace common
{
class ObVTableScanParam;
class ObNewRowIterator;
class ObServerConfig;
}
namespace rootserver
{
class ObRootService;
}
namespace observer
{
class ObVirtualDataAccessService : public common::ObITabletScan
{
public:
  ObVirtualDataAccessService(
      rootserver::ObRootService &root_service,
      common::ObAddr &addr,
      common::ObServerConfig *config)
      : vt_iter_factory_(root_service, addr, config)
  {
  }
  virtual ~ObVirtualDataAccessService() {}

  virtual int table_scan(common::ObVTableScanParam &param, common::ObNewRowIterator *&result);

  virtual int revert_scan_iter(common::ObNewRowIterator *iter);

  ObVirtualTableIteratorFactory &get_vt_iter_factory() { return vt_iter_factory_; }
  virtual int check_iter(common::ObVTableScanParam &param);
private:
  ObVirtualTableIteratorFactory vt_iter_factory_;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualDataAccessService);
};
}
}
#endif
