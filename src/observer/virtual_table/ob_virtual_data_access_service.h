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

#include "share/ob_i_data_access_service.h"
#include "ob_virtual_table_iterator_factory.h"

namespace oceanbase {
namespace common {
class ObVTableScanParam;
class ObIDataAccessService;
class ObNewRowIterator;
class ObServerConfig;
}  // namespace common
namespace rootserver {
class ObRootService;
}
namespace observer {
class ObVirtualDataAccessService : public common::ObIDataAccessService {
public:
  ObVirtualDataAccessService(
      rootserver::ObRootService& root_service, common::ObAddr& addr, common::ObServerConfig* config)
      : vt_iter_factory_(root_service, addr, config)
  {}
  virtual ~ObVirtualDataAccessService()
  {}

  virtual int table_scan(common::ObVTableScanParam& param, common::ObNewRowIterator*& result);
  virtual int revert_scan_iter(common::ObNewRowIterator* iter);

  virtual int join_mv_scan(storage::ObTableScanParam&, storage::ObTableScanParam&, common::ObNewRowIterator*&)
  {
    int ret = common::OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "virtual data access not support join mv scan interface", K(ret));
    return ret;
  }

  ObVirtualTableIteratorFactory& get_vt_iter_factory()
  {
    return vt_iter_factory_;
  }

private:
  ObVirtualTableIteratorFactory vt_iter_factory_;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualDataAccessService);
};
}  // namespace observer
}  // namespace oceanbase
#endif
