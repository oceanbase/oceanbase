/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOCALITY_ADAPTER_H_
#define OCEANBASE_LOGSERVICE_OB_LOCALITY_ADAPTER_H_

#include <stdint.h>
#include "logservice/palf/palf_callback.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace storage
{
class ObLocalityManager;
}
namespace logservice
{
class ObLocalityAdapter : public palf::PalfLocalityInfoCb
{
public:
  ObLocalityAdapter();
  virtual ~ObLocalityAdapter();
  int init(storage::ObLocalityManager *locality_manager);
  void destroy();
public:
  int get_server_region(const common::ObAddr &server, common::ObRegion &region) const override final;
private:
  bool is_inited_;
  storage::ObLocalityManager *locality_manager_;
};

} // logservice
} // oceanbase

#endif
