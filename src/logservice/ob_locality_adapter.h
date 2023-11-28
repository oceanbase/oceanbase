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
