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

#include "ob_locality_adapter.h"
#include "storage/ob_locality_manager.h"

namespace oceanbase
{
namespace logservice
{

ObLocalityAdapter::ObLocalityAdapter() :
    is_inited_(false),
    locality_manager_(NULL)
  {}

ObLocalityAdapter::~ObLocalityAdapter()
{
  destroy();
}

int ObLocalityAdapter::init(storage::ObLocalityManager *locality_manager)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLocalityAdapter init twice");
  } else if (OB_ISNULL(locality_manager)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KP(locality_manager));
  } else {
    locality_manager_ = locality_manager;
    is_inited_ = true;
    PALF_LOG(INFO, "ObLocalityAdapter init success", K(locality_manager_));
  }
  return ret;
}

void ObLocalityAdapter::destroy()
{
  is_inited_ = false;
  locality_manager_ = NULL;
}

int ObLocalityAdapter::get_server_region(const common::ObAddr &server,
                                         common::ObRegion &region) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(locality_manager_->get_server_region(server, region))) {
    CLOG_LOG(WARN, "get_server_region failed", K(server));
  }
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase
