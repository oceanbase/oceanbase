/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_hdfs_table_device.h"

namespace oceanbase
{
namespace share
{
ObHDFSTableDevice::ObHDFSTableDevice()
 :external_storage_info_()
{
}

ObHDFSTableDevice::~ObHDFSTableDevice()
{
  destroy();
}

int ObHDFSTableDevice::setup_storage_info(const ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(external_storage_info_.set(device_type_, opts.opts_[0].value_.value_str))) {
    OB_LOG(WARN, "failed to build external storage info", K(ret));
  }
  return ret;
}

void ObHDFSTableDevice::destroy()
{
  ObObjectDevice::destroy();
}

} // namespace share
} // namespace oceanbase