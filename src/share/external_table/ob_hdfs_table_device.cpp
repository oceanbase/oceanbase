/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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