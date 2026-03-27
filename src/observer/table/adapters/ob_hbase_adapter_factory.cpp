/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#define USING_LOG_PREFIX SERVER
#include "ob_hbase_adapter_factory.h"

namespace oceanbase
{
namespace table
{
int ObHbaseAdapterGuard::get_hbase_adapter(ObIHbaseAdapter *&hbase_adapter, ObHbaseModeType mode_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hbase_adapter_)) {
    if (OB_FAIL(ObHbaseAdapterFactory::alloc_hbase_adapter(allocator_,
                                                           mode_type,
                                                           hbase_adapter_))) {
      LOG_WARN("failed to alloc hbase adapter", K(ret), K(mode_type));
    } else if (OB_ISNULL(hbase_adapter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null hbase adapter", K(ret), K(mode_type));
    }
  }

  if (OB_SUCC(ret)) {
    hbase_adapter = hbase_adapter_;
  }
  return ret;
}

} // end of namespace table
} // end of namespace oceanbase