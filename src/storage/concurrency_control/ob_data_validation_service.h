/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_DATA_VALIDATION_SERVICE
#define OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_DATA_VALIDATION_SERVICE

#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace concurrency_control
{

class ObDataValidationService
{
public:
  static bool need_delay_resource_recycle(const ObLSID ls_id);
  static void set_delay_resource_recycle(const ObLSID ls_id);
};

} // namespace concurrency_control
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_DATA_VALIDATION_SERVICE
