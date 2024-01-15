/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
