/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_MULTI_DATA_SOURCE_PRINTER_H
#define OCEANBASE_TRANSACTION_OB_MULTI_DATA_SOURCE_PRINTER_H

#include <stdint.h>

namespace oceanbase
{
namespace transaction
{
enum class ObTxDataSourceType : int64_t;
enum class NotifyType : int64_t;

class ObMultiDataSourcePrinter
{
public:
  static const char *to_str_mds_type(const ObTxDataSourceType &mds_type);
  static const char *to_str_notify_type(const NotifyType &notify_type);
};
} // namespace transaction
} // namespace oceanbase

#endif // OCEANBASE_TRANSACTION_OB_MULTI_DATA_SOURCE_PRINTER_H
