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
