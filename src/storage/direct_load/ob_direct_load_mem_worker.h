/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include "lib/utility/ob_print_utils.h"
#include "storage/direct_load/ob_direct_load_i_table.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMemWorker
{
public:
  virtual ~ObDirectLoadMemWorker() {}

  virtual int add_table(const ObDirectLoadTableHandle &table_handle) = 0;
  virtual int work() = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // namespace storage
} // namespace oceanbase
