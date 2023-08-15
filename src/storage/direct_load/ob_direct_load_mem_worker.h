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
#pragma once

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

class ObIDirectLoadPartitionTable;

class ObDirectLoadMemWorker
{
public:
  virtual ~ObDirectLoadMemWorker() {}

  virtual int add_table(ObIDirectLoadPartitionTable *table) = 0;
  virtual int work() = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // namespace storage
} // namespace oceanbase
