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

#define USING_LOG_PREFIX STORAGE
#include "ob_sstable_row_multi_scanner.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
ObSSTableRowMultiScanner::~ObSSTableRowMultiScanner()
{
}

void ObSSTableRowMultiScanner::reset()
{
  ObSSTableRowScanner::reset();
}

void ObSSTableRowMultiScanner::reuse()
{
  ObSSTableRowScanner::reuse();
}

}
}
