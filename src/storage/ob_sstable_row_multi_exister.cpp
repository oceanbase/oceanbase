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

#include "ob_sstable_row_multi_exister.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace storage {

ObSSTableRowMultiExister::ObSSTableRowMultiExister() : store_row_()
{}

ObSSTableRowMultiExister::~ObSSTableRowMultiExister()
{}

int ObSSTableRowMultiExister::fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exist_row(read_handle, store_row_))) {
    STORAGE_LOG(WARN, "Fail to check exist row, ", K(ret));
  } else {
    store_row = &store_row_;
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
