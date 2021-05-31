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
#include "ob_micro_block_row_exister.h"

namespace oceanbase {
namespace blocksstable {

ObMicroBlockRowExister::ObMicroBlockRowExister()
{}

ObMicroBlockRowExister::~ObMicroBlockRowExister()
{}

int ObMicroBlockRowExister::is_exist(const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
    const ObMicroBlockData& block_data, const storage::ObSSTableRowkeyHelper* rowkey_helper, bool& exist, bool& found)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the micro block row exister has not been inited, ", K(ret));
  } else if (OB_FAIL(prepare_reader(macro_meta))) {
    LOG_WARN("failed to prepare reader", K(ret));
  } else if (OB_FAIL(reader_->exist_row(
                 context_->pkey_.get_tenant_id(), block_data, rowkey, macro_meta, rowkey_helper, exist, found))) {
    LOG_WARN("failed to check exist row, ", K(ret), K(rowkey));
  } else {
    LOG_DEBUG("Success to check exist row, ", K(rowkey), K(exist), K(found));
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
