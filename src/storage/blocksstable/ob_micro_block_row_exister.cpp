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
#include "storage/access/ob_table_access_param.h"

namespace oceanbase {
namespace blocksstable {

int ObMicroBlockRowExister::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowFetcher::init(param, context, sstable))) {
    LOG_WARN("fail to init", K(ret));
  } else if (OB_ISNULL(read_info_ = param.get_read_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null columns info", K(ret), K(param));
  }
  return ret;
}

int ObMicroBlockRowExister::is_exist(
    const ObDatumRowkey &rowkey,
    const ObMicroBlockData &block_data,
    bool &exist,
    bool &found)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the micro block row exister has not been inited", K(ret));
  } else if (OB_FAIL(prepare_reader(block_data.get_store_type()))) {
    LOG_WARN("failed to prepare reader", K(ret), K(block_data));
  } else if (OB_FAIL(reader_->exist_row(block_data, rowkey, *read_info_, exist, found))) {
    LOG_WARN("failed to check exist row", K(ret), K(rowkey));
  } else {
    LOG_DEBUG("Success to check exist row", K(rowkey), K(exist), K(found));
  }
  return ret;
}

}
}
