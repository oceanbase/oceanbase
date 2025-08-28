/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "ob_hbase_cell_iter.h"

namespace oceanbase
{   
namespace table
{

ObHbaseCellIter::ObHbaseCellIter()
    : ObHbaseICellIter(),
      allocator_("HtCelIterAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      tb_row_iter_(),
      tb_ctx_(allocator_),
      is_opened_(false)
{}

int ObHbaseCellIter::open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tb_row_iter_.open())) {
    LOG_WARN("fail to open tb_row_iter", K(ret));
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObHbaseCellIter::get_next_cell(ObNewRow *&row)
{
  return OB_SUCCESS;
}

int ObHbaseCellIter::rescan(ObHbaseRescanParam &rescan_param)
{
  int ret = OB_SUCCESS;
  tb_ctx_.set_limit(rescan_param.get_limit());
  ObIArray<ObNewRange> &key_ranges = tb_ctx_.get_key_ranges();
  key_ranges.reset();
  tb_ctx_.set_batch_tablet_ids(nullptr);
  if (OB_FAIL(key_ranges.push_back(rescan_param.get_scan_range()))) {
    LOG_WARN("fail to push back scan range", K(ret), K(rescan_param));
  } else if (OB_FAIL(tb_row_iter_.rescan())) {
    LOG_WARN("fail to rescan tb_row_iter", K(ret), K(rescan_param));
  }
  return ret;
}

int ObHbaseCellIter::close()
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    // do nothing
  } else if (OB_FAIL(tb_row_iter_.close())) {
    LOG_WARN("fail to close tb_row_iter", K(ret));
  }
  return ret;
}

}  // end of namespace table
}  // end of namespace oceanbase
