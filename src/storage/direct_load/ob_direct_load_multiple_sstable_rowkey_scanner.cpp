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

#include "storage/direct_load/ob_direct_load_multiple_sstable_rowkey_scanner.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;


/**
 * ObDirectLoadMultipleSSTableRowkeyScanner
 */

ObDirectLoadMultipleSSTableRowkeyScanner::ObDirectLoadMultipleSSTableRowkeyScanner()
  : sstable_(nullptr), fragment_idx_(0), is_inited_(false)
{
}

ObDirectLoadMultipleSSTableRowkeyScanner::~ObDirectLoadMultipleSSTableRowkeyScanner()
{
}

int ObDirectLoadMultipleSSTableRowkeyScanner::init(
  ObDirectLoadMultipleSSTable *sstable,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableRowkeyScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid() ||
                         !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable), K(table_data_desc));
  } else {
    if (OB_FAIL(data_block_reader_.init(table_data_desc.sstable_data_block_size_,
                                        table_data_desc.compressor_type_))) {
      LOG_WARN("fail to data block reader", KR(ret));
    } else {
      sstable_ = sstable;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableRowkeyScanner::get_next_rowkey(const ObDirectLoadMultipleDatumRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableRowkeyScanner not init", KR(ret), KP(this));
  } else {
    while (OB_SUCC(ret) && nullptr == rowkey) {
      if (OB_FAIL(data_block_reader_.get_next_row(rowkey))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(switch_next_fragment())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to switch next fragment", KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableRowkeyScanner::switch_next_fragment()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObDirectLoadMultipleSSTableFragment> &fragments = sstable_->get_fragments();
  data_block_reader_.reuse();
  while (OB_SUCC(ret) && !data_block_reader_.is_opened()) {
    if (fragment_idx_ >= fragments.count()) {
      ret = OB_ITER_END;
    } else {
      const ObDirectLoadMultipleSSTableFragment &fragment = fragments.at(fragment_idx_);
      if (fragment.rowkey_file_size_ > 0 &&
          OB_FAIL(data_block_reader_.open(fragment.rowkey_file_handle_, 0, fragment.rowkey_file_size_))) {
        LOG_WARN("fail to open fragment", KR(ret), K(fragment));
      } else {
        ++fragment_idx_;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadSSTableRowkeyScanner
 */

ObDirectLoadSSTableRowkeyScanner::ObDirectLoadSSTableRowkeyScanner()
  : sstable_(nullptr), is_inited_(false)
{
}

ObDirectLoadSSTableRowkeyScanner::~ObDirectLoadSSTableRowkeyScanner()
{
}

int ObDirectLoadSSTableRowkeyScanner::init(ObDirectLoadMultipleSSTable *sstable,
                                           const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadSSTableRowkeyScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid() ||
                         !sstable->get_tablet_id().is_valid() || !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable), K(table_data_desc));
  } else {
    if (OB_FAIL(scanner_.init(sstable, table_data_desc))) {
      LOG_WARN("fail to data block reader", KR(ret));
    } else {
      sstable_ = sstable;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadSSTableRowkeyScanner::get_next_rowkey(const ObDatumRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableRowkeyScanner not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadMultipleDatumRowkey *multiple_rowkey = nullptr;
    if (OB_FAIL(scanner_.get_next_rowkey(multiple_rowkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else if (OB_UNLIKELY(multiple_rowkey->tablet_id_ != sstable_->get_tablet_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected multiple rowkey", KR(ret), KPC(sstable_), KPC(multiple_rowkey));
    } else if (OB_FAIL(multiple_rowkey->get_rowkey(rowkey_))) {
      LOG_WARN("fail to get rowkey", KR(ret));
    } else {
      rowkey = &rowkey_;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

