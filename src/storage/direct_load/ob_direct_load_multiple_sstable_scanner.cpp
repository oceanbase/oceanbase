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

#include "storage/direct_load/ob_direct_load_multiple_sstable_scanner.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

ObDirectLoadMultipleSSTableScanner::ObDirectLoadMultipleSSTableScanner()
  : allocator_("TLD_Scanner"),
    sstable_(nullptr),
    range_(nullptr),
    datum_utils_(nullptr),
    is_iter_start_(false),
    is_iter_end_(false),
    is_inited_(false)
{
}

ObDirectLoadMultipleSSTableScanner::~ObDirectLoadMultipleSSTableScanner()
{
}

int ObDirectLoadMultipleSSTableScanner::init(ObDirectLoadMultipleSSTable *sstable,
                                             const ObDirectLoadTableDataDesc &table_data_desc,
                                             const ObDirectLoadMultipleDatumRange &range,
                                             const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid() ||
                         !table_data_desc.is_valid() || !range.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable), K(table_data_desc), K(range), KP(datum_utils));
  } else {
    sstable_ = sstable;
    table_data_desc_ = table_data_desc;
    range_ = &range;
    datum_utils_ = datum_utils;
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL(data_block_scanner_.init(sstable, table_data_desc, range, datum_utils))) {
      LOG_WARN("fail to init data block scanner", KR(ret));
    } else if (OB_FAIL(data_block_reader_.init(table_data_desc.sstable_data_block_size_,
                                               sstable->get_meta().max_data_block_size_,
                                               table_data_desc.compressor_type_))) {
      LOG_WARN("fail to init data block reader", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableScanner::switch_next_fragment()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_block_scanner_.get_next_data_block(data_block_desc_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next data block", KR(ret));
    }
  } else {
    const ObDirectLoadMultipleSSTableFragment &fragment =
      sstable_->get_fragments().at(data_block_desc_.fragment_idx_);
    data_block_reader_.reuse();
    if (OB_FAIL(data_block_reader_.open(fragment.data_file_handle_, data_block_desc_.offset_,
                                        data_block_desc_.size_))) {
      LOG_WARN("fail to open data file", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableScanner::get_next_row(const ObDirectLoadMultipleDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableScanner not init", KR(ret), KP(this));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    datum_row = nullptr;
    while (OB_SUCC(ret) && nullptr == datum_row) {
      if (OB_FAIL(data_block_reader_.get_next_row(datum_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(switch_next_fragment())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to switch next fragment", KR(ret));
            } else {
              is_iter_end_ = true;
            }
          }
        }
      } else {
        if (!is_iter_start_) {
          int cmp_ret = 0;
          if (OB_FAIL(datum_row->rowkey_.compare(range_->start_key_, *datum_utils_, cmp_ret))) {
            LOG_WARN("fail to compare rowkey", KR(ret));
          } else if (cmp_ret < 0 || (cmp_ret == 0 && range_->is_left_open())) {
            datum_row = nullptr;
          } else {
            is_iter_start_ = true;
          }
        }
        if (OB_SUCC(ret) && nullptr != datum_row && data_block_desc_.is_right_border_ &&
            data_block_reader_.get_block_count() == data_block_desc_.block_count_) {
          int cmp_ret = 0;
          if (OB_FAIL(datum_row->rowkey_.compare(range_->end_key_, *datum_utils_, cmp_ret))) {
            LOG_WARN("fail to compare rowkey", KR(ret));
          } else if (cmp_ret > 0 || (cmp_ret == 0 && range_->is_right_open())) {
            datum_row = nullptr;
            ret = OB_ITER_END;
            is_iter_end_ = true;
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
