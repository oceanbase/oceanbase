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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_scanner.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadMultipleHeapTableTabletWholeScanner::ObDirectLoadMultipleHeapTableTabletWholeScanner()
  : heap_table_(nullptr), is_iter_end_(false), is_inited_(false)
{
}

ObDirectLoadMultipleHeapTableTabletWholeScanner::~ObDirectLoadMultipleHeapTableTabletWholeScanner()
{
}

int ObDirectLoadMultipleHeapTableTabletWholeScanner::init(
  ObDirectLoadMultipleHeapTable *heap_table,
  const ObTabletID &tablet_id,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleHeapTableTabletWholeScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == heap_table || !heap_table->is_valid() ||
                         !tablet_id.is_valid() || !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(heap_table), K(tablet_id), K(table_data_desc));
  } else {
    heap_table_ = heap_table;
    tablet_id_ = tablet_id;
    if (OB_FAIL(index_scanner_.init(heap_table, tablet_id, table_data_desc))) {
      LOG_WARN("fail to init index scanner", KR(ret));
    } else if (OB_FAIL(data_block_reader_.init(table_data_desc.sstable_data_block_size_,
                                               heap_table->get_meta().max_data_block_size_,
                                               table_data_desc.compressor_type_))) {
      LOG_WARN("fail to init data block reader", KR(ret));
    } else if (OB_FAIL(switch_next_fragment())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to switch next fragment", KR(ret));
      } else {
        ret = OB_SUCCESS;
        is_iter_end_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableTabletWholeScanner::switch_next_fragment()
{
  int ret = OB_SUCCESS;
  const ObDirectLoadMultipleHeapTableTabletIndex *tablet_index = nullptr;
  if (OB_FAIL(index_scanner_.get_next_index(tablet_index))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next index", KR(ret));
    }
  } else {
    const ObDirectLoadMultipleHeapTableDataFragment &data_fragment =
      heap_table_->get_data_fragments().at(tablet_index->fragment_idx_);
    data_block_reader_.reuse();
    if (OB_FAIL(data_block_reader_.open(data_fragment.file_handle_, tablet_index->offset_,
                                        data_fragment.file_size_ - tablet_index->offset_))) {
      LOG_WARN("fail to open file", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableTabletWholeScanner::get_next_row(const RowType *&external_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableTabletWholeScanner not init", KR(ret), KP(this));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    external_row = nullptr;
    while (OB_SUCC(ret) && nullptr == external_row) {
      // get next row from current fragment
      if (OB_FAIL(data_block_reader_.get_next_item(external_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next item", KR(ret));
        }
      } else {
        int cmp_ret = external_row->tablet_id_.compare(tablet_id_);
        if (cmp_ret < 0) {
          // ignore
          external_row = nullptr;
        } else if (cmp_ret > 0) {
          ret = OB_ITER_END;
          external_row = nullptr;
        }
      }
      // switch next fragment
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(switch_next_fragment())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to switch next fragment", KR(ret));
          } else {
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
