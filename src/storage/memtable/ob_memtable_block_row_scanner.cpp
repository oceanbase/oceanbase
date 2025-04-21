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

#include "ob_memtable_block_row_scanner.h"
#include "ob_memtable_block_reader.h"
#include "storage/access/ob_vector_store.h"

namespace oceanbase {
using namespace storage;
using namespace blocksstable;

namespace memtable {
int ObMemtableBlockRowScanner::init(const storage::ObTableIterParam &param,
                                    storage::ObTableAccessContext &context,
                                    ObMemtableSingleRowReader &single_row_reader)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() ||
                         nullptr != memtable_reader_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(context), KP_(memtable_reader));
  } else if (FALSE_IT(read_info_ = single_row_reader.get_read_info())) {
  } else if (OB_UNLIKELY(nullptr == read_info_ || !read_info_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read info", K(ret), KPC_(read_info), K(single_row_reader));
  } else if (OB_ISNULL(memtable_reader_ =
                       OB_NEWx(ObMemtableBlockReader, &allocator_, allocator_, single_row_reader))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memtable block reader failed", KR(ret));
  } else if (OB_FAIL(memtable_reader_->init(param.is_delete_insert_))) {
    LOG_WARN("init memtable block reader failed", KR(ret));
  } else if (OB_FAIL(row_.init(allocator_, param.get_buffered_out_col_cnt()))) {
    LOG_WARN("Failed to init datum row", K(ret));
  } else {
    param_ = &param;
    context_ = &context;
    reader_ = memtable_reader_;
    block_row_store_ = context.block_row_store_;
    range_ = nullptr;
    is_inited_ = true;
  }
  return ret;
}

int ObMemtableBlockRowScanner::prefetch()
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_FAIL(memtable_reader_->prefetch_rows())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to read rows", K(ret));
    }
  } else if (OB_UNLIKELY(0 >= memtable_reader_->get_row_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected block reader count", K(ret), KPC_(memtable_reader));
  } else {
    // memtable already reads rows in reverse order if needed
    step_ = 1;
    start_ = 0;
    current_ = start_;
    last_ = memtable_reader_->get_row_count() - 1;
    can_ignore_multi_version_ = memtable_reader_->is_single_version_rows();
    use_private_bitmap_ = param_->is_delete_insert_ && nullptr != param_->pushdown_filter_;
    if (use_private_bitmap_) {
      if (OB_FAIL(init_bitmap())) {
        LOG_WARN("Failed to init bitmap", K(ret));
      } else if (OB_FAIL(apply_filter(false))) {
        LOG_WARN("Fail to apply filter", K(ret));
      }
    }
  }

  return ret;
}

int ObMemtableBlockRowScanner::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  while(OB_SUCC(ret) && nullptr == row) {
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to judge end of block or not", K(ret));
      }
    } else if (OB_FAIL(fetch_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to fetch row", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      if (OB_FAIL(prefetch())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to prefetch", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMemtableBlockRowScanner::fetch_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObFilterResult res;
  if (OB_FAIL(get_filter_result(res))) {
    LOG_WARN("Failed to get pushdown filter result bitmap", K(ret));
  } else {
    bool readed = false;
    while (OB_SUCC(ret) && !readed) {
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to judge end of block or not", K(ret));
        }
      } else if (OB_FAIL(memtable_reader_->get_next_di_row(res, current_, row_))) {
        LOG_WARN("Fail to get next di row", K(ret), K_(current));
      } else {
        readed = true;
        row_.fast_filter_skipped_ = is_filter_applied_;
        row = &row_;
      }
    }
  }
  return ret;
}

int ObMemtableBlockRowScanner::get_next_rows()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || nullptr == block_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The memtable block row scanner has not been inited", K(ret), K_(is_inited), KP_(block_row_store));
  } else {
    while(OB_SUCC(ret) && !block_row_store_->is_end()) {
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to judge end of block or not", K(ret));
        }
      } else if (OB_FAIL(ObIMicroBlockRowScanner::get_next_rows())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next rows", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        if (OB_FAIL(prefetch())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to prefetch", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

}  // end of namespace memtable
}  // end of namespace oceanbase
