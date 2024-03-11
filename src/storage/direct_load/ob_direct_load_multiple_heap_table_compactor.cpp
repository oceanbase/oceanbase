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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_compactor.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block_writer.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scan_merge.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scanner.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadMultipleHeapTableCompactParam
 */

ObDirectLoadMultipleHeapTableCompactParam::ObDirectLoadMultipleHeapTableCompactParam()
  : file_mgr_(nullptr), index_dir_id_(-1)
{
}

ObDirectLoadMultipleHeapTableCompactParam::~ObDirectLoadMultipleHeapTableCompactParam()
{
}

void ObDirectLoadMultipleHeapTableCompactParam::reset()
{
  table_data_desc_.reset();
  file_mgr_ = nullptr;
  index_dir_id_ = -1;
}

bool ObDirectLoadMultipleHeapTableCompactParam::is_valid() const
{
  return table_data_desc_.is_valid() && nullptr != file_mgr_ && index_dir_id_ > 0;
}

/**
 * ObDirectLoadMultipleHeapTableCompactor
 */

ObDirectLoadMultipleHeapTableCompactor::ObDirectLoadMultipleHeapTableCompactor()
  : allocator_("TLD_MHT_Comp"),
    index_block_count_(0),
    data_block_count_(0),
    index_file_size_(0),
    data_file_size_(0),
    index_entry_count_(0),
    row_count_(0),
    max_data_block_size_(0),
    is_stop_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  index_scanners_.set_tenant_id(MTL_ID());
  base_data_fragment_idxs_.set_tenant_id(MTL_ID());
  data_fragments_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleHeapTableCompactor::~ObDirectLoadMultipleHeapTableCompactor()
{
  reset();
}

void ObDirectLoadMultipleHeapTableCompactor::reset()
{
  param_.reset();
  index_block_count_ = 0;
  data_block_count_ = 0;
  index_file_size_ = 0;
  data_file_size_ = 0;
  index_entry_count_ = 0;
  row_count_ = 0;
  max_data_block_size_ = 0;
  for (int64_t i = 0; i < index_scanners_.count(); ++i) {
    ObIDirectLoadMultipleHeapTableIndexScanner *scanner = index_scanners_.at(i);
    scanner->~ObIDirectLoadMultipleHeapTableIndexScanner();
    allocator_.free(scanner);
  }
  index_scanners_.reset();
  base_data_fragment_idxs_.reset();
  data_fragments_.reset();
  compacted_index_file_handle_.reset();
  allocator_.reset();
  is_inited_ = false;
}

void ObDirectLoadMultipleHeapTableCompactor::reuse()
{
  index_block_count_ = 0;
  data_block_count_ = 0;
  index_file_size_ = 0;
  data_file_size_ = 0;
  index_entry_count_ = 0;
  row_count_ = 0;
  max_data_block_size_ = 0;
  for (int64_t i = 0; i < index_scanners_.count(); ++i) {
    ObIDirectLoadMultipleHeapTableIndexScanner *scanner = index_scanners_.at(i);
    scanner->~ObIDirectLoadMultipleHeapTableIndexScanner();
    allocator_.free(scanner);
  }
  index_scanners_.reset();
  base_data_fragment_idxs_.reset();
  data_fragments_.reset();
  compacted_index_file_handle_.reset();
  allocator_.reuse();
}

int ObDirectLoadMultipleHeapTableCompactor::init(
  const ObDirectLoadMultipleHeapTableCompactParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleHeapTableCompactor init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    param_ = param;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableCompactor::add_table(ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableCompactor not init", KR(ret), KP(this));
  } else {
    int cmp_ret = 0;
    ObDirectLoadMultipleHeapTable *heap_table =
      dynamic_cast<ObDirectLoadMultipleHeapTable *>(table);
    if (OB_ISNULL(heap_table)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), KPC(table));
    } else if (OB_FAIL(check_table_compactable(heap_table))) {
      LOG_WARN("fail to check table compactable", KR(ret), KPC(heap_table));
    } else if (OB_FAIL(construct_index_scanner(heap_table))) {
      LOG_WARN("fail to construct index scanner", KR(ret), KPC(heap_table));
    } else {
      const ObDirectLoadMultipleHeapTableMeta &table_meta = heap_table->get_meta();
      const ObIArray<ObDirectLoadMultipleHeapTableDataFragment> &data_fragments =
        heap_table->get_data_fragments();
      data_block_count_ += table_meta.data_block_count_;
      data_file_size_ += table_meta.data_file_size_;
      index_entry_count_ += table_meta.index_entry_count_;
      row_count_ += table_meta.row_count_;
      max_data_block_size_ = MAX(max_data_block_size_, table_meta.max_data_block_size_);
      if (OB_FAIL(base_data_fragment_idxs_.push_back(data_fragments_.count()))) {
        LOG_WARN("fail to push back index fragment", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < data_fragments.count(); ++i) {
        if (OB_FAIL(data_fragments_.push_back(data_fragments.at(i)))) {
          LOG_WARN("fail to push back data fragment", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableCompactor::check_table_compactable(
  ObDirectLoadMultipleHeapTable *heap_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == heap_table || !heap_table->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(heap_table));
  } else {
    const ObDirectLoadMultipleHeapTableMeta &table_meta = heap_table->get_meta();
    if (OB_UNLIKELY(
          table_meta.column_count_ != param_.table_data_desc_.column_count_ ||
          table_meta.index_block_size_ != param_.table_data_desc_.sstable_index_block_size_ ||
          table_meta.data_block_size_ != param_.table_data_desc_.sstable_data_block_size_)) {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("table meta not match", KR(ret), K(param_), K(table_meta));
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableCompactor::construct_index_scanner(
  ObDirectLoadMultipleHeapTable *heap_table)
{
  int ret = OB_SUCCESS;
  ObDirectLoadMultipleHeapTableIndexWholeScanner *index_whole_scanner = nullptr;
  if (OB_ISNULL(index_whole_scanner =
                  OB_NEWx(ObDirectLoadMultipleHeapTableIndexWholeScanner, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadMultipleHeapTableIndexWholeScanner", KR(ret));
  } else if (OB_FAIL(index_whole_scanner->init(heap_table->get_index_file_handle(),
                                               heap_table->get_meta().index_file_size_,
                                               param_.table_data_desc_))) {
    LOG_WARN("fail to init index whole scanner", KR(ret));
  } else if (OB_FAIL(index_scanners_.push_back(index_whole_scanner))) {
    LOG_WARN("fail to push back", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != index_whole_scanner) {
      index_whole_scanner->~ObDirectLoadMultipleHeapTableIndexWholeScanner();
      allocator_.free(index_whole_scanner);
      index_whole_scanner = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableCompactor::compact()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableCompactor not init", KR(ret), KP(this));
  } else {
    ObTabletID last_tablet_id;
    int64_t idx = -1;
    const ObDirectLoadMultipleHeapTableTabletIndex *tablet_index = nullptr;
    ObDirectLoadMultipleHeapTableTabletIndex compacted_tablet_index;
    ObDirectLoadMultipleHeapTableIndexScanMerge scan_merge;
    ObDirectLoadMultipleHeapTableIndexBlockWriter index_block_writer;
    int64_t index_entry_count = 0;
    if (OB_FAIL(param_.file_mgr_->alloc_file(param_.index_dir_id_, compacted_index_file_handle_))) {
      LOG_WARN("fail to alloc file", KR(ret));
    } else if (OB_FAIL(index_block_writer.init(param_.table_data_desc_.sstable_index_block_size_,
                                               param_.table_data_desc_.compressor_type_))) {
      LOG_WARN("fail to init index block writer", KR(ret));
    } else if (OB_FAIL(index_block_writer.open(compacted_index_file_handle_))) {
      LOG_WARN("fail to open file", KR(ret));
    } else if (OB_FAIL(scan_merge.init(index_scanners_))) {
      LOG_WARN("fail to init scan merge", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_UNLIKELY(is_stop_)) {
        ret = OB_CANCELED;
        LOG_WARN("compact canceled", KR(ret));
      } else if (OB_FAIL(scan_merge.get_next_index(idx, tablet_index))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next index", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      }
      // check tablet id order
      else if (last_tablet_id != tablet_index->tablet_id_) {
        if (last_tablet_id.is_valid() &&
            OB_UNLIKELY(last_tablet_id.compare(tablet_index->tablet_id_) > 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected tablet index", KR(ret), K(last_tablet_id), KPC(tablet_index));
        } else {
          last_tablet_id = tablet_index->tablet_id_;
        }
      }
      // append index
      if (OB_SUCC(ret)) {
        const int64_t base_data_fragment_idx = base_data_fragment_idxs_.at(idx);
        compacted_tablet_index.tablet_id_ = tablet_index->tablet_id_;
        compacted_tablet_index.row_count_ = tablet_index->row_count_;
        compacted_tablet_index.fragment_idx_ = base_data_fragment_idx + tablet_index->fragment_idx_;
        compacted_tablet_index.offset_ = tablet_index->offset_;
        if (OB_FAIL(index_block_writer.append_index(compacted_tablet_index))) {
          LOG_WARN("fail to append index", KR(ret));
        } else {
          ++index_entry_count;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(index_block_writer.close())) {
        LOG_WARN("fail to close index block writer", KR(ret));
      } else if (OB_UNLIKELY(index_entry_count != index_entry_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index entry count", KR(ret), K(index_entry_count_),
                 K(index_entry_count));
      } else {
        index_block_count_ = index_block_writer.get_block_count();
        index_file_size_ = index_block_writer.get_file_size();
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableCompactor::get_table(ObIDirectLoadPartitionTable *&table,
                                                      ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableCompactor not init", KR(ret), KP(this));
  } else {
    ObDirectLoadMultipleHeapTableCreateParam create_param;
    create_param.column_count_ = param_.table_data_desc_.column_count_;
    create_param.index_block_size_ = param_.table_data_desc_.sstable_index_block_size_;
    create_param.data_block_size_ = param_.table_data_desc_.sstable_data_block_size_;
    create_param.index_block_count_ = index_block_count_;
    create_param.data_block_count_ = data_block_count_;
    create_param.index_file_size_ = index_file_size_;
    create_param.data_file_size_ = data_file_size_;
    create_param.index_entry_count_ = index_entry_count_;
    create_param.row_count_ = row_count_;
    create_param.max_data_block_size_ = max_data_block_size_;
    if (OB_FAIL(create_param.index_file_handle_.assign(compacted_index_file_handle_))) {
      LOG_WARN("fail to assign file handle", KR(ret));
    } else if (OB_FAIL(create_param.data_fragments_.assign(data_fragments_))) {
      LOG_WARN("fail to assign data fragments", KR(ret));
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadMultipleHeapTable *heap_table = nullptr;
      if (OB_ISNULL(heap_table = OB_NEWx(ObDirectLoadMultipleHeapTable, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadMultipleHeapTable", KR(ret));
      } else if (OB_FAIL(heap_table->init(create_param))) {
        LOG_WARN("fail to init multiple heap table table", KR(ret));
      } else {
        table = heap_table;
      }
      if (OB_FAIL(ret)) {
        if (nullptr != heap_table) {
          heap_table->~ObDirectLoadMultipleHeapTable();
          allocator.free(heap_table);
          heap_table = nullptr;
        }
      }
    }
  }
  return ret;
}

void ObDirectLoadMultipleHeapTableCompactor::stop()
{
  is_stop_ = true;
}

} // namespace storage
} // namespace oceanbase
