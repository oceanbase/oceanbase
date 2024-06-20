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

#include "storage/direct_load/ob_direct_load_multiple_sstable_compactor.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadMultipleSSTableCompactParam
 */

ObDirectLoadMultipleSSTableCompactParam::ObDirectLoadMultipleSSTableCompactParam()
  : datum_utils_(nullptr)
{
}

ObDirectLoadMultipleSSTableCompactParam::~ObDirectLoadMultipleSSTableCompactParam()
{
}

bool ObDirectLoadMultipleSSTableCompactParam::is_valid() const
{
  return table_data_desc_.is_valid() && nullptr != datum_utils_;
}

/**
 * ObDirectLoadMultipleSSTableCompactor
 */

ObDirectLoadMultipleSSTableCompactor::ObDirectLoadMultipleSSTableCompactor()
  : index_block_count_(0),
    data_block_count_(0),
    row_count_(0),
    max_data_block_size_(0),
    start_key_allocator_("TLD_SRowkey"),
    end_key_allocator_("TLD_ERowkey"),
    is_inited_(false)
{
  start_key_allocator_.set_tenant_id(MTL_ID());
  end_key_allocator_.set_tenant_id(MTL_ID());
  fragments_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleSSTableCompactor::~ObDirectLoadMultipleSSTableCompactor()
{
}

int ObDirectLoadMultipleSSTableCompactor::init(const ObDirectLoadMultipleSSTableCompactParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableCompactor init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    param_ = param;
    start_key_.set_min_rowkey();
    end_key_.set_min_rowkey();
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMultipleSSTableCompactor::add_table(ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableCompactor not init", KR(ret), KP(this));
  } else {
    int cmp_ret = 0;
    ObDirectLoadMultipleSSTable *sstable = dynamic_cast<ObDirectLoadMultipleSSTable *>(table);
    if (OB_ISNULL(sstable)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), KP(sstable));
    } else if (OB_FAIL(check_table_compactable(sstable))) {
      LOG_WARN("fail to check table compactable", KR(ret), KPC(sstable));
    } else if (!sstable->is_empty()) {
      const ObDirectLoadMultipleSSTableMeta &table_meta = sstable->get_meta();
      index_block_count_ += table_meta.index_block_count_;
      data_block_count_ += table_meta.data_block_count_;
      row_count_ += table_meta.row_count_;
      max_data_block_size_ = MAX(max_data_block_size_, table_meta.max_data_block_size_);
      for (int64_t i = 0; OB_SUCC(ret) && i < sstable->get_fragments().count(); ++i) {
        if (OB_FAIL(fragments_.push_back(sstable->get_fragments().at(i)))) {
          LOG_WARN("fail to push back table fragment", KR(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        end_key_allocator_.reuse();
        if (start_key_.is_min_rowkey() &&
            OB_FAIL(start_key_.deep_copy(sstable->get_start_key(), start_key_allocator_))) {
          LOG_WARN("fail to deep copy start key", KR(ret));
        } else if (OB_FAIL(end_key_.deep_copy(sstable->get_end_key(), end_key_allocator_))) {
          LOG_WARN("fail to deep copy end key", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableCompactor::check_table_compactable(
  ObDirectLoadMultipleSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable));
  } else {
    const ObDirectLoadMultipleSSTableMeta &table_meta = sstable->get_meta();
    if (OB_UNLIKELY(
          sstable->get_tablet_id() != param_.tablet_id_ ||
          table_meta.rowkey_column_num_ != param_.table_data_desc_.rowkey_column_num_ ||
          table_meta.column_count_ != param_.table_data_desc_.column_count_ ||
          table_meta.index_block_size_ != param_.table_data_desc_.sstable_index_block_size_ ||
          table_meta.data_block_size_ != param_.table_data_desc_.sstable_data_block_size_)) {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("table meta not match", KR(ret), K(param_), K(table_meta));
    } else if (!sstable->is_empty()) {
      int cmp_ret = 0;
      if (OB_FAIL(end_key_.compare(sstable->get_start_key(), *param_.datum_utils_, cmp_ret))) {
        LOG_WARN("fail to compare rowkey", KR(ret));
      } else if (cmp_ret >= 0) {
        ret = OB_ROWKEY_ORDER_ERROR;
        LOG_WARN("sstable is not contiguous", KR(ret), K(end_key_), K(sstable->get_start_key()));
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableCompactor::compact()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableCompactor not init", KR(ret), KP(this));
  } else {
    // do nothing
  }
  return ret;
}

int ObDirectLoadMultipleSSTableCompactor::get_table(ObIDirectLoadPartitionTable *&table,
                                                    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableCompactor not init", KR(ret), KP(this));
  } else {
    ObDirectLoadMultipleSSTable *sstable = nullptr;
    ObDirectLoadMultipleSSTableCreateParam create_param;
    create_param.tablet_id_ = param_.tablet_id_;
    create_param.rowkey_column_num_ = param_.table_data_desc_.rowkey_column_num_;
    create_param.column_count_ = param_.table_data_desc_.column_count_;
    create_param.index_block_size_ = param_.table_data_desc_.sstable_index_block_size_;
    create_param.data_block_size_ = param_.table_data_desc_.sstable_data_block_size_;
    create_param.index_block_count_ = index_block_count_;
    create_param.data_block_count_ = data_block_count_;
    create_param.row_count_ = row_count_;
    create_param.max_data_block_size_ = max_data_block_size_;
    create_param.start_key_ = start_key_;
    create_param.end_key_ = end_key_;
    if (OB_FAIL(create_param.fragments_.assign(fragments_))) {
      LOG_WARN("fail to assign fragments", KR(ret));
    } else if (OB_ISNULL(sstable = OB_NEWx(ObDirectLoadMultipleSSTable, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTable", KR(ret));
    } else if (OB_FAIL(sstable->init(create_param))) {
      LOG_WARN("fail to init sstable table", KR(ret));
    } else {
      table = sstable;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != sstable) {
        sstable->~ObDirectLoadMultipleSSTable();
        allocator.free(sstable);
        sstable = nullptr;
      }
    }
  }
  return ret;
}

void ObDirectLoadMultipleSSTableCompactor::stop()
{
}

} // namespace storage
} // namespace oceanbase
