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
    rowkey_block_count_(0),
    row_count_(0),
    rowkey_count_(0),
    max_data_block_size_(0),
    is_inited_(false)
{
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
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMultipleSSTableCompactor::add_table(const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableCompactor not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_multiple_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_handle));
  } else {
    int cmp_ret = 0;
    ObDirectLoadMultipleSSTable *sstable = static_cast<ObDirectLoadMultipleSSTable *>(table_handle.get_table());
    if (OB_FAIL(check_table_compactable(sstable))) {
      LOG_WARN("fail to check table compactable", KR(ret), KPC(sstable));
    } else if (!sstable->is_empty()) {
      const ObDirectLoadMultipleSSTableMeta &table_meta = sstable->get_meta();
      index_block_count_ += table_meta.index_block_count_;
      data_block_count_ += table_meta.data_block_count_;
      rowkey_block_count_ += table_meta.rowkey_block_count_;
      row_count_ += table_meta.row_count_;
      rowkey_count_ += table_meta.rowkey_count_;
      max_data_block_size_ = MAX(max_data_block_size_, table_meta.max_data_block_size_);
      for (int64_t i = 0; OB_SUCC(ret) && i < sstable->get_fragments().count(); ++i) {
        if (OB_FAIL(fragments_.push_back(sstable->get_fragments().at(i)))) {
          LOG_WARN("fail to push back table fragment", KR(ret), K(i));
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
          table_meta.rowkey_column_num_ != param_.table_data_desc_.rowkey_column_num_ ||
          table_meta.column_count_ != param_.table_data_desc_.column_count_ ||
          table_meta.index_block_size_ != param_.table_data_desc_.sstable_index_block_size_ ||
          table_meta.data_block_size_ != param_.table_data_desc_.sstable_data_block_size_ ||
          table_meta.rowkey_block_size_ != param_.table_data_desc_.sstable_data_block_size_)) {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("table meta not match", KR(ret), K(param_), K(table_meta));
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

int ObDirectLoadMultipleSSTableCompactor::get_table(
  ObDirectLoadTableHandle &table_handle, ObDirectLoadTableManager *table_manager)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableCompactor not init", KR(ret), KP(this));
  } else {
    ObDirectLoadTableHandle sstable_handle;
    ObDirectLoadMultipleSSTable *sstable = nullptr;
    ObDirectLoadMultipleSSTableCreateParam create_param;
    create_param.tablet_id_ = param_.tablet_id_;
    create_param.rowkey_column_num_ = param_.table_data_desc_.rowkey_column_num_;
    create_param.column_count_ = param_.table_data_desc_.column_count_;
    create_param.index_block_size_ = param_.table_data_desc_.sstable_index_block_size_;
    create_param.data_block_size_ = param_.table_data_desc_.sstable_data_block_size_;
    create_param.rowkey_block_size_ = param_.table_data_desc_.sstable_data_block_size_;
    create_param.index_block_count_ = index_block_count_;
    create_param.data_block_count_ = data_block_count_;
    create_param.rowkey_block_count_ = rowkey_block_count_;
    create_param.row_count_ = row_count_;
    create_param.rowkey_count_ = rowkey_count_;
    create_param.max_data_block_size_ = max_data_block_size_;
    create_param.compressor_type_ = param_.table_data_desc_.compressor_type_;
    if (OB_FAIL(create_param.fragments_.assign(fragments_))) {
      LOG_WARN("fail to assign fragments", KR(ret));
    } else if (OB_FAIL(table_manager->alloc_multiple_sstable(sstable_handle))) {
      LOG_WARN("fail to alloc multiple sstable", KR(ret));
    } else if (FALSE_IT(sstable = static_cast<ObDirectLoadMultipleSSTable *>(sstable_handle.get_table()))) {
    } else if (OB_FAIL(sstable->init(create_param))) {
      LOG_WARN("fail to init sstable table", KR(ret));
    } else if (FALSE_IT(table_handle = sstable_handle)) {
    }
  }
  return ret;
}

void ObDirectLoadMultipleSSTableCompactor::stop()
{
}

} // namespace storage
} // namespace oceanbase
