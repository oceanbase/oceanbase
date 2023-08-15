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

#include "storage/direct_load/ob_direct_load_external_table_compactor.h"
#include "storage/direct_load/ob_direct_load_external_table.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadExternalTableCompactParam
 */

ObDirectLoadExternalTableCompactParam::ObDirectLoadExternalTableCompactParam()
{
}

ObDirectLoadExternalTableCompactParam::~ObDirectLoadExternalTableCompactParam()
{
}

bool ObDirectLoadExternalTableCompactParam::is_valid() const
{
  return tablet_id_.is_valid() && table_data_desc_.is_valid() ;
}

/**
 * ObDirectLoadExternalTableCompactor
 */

ObDirectLoadExternalTableCompactor::ObDirectLoadExternalTableCompactor()
  : row_count_(0), max_data_block_size_(0), is_inited_(false)
{
}

ObDirectLoadExternalTableCompactor::~ObDirectLoadExternalTableCompactor()
{
}

int ObDirectLoadExternalTableCompactor::init(const ObDirectLoadExternalTableCompactParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadExternalTableCompactor init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    param_ = param;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadExternalTableCompactor::add_table(ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalTableCompactor not init", KR(ret), KP(this));
  } else {
    ObDirectLoadExternalTable *external_table = dynamic_cast<ObDirectLoadExternalTable *>(table);
    if (OB_ISNULL(external_table)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), KPC(table), KP(external_table));
    } else if (OB_FAIL(check_table_compactable(external_table))) {
      LOG_WARN("fail to check table compactable", KR(ret), KPC(external_table));
    } else {
      row_count_ += external_table->get_meta().row_count_;
      max_data_block_size_ = MAX(max_data_block_size_, external_table->get_meta().max_data_block_size_);
      if (OB_FAIL(fragments_.push_back(external_table->get_fragments()))) {
        LOG_WARN("fail to push back fragments", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadExternalTableCompactor::check_table_compactable(
  ObDirectLoadExternalTable *external_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == external_table || !external_table->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(external_table));
  } else {
    const ObDirectLoadExternalTableMeta &table_meta = external_table->get_meta();
    if (OB_UNLIKELY(table_meta.tablet_id_ != param_.tablet_id_ ||
                    table_meta.data_block_size_ != param_.table_data_desc_.external_data_block_size_ ||
                    table_meta.row_count_ <= 0)) {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("table meta not match", KR(ret), K(param_), K(table_meta));
    }
  }
  return ret;
}

int ObDirectLoadExternalTableCompactor::compact()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalTableCompactor not init", KR(ret), KP(this));
  } else {
    // do nothing
  }
  return ret;
}

int ObDirectLoadExternalTableCompactor::get_table(ObIDirectLoadPartitionTable *&table,
                                                  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalTableCompactor not init", KR(ret), KP(this));
  } else {
    ObDirectLoadExternalTable *external_table = nullptr;
    ObDirectLoadExternalTableCreateParam create_param;
    create_param.tablet_id_ = param_.tablet_id_;
    create_param.data_block_size_ = param_.table_data_desc_.external_data_block_size_;
    create_param.row_count_ = row_count_;
    create_param.max_data_block_size_ = max_data_block_size_;
    if (OB_FAIL(create_param.fragments_.assign(fragments_))) {
      LOG_WARN("fail to assign fragments", KR(ret));
    } else if (OB_ISNULL(external_table = OB_NEWx(ObDirectLoadExternalTable, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadExternalTable", KR(ret));
    } else if (OB_FAIL(external_table->init(create_param))) {
      LOG_WARN("fail to init external table", KR(ret));
    } else {
      table = external_table;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != external_table) {
        external_table->~ObDirectLoadExternalTable();
        allocator.free(external_table);
        external_table = nullptr;
      }
    }
  }
  return ret;
}

void ObDirectLoadExternalTableCompactor::stop()
{
}

} // namespace storage
} // namespace oceanbase
