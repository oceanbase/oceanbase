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

#include "storage/direct_load/ob_direct_load_external_table_builder.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "storage/direct_load/ob_direct_load_external_table.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadExternalTableBuildParam
 */

ObDirectLoadExternalTableBuildParam::ObDirectLoadExternalTableBuildParam()
  : datum_utils_(nullptr), file_mgr_(nullptr), extra_buf_(nullptr), extra_buf_size_(0)
{
}

ObDirectLoadExternalTableBuildParam::~ObDirectLoadExternalTableBuildParam()
{
}

bool ObDirectLoadExternalTableBuildParam::is_valid() const
{
  return tablet_id_.is_valid() && table_data_desc_.is_valid() && nullptr != datum_utils_ &&
         nullptr != file_mgr_ && nullptr != extra_buf_ && extra_buf_size_ > 0 &&
         extra_buf_size_ % DIO_ALIGN_SIZE == 0;
}

/**
 * ObDirectLoadExternalTableBuilder
 */

ObDirectLoadExternalTableBuilder::ObDirectLoadExternalTableBuilder()
  : row_count_(0), is_closed_(false), is_inited_(false)
{
}

ObDirectLoadExternalTableBuilder::~ObDirectLoadExternalTableBuilder()
{
}

int ObDirectLoadExternalTableBuilder::init(const ObDirectLoadExternalTableBuildParam &build_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadExternalTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!build_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(build_param));
  } else {
    build_param_ = build_param;
    int64_t dir_id = -1;
    if (OB_FAIL(build_param_.file_mgr_->alloc_dir(dir_id))) {
      LOG_WARN("fail to alloc dir", KR(ret));
    } else if (OB_FAIL(build_param_.file_mgr_->alloc_file(dir_id, file_handle_))) {
      LOG_WARN("fail to alloc fragment", KR(ret));
    } else if (OB_FAIL(
                 external_writer_.init(build_param_.table_data_desc_.external_data_block_size_,
                                       build_param_.table_data_desc_.compressor_type_,
                                       build_param_.extra_buf_, build_param_.extra_buf_size_))) {
      LOG_WARN("fail to init external writer", KR(ret));
    } else if (OB_FAIL(external_writer_.open(file_handle_))) {
      LOG_WARN("fail to open file", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadExternalTableBuilder::append_row(const ObTabletID &tablet_id,
                                                 const table::ObTableLoadSequenceNo &seq_no,
                                                 const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load external table is closed", KR(ret));
  } else if (OB_UNLIKELY(tablet_id != build_param_.tablet_id_ || !datum_row.is_valid() ||
                         datum_row.get_column_count() !=
                           build_param_.table_data_desc_.column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(build_param_), K(tablet_id), K(datum_row));
  } else {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_append_row_time_us);
    if (OB_FAIL(external_row_.from_datums(datum_row.storage_datums_, datum_row.count_,
                                          build_param_.table_data_desc_.rowkey_column_num_, seq_no))) {
      LOG_WARN("fail to from datums", KR(ret));
    } else if (OB_FAIL(external_writer_.write_item(external_row_))) {
      LOG_WARN("fail to write item", KR(ret));
    } else {
      ++row_count_;
    }
  }
  return ret;
}

int ObDirectLoadExternalTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load external table is closed", KR(ret));
  } else {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_append_row_time_us);
    if (OB_FAIL(external_writer_.close())) {
      LOG_WARN("fail to close external writer", KR(ret));
    } else {
      is_closed_ = true;
    }
  }
  return ret;
}

int ObDirectLoadExternalTableBuilder::get_tables(
  ObIArray<ObIDirectLoadPartitionTable *> &table_array, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load external table not closed", KR(ret));
  } else if (row_count_ == 0) {
    // do nothing
  } else {
    ObDirectLoadExternalTableCreateParam create_param;
    create_param.tablet_id_ = build_param_.tablet_id_;
    create_param.data_block_size_ = build_param_.table_data_desc_.external_data_block_size_;
    create_param.row_count_ = row_count_;
    create_param.max_data_block_size_ = external_writer_.get_max_block_size();
    ObDirectLoadExternalFragment fragment;
    fragment.file_size_ = external_writer_.get_file_size();
    fragment.row_count_ = row_count_;
    fragment.max_data_block_size_ = external_writer_.get_max_block_size();
    if (OB_FAIL(fragment.file_handle_.assign(file_handle_))) {
      LOG_WARN("fail to assign file handle", KR(ret));
    } else if (OB_FAIL(create_param.fragments_.push_back(fragment))) {
      LOG_WARN("fail to push back fragment", KR(ret));
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadExternalTable *external_table = nullptr;
      if (OB_ISNULL(external_table = OB_NEWx(ObDirectLoadExternalTable, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadExternalTable", KR(ret));
      } else if (OB_FAIL(external_table->init(create_param))) {
        LOG_WARN("fail to init external table", KR(ret));
      } else if (OB_FAIL(table_array.push_back(external_table))) {
        LOG_WARN("fail to push back external table", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != external_table) {
          external_table->~ObDirectLoadExternalTable();
          allocator.free(external_table);
          external_table = nullptr;
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
