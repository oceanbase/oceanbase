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

#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadExternalMultiPartitionTableBuildParam
 */

ObDirectLoadExternalMultiPartitionTableBuildParam::
  ObDirectLoadExternalMultiPartitionTableBuildParam()
  : datum_utils_(nullptr), file_mgr_(nullptr), extra_buf_(nullptr), extra_buf_size_(0)
{
}

ObDirectLoadExternalMultiPartitionTableBuildParam::
  ~ObDirectLoadExternalMultiPartitionTableBuildParam()
{
}

bool ObDirectLoadExternalMultiPartitionTableBuildParam::is_valid() const
{
  return table_data_desc_.is_valid() && nullptr != datum_utils_ && nullptr != file_mgr_ &&
         nullptr != extra_buf_ && extra_buf_size_ > 0 && extra_buf_size_ % DIO_ALIGN_SIZE == 0;
}

/**
 * ObDirectLoadExternalMultiPartitionTableBuilder
 */

ObDirectLoadExternalMultiPartitionTableBuilder::ObDirectLoadExternalMultiPartitionTableBuilder()
  : allocator_("TLD_EMPTBuilder"),
    fragment_array_(),
    total_row_count_(0),
    fragment_row_count_(0),
    max_data_block_size_(0),
    is_closed_(false),
    is_inited_(false)
{
}

ObDirectLoadExternalMultiPartitionTableBuilder::~ObDirectLoadExternalMultiPartitionTableBuilder()
{
}

int ObDirectLoadExternalMultiPartitionTableBuilder::init(
  const ObDirectLoadExternalMultiPartitionTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadExternalMultiPartitionTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    param_ = param;
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL(alloc_tmp_file())) {
      LOG_WARN("fail to alloc tmp file", KR(ret));
    } else if (OB_FAIL(external_writer_.init(param_.table_data_desc_.external_data_block_size_,
                                             param_.table_data_desc_.compressor_type_,
                                             param_.extra_buf_, param_.extra_buf_size_))) {
      LOG_WARN("fail to init external writer", KR(ret));
    } else if (OB_FAIL(external_writer_.open(file_handle_))) {
      LOG_WARN("fail to open file", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadExternalMultiPartitionTableBuilder::append_row(const ObTabletID &tablet_id,
                                                               const table::ObTableLoadSequenceNo &seq_no,
                                                               const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalMultiPartitionTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load external multi partition table is closed", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() ||
                         datum_row.get_column_count() != param_.table_data_desc_.column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param_), K(datum_row));
  } else {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_append_row_time_us);
    row_.tablet_id_ = tablet_id;
    if (OB_FAIL(row_.external_row_.from_datums(datum_row.storage_datums_, datum_row.count_,
                                               param_.table_data_desc_.rowkey_column_num_, seq_no))) {
      LOG_WARN("fail to from datums", KR(ret));
    } else if (OB_FAIL(external_writer_.write_item(row_))) {
      LOG_WARN("fail to write item", KR(ret));
    } else {
      ++fragment_row_count_;
      ++total_row_count_;
    }
    if (OB_SUCC(ret) && (external_writer_.get_file_size() >= MAX_TMP_FILE_SIZE)) {
      if (OB_FAIL(switch_fragment())) {
        LOG_WARN("fail to switch fragment", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadExternalMultiPartitionTableBuilder::alloc_tmp_file()
{
  int ret = OB_SUCCESS;
  int64_t dir_id = -1;
  if (OB_FAIL(param_.file_mgr_->alloc_dir(dir_id))) {
    LOG_WARN("fail to alloc dir", KR(ret));
  } else if (OB_FAIL(param_.file_mgr_->alloc_file(dir_id, file_handle_))) {
    LOG_WARN("fail to alloc file", KR(ret));
  }
  return ret;
}

int ObDirectLoadExternalMultiPartitionTableBuilder::generate_fragment()
{
  int ret = OB_SUCCESS;
  ObDirectLoadExternalFragment fragment;
  fragment.file_size_ = external_writer_.get_file_size();
  fragment.row_count_ = fragment_row_count_;
  fragment.max_data_block_size_ = external_writer_.get_max_block_size();
  if (OB_FAIL(fragment.file_handle_.assign(file_handle_))) {
    LOG_WARN("fail to assign file handle", KR(ret));
  } else if (OB_FAIL(fragment_array_.push_back(fragment))) {
    LOG_WARN("fail to push back fragment", KR(ret));
  } else if (max_data_block_size_ < fragment.max_data_block_size_) {
    max_data_block_size_ = fragment.max_data_block_size_;
  }
  return ret;
}

int ObDirectLoadExternalMultiPartitionTableBuilder::switch_fragment()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(external_writer_.close())) {
    LOG_WARN("fail to close external writer", KR(ret));
  } else if (OB_FAIL(generate_fragment())) {
    LOG_WARN("fail to generate fragment", KR(ret));
  } else if (OB_FAIL(alloc_tmp_file())) {
    LOG_WARN("fail to alloc tmp file", KR(ret));
  } else {
    external_writer_.reuse();
    if (OB_FAIL(external_writer_.open(file_handle_))) {
      LOG_WARN("fail to open file", KR(ret));
    } else {
      fragment_row_count_ = 0;
    }
  }
  return ret;
}

int ObDirectLoadExternalMultiPartitionTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load external multi partition table is closed", KR(ret));
  } else if (OB_FAIL(external_writer_.close())) {
    LOG_WARN("fail to close external writer", KR(ret));
  } else if ((fragment_row_count_ > 0) || fragment_array_.empty()) {
    if (OB_FAIL(generate_fragment())) {
      LOG_WARN("failed to generate fragment", KR(ret));
    } else {
      fragment_row_count_ = 0;
    }
  }
  if (OB_SUCC(ret)) {
    is_closed_ = true;
  }
  return ret;
}

int ObDirectLoadExternalMultiPartitionTableBuilder::get_tables(
  ObIArray<ObIDirectLoadPartitionTable *> &table_array, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load external table not closed", KR(ret));
  } else {
    ObDirectLoadExternalTableCreateParam create_param;
    create_param.tablet_id_ = 0; //因为包含了所有的tablet_id，设置为一个无效值
    create_param.data_block_size_ = param_.table_data_desc_.external_data_block_size_;
    create_param.row_count_ = total_row_count_;
    create_param.max_data_block_size_ = max_data_block_size_;
    if (OB_FAIL(create_param.fragments_.assign(fragment_array_))) {
      LOG_WARN("fail to assign fragment array", KR(ret), K(fragment_array_));
    } else {
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
