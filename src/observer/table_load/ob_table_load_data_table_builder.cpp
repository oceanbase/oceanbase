/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_data_table_builder.h"
#include "observer/table_load/ob_table_load_row_projector.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace table;

/**
 * ObTableLoadDataTableBuildParam
 */

ObTableLoadDataTableBuildParam::ObTableLoadDataTableBuildParam()
  : datum_utils_(nullptr), table_data_desc_(), project_(nullptr), file_mgr_(nullptr)
{
}

bool ObTableLoadDataTableBuildParam::is_valid() const
{
  return nullptr != datum_utils_ && table_data_desc_.is_valid() && nullptr != project_ &&
         nullptr != file_mgr_;
}

ObTableLoadDataTableBuilder::ObTableLoadDataTableBuilder()
  : datum_utils_(nullptr), project_(nullptr), is_inited_(false)
{
}

ObTableLoadDataTableBuilder::~ObTableLoadDataTableBuilder() {}

int ObTableLoadDataTableBuilder::init(const ObTableLoadDataTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDataTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    ObDirectLoadExternalMultiPartitionTableBuildParam builder_param;
    builder_param.table_data_desc_ = param.table_data_desc_;
    builder_param.file_mgr_ = param.file_mgr_;
    builder_param.extra_buf_ = reinterpret_cast<char *>(1);
    builder_param.extra_buf_size_ = 4096;
    if (OB_FAIL(index_row_.init(param.project_->get_src_column_num()))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(ack_row_.init(param.table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(delete_row_.init(param.table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(table_builder_.init(builder_param))) {
      LOG_WARN("fail to init new_builder", KR(ret));
    } else {
      datum_utils_ = param.datum_utils_;
      project_ = param.project_;
      ack_row_.is_ack_ = true;
      delete_row_.is_delete_ = true;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDataTableBuilder::append_ack_row(const ObTabletID &tablet_id,
                                                const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else {
    ack_row_.seq_no_ = datum_row.seq_no_;
    ObTabletID data_tablet_id;
    if (OB_FAIL(project_->projector(tablet_id, datum_row, data_tablet_id, ack_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(datum_row));
    } else if (OB_FAIL(table_builder_.append_row(data_tablet_id, ack_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDataTableBuilder::append_delete_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else {
    delete_row_.seq_no_ = datum_row.seq_no_;
    ObTabletID data_tablet_id;
    if (OB_FAIL(project_->projector(tablet_id, datum_row, data_tablet_id, delete_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(datum_row));
    } else if (OB_FAIL(table_builder_.append_row(data_tablet_id, delete_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDataTableBuilder::append_delete_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadExternalRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else {
    delete_row_.seq_no_ = row.seq_no_;
    ObTabletID data_tablet_id;
    if (OB_FAIL(row.to_datum_row(index_row_))) {
      LOG_WARN("fail to get datum row", KR(ret), K(row));
    } else if (OB_FAIL(project_->projector(tablet_id, index_row_, data_tablet_id, delete_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(index_row_));
    } else if (OB_FAIL(table_builder_.append_row(data_tablet_id, delete_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDataTableBuilder::append_delete_row(const ObDirectLoadMultipleDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else {
    delete_row_.seq_no_ = row.seq_no_;
    const ObTabletID &tablet_id = row.rowkey_.tablet_id_;
    ObTabletID data_tablet_id;
    if (OB_FAIL(row.to_datum_row(index_row_))) {
      LOG_WARN("fail to get datum row", KR(ret), K(row));
    } else if (OB_FAIL(project_->projector(tablet_id, index_row_, data_tablet_id, delete_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(index_row_));
    } else if (OB_FAIL(table_builder_.append_row(data_tablet_id, delete_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDataTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_builder_.close())) {
      LOG_WARN("fail to close table builder", KR(ret));
    }
  }
  return ret;
}

int64_t ObTableLoadDataTableBuilder::get_row_count() const
{
  return table_builder_.get_row_count();
}

int ObTableLoadDataTableBuilder::get_tables(ObDirectLoadTableHandleArray &table_array,
                                            ObDirectLoadTableManager *table_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else if (OB_FAIL(table_builder_.get_tables(table_array, table_mgr))) {
    LOG_WARN("fail to get tables", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase