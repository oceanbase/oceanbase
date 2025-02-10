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

#include "observer/table_load/ob_table_load_index_table_builder.h"
#include "observer/table_load/ob_table_load_row_projector.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace table;

/**
 * ObTableLoadIndexTableBuildParam
 */

ObTableLoadIndexTableBuildParam::ObTableLoadIndexTableBuildParam()
  : rowkey_column_count_(0),
    datum_utils_(nullptr),
    table_data_desc_(),
    project_(nullptr),
    file_mgr_(nullptr)
{
}

bool ObTableLoadIndexTableBuildParam::is_valid() const
{
  return rowkey_column_count_ > 0 && nullptr != datum_utils_ && table_data_desc_.is_valid() &&
         nullptr != project_ && nullptr != file_mgr_;
}

/**
 * ObTableLoadIndexTableBuilder
 */

ObTableLoadIndexTableBuilder::ObTableLoadIndexTableBuilder()
  : rowkey_column_count_(0), datum_utils_(nullptr), project_(nullptr), is_inited_(false)
{
}

ObTableLoadIndexTableBuilder::~ObTableLoadIndexTableBuilder() {}

int ObTableLoadIndexTableBuilder::init(const ObTableLoadIndexTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadIndexTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    ObDirectLoadExternalMultiPartitionTableBuildParam builder_param;
    builder_param.table_data_desc_ = param.table_data_desc_;
    builder_param.file_mgr_ = param.file_mgr_;
    builder_param.extra_buf_ = reinterpret_cast<char *>(1);
    builder_param.extra_buf_size_ = 4096;
    if (OB_FAIL(insert_row_.init(param.table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(delete_row_.init(param.table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(table_builder_.init(builder_param))) {
      LOG_WARN("fail to init new_builder", KR(ret));
    } else {
      rowkey_column_count_ = param.rowkey_column_count_;
      datum_utils_ = param.datum_utils_;
      project_ = param.project_;
      delete_row_.is_delete_ = true;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadIndexTableBuilder::append_insert_row(const ObTabletID &tablet_id,
                                                    const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableBuilder not init", KR(ret), KP(this));
  } else {
    insert_row_.seq_no_ = datum_row.seq_no_;
    ObTabletID index_tablet_id;
    if (OB_FAIL(project_->projector(tablet_id, datum_row, index_tablet_id, insert_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(datum_row));
    } else if (OB_FAIL(table_builder_.append_row(index_tablet_id, insert_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadIndexTableBuilder::append_insert_row(const ObTabletID &tablet_id,
                                                    const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableBuilder not init", KR(ret), KP(this));
  } else {
    insert_row_.seq_no_ = 0; // seq_no丢失
    ObTabletID index_tablet_id;
    if (OB_FAIL(project_->projector(tablet_id, datum_row, index_tablet_id, insert_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(datum_row));
    } else if (OB_FAIL(table_builder_.append_row(index_tablet_id, insert_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadIndexTableBuilder::append_insert_batch(const ObTabletID &tablet_id,
                                                      const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableBuilder not init", KR(ret), KP(this));
  } else {
    insert_row_.seq_no_ = 0; // seq_no丢失
    ObTabletID index_tablet_id;
    if (OB_FAIL(project_->get_dest_tablet_id(tablet_id, index_tablet_id))) {
      LOG_WARN("fail to get index tablet id", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.row_count_; ++i) {
      if (OB_FAIL(project_->projector(datum_rows, i, insert_row_))) {
        LOG_WARN("fail to projector", KR(ret), K(tablet_id));
      } else if (OB_FAIL(table_builder_.append_row(index_tablet_id, insert_row_))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadIndexTableBuilder::append_replace_row(const ObTabletID &tablet_id,
                                                     const ObDirectLoadDatumRow &old_row,
                                                     const ObDirectLoadDatumRow &new_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableBuilder not init", KR(ret), KP(this));
  } else {
    insert_row_.seq_no_ = new_row.seq_no_;
    delete_row_.seq_no_ = old_row.seq_no_;
    ObTabletID index_tablet_id;
    if (OB_FAIL(project_->projector(tablet_id, new_row, index_tablet_id, insert_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(new_row));
    } else if (OB_FAIL(project_->projector(tablet_id, old_row, index_tablet_id, delete_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(old_row));
    } else {
      ObDatumRowkey new_key(insert_row_.storage_datums_, rowkey_column_count_);
      ObDatumRowkey old_key(delete_row_.storage_datums_, rowkey_column_count_);
      int cmp_ret;
      if (OB_FAIL(old_key.compare(new_key, *datum_utils_, cmp_ret))) {
        LOG_WARN("fail to compare", KR(ret));
      } else {
        // when cmp_ret == 0, insert new row is equal to  delete old row and insert new row.
        if (0 != cmp_ret) {
          if (OB_FAIL(table_builder_.append_row(index_tablet_id, delete_row_))) {
            LOG_WARN("fail to append row", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_builder_.append_row(index_tablet_id, insert_row_))) {
          LOG_WARN("fail to append row", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadIndexTableBuilder::append_delete_row(const ObTabletID &tablet_id,
                                                    const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableBuilder not init", KR(ret), KP(this));
  } else {
    delete_row_.seq_no_ = datum_row.seq_no_;
    ObTabletID index_tablet_id;
    if (OB_FAIL(project_->projector(tablet_id, datum_row, index_tablet_id, delete_row_))) {
      LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(datum_row));
    } else if (OB_FAIL(table_builder_.append_row(index_tablet_id, delete_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadIndexTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableBuilder not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_builder_.close())) {
      LOG_WARN("fail to close table builder", KR(ret));
    }
  }
  return ret;
}

int64_t ObTableLoadIndexTableBuilder::get_row_count() const
{
  return table_builder_.get_row_count();
}

int ObTableLoadIndexTableBuilder::get_tables(ObDirectLoadTableHandleArray &table_array,
                                             ObDirectLoadTableManager *table_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableBuilder not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_builder_.get_tables(table_array, table_mgr))) {
      LOG_WARN("fail to get tables", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
