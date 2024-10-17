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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_pre_sort_writer.h"
#include "observer/table_load/ob_table_load_pre_sorter.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
 #include "share/table/ob_table_load_row.h"
 #include "storage/blocksstable/ob_datum_row.h"
 #include "storage/direct_load/ob_direct_load_mem_chunk.h"
 #include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace observer
{

using namespace storage;
using namespace table;


ObTableLoadPreSortWriter::ObTableLoadPreSortWriter()
  : pre_sorter_(nullptr),
    store_writer_(nullptr),
    error_row_handler_(nullptr),
    table_data_desc_(nullptr),
    chunks_manager_(nullptr),
    chunk_node_id_(-1),
    chunk_(nullptr),
    is_inited_(false)
{
}
ObTableLoadPreSortWriter::~ObTableLoadPreSortWriter()
{
}

int ObTableLoadPreSortWriter::init(ObTableLoadPreSorter *pre_sorter,
                                   ObTableLoadTransStoreWriter *store_writer,
                                   ObTableLoadErrorRowHandler *error_row_handler,
                                   ObDirectLoadTableDataDesc *table_data_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPreSortWriter init twice", KR(ret));
  } else if (OB_ISNULL(store_writer)
            || OB_ISNULL(pre_sorter)
            || OB_ISNULL(error_row_handler)
            || OB_ISNULL(table_data_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(pre_sorter), KP(store_writer), KP(error_row_handler), KP(table_data_desc));
  } else {
    store_writer_ = store_writer;
    pre_sorter_ = pre_sorter;
    error_row_handler_ = error_row_handler;
    table_data_desc_ = table_data_desc;
    if (OB_ISNULL(pre_sorter_->chunks_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("chunks manager is nullptr", KR(ret));
    } else {
      chunks_manager_ = pre_sorter_->chunks_manager_;
      is_inited_ = true;
    }
  }
  return ret;
}


int ObTableLoadPreSortWriter::write(int32_t session_id,
                                    const ObTableLoadTabletObjRowArray &row_array)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *datum_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter not init", KR(ret));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > pre_sorter_->ctx_->param_.write_session_count_)
            || row_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(session_id), K(row_array.empty()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_array.count(); ++i) {
      const ObTableLoadTabletObjRow &row = row_array.at(i);
      ObNewRow new_row(row.obj_row_.cells_, row.obj_row_.count_);
      if (OB_FAIL(store_writer_->cast_row(session_id, new_row, datum_row))) {
        if (OB_FAIL(error_row_handler_->handle_error_row(ret))) {
          LOG_WARN("failed to handle error row", K(ret), K(row));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(datum_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum row is nullptr", KR(ret));
      } else if (OB_FAIL(append_row(row.tablet_id_, *datum_row, row.obj_row_.seq_no_))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    } // for
  }
  return ret;
}

int ObTableLoadPreSortWriter::px_write(const ObTabletID &tablet_id,
                                       const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObTableLoadSequenceNo seq_no(0);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter is not init", KR(ret));
  } else if (OB_FAIL(append_row(tablet_id, row, seq_no))) {
    LOG_WARN("fail to append row", KR(ret));
  }
  return ret;
}

int ObTableLoadPreSortWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter is not init", KR(ret));
  } else if (OB_NOT_NULL(chunk_) && -1 != chunk_node_id_) {
    if (OB_FAIL(chunks_manager_->push_chunk(chunk_node_id_))) {
      LOG_WARN("fail to push chunk", K(chunk_node_id_), KR(ret));
    } else {
      chunk_ = nullptr;
      chunk_node_id_ = -1;
    }
  }
  return ret;
}

int ObTableLoadPreSortWriter::append_row(const ObTabletID &tablet_id,
                                         const ObDatumRow &datum_row,
                                         ObTableLoadSequenceNo seq_no)
{
  int ret = OB_SUCCESS;
  ObTableLoadMemChunkManager *chunks_manager = nullptr;
  bool success_write = false;
  RowType const_row;
  external_row_.tablet_id_ = tablet_id;
  if (OB_FAIL(external_row_.external_row_.from_datums(datum_row.storage_datums_,
                                                            datum_row.count_,
                                                            table_data_desc_->rowkey_column_num_,
                                                            seq_no,
                                                            false))) {
    LOG_WARN("fail to cast row from datum", KR(ret));
  }
  while (OB_SUCC(ret) && !success_write) {
    if (OB_ISNULL(chunk_) && OB_FAIL(chunks_manager_->get_chunk(chunk_node_id_, chunk_))) {
      LOG_WARN("fail to get chunk", KR(ret));
    } else {
      const_row = external_row_;
      ret = chunk_->add_item(const_row);
      if (OB_BUF_NOT_ENOUGH == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(chunks_manager_->close_chunk(chunk_node_id_))) {
          LOG_WARN("fail to close chunk", KR(ret));
        } else {
          chunk_node_id_ = -1;
          chunk_ = nullptr;
        }
      } else if (OB_FAIL(ret)) {
        LOG_WARN("fail to add item");
      } else {
        success_write = true;
        ATOMIC_AAF(&pre_sorter_->ctx_->job_stat_->store_.processed_rows_, 1);
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
