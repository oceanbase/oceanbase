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
namespace oceanbase
{
namespace observer
{

using namespace storage;
using namespace table;


ObTableLoadPreSortWriter::ObTableLoadPreSortWriter()
  : store_writer_(nullptr),
    chunk_node_id_(-1),
    chunk_(nullptr),
    pre_sorter_(nullptr),
    is_inited_(false)
{
}
ObTableLoadPreSortWriter::~ObTableLoadPreSortWriter()
{
}

int ObTableLoadPreSortWriter::init(ObTableLoadPreSorter *pre_sorter,
                                   ObTableLoadTransStoreWriter *store_writer)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPreSortWriter init twice", KR(ret));
  } else if (OB_ISNULL(store_writer) || OB_ISNULL(pre_sorter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("store_writer or pre_sorter is nullptr", KR(ret));
  } else {
    store_writer_ = store_writer;
    pre_sorter_ = pre_sorter;
    is_inited_ = true;
  }
  return ret;
}


int ObTableLoadPreSortWriter::write(int32_t session_id,
                                    const ObTableLoadTabletObjRowArray &row_array)
{
  int ret = OB_SUCCESS;
  ObDirectLoadExternalMultiPartitionRow external_row;
  ObTableLoadMemChunkManager *chunks_manager = pre_sorter_->chunks_manager_;
  RowType const_row;
  bool get_next_row = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter not init", KR(ret));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > pre_sorter_->ctx_->param_.write_session_count_)
            || row_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(session_id), K(row_array.empty()));
  } else if (OB_ISNULL(chunks_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunks_manager should not be nullptr", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_array.count(); ++i) {
      const ObTableLoadTabletObjRow &row = row_array.at(i);
      ObNewRow new_row(row.obj_row_.cells_, row.obj_row_.count_);
      ObDatumRow datum_row;
      external_row.tablet_id_ = row.tablet_id_;
      if (OB_ISNULL(store_writer_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("store writer is nullptr", KR(ret));
      } else if (OB_FAIL(datum_row.init(pre_sorter_->store_ctx_->data_store_table_ctx_->table_data_desc_.column_count_))) {
        LOG_WARN("fail to init datum row", KR(ret));
      } else if (OB_FAIL(store_writer_->cast_row(session_id, new_row, datum_row))) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to cast row", KR(ret), K(session_id), K(row.tablet_id_), K(i));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(external_row.external_row_.from_datums(datum_row.storage_datums_,
                                                                datum_row.count_,
                                                                pre_sorter_->store_ctx_->data_store_table_ctx_->table_data_desc_.rowkey_column_num_,
                                                                row.obj_row_.seq_no_,
                                                                false))) {
        LOG_WARN("fail to cast to external row", KR(ret));
      } else if (OB_FAIL(append_row(external_row))) {
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
  ObDirectLoadExternalMultiPartitionRow external_row;
  ObTableLoadSequenceNo seq_no(0);
  external_row.tablet_id_ = tablet_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter is not init", KR(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(external_row.external_row_.from_datums(row.storage_datums_,
                                                            row.count_,
                                                            pre_sorter_->store_ctx_->data_store_table_ctx_->table_data_desc_.rowkey_column_num_,
                                                            seq_no,
                                                            false))) {
    LOG_WARN("fail to cast to external row", KR(ret));
  } else if (OB_FAIL(append_row(external_row))) {
    LOG_WARN("fail to append row", KR(ret));
  }
  return ret;
}

int ObTableLoadPreSortWriter::close_chunk()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(chunk_) && -1 != chunk_node_id_) {
    int ret = pre_sorter_->chunks_manager_->push_chunk(chunk_node_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to push chunk", K(chunk_node_id_), KR(ret));
    } else {
      chunk_ = nullptr;
      chunk_node_id_ = -1;
    }
    if (OB_NOT_NULL(chunk_)) {
      chunk_->~ChunkType();
      ob_free(chunk_);
      chunk_ = nullptr;
    }
  }
  return ret;
}

int ObTableLoadPreSortWriter::append_row(ObDirectLoadExternalMultiPartitionRow &external_row)
{
  int ret = OB_SUCCESS;
  bool success_write = false;
  RowType const_row;
  while (OB_SUCC(ret) && !success_write) {
    if (OB_ISNULL(chunk_) && OB_FAIL(pre_sorter_->chunks_manager_->get_chunk(chunk_node_id_, chunk_))) {
      LOG_WARN("fail to get chunk", KR(ret));
    } else {
      const_row = external_row;
      ret = chunk_->add_item(const_row);
      if (OB_BUF_NOT_ENOUGH == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(pre_sorter_->chunks_manager_->close_chunk(chunk_node_id_))) {
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
