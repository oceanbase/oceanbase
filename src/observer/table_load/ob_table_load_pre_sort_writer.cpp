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
#include "observer/table_load/ob_table_load_mem_chunk_manager.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

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
    mem_ctx_(nullptr),
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
                                   ObTableLoadErrorRowHandler *error_row_handler)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPreSortWriter init twice", KR(ret));
  } else if (OB_ISNULL(store_writer)
            || OB_ISNULL(pre_sorter)
            || OB_ISNULL(error_row_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(pre_sorter), KP(store_writer), KP(error_row_handler));
  } else {
    pre_sorter_ = pre_sorter;
    store_writer_ = store_writer;
    error_row_handler_ = error_row_handler;
    mem_ctx_ = &pre_sorter->mem_ctx_;
    chunks_manager_ = pre_sorter_->chunks_manager_;
    is_inited_ = true;
  }
  return ret;
}


int ObTableLoadPreSortWriter::write(int32_t session_id,
                                    const ObTableLoadTabletObjRowArray &row_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPreSortWriter not init", KR(ret));
  } else if (OB_UNLIKELY(session_id < 1 ||
                         session_id > pre_sorter_->ctx_->param_.write_session_count_ ||
                         row_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(session_id), K(row_array.empty()));
  } else {
    const ObDirectLoadDatumRow *datum_row = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_array.count(); ++i) {
      const ObTableLoadTabletObjRow &row = row_array.at(i);
      if (OB_FAIL(store_writer_->cast_row(session_id, row.obj_row_, datum_row))) {
        if (OB_FAIL(error_row_handler_->handle_error_row(ret))) {
          LOG_WARN("failed to handle error row", K(ret), K(row));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(datum_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum row is nullptr", KR(ret));
      } else if (OB_FAIL(append_row(row.tablet_id_, *datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    } // for
    if (OB_SUCC(ret)) {
      ATOMIC_AAF(&pre_sorter_->ctx_->job_stat_->store_.processed_rows_, row_array.count());
    }
  }
  return ret;
}

int ObTableLoadPreSortWriter::px_write(ObIVector *tablet_id_vector,
                                       const ObIArray<ObIVector *> &vectors,
                                       const ObBatchRows &batch_rows,
                                       int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPreSortWriter not init", KR(ret));
  } else if (OB_UNLIKELY(nullptr == tablet_id_vector ||
                         vectors.count() != mem_ctx_->table_data_desc_.column_count_ ||
                         (!batch_rows.all_rows_active_ && nullptr == batch_rows.skip_) ||
                         batch_rows.size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(tablet_id_vector), K(vectors.count()), K(batch_rows));
  } else if (!datum_row_.is_valid() && OB_FAIL(datum_row_.init(mem_ctx_->table_data_desc_.column_count_))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else if (OB_UNLIKELY(-1 != chunk_node_id_ || nullptr != chunk_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected chunk not close", KR(ret), K(chunk_node_id_), KP(chunk_));
  } else {
    datum_row_.seq_no_ = 0;
    ObTabletID tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.size_; ++i) {
      if (!batch_rows.all_rows_active_ && batch_rows.skip_->at(i)) {
        continue;
      } else {
        ++affected_rows;
        tablet_id = ObDirectLoadVectorUtils::get_tablet_id(tablet_id_vector, i);
        if (OB_FAIL(ObDirectLoadVectorUtils::to_datums(vectors,
                                                       i,
                                                       datum_row_.storage_datums_,
                                                       datum_row_.count_))) {
          LOG_WARN("fail to transfer vectors to datums", KR(ret), K(i));
        } else if (OB_FAIL(append_row(tablet_id, datum_row_))) {
          LOG_WARN("fail to append row", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_AAF(&pre_sorter_->ctx_->job_stat_->store_.processed_rows_, affected_rows);
    }
    if (OB_LIKELY(-1 != chunk_node_id_ && nullptr != chunk_)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(chunks_manager_->push_chunk(chunk_node_id_))) {
        LOG_WARN("fail to push chunk", KR(tmp_ret), K(chunk_node_id_));
        ret = COVER_SUCC(tmp_ret);
      } else {
        chunk_ = nullptr;
        chunk_node_id_ = -1;
      }
    }
  }
  return ret;
}

int ObTableLoadPreSortWriter::append_row(const ObTabletID &tablet_id,
                                         const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  bool success_write = false;
  RowType const_row;
  external_row_.tablet_id_ = tablet_id;
  if (OB_FAIL(external_row_.external_row_.from_datum_row(datum_row,
                                                         mem_ctx_->table_data_desc_.rowkey_column_num_))) {
    LOG_WARN("fail to cast row from datum row", KR(ret));
  }
  while (OB_SUCC(ret) && !success_write) {
    if (nullptr == chunk_ && OB_FAIL(chunks_manager_->get_chunk(chunk_node_id_, chunk_))) {
      LOG_WARN("fail to get chunk", KR(ret));
    } else {
      const_row = external_row_;
      if (OB_FAIL(chunk_->add_item(const_row))) {
        if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
          LOG_WARN("fail to add item", KR(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(chunks_manager_->close_chunk(chunk_node_id_))) {
            LOG_WARN("fail to close chunk", KR(ret));
          } else {
            chunk_node_id_ = -1;
            chunk_ = nullptr;
          }
        }
      } else {
        success_write = true;
      }
    }
  }
  return ret;
}

int ObTableLoadPreSortWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPreSortWriter is not init", KR(ret));
  } else if (OB_LIKELY(-1 != chunk_node_id_ && nullptr != chunk_)) {
    if (OB_FAIL(chunks_manager_->push_chunk(chunk_node_id_))) {
      LOG_WARN("fail to push chunk", K(chunk_node_id_), KR(ret));
    } else {
      chunk_ = nullptr;
      chunk_node_id_ = -1;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
