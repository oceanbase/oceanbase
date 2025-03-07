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

#include "observer/table_load/ob_table_load_trans_store.h"
#include "observer/table_load/ob_table_load_autoinc_nextval.h"
#include "observer/table_load/ob_table_load_data_row_insert_handler.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_trans_ctx.h"
#include "src/pl/ob_pl.h"
#include "share/sequence/ob_sequence_cache.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_writer.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace common::hash;
using namespace share::schema;
using namespace share;
using namespace sql;
using namespace storage;
using namespace table;

/**
 * ObTableLoadTransStore
 */

int ObTableLoadTransStore::init()
{
  int ret = OB_SUCCESS;
  const int32_t session_count = trans_ctx_->ctx_->param_.px_mode_?
                                1 : trans_ctx_->ctx_->param_.write_session_count_;
  SessionStore *session_store = nullptr;
  for (int32_t i = 0; OB_SUCC(ret) && i < session_count; ++i) {
    if (OB_ISNULL(session_store = OB_NEWx(SessionStore, (&trans_ctx_->allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new SessionStore", KR(ret));
    } else {
      if (trans_ctx_->ctx_->param_.px_mode_) {
        session_store->session_id_ = (ATOMIC_FAA(&(trans_ctx_->ctx_->store_ctx_->next_session_id_), 1) % trans_ctx_->ctx_->param_.write_session_count_) + 1;
      } else {
        session_store->session_id_ = i + 1;
      }
      if (OB_FAIL(session_store_array_.push_back(session_store))) {
        LOG_WARN("fail to push back session store", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      if (nullptr != session_store) {
        session_store->~SessionStore();
        trans_ctx_->allocator_.free(session_store);
        session_store = nullptr;
      }
    }
  }
  return ret;
}

void ObTableLoadTransStore::reset()
{
  for (int64_t i = 0; i < session_store_array_.count(); ++i) {
    SessionStore *session_store = session_store_array_.at(i);
    // free partition tables
    session_store->tables_handle_.reset();
    // free session_store
    session_store->~SessionStore();
    trans_ctx_->allocator_.free(session_store);
  }
  session_store_array_.reset();
}

/**
 * ObTableLoadTransStoreWriter
 */

    /**
     * StoreWriter
     */

ObTableLoadTransStoreWriter::StoreWriter::StoreWriter()
  : store_ctx_(nullptr),
    trans_store_(nullptr),
    session_id_(0),
    allocator_("TLD_SW"),
    is_single_part_(false),
    is_closed_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  table_builders_.set_attr(ObMemAttr(MTL_ID(), "TLD_SW"));
}

ObTableLoadTransStoreWriter::StoreWriter::~StoreWriter()
{
  reset();
}

void ObTableLoadTransStoreWriter::StoreWriter::reset()
{
  store_ctx_ = nullptr;
  trans_store_ = nullptr;
  session_id_ = 0;
  table_builder_map_.destroy();
  for (int64_t i = 0; i < table_builders_.count(); ++i) {
    ObIDirectLoadPartitionTableBuilder *table_builder = table_builders_.at(i);
    table_builder->~ObIDirectLoadPartitionTableBuilder();
    allocator_.free(table_builder);
  }
  table_builders_.reset();
  allocator_.reset();
  is_single_part_ = false;
  is_closed_ = false;
  is_inited_ = false;
}

int ObTableLoadTransStoreWriter::StoreWriter::init(ObTableLoadStoreCtx *store_ctx,
                                                   ObTableLoadTransStore *trans_store,
                                                   int32_t session_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("StoreWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == store_ctx || nullptr == trans_store || session_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx), KP(trans_store), K(session_id));
  } else {
    if (OB_FAIL(table_builder_map_.create(64, "TLD_SW_Map", "TLD_SW_Map", MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret));
    } else if (OB_FAIL(datum_row_.init(store_ctx->write_ctx_.table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      store_ctx_ = store_ctx;
      trans_store_ = trans_store;
      session_id_ = session_id;
      is_single_part_ = (store_ctx_->write_ctx_.is_multiple_mode_ ||
                         1 == store_ctx->data_store_table_ctx_->ls_partition_ids_.count());
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::StoreWriter::inner_append_row(
  const ObTabletID &tablet_id,
  const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  ObIDirectLoadPartitionTableBuilder *table_builder = nullptr;
  if (OB_FAIL(get_table_builder(tablet_id, table_builder))) {
    LOG_WARN("fail to get table builder", KR(ret), K(tablet_id));
  } else if (OB_FAIL(table_builder->append_row(tablet_id, datum_row))) {
    LOG_WARN("fail to append row", KR(ret), K(datum_row));
  }
  if (OB_FAIL(ret)) {
    ObTableLoadErrorRowHandler *error_row_handler = store_ctx_->error_row_handler_;
    ObDirectLoadDMLRowHandler *dml_row_handler = store_ctx_->write_ctx_.dml_row_handler_;
    if (OB_LIKELY(OB_ERR_PRIMARY_KEY_DUPLICATE == ret)) {
      if (OB_FAIL(dml_row_handler->handle_update_row(tablet_id, datum_row))) {
        LOG_WARN("fail to handle update row", KR(ret), K(datum_row));
      }
    } else if (OB_LIKELY(OB_ROWKEY_ORDER_ERROR == ret)) {
      if (OB_FAIL(error_row_handler->handle_error_row(ret))) {
        LOG_WARN("fail to handle error row", KR(ret), K(tablet_id), K(datum_row));
      }
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::StoreWriter::append_row(const ObTabletID &tablet_id,
                                                         const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("StoreWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected writer is closed", KR(ret));
  } else if (OB_FAIL(inner_append_row(tablet_id, datum_row))) {
    LOG_WARN("fail to append row", KR(ret));
  }
  return ret;
}

int ObTableLoadTransStoreWriter::StoreWriter::append_batch(
  ObIVector *tablet_id_vector,
  const ObIArray<ObIVector *> &vectors,
  const ObBatchRows &batch_rows,
  int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("StoreWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected writer is closed", KR(ret));
  } else if (OB_UNLIKELY(nullptr == tablet_id_vector ||
                         vectors.count() != datum_row_.get_column_count() ||
                         (!batch_rows.all_rows_active_ && nullptr == batch_rows.skip_) ||
                         batch_rows.size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(tablet_id_vector), K(vectors), K(batch_rows));
  } else {
    // TODO(suzhi.yt) 这一期只有px_write会走append_batch, 这里先写死seq_no
    datum_row_.seq_no_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.size_; ++i) {
      if (!batch_rows.all_rows_active_ && batch_rows.skip_->at(i)) {
        continue;
      } else {
        const ObTabletID tablet_id = ObDirectLoadVectorUtils::get_tablet_id(tablet_id_vector, i);
        if (OB_FAIL(ObDirectLoadVectorUtils::to_datums(vectors,
                                                       i,
                                                       datum_row_.storage_datums_,
                                                       datum_row_.count_))) {
          LOG_WARN("fail to transfer vectors to datums", KR(ret), K(i));
        } else if (OB_FAIL(inner_append_row(tablet_id, datum_row_))) {
          LOG_WARN("fail to append row", KR(ret));
        } else {
          ++affected_rows;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::StoreWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("StoreWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected writer is closed", KR(ret));
  } else {
    ObTableLoadTransStore::SessionStore *session_store =
      trans_store_->session_store_array_.at(session_id_ - 1);
    for (int64_t i = 0; OB_SUCC(ret) && i < table_builders_.count(); ++i) {
      ObIDirectLoadPartitionTableBuilder *table_builder = table_builders_.at(i);
      if (OB_FAIL(table_builder->close())) {
        LOG_WARN("fail to close table store", KR(ret));
      } else if (OB_FAIL(table_builder->get_tables(session_store->tables_handle_,
                                                   store_ctx_->table_mgr_))) {
        LOG_WARN("fail to get tables", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_closed_ = true;
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::StoreWriter::new_table_builder(
  const ObTabletID &tablet_id,
  ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  if (store_ctx_->write_ctx_.is_multiple_mode_) {
    // 排序路径
    ObDirectLoadExternalMultiPartitionTableBuildParam param;
    param.table_data_desc_ = store_ctx_->write_ctx_.table_data_desc_;
    param.file_mgr_ = store_ctx_->tmp_file_mgr_;
    param.extra_buf_ = reinterpret_cast<char *>(1); // unuse, delete in future
    param.extra_buf_size_ = param.table_data_desc_.extra_buf_size_;
    ObDirectLoadExternalMultiPartitionTableBuilder *external_mp_table_builder = nullptr;
    if (OB_ISNULL(table_builder = external_mp_table_builder =
                    OB_NEWx(ObDirectLoadExternalMultiPartitionTableBuilder, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadExternalMultiPartitionTableBuilder", KR(ret));
    } else if (OB_FAIL(external_mp_table_builder->init(param))) {
      LOG_WARN("fail to init external multi partition table builder", KR(ret));
    }
  } else {
    // 有主键表不排序路径
    abort_unless(!store_ctx_->data_store_table_ctx_->schema_->is_table_without_pk_);
    ObDirectLoadMultipleSSTableBuildParam param;
    param.tablet_id_ = tablet_id;
    param.table_data_desc_ = store_ctx_->write_ctx_.table_data_desc_;
    param.datum_utils_ = &(store_ctx_->data_store_table_ctx_->schema_->datum_utils_);
    param.file_mgr_ = store_ctx_->tmp_file_mgr_;
    param.extra_buf_ = reinterpret_cast<char *>(1); // unuse, delete in future
    param.extra_buf_size_ = param.table_data_desc_.extra_buf_size_;
    ObDirectLoadMultipleSSTableBuilder *sstable_builder = nullptr;
    if (OB_ISNULL(table_builder = sstable_builder =
                    OB_NEWx(ObDirectLoadMultipleSSTableBuilder, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTableBuilder", KR(ret));
    } else if (OB_FAIL(sstable_builder->init(param))) {
      LOG_WARN("fail to init sstable builder", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_builder) {
      table_builder->~ObIDirectLoadPartitionTableBuilder();
      allocator_.free(table_builder);
      table_builder = nullptr;
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::StoreWriter::get_table_builder(
  const ObTabletID &tablet_id,
  ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id));
  } else {
    if (is_single_part_) {
      if (!table_builders_.empty()) {
        table_builder = table_builders_.at(0);
      }
    } else {
      if (OB_FAIL(table_builder_map_.get_refactored(tablet_id, table_builder))) {
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
          LOG_WARN("fail to get refactored", KR(ret), K(tablet_id));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret) && nullptr == table_builder) {
      // new table builder
      if (OB_FAIL(new_table_builder(tablet_id, table_builder))) {
        LOG_WARN("fail to new table builder", KR(ret), K(tablet_id));
      } else if (OB_FAIL(table_builder_map_.set_refactored(tablet_id, table_builder))) {
        LOG_WARN("fail to set refactored", KR(ret), K(tablet_id));
      } else if (OB_FAIL(table_builders_.push_back(table_builder))) {
        LOG_WARN("fail to push back", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != table_builder) {
          table_builder->~ObIDirectLoadPartitionTableBuilder();
          allocator_.free(table_builder);
          table_builder = nullptr;
        }
      }
    }
  }
  return ret;
}

    /**
     * DirectWriter
     */

ObTableLoadTransStoreWriter::DirectWriter::DirectWriter()
  : store_ctx_(nullptr),
    allocator_("TLD_DW"),
    lob_allocator_("TLD_LobAlloc"),
    max_batch_size_(0),
    is_single_part_(false),
    is_closed_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  lob_allocator_.set_tenant_id(MTL_ID());
  batch_writers_.set_attr(ObMemAttr(MTL_ID(), "TLD_DW"));
}

ObTableLoadTransStoreWriter::DirectWriter::~DirectWriter()
{
  reset();
}

void ObTableLoadTransStoreWriter::DirectWriter::reset()
{
  store_ctx_ = nullptr;
  batch_writer_map_.destroy();
  for (int64_t i = 0; i < batch_writers_.count(); ++i) {
    ObDirectLoadInsertTableBatchRowDirectWriter *batch_writer = batch_writers_.at(i);
    batch_writer->~ObDirectLoadInsertTableBatchRowDirectWriter();
    allocator_.free(batch_writer);
  }
  batch_writers_.reset();
  allocator_.reset();
  max_batch_size_ = 0;
  is_single_part_ = false;
  is_closed_ = false;
  is_inited_ = false;
}

int ObTableLoadTransStoreWriter::DirectWriter::init(ObTableLoadStoreCtx *store_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("DirectWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx));
  } else {
    if (OB_FAIL(batch_writer_map_.create(64, "TLD_DW_Map", "TLD_DW_Map", MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret));
    } else {
      store_ctx_ = store_ctx;
      max_batch_size_ = store_ctx->ctx_->param_.batch_size_;
      is_single_part_ = (1 == store_ctx->data_store_table_ctx_->ls_partition_ids_.count());
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::DirectWriter::append_row(const ObTabletID &tablet_id,
                                                          const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("DirectWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected writer is closed", KR(ret));
  } else {
    ObDirectLoadInsertTableBatchRowDirectWriter *batch_writer = nullptr;
    if (OB_FAIL(get_batch_writer(tablet_id, batch_writer))) {
      LOG_WARN("fail to get batch writer", KR(ret), K(tablet_id));
    } else if (OB_FAIL(batch_writer->append_row(datum_row))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::DirectWriter::append_batch(
  ObIVector *tablet_id_vector,
  const ObIArray<ObIVector *> &vectors,
  const ObBatchRows &batch_rows,
  int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("DirectWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected writer is closed", KR(ret));
  } else if (OB_UNLIKELY(nullptr == tablet_id_vector ||
                         (!batch_rows.all_rows_active_ && nullptr == batch_rows.skip_) ||
                         batch_rows.size_ <= 0 || batch_rows.size_ > max_batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(tablet_id_vector), K(vectors), K(batch_rows),
             K(max_batch_size_));
  } else {
    const bool all_tablet_id_is_same = ObDirectLoadVectorUtils::check_all_tablet_id_is_same(tablet_id_vector, batch_rows.size_);
    ObDirectLoadInsertTableBatchRowDirectWriter *batch_writer = nullptr;
    // direct path
    if (batch_rows.all_rows_active_ && all_tablet_id_is_same && batch_rows.size_ >= max_batch_size_ / 2) {
      const ObTabletID tablet_id = ObDirectLoadVectorUtils::get_tablet_id(tablet_id_vector, 0);
      if (OB_FAIL(get_batch_writer(tablet_id, batch_writer))) {
        LOG_WARN("fail to get batch writer", KR(ret), K(tablet_id));
      } else if (OB_FAIL(batch_writer->append_batch(vectors, batch_rows.size_))) {
        LOG_WARN("fail to append batch", KR(ret));
      } else {
        affected_rows = batch_rows.size_;
      }
    }
    // buffer path
    else if (all_tablet_id_is_same) {
      const ObTabletID tablet_id = ObDirectLoadVectorUtils::get_tablet_id(tablet_id_vector, 0);
      if (OB_FAIL(get_batch_writer(tablet_id, batch_writer))) {
        LOG_WARN("fail to get batch writer", KR(ret), K(tablet_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.size_; ++i) {
        if (!batch_rows.all_rows_active_ && batch_rows.skip_->at(i)) {
          continue;
        } else if (OB_FAIL(batch_writer->append_row(vectors, i))) {
          LOG_WARN("fail to append row", KR(ret), K(i));
        } else {
          ++affected_rows;
        }
      }
    } else if (batch_rows.all_rows_active_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.size_; ++i) {
        const ObTabletID tablet_id = ObDirectLoadVectorUtils::get_tablet_id(tablet_id_vector, i);
        if (OB_FAIL(get_batch_writer(tablet_id, batch_writer))) {
          LOG_WARN("fail to get batch writer", KR(ret), K(tablet_id));
        } else if (OB_FAIL(batch_writer->append_row(vectors, i))) {
          LOG_WARN("fail to append row", KR(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        affected_rows = batch_rows.size_;
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.size_; ++i) {
        if (!batch_rows.all_rows_active_ && batch_rows.skip_->at(i)) {
          continue;
        } else {
          const ObTabletID tablet_id = ObDirectLoadVectorUtils::get_tablet_id(tablet_id_vector, i);
          if (OB_FAIL(get_batch_writer(tablet_id, batch_writer))) {
            LOG_WARN("fail to get batch writer", KR(ret), K(tablet_id));
          } else if (OB_FAIL(batch_writer->append_row(vectors, i))) {
            LOG_WARN("fail to append row", KR(ret), K(i));
          } else {
            ++affected_rows;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::DirectWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("StoreWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected writer is closed", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_writers_.count(); ++i) {
      ObDirectLoadInsertTableBatchRowDirectWriter *batch_writer = batch_writers_.at(i);
      if (OB_FAIL(batch_writer->close())) {
        LOG_WARN("fail to close", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_closed_ = true;
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::DirectWriter::new_batch_writer(
  const ObTabletID &tablet_id,
  ObDirectLoadInsertTableBatchRowDirectWriter *&batch_writer)
{
  int ret = OB_SUCCESS;
  batch_writer = nullptr;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx = nullptr;
  ObDirectLoadInsertTableRowInfo row_info;
  if (OB_FAIL(store_ctx_->data_store_table_ctx_->insert_table_ctx_->get_tablet_context(
        tablet_id, insert_tablet_ctx))) {
    LOG_WARN("fail to get tablet context ", KR(ret), K(tablet_id));
  } else if (OB_FAIL(insert_tablet_ctx->get_row_info(row_info))) {
    LOG_WARN("fail to get row info", KR(ret));
  } else if (OB_ISNULL(batch_writer =
                         OB_NEWx(ObDirectLoadInsertTableBatchRowDirectWriter, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadInsertTableBatchRowDirectWriter", KR(ret));
  } else if (OB_FAIL(batch_writer->init(insert_tablet_ctx,
                                        row_info,
                                        store_ctx_->write_ctx_.dml_row_handler_,
                                        &lob_allocator_))) {
    LOG_WARN("fail to init direct batch writer", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != batch_writer) {
      batch_writer->~ObDirectLoadInsertTableBatchRowDirectWriter();
      allocator_.free(batch_writer);
      batch_writer = nullptr;
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::DirectWriter::get_batch_writer(
  const ObTabletID &tablet_id,
  ObDirectLoadInsertTableBatchRowDirectWriter *&batch_writer)
{
  int ret = OB_SUCCESS;
  batch_writer = nullptr;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id));
  } else {
    if (is_single_part_) {
      if (!batch_writers_.empty()) {
        batch_writer = batch_writers_.at(0);
      }
    } else {
      if (OB_FAIL(batch_writer_map_.get_refactored(tablet_id, batch_writer))) {
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
          LOG_WARN("fail to get refactored", KR(ret), K(tablet_id));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret) && nullptr == batch_writer) {
      // new batch writer
      if (OB_FAIL(new_batch_writer(tablet_id, batch_writer))) {
        LOG_WARN("fail to new batch writer", KR(ret), K(tablet_id));
      } else if (OB_FAIL(batch_writer_map_.set_refactored(tablet_id, batch_writer))) {
        LOG_WARN("fail to set refactored", KR(ret), K(tablet_id));
      } else if (OB_FAIL(batch_writers_.push_back(batch_writer))) {
        LOG_WARN("fail to push back", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != batch_writer) {
          batch_writer->~ObDirectLoadInsertTableBatchRowDirectWriter();
          allocator_.free(batch_writer);
          batch_writer = nullptr;
        }
      }
    }
  }
  return ret;
}

    /**
     * SessionContext
     */
ObTableLoadTransStoreWriter::SessionContext::SessionContext(int32_t session_id, uint64_t tenant_id, ObDataTypeCastParams cast_params)
  : session_id_(session_id),
    cast_allocator_("TLD_TS_Caster"),
    cast_params_(cast_params),
    last_receive_sequence_no_(0)
{
  cast_allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadTransStoreWriter::SessionContext::~SessionContext()
{
  datum_row_.reset();
  if (nullptr != writer_) {
    writer_->~IWriter();
    writer_ = nullptr;
  }
}

ObTableLoadTransStoreWriter::ObTableLoadTransStoreWriter(ObTableLoadTransStore *trans_store)
  : trans_store_(trans_store),
    trans_ctx_(trans_store->trans_ctx_),
    store_ctx_(trans_ctx_->ctx_->store_ctx_),
    param_(trans_ctx_->ctx_->param_),
    allocator_("TLD_TSWriter"),
    table_data_desc_(nullptr),
    cast_mode_(CM_NONE),
    session_ctx_array_(nullptr),
    lob_inrow_threshold_(0),
    ref_count_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  column_schemas_.set_tenant_id(MTL_ID());
}

ObTableLoadTransStoreWriter::~ObTableLoadTransStoreWriter()
{
  if (nullptr != session_ctx_array_) {
    int32_t session_count = param_.px_mode_ ? 1 : param_.write_session_count_;
    for (int64_t i = 0; i < session_count; ++i) {
      SessionContext *session_ctx = session_ctx_array_ + i;
      session_ctx->~SessionContext();
    }
    allocator_.free(session_ctx_array_);
    session_ctx_array_ = nullptr;
  }
}

int ObTableLoadTransStoreWriter::init()
{
  int ret = OB_SUCCESS;
  int32_t session_count = param_.px_mode_ ? 1 : param_.write_session_count_;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadTransStoreWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(trans_store_->session_store_array_.count() != session_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(trans_store_));
  } else {
    table_data_desc_ = &store_ctx_->write_ctx_.table_data_desc_;
    collation_type_ = store_ctx_->data_store_table_ctx_->schema_->collation_type_;
    if (OB_FAIL(ObSQLUtils::get_default_cast_mode(store_ctx_->ctx_->session_info_, cast_mode_))) {
      LOG_WARN("fail to get_default_cast_mode", KR(ret));
    } else if (OB_FAIL(init_session_ctx_array())) {
      LOG_WARN("fail to init session ctx array", KR(ret));
    } else if (OB_FAIL(init_column_schemas_and_lob_info())) {
      LOG_WARN("fail to init column schemas and lob info", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::init_column_schemas_and_lob_info()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColDesc> &column_descs = store_ctx_->data_store_table_ctx_->schema_->column_descs_;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(ObTableLoadSchema::get_table_schema(param_.tenant_id_, param_.table_id_, schema_guard_,
                                                  table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(param_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
    const ObColumnSchemaV2 *column_schema =
      table_schema->get_column_schema(column_descs.at(i).col_id_);
    if (ObColumnSchemaV2::is_hidden_pk_column_id(column_schema->get_column_id())) {
    } else if (OB_FAIL(column_schemas_.push_back(column_schema))) {
      LOG_WARN("failed to push back column schema", K(ret), K(i), KPC(column_schema));
    }
  }
  if (OB_SUCC(ret)) {
    lob_inrow_threshold_ = table_schema->get_lob_inrow_threshold();
  }
  return ret;
}

int ObTableLoadTransStoreWriter::init_session_ctx_array()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  int32_t session_count = param_.px_mode_ ? 1 : param_.write_session_count_;
  ObDataTypeCastParams cast_params(trans_ctx_->ctx_->session_info_->get_timezone_info());
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(SessionContext) * session_count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else if (OB_FAIL(time_cvrt_.init(cast_params.get_nls_format(ObDateTimeType)))) {
    LOG_WARN("fail to init time converter", KR(ret));
  } else {
    session_ctx_array_ = static_cast<SessionContext *>(buf);
    for (int64_t i = 0; i < session_count; ++i) {
      new (session_ctx_array_ + i) SessionContext(i + 1, param_.tenant_id_, cast_params);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < session_count; ++i) {
    SessionContext *session_ctx = session_ctx_array_ + i;
    // init writer_
    if (store_ctx_->write_ctx_.is_fast_heap_table_) {
      DirectWriter *direct_writer = nullptr;
      if (OB_ISNULL(session_ctx->writer_ = direct_writer = OB_NEWx(DirectWriter, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new DirectWriter", KR(ret));
      } else if (OB_FAIL(direct_writer->init(store_ctx_))) {
        LOG_WARN("fail to init direct writer", KR(ret));
      }
    } else {
      StoreWriter *store_writer = nullptr;
      if (OB_ISNULL(session_ctx->writer_ = store_writer = OB_NEWx(StoreWriter, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new StoreWriter", KR(ret));
      } else if (OB_FAIL(store_writer->init(store_ctx_, trans_store_, session_ctx->session_id_))) {
        LOG_WARN("fail to init store writer", KR(ret));
      }
    }
    // init datum_row_
    if (OB_SUCC(ret)) {
      if (OB_FAIL(session_ctx->datum_row_.init(table_data_desc_->column_count_))) {
        LOG_WARN("fail to init datum row", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::advance_sequence_no(int32_t session_id, uint64_t sequence_no,
                                                     ObTableLoadMutexGuard &guard)
{
  int ret = OB_SUCCESS;
  int32_t session_count = param_.px_mode_ ? 1 : param_.write_session_count_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter not init", KR(ret));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > session_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id));
  } else {
    SessionContext &session_ctx = session_ctx_array_[session_id - 1];
    if (OB_UNLIKELY(sequence_no != session_ctx.last_receive_sequence_no_ + 1)) {
      if (OB_UNLIKELY(sequence_no != session_ctx.last_receive_sequence_no_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid sequence no", KR(ret), K(sequence_no),
                 K(session_ctx.last_receive_sequence_no_));
      } else {
        ret = OB_ENTRY_EXIST;
      }
    } else {
      session_ctx.last_receive_sequence_no_ = sequence_no;
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::write(int32_t session_id,
                                       const ObTableLoadTabletObjRowArray &row_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter not init", KR(ret));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > param_.write_session_count_) ||
             row_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id), K(row_array.empty()));
  } else {
    SessionContext &session_ctx = session_ctx_array_[session_id - 1];
    for (int64_t i = 0; OB_SUCC(ret) && i < row_array.count(); ++i) {
      const ObTableLoadTabletObjRow &row = row_array.at(i);
      if (OB_FAIL(cast_row(session_ctx.cast_allocator_, session_ctx.cast_params_, row.obj_row_,
                           session_ctx.datum_row_, session_id))) {
        ObTableLoadErrorRowHandler *error_row_handler =
          trans_ctx_->ctx_->store_ctx_->error_row_handler_;
        if (OB_FAIL(error_row_handler->handle_error_row(ret))) {
          LOG_WARN("failed to handle error row", K(ret), K(row));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(session_ctx.writer_->append_row(row.tablet_id_,
                                                         session_ctx.datum_row_))) {
        LOG_WARN("fail to write row", KR(ret), K(session_id), K(row.tablet_id_), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_AAF(&trans_ctx_->ctx_->job_stat_->store_.processed_rows_, row_array.count());
    }
    session_ctx.cast_allocator_.reuse();
  }
  return ret;
}

int ObTableLoadTransStoreWriter::px_write(ObIVector *tablet_id_vector,
                                          const ObIArray<ObIVector *> &vectors,
                                          const ObBatchRows &batch_rows,
                                          int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter not init", KR(ret));
  } else {
    SessionContext &session_ctx = session_ctx_array_[0];
    if (OB_FAIL(session_ctx.writer_->append_batch(tablet_id_vector, vectors, batch_rows, affected_rows))) {
      LOG_WARN("fail to append batch", KR(ret));
    } else {
      ATOMIC_AAF(&trans_ctx_->ctx_->job_stat_->store_.processed_rows_, affected_rows);
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::flush(int32_t session_id)
{
  int ret = OB_SUCCESS;
  int32_t session_count = param_.px_mode_ ? 1 : param_.write_session_count_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter not init", KR(ret));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > session_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id));
  } else {
    SessionContext &session_ctx = session_ctx_array_[session_id - 1];
    if (OB_FAIL(session_ctx.writer_->close())) {
      LOG_WARN("fail to close writer", KR(ret), K(session_id));
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::clean_up(int32_t session_id)
{
  int ret = OB_SUCCESS;
  int32_t session_count = param_.px_mode_ ? 1 : param_.write_session_count_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransStoreWriter not init", KR(ret));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > session_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id));
  } else {
    SessionContext &session_ctx = session_ctx_array_[session_id - 1];
    session_ctx.writer_->reset();
  }
  return ret;
}

int ObTableLoadTransStoreWriter::cast_row(ObArenaAllocator &cast_allocator,
                                          ObDataTypeCastParams cast_params,
                                          const ObTableLoadObjRow &obj_row,
                                          ObDirectLoadDatumRow &datum_row,
                                          int32_t session_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj_row.count_ != table_data_desc_->column_count_)) {
    ret = OB_ERR_INVALID_COLUMN_NUM;
    LOG_WARN("column count not match", KR(ret), K(obj_row.count_), K(table_data_desc_->column_count_));
  }
  datum_row.seq_no_ = obj_row.seq_no_;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_data_desc_->column_count_; ++i) {
    const ObColumnSchemaV2 *column_schema = column_schemas_.at(i);
    const ObObj &obj = obj_row.cells_[i];
    ObStorageDatum &datum = datum_row.storage_datums_[i];
    if (OB_FAIL(cast_column(cast_allocator, cast_params, column_schema, obj, datum, session_id))) {
      LOG_WARN("fail to cast column", KR(ret), K(i), K(obj), KPC(column_schema));
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::cast_row(int32_t session_id,
                                          const ObTableLoadObjRow &obj_row,
                                          const ObDirectLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  SessionContext &session_ctx = session_ctx_array_[session_id - 1];
  session_ctx.cast_allocator_.reuse();
  if (OB_FAIL(cast_row(session_ctx.cast_allocator_, session_ctx.cast_params_, obj_row,
                       session_ctx.datum_row_, session_id))) {
    LOG_WARN("fail to cast row", KR(ret));
  } else {
    datum_row = &session_ctx.datum_row_;
  }
  return ret;
}

int ObTableLoadTransStoreWriter::cast_column(
    ObArenaAllocator &cast_allocator,
    ObDataTypeCastParams cast_params,
    const ObColumnSchemaV2 *column_schema,
    const ObObj &obj,
    ObStorageDatum &datum,
    int32_t session_id)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx(&cast_allocator, &cast_params, cast_mode_, column_schema->get_collation_type());
  ObTableLoadCastObjCtx cast_obj_ctx(param_, &time_cvrt_, &cast_ctx, true);
  cast_ctx.exec_ctx_ = trans_ctx_->ctx_->exec_ctx_;
  ObObj out_obj;
  if (column_schema->is_autoincrement()) {
    // mysql模式还不支持快速删列, 先加个拦截
    if (OB_UNLIKELY(column_schema->is_unused())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected unused identity column", KR(ret), KPC(column_schema));
    } else if (obj.is_null() || obj.is_nop_value()) {
      out_obj = obj;
    } else if (OB_FAIL(ObTableLoadObjCaster::cast_obj(cast_obj_ctx,
                                                      column_schema,
                                                      obj,
                                                      out_obj))) {
      LOG_WARN("fail to cast obj", KR(ret), K(obj), KPC(column_schema));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(handle_autoinc_column(column_schema, out_obj, datum, session_id))) {
        LOG_WARN("fail to handle autoinc column", KR(ret), K(out_obj));
      }
    }
  } else if (column_schema->is_identity_column()) {
    // identity列在快速删除的时候会抹去identity属性
    if (OB_UNLIKELY(column_schema->is_unused())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected unused identity column", KR(ret), KPC(column_schema));
    } else if (column_schema->is_tbl_part_key_column()) {
      // 自增列是分区键, 在分区计算的时候就已经确定值了
      out_obj = obj;
    } else {
      // 生成的seq_value是number, 可能需要转换成decimal int
      ObObj tmp_obj;
      if (OB_FAIL(handle_identity_column(column_schema, obj, tmp_obj, cast_allocator))) {
        LOG_WARN("fail to handle identity column", KR(ret), K(obj));
      } else if (OB_FAIL(ObTableLoadObjCaster::cast_obj(cast_obj_ctx, column_schema, tmp_obj, out_obj))) {
        LOG_WARN("fail to cast obj and check", KR(ret), K(tmp_obj));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(datum.from_obj_enhance(out_obj))) {
        LOG_WARN("fail to from obj enhance", KR(ret), K(out_obj));
      }
    }
  } else {
    // 普通列
    if (OB_FAIL(ObTableLoadObjCaster::cast_obj(cast_obj_ctx, column_schema, obj, out_obj))) {
      LOG_WARN("fail to cast obj and check", KR(ret), K(obj));
    } else if (OB_FAIL(datum.from_obj_enhance(out_obj))) {
      LOG_WARN("fail to from obj enhance", KR(ret), K(out_obj));
    }
  }
  return ret;
}

int ObTableLoadTransStoreWriter::handle_autoinc_column(const ObColumnSchemaV2 *column_schema,
                                                       const ObObj &obj,
                                                       ObStorageDatum &datum,
                                                       int32_t session_id)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass &tc = column_schema->get_meta_type().get_type_class();
  if (OB_FAIL(datum.from_obj_enhance(obj))) {
    LOG_WARN("fail to from obj enhance", KR(ret), K(obj));
  } else if (OB_FAIL(ObTableLoadAutoincNextval::eval_nextval(
        &(store_ctx_->session_ctx_array_[session_id - 1].autoinc_param_), datum, tc,
        store_ctx_->ctx_->session_info_->get_sql_mode()))) {
    LOG_WARN("fail to get auto increment next value", KR(ret));
  }
  return ret;
}

int ObTableLoadTransStoreWriter::handle_identity_column(const ObColumnSchemaV2 *column_schema,
                                                        const ObObj &obj,
                                                        ObObj &out_obj,
                                                        ObArenaAllocator &cast_allocator)
{
  int ret = OB_SUCCESS;
  // 1. generated always as identity : 不能指定此列导入
  // 2. generated by default as identity : 不指定时自动生成, 不能导入null
  // 3. generated by default on null as identity : 不指定或者指定null会自动生成
  if (OB_UNLIKELY(column_schema->is_always_identity_column() && !obj.is_nop_value())) {
    ret = OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN;
    LOG_USER_ERROR(OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN);
  } else if (OB_UNLIKELY(column_schema->is_default_identity_column() && obj.is_null())) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("default identity column cannot insert null", KR(ret));
  } else {
    // 不论用户有没有指定自增列的值, 都取一个seq_value, 行为与insert into保持一致
    // 取seq_value的性能受表的参数cache影响
    ObSequenceValue seq_value;
    if (OB_FAIL(ObSequenceCache::get_instance().nextval(trans_ctx_->ctx_->store_ctx_->sequence_schema_,
                                                        cast_allocator,
                                                        seq_value))) {
      LOG_WARN("fail get nextval for seq", KR(ret));
    } else if (obj.is_nop_value() || obj.is_null()) {
      ObNumber number;
      if (OB_FAIL(number.from(seq_value.val(), cast_allocator))) {
        LOG_WARN("fail deep copy value", KR(ret), K(seq_value));
      } else {
        out_obj.set_number(number);
      }
    } else {
      out_obj = obj;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
