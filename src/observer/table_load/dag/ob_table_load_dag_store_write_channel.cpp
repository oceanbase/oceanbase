/**
 * Copyright (c) 2025 OceanBase
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

#include "observer/table_load/dag/ob_table_load_dag_store_write_channel.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_store_trans.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "observer/table_load/plan/ob_table_load_write_op.h"
#include "storage/direct_load/ob_direct_load_batch_rows.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace storage;

/**
 * ObTableLoadDagStoreWriteChannel
 */

ObTableLoadDagStoreWriteChannel::ObTableLoadDagStoreWriteChannel() : op_(nullptr) {}

int ObTableLoadDagStoreWriteChannel::init(ObTableLoadDag *dag, ObTableLoadStoreWriteOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDagStoreWriteChannel init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == dag || nullptr == op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(dag), KP(op));
  } else {
    store_ctx_ = dag->store_ctx_;
    dag_ = dag;
    op_ = op;
    if (OB_FAIL(inner_init())) {
      LOG_WARN("fail to init", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDagStoreWriteChannel::create_writer(ObTableLoadDagChunkWriter *&writer,
                                                   ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(writer = OB_NEWx(ObTableLoadDagStoreChunkWriter, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadDagStoreChunkWriter", KR(ret));
  }
  return ret;
}

int ObTableLoadDagStoreWriteChannel::do_close()
{
  int ret = OB_SUCCESS;
  ObDirectLoadTableStore &table_store = op_->op_ctx_->table_store_;
  if (OB_FAIL(store_ctx_->get_table_store_for_store_write(table_store))) {
    LOG_WARN("fail to get table store", KR(ret));
  }
  return ret;
}

/**
 * ObTableLoadDagStoreChunkWriter
 */

ObTableLoadDagStoreChunkWriter::ObTableLoadDagStoreChunkWriter()
  : store_ctx_(nullptr),
    write_channel_(nullptr),
    table_builder_allocator_("TLD_TblBuilder"),
    is_single_part_(false),
    is_closed_(false)
{
  table_builder_allocator_.set_tenant_id(MTL_ID());
  table_builders_.set_attr(ObMemAttr(MTL_ID(), "TLD_TblBuilder"));
}

ObTableLoadDagStoreChunkWriter::~ObTableLoadDagStoreChunkWriter()
{
  for (int64_t i = 0; i < table_builders_.count(); ++i) {
    ObIDirectLoadPartitionTableBuilder *table_builder = table_builders_.at(i);
    table_builder->~ObIDirectLoadPartitionTableBuilder();
    table_builder_allocator_.free(table_builder);
  }
  table_builders_.reset();
  table_builder_map_.destroy();
}

int ObTableLoadDagStoreChunkWriter::init(ObTableLoadDagWriteChannel *write_channel,
                                         ObTableLoadStoreTrans *trans,
                                         ObTableLoadTransStoreWriter *store_writer,
                                         const int32_t session_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDagStoreChunkWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == write_channel || nullptr == trans || nullptr == store_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(write_channel), KP(trans), KP(store_writer));
  } else {
    write_channel_ = static_cast<ObTableLoadDagStoreWriteChannel *>(write_channel);
    dag_ = write_channel->dag_;
    trans_ = trans;
    store_writer_ = store_writer;
    session_id_ = session_id;
    store_ctx_ = write_channel->store_ctx_;
    is_single_part_ =
      (store_ctx_->write_ctx_.is_multiple_mode_ ||
       1 == write_channel_->op_->op_ctx_->store_table_ctx_->ls_partition_ids_.count());
    if (OB_FAIL(table_builder_map_.create(64, "TLD_TB_Map", "TLD_TB_Map", MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret));
    } else if (OB_FAIL(datum_row_.init(
                 write_channel_->op_->op_ctx_->table_store_.get_table_data_desc().column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDagStoreChunkWriter::new_table_builder(
  const ObTabletID &tablet_id, ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  const ObDirectLoadTableDataDesc &table_data_desc =
    write_channel_->op_->op_ctx_->table_store_.get_table_data_desc();
  if (store_ctx_->write_ctx_.is_multiple_mode_) {
    // 排序路径
    ObDirectLoadExternalMultiPartitionTableBuildParam param;
    param.table_data_desc_ = table_data_desc;
    param.file_mgr_ = store_ctx_->tmp_file_mgr_;
    param.extra_buf_ = reinterpret_cast<char *>(1); // unuse, delete in future
    param.extra_buf_size_ = 4096;
    ObDirectLoadExternalMultiPartitionTableBuilder *me_table_builder = nullptr;
    if (OB_ISNULL(table_builder = me_table_builder = OB_NEWx(
                    ObDirectLoadExternalMultiPartitionTableBuilder, &table_builder_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadExternalMultiPartitionTableBuilder", KR(ret));
    } else if (OB_FAIL(me_table_builder->init(param))) {
      LOG_WARN("fail to init table builder", KR(ret));
    }
  } else {
    // 有主键表不排序路径
    ObDirectLoadMultipleSSTableBuildParam param;
    param.tablet_id_ = tablet_id;
    param.table_data_desc_ = table_data_desc;
    param.datum_utils_ = &(write_channel_->op_->op_ctx_->store_table_ctx_->schema_->datum_utils_);
    param.file_mgr_ = store_ctx_->tmp_file_mgr_;
    param.extra_buf_ = reinterpret_cast<char *>(1); // unuse, delete in future
    param.extra_buf_size_ = 4096;
    ObDirectLoadMultipleSSTableBuilder *ms_table_builder = nullptr;
    if (OB_ISNULL(table_builder = ms_table_builder =
                    OB_NEWx(ObDirectLoadMultipleSSTableBuilder, &table_builder_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTableBuilder", KR(ret));
    } else if (OB_FAIL(ms_table_builder->init(param))) {
      LOG_WARN("fail to init table builder", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_builder) {
      table_builder->~ObIDirectLoadPartitionTableBuilder();
      table_builder_allocator_.free(table_builder);
      table_builder = nullptr;
    }
  }
  return ret;
}

int ObTableLoadDagStoreChunkWriter::get_table_builder(
  const ObTabletID &tablet_id, ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
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
        table_builder_allocator_.free(table_builder);
        table_builder = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadDagStoreChunkWriter::inner_append_row(const ObTabletID &tablet_id,
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
    if (OB_LIKELY(OB_ERR_PRIMARY_KEY_DUPLICATE == ret)) {
      ObDirectLoadDMLRowHandler *dml_row_handler = write_channel_->op_->op_ctx_->dml_row_handler_;
      if (OB_FAIL(dml_row_handler->handle_update_row(tablet_id, datum_row))) {
        LOG_WARN("fail to handle update row", KR(ret), K(datum_row));
      }
    } else if (OB_LIKELY(OB_ROWKEY_ORDER_ERROR == ret)) {
      ObTableLoadErrorRowHandler *error_row_handler = store_ctx_->error_row_handler_;
      LOG_INFO("rowkey order error", K(tablet_id), K(datum_row));
      if (OB_FAIL(error_row_handler->handle_error_row(ret))) {
        LOG_WARN("fail to handle error row", KR(ret), K(tablet_id), K(datum_row));
      }
    }
  }
  return ret;
}

int ObTableLoadDagStoreChunkWriter::append_row(const ObTabletID &tablet_id,
                                               const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagStoreChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else if (OB_FAIL(inner_append_row(tablet_id, datum_row))) {
    LOG_WARN("fail to append row", KR(ret));
  }
  return ret;
}

int ObTableLoadDagStoreChunkWriter::append_batch(ObIVector *tablet_id_vector,
                                                 const ObDirectLoadBatchRows &batch_rows,
                                                 int64_t &start)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagStoreChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else if (OB_UNLIKELY(nullptr == tablet_id_vector || batch_rows.empty() || 0 != start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(tablet_id_vector), K(batch_rows), K(start));
  } else {
    datum_row_.seq_no_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.size(); ++i) {
      const ObTabletID tablet_id = ObDirectLoadVectorUtils::get_tablet_id(tablet_id_vector, i);
      if (OB_FAIL(batch_rows.get_datum_row(i, datum_row_))) {
        LOG_WARN("fail to get datum row", KR(ret), K(i));
      } else if (OB_FAIL(inner_append_row(tablet_id, datum_row_))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      start = batch_rows.size();
    }
  }
  return ret;
}

int ObTableLoadDagStoreChunkWriter::close(ObTableLoadStoreTrans *trans, const int32_t session_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagStoreChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else {
    ObTableLoadTransStore::SessionStore *session_store =
      trans->get_trans_store()->session_store_array_.at(session_id - 1);
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

} // namespace observer
} // namespace oceanbase
