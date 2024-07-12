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

#include <stdint.h>
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/multi_data_source/adapter_define/mds_dump_kv_wrapper.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"
#include "storage/blocksstable/ob_block_manager.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace storage
{

ObMdsMergeMultiVersionRowStore::ObMdsMergeMultiVersionRowStore()
  : data_store_desc_(nullptr),
    macro_writer_(nullptr),
    row_queue_allocator_(common::ObMemAttr(MTL_ID(), "MdsMVRowStore")),
    shadow_row_(),
    cur_key_(),
    last_key_(),
    row_queue_(),
    is_inited_(false)
{
}

int ObMdsMergeMultiVersionRowStore::init(const ObDataStoreDesc &data_store_desc, blocksstable::ObMacroBlockWriter &macro_writer)
{
  int ret = OB_SUCCESS;
  const int64_t row_column_cnt = data_store_desc.get_row_column_count();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(shadow_row_.init(row_column_cnt))) {
    LOG_WARN("fail to init datum row", K(ret), K(row_column_cnt));
  } else if (OB_FAIL(row_queue_.init(row_column_cnt))) {
    LOG_WARN("fail to init row queue", K(ret), K(row_column_cnt));
  } else {
    data_store_desc_ = &data_store_desc;
    macro_writer_ = &macro_writer;
    is_inited_ = true;
    LOG_DEBUG("succeed to init mds mini helper", K(ret), KPC(data_store_desc_));
  }
  return ret;
}

int ObMdsMergeMultiVersionRowStore::finish()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(row_queue_.is_empty())) {
    ret = OB_EMPTY_RESULT;
    LOG_WARN("unexpected row queue is empty, which means no data come in", K(ret));
  } else if (OB_FAIL(dump_row_queue())) {
    LOG_WARN("fail to dump row queue", K(ret), K(row_queue_));
  } else {
    LOG_DEBUG("succeed to finish operator", K(ret));
  }
  return ret;
}

int ObMdsMergeMultiVersionRowStore::put_row_into_queue(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (row_queue_.is_empty()) {
    if (OB_FAIL(row_queue_.add_row(row, row_queue_allocator_))) {
      LOG_WARN("fail to add row to row_queue", K(ret), K(row), K(row_queue_));
    }
  } else {
    cur_key_.reset();
    last_key_.reset();
    int32_t compare_result = 0;
    const ObDatumRow *last_row_in_qu = row_queue_.get_last();
    if (OB_ISNULL(last_row_in_qu)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected last row is nullptr", K(ret), K(row_queue_));
    } else if (OB_FAIL(last_key_.assign(last_row_in_qu->storage_datums_, data_store_desc_->get_schema_rowkey_col_cnt()))) {
      LOG_WARN("Failed to assign qu rowkey", K(ret));
    } else if (OB_FAIL(cur_key_.assign(row.storage_datums_, data_store_desc_->get_schema_rowkey_col_cnt()))) {
      LOG_WARN("Failed to assign cur key", K(ret));
    } else if (OB_FAIL(cur_key_.compare(last_key_, data_store_desc_->get_datum_utils(), compare_result))) {
      LOG_WARN("Failed to compare last key", K(ret), K(cur_key_), K(last_key_));
    } else if (OB_UNLIKELY(compare_result < 0)) {
      ret = OB_ROWKEY_ORDER_ERROR;
      LOG_ERROR("input rowkey is less then last rowkey", K(ret), K(cur_key_), K(last_key_), K(ret));
    } else if (compare_result == 0) {
      if (OB_FAIL(put_same_rowkey_row_into_queue(row, *last_row_in_qu))) {
        LOG_WARN("Failed to put same rowkey into queue", K(ret), K(row), KPC(last_row_in_qu));
      }
    } else {
      // put another row key, dump current row queue
      if (OB_FAIL(dump_row_queue())) {
        LOG_WARN("Failed to dump row queue", K(ret), K(row_queue_));
      } else if (OB_FAIL(row_queue_.add_row(row, row_queue_allocator_))) {
        LOG_WARN("fail to add row to row_queue", K(row), K(row_queue_));
      }
    }
  }

  return ret;
}

int ObMdsMergeMultiVersionRowStore::put_same_rowkey_row_into_queue(const ObDatumRow &row, const ObDatumRow &last_row_in_qu)
{
  int ret = OB_SUCCESS;
  const int64_t qu_trans = last_row_in_qu.storage_datums_[ObMdsSchemaHelper::SNAPSHOT_IDX].get_int();
  const int64_t qu_sql_no = last_row_in_qu.storage_datums_[ObMdsSchemaHelper::SEQ_NO_IDX].get_int();
  const int64_t cur_trans = row.storage_datums_[ObMdsSchemaHelper::SNAPSHOT_IDX].get_int();
  const int64_t cur_sql_no = row.storage_datums_[ObMdsSchemaHelper::SEQ_NO_IDX].get_int();
  if (qu_trans > cur_trans) {
    ret = OB_ROWKEY_ORDER_ERROR;
    LOG_ERROR("unexpected to check order", K(ret), K(cur_trans), K(qu_trans), K(row), K(last_row_in_qu));
  } else if (qu_trans == cur_trans) {
    if (OB_UNLIKELY(qu_sql_no >= cur_sql_no)) {
      ret = OB_ROWKEY_ORDER_ERROR;
      LOG_ERROR("unexpected to check order", K(ret), K(cur_sql_no), K(qu_sql_no), K(row), K(last_row_in_qu));
    } else {
      // do no thing, mds row is compact row, only need to store smaller sql no (not fresh).
    }
  } else {
    // another trans version rowkey.
    if (OB_FAIL(row_queue_.add_row(row, row_queue_allocator_))) {
      LOG_WARN("fail to add row to row_queue", K(ret), K(row), K(row_queue_));
    }
  }
  return ret;
}

int ObMdsMergeMultiVersionRowStore::dump_row_queue()
{
  int ret = OB_SUCCESS;
  if (row_queue_.is_empty()) {
    //do nothing
  } else if (1 == row_queue_.count()) {
    ObDatumRow * last_row_in_qu = row_queue_.get_last();
    if (OB_ISNULL(last_row_in_qu)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected last row is nullptr", K(ret), K(row_queue_));
    } else {
      last_row_in_qu->set_first_multi_version_row();
      last_row_in_qu->set_last_multi_version_row();
      last_row_in_qu->set_compacted_multi_version_row();
      last_row_in_qu->storage_datums_[ObMdsSchemaHelper::SEQ_NO_IDX].set_int(0);
      if (OB_FAIL(macro_writer_->append_row(*last_row_in_qu))) {
        LOG_WARN("fail to append row", K(ret), KPC(last_row_in_qu), KPC(macro_writer_));
      } else {
        LOG_DEBUG("succeed to append mds row", K(ret), KPC(last_row_in_qu));
      }
    }
  } else {
    if (OB_FAIL(dump_shadow_row())){
      LOG_WARN("Failed to dump mds shadow row", K(ret));
    } else {
      const ObDatumRow *row = nullptr;
      ObDatumRow *dump_row = nullptr;
      while (OB_SUCC(ret) && row_queue_.has_next()) {
        if (OB_FAIL(row_queue_.get_next_row(row))) {
          LOG_WARN("Failed to get row from row queue", K(ret), K(row_queue_));
        } else {
          dump_row = const_cast<ObDatumRow *> (row);
          dump_row->storage_datums_[ObMdsSchemaHelper::SEQ_NO_IDX].set_int(0);
          dump_row->set_compacted_multi_version_row();
          if (!row_queue_.has_next()) {
            dump_row->set_last_multi_version_row();
          }
          if (OB_FAIL(macro_writer_->append_row(*dump_row))) {
            LOG_WARN("fail to append row", K(ret), KPC(dump_row), KPC(macro_writer_));
          } else {
            LOG_DEBUG("succeed to append mds row", K(ret), KPC(dump_row));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    row_queue_.print_rows();
  } else {
    row_queue_.reuse();
    row_queue_allocator_.reuse();
  }
  return ret;
}

int ObMdsMergeMultiVersionRowStore::dump_shadow_row()
{
  int ret = OB_SUCCESS;
  shadow_row_.reuse();
  ObDatumRow * first_row_in_qu = row_queue_.get_first();
  if (OB_ISNULL(first_row_in_qu)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected last row is nullptr", K(ret), K(row_queue_));
  } else if (OB_FAIL(shadow_row_.deep_copy((*first_row_in_qu), row_queue_allocator_))) {
    LOG_WARN("Failed to deep copy datum row", K(ret), KPC(first_row_in_qu));
  } else {
    shadow_row_.set_first_multi_version_row();
    shadow_row_.set_shadow_row();
    shadow_row_.set_compacted_multi_version_row();
    shadow_row_.storage_datums_[ObMdsSchemaHelper::SEQ_NO_IDX].set_int(-INT64_MAX);
    if (OB_FAIL(macro_writer_->append_row(shadow_row_))) {
      LOG_WARN("fail to append row", K(ret), K(shadow_row_), KPC(macro_writer_));
    } else {
      LOG_DEBUG("succeed to append mds shadow row", K(ret), K(shadow_row_));
    }
  }
  return ret;
}

ObMdsMiniMergeOperator::ObMdsMiniMergeOperator()
  : is_inited_(false),
    row_store_(),
    cur_allocator_(common::ObMemAttr(MTL_ID(), "MdsMiniOP")),
    cur_row_()
{
}

int ObMdsMiniMergeOperator::init(
    const ObDataStoreDesc &data_store_desc,
    blocksstable::ObMacroBlockWriter &macro_writer)
{
  int ret = OB_SUCCESS;
  const int64_t row_column_cnt = data_store_desc.get_row_column_count();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!data_store_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data store desc", K(ret), K(data_store_desc));
  } else if (OB_FAIL(row_store_.init(data_store_desc, macro_writer))) {
    LOG_WARN("fail to init mds merge helper", K(ret), K(data_store_desc));
  } else if (OB_FAIL(cur_row_.init(row_column_cnt))) {
    LOG_WARN("fail to init datum row", K(ret), K(row_column_cnt));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTabletDumpMds2MiniOperator::operator()(const mds::MdsDumpKV &kv)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!kv.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dump kv is invalid", K(ret), K(kv));
  } else {
    cur_row_.reuse();
    cur_allocator_.reuse();
    mds::MdsDumpKVStorageAdapter adapter(kv);
    if (OB_FAIL(adapter.convert_to_mds_row(cur_allocator_, cur_row_))) {
      LOG_WARN("fail to convert MdsDumpKVStorageAdapter to row", K(ret), K(adapter), K(cur_row_));
    } else if (OB_FAIL(row_store_.put_row_into_queue(cur_row_))) {
      LOG_WARN("fail to put row into queue", K(ret));
    } else {
      LOG_INFO("mds op succeed to add row", K(ret), K(adapter), K(cur_row_));
    }
  }

  return ret;
}

ObCrossLSMdsMiniMergeOperator::ObCrossLSMdsMiniMergeOperator(const share::SCN &scan_end_scn)
  : ObMdsMiniMergeOperator(),
    scan_end_scn_(scan_end_scn)
{
}

int ObCrossLSMdsMiniMergeOperator::operator()(const mds::MdsDumpKV &kv)
{
  int ret = OB_SUCCESS;
  constexpr uint8_t tablet_status_mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable,
                                                                  mds::MdsUnit<mds::DummyKey,
                                                                               ObTabletCreateDeleteMdsUserData>>::value;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (tablet_status_mds_unit_id == kv.k_.mds_unit_id_) {
    // filter tablet status mds kv
    LOG_INFO("meet tablet status mds row, should skip", K(ret), K(tablet_status_mds_unit_id));
  } else if (kv.v_.end_scn_ > scan_end_scn_) {
    LOG_INFO("node end scn is beyond scan end scn, should skip", K(ret), K_(scan_end_scn), K(kv));
  } else {
    cur_row_.reuse();
    cur_allocator_.reuse();
    mds::MdsDumpKVStorageAdapter adapter(kv);
    if (OB_FAIL(adapter.convert_to_mds_row(cur_allocator_, cur_row_))) {
      LOG_WARN("fail to convert MdsDumpKVStorageAdapter to row", K(ret), K(adapter), K(cur_row_));
    } else if (OB_FAIL(row_store_.put_row_into_queue(cur_row_))) {
      LOG_WARN("fail to put row into queue", K(ret));
    } else {
      LOG_INFO("cross ls mds op succeed to add row", K(ret), K(adapter), K(cur_row_));
    }
  }

  return ret;
}

/*
------------------------------------------ObMdsTableMiniMerger-----------------------------------
*/
ObMdsTableMiniMerger::ObMdsTableMiniMerger()
  : allocator_(common::ObMemAttr(MTL_ID(), "MdsMiniMerger")),
    data_desc_(),
    macro_writer_(),
    sstable_builder_(),
    ctx_(nullptr),
    storage_schema_(nullptr),
    is_inited_(false)
{
}

void ObMdsTableMiniMerger::reset()
{
  allocator_.reset();
  data_desc_.reset();
  macro_writer_.reset();
  sstable_builder_.reset();
  ctx_ = nullptr;
  storage_schema_ = nullptr;
  is_inited_ = false;
}

int ObMdsTableMiniMerger::init(compaction::ObTabletMergeCtx &ctx, ObMdsMiniMergeOperator &op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    MDS_LOG(WARN, "init twice", K(ret));
  } else {
    const share::ObLSID &ls_id = ctx.get_ls_id();
    const common::ObTabletID &tablet_id = ctx.get_tablet_id();
    const ObStorageSchema *storage_schema = ObMdsSchemaHelper::get_instance().get_storage_schema();
    uint64_t data_version = 0;
    ObMacroDataSeq macro_start_seq(0);
    if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("fail to get data version", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(storage_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage schema is null", K(ret), KP(storage_schema));
    } else if (OB_UNLIKELY(!storage_schema->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mds storage schema is invalid", K(ret), KP(storage_schema), KPC(storage_schema));
    } else if (OB_FAIL(data_desc_.init(*storage_schema, ls_id, tablet_id,
        ctx.get_merge_type(), ctx.get_snapshot(), data_version,
        ctx.static_param_.scn_range_.end_scn_))) {
      LOG_WARN("fail to init whole desc", KR(ret), K(ctx), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(macro_start_seq.set_parallel_degree(0))) {
      LOG_WARN("Failed to set parallel degree to macro start seq", K(ret));
    } else if (OB_FAIL(macro_start_seq.set_sstable_seq(ctx.static_param_.sstable_logic_seq_))) {
      LOG_WARN("Failed to set sstable seq", K(ret), K(ctx.static_param_.sstable_logic_seq_));
    } else if (FALSE_IT(data_desc_.get_desc().sstable_index_builder_ = &sstable_builder_)) {
    } else if (OB_FAIL(sstable_builder_.init(data_desc_.get_desc()))) {
      LOG_WARN("Failed to init sstable builder", K(ret), K(data_desc_.get_desc()));
    } else if (OB_FAIL(macro_writer_.open(data_desc_.get_desc(), macro_start_seq))) {
      LOG_WARN("Failed to open macro block writer", K(ret));
    } else if (OB_FAIL(op.init(data_desc_.get_desc(), macro_writer_))) {
      LOG_WARN("fail to init op", K(ret), "row column count", data_desc_.get_desc().get_row_column_count());
    } else {
      ctx_ = &ctx;
      storage_schema_ = storage_schema;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObMdsTableMiniMerger::generate_mds_mini_sstable(
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    SMART_VARS_2((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, param)) {
      if (OB_FAIL(macro_writer_.close())) {
        LOG_WARN("fail to close macro writer", K(ret), K(macro_writer_));
      } else if (OB_FAIL(sstable_builder_.close(res))) {
        LOG_WARN("fail to close sstable builder", K(ret), K(sstable_builder_));
      } else if (OB_FAIL(param.init_for_mds(*ctx_, res, *storage_schema_))) {
        LOG_WARN("fail to create sstable param for mds", K(ret));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, table_handle))) {
        LOG_WARN("fail to create sstable", K(ret), K(param));
        CTX_SET_DIAGNOSE_LOCATION(*ctx_);
      } else {
        // need macro block count for try schedule mds minor after mds mini
        ctx_->get_merge_info().get_sstable_merge_info().macro_block_count_ = res.data_blocks_cnt_;
      }
    }
  }
  if (OB_FAIL(ret)) {
    FLOG_WARN("fail to generate mds mini sstable", K(ret));
  } else {
    const share::ObLSID &ls_id = ctx_->get_ls_id();
    const common::ObTabletID &tablet_id = ctx_->get_tablet_id();
    const blocksstable::ObSSTable *sstable = static_cast<blocksstable::ObSSTable*>(table_handle.get_table());
    FLOG_INFO("succeed to generate mds mini sstable", K(ret), K(ls_id), K(tablet_id), KPC(sstable));
  }
  return ret;
}

int ObMdsDataCompatHelper::generate_mds_mini_sstable(
    const ObMigrationTabletParam &mig_param,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  TIMEGUARD_INIT(STORAGE, 10_ms);
  compaction::ObTabletMergeDagParam param;
  const share::ObLSID &ls_id = mig_param.ls_id_;
  const common::ObTabletID &tablet_id = mig_param.tablet_id_;
  compaction::ObTabletMergeCtx *ctx = nullptr;
  void *buf = nullptr;
  if (OB_UNLIKELY(!mig_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid migration tablet param", K(ret), K(mig_param));
  } else if (OB_ISNULL((buf = allocator.alloc(sizeof(compaction::ObTabletMergeCtx))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTabletMergeCtx", K(ret), K(allocator));
  } else {
    // (start_scn, enc_scn] set (1, tablet_mds_checkpoint_scn], which means contains all mds data.
    // After we set in this way, next mini merge, next mds sstable start scn will be tablet_mds_checkpoint_scn, sstables will be continuous.
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet_id;
    param.merge_type_ = compaction::ObMergeType::MDS_MINI_MERGE;
    param.merge_version_ = 0;
    ctx = new (buf) compaction::ObTabletMergeCtx(param, allocator);
    ctx->static_param_.start_time_ = common::ObTimeUtility::fast_current_time();
    ctx->static_param_.scn_range_.start_scn_ = share::SCN::plus(share::SCN::min_scn(), 1);
    ctx->static_param_.scn_range_.end_scn_ = mig_param.mds_checkpoint_scn_;
    ctx->static_param_.version_range_.snapshot_version_ = mig_param.mds_checkpoint_scn_.get_val_for_tx();
  }

  if (OB_FAIL(ret)) {
  } else {
    SMART_VARS_2((ObMdsTableMiniMerger, mds_mini_merger), (ObTabletDumpMds2MiniOperator, op)) {
      if (OB_FAIL(mds_mini_merger.init(*ctx, op))) {
        LOG_WARN("fail to init mds mini merger", K(ret), KPC(ctx), K(ls_id), K(tablet_id));
      } else if (CLICK_FAIL((mig_param.mds_data_.scan_all_mds_data_with_op(op)))) {
        LOG_WARN("failed to handle full memory mds data", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(mds_mini_merger.generate_mds_mini_sstable(allocator, table_handle))) {
        LOG_WARN("fail to generate mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
      }
    }
  }

  // always destruct merge ctx
  if (OB_NOT_NULL(ctx)) {
    ctx->~ObTabletMergeCtx();
  }
  return ret;
}

int ObMdsDataCompatHelper::generate_mds_mini_sstable(
    const ObTablet &tablet,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  TIMEGUARD_INIT(STORAGE, 10_ms);
  compaction::ObTabletMergeDagParam param;
  const share::ObLSID &ls_id = tablet.get_ls_id();
  const common::ObTabletID &tablet_id = tablet.get_tablet_id();
  void *buf = nullptr;
  if (tablet.is_ls_inner_tablet()) {
    ret = OB_NO_NEED_UPDATE;
    LOG_INFO("no need to generate mds sstable for ls inner tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(compaction::ObTabletMergeCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTabletMergeCtx", K(ret), K(allocator));
  } else {
    ObTabletFullMemoryMdsData data;
    // (start_scn, enc_scn] set (1, tablet_mds_checkpoint_scn], which means contains all mds data.
    // After we set in this way, next mini merge, next mds sstable start scn will be tablet_mds_checkpoint_scn, sstables will be continuous.
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet_id;
    param.merge_type_ = compaction::ObMergeType::MDS_MINI_MERGE;
    param.merge_version_ = 0;
    compaction::ObTabletMergeCtx *ctx = new (buf) compaction::ObTabletMergeCtx(param, allocator);
    ctx->static_param_.start_time_ = common::ObTimeUtility::fast_current_time();
    ctx->static_param_.scn_range_.start_scn_ = share::SCN::plus(share::SCN::min_scn(), 1);
    ctx->static_param_.scn_range_.end_scn_ = tablet.get_mds_checkpoint_scn();
    ctx->static_param_.version_range_.snapshot_version_ = tablet.get_mds_checkpoint_scn().get_val_for_tx();

    if (CLICK_FAIL(tablet.build_full_memory_mds_data(allocator, data))) {
      LOG_WARN("fail to build full memory mds data", K(ret));
    } else {
      SMART_VARS_2((ObMdsTableMiniMerger, mds_mini_merger), (ObTabletDumpMds2MiniOperator, op)) {
        if (CLICK_FAIL(mds_mini_merger.init(*ctx, op))) {
          LOG_WARN("fail to init mds mini merger", K(ret), KPC(ctx), K(ls_id), K(tablet_id));
        } else if (CLICK_FAIL((data.scan_all_mds_data_with_op(op)))) {
          LOG_WARN("failed to handle full memory mds data", K(ret), K(ls_id), K(tablet_id));
        } else if (CLICK_FAIL(mds_mini_merger.generate_mds_mini_sstable(allocator, table_handle))) {
          LOG_WARN("fail to generate mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
        } else {
          LOG_INFO("succeed to generate mds mini sstable for compat", K(ret), K(ls_id), K(tablet_id), K(data));
        }
      }
    }

    // always destruct merge ctx
    if (OB_NOT_NULL(ctx)) {
      ctx->~ObTabletMergeCtx();
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
