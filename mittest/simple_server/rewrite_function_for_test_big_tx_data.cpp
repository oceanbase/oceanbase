#include "storage/tx_table/ob_tx_table_iterator.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "share/ob_table_access_helper.h"
#include "lib/ob_define.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/compaction/ob_partition_parallel_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include <iostream>
int64_t TEST_TX_ID = 0;
bool DUMP_BIG_TX_DATA = false;
bool LOAD_BIG_TX_DATA = false;
bool READ_TEST_TX_FROM_SSTABLE = false;
int64_t BIGGEST_TX_DATA_SIZE = 0;

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
namespace storage
{
int ObTxData::add_undo_action(ObTxTable *tx_table,
                              transaction::ObUndoAction &new_undo_action,
                              ObUndoStatusNode *undo_node)
{
  // STORAGE_LOG(DEBUG, "do add_undo_action");
  UNUSED(undo_node);
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(undo_status_list_.lock_);
  ObTxDataTable *tx_data_table = nullptr;
  ObUndoStatusNode *node = undo_status_list_.head_;
  if (OB_ISNULL(tx_table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tx table is nullptr.", KR(ret));
  } else if (OB_ISNULL(tx_data_table = tx_table->get_tx_data_table())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data table in tx table is nullptr.", KR(ret));
  } else {
/**************************************************************************************************/
    // 在测试big tx data的时候，连续挂很多无效的undo action上去，并且disable掉merge的逻辑
    // 可以节省测试时间，否则构建big tx data的耗时太长，没办法加到farm里
    int loop_times = 10000;
    while (OB_SUCC(ret) && --loop_times) {
      ObUndoStatusNode *new_node = nullptr;
      if (OB_FAIL(tx_data_table->alloc_undo_status_node(new_node))) {
        STORAGE_LOG(WARN, "alloc_undo_status_node() fail", KR(ret));
      } else {
        new_node->next_ = node;
        undo_status_list_.head_ = new_node;
        node = new_node;
        undo_status_list_.undo_node_cnt_++;
      }
      for (int64_t idx = 0; idx < TX_DATA_UNDO_ACT_MAX_NUM_PER_NODE; ++idx) {
        node->undo_actions_[node->size_++] = new_undo_action;
      }
    }
/**************************************************************************************************/
  }
  return ret;
}

int ObTxDataMemtableScanIterator
    ::TxData2DatumRowConverter::generate_next_now(const blocksstable::ObDatumRow *&row)
{
  const int64_t SSTABLE_HIDDEN_COLUMN_CNT = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tx_data_)) {
    ret = OB_ITER_END;// no tx data remained
  /*****************[NEED REMOVE IN FUTURE]*****************/
  } else if (INT64_MAX == tx_data_->tx_id_.get_id() &&
             generate_size_ == 1) {
    ret = OB_ITER_END;// fake tx data datum row has been generated
  /*********************************************************/
  } else if (INT64_MAX != tx_data_->tx_id_.get_id() &&
             generate_size_ == std::ceil(buffer_len_ * 1.0 / common::OB_MAX_VARCHAR_LENGTH)) {
    ret = OB_ITER_END;// all tx data datum row has been generated
  } else {
    if (generate_size_ >= 1) {
      STORAGE_LOG(INFO, "meet big tx data", KR(ret), K(*this));
/**************************************************************************************************/
      if (buffer_len_ > ATOMIC_LOAD(&BIGGEST_TX_DATA_SIZE)) {
        // not exactly accurate, but enough for unittest
        ATOMIC_STORE(&BIGGEST_TX_DATA_SIZE, buffer_len_);
      }
      if (tx_data_->undo_status_list_.undo_node_cnt_ > 0) {
        std::cout << "tx_id:" << tx_data_->tx_id_.get_id() << ", undo cnt:" << tx_data_->undo_status_list_.undo_node_cnt_ << ", generate size:" << generate_size_ << std::endl;
      }
      ATOMIC_STORE(&DUMP_BIG_TX_DATA, true);
/**************************************************************************************************/
    }
    datum_row_.reset();
    new (&datum_row_) ObDatumRow();// CAUTIONS: this is needed, or will core dump
    if (OB_FAIL(datum_row_.init(DEFAULT_TX_DATA_ALLOCATOR,
                                TX_DATA_MAX_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT))) {
      STORAGE_LOG(ERROR, "fail to init datum row", KR(ret), K(*this));
    } else {
      datum_row_.row_flag_.set_flag(blocksstable::ObDmlFlag::DF_INSERT);
      datum_row_.storage_datums_[TX_DATA_ID_COLUMN].set_int(tx_data_->tx_id_.get_id());
      datum_row_.storage_datums_[TX_DATA_IDX_COLUMN].set_int(generate_size_);
      datum_row_.storage_datums_[TX_DATA_IDX_COLUMN + 1].set_int(-4096);// storage layer needed
      datum_row_.storage_datums_[TX_DATA_IDX_COLUMN + 2].set_int(0);// storage layer needed
      int64_t total_row_cnt_column = TX_DATA_TOTAL_ROW_CNT_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
      int64_t end_ts_column = TX_DATA_END_TS_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
      int64_t value_column = TX_DATA_VAL_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
      char *p_value_begin = serialize_buffer_ + common::OB_MAX_VARCHAR_LENGTH * generate_size_;
      generate_size_++;
      ObString value;
      /*****************[NEED REMOVE IN FUTURE]*****************/
      // TODO : remove this after the sstables do not need upper trans version
      if (INT64_MAX == tx_data_->tx_id_.get_id()) {
        // NOTE : this fake tx data is generated in
        // ObTxDataMemtable::pre_process_commit_version_row_
        datum_row_.storage_datums_[total_row_cnt_column].set_int(1);
        datum_row_.storage_datums_[end_ts_column].set_int(INT64_MAX);
        value.assign((char *)tx_data_->start_scn_.get_val_for_tx(), tx_data_->commit_version_.get_val_for_tx());
      /*********************************************************/
      } else {
        datum_row_.storage_datums_[total_row_cnt_column].set_int(std::ceil(buffer_len_ * 1.0 / common::OB_MAX_VARCHAR_LENGTH));
        datum_row_.storage_datums_[end_ts_column].set_int(tx_data_->end_scn_.get_val_for_tx());
        value.assign(p_value_begin,
                    std::min(common::OB_MAX_VARCHAR_LENGTH,
                              buffer_len_ - (p_value_begin - serialize_buffer_)));
      }
      datum_row_.storage_datums_[value_column].set_string(value);
      datum_row_.set_first_multi_version_row();// storage layer needed for compatibility
      datum_row_.set_last_multi_version_row();// storage layer needed for compatibility
      datum_row_.set_compacted_multi_version_row();// storage layer needed for compatibility
      row = &datum_row_;
    }
  }
  return ret;
}

int ObTxDataSingleRowGetter::deserialize_tx_data_from_store_buffers_(ObTxData &tx_data)
{
  int ret = OB_SUCCESS;
  int64_t total_buffer_size = 0;
  for (int64_t idx = 0; idx < tx_data_buffers_.count(); ++idx) {
    total_buffer_size += tx_data_buffers_[idx].get_ob_string().length();
  }
  OB_ASSERT(total_buffer_size != 0);
  char *merge_buffer = (char*)DEFAULT_TX_DATA_ALLOCATOR.alloc(total_buffer_size);
  int64_t pos = 0;
  if (OB_ISNULL(merge_buffer)) {
    STORAGE_LOG(ERROR, "fail to alloc merge buffer", KR(ret), K(total_buffer_size));
  } else {
    char *p_dest = merge_buffer;
    for (int64_t idx = 0; idx < tx_data_buffers_.count(); ++idx) {
      OB_ASSERT(p_dest + tx_data_buffers_[idx].get_ob_string().length() <= merge_buffer + total_buffer_size);
      memcpy(p_dest, tx_data_buffers_[idx].get_ob_string().ptr(), tx_data_buffers_[idx].get_ob_string().length());
      p_dest += tx_data_buffers_[idx].get_ob_string().length();
    }
    tx_data.tx_id_ = tx_id_;
    if (OB_FAIL(tx_data.deserialize(merge_buffer, total_buffer_size, pos, slice_allocator_))) {
      STORAGE_LOG(WARN, "deserialize tx data failed", KR(ret), KPHEX(merge_buffer, total_buffer_size));
      hex_dump(merge_buffer, total_buffer_size, true, OB_LOG_LEVEL_WARN);
    } else if (!tx_data.is_valid_in_tx_data_table()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "the deserialized tx data is invalid.", KR(ret), K(tx_data));
    }
/**************************************************************************************************/
    if (tx_data.tx_id_.get_id() == ATOMIC_LOAD(&TEST_TX_ID)) {
      if (tx_data_buffers_.count() > 1) {
        ATOMIC_STORE(&LOAD_BIG_TX_DATA, true);
        std::cout << "read big tx id from sstable, tx_id:" << ATOMIC_LOAD(&TEST_TX_ID) << ", undo cnt:" << tx_data.undo_status_list_.undo_node_cnt_ << ", buffer cnt:" << tx_data_buffers_.count() << std::endl;
      }
    }
/**************************************************************************************************/
  }
  if (OB_NOT_NULL(merge_buffer)) {
    DEFAULT_TX_DATA_ALLOCATOR.free(merge_buffer);
  }
  return ret;
}

int ObTxDataTable::check_with_tx_data(const ObTransID tx_id,
                                      ObITxDataCheckFunctor &fn,
                                      ObTxDataGuard &tx_data_guard,
                                      SCN &recycled_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init.", KR(ret), KP(this), K(tx_id));
  } else if (OB_SUCC(check_tx_data_in_memtable_(tx_id, fn, tx_data_guard))) {
    // successfully do check function in memtable, check done
    STORAGE_LOG(DEBUG, "tx data table check with tx memtable data succeed", K(tx_id), K(fn));
  } else if (OB_TRANS_CTX_NOT_EXIST == ret && OB_SUCC(check_tx_data_in_sstable_(tx_id, fn, tx_data_guard, recycled_scn))) {
/**************************************************************************************************/
    if (tx_id.get_id() == ATOMIC_LOAD(&TEST_TX_ID)) {
      ATOMIC_STORE(&READ_TEST_TX_FROM_SSTABLE, true);
    }
/**************************************************************************************************/
    // successfully do check function in sstable
    STORAGE_LOG(DEBUG, "tx data table check with tx sstable data succeed", K(tx_id), K(fn));
  } else {
    STORAGE_LOG(WARN, "check something in tx data fail.", KR(ret), K(tx_id), KP(this),
                K(tablet_id_));
  }

  return ret;
}

int ObParallelMergeCtx::init_parallel_mini_merge(compaction::ObTabletMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(MINI_MERGE != merge_ctx.param_.merge_type_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel mini merge", K(ret), K(merge_ctx));
  } else {
    const int64_t tablet_size = merge_ctx.get_schema()->get_tablet_size();
    memtable::ObIMemtable *memtable = nullptr;
    if (OB_FAIL(merge_ctx.tables_handle_.get_first_memtable(memtable))) {
      STORAGE_LOG(WARN, "failed to get first memtable", K(ret),
                  "merge tables", merge_ctx.tables_handle_);
    } else {
      int64_t total_bytes = 0;
      int64_t total_rows = 0;
      int64_t mini_merge_thread = 0;
      if (OB_FAIL(memtable->estimate_phy_size(nullptr, nullptr, total_bytes, total_rows))) {
        STORAGE_LOG(WARN, "Failed to get estimate size from memtable", K(ret));
      } else if (MTL(ObTenantDagScheduler *)->get_up_limit(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, mini_merge_thread)) {
        STORAGE_LOG(WARN, "failed to get uplimit", K(ret), K(mini_merge_thread));
      } else {
        ObArray<ObStoreRange> store_ranges;
        mini_merge_thread = MAX(mini_merge_thread, PARALLEL_MERGE_TARGET_TASK_CNT);
/**************************************************************************************************/
        concurrent_cnt_ = 2;
        // concurrent_cnt_ = MIN((total_bytes + tablet_size - 1) / tablet_size, mini_merge_thread);
/**************************************************************************************************/
        if (concurrent_cnt_ <= 1) {
          if (OB_FAIL(init_serial_merge())) {
            STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
          }
        } else if (OB_FAIL(memtable->get_split_ranges(nullptr, nullptr, concurrent_cnt_, store_ranges))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            if (OB_FAIL(init_serial_merge())) {
              STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
            }
          } else {
            STORAGE_LOG(WARN, "Failed to get split ranges from memtable", K(ret));
          }
        } else if (OB_UNLIKELY(store_ranges.count() != concurrent_cnt_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected range array and concurrent_cnt", K(ret), K_(concurrent_cnt),
                      K(store_ranges));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < store_ranges.count(); i++) {
            ObDatumRange datum_range;
            if (OB_FAIL(datum_range.from_range(store_ranges.at(i), allocator_))) {
              STORAGE_LOG(WARN, "Failed to transfer store range to datum range", K(ret), K(i), K(store_ranges.at(i)));
            } else if (OB_FAIL(range_array_.push_back(datum_range))) {
              STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(datum_range));
            }
          }
          parallel_type_ = PARALLEL_MINI;
          STORAGE_LOG(INFO, "Succ to get parallel mini merge ranges", K_(concurrent_cnt), K_(range_array));
        }
      }
    }
  }

  return ret;
}

int ObParallelMergeCtx::init(compaction::ObTabletMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObParallelMergeCtx init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == merge_ctx.get_schema() || merge_ctx.tables_handle_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel merge", K(ret), K(merge_ctx));
  } else {
    int64_t tablet_size = merge_ctx.get_schema()->get_tablet_size();
/**************************************************************************************************/
    bool enable_parallel_minor_merge = true;
    // bool enable_parallel_minor_merge = false;
    // {
    //   omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    //   if (tenant_config.is_valid()) {
    //     enable_parallel_minor_merge = tenant_config->_enable_parallel_minor_merge;
    //   }
    // }
/**************************************************************************************************/
    if (enable_parallel_minor_merge && tablet_size > 0 && is_mini_merge(merge_ctx.param_.merge_type_)) {
      if (OB_FAIL(init_parallel_mini_merge(merge_ctx))) {
        STORAGE_LOG(WARN, "Failed to init parallel setting for mini merge", K(ret));
      }
    } else if (enable_parallel_minor_merge && tablet_size > 0 && is_minor_merge(merge_ctx.param_.merge_type_)) {
      if (OB_FAIL(init_parallel_mini_minor_merge(merge_ctx))) {
        STORAGE_LOG(WARN, "Failed to init parallel setting for mini minor merge", K(ret));
      }
    } else if (tablet_size > 0 && is_major_merge_type(merge_ctx.param_.merge_type_)) {
      if (OB_FAIL(init_parallel_major_merge(merge_ctx))) {
        STORAGE_LOG(WARN, "Failed to init parallel major merge", K(ret));
      }
    } else if (OB_FAIL(init_serial_merge())) {
      STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      STORAGE_LOG(INFO, "Succ to init parallel merge ctx",
          K(enable_parallel_minor_merge), K(tablet_size), K(merge_ctx.param_));
    }
  }

  return ret;
}

}
}
