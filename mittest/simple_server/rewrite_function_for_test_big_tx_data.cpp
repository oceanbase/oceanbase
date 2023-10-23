/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

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
int64_t BIGGEST_TX_DATA_SIZE = 0;

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
namespace storage
{

int ObTxDataMemtable::estimate_phy_size(const ObStoreRowkey *start_key,
                                        const ObStoreRowkey *end_key,
                                        int64_t &total_bytes,
                                        int64_t &total_rows)
{
  int ret = OB_SUCCESS;
  total_bytes = 134217728LL * 2LL;
  total_rows = 1;
  return ret;
}

int ObTxTable::check_with_tx_data(ObReadTxDataArg &read_tx_data_arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx table is not init.", KR(ret), K(read_tx_data_arg));
    return ret;
  }
  /**************************************************************************************************/
  // 跳过读缓存
  /**************************************************************************************************/

  // step 3 : read tx data in tx_ctx table and tx_data table
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_tx_data_in_tables_(read_tx_data_arg, fn))) {
    if (OB_TRANS_CTX_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "check tx data in tables failed", KR(ret), K(ls_id_), K(read_tx_data_arg));
    }
  }

  // step 4 : make sure tx table can be read
  if (OB_SUCC(ret) || OB_TRANS_CTX_NOT_EXIST == ret) {
    check_state_and_epoch_(read_tx_data_arg.tx_id_, read_tx_data_arg.read_epoch_, true /*need_log_error*/, ret);
  }
  return ret;
}


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
  // fprintf(stdout, "deserialize tx data from sstable, tx_id = %ld, buffer size = %ld\n", tx_data.tx_id_.get_id(), tx_data_buffers_.count());
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
        fprintf(stdout,
                "deserialize large tx data from sstable, tx_id = %ld tx_data_buffers_.count() = %ld\n",
                tx_data.tx_id_.get_id(),
                tx_data_buffers_.count());
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

}
}
