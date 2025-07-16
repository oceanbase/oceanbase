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

#define USING_LOG_PREFIX STORAGE

#include "ob_memtable_read_row_util.h"

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_nop_bitmap.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"

namespace oceanbase {

using namespace blocksstable;
using namespace storage;
using namespace transaction;

namespace memtable {

int ObReadRow::iterate_row(
    const ObITableReadInfo &read_info,
    const ObStoreRowkey &key,
    ObMvccValueIterator &value_iter,
    ObDatumRow &row,
    ObNopBitMap &bitmap,
    int64_t &row_scn)
{
  int ret = OB_SUCCESS;
  bitmap.reuse();
  if (OB_FAIL(iterate_row_key(key, row))) {
    TRANS_LOG(WARN, "Failed to iterate_row_key", K(ret), K(key));
  } else if (OB_FAIL(iterate_row_value_(read_info, value_iter, row, bitmap, row_scn))) {
    TRANS_LOG(WARN, "Failed to iterate_row_value", K(ret), K(key));
  } else {
    if (!bitmap.is_empty()) {
      bitmap.set_nop_datums(row.storage_datums_);
    }
    TRANS_LOG(DEBUG, "Success to iterate memtable row", K(key), K(row), K(bitmap.get_nop_cnt()));
    // do nothing
  }
  return ret;
}

int ObReadRow::iterate_row_key(const ObStoreRowkey &rowkey, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj *obj_ptr = rowkey.get_obj_ptr();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
    if (OB_FAIL(row.storage_datums_[i].from_obj_enhance(obj_ptr[i]))) {
      TRANS_LOG(WARN, "Failed to transform obj to datum", K(ret), K(i), K(rowkey));
    } else if (OB_UNLIKELY(row.storage_datums_[i].is_nop_value())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "col in rowkey is unexpected nop", K(ret), K(i), K(rowkey));
    }
  }
  return ret;
}

int ObReadRow::iterate_row_value_(const ObITableReadInfo &read_info,
                                  ObMvccValueIterator &value_iter,
                                  ObDatumRow &row,
                                  ObNopBitMap &bitmap,
                                  int64_t &row_scn)
{
  int ret = OB_SUCCESS;
  const void *tnode = nullptr;
  const ObMvccTransNode *tx_node = nullptr;
  bool read_finished = false;

  row_scn = 0;
  row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  row.snapshot_version_ = 0;
  while (!read_finished && OB_SUCC(ret) && OB_SUCC(value_iter.get_next_node(tnode))) {
    if (OB_ISNULL(tx_node = reinterpret_cast<const ObMvccTransNode *>(tnode))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else if (OB_FAIL(fill_in_row_with_tx_node_(read_info, value_iter, tx_node, row, bitmap, row_scn, read_finished))) {
      STORAGE_LOG(WARN, "fill in row with tx node failed", KR(ret));
    }
  }

  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

int ObReadRow::fill_in_row_with_tx_node_(const storage::ObITableReadInfo &read_info,
                                         ObMvccValueIterator &value_iter,
                                         const ObMvccTransNode *tx_node,
                                         blocksstable::ObDatumRow &row,
                                         ObNopBitMap &bitmap,
                                         int64_t &row_scn,
                                         bool &read_finished)
{
  int ret = OB_SUCCESS;

  const ObMemtableDataHeader *mtd = nullptr;
  const ObRowHeader *row_header = nullptr;
  ObCompatRowReader reader;
  if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader *>(tx_node->buf_))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans node value is null", K(ret), KP(mtd), KP(tx_node));
  } else {
    const bool is_committed = tx_node->is_committed();
    const int64_t trans_version = is_committed ? tx_node->trans_version_.get_val_for_tx() : INT64_MAX;
    if (row.row_flag_.is_not_exist()) {
      if (ObDmlFlag::DF_DELETE == mtd->dml_flag_) {
        row.row_flag_.set_flag(ObDmlFlag::DF_DELETE);
      } else {
        row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
      }
    }
    row.snapshot_version_ = std::max(trans_version, row.snapshot_version_);
    if (row.snapshot_version_ == INT64_MAX) {
      row.set_have_uncommited_row();
    }
    TRANS_LOG(DEBUG, "row snapshot version", K(row.snapshot_version_));

    if (OB_FAIL(
            reader.read_memtable_row(mtd->buf_, mtd->buf_len_, read_info, row, bitmap, read_finished, row_header))) {
      TRANS_LOG(WARN, "Failed to read memtable row", K(ret));
    } else if (0 == row_scn) {
      const ObTransID snapshot_tx_id = value_iter.get_snapshot_tx_id();
      const ObTransID reader_tx_id = value_iter.get_reader_tx_id();
      share::SCN row_version = tx_node->trans_version_;
      row_scn = row_version.get_val_for_tx();
      if (!row.is_have_uncommited_row() && !value_iter.get_mvcc_acc_ctx()->is_standby_read_ &&
          !(snapshot_tx_id == tx_node->get_tx_id() || reader_tx_id == tx_node->get_tx_id()) && row_version.is_max()) {
        TRANS_LOG(
            ERROR, "meet row scn with undecided value", KPC(tx_node), K(is_committed), K(trans_version), K(value_iter));
      }
    }
    if (OB_SUCC(ret) && ObDmlFlag::DF_INSERT == mtd->dml_flag_) {
      read_finished = true;
      STORAGE_LOG(DEBUG, "chaser debug iter memtable row", KPC(mtd), K(read_finished));
    }
  }
  return ret;
}

int ObReadRow::iterate_delete_insert_row(const ObITableReadInfo &read_info,
                                         const ObStoreRowkey &key,
                                         ObMvccValueIterator &value_iter,
                                         ObDatumRow &latest_row,
                                         ObDatumRow &earliest_row,
                                         ObNopBitMap &bitmap,
                                         int64_t &latest_row_scn,
                                         int64_t &earliest_row_scn,
                                         int64_t &acquired_row_cnt)
{
  int ret = OB_SUCCESS;
  bitmap.reuse();
  acquired_row_cnt = 0;
  latest_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  latest_row.snapshot_version_ = 0;
  earliest_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  earliest_row.snapshot_version_ = 0;
  bool read_finished = false;
  const ObMvccTransNode *latest_tx_node = nullptr;
  const ObMvccTransNode *earliest_tx_node = nullptr;

  if (OB_FAIL(acquire_delete_insert_tx_node_(value_iter, latest_tx_node, earliest_tx_node))) {
    STORAGE_LOG(WARN, "acquire delete insert tx node failed", KR(ret));
  } else if (OB_ISNULL(latest_tx_node)) {
    ret = OB_SUCCESS;
    STORAGE_LOG(DEBUG, "all tx node has been folded by major snapshot", KR(ret), K(key));
  } else if (latest_tx_node == earliest_tx_node) {
    // they point to the same tx node, which means the same modification
    if (OB_FAIL(fill_in_row_with_tx_node_(
                read_info, value_iter, latest_tx_node, latest_row, bitmap, latest_row_scn, read_finished))) {
      STORAGE_LOG(WARN, "fill in row failed", KR(ret));
    } else if (OB_UNLIKELY(!read_finished)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected unfinished reading row", K(ret), K(latest_row), K(bitmap.get_nop_cnt()));
    } else {
      acquired_row_cnt = 1;
    }
  } else {
    // TODO : @gengli.wzy What if the tx_node is a compact_node ?
    const ObMemtableDataHeader *latest_mtd = nullptr;
    const ObMemtableDataHeader *earliest_mtd = nullptr;

    if (OB_ISNULL(latest_mtd = reinterpret_cast<const ObMemtableDataHeader *>(latest_tx_node->buf_)) ||
        OB_ISNULL(earliest_mtd = reinterpret_cast<const ObMemtableDataHeader *>(earliest_tx_node->buf_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node value is null", K(ret), KP(latest_tx_node), KP(earliest_tx_node));
    } else {
      bool fill_latest = false;
      bool fill_earliest = false;
      ObDmlFlag earliest_dml = earliest_mtd->dml_flag_;
      ObDmlFlag latest_dml = latest_mtd->dml_flag_;
      if (ObDmlFlag::DF_INSERT == earliest_dml && ObDmlFlag::DF_DELETE == latest_dml) {
        // Insert1->Delete2->Insert2->Delete3 (Drop all node)
        // earliest(Insert1) latest(Delete3)
        fill_latest = true;
      } else if (ObDmlFlag::DF_INSERT == earliest_dml && ObDmlFlag::DF_INSERT == latest_dml) {
        // Insert1->Delete2->Insert2->Delete3->Insert3 (Keep Insert3)
        // latest(Insert3)
        fill_latest = true;
      } else if (ObDmlFlag::DF_DELETE == earliest_dml && ObDmlFlag::DF_INSERT == latest_dml) {
        // Delete1->Insert1->Delete2->Inserte2->Delete3->Insert3 (Keep Delete1 and Insert3)
        // earliest(Delete1) latest(Insert3)
        fill_latest = true;
        fill_earliest = true;
      } else if (ObDmlFlag::DF_DELETE == earliest_dml && ObDmlFlag::DF_DELETE == latest_dml) {
        // Delete1->Insert1->Delete2->Insert2->Delete3 (Keep Delete1)
        // Drop all node after Delete1. latest(Delete1)
        latest_tx_node = earliest_tx_node;
        fill_latest = true;
      }

      if (fill_latest) {
        if (OB_FAIL(iterate_row_key(key, latest_row))) {
          STORAGE_LOG(WARN, "Failed to iterate_row_key", K(ret), K(key));
        } else if (OB_FAIL(fill_in_row_with_tx_node_(
                    read_info, value_iter, latest_tx_node, latest_row, bitmap, latest_row_scn, read_finished))) {
          STORAGE_LOG(WARN, "fill in latest row failed", KR(ret));
        } else if (OB_UNLIKELY(!read_finished)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected unfinished reading row", K(ret), K(latest_row), K(bitmap.get_nop_cnt()));
        } else {
          acquired_row_cnt++;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (fill_earliest) {
        bitmap.reuse();
        read_finished = false;
        if (OB_FAIL(iterate_row_key(key, earliest_row))) {
          STORAGE_LOG(WARN, "Failed to iterate_row_key", K(ret), K(key));
        } else if (OB_FAIL(fill_in_row_with_tx_node_(
                    read_info, value_iter, earliest_tx_node, earliest_row, bitmap, earliest_row_scn, read_finished))) {
          STORAGE_LOG(WARN, "fill in earliest row failed", KR(ret));
        } else if (OB_UNLIKELY(!read_finished)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected unfinished reading row", K(ret), K(earliest_row), K(bitmap.get_nop_cnt()));
        } else {
          acquired_row_cnt++;
        }
      }

      if (OB_SUCC(ret)) {
        if (!fill_latest && !fill_earliest) {
          latest_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        }
      }
      STORAGE_LOG(DEBUG, "Delete-Insert", K(latest_row), K(earliest_row), K(latest_row_scn));
    }
  }
  return ret;
}

/**
 * @brief only for delete insert read.
 *
 * @param[in] value_iter to iterate tx nodes on one row
 * @param[out] latest_tx_node the latest modification on this row
 * @param[out] earliest_tx_node the earliest modification on this row(folded by major snapshot)
 */
int ObReadRow::acquire_delete_insert_tx_node_(ObMvccValueIterator &value_iter,
                                              const ObMvccTransNode *&latest_tx_node,
                                              const ObMvccTransNode *&earliest_tx_node)
{
  int ret = OB_SUCCESS;
  const void *tnode = nullptr;
  int64_t iterate_cnt = 0;
  while (OB_SUCC(ret) && OB_SUCC(value_iter.get_next_node(tnode))) {
    if (OB_ISNULL(tnode)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else {
      const ObMvccTransNode *tx_node = reinterpret_cast<const ObMvccTransNode *>(tnode);

      const int64_t trans_version = tx_node->trans_version_.get_val_for_tx();
      if (trans_version <= value_iter.get_major_snapshot()) {
        // fold multi-version rows less than major snapshot
        TRANS_LOG(DEBUG, "fold rows by major snapshot", K(value_iter.get_major_snapshot()), KPC(tx_node));
        break;
      } else {
        // Use two tx_node pointer to record the tx_node range need to be fused
        if (OB_ISNULL(latest_tx_node)) {
          latest_tx_node = tx_node;
        } else if (DF_DELETE == latest_tx_node->get_dml_flag() && DF_DELETE == tx_node->get_dml_flag()) {
          latest_tx_node = tx_node;
        }
        earliest_tx_node = tx_node;
        iterate_cnt++;
      }
    }
  }
  // print debug log if iterate nothing
  if (0 == iterate_cnt) {
    TRANS_LOG(DEBUG, "all tx nodes has been folded", KR(ret), K(iterate_cnt), KP(tnode), K(value_iter));
  }

  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
