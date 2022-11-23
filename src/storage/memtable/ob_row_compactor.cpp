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

#include "share/config/ob_server_config.h"
#include "lib/stat/ob_diagnose_info.h"
#include "ob_row_compactor.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable_compact_writer.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/blocksstable/ob_row_writer.h"

namespace oceanbase
{

namespace storage
{
class ObTxTable;
};

using namespace common;
using namespace storage;
using namespace blocksstable;
namespace memtable
{
CompactMapImproved::StaticMemoryHelper::StaticMemoryHelper()
{
  int ret = OB_SUCCESS;
  // TODO mod id && tenant id.
  if (OB_FAIL(node_alloc_.init(sizeof(Node),
                               ObModIds::OB_ROW_COMPACTION))) {
    TRANS_LOG(WARN, "failed to init small allocator", K(ret));
  } else {
    // Do nothing. Thread local vars are inited by their default vals.
  }
  TRANS_LOG(INFO, "global mem_helper inited", K(ret));
}

CompactMapImproved::StaticMemoryHelper::~StaticMemoryHelper()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(node_alloc_.destroy())) {
    TRANS_LOG(WARN, "failed to destroy small allocator", K(ret));
  } else {
    arr_arena_.free();
    // the following statement annotated may cause core dump
    // causing by the undefined destruct order of static variables and thread local variables
    //bkts_ = NULL;
    //ver_ = 0;
  }
  //TRANS_LOG(INFO, "global mem_helper destoryed", K(ret));
}

CompactMapImproved::Node *CompactMapImproved::StaticMemoryHelper::get_tl_arr()
{
  CompactMapImproved::Node *ret = NULL;
  if (NULL == bkts_) {
    // Alloc thread local bucket array.
    lib::ObLockGuard<ObSpinLock> guard(arr_arena_lock_);
    if (NULL == (bkts_ = reinterpret_cast<Node *>(GET_TSI(Node[BKT_N])))) {
      TRANS_LOG(WARN, "failed to alloc thread local bucket array");
    } else {
      // Init bucket array.
      for (int64_t idx = 0; idx < BKT_N; ++idx) {
        CompactMapImproved::Node &bkt = bkts_[idx];
        bkt.ver_ = 0;
        bkt.col_id_ = INVALID_COL_ID;
        bkt.next_ = NULL;
        // Ignore cell.
      }
    }
  }
  ret = bkts_;
  return ret;
}

uint32_t &CompactMapImproved::StaticMemoryHelper::get_tl_arr_ver()
{
  // Return its reference.
  return ver_;
}

CompactMapImproved::Node *CompactMapImproved::StaticMemoryHelper::get_node()
{
  CompactMapImproved::Node *ret = NULL;
  if (OB_ISNULL(ret = reinterpret_cast<CompactMapImproved::Node *>(node_alloc_.alloc()))) {
    TRANS_LOG(WARN, "failed to alloc node", K(sizeof(Node)), K(node_alloc_));
  } else {
    ret->ver_ = 0;
    ret->col_id_ = INVALID_COL_ID;
    ret->next_ = NULL;
    // Ignore cell.
  }
  return ret;
}

void CompactMapImproved::StaticMemoryHelper::revert_node(CompactMapImproved::Node *n)
{
  if (NULL != n) {
    node_alloc_.free(n);
    n = NULL;
  }
}

_RLOCAL(uint32_t, CompactMapImproved::StaticMemoryHelper::ver_);		

_RLOCAL(CompactMapImproved::Node*, CompactMapImproved::StaticMemoryHelper::bkts_);

int CompactMapImproved::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bkts_ = mem_helper_.get_tl_arr())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "failed to get thread local bucket array");
  } else {
    // Init cursor.
    scan_cur_bkt_ = &bkts_[0];
    scan_cur_node_ = scan_cur_bkt_;
    // Inc version.
    // ver_ : [0, MAX_VER).
    // bucket ver_ init at MAX_VER.
    ver_ = ++(mem_helper_.get_tl_arr_ver());
    if (MAX_VER == ver_) {
      mem_helper_.get_tl_arr_ver() = 0;
      ver_ = 0;
      for (int64_t idx = 0; idx < BKT_N; ++idx) {
        bkts_[idx].ver_ = MAX_VER;
      }
    }
  }
  return ret;
}

void CompactMapImproved::destroy()
{
  bkts_ = NULL;
  ver_ = 0;
  scan_cur_node_ = NULL;
  scan_cur_bkt_ = NULL;
}

int CompactMapImproved::set(const uint64_t col_id, const ObObj &cell)
{
  int ret = OB_SUCCESS;
  const int64_t bkt_idx = static_cast<int64_t>(col_id) & BKT_N_MOD_MASK; // mod 512.
  if (OB_INVALID_ID == col_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == bkts_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "bkts_ is NULL", K(bkts_), K(bkt_idx));
  } else {
    Node &bkt = bkts_[bkt_idx];
    if (ver_ != bkt.ver_) {
      // Empty bucket.
      bkt.ver_ = ver_;
      bkt.col_id_ = static_cast<uint32_t>(col_id);
      bkt.cell_ = cell;
      bkt.next_ = NULL;
    } else {
      // Non-empty. Check dup.
      bool exist = false;
      Node *node = &bkt;
      while (!exist && NULL != node) {
        exist = (col_id == static_cast<uint64_t>(node->col_id_));
        node = node->next_;
      }
      if (!exist) {
        // No-dup, set it.
        node = mem_helper_.get_node();
        if (OB_ISNULL(node)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "failed to get Node from memory helper");
        } else {
          node->ver_ = ver_;
          node->col_id_ = static_cast<uint32_t>(col_id);
          node->cell_ = cell;
          node->next_ = bkt.next_;
          bkt.next_ = node;
        }
      } else {
        // Dup.
        ret = OB_ENTRY_EXIST;
      }
    }
  }
  return ret;
}

int CompactMapImproved::get_next(uint64_t &col_id, ObObj &cell)
{
  int ret = OB_SUCCESS;
  if (NULL == scan_cur_bkt_) {
    ret = OB_NOT_INIT;
  } else {
    // Find next valid node.
    while (OB_SUCCESS == ret
           && (ver_ != scan_cur_bkt_->ver_ || NULL == scan_cur_node_)) {
      if (BKT_N > (scan_cur_bkt_ + 1 - bkts_)) {
        scan_cur_bkt_ += 1;
        scan_cur_node_ = scan_cur_bkt_;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  // Retrieve value.
  if (OB_SUCC(ret)) {
    col_id = static_cast<uint64_t>(scan_cur_node_->col_id_);
    cell = scan_cur_node_->cell_;
    CompactMapImproved::Node *scanned_node = scan_cur_node_;
    scan_cur_node_ = scan_cur_node_->next_;
    if (scanned_node != scan_cur_bkt_) {
      // Scanned nodes are freed.
      scan_cur_bkt_->next_ = scan_cur_node_;
      mem_helper_.revert_node(scanned_node);
      scanned_node = NULL;
    }
  }
  return ret;
}

CompactMapImproved::StaticMemoryHelper CompactMapImproved::mem_helper_;

ObMemtableRowCompactor::ObMemtableRowCompactor()
  : is_inited_(false),
    row_(NULL),
    memtable_(NULL),
    node_alloc_(NULL),
    for_replay_(false),
    map_ins_(),
    map_(map_ins_)
{}

ObMemtableRowCompactor::~ObMemtableRowCompactor() {}

int ObMemtableRowCompactor::init(ObMvccRow *row,
                                 ObMemtable *mt,
                                 ObIAllocator *node_alloc,
                                 const bool for_replay)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObMemtableRowCompactor init twice", K(ret));
  } else if (OB_ISNULL(row) || OB_ISNULL(node_alloc) || OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(row), KP(node_alloc), KP(mt));
  } else if (OB_FAIL(map_.init())) {
    TRANS_LOG(WARN, "failed to init compact map", K(ret));
  } else {
    is_inited_ = true;
    row_ = row;
    memtable_ = mt;
    node_alloc_ = node_alloc;
    for_replay_ = for_replay;
  }
  return ret;
}

// Row compactor guarantee holding the row latch before compact
// So modification is guaranteed to be safety with another modification,
// while we need pay attention to the concurrency between lock_for_read
// and modification(such as compact)
int ObMemtableRowCompactor::compact(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (0 >= snapshot_version || INT64_MAX == snapshot_version) {
    STORAGE_LOG(ERROR, "unexpected snapshot version", K(ret), K(snapshot_version));
    ret = OB_ERR_UNEXPECTED;
  } else if (NULL != row_->latest_compact_node_ &&
             snapshot_version <= row_->latest_compact_node_->trans_version_) {
    // concurrent do compact
  } else {
    ObTimeGuard tg("row compact", 50L * 1000L);
    ObMvccTransNode *start = NULL;

    find_start_pos_(snapshot_version, start);
    tg.click();

    ObMvccTransNode *compact_node = construct_compact_node_(snapshot_version, start);
    tg.click();

    if (OB_NOT_NULL(compact_node)) {
      insert_compact_node_(compact_node, start);
    }
    tg.click();
  }

  return ret;
}


// Find position from where compaction started forward or backward until reached
// oldest node or latest compaction node
void ObMemtableRowCompactor::find_start_pos_(const int64_t snapshot_version,
                                             ObMvccTransNode *&start)
{
  int64_t search_cnt = 0;

  // the first time to do row compact, traverse from list_head
  start = ((NULL == row_->latest_compact_node_) ? (row_->list_head_) : (row_->latest_compact_node_));
  while (NULL != start) {
    if (NULL == row_->latest_compact_node_) {
      // Traverse forward from list_head
      //   We go from head to find the suitable node for compact node start
      if (INT64_MAX == start->trans_version_          // skip uncommited
          || snapshot_version < start->trans_version_ // skip bigger txn
          || !start->is_committed()) {                // skip uncommited
        start = start->prev_;
        search_cnt++;
      } else {
        break;
      }
    } else {
      // Traverse backward from latest_compact_node
      //   We need handle the bad case when elr, so we traverse from backward
      //   when there exists latest_compact_node
      if (NULL != start->next_                                // stop at null
          && snapshot_version >= start->next_->trans_version_ // stop at bigger txn
          && start->next_->is_committed()                     // stop at uncommitted
          && INT64_MAX != start->next_->trans_version_) {     // stop at uncommitted
        start = start->next_;
        search_cnt++;
      } else {
        break;
      }
    }

    /*
    if (NULL != start
      && (start->trans_version_ > INT64_MAX / 2 || !start->is_committed())) {
      TRANS_LOG(ERROR, "unexpected start node when row comapct", K(*start), K(snapshot_version));
      ob_abort();
    }*/

    if (search_cnt >= 100
        && 0 == search_cnt % 100
        && NULL != row_->latest_compact_node_) {
      TRANS_LOG(WARN, "too much trans node scaned when row compact",
                K(search_cnt), K(snapshot_version), KPC(start), K(*row_),
                K(*(row_->list_head_)), K(*(row_->latest_compact_node_)));
    }
  }
}

int ObMemtableRowCompactor::try_cleanout_tx_node_during_compact_(ObTxTableGuard &tx_table_guard,
                                                                 ObMvccTransNode *tnode)
{
  int ret = OB_SUCCESS;

  ObTxTable *tx_table = tx_table_guard.get_tx_table();
  int64_t read_epoch = tx_table_guard.epoch();
  if (!(tnode->is_committed() || tnode->is_aborted())) {
    if (!tnode->is_delayed_cleanout() && !tnode->is_elr()) {
      // It may be the case that one row contains multiple version from one txn.
      // May be the case like below(v1 to v5 are all belong to one txn, and they
      // are at the stage before backfilling the tnode when committing txn):
      // v5(v = ?) -> v4(v = ?) -> v3(v = ?) -> v2(v = ?) -> v1(v = ?)
      //
      // They are committing from v1 to v5, while it may be that v5 has been
      // delay cleanout and commit first like below.
      // v5(v = 100) -> v4(v = ?) -> v3(v = ?) -> v2(v = ?) -> v1(v = ?)
      //
      // v5 will be chosen as start position, go through compaction and be
      // compacted, then v4 is compacted with state as not committed/aborted and
      // tag as not delay cleanout. So we remove the error log here!
      //
      // NB: Now tnode will only be delay cleanout or filled back exactly once.
      // So the case before will not happen again before v5 will not be delay
      // cleanout and filled back through commit callback. So we add the error
      // log back
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected non cleanout uncommitted node", KPC(tnode), KPC(row_));
    } else if (OB_FAIL(tx_table->cleanout_tx_node(tnode->tx_id_,
                                                  read_epoch,
                                                  *row_,
                                                  *tnode,
                                                  false   /*need_row_latch*/))) {
      TRANS_LOG(WARN, "cleanout tx state failed", K(ret), KPC(row_), KPC(tnode));
    }
  }

  return ret;
}

ObMvccTransNode *ObMemtableRowCompactor::construct_compact_node_(const int64_t snapshot_version,
                                                                 ObMvccTransNode *save)
{
  int ret = OB_SUCCESS;
  ObRowReader row_reader;
  ObDatumRow datum_row;
  ObDatumRow compact_datum_row;
  ObMvccTransNode *trans_node = nullptr;
  ObMvccTransNode *cur = save;
  ObTxTableGuard tx_table_guard;
  ObTxTable *tx_table = NULL;
  ObDmlFlag dml_flag = ObDmlFlag::DF_NOT_EXIST;
  int64_t compact_row_cnt = 0;
  int64_t rowkey_cnt = 0;

  if (NULL == memtable_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "memtable is NULL");
  } else if (OB_FAIL(memtable_->get_tx_table_guard(tx_table_guard))) {
    TRANS_LOG(WARN, "get tx table guard failed", K(ret), KPC(memtable_));
  } else if (!tx_table_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "get tx table failed", K(ret), KPC(memtable_));
  } else if (OB_FAIL(compact_datum_row.init(OB_ROW_DEFAULT_COLUMNS_COUNT))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  }

  TRANS_LOG(DEBUG, "chaser debug start compact memtable row", K(memtable_->get_key()));
  // Scan nodes till tail OR a previous compact node OR a delete node.
  while (OB_SUCCESS == ret && NULL != cur) {
    // Read cells & compact them by a map.
    const ObMemtableDataHeader *mtd = NULL;
    bool find_committed_tnode = true;
    if (OB_FAIL(try_cleanout_tx_node_during_compact_(tx_table_guard, cur))) {
      TRANS_LOG(WARN, "cleanout tx state failed", K(ret), KPC(row_), KPC(cur));
    } else if (!(cur->is_aborted() || cur->is_committed() || cur->is_elr())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected cleanout state", K(*cur), K(*row_));
    } else if (cur->is_aborted()) {
      TRANS_LOG(INFO, "ignore aborted node when compact", K(*cur), K(*row_));
      cur = cur->prev_;
      find_committed_tnode = false;
    } else if (snapshot_version < cur->trans_version_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected snapshot version", K(snapshot_version), K(*cur), K(*row_));
    } else if (NULL == (mtd = reinterpret_cast<const ObMemtableDataHeader *>(cur->buf_))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (blocksstable::ObDmlFlag::DF_LOCK == mtd->dml_flag_) {
      TRANS_LOG(INFO, "ignore lock node when compact", K(*cur), K(*row_));
      cur = cur->prev_;
      find_committed_tnode = false;
    } else if (compact_row_cnt <= 0 && NDT_COMPACT == cur->type_) {
      ret = OB_ITER_END;
    } else if (rowkey_cnt == 0) {
      const ObRowHeader *row_header = nullptr;
      if (OB_FAIL(row_reader.read_row_header(mtd->buf_, mtd->buf_len_, row_header))) {
        TRANS_LOG(WARN, "Failed to read row header", K(ret));
      } else if (OB_ISNULL(row_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null row header", K(ret));
      } else {
        rowkey_cnt = row_header->get_rowkey_count();
        compact_datum_row.count_ = rowkey_cnt;
      }
    }
    if (OB_SUCC(ret) && find_committed_tnode) {
      if (NULL == mtd) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "mtd init fail", "ret", ret);
      } else if (blocksstable::ObDmlFlag::DF_DELETE == mtd->dml_flag_) {
        // DELETE node & its previous ones are ignored.
        if (0 == compact_row_cnt) {
          if (OB_FAIL(row_reader.read_row(mtd->buf_, mtd->buf_len_, nullptr, compact_datum_row))) {
            TRANS_LOG(WARN, "Failed to read delete row", K(ret), KPC(mtd));
          } else {
            //force compact
            dml_flag = ObDmlFlag::DF_DELETE;
            compact_row_cnt++;
            compact_datum_row.row_flag_.set_flag(dml_flag);
            ret = OB_ITER_END;
          }
        } else {
          ret = OB_ITER_END;
        }
      } else if (OB_FAIL(row_reader.read_row(mtd->buf_, mtd->buf_len_, nullptr, datum_row))) {
        TRANS_LOG(WARN, "Failed to read datum row", K(ret));
      } else if (OB_FAIL(compact_datum_row.reserve(datum_row.get_column_count(), true))) {
          STORAGE_LOG(WARN, "Failed to reserve datum row", K(ret), K(datum_row));
      } else {
        compact_datum_row.count_ = MAX(datum_row.get_column_count(), compact_datum_row.count_);
        if (ObDmlFlag::DF_NOT_EXIST == dml_flag) {
          dml_flag = mtd->dml_flag_;
          compact_datum_row.row_flag_.set_flag(dml_flag);
        }
        for (int64_t i = 0; i < compact_datum_row.get_column_count(); ++i) {
          if (compact_datum_row.storage_datums_[i].is_nop()) {
            compact_datum_row.storage_datums_[i] = datum_row.storage_datums_[i];
          }
        }
        TRANS_LOG(DEBUG, "chaser debug compact memtable row", K(datum_row), K(dml_flag), K(compact_datum_row));
        compact_row_cnt++;
        if (NDT_COMPACT == cur->type_) {
          // Stop at compact node.
          ret = OB_ITER_END;
        } else {
          // Go prev.
          cur = cur->prev_;
        }
      }
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;

  // Write compact row
  if (OB_SUCC(ret) && compact_row_cnt > 0) {
    EVENT_INC(MEMSTORE_ROW_COMPACTION_COUNT);
    SMART_VAR(blocksstable::ObRowWriter, row_writer) {
      char *buf = nullptr;
      int64_t len = 0;
      if (OB_FAIL(row_writer.write(rowkey_cnt, compact_datum_row, buf, len))) {
        TRANS_LOG(WARN, "Failed to writer compact row", K(ret), K(datum_row));
      } else if (OB_UNLIKELY(ObDmlFlag::DF_NOT_EXIST == dml_flag)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "Unexpected not exist trans node", K(ret), K(dml_flag), K(compact_datum_row), K(snapshot_version));
      } else {
        // Build trans node & insert it to its place.
        ObMemtableData mtd(dml_flag, len, buf);
        bool is_lock_node = true;
        int64_t node_size = (int64_t)sizeof(*trans_node) + mtd.dup_size();

        if (OB_ISNULL(trans_node = (ObMvccTransNode *)node_alloc_->alloc(node_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "failed to alloc trans node", K(ret), K(node_size));
        } else if (OB_FAIL(ObMemtableDataHeader::build(reinterpret_cast<ObMemtableDataHeader *>(trans_node->buf_), &mtd))) {
          TRANS_LOG(WARN, "failed to dup data to trans node", K(ret), K(trans_node->buf_));
        } else if (OB_FAIL(save->is_lock_node(is_lock_node))) {
          TRANS_LOG(ERROR, "unexpected lock node", K(ret), "node", *save);
        } else if (is_lock_node) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected lock node", K(ret), "node", *save);
        } else if (INT64_MAX == save->trans_version_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected trans version", K(ret), "node", *save);
        } else {
          trans_node->tx_id_ = save->tx_id_;
          trans_node->seq_no_ = save->seq_no_;
          trans_node->trans_version_ = save->trans_version_;
          trans_node->modify_count_ = save->modify_count_;
          trans_node->acc_checksum_ = save->acc_checksum_;
          trans_node->version_ = save->version_;
          trans_node->type_ = NDT_COMPACT;
          trans_node->flag_ = save->flag_;
          trans_node->log_timestamp_ = save->log_timestamp_;
          trans_node->set_snapshot_version_barrier(snapshot_version);
          TRANS_LOG(DEBUG, "success to compact row, ", K(trans_node->tx_id_), K(dml_flag), K(compact_row_cnt), KPC(save));
        }
      }
    }
  }
  return OB_SUCC(ret) ? trans_node : NULL;
}

// Modification is guaranteed to be safety with another modification, while we
// need pay attention to the concurrency with lock_for_read(read will not hold
// row latch)
void ObMemtableRowCompactor::insert_compact_node_(ObMvccTransNode *tx_node,
                                                  ObMvccTransNode *start)
{
  int ret = OB_SUCCESS;

  // Insert the compact node before the start
  ATOMIC_STORE(&(tx_node->prev_), start);
  if (NULL == start->next_) {
    ATOMIC_STORE(&(tx_node->next_), NULL);
    ATOMIC_STORE(&(row_->list_head_), tx_node);
  } else {
    ATOMIC_STORE(&(tx_node->next_), start->next_);
    ATOMIC_STORE(&(start->next_->prev_), tx_node);
  }
  ATOMIC_STORE(&(start->next_), tx_node);

  // Update statistics about compact node
  ATOMIC_STORE(&(row_->latest_compact_node_), tx_node);
  const int64_t end_ts = ObTimeUtility::current_time();
  ATOMIC_STORE(&(row_->latest_compact_ts_), end_ts);
  ATOMIC_STORE(&(row_->last_compact_cnt_), tx_node->modify_count_);
  ATOMIC_STORE(&(row_->update_since_compact_), 0);
}

}
}
