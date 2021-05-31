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
#include "common/cell/ob_cell_reader.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable_compact_writer.h"

namespace oceanbase {
using namespace common;
namespace memtable {
CompactMapImproved::StaticMemoryHelper::StaticMemoryHelper()
{
  int ret = OB_SUCCESS;
  // TODO mod id && tenant id.
  if (OB_FAIL(node_alloc_.init(sizeof(Node), ObModIds::OB_ROW_COMPACTION))) {
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
    // bkts_ = NULL;
    // ver_ = 0;
  }
  // TRANS_LOG(INFO, "global mem_helper destoryed", K(ret));
}

CompactMapImproved::Node* CompactMapImproved::StaticMemoryHelper::get_tl_arr()
{
  CompactMapImproved::Node* ret = NULL;
  if (NULL == bkts_) {
    // Alloc thread local bucket array.
    lib::ObLockGuard<ObSpinLock> guard(arr_arena_lock_);
    if (NULL == (bkts_ = GET_TSI(Node[BKT_N]))) {
      TRANS_LOG(WARN, "failed to alloc thread local bucket array");
    } else {
      // Init bucket array.
      for (int64_t idx = 0; idx < BKT_N; ++idx) {
        CompactMapImproved::Node& bkt = bkts_[idx];
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

uint32_t& CompactMapImproved::StaticMemoryHelper::get_tl_arr_ver()
{
  // Return its reference.
  return ver_;
}

CompactMapImproved::Node* CompactMapImproved::StaticMemoryHelper::get_node()
{
  CompactMapImproved::Node* ret = NULL;
  if (OB_ISNULL(ret = reinterpret_cast<CompactMapImproved::Node*>(node_alloc_.alloc()))) {
    TRANS_LOG(WARN, "failed to alloc node", K(sizeof(Node)), K(node_alloc_));
  } else {
    ret->ver_ = 0;
    ret->col_id_ = INVALID_COL_ID;
    ret->next_ = NULL;
    // Ignore cell.
  }
  return ret;
}

void CompactMapImproved::StaticMemoryHelper::revert_node(CompactMapImproved::Node* n)
{
  if (NULL != n) {
    node_alloc_.free(n);
    n = NULL;
  }
}

RLOCAL(uint32_t, CompactMapImproved::StaticMemoryHelper::ver_);

RLOCAL(CompactMapImproved::Node*, CompactMapImproved::StaticMemoryHelper::bkts_);

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

int CompactMapImproved::set(const uint64_t col_id, const ObObj& cell)
{
  int ret = OB_SUCCESS;
  const int64_t bkt_idx = static_cast<int64_t>(col_id) & BKT_N_MOD_MASK;  // mod 512.
  if (OB_INVALID_ID == col_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == bkts_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "bkts_ is NULL", K(bkts_), K(bkt_idx));
  } else {
    Node& bkt = bkts_[bkt_idx];
    if (ver_ != bkt.ver_) {
      // Empty bucket.
      bkt.ver_ = ver_;
      bkt.col_id_ = static_cast<uint32_t>(col_id);
      bkt.cell_ = cell;
      bkt.next_ = NULL;
    } else {
      // Non-empty. Check dup.
      bool exist = false;
      Node* node = &bkt;
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

int CompactMapImproved::get_next(uint64_t& col_id, ObObj& cell)
{
  int ret = OB_SUCCESS;
  if (NULL == scan_cur_bkt_) {
    ret = OB_NOT_INIT;
  } else {
    // Find next valid node.
    while (OB_SUCCESS == ret && (ver_ != scan_cur_bkt_->ver_ || NULL == scan_cur_node_)) {
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
    CompactMapImproved::Node* scanned_node = scan_cur_node_;
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
    : is_inited_(false), row_(NULL), node_alloc_(NULL), map_ins_(), map_(map_ins_)
{}

ObMemtableRowCompactor::~ObMemtableRowCompactor()
{}

int ObMemtableRowCompactor::init(ObMvccRow* row, ObIAllocator* node_alloc)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObMemtableRowCompactor init twice", K(ret));
  } else if (OB_ISNULL(row) || OB_ISNULL(node_alloc)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(row), KP(node_alloc));
  } else if (OB_FAIL(map_.init())) {
    TRANS_LOG(WARN, "failed to init compact map", K(ret));
  } else {
    is_inited_ = true;
    row_ = row;
    node_alloc_ = node_alloc;
  }
  return ret;
}

int ObMemtableRowCompactor::compact(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (0 >= snapshot_version || INT64_MAX == snapshot_version) {
    STORAGE_LOG(ERROR, "unexpected snapshot version", K(ret), K(snapshot_version));
    ret = OB_ERR_UNEXPECTED;
  } else if (NULL != row_->latest_compact_node_ && snapshot_version <= row_->latest_compact_node_->trans_version_) {
    // concurrent do compact
  } else {
    ObTimeGuard tg("row compact", 50L * 1000L);

    ObMvccTransNode** start = NULL;
    ObMvccTransNode* save = NULL;
    ObMvccTransNode* next_node = NULL;

    find_start_pos_(snapshot_version, start, save, next_node);
    tg.click();

    ObMvccTransNode* compact_node = construct_compact_node_(snapshot_version, *start);
    tg.click();

    if (OB_NOT_NULL(compact_node)) {
      if (OB_FAIL(insert_compact_node_(compact_node, start, save, next_node))) {
        TRANS_LOG(WARN, "fail to insert compact node", K(ret), K(compact_node));
      }
    }
    tg.click();
  }

  return ret;
}

void ObMemtableRowCompactor::find_start_pos_(
    const int64_t snapshot_version, ObMvccTransNode**& start, ObMvccTransNode*& save, ObMvccTransNode*& next_node)
{
  int64_t search_cnt = 0;

  // the first time to do row compact, traverse from list_head
  start = ((NULL == row_->latest_compact_node_) ? &(row_->list_head_) : &(row_->latest_compact_node_));
  save = ATOMIC_LOAD(start);
  while (NULL != save) {
    // Find start
    bool can_break = true;
    if (NULL == row_->latest_compact_node_) {
      if (INT64_MAX == save->trans_version_ || snapshot_version < save->trans_version_ || !save->is_committed()) {
        // Skip uncommitted nodes and find snapshot nodes
        start = &(save->prev_);
        save = ATOMIC_LOAD(start);
        can_break = false;
        search_cnt++;
      }
    } else {
      // elr trans_node is not allowed to do row compact
      if (snapshot_version >= save->trans_version_ && save->is_committed() && NULL != save->next_) {
        // Skip uncommitted nodes and find snapshot nodes
        start = &(save->next_);
        save = ATOMIC_LOAD(start);
        can_break = false;
        search_cnt++;
      }
    }
    if (can_break) {
      next_node = save;
      start = &(save->prev_);
      save = ATOMIC_LOAD(start);
      break;
    }
    if (search_cnt > 100 && NULL != row_->latest_compact_node_) {
      TRANS_LOG(WARN,
          "too much trans node scaned when row compact",
          K(search_cnt),
          K(*(row_->latest_compact_node_)),
          K(*row_));
    }
  }
}

ObMvccTransNode* ObMemtableRowCompactor::construct_compact_node_(const int64_t snapshot_version, ObMvccTransNode* save)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode* trans_node = NULL;
  ObMvccTransNode* cur = save;
  int64_t compact_cell_cnt = 0;
  ObCellReader reader;
  bool is_row_delete = false;

  if (NULL != save && snapshot_version < save->trans_version_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected snapshot version", K(snapshot_version), K(*save), K(*row_));
  }

  // Scan nodes till tail OR a previous compact node OR a delete node.
  bool overflow = false;
  while (OB_SUCCESS == ret && NULL != cur) {
    // Read cells & compact them by a map.
    overflow |= cur->is_overflow();
    const ObMemtableDataHeader* mtd = NULL;
    reader.reset();
    if (NULL == (mtd = reinterpret_cast<const ObMemtableDataHeader*>(cur->buf_))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (compact_cell_cnt <= 0 && NDT_COMPACT == cur->type_) {
      ret = OB_ITER_END;
      // TRANS_LOG(WARN, "ROWCOMPACT: nothing to compact since last compact");
    } else if (storage::T_DML_DELETE == mtd->dml_type_) {
      // DELETE node & its previous ones are ignored.
      ret = OB_ITER_END;
      is_row_delete = true;
    } else if (OB_FAIL(reader.init(mtd->buf_, mtd->buf_len_, SPARSE))) {
      TRANS_LOG(WARN, "failed to init cell reader", K(ret), K(mtd), K(mtd->buf_));
    } else {
      uint64_t col_id = OB_INVALID_ID;
      const ObObj* cell = NULL;
      // Read cell by cell.
      while (OB_SUCC(ret)) {
        if (OB_FAIL(reader.next_cell())) {
          TRANS_LOG(WARN, "failed to call next cell from cell reader", K(ret));
        } else if (OB_FAIL(reader.get_cell(col_id, cell))) {
          TRANS_LOG(WARN, "failed to get cell from cell reader", K(ret), K(col_id), K(cell));
        } else if (NULL == cell) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "failed to get cell from cell reader", K(ret), K(col_id), K(cell));
        } else if (ObExtendType == cell->get_type() && ObActionFlag::OP_END_FLAG == cell->get_ext()) {
          // Iterate to the end.
          ret = OB_ITER_END;
          // for lock trans node, increase compact cell cnt,
          // but no extra <col_id, cell> would be added to the map
          if (storage::T_DML_LOCK == mtd->dml_type_) {
            compact_cell_cnt += 1;
          }
        } else if (ObExtendType == cell->get_type() && ObActionFlag::OP_DEL_ROW == cell->get_ext()) {
          is_row_delete = true;
        } else if (OB_FAIL(map_.set(col_id, *cell)) && OB_ENTRY_EXIST != ret) {
          TRANS_LOG(WARN, "failed to set cell to compact map", K(ret), K(col_id), K(*cell));
        } else {
          ret = (OB_ENTRY_EXIST == ret) ? OB_SUCCESS : ret;
          compact_cell_cnt += 1;
        }
      }
      ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
      if (OB_FAIL(ret)) {
        TRANS_LOG(ERROR, "failed to compact cells to compact map", K(ret), K(compact_cell_cnt));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (NDT_COMPACT == cur->type_) {
      // Stop at compact node.
      ret = OB_ITER_END;
    } else {
      // Go prev.
      cur = cur->prev_;
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;

  // Write cells.
  if (OB_SUCC(ret) && (is_row_delete || compact_cell_cnt > 0)) {
    ObMemtableCompactWriter writer;
    if (OB_FAIL(writer.init())) {
      TRANS_LOG(WARN, "compact writer init fail", KR(ret));
    } else {
      EVENT_INC(MEMSTORE_ROW_COMPACTION_COUNT);
      storage::ObRowDml dml_type = storage::T_DML_UNKNOWN;
      writer.reset();
      if (0 == compact_cell_cnt) {  // is_row_delete must be true
        if (OB_FAIL(writer.row_delete())) {
          TRANS_LOG(WARN, "col_ccw append row_delete fail", "ret", ret);
        } else {
          dml_type = storage::T_DML_DELETE;
        }
      } else {
        // Scan compact map & write cells to writer.
        uint64_t col_id = OB_INVALID_ID;
        ObObj cell;
        while (OB_SUCCESS == ret && OB_SUCCESS == (ret = map_.get_next(col_id, cell))) {
          if (OB_FAIL(writer.append(col_id, cell))) {
            TRANS_LOG(WARN, "failed to append cell to compact cell writer", K(ret), K(col_id), K(cell));
          }
        }
        ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
      }
      if (OB_SUCC(ret) && OB_FAIL(writer.row_finish())) {
        TRANS_LOG(WARN, "compact cell writer row finish error", K(ret));
      } else {
        // Build trans node & insert it to its place.
        ObMemtableData mtd(dml_type, writer.size(), writer.get_buf());
        int64_t node_size = (int64_t)sizeof(*trans_node) + mtd.dup_size();
        if (OB_ISNULL(trans_node = (ObMvccTransNode*)node_alloc_->alloc(node_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "failed to alloc trans node", K(ret), K(node_size));
        } else if (OB_FAIL(
                       ObMemtableDataHeader::build(reinterpret_cast<ObMemtableDataHeader*>(trans_node->buf_), &mtd))) {
          TRANS_LOG(WARN, "failed to dup data to trans node", K(ret), K(trans_node->buf_));
        } else if (INT64_MAX == save->trans_version_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected trans version", K(ret), "node", *save);
        } else {
          trans_node->trans_version_ = save->trans_version_;
          trans_node->modify_count_ = save->modify_count_;
          trans_node->acc_checksum_ = save->acc_checksum_;
          trans_node->version_ = save->version_;
          trans_node->type_ = NDT_COMPACT;
          trans_node->flag_ = save->flag_;
          trans_node->set_overflow(overflow);
          trans_node->log_timestamp_ = save->log_timestamp_;
          trans_node->set_snapshot_version_barrier(snapshot_version);
        }
      }
    }
  }

  return OB_SUCC(ret) ? trans_node : NULL;
}

int ObMemtableRowCompactor::insert_compact_node_(
    ObMvccTransNode* trans_node, ObMvccTransNode** start, ObMvccTransNode* save, ObMvccTransNode* next_node)
{
  int ret = OB_SUCCESS;

  trans_node->prev_ = save;
  ATOMIC_STORE(&(save->next_), trans_node);

  if (!ATOMIC_BCAS(start, save, trans_node)) {
    TRANS_LOG(WARN, "insert compact node failed", "start", **start, "save", *save, "compact", *trans_node);
  } else if (OB_ISNULL(next_node)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "next_node should not be NULL", "start", **start, "save", *save, "compact", *trans_node);
  } else {
    ATOMIC_STORE(&(trans_node->next_), next_node);
    ATOMIC_STORE(&(row_->latest_compact_node_), trans_node);
    const int64_t end_ts = ObTimeUtility::current_time();
    ATOMIC_STORE(&(row_->latest_compact_ts_), end_ts);
    ATOMIC_STORE(&(row_->update_since_compact_), 0);
  }

  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
