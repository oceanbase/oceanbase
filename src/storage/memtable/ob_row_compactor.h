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

#ifndef OCEANBASE_STORAGE_OB_ROW_COMPACTOR_
#define OCEANBASE_STORAGE_OB_ROW_COMPACTOR_

#include "share/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_small_allocator.h"
#include "lib/lock/ob_spin_lock.h"
#include "common/object/ob_object.h"

namespace oceanbase {

namespace common {
class ObIAllocator;
}

namespace memtable {

class ObMvccRow;
class ObMvccTransNode;

class ICompactMap {
public:
  ICompactMap()
  {}
  virtual ~ICompactMap()
  {}
  virtual int init() = 0;
  virtual void destroy() = 0;
  virtual int set(const uint64_t col_id, const common::ObObj& cell) = 0;
  virtual int get_next(uint64_t& col_id, common::ObObj& cell) = 0;
};

// Map with thread local array, which runs faster.
class CompactMapImproved : public ICompactMap {
private:
  struct Node {
    uint32_t ver_;
    uint32_t col_id_;
    common::ObObj cell_;
    Node* next_;
  };

private:
  static const int64_t BKT_N_BIT_SHIFT = 9;           // 2^9=512. Perf opt.
  static const int64_t BKT_N = 1 << BKT_N_BIT_SHIFT;  // 512
  static const int64_t BKT_N_MOD_MASK = BKT_N - 1;
  static const uint32_t INVALID_COL_ID = UINT16_MAX;
  static const uint32_t MAX_VER = UINT32_MAX;

private:
  class StaticMemoryHelper {
  public:
    StaticMemoryHelper();
    ~StaticMemoryHelper();

  public:
    Node* get_tl_arr();
    uint32_t& get_tl_arr_ver();
    Node* get_node();
    void revert_node(Node* n);

  private:
    static RLOCAL(Node*, bkts_);    // Thread local array.
    static RLOCAL(uint32_t, ver_);  // Thread local array version.
    common::PageArena<> arr_arena_;
    common::ObSpinLock arr_arena_lock_;
    common::ObSmallAllocator node_alloc_;
  };

public:
  CompactMapImproved() : bkts_(NULL), ver_(0), scan_cur_bkt_(NULL), scan_cur_node_(NULL)
  {}
  virtual ~CompactMapImproved()
  {}
  int init();
  void destroy();
  int set(const uint64_t col_id, const common::ObObj& cell);
  int get_next(uint64_t& col_id, common::ObObj& cell);

private:
  DISALLOW_COPY_AND_ASSIGN(CompactMapImproved);

private:
  Node* bkts_;
  uint32_t ver_;
  Node* scan_cur_bkt_;
  Node* scan_cur_node_;
  static StaticMemoryHelper mem_helper_;
};

// Memtable Row Compactor.
class ObMemtableRowCompactor {
public:
  ObMemtableRowCompactor();
  virtual ~ObMemtableRowCompactor();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableRowCompactor);

public:
  int init(ObMvccRow* row, common::ObIAllocator* node_alloc);
  // compact and refresh the update counter by snapshot version
  int compact(const int64_t snapshot_version);

private:
  void find_start_pos_(
      const int64_t snapshot_version, ObMvccTransNode**& start, ObMvccTransNode*& save, ObMvccTransNode*& next_node);
  ObMvccTransNode* construct_compact_node_(const int64_t snapshot_version, ObMvccTransNode* save);
  int insert_compact_node_(
      ObMvccTransNode* trans_node, ObMvccTransNode** start, ObMvccTransNode* save, ObMvccTransNode* next_node);

private:
  bool is_inited_;
  ObMvccRow* row_;
  common::ObIAllocator* node_alloc_;
  CompactMapImproved map_ins_;
  ICompactMap& map_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif
