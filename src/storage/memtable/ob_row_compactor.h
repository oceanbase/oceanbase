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
#include "share/scn.h"

namespace oceanbase
{

namespace common
{
class ObIAllocator;
}

namespace storage
{
class ObTxTableGuard;
}

namespace memtable
{

class ObMemtable;
struct ObMvccRow;
struct ObMvccTransNode;

// Memtable Row Compactor.
class ObMemtableRowCompactor
{
public:
  ObMemtableRowCompactor();
  virtual ~ObMemtableRowCompactor();
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableRowCompactor);
public:
  int init(ObMvccRow *row,
           ObMemtable *mt,
           common::ObIAllocator *node_alloc);
  // compact and refresh the update counter by snapshot version
  int compact(const share::SCN snapshot_version,
              const int64_t flag);
private:
  void find_start_pos_(const share::SCN snapshot_version,
                       ObMvccTransNode *&save);
  ObMvccTransNode *construct_compact_node_(const share::SCN snapshot_version,
                                           const int64_t flag,
                                           ObMvccTransNode *save);
  int try_cleanout_tx_node_during_compact_(storage::ObTxTableGuard &tx_table_guard,
                                            ObMvccTransNode *tnode);
  void insert_compact_node_(ObMvccTransNode *trans_node,
                            ObMvccTransNode *save);
private:
  bool is_inited_;
  ObMvccRow *row_;
  ObMemtable *memtable_;
  common::ObIAllocator *node_alloc_;
};


}
}



#endif
