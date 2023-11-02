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

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;

class ObTableLoadManager
{
public:
  ObTableLoadManager();
  ~ObTableLoadManager();
  int init();
  // table ctx holds a reference count
  int add_table_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *table_ctx);
  int remove_table_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *table_ctx);
  // table ctx holds a reference count
  int get_all_table_ctx(common::ObIArray<ObTableLoadTableCtx *> &table_ctx_array);
  // table ctx holds a reference count
  int get_table_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *&table_ctx);
  // table ctx holds a reference count
  int get_table_ctx_by_table_id(uint64_t table_id, ObTableLoadTableCtx *&table_ctx);
  // all table ctx hold a reference count
  int get_inactive_table_ctx_list(common::ObIArray<ObTableLoadTableCtx *> &table_ctx_array);
  void put_table_ctx(ObTableLoadTableCtx *table_ctx);
  int64_t get_table_ctx_count() const;
  // table ctx no reference counting
  int get_releasable_table_ctx_list(common::ObIArray<ObTableLoadTableCtx *> &table_ctx_array);
  int64_t get_dirty_list_count() const;
private:
  int add_dirty_list(ObTableLoadTableCtx *table_ctx);
private:
  // key => table_ctx
  typedef common::hash::ObHashMap<ObTableLoadUniqueKey, ObTableLoadTableCtx *,
                                  common::hash::NoPthreadDefendMode>
    TableCtxMap;
  // table_id => table_ctx
  typedef common::hash::ObHashMap<uint64_t, ObTableLoadTableCtx *,
                                  common::hash::NoPthreadDefendMode>
    TableCtxIndexMap;

  class HashMapEraseIfEqual
  {
  public:
    HashMapEraseIfEqual(ObTableLoadTableCtx *table_ctx) : table_ctx_(table_ctx) {}
    bool operator()(
      common::hash::HashMapPair<ObTableLoadUniqueKey, ObTableLoadTableCtx *> &entry) const
    {
      return table_ctx_ == entry.second;
    }
    bool operator()(common::hash::HashMapPair<uint64_t, ObTableLoadTableCtx *> &entry) const
    {
      return table_ctx_ == entry.second;
    }
  public:
    ObTableLoadTableCtx *table_ctx_;
  };
private:
  mutable obsys::ObRWLock rwlock_;
  TableCtxMap table_ctx_map_;
  TableCtxIndexMap table_ctx_index_map_; // index of the latest task
  // for release table ctx in background
  mutable lib::ObMutex mutex_;
  common::ObDList<ObTableLoadTableCtx> dirty_list_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
