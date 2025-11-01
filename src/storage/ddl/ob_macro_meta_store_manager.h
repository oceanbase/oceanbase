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

#ifndef _OCEANBASE_STORAGE_DDL_OB_MACRO_META_STORE_MANAGER_H_
#define _OCEANBASE_STORAGE_DDL_OB_MACRO_META_STORE_MANAGER_H_

#include "storage/ddl/ob_ddl_struct.h"

namespace oceanbase
{

namespace blocksstable
{
class ObMacroMetaTempStore;
}

namespace storage
{

class ObMacroMetaStoreManager
{
public:
  struct StoreItem
  {
  public:
    StoreItem() : macro_meta_store_(nullptr), cg_idx_(-1), parallel_idx_(-1), lob_start_seq_(0) {}
    TO_STRING_KV(KP(macro_meta_store_), K(tablet_id_), K(cg_idx_), K(parallel_idx_), K(lob_start_seq_));
  public:
    blocksstable::ObMacroMetaTempStore *macro_meta_store_;
    ObTabletID tablet_id_; // for distinguish data tablet and lob meta tablet
    int64_t cg_idx_;
    int64_t parallel_idx_;
    int64_t lob_start_seq_;
  };
public:
  ObMacroMetaStoreManager();
  ~ObMacroMetaStoreManager();
  // 对于非lob表, lob_start_seq统一填0
  // 对于lob表, parallel_idx是主表的slice_idx, 一个主表slice可能会对应多个lob slice, 且lob slice数据与主表slice_idx没有顺序关系, 需要根据lob slice的start_seq进行排序
  int add_macro_meta_store(const ObTabletID &tablet_id, const int64_t cg_idx, const int64_t parallel_idx, const int64_t lob_start_seq, blocksstable::ObMacroMetaTempStore *&macro_meta_store);
  int get_sorted_macro_meta_stores(const ObTabletID &tablet_id, const int64_t cg_idx, ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores);

  int add_macro_meta_store(const int64_t cg_idx, const int64_t parallel_idx, const int64_t dir_id, blocksstable::ObMacroMetaTempStore *&macro_meta_store);
  int get_sorted_macro_meta_stores(const int64_t cg_idx, ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores);
  int get_macro_meta_store(const ObTabletID &tablet_id, const int64_t cg_idx, const int64_t parallel_idx, blocksstable::ObMacroMetaTempStore *&macro_meta_store);

  TO_STRING_KV(K(dir_id_), K(store_items_.count()));
private:
  int add_store_item(const ObTabletID &tablet_id, const int64_t cg_idx, const int64_t parallel_idx, const int64_t lob_start_seq, const int64_t dir_id, blocksstable::ObMacroMetaTempStore *&macro_meta_store);
private:
  ObArenaAllocator allocator_;
  lib::ObMutex mutex_;
  int64_t dir_id_;
  ObArray<StoreItem> store_items_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif//_OCEANBASE_STORAGE_DDL_OB_MACRO_META_STORE_MANAGER_H_
