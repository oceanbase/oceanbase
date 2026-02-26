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

#ifndef OB_STORAGE_MEMTABLE_OB_MEMTABLE_READ_ROW_UTIL_H_
#define OB_STORAGE_MEMTABLE_OB_MEMTABLE_READ_ROW_UTIL_H_

#include "share/ob_define.h"

namespace oceanbase {

namespace common {
class ObStoreRowkey;
}

namespace storage {
class ObITableReadInfo;
}

namespace blocksstable {
class ObDatumRow;
}

namespace memtable {
class ObMvccValueIterator;
class ObNopBitMap;
class ObMvccTransNode;

class ObReadRow {
  DEFINE_ALLOCATOR_WRAPPER
public:
  static int iterate_row(const storage::ObITableReadInfo &read_info,
                         const common::ObStoreRowkey &key,
                         ObMvccValueIterator &value_iter,
                         blocksstable::ObDatumRow &row,
                         ObNopBitMap &bitmap,
                         int64_t &row_scn);

  static int iterate_delete_insert_row(const storage::ObITableReadInfo &read_info,
                                       const ObStoreRowkey &key,
                                       ObMvccValueIterator &value_iter,
                                       blocksstable::ObDatumRow &latest_row,
                                       blocksstable::ObDatumRow &earliest_row,
                                       ObNopBitMap &bitmap,
                                       int64_t &latest_row_scn,
                                       int64_t &earliest_row_scn,
                                       int64_t &acquired_row_cnt);
  static int iterate_row_key(const common::ObStoreRowkey &rowkey, blocksstable::ObDatumRow &row);

private:
  DISALLOW_COPY_AND_ASSIGN(ObReadRow);
private:
  static int iterate_row_value_(const storage::ObITableReadInfo &read_info,
                                ObMvccValueIterator &value_iter,
                                blocksstable::ObDatumRow &row,
                                ObNopBitMap &bitmap,
                                int64_t &row_scn);
  static int fill_in_row_with_tx_node_(const storage::ObITableReadInfo &read_info,
                                       ObMvccValueIterator &value_iter,
                                       const ObMvccTransNode *tx_node,
                                       blocksstable::ObDatumRow &row,
                                       ObNopBitMap &bitmap,
                                       int64_t &row_scn,
                                       bool &read_finished);
  static int acquire_delete_insert_tx_node_(ObMvccValueIterator &value_iter,
                                            const ObMvccTransNode *&latest_tx_node,
                                            const ObMvccTransNode *&earliest_tx_node);
};

}  // namespace memtable
}  // namespace oceanbase
#endif