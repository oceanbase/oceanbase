// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

#pragma once

#include "common/row/ob_row.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_row.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadTableCtx;

class ObTableLoadErrorRowHandler
{
public:
  static const int64_t DEFAULT_REPEATED_ERROR_ROW_COUNT = 100;
  ObTableLoadErrorRowHandler();
  ~ObTableLoadErrorRowHandler();
  int init(ObTableLoadTableCtx *const ctx);
  int append_error_row(const common::ObNewRow &row);
  int append_error_row(const blocksstable::ObDatumRow &row);
  int append_repeated_row(const common::ObNewRow &row);
  int append_repeated_row(const blocksstable::ObDatumRow &row);
  int get_all_error_rows(table::ObTableLoadArray<common::ObObj> &obj_array);
  uint64_t get_error_row_cnt() const { return error_row_cnt_; }
  int check_rowkey_order(int32_t session_id, const common::ObTabletID &tablet_id,
                         const blocksstable::ObDatumRow &datum_row);
  sql::ObLoadDupActionType get_action() const {return param_.dup_action_;}
  uint64_t get_capacity() const { return capacity_; }
  TO_STRING_KV(K_(capacity), K_(error_row_cnt), K_(repeated_row_cnt), K_(session_cnt), K_(is_inited));
private:
  int inner_append_error_row(const common::ObNewRow &row,
                             common::ObIArray<ObNewRow> &error_row_array);
  int inner_append_repeated_row(const common::ObNewRow &row,
                                common::ObIArray<ObNewRow> &repeated_row_array);
  class PartitionRowkey
  {
  public:
    PartitionRowkey() : allocator_("TLD_err_chk")
    {
      allocator_.set_tenant_id(MTL_ID());
      last_rowkey_.set_min_rowkey();
    }
    ~PartitionRowkey();
    TO_STRING_KV(K(last_rowkey_));
  public:
    common::ObArenaAllocator allocator_;
    blocksstable::ObDatumRowkey last_rowkey_;
  };
  class PartitionRowkeyMap
  {
  public:
    PartitionRowkeyMap() : map_() {}
    ~PartitionRowkeyMap();
    PartitionRowkeyMap(const PartitionRowkeyMap &other) {}
    PartitionRowkeyMap &operator=(const PartitionRowkeyMap &other) { return *this; }
    TO_STRING_KV(K(map_.size()));
    // all partitions are written sequentially within one session
    // thus they can share the same allocator
    common::ObArenaAllocator allocator_;
    common::hash::ObHashMap<common::ObTabletID, PartitionRowkey *> map_;
  };
private:
  observer::ObTableLoadParam param_;
  sql::ObLoadDataStat *job_stat_;
  const oceanbase::blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  uint64_t capacity_; // maximum allowed error row count
  int64_t rowkey_column_num_;
  int32_t session_cnt_;
  common::ObArray<PartitionRowkeyMap> session_maps_;
  common::ObArenaAllocator row_allocator_; //just for safe allocator
  common::ObSafeArenaAllocator safe_allocator_; //该分配器是线程安全的
  mutable lib::ObMutex append_row_mutex_;
  uint64_t error_row_cnt_;
  uint64_t repeated_row_cnt_;
  common::ObArray<ObNewRow> error_row_array_;
  common::ObArray<ObNewRow> error_new_row_array_;
  common::ObArray<ObNewRow> repeated_row_array_;
  common::ObArray<ObNewRow> repeated_new_row_array_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
