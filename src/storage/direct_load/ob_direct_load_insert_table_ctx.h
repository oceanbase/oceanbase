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
#include "lib/lock/ob_mutex.h"
#include "share/ob_ls_id.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/table/ob_table_load_define.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "sql/engine/px/ob_sub_trans_ctrl.h"

namespace oceanbase
{
namespace sql
{
class ObDDLCtrl;
}

namespace storage
{
struct ObDirectLoadInsertTableRowIteratorParam;
struct ObDirectLoadInsertTableParam
{
public:
  ObDirectLoadInsertTableParam();
  ~ObDirectLoadInsertTableParam();
  int assign(const ObDirectLoadInsertTableParam &other);
  bool is_valid() const;
  TO_STRING_KV(K_(table_id), K_(schema_version), K_(snapshot_version), K_(execution_id),
               K_(ddl_task_id), K_(data_version), K_(reserved_parallel), K_(ls_partition_ids),
               K_(target_ls_partition_ids));

public:
  uint64_t table_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t execution_id_;
  int64_t ddl_task_id_;
  int64_t data_version_;
  int64_t reserved_parallel_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> ls_partition_ids_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> target_ls_partition_ids_;
};

struct ObDirectLoadInsertTabletParam
{
public:
  ObDirectLoadInsertTabletParam();
  ~ObDirectLoadInsertTabletParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(ls_id), K_(table_id), K_(origin_tablet_id), K_(tablet_id),
               K_(lob_tablet_id), K_(schema_version), K_(snapshot_version), K_(execution_id), K_(ddl_task_id),
               K_(data_version), K_(reserved_parallel), K_(context_id));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID lob_tablet_id_;
  common::ObTabletID origin_tablet_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t execution_id_;
  int64_t ddl_task_id_;
  int64_t data_version_;
  int64_t reserved_parallel_;
  int64_t context_id_;
};

struct ObDirectLoadInsertTabletWriteCtx
{
  blocksstable::ObMacroDataSeq start_seq_;
  share::ObTabletCacheInterval pk_interval_;
  TO_STRING_KV(K_(start_seq), K_(pk_interval));
};

class ObDirectLoadInsertTabletContext
{
  static const int64_t PK_CACHE_SIZE = 5000000;
  static const int64_t WRITE_BATCH_SIZE = 5000000;
public:
  ObDirectLoadInsertTabletContext();
  ~ObDirectLoadInsertTabletContext();
  bool is_open() const { return is_open_; }
  const common::ObTabletID &get_tablet_id() const { return param_.tablet_id_; }
public:
  int init(const ObDirectLoadInsertTabletParam &param);
  int open();
  int close();
  int open_sstable_slice(const blocksstable::ObMacroDataSeq &start_seq, int64_t &slice_id);
  int close_sstable_slice(const int64_t slice_id);
  int fill_sstable_slice(const int64_t &slice_id, ObIStoreRowIterator &iter,
                         int64_t &affected_rows);
  int get_write_ctx(ObDirectLoadInsertTabletWriteCtx &write_ctx);
  int open_lob_sstable_slice(const blocksstable::ObMacroDataSeq &start_seq, int64_t &slice_id);
  int close_lob_sstable_slice(const int64_t slice_id);
  int fill_lob_sstable_slice(ObIAllocator &allocator, const int64_t &lob_slice_id, share::ObTabletCacheInterval &pk_interval,
                             blocksstable::ObDatumRow &datum_row);

  int get_lob_write_ctx(ObDirectLoadInsertTabletWriteCtx &write_ctx);
  int calc_range();
  int fill_column_group(const int64_t thread_cnt, const int64_t thread_id);
  int cancel();
  TO_STRING_KV(K_(param), K_(is_open));
private:
  int get_pk_interval(uint64_t count, share::ObTabletCacheInterval &pk_interval);
  int get_lob_pk_interval(uint64_t count, share::ObTabletCacheInterval &pk_interval);
  int refresh_pk_cache(const common::ObTabletID &tablet_id, share::ObTabletCacheInterval &pk_cache);
private:
  ObDirectLoadInsertTabletParam param_;
  volatile bool is_open_;
  lib::ObMutex mutex_;
  blocksstable::ObMacroDataSeq start_seq_;
  blocksstable::ObMacroDataSeq lob_start_seq_;
  share::ObTabletCacheInterval pk_cache_;
  share::ObTabletCacheInterval lob_pk_cache_;
  share::SCN start_scn_;
  ObTabletDirectLoadMgrHandle handle_;
  bool is_inited_;
};

class ObDirectLoadInsertTableContext
{
private:
  typedef common::hash::ObHashMap<common::ObTabletID, ObDirectLoadInsertTabletContext *>
    TABLET_CTX_MAP;
public:
  ObDirectLoadInsertTableContext();
  ~ObDirectLoadInsertTableContext();
  void destory();
  int init(const ObDirectLoadInsertTableParam &param);
  int get_tablet_context(const common::ObTabletID &tablet_id,
                         ObDirectLoadInsertTabletContext *&tablet_ctx) const;
  void cancel();
  TO_STRING_KV(K_(param));
private:
  int create_all_tablet_contexts();
private:
  ObDirectLoadInsertTableParam param_;
  TABLET_CTX_MAP tablet_ctx_map_;
  common::ObArenaAllocator allocator_;
  sql::ObDDLCtrl ddl_ctrl_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
