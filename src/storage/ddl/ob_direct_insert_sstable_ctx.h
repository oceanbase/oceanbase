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

#ifndef OCEANBASE_STORAGE_OB_DIRECT_INSERT_SSTABLE_CTX_H
#define OCEANBASE_STORAGE_OB_DIRECT_INSERT_SSTABLE_CTX_H

#include "storage/meta_mem/ob_tablet_handle.h"
#include "lib/lock/ob_mutex.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_bucket_lock.h"
#include "common/ob_tablet_id.h"
#include "common/row/ob_row_iterator.h"
#include "storage/ob_i_table.h"
#include "storage/ob_row_reshape.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/tx_storage/ob_ls_map.h"

namespace oceanbase
{
namespace sql
{
class ObPxMultiPartSSTableInsertOp;
class ObExecContext;
}

namespace blocksstable
{
class ObSSTableMergeRes;
}

namespace share
{
struct ObTabletCacheInterval;
}

namespace storage
{
class ObDDLRedoLogWriterCallback;
class ObTablet;

struct ObSSTableInsertTabletParam final
{
public:
  ObSSTableInsertTabletParam();
  ~ObSSTableInsertTabletParam();
  bool is_valid() const;
  TO_STRING_KV(K(context_id_), K(ls_id_), K(tablet_id_), K(table_id_), K(write_major_),
      K(task_cnt_), K(schema_version_), K(snapshot_version_), K_(execution_id), K_(ddl_task_id));
public:
  int64_t context_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  uint64_t table_id_;
  bool write_major_;
  int64_t task_cnt_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t execution_id_;
  int64_t ddl_task_id_;
};

typedef std::pair<share::ObLSID, common::ObTabletID> LSTabletIDPair;

class ObSSTableInsertRowIterator : public common::ObNewRowIterator
{
public:
  ObSSTableInsertRowIterator(sql::ObExecContext &exec_ctx, sql::ObPxMultiPartSSTableInsertOp *op);
  virtual ~ObSSTableInsertRowIterator();
  virtual void reset() override;
  virtual int get_next_row(common::ObNewRow *&row) override;
  int get_sql_mode(ObSQLMode &sql_mode) const;
  int get_next_row_with_tablet_id(
      const uint64_t table_id,
      const int64_t rowkey_count,
      const int64_t snapshot_version,
      common::ObNewRow *&row,
      common::ObTabletID &tablet_id);
  common::ObTabletID get_current_tablet_id() const;
private:
  sql::ObExecContext &exec_ctx_;
  sql::ObPxMultiPartSSTableInsertOp *op_;
  common::ObNewRow current_row_;
  common::ObTabletID current_tablet_id_;
  bool is_next_row_cached_;
};

class ObSSTableInsertTabletContext final
{
public:
  ObSSTableInsertTabletContext();
  ~ObSSTableInsertTabletContext();
  int init(const ObSSTableInsertTabletParam &build_param);
  int update(const int64_t snapshot_version);
  int build_sstable_slice(
      const ObSSTableInsertTabletParam &build_param,
      const blocksstable::ObMacroDataSeq &start_seq,
      common::ObNewRowIterator &iter,
      int64_t &affected_rows);
  int create_sstable();
  int inc_finish_count(bool &is_ready);
  int get_tablet_cache_interval(share::ObTabletCacheInterval &interval);
  TO_STRING_KV(K(build_param_), K(sstable_created_));
private:
  int prepare_reshape(
    const share::schema::ObTableSchema *table_schema,
    share::schema::ObTableSchemaParam &schema_param,
    ObRelativeTable &relative_table) const;
  int check_null(
    const share::schema::ObTableSchema &table_schema,
    const ObNewRow &row,
    const int64_t rowkey_column_num) const;
  int create_sstable_with_clog(
      ObTablet *tablet,
      const ObITable::TableKey &table_key,
      const int64_t table_id);
  int get_table_key(ObITable::TableKey &table_key);
  int prepare_index_builder_if_need(const share::schema::ObTableSchema &table_schema);

private:
  lib::ObMutex mutex_;
  common::ObConcurrentFIFOAllocator allocator_;
  ObSSTableInsertTabletParam build_param_;
  ObLSHandle ls_handle_;
  ObTabletHandle tablet_handle_;
  ObDDLSSTableRedoWriter data_sstable_redo_writer_;
  bool sstable_created_;
  int64_t task_finish_count_;
  blocksstable::ObSSTableIndexBuilder *index_builder_;
  int64_t task_id_;
  ObDDLKvMgrHandle ddl_kv_mgr_handle_; // for keeping ddl kv mgr alive
};

struct ObSSTableInsertTableParam final
{
public:
  ObSSTableInsertTableParam();
  ~ObSSTableInsertTableParam() = default;
  int assign(const ObSSTableInsertTableParam &other);
  bool is_valid() const { return exec_ctx_ != nullptr && OB_INVALID_ID != dest_table_id_
    && schema_version_ >= 0 && snapshot_version_ >= 0 && task_cnt_ >= 0
    && execution_id_ > 0 && ddl_task_id_ > 0 && ls_tablet_ids_.count() > 0; }
  TO_STRING_KV(K_(context_id), K_(dest_table_id), K_(write_major), K_(schema_version), K_(snapshot_version),
      K_(task_cnt), K_(execution_id), K_(ddl_task_id), K_(ls_tablet_ids));
public:
  sql::ObExecContext *exec_ctx_;
  int64_t context_id_;
  int64_t dest_table_id_;
  bool write_major_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t task_cnt_;
  int64_t execution_id_;
  int64_t ddl_task_id_;
  common::ObArray<LSTabletIDPair> ls_tablet_ids_;
};

class ObSSTableInsertTableContext final
{
public:
  ObSSTableInsertTableContext();
  ~ObSSTableInsertTableContext();
  int init(const ObSSTableInsertTableParam &param);
  int update_context(const int64_t snapshot_version);
  int add_sstable_slice(
      const ObSSTableInsertTabletParam &build_param,
      const blocksstable::ObMacroDataSeq &start_seq,
      common::ObNewRowIterator &iter,
      int64_t &affected_rows);
  int finish(const bool need_commit);
  int get_tablet_ids(common::ObIArray<ObTabletID> &tablet_ids);
  int notify_tablet_end(const ObTabletID &tablet_id);
  int finish_ready_tablets(const int64_t target_count);
  int get_tablet_cache_interval(const ObTabletID &tablet_id,
                                share::ObTabletCacheInterval &interval);
private:
  void destroy();
  int create_all_tablet_contexts(const common::ObIArray<LSTabletIDPair> &ls_tablet_ids);
  int get_tablet_context(const common::ObTabletID &tablet_id, ObSSTableInsertTabletContext *&tablet_ctx);
  int remove_tablet_context(const common::ObTabletID &tablet_id);
private:
  typedef
  common::hash::ObHashMap<
    common::ObTabletID,
    ObSSTableInsertTabletContext *,
    common::hash::NoPthreadDefendMode> TABLET_CTX_MAP;
  bool is_inited_;
  common::ObSpinLock lock_;
  ObSSTableInsertTableParam param_;
  common::ObConcurrentFIFOAllocator allocator_;
  TABLET_CTX_MAP tablet_ctx_map_;
  ObArray<ObTabletID> ready_tablets_;
  int64_t finishing_idx_;
};

class ObSSTableInsertManager final
{
public:
  static ObSSTableInsertManager &get_instance();
  int init();
  int create_table_context(
    const ObSSTableInsertTableParam &build_param,
    int64_t &context_id);
  int finish_table_context(const int64_t context_id, const bool need_commit);
  int update_table_context(
      const int64_t context_id,
      const int64_t snapshot_version);
  int add_sstable_slice(
      const ObSSTableInsertTabletParam &build_param,
      const blocksstable::ObMacroDataSeq &start_seq,
      common::ObNewRowIterator &iter,
      int64_t &affected_rows);
  void destroy();
  int get_tablet_ids(const int64_t context_id, common::ObIArray<ObTabletID> &tablet_ids);
  int notify_tablet_end(const int64_t context_id, const ObTabletID &tablet_id);
  int finish_ready_tablets(const int64_t context_id, const int64_t target_count);
  int get_tablet_cache_interval(const int64_t context_id,
                                const ObTabletID &tablet_id,
                                share::ObTabletCacheInterval &interval);
private:
  ObSSTableInsertManager();
  ~ObSSTableInsertManager();
  int get_context(
      const int64_t context_id,
      ObSSTableInsertTableContext *&ctx);
  int get_context_no_lock(
      const int64_t context_id,
      ObSSTableInsertTableContext *&ctx);
  int remove_context(const int64_t context_id);
  int64_t alloc_context_id();
  uint64_t get_context_id_hash(const int64_t context_id);
private:
  typedef common::hash::ObHashMap<
    int64_t, // context id
    ObSSTableInsertTableContext *,
    common::hash::NoPthreadDefendMode> TABLE_CTX_MAP;
  bool is_inited_;
  lib::ObMutex mutex_;
  common::ObBucketLock bucket_lock_;
  common::ObConcurrentFIFOAllocator allocator_;
  int64_t context_id_generator_;
  TABLE_CTX_MAP table_ctx_map_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableInsertManager);
};

}// namespace storage
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_OB_DIRECT_INSERT_SSTABLE_CTX_H
