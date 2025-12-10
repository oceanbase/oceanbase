/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_TTL_TASK_H_
#define OCEANBASE_OBSERVER_OB_TABLE_TTL_TASK_H_
#include "share/table/ob_table_ttl_common.h"
#include "sql/ob_sql_trans_control.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "observer/table/ob_table_context.h"
#include "observer/table/ob_table_scan_executor.h"
#include "share/table/ob_table.h"
#include "observer/table/ob_table_cache.h"
#include "observer/table/ttl/ob_tenant_tablet_ttl_mgr.h"
#include "observer/table/ob_table_schema_cache.h"
namespace oceanbase
{
namespace table
{

class ObTableTTLRowIterator : public table::ObTableApiScanRowIterator
{
public:
  explicit ObTableTTLRowIterator()
    : is_inited_(false),
      max_version_cnt_(0),
      cur_version_(0),
      ttl_cnt_(0),
      scan_cnt_(0),
      last_row_(nullptr),
      has_cell_ttl_(false),
      rowkey_cnt_(0),
      iter_end_ts_(0)
  {
    column_pairs_.set_attr(ObMemAttr(MTL_ID(), "TTLRowIter"));
  }
  virtual ~ObTableTTLRowIterator() {}
  int init_common(const share::schema::ObTableSchema &table_schema);
  virtual int close() override;
public:
  struct ColumnPair
  {
    ColumnPair() = default;
    ColumnPair(uint64_t cell_idx, const common::ObString &col_name)
    : cell_idx_(cell_idx),
      col_id_(OB_INVALID_ID),
      col_name_(col_name),
      is_rowkey_column_(false),
      rowkey_pos_idx_(0)
    {}
    uint64_t cell_idx_;
    uint64_t col_id_;
    common::ObString col_name_;
    bool is_rowkey_column_;
    uint64_t rowkey_pos_idx_;
    TO_STRING_KV(K_(cell_idx), K_(col_name), K_(is_rowkey_column), K_(col_id), K_(rowkey_pos_idx));
  };

public:
  const static int64_t ONE_ITER_EXECUTE_MAX_TIME = 30 * 1000 * 1000; // 30s
  bool is_inited_;
  uint64_t max_version_cnt_; // for hbase
  uint64_t cur_version_; // for hbase
  uint64_t ttl_cnt_;
  uint64_t scan_cnt_;
  ObNewRow *last_row_;
  bool has_cell_ttl_;
  common::ObSArray<ColumnPair> column_pairs_;
  int64_t rowkey_cnt_;
  int64_t iter_end_ts_;
};

class ObTableTTLDeleteRowIterator : public ObTableTTLRowIterator
{
public:
  ObTableTTLDeleteRowIterator();
  ~ObTableTTLDeleteRowIterator() {}
  virtual int get_next_row(ObNewRow*& row);
  int init(const share::schema::ObTableSchema &index_schema,
           const ObString &ttl_definition,
           const table::ObTableTTLOperation &ttl_operation);
  virtual int close() override;
private:
  int covert_row_to_entity(const ObNewRow &row, ObTableEntity &entity);
public:
  common::ObArenaAllocator hbase_kq_allocator_;
  int32_t max_version_;
  int64_t time_to_live_ms_; // ttl in millisecond
  uint64_t limit_del_rows_; // maximum delete row
  uint64_t cur_del_rows_; // current delete row
  ObString cur_rowkey_; // K
  ObString cur_qualifier_; // Q
  bool is_last_row_ttl_; // false indicate row del by max version
  bool is_hbase_table_;
  bool has_cell_ttl_;
  common::ObTableTTLChecker ttl_checker_;
  bool hbase_new_cq_;
};


class ObTableTTLDeleteTask : public share::ObITask
{
public:
  ObTableTTLDeleteTask();
  ~ObTableTTLDeleteTask();
  virtual int init(table::ObTabletTTLScheduler *ttl_tablet_mgr,
                   const table::ObTTLTaskParam &ttl_para,
                   table::ObTTLTaskInfo &ttl_info);
  virtual int process() override;
protected:
  virtual int get_scan_ranges(ObIArray<ObNewRange> &ranges, const ObKVAttr &kv_attributes);
  virtual int init_ob_table_query(ObTableQuery &query, ObKVAttr &attr);
  int init_tb_ctx(ObKvSchemaCacheGuard &schema_cache_guard,
                  const table::ObITableEntity &entity,
                  table::ObTableCtx &ctx);
  int init_scan_tb_ctx(ObKvSchemaCacheGuard &schema_cache_guard,
                       ObTableQuery &query,
                       table::ObTableCtx &tb_ctx,
                       table::ObTableApiCacheGuard &cache_guard);
  int execute_ttl_delete(ObKvSchemaCacheGuard &schema_cache_guard,
                         ObTableTTLRowIterator &ttl_row_iter,
                         table::ObTableTTLOperationResult &result,
                         transaction::ObTxDesc *trans_desc,
                         transaction::ObTxReadSnapshot &snapshot);
  int process_ttl_delete(ObKvSchemaCacheGuard &schema_cache_guard,
                         const table::ObITableEntity &new_entity,
                         int64_t &affected_rows,
                         transaction::ObTxDesc *trans_desc,
                         transaction::ObTxReadSnapshot &snapshot,
                         bool is_skip_scan);
  common::ObIAllocator &get_allocator() { return allocator_; }
  int init_credential(const table::ObTTLTaskParam &ttl_param);
  int init_sess_info();
  int init_schema_info(int64_t tenant_id, uint64_t table_id);
  int init_kv_schema_guard(ObKvSchemaCacheGuard &schema_cache_guard);
  int decide_and_check_scan_index(const share::schema::ObTableSchema &table_schema,
                                  ObKVAttr &attr);
  int construct_delete_entity(ObIArray<ObTableTTLRowIterator::ColumnPair> &column_pairs,
                              const ObNewRow &row,
                              ObTableEntity &entity);
  int init_scan_index(const ObString &scan_index);
  common::ObTabletID get_tablet_id() const
  {
    return info_.get_tablet_id();
  }
  uint64_t get_table_id() const { return param_.table_id_; }
  int64_t get_timeout_ts() { return ONE_TASK_TIMEOUT + ObTimeUtility::current_time(); }
  common::ObRowkey &get_start_rowkey() { return rowkey_; }

private:
  static const int64_t RETRY_INTERVAL = 30 * 60 * 1000 * 1000l; // 30min
  static const int64_t PER_TASK_DEL_ROWS = 1024l;
  static const int64_t ONE_TASK_TIMEOUT = 1 * 60 * 1000 * 1000l; // 1min
private:
  int process_one();
private:
  common::ObArenaAllocator rowkey_allocator_;
  bool is_inited_;
  table::ObTTLTaskParam param_;
  table::ObTTLTaskInfo info_;
  table::ObTableApiCredential credential_;
  ObTableApiSessGuard sess_guard_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObTableSchema *table_schema_;
  common::ObArenaAllocator allocator_;
  common::ObRowkey rowkey_;
  table::ObTabletTTLScheduler *ttl_tablet_scheduler_;
  share::ObLSID ls_id_;
  ObTableEntity delete_entity_;
  uint64_t hbase_cur_version_;
  const ObTableSchema* scan_index_schema_;
  ObString scan_index_;
  char scan_index_buf_[OB_MAX_OBJECT_NAME_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObTableTTLDeleteTask);
};

// TTL delete task using hbase rowkey
class ObTableHRowKeyTTLDelTask : public ObTableTTLDeleteTask
{
public:
  ObTableHRowKeyTTLDelTask();
  ~ObTableHRowKeyTTLDelTask();
  virtual int init(ObTabletTTLScheduler *ttl_tablet_scheduler,
                   const table::ObTTLHRowkeyTaskParam &ttl_para,
                   table::ObTTLTaskInfo &ttl_info);
private:
  virtual int get_scan_ranges(ObIArray<ObNewRange> &ranges, ObKVAttr &attr);

private:
  common::ObArenaAllocator hrowkey_alloc_;
  ObSEArray<ObString, 4> rowkeys_;
};

class ObTableTTLDag final: public share::ObIDag
{
public:
  ObTableTTLDag();
  virtual ~ObTableTTLDag();
  virtual bool operator==(const ObIDag& other) const override;
  virtual uint64_t hash() const override;
  int init(const table::ObTTLTaskParam &param, table::ObTTLTaskInfo &info);
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const { return false; }
private:
  bool is_inited_;
  table::ObTTLTaskParam param_;
  table::ObTTLTaskInfo info_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObTableTTLDag);
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_TTL_TASK_H_ */
