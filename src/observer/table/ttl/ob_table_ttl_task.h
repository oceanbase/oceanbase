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

class ObTableTTLDeleteRowIterator : public table::ObTableApiScanRowIterator
{
public:
  ObTableTTLDeleteRowIterator();
  ~ObTableTTLDeleteRowIterator() {}
  virtual int get_next_row(ObNewRow*& row);
  int init(const share::schema::ObTableSchema &table_schema, const table::ObTableTTLOperation &ttl_operation);
  int64_t get_rowkey_column_cnt() const { return rowkey_cnt_; }

public:
  struct PropertyPair
  {
    PropertyPair() = default;
    PropertyPair(uint64_t cell_idx, const common::ObString &property_name)
    : cell_idx_(cell_idx),
      property_name_(property_name)
    {}
    uint64_t cell_idx_;
    common::ObString property_name_;
    TO_STRING_KV(K_(cell_idx), K_(property_name));
  };

public:
  const static int64_t ONE_ITER_EXECUTE_MAX_TIME = 30 * 1000 * 1000; // 30s
  common::ObArenaAllocator hbase_kq_allocator_;
  bool is_inited_;
  int32_t max_version_;
  int64_t time_to_live_ms_; // ttl in millisecond
  uint64_t limit_del_rows_; // maximum delete row
  uint64_t cur_del_rows_; // current delete row
  uint64_t cur_version_;
  ObString cur_rowkey_; // K
  ObString cur_qualifier_; // Q
  uint64_t max_version_cnt_;
  uint64_t ttl_cnt_;
  uint64_t scan_cnt_;
  bool is_last_row_ttl_; // false indicate row del by max version
  bool is_hbase_table_;
  ObNewRow *last_row_;
  common::ObTableTTLChecker ttl_checker_;
  int64_t rowkey_cnt_;
  // map new row -> rowkey column
  common::ObSArray<uint64_t> rowkey_cell_ids_;
  // map new row -> normal column
  common::ObSArray<PropertyPair> properties_pairs_;
  bool hbase_new_cq_;
  int64_t iter_end_ts_;
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
  virtual int get_scan_ranges(ObIArray<ObNewRange> &ranges);

  int init_tb_ctx(ObKvSchemaCacheGuard &schema_cache_guard,
                  const table::ObITableEntity &entity,
                  table::ObTableCtx &ctx);
  int init_scan_tb_ctx(ObKvSchemaCacheGuard &schema_cache_guard,
                       table::ObTableCtx &tb_ctx,
                       table::ObTableApiCacheGuard &cache_guard);
  int execute_ttl_delete(ObKvSchemaCacheGuard &schema_cache_guard,
                         ObTableTTLDeleteRowIterator &ttl_row_iter,
                         table::ObTableTTLOperationResult &result,
                         transaction::ObTxDesc *trans_desc,
                         transaction::ObTxReadSnapshot &snapshot);
  int process_ttl_delete(ObKvSchemaCacheGuard &schema_cache_guard,
                         const table::ObITableEntity &new_entity,
                         int64_t &affected_rows,
                         transaction::ObTxDesc *trans_desc,
                         transaction::ObTxReadSnapshot &snapshot);
  common::ObIAllocator &get_allocator() { return allocator_; }
  int init_credential(const table::ObTTLTaskParam &ttl_param);
  int init_sess_info();
  int init_schema_info(int64_t tenant_id, uint64_t table_id);
  int init_kv_schema_guard(ObKvSchemaCacheGuard &schema_cache_guard);

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
  const share::schema::ObSimpleTableSchemaV2 *simple_table_schema_;
  common::ObArenaAllocator allocator_;
  common::ObRowkey rowkey_;
  table::ObTabletTTLScheduler *ttl_tablet_scheduler_;
  share::ObLSID ls_id_;
  ObTableEntity delete_entity_;
  uint64_t hbase_cur_version_;
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
  virtual int get_scan_range(ObIArray<ObNewRange> &ranges);

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
  virtual int64_t hash() const override;
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
