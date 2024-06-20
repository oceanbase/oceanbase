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

#ifndef OCEANBASE_STORAGE_OB_DATA_ACCESS_SERVICE_
#define OCEANBASE_STORAGE_OB_DATA_ACCESS_SERVICE_

#include "share/ob_i_tablet_scan.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/access/ob_table_scan_range.h"
#include "share/stat/ob_stat_define.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace common {
class ObTabletID;
class ObNewRowIterator;
}
namespace share {
class ObLSID;
}
namespace transaction
{
class ObTxDesc;
}
namespace storage
{
struct ObDMLBaseParam;
class ObLSService;
class ObStoreCtx;

class ObStoreCtxGuard
{
public:
  ObStoreCtxGuard() : is_inited_(false), init_ts_(0)
  {
  }
  ~ObStoreCtxGuard()
  {
    reset();
  }
  int init(const share::ObLSID &ls_id);
  void reset();
  ObStoreCtx &get_store_ctx() { return ctx_; }
  ObLSHandle &get_ls_handle() { return handle_; }
private:
  bool is_inited_;
  ObStoreCtx ctx_;
  share::ObLSID ls_id_;
  ObLSHandle handle_;
  int64_t init_ts_;

  DISALLOW_COPY_AND_ASSIGN(ObStoreCtxGuard);
};

class ObAccessService : public common::ObITabletScan
{
public:
  ObAccessService();
  virtual ~ObAccessService();
  int init(const uint64_t tenant_id, ObLSService *ls_service);
  static int mtl_init(ObAccessService* &access_service);

  void destroy();
  void stop();
public:
  // pre_check_lock
  // @param [in] ls_id, this check op will be processed at which logstream.
  // @param [in] tx_desc, the trans context.
  // @param [in] param, contain all the check parameters.
  int pre_check_lock(
      const share::ObLSID &ls_id,
      transaction::ObTxDesc &tx_desc,
      const transaction::tablelock::ObLockParam &param);
  // lock obj
  // @param [in] ls_id, this lock op will be processed at which logstream.
  // @param [in] tx_desc, the trans context.
  // @param [in] param, contain all the lock parameters.
  int lock_obj(
      const share::ObLSID &ls_id,
      transaction::ObTxDesc &tx_desc,
      const transaction::tablelock::ObLockParam &param);
  // unlock obj
  // @param [in] ls_id, this unlock op will be processed at which logstream.
  // @param [in] tx_desc, the trans context.
  // @param [in] param, contain all the unlock parameters.
  int unlock_obj(
      const share::ObLSID &ls_id,
      transaction::ObTxDesc &tx_desc,
      const transaction::tablelock::ObLockParam &param);
  // ObITabletScan interface
  virtual int table_scan(
      ObVTableScanParam &vparam,
      ObNewRowIterator *&result) override;
  virtual int table_rescan(
      ObVTableScanParam &vparam,
      ObNewRowIterator *result) override;
  virtual int reuse_scan_iter(const bool switch_param, common::ObNewRowIterator *iter) override;
  virtual int revert_scan_iter(common::ObNewRowIterator *iter) override;
  virtual int get_multi_ranges_cost(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      const common::ObIArray<common::ObStoreRange> &ranges,
      int64_t &total_size) override;
  virtual int split_multi_ranges(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      const common::ObIArray<ObStoreRange> &ranges,
      const int64_t expected_task_count,
      common::ObIAllocator &allocator,
      common::ObArrayArray<ObStoreRange> &multi_range_split_array) override;
  int get_write_store_ctx_guard(
      const share::ObLSID &ls_id,
      const int64_t timeout,
      transaction::ObTxDesc &tx_desc,
      const transaction::ObTxReadSnapshot &snapshot,
      const int16_t branch_id,
      ObStoreCtxGuard &ctx_guard,
      const transaction::ObTxSEQ &spec_seq_no = transaction::ObTxSEQ::INVL());

  // DML interface
  int delete_rows(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int put_rows(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int insert_rows(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int insert_row(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &duplicated_column_ids,
      const common::ObNewRow &row,
      const ObInsertFlag flag,
      int64_t &affected_rows,
      common::ObNewRowIterator *&duplicated_rows);
  int revert_insert_iter(common::ObNewRowIterator *iter);
  int update_rows(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray< uint64_t> &updated_column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int lock_rows(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDMLBaseParam &dml_param,
      const int64_t abs_lock_timeout, /* -1: undefined, 0: nowait */
      const ObLockFlag lock_flag,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int lock_row(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDMLBaseParam &dml_param,
      const int64_t abs_lock_timeout,
      const common::ObNewRow &row,
      const ObLockFlag lock_flag);
  int estimate_row_count(
      const ObTableScanParam &param,
      const ObTableScanRange &scan_range,
      const int64_t timeout_us,
      ObIArray<ObEstRowCountRecord> &est_records,
      int64_t &logical_row_count,
      int64_t &physical_row_count) const;
  int estimate_block_count_and_row_count(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      int64_t &macro_block_count,
      int64_t &micro_block_count,
      int64_t &sstable_row_count,
      int64_t &memtable_row_count,
      common::ObIArray<int64_t> &cg_macro_cnt_arr,
      common::ObIArray<int64_t> &cg_micro_cnt_arr) const;
protected:
  int check_tenant_out_of_memstore_limit_(bool &is_out_of_mem);
  int check_data_disk_full_(
      const share::ObLSID &ls_id,
      bool &is_full);
  int get_write_store_ctx_guard_(
      const share::ObLSID &ls_id,
      const int64_t timeout,
      transaction::ObTxDesc &tx_desc,
      const transaction::ObTxReadSnapshot &snapshot,
      const int16_t branch_id,
      const concurrent_control::ObWriteFlag write_flag,
      ObStoreCtxGuard &ctx_guard,
      const transaction::ObTxSEQ &spec_seq_no = transaction::ObTxSEQ::INVL());
  int check_read_allowed_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObStoreAccessType access_type,
      const ObTableScanParam &scan_param,
      ObTabletHandle &tablet_handle,
      ObStoreCtxGuard &ctx_guard,
      share::SCN user_specified_snapshot);
  int check_write_allowed_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObStoreAccessType access_type,
      const ObDMLBaseParam &dml_param,
      const int64_t lock_wait_timeout_ts,
      transaction::ObTxDesc &tx_desc,
      ObTabletHandle &tablet_handle,
      ObStoreCtxGuard &ctx_guard);
  int get_source_ls_tx_table_guard_(
      const ObTabletHandle &tablet_handle,
      ObStoreCtxGuard &ctx_guard);
  int construct_store_ctx_other_variables_(
      ObLS &ls,
      const common::ObTabletID &tablet_id,
      const int64_t timeout,
      const share::SCN &snapshot,
      ObTabletHandle &tablet_handle,
      ObStoreCtxGuard &ctx_guard);
  static OB_INLINE int64_t get_lock_wait_timeout_(const int64_t abs_lock_timeout, const int64_t stmt_timeout)
  {
    return (abs_lock_timeout < 0 ? stmt_timeout : (abs_lock_timeout > stmt_timeout ? stmt_timeout : abs_lock_timeout));
  }

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObLSService *ls_svr_;
};

}
}
#endif
