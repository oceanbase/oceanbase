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

#ifndef OCEANBASE_STORAGE_OB_LS_DATA_ACCESS_SERVICE_
#define OCEANBASE_STORAGE_OB_LS_DATA_ACCESS_SERVICE_

namespace oceanbase
{
namespace storage
{
// TODO: SQL need change pkey in param to tablet_id
class ObLSAccessService
{
public:
  int table_scan(
      ObTableScanParam &param,
      common::ObNewRowIterator *&result);
  int table_scan(
      ObTableScanParam &param,
      common::ObNewIterIterator *&result);
  int join_mv_scan(
      ObTableScanParam &left_param,
      ObTableScanParam &right_param,
      ObIPartitionGroup &right_partition,
      common::ObNewRowIterator *&result);
  int delete_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int delete_row(ObStoreCtx &ctx,
                         const ObDMLBaseParam &dml_param,
                         const common::ObIArray<uint64_t> &column_ids,
                         const common::ObNewRow &row);
  int put_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int insert_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int insert_row(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObNewRow &row);
  int insert_row(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &duplicated_column_ids,
      const common::ObNewRow &row,
      const ObInsertFlag flag,
      int64_t &affected_rows,
      common::ObNewRowIterator *&duplicated_rows);
  int revert_insert_iter(const share::ObLSID &id, common::ObNewRowIterator *iter);
  int update_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray< uint64_t> &updated_column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int update_row(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &updated_column_ids,
      const common::ObNewRow &old_row,
      const common::ObNewRow &new_row);
  int lock_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const int64_t abs_lock_timeout,
      common::ObNewRowIterator *row_iter,
      const ObLockFlag lock_flag,
      const bool is_sfu,
      int64_t &affected_rows);
  int lock_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const int64_t abs_lock_timeout,
      const common::ObNewRow &row,
      ObLockFlag lock_flag,
      const bool is_sfu);
private:
  ObLSTabletService *ls_tablet_svr_;
};

}
}
#endif
