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

#ifndef OCEABASE_STORAGE_INNER_TABLET_ACCESS_SERVICE_
#define OCEABASE_STORAGE_INNER_TABLET_ACCESS_SERVICE_

#include "lib/guard/ob_shared_guard.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"          // ObConcurrentFIFOAllocator
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/ob_storage_rpc.h"
#include "storage/tx/ob_location_adapter.h"
#include "storage/tx_storage/ob_access_service.h"
#include "share/schema/ob_table_dml_param.h"

namespace oceanbase
{
namespace share
{
class ObLSID;
}

namespace storage
{

struct ObInnerTabletWriteCtx final
{
public:
  ObInnerTabletWriteCtx();
  ~ObInnerTabletWriteCtx();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(KP_(tx_desc), K_(ls_id), K_(tablet_id), KP_(buf), K_(buf_len));
public:
  transaction::ObTxDesc *tx_desc_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  const char *buf_;
  int64_t buf_len_;
};

struct ObInnerTableReadCtx final
{
public:
  ObInnerTableReadCtx();
  ~ObInnerTableReadCtx();
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(ls_id), K_(tablet_id), KPC_(key_range), K_(is_get), K_(snapshot),
      K_(abs_timeout_us), K_(table_param), K_(scan_param));
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  const common::ObNewRange *key_range_;
  bool is_get_;
  share::SCN snapshot_;
  int64_t abs_timeout_us_;
  bool get_multi_version_row_;
  common::ObArenaAllocator allocator_;
  share::schema::ObTableParam table_param_;
  ObTableScanParam scan_param_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerTableReadCtx);
};

class ObInnerTabletIterator : public blocksstable::ObDatumRowIterator
{
public:
  static const int64_t DEFAULT_BATCH_SIZE = 256;
public:
  ObInnerTabletIterator();
  virtual ~ObInnerTabletIterator();
  virtual int get_next_row(blocksstable::ObDatumRow *&datum_row) override;
  virtual int get_next_rows(blocksstable::ObDatumRow *&rows, int64_t &row_count) override;
  virtual void reset() override {}
  void reuse();
  void destroy();
  int init(
      char *buf,
      const int64_t buf_length,
      common::ObIAllocator &allocator);
private:
  int convert_buffer_to_datum_();

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;
  common::ObArray<blocksstable::ObDatumRow *> datum_row_array_;
  blocksstable::ObDataBuffer data_buffer_;
  int64_t total_count_;
  int64_t index_;
  blocksstable::ObDatumRow *cur_datum_rows_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerTabletIterator);
};

struct ObInnerTabletSQLStr final
{
  OB_UNIS_VERSION(1);
public:
  ObInnerTabletSQLStr();
  ~ObInnerTabletSQLStr();
  void reset();
  bool is_valid() const;
  int set(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const char *buf,
      const int64_t buf_len);

  const char *get_buf() { return inner_tablet_str_.ptr(); }
  int64_t get_buf_len() { return inner_tablet_str_.length(); }
  const share::ObLSID &get_ls_id() { return ls_id_; }
  const common::ObTabletID &get_tablet_id() { return tablet_id_; }
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(inner_tablet_str));
private:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObString inner_tablet_str_;
};

class ObInnerTabletAccessService final
{
public:
  ObInnerTabletAccessService();
   ~ObInnerTabletAccessService();

  static int mtl_init(ObInnerTabletAccessService* &inner_tablet_access_service);
  int init();
  void destroy();
public:
  int insert_rows(
      const ObInnerTabletWriteCtx &ctx,
      int64_t &affected_rows);
  int read_rows(
      ObInnerTableReadCtx &ctx,
      common::ObNewRowIterator *&scan_iter);

private:
  //insert rows
  int inner_insert_rows_(
      const ObInnerTabletWriteCtx &ctx,
      int64_t &affected_rows);
  int execute_local_insert_rows_(
      const ObInnerTabletWriteCtx &ctx,
      int64_t &affected_rows);
  int execute_remote_insert_rows_(
      const ObInnerTabletWriteCtx &ctx,
      const ObAddr &ls_leader_addr,
      transaction::ObTxExecResult &result,
      int64_t &affected_rows);
  int do_insert_rows_(
      const ObInnerTabletWriteCtx &ctx,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows);

  //read rows
  int do_read_rows_(
      ObInnerTableReadCtx &ctx,
      common::ObNewRowIterator *&scan_iter);
  int build_scan_param_(
      ObInnerTableReadCtx &ctx,
      const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<uint64_t> &column_ids,
      share::schema::ObTableParam &table_param,
      ObTableScanParam &param);

private:
  //transaction
  int create_implicit_savepoint_(
      transaction::ObTxDesc &tx_desc,
      transaction::ObTxSEQ &savepoint);
  int collect_tx_exec_result_(
      transaction::ObTxDesc &tx_desc,
      transaction::ObTxExecResult &result);
  int add_tx_exec_result_(
      transaction::ObTxDesc &tx_desc,
      const transaction::ObTxExecResult &result);
  int rollback_to_implicit_savepoint_(
      const transaction::ObTxSEQ &savepoint,
      const share::ObLSID &ls_id,
      transaction::ObTxDesc &tx_desc);
  int get_read_snapshot_(
      transaction::ObTxDesc &tx_desc,
      const share::ObLSID &ls_id,
      transaction::ObTxReadSnapshot &snapshot);
  int get_write_store_ctx_guard_(
      const share::ObLSID &ls_id,
      transaction::ObTxDesc &tx_desc,
      const transaction::ObTxReadSnapshot &snapshot,
      ObStoreCtxGuard &ctx_guard);

  //dml param
  int init_table_dml_param_(
      const ObInnerTabletWriteCtx &ctx,
      share::schema::ObTableDMLParam &table_dml_param,
      common::ObIArray<uint64_t> &column_ids);
  int init_dml_param_(
      const ObInnerTabletWriteCtx &ctx,
      const transaction::ObTxReadSnapshot &snapshot,
      common::ObIAllocator &allocator,
      share::schema::ObTableDMLParam &table_dml_param,
      ObStoreCtxGuard &ctx_guard,
      storage::ObDMLBaseParam &dml_param);
  int get_table_schema_(
      const common::ObTabletID &tablet_id,
      const share::schema::ObTableSchema *&table_schema);
  int init_table_param_(
      const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<uint64_t> &column_ids,
      share::schema::ObTableParam &table_param);
  int get_column_ids_(
      const common::ObTabletID &tablet_id,
      const bool is_query,
      common::ObIArray<uint64_t> &column_ids);

private:
  bool is_inited_;
  transaction::ObTransService *trans_service_;
  storage::ObAccessService *access_service_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  transaction::ObLocationAdapter location_adapter_def_;
  transaction::ObILocationAdapter *location_adapter_;
  common::ObAddr self_;
  DISALLOW_COPY_AND_ASSIGN(ObInnerTabletAccessService);
};


}
}
#endif
