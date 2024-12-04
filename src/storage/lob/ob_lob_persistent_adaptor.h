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

#ifndef OCEABASE_STORAGE_OB_LOB_PERSISTENT_ADAPTOR_
#define OCEABASE_STORAGE_OB_LOB_PERSISTENT_ADAPTOR_
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "ob_i_lob_adaptor.h"
#include "common/row/ob_row_iterator.h"
#include "storage/blocksstable/ob_datum_row_iterator.h"

namespace oceanbase
{
namespace storage
{

class ObLobMetaIterator;
class ObStoreCtxGuard;

class ObPersistentLobApator : public ObILobApator
{
public:
  explicit ObPersistentLobApator(const uint64_t tenant_id):
    tenant_id_(tenant_id),
    allocator_(lib::ObMemAttr(tenant_id, "LobPersist", ObCtxIds::LOB_CTX_ID)),
    table_param_inited_(false),
    meta_table_param_(nullptr),
    meta_table_dml_param_(nullptr)
  {}

  virtual ~ObPersistentLobApator();

  virtual void destroy();

  virtual int scan_lob_meta(
      ObLobAccessParam &param,
      ObLobMetaIterator *&iter);
  int scan_with_ctx(
      ObLobAccessParam &param,
      ObLobMetaIterator *&meta_iter);

  virtual int get_lob_data(ObLobAccessParam &param,
    uint64_t piece_id,
    ObLobPieceInfo& info) override;
  virtual int revert_scan_iter(ObLobMetaIterator *iter) override;
  virtual int fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id) override;
  // write meta tablet
  virtual int write_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& row_info) override;
  // erase meta tablet item
  virtual int erase_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& row_info) override;
  // update lob meta tablet item
  virtual int update_lob_meta(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row) override;

  // write piece tablet
  int write_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row) {return OB_NOT_IMPLEMENT; };
  // erase piece tablet item
  int erase_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row) {return OB_NOT_IMPLEMENT; };
  // update piece tabliet item
  int update_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row) {return OB_NOT_IMPLEMENT; };
  static void set_lob_meta_row(
      blocksstable::ObDatumRow& datum_row,
      ObLobMetaInfo& in_row);

  int write_lob_meta(ObLobAccessParam &param, blocksstable::ObDatumRowIterator& row_iter);
  int erase_lob_meta(ObLobAccessParam &param, blocksstable::ObDatumRowIterator& row_iter);
  int update_lob_meta(ObLobAccessParam& param, blocksstable::ObDatumRowIterator &row_iter);

  int prepare_table_scan_param(
      const ObLobAccessParam &param,
      const bool is_get,
      ObTableScanParam &scan_param,
      ObIAllocator *scan_allocator);
  int prepare_lob_tablet_id(ObLobAccessParam& param);
  int prepare_scan_param_schema_version(
      ObLobAccessParam &param,
      ObTableScanParam &scan_param);

private:
  int get_meta_table_param(const ObTableParam *&table_param);
  int get_meta_table_dml_param(const ObTableDMLParam *&table_param);
  // get schema from schema service 
  int get_lob_tablet_schema(
      uint64_t tenant_id,
      bool is_meta,
      ObTableSchema& schema,
      int64_t &tenant_schema_version);

  int inner_get_tablet(
      const ObLobAccessParam &param,
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle);
  int inner_get_tablet(
      const ObLobAccessParam &param,
      const common::ObTabletID &tablet_id,
      ObLSHandle &ls_handle,
      ObTabletHandle &handle);

  bool check_lob_tablet_id(
      const common::ObTabletID &data_tablet_id,
      const common::ObTabletID &lob_meta_tablet_id,
      const common::ObTabletID &lob_piece_tablet_id);

  int fetch_lob_id_for_split_src(const ObLobAccessParam& param, const ObTabletID &lob_tablet_id, uint64_t &lob_id);
  int prepare_lob_meta_dml(ObLobAccessParam& param);

  int build_lob_meta_table_dml(ObLobAccessParam& param);

  int build_lob_meta_table_dml(
      ObLobAccessParam& param,
      ObDMLBaseParam &dml_base_param,
      ObStoreCtxGuard *store_ctx_guard);

  int set_lob_piece_row(
      char* buf,
      size_t buf_len,
      blocksstable::ObDatumRow& datum_row,
      blocksstable::ObSingleDatumRowIteratorWrapper* new_row_iter,
      ObLobPieceInfo& in_row);

  int init_table_param();
  int init_meta_column_ids(ObSEArray<uint64_t, 6> &meta_column_ids);
  int set_dml_seq_no(ObLobAccessParam &param);

  int build_common_scan_param(
      const ObLobAccessParam &param,
      const bool is_get,
      uint32_t col_num,
      ObTableScanParam& scan_param,
      ObIAllocator *scan_allocator);
  int prepare_table_param(
      const ObLobAccessParam &param,
      ObTableScanParam &scan_param);

private:

  const uint64_t tenant_id_;
  ObArenaAllocator allocator_;
  mutable ObSpinLock lock_;
  bool table_param_inited_;
  ObTableParam *meta_table_param_;
  ObTableDMLParam *meta_table_dml_param_;

private:
  static const uint64_t LOB_EXPIRE_TIME_US = 3 * 1000 * 1000; // 3s
};


} // storage
} // oceanbase

#endif



