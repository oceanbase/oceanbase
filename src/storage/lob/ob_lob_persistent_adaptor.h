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

namespace oceanbase
{
namespace storage
{

class ObLobUpdIterator : public ObNewRowIterator
{
public:
  ObLobUpdIterator(ObNewRow *old_row,
                   ObNewRow *new_row)
    : old_row_(old_row),
      new_row_(new_row),
      got_old_row_(false),
      is_iter_end_(false)
  {}
  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override { return OB_NOT_IMPLEMENT; }
  virtual void reset() override {}
private:
  ObNewRow *old_row_;
  ObNewRow *new_row_;
  bool got_old_row_;
  bool is_iter_end_;
};

class ObPersistentLobApator : public ObILobApator
{
public:
  ObPersistentLobApator() {}
  virtual int scan_lob_meta(ObLobAccessParam &param,
    ObTableScanParam &scan_param,
    common::ObNewRowIterator *&meta_iter) override;
  virtual int get_lob_data(ObLobAccessParam &param,
    uint64_t piece_id,
    ObLobPieceInfo& info) override;
  virtual int revert_scan_iter(common::ObNewRowIterator *iter) override;
  virtual int fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id) override;
  // write meta tablet
  virtual int write_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& row_info) override;
  // write piece tablet
  int write_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row);
  // erase meta tablet item
  virtual int erase_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& row_info) override;
  // erase piece tablet item
  int erase_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row);
  // update piece tabliet item
  int update_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row);
  // update lob meta tablet item
  virtual int update_lob_meta(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row) override;
private:
  // get schema from schema service 
  int get_lob_tablet_schema(
      uint64_t tenant_id,
      bool is_meta,
      ObTableSchema& schema,
      int64_t &tenant_schema_version);
    
  int get_lob_tablets(
      ObLobAccessParam& param,
      ObTabletHandle &data_tablet,
      ObTabletHandle &lob_meta_tablet,
      ObTabletHandle &lob_piece_tablet);

  int get_lob_tablets_id(
      ObLobAccessParam& param,
      common::ObTabletID &lob_meta_tablet_id,
      common::ObTabletID &lob_piece_tablet_id);
  int prepare_table_param(
      const ObLobAccessParam &param,
      ObTableScanParam &scan_param,
      bool is_meta);
  int build_common_scan_param(
      const ObLobAccessParam &param,
      const uint64_t table_id,
      uint32_t col_num,
      ObTableScanParam& scan_param);
  int inner_get_tablet(
      const ObLobAccessParam &param,
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle);
  bool check_lob_tablet_id(
      const common::ObTabletID &data_tablet_id,
      const common::ObTabletID &lob_meta_tablet_id,
      const common::ObTabletID &lob_piece_tablet_id);

  int build_lob_piece_table_dml(
      ObLobAccessParam& param,
      const uint64_t tenant_id,
      ObTableDMLParam& dml_param,
      ObDMLBaseParam& dml_base_param,
      ObSEArray<uint64_t, 3>& column_ids,
      const ObTabletHandle& data_tablet,
      const ObTabletHandle& lob_piece_tablet);

  int prepare_lob_meta_dml(
      ObLobAccessParam& param,
      const uint64_t tenant_id,
      const ObTabletHandle& data_tablet,
      const ObTabletHandle& lob_meta_tablet);

  int build_lob_meta_table_dml(
      ObLobAccessParam& param,
      const uint64_t tenant_id,
      ObTableDMLParam* dml_param,
      ObDMLBaseParam& dml_base_param,
      ObSEArray<uint64_t, 6>& column_ids,
      const ObTabletHandle& data_tablet,
      const ObTabletHandle& lob_meta_tablet);

  void set_lob_meta_row(
      ObObj* cell, 
      ObNewRow& new_row,
      ObLobMetaInfo& in_row);

  int set_lob_piece_row(
      char* buf,
      size_t buf_len,
      ObObj* cell, 
      ObNewRow& new_row,
      common::ObSingleRowIteratorWrapper* new_row_iter,
      ObLobPieceInfo& in_row);
private:
  static const uint64_t LOB_EXPIRE_TIME_US = 3 * 1000 * 1000; // 3s
};


} // storage
} // oceanbase

#endif



