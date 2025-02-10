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

#ifndef OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_
#define OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_

#include "storage/lob/ob_lob_util.h"

namespace oceanbase
{
namespace storage
{
struct ObDMLRunningCtx;
struct ObLobDataInsertTask
{

  ObLobDataInsertTask():
    src_data_locator_(),
    cur_data_locator_(),
    col_idx_(0),
    row_idx_(0)
  {
    lob_meta_list_.set_attr(ObMemAttr(MTL_ID(), "LobDml"));
  }

  ObLobLocatorV2 src_data_locator_;
  ObLobLocatorV2 cur_data_locator_;
  int16_t col_idx_;
  int16_t row_idx_;
  ObArray<ObLobMetaInfo> lob_meta_list_;

  TO_STRING_KV(K_(col_idx), K_(row_idx), K_(src_data_locator), K_(cur_data_locator));
};

struct ObLobTabletDmlCtx
{
  ObLobTabletDmlCtx():
    insert_data_info_()
  {
    insert_data_info_.set_attr(ObMemAttr(MTL_ID(), "LobDml"));
  }

  ~ObLobTabletDmlCtx();

  bool is_all_task_done() const { return  insert_data_info_.count() != 0; }

  int64_t task_count() const { return insert_data_info_.count(); }

  ObLobDataInsertTask& task(const int64_t idx) { return insert_data_info_[idx]; }

  void reuse() { insert_data_info_.reuse(); }

  common::ObTabletID lob_meta_tablet_id_;
  common::ObTabletID lob_piece_tablet_id_;
  ObArray<ObLobDataInsertTask> insert_data_info_;

  TO_STRING_KV(K_(lob_meta_tablet_id), K_(lob_piece_tablet_id), K_(insert_data_info));

};

class ObLobTabletDmlHelper
{

public:
  // insert
  static int process_lob_column_before_insert(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &row,
      const int16_t row_idx,
      const int16_t col_idx,
      blocksstable::ObStorageDatum &datum);

  static int process_lob_column_after_insert(
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &row,
      ObLobDataInsertTask &info);

  // update
  static int process_lob_column_before_update(
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &old_row,
      blocksstable::ObDatumRow &new_row,
      const bool data_tbl_rowkey_change,
      const int16_t row_idx,
      const int16_t col_idx,
      blocksstable::ObStorageDatum &old_datum,
      blocksstable::ObStorageDatum &new_datum);

  static int process_lob_column_after_update(
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &old_row,
      blocksstable::ObDatumRow &new_row,
      const bool data_tbl_rowkey_change,
      ObLobDataInsertTask &info);

  static int insert_lob_col(
      ObDMLRunningCtx &run_ctx,
      const blocksstable::ObDatumRow &data_row,
      const int16_t col_idx,
      blocksstable::ObStorageDatum &datum,
      ObLobAccessParam *del_param,
      ObString &disk_locator_data,
      ObArray<ObLobMetaInfo> *lob_meta_list = nullptr,
      const bool try_flush_redo = false);
  static int delete_lob_col(
      ObDMLRunningCtx &run_ctx,
      const blocksstable::ObDatumRow &data_row,
      const int16_t col_idx,
      blocksstable::ObStorageDatum &datum,
      ObLobCommon *&lob_common,
      ObLobAccessParam &lob_param,
      const bool try_flush_redo = false);
  static int update_lob_col(
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &old_row,
      blocksstable::ObDatumRow &new_row,
      bool data_tbl_rowkey_change,
      const int16_t col_idx,
      blocksstable::ObStorageDatum &old_datum,
      blocksstable::ObStorageDatum &new_datum);

  // partial update
  static int process_delta_lob(
      ObDMLRunningCtx &run_ctx,
      const blocksstable::ObDatumRow &data_row,
      const int16_t col_idx,
      blocksstable::ObStorageDatum &old_datum,
      ObLobLocatorV2 &delta_lob,
      blocksstable::ObStorageDatum &datum);

private:
  static int build_common_lob_param_for_dml(
      ObDMLRunningCtx &run_ctx,
      const blocksstable::ObDatumRow &data_row,
      const int16_t col_idx,
      ObString &disk_lob_locator,
      ObLobAccessParam &lob_param);

  static int prepare_lob_write(
      ObDMLRunningCtx &run_ctx,
      const blocksstable::ObDatumRow &data_row,
      const int16_t row_idx,
      const int16_t col_idx,
      ObString &old_disk_locator,
      blocksstable::ObStorageDatum &new_datum,
      bool &need_do_write);

  static int register_ext_info_commit_cb(
      ObDMLRunningCtx &run_ctx,
      const ObColDesc &column,
      ObDatum &col_data,
      ObObj &ext_info_data);
  static int register_ext_info_commit_cb(
      ObDMLRunningCtx &run_ctx,
      const ObColDesc &column,
      ObString &index_data,
      ObString &data);

  static int set_lob_storage_params(
      ObDMLRunningCtx &run_ctx,
      const ObColDesc &column,
      ObLobAccessParam &lob_param);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_
