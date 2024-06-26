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

#ifndef OB_STORAGE_LOB_LOCATOR_H
#define OB_STORAGE_LOB_LOCATOR_H

#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace storage
{

class ObLobLocatorHelper
{
public:
  ObLobLocatorHelper();
  virtual ~ObLobLocatorHelper();
  void reset();
  void reuse() {
    locator_allocator_.reuse();
    rowid_objs_.reuse();
    rowkey_str_.reset();
  }
  int init(const ObTableScanParam &scan_param,
           const ObStoreCtx &ctx,
           const share::ObLSID &ls_id,
           const int64_t snapshot_version);
  int init(const uint64_t table_id,
           const uint64_t tablet_id,
           const ObStoreCtx &ctx,
           const share::ObLSID &ls_id,
           const int64_t snapshot_version);
  int fill_lob_locator(blocksstable::ObDatumRow &row, bool is_projected_row,
                       const ObTableAccessParam &access_param);
  int fill_lob_locator_v2(blocksstable::ObDatumRow &row,
                          const ObTableAccessContext &access_ctx,
                          const ObTableAccessParam &access_param);
  int fill_lob_locator_v2(common::ObDatum &datum,
                          const ObColumnParam &col_param,
                          const ObTableIterParam &iter_param,
                          const ObTableAccessContext &access_ctx);
  int fuse_mem_lob_header(ObObj &def_obj, uint64_t col_id, bool is_systable);
  void update_lob_locator_ctx(uint64_t table_id, uint64_t tablet_id, int64_t tx_id) {
    table_id_ = table_id;
    tablet_id_ = tablet_id;
    tx_id_ = tx_id;
  }
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool enable_lob_locator_v2() const { return enable_locator_v2_; }
  TO_STRING_KV(K_(table_id), K_(ls_id), K_(snapshot_version), K_(rowid_version),
               KPC(rowid_project_), K_(enable_locator_v2), K_(is_inited), K_(scan_flag));
private:
  static const int64_t DEFAULT_LOCATOR_OBJ_ARRAY_SIZE = 8;
  static const int64_t LOB_FORCE_INROW_SIZE = 64 * 1024L; // 64K
  int init_rowid_version(const share::schema::ObTableSchema &table_schema);
  int build_rowid_obj(blocksstable::ObDatumRow &row,
                      common::ObString &rowid_str,
                      bool is_projected_row,
                      const ObColDescIArray &col_descs,
                      const common::ObIArray<int32_t> &out_project,
                      const common::ObTabletID &tablet_id);
  int build_lob_locator(common::ObString payload, const uint64_t column_id,
                        const common::ObString &rowid_str, ObLobLocator *&locator);
  int build_lob_locatorv2(ObLobLocatorV2 &locator,
                          const common::ObString &payload,
                          const uint64_t column_id,
                          const common::ObString &rowid_str,
                          const ObTableAccessContext &access_ctx,
                          ObCollationType cs_type,
                          bool is_simple,
                          bool is_systable);
  bool can_skip_build_mem_lob_locator(const common::ObString &payload);
private:
  uint64_t table_id_;
  uint64_t tablet_id_;
  int64_t ls_id_;
  int64_t tx_id_;
  int64_t snapshot_version_;
  transaction::ObTxSnapshot read_snapshot_;
  int64_t rowid_version_;
  const common::ObIArray<int32_t> *rowid_project_; //map to projected row
  common::ObSEArray<common::ObObj, DEFAULT_LOCATOR_OBJ_ARRAY_SIZE> rowid_objs_;
  common::ObArenaAllocator locator_allocator_;
  ObString rowkey_str_; // for default values
  bool enable_locator_v2_;
  bool is_inited_;
  ObQueryFlag scan_flag_;
};

} // namespace storage
} // namespace oceanbase
#endif
