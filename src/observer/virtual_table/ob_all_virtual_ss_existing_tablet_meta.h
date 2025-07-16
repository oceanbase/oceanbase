/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_EXISTING_SS_TABLET_META_H_
#define OB_ALL_VIRTUAL_EXISTING_SS_TABLET_META_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_tablet_id.h"
#include "observer/virtual_table/ob_all_virtual_ss_tablet_meta.h"
namespace oceanbase
{
namespace observer
{

class ObAllVirtualSSExistingTabletMeta : public common::ObVirtualTableScannerIterator
{
  static const int64_t ROWKEY_COL_COUNT = 5;
public:
  ObAllVirtualSSExistingTabletMeta();
  virtual ~ObAllVirtualSSExistingTabletMeta();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  enum TABLE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    LS_ID,
    TABLET_ID,
    TRANSFER_SCN,
    META_VERSION,
    DATA_TABLET_ID,
    CREATE_SCN,
    START_SCN,
    CREATE_SCHEMA_VERSION,
    DATA_CHECKPOINT_SCN,
    MDS_CHECKPOINT_SCN,
    DDL_CHECKPOINT_SCN,
    MULTI_VERSION_START,
    TABLET_SNAPSHOT_VERSION,
    SSTABLE_OP_ID,
    UPDATE_REASON,
  };
private:
#ifdef OB_BUILD_SHARED_STORAGE
  int get_primary_key_();
  int handle_key_range_(ObNewRange &key_range);
  int generate_virtual_rows_(ObArray<VirtualTabletMetaRow> &row_datas);
  int fill_in_rows_(const ObArray<VirtualTabletMetaRow> &row_datas);
  int extract_result_(common::sqlclient::ObMySQLResult &res, VirtualTabletMetaRow &row);
  int get_virtual_rows_remote_(common::sqlclient::ObMySQLResult &res,
                              ObArray<VirtualTabletMetaRow> &row_datas);
  int get_virtual_rows_remote_(const uint64_t tenant_id,
                               const share::ObLSID &ls_id,
                               common::ObTabletID &tablet_id,
                               share::SCN &transfer_scn,
                               ObArray<VirtualTabletMetaRow> &row_datas);
#endif
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  ObISQLClient::ReadResult result_; // make sure lifetime is long enough to hold varchar results.
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSExistingTabletMeta);
};

} // observer
} // oceanbase
#endif
