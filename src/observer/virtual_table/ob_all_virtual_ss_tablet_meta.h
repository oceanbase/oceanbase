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

#ifndef OB_ALL_VIRTUAL_OB_SS_TABLET_META_H_
#define OB_ALL_VIRTUAL_OB_SS_TABLET_META_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace observer
{

struct VirtualTabletMetaRow {
  share::SCN version_;
  common::ObTabletID data_tablet_id_;
  share::SCN create_scn_;
  share::SCN start_scn_;
  int64_t create_schema_version_;
  share::SCN data_checkpoint_scn_;
  share::SCN mds_checkpoint_scn_;
  share::SCN ddl_checkpoint_scn_;
  int64_t multi_version_start_;
  int64_t tablet_snapshot_version_;

  VirtualTabletMetaRow()
    : version_(), data_tablet_id_(),
      create_scn_(), start_scn_(), create_schema_version_(0),
      data_checkpoint_scn_(), mds_checkpoint_scn_(), ddl_checkpoint_scn_(),
      multi_version_start_(0), tablet_snapshot_version_(0) { }

  TO_STRING_KV(K(version_), K(data_tablet_id_),
               K(create_scn_), K(start_scn_), K(create_schema_version_),
               K(data_checkpoint_scn_), K(mds_checkpoint_scn_),
               K(ddl_checkpoint_scn_), K(multi_version_start_),
               K(tablet_snapshot_version_));
};

class ObAllVirtualSSTabletMeta : public common::ObVirtualTableScannerIterator
{
  static const int64_t ROWKEY_COL_COUNT = 4;
public:
  ObAllVirtualSSTabletMeta();
  virtual ~ObAllVirtualSSTabletMeta();
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
  };
private:
#ifdef OB_BUILD_SHARED_STORAGE
  int get_primary_key_();
  int handle_key_range_(ObNewRange &key_range);
  int generate_virtual_row_(VirtualTabletMetaRow &row);
  int fill_in_row_(const VirtualTabletMetaRow &row_data, common::ObNewRow *&row);
#endif
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;

  VirtualTabletMetaRow tablet_meta_row_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSTabletMeta);
};

} // observer
} // oceanbase
#endif
