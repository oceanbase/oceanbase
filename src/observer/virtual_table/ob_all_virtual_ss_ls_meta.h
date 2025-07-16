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

#ifndef OB_ALL_VIRTUAL_OB_SS_LS_META_H_
#define OB_ALL_VIRTUAL_OB_SS_LS_META_H_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{

struct VirtualSSLSMetaRow {
  share::SCN version_;
  share::SCN ss_checkpoint_scn_;
  palf::LSN ss_checkpoint_lsn_;
  int64_t clog_checksum_;

  VirtualSSLSMetaRow()
    : version_(),
      ss_checkpoint_scn_(),
      ss_checkpoint_lsn_(),
      clog_checksum_(0) {}

  TO_STRING_KV(K(version_), K(ss_checkpoint_scn_),
               K(ss_checkpoint_lsn_), K(clog_checksum_));
};

class ObAllVirtualSSLSMeta : public common::ObVirtualTableScannerIterator
{
  static const int64_t ROWKEY_COL_COUNT = 2;
public:
  ObAllVirtualSSLSMeta();
  virtual ~ObAllVirtualSSLSMeta();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  enum TABLE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    LS_ID,
    META_VERSION,
    SS_CHECKPOINT_SCN,
    SS_CHECKPOINT_LSN,
    SS_CLOG_ACCUM_CHECKSUM,
  };
private:
#ifdef OB_BUILD_SHARED_STORAGE
  int get_primary_key_();
  int handle_key_range_(ObNewRange &key_range);
  int generate_virtual_row_(VirtualSSLSMetaRow &row);
  int fill_in_row_(const VirtualSSLSMetaRow &row_data, common::ObNewRow *&row);
  int extract_result_(common::sqlclient::ObMySQLResult &res, VirtualSSLSMetaRow &row);
  int get_virtual_row_remote_(common::sqlclient::ObMySQLResult &res, VirtualSSLSMetaRow &row);
  int get_virtual_row_remote_(
    const uint64_t tenant_id,
    const share::ObLSID ls_id,
    VirtualSSLSMetaRow &row);
#endif
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;

  VirtualSSLSMetaRow tablet_meta_row_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSLSMeta);
};

} // observer
} // oceanbase
#endif
