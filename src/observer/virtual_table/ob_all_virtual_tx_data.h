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

#ifndef OB_ALL_VIRTUAL_TX_DATA_H_
#define OB_ALL_VIRTUAL_TX_DATA_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{

namespace storage
{

class ObTxDataGuard;

}

namespace observer
{

struct VirtualTxDataRow {
  int32_t state_;
  share::SCN start_scn_;
  share::SCN end_scn_;
  share::SCN commit_version_;
  char undo_status_list_str_[common::MAX_UNDO_LIST_CHAR_LENGTH];

  VirtualTxDataRow() : state_(0), start_scn_(), end_scn_(), commit_version_() {}

  TO_STRING_KV(K(state_), K(start_scn_), K(end_scn_), K(commit_version_), K(undo_status_list_str_));
};

class ObAllVirtualTxData : public common::ObVirtualTableScannerIterator {
private:
  enum VirtualTxDataTableColumnID : uint64_t {
    TENANT_ID_COL = OB_APP_MIN_COLUMN_ID,
    LS_ID_COL,
    TX_ID_COL,
    SVR_IP_COL,
    SVR_PORT_COL,
    STATE_COL,
    START_SCN_COL,
    END_SCN_COL,
    COMMIT_VERSION_COL,
    UNDO_STATUS_COL
  };


public:
  ObAllVirtualTxData() : addr_(), tenant_id_(0), ls_id_(0), tx_id_(0) {}
  ~ObAllVirtualTxData() {}

  TO_STRING_KV(K(MTL_ID()), K_(tenant_id), K_(ls_id), K_(tx_id));

public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset()
  {
    addr_.reset();
    tenant_id_ = 0;
    ls_id_.reset();
    tx_id_.reset();
    memset(ip_buf_, 0, sizeof(ip_buf_));
  }
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }

private:
  int get_primary_key_();
  int handle_key_range_(ObNewRange &key_range);
  int generate_virtual_tx_data_row_(VirtualTxDataRow &tx_data_row);
  int fill_in_row_(const VirtualTxDataRow &row_data, common::ObNewRow *&row);

private:
  common::ObAddr addr_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  transaction::ObTransID tx_id_;
  char ip_buf_[common::OB_IP_STR_BUFF];

  VirtualTxDataRow tx_data_row_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTxData);
};
}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_MEMSTORE_INFO_H */
