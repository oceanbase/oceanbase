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

#ifndef OB_ALL_VIRTUAL_TX_DATA_TABLE_H_
#define OB_ALL_VIRTUAL_TX_DATA_TABLE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualTxDataTable : public common::ObVirtualTableScannerIterator, public omt::ObMultiTenantOperator {
private:
  struct RowData {
    const char *state_;
    int64_t tx_data_count_;
    share::SCN min_tx_scn_;
    share::SCN max_tx_scn_;
    RowData() : state_(""), tx_data_count_(-1), min_tx_scn_(), max_tx_scn_() {}
  };

  enum VirtualTxDataTableColumnID : uint64_t {
    SVR_IP_COL = OB_APP_MIN_COLUMN_ID,
    SVR_PORT_COL,
    TENANT_ID_COL,
    LS_ID_COL,
    STATE_COL,
    START_SCN_COL,
    END_SCN_COL,
    TX_DATA_COUNT_COL,
    MIN_TX_SCN_COL,
    MAX_TX_SCN_COL
  };

public:
  ObAllVirtualTxDataTable();
  ~ObAllVirtualTxDataTable();

  TO_STRING_KV(K_(ls_id), K(MTL_ID()), K_(memtable_array_pos), K_(sstable_array_pos));
public:
  virtual int inner_get_next_row(common::ObNewRow *&row) { return execute(row);}
  virtual void reset();
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }

private:
  int get_next_tx_data_table_(ObITable *&tx_data_memtable);

  int prepare_row_data_(ObITable *tx_data_table, RowData &row_data);

  virtual bool is_need_process(uint64_t tenant_id) override;

  virtual int process_curr_tenant(common::ObNewRow *&row) override;

  virtual void release_last_tenant() override;

private:
  common::ObAddr addr_;
  int64_t ls_id_;
  char ip_buf_[common::OB_IP_STR_BUFF];

  /****************   NOTE : These resources must be released in their own tenant    *****************/
  int64_t memtable_array_pos_;
  int64_t sstable_array_pos_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
  ObTabletHandle tablet_handle_;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper_;
  ObMemtableMgrHandle mgr_handle_;
  common::ObSEArray<ObTableHandleV2, 1> memtable_handles_;
  common::ObSEArray<ObITable *, 8> sstable_handles_;
  /****************   NOTE : These resources must be released in their own tenant    *****************/

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTxDataTable);
};
}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_MEMSTORE_INFO_H */
