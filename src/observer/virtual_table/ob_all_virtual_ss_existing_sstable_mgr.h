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

#ifndef OB_ALL_VIRTUAL_OB_SS_EXISTING_SSTABLE_MGR_H_
#define OB_ALL_VIRTUAL_OB_SS_EXISTING_SSTABLE_MGR_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"
#include "common/ob_tablet_id.h"
#include "observer/virtual_table/ob_all_virtual_ss_sstable_mgr.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ob_shared_meta_service.h"
#endif
namespace oceanbase
{
namespace observer
{
class ObAllVirtualSSExistingSSTableMgr : public common::ObVirtualTableScannerIterator
{
  static const int64_t ROWKEY_COL_COUNT = 7;
public:
  ObAllVirtualSSExistingSSTableMgr();
  virtual ~ObAllVirtualSSExistingSSTableMgr();
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
    TABLE_TYPE,
    START_LOG_SCN,
    END_LOG_SCN,
    UPPER_TRANS_VERSION,
    SIZE,
    DATA_BLOCK_COUNT,
    INDEX_BLOCK_COUNT,
    LINKED_BLOCK_COUNT,
    CONTAIN_UNCOMMITTED_ROW,
    NESTED_OFFSET,
    NESTED_SIZE,
    CG_IDX,
    DATA_CHECKSUM,
    TABLE_FLAG,
    REC_SCN
  };
private:
#ifdef OB_BUILD_SHARED_STORAGE
  int get_primary_key_();
  int get_first_key_(ObNewRange &key_range);
  int check_rowkey_same_(ObNewRange &key_range);
  int get_next_tablet_();
  int get_next_table_(storage::ObITable *&table);
  int generate_virtual_row_(VirtualSSSSTableRow &row);
  int fill_in_row_(const VirtualSSSSTableRow &row_data, common::ObNewRow *&row);
  int extract_result_(common::sqlclient::ObMySQLResult &res, VirtualSSSSTableRow &row);
  int get_virtual_row_remote_(VirtualSSSSTableRow &row);
  int get_virtual_row_remote_(const uint64_t tenant_id,
                              const share::ObLSID &ls_id,
                              common::ObTabletID &tablet_id,
                              share::SCN &transfer_scn,
                              VirtualSSSSTableRow &row);
#endif
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;

  common::ObArenaAllocator tablet_allocator_;
  ObTablet *tablet_;
  storage::ObTableStoreIterator table_store_iter_;
  VirtualSSSSTableRow ss_sstable_row_;
  ObTabletHandle tablet_hdl_; // for sstablet life
  ObISQLClient::ReadResult read_result_;
  common::sqlclient::ObMySQLResult *sql_result_;
#ifdef OB_BUILD_SHARED_STORAGE
  share::SCN cur_row_scn_; // current tablet's scn
  ObSSMetaIterGuard<ObSSTabletIterator> tablet_iter_guard_; // for tablet_hdl_ life
  ObSSTabletIterator *tablet_iter_;
#endif
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSExistingSSTableMgr);
};

} // observer
} // oceanbase
#endif
