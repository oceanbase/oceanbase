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

#ifndef OB_ALL_VIRTUAL_OB_SS_SSTABLE_MGR_H_
#define OB_ALL_VIRTUAL_OB_SS_SSTABLE_MGR_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"
#include "common/ob_tablet_id.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ob_shared_meta_service.h"
#endif
namespace oceanbase
{
namespace observer
{
struct VirtualSSSSTableRow {
  int64_t version_;
  int64_t table_type_;
  int64_t start_log_scn_;
  int64_t end_log_scn_;
  int64_t upper_trans_version_;
  int64_t size_;
  int64_t data_block_count_;
  int64_t index_block_count_;
  int64_t linked_block_count_;
  bool contain_uncommitted_row_;
  int64_t nested_offset_;
  int64_t nested_size_;
  int64_t cg_idx_;
  int64_t data_checksum_;
  int64_t table_flag_;
  int64_t rec_scn_;

  VirtualSSSSTableRow()
    : version_(),
      table_type_(0),
      start_log_scn_(0),
      end_log_scn_(0),
      upper_trans_version_(0),
      size_(0),
      data_block_count_(0),
      index_block_count_(0),
      linked_block_count_(0),
      contain_uncommitted_row_(false),
      nested_offset_(0),
      nested_size_(0),
      cg_idx_(0),
      data_checksum_(0),
      table_flag_(0),
      rec_scn_(0)
  { }

  TO_STRING_KV(K(version_), K(table_type_), K(start_log_scn_), K(end_log_scn_),
               K(upper_trans_version_), K(size_), K(data_block_count_),
               K(index_block_count_), K(linked_block_count_),
               K(nested_offset_), K(nested_offset_), K(nested_size_),
               K(cg_idx_), K(data_checksum_), K(table_flag_), K(rec_scn_), K(contain_uncommitted_row_));
};

class ObAllVirtualSSSSTableMgr : public common::ObVirtualTableScannerIterator
{
  static const int64_t ROWKEY_COL_COUNT = 7;
public:
  ObAllVirtualSSSSTableMgr();
  virtual ~ObAllVirtualSSSSTableMgr();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  enum TABLE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    LS_ID,
    TABLET_ID,
    TRANSFER_SCN,
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
                              VirtualSSSSTableRow &row);
#endif
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObArenaAllocator tablet_allocator_;
  ObTablet *tablet_;
  storage::ObTableStoreIterator table_store_iter_;
  VirtualSSSSTableRow ss_sstable_row_;
  ObTabletHandle tablet_hdl_; // for sstablet life
  share::SCN cur_reorganization_scn_;
  ObISQLClient::ReadResult read_result_;
  common::sqlclient::ObMySQLResult *sql_result_;
#ifdef OB_BUILD_SHARED_STORAGE
  ObSSMetaIterGuard<ObSSTabletIterator> tablet_iter_guard_; // for tablet_hdl_ life
  ObSSTabletIterator *tablet_iter_;
#endif
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSSSTableMgr);
};

} // observer
} // oceanbase
#endif
