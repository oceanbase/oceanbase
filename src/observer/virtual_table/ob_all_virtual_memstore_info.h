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

#ifndef OB_ALL_VIRTUAL_MEMSTORE_INFO_H_
#define OB_ALL_VIRTUAL_MEMSTORE_INFO_H_

#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtable;
}
namespace observer
{
class ObAllVirtualMemstoreInfo : public common::ObVirtualTableScannerIterator,
                                 public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualMemstoreInfo();
  virtual ~ObAllVirtualMemstoreInfo();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  int get_next_ls(ObLS *&ls);
  int get_next_tablet(storage::ObTabletHandle &tablet_handle);
  int get_next_memtable(memtable::ObMemtable *&mt);
  void get_freeze_time_dist(const memtable::ObMtStat& mt_stat);
private:
  common::ObAddr addr_;
  int64_t ls_id_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
  storage::ObLSTabletIterator ls_tablet_iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  common::ObSEArray<ObTableHandleV2, 2> tables_handle_;
  int64_t memtable_array_pos_;
  char freeze_time_dist_[OB_MAX_CHAR_LENGTH];
  char compaction_info_buf_[common::OB_COMPACTION_INFO_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMemstoreInfo);
};

}
}
#endif /* OB_ALL_VIRTUAL_MEMSTORE_INFO_H */
