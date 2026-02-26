/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_CS_REPLICA_TABLET_STATS_H_
#define OB_ALL_VIRTUAL_CS_REPLICA_TABLET_STATS_H_

#include "common/row/ob_row.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
namespace oceanbase
{
namespace storage
{
class ObTenantTabletIterator;
}
namespace observer
{

class ObAllVirtualTableLSTabletIter : public common::ObVirtualTableScannerIterator,
                                      public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualTableLSTabletIter();
  virtual ~ObAllVirtualTableLSTabletIter();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  int init(common::ObIAllocator *allocator, common::ObAddr &addr);
protected:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual void release_last_tenant() override { inner_reset(); };
  virtual int process_curr_tenant(common::ObNewRow *&row) = 0;
  virtual void inner_reset();
  virtual int inner_get_ls_infos(const ObLS &ls) { UNUSED(ls); return OB_SUCCESS; }
  virtual int check_need_iterate_ls(const ObLS &ls, bool &need_iterate);
  int get_next_ls(ObLS *&ls);
  int get_next_tablet(storage::ObTabletHandle &tablet_handle);
protected:
  common::ObAddr addr_;
  int64_t ls_id_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
  storage::ObLSTabletIterator ls_tablet_iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTableLSTabletIter);
};

class ObAllVirtualCSReplicaTabletStats : public ObAllVirtualTableLSTabletIter
{
  enum COLUMN_ID_LIST
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    LS_ID,
    TABLET_ID,
    MACRO_BLOCK_CNT,
    IS_CS,
    IS_CS_REPLICA,
    AVAILABLE
  };
public:
  ObAllVirtualCSReplicaTabletStats();
  virtual ~ObAllVirtualCSReplicaTabletStats();
private:
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void inner_reset() override;
  virtual int inner_get_ls_infos(const ObLS &ls) override;
  virtual int check_need_iterate_ls(const ObLS &ls, bool &need_iterate) override;
protected:
  ObMigrationStatus migration_status_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualCSReplicaTabletStats);
};

} // end observer
} // end oceanbase

#endif /* OB_ALL_VIRTUAL_CS_REPLICA_TABLET_STATS_H_ */