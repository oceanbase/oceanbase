/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_TABLET_DDL_KV_INFO_H_
#define OB_ALL_VIRTUAL_TABLET_DDL_KV_INFO_H_

#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualTabletDDLKVInfo : public common::ObVirtualTableScannerIterator,
                                    public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualTabletDDLKVInfo();
  virtual ~ObAllVirtualTabletDDLKVInfo();
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
  int get_next_ddl_kv_mgr(storage::ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int get_next_ddl_kv(ObDDLKV *&ddl_kv);
private:
  common::ObAddr addr_;
  int64_t ls_id_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
  storage::ObLSTabletIterator ls_tablet_iter_;
  ObArray<ObDDLKVHandle> ddl_kvs_handle_;
  common::ObTabletID curr_tablet_id_;
  int64_t ddl_kv_idx_;
  char ip_buf_[common::OB_IP_STR_BUFF];
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletDDLKVInfo);
};

}
}
#endif /* OB_ALL_VIRTUAL_TABLET_DDL_KV_INFO_H_ */
