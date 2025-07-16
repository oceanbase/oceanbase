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

#ifndef OB_ALL_VIRTUAL_SS_LS_TABLET_REORG_INFO_H_
#define OB_ALL_VIRTUAL_SS_LS_TABLET_REORG_INFO_H_

#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"
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
namespace observer
{
class ObAllVirtualSSLsTabletReorgInfo : public common::ObVirtualTableScannerIterator,
                               public omt::ObMultiTenantOperator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    TABLET_ID,
    REORGANIZATION_SCN,
    DATA_TYPE,
    COMMIT_SCN,
    VALUE,
  };

public:
  ObAllVirtualSSLsTabletReorgInfo();
  virtual ~ObAllVirtualSSLsTabletReorgInfo();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }
private:
  // whether a tenant is need return content.
  virtual bool is_need_process(uint64_t tenant_id) override;
  // deal with current tenant's row.
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  // release last tenant's resource.
  virtual void release_last_tenant() override;
  int get_next_ls_(ObLS *&ls);
  int get_next_reorg_info_data_(ObTabletReorgInfoData &data, share::SCN &commit_scn);
  int init_read_op_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);
  void free_read_op_(ObTabletReorgInfoTableReadOperator *&read_op);

private:
  common::ObAddr addr_;
  int64_t ls_id_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
  ObTabletReorgInfoTableReadOperator *read_op_;
  char buf_[OB_MAX_VARCHAR_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSLsTabletReorgInfo);
};

}
}
#endif /* OB_ALL_VIRTUAL_SS_LS_TABLET_REORG_INFO_H_ */
