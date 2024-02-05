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

#ifndef OB_OBSERVER_OB_ALL_VIRTUAL_LS_SNAPSHOT_IN_STORAGE_NODE_H
#define OB_OBSERVER_OB_ALL_VIRTUAL_LS_SNAPSHOT_IN_STORAGE_NODE_H

#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_mgr.h"

namespace oceanbase
{
namespace storage
{
class ObLSSnapshotVTInfo;
};

namespace observer
{

class ObAllVirtualLSSnapshot : public common::ObVirtualTableScannerIterator,
                               public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualLSSnapshot();
  virtual ~ObAllVirtualLSSnapshot();

public:
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(ObAddr &addr) {
    addr_ = addr;
  }

private:
  enum COLUMN_ID_LIST
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SNAPSHOT_ID,
    LS_ID,
    SVR_IP,
    SVR_PORT,
    META_EXISTED,
    BUILD_STATUS,
    REBUILD_SEQ_START,
    REBUILD_SEQ_END,
    END_INTERVAL_SCN,
    LS_META_PACKAGE,
    TSNAP_IS_RUNNING,
    TSNAP_HAS_UNFINISHED_CREATE_DAG,
    TSNAP_HAS_UNFINISHED_GC_DAG,
    TSNAP_CLONE_REF,
    TSNAP_META_EXISTED
  };
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  int get_next_ls_snapshot_vt_info_(ObLSSnapshotVTInfo &ls_snapshot_vt_info);
  int fill_row_(ObLSSnapshotVTInfo &ls_snap_info);

private:
  static constexpr int64_t LS_META_BUFFER_SIZE = 16384;

  common::ObAddr addr_;
  char *ls_meta_package_buf_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  ObArray<ObLSSnapshotMapKey> ls_snapshot_key_arr_;
  int64_t ls_snap_idx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLSSnapshot);
};

} // namespace observer
} // namespace oceanbase
#endif  // OB_OBSERVER_OB_ALL_VIRTUAL_LS_SNAPSHOT_IN_STORAGE_NODE_H
