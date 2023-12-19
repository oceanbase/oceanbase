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

#ifndef OB_OBSERVER_OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_H
#define OB_OBSERVER_OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_H

#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/ls/ob_ls_meta_package.h"

namespace oceanbase {
namespace observer {

class ObAllVirtualTenantSnapshotLSReplicaHistory : public common::ObVirtualTableScannerIterator,
                                            public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualTenantSnapshotLSReplicaHistory();
  virtual ~ObAllVirtualTenantSnapshotLSReplicaHistory();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  virtual int inner_open();

private:
  enum COLUMN_ID_LIST
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SNAPSHOT_ID,
    LS_ID,
    SVR_IP,
    SVR_PORT,
    GMT_CREATE,
    GMT_MODIFIED,
    STATUS,
    ZONE,
    UNIT_ID,
    BEGIN_INTERVAL_SCN,
    END_INTERVAL_SCN,
    LS_META_PACKAGE
  };
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  int decode_hex_string_to_package_(const ObString& hex_str,
                                    ObIAllocator& allocator,
                                    ObLSMetaPackage& ls_meta_package);
  int get_tenant_snapshot_ls_replica_entries_();

private:
  static const int64_t LS_META_BUFFER_SIZE = 16 * 1024;
  char *ls_meta_buf_;
  common::ObMySQLProxy::MySQLResult *sql_res_;
  common::sqlclient::ObMySQLResult *result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantSnapshotLSReplicaHistory);
};

}  // namespace observer
}  // namespace oceanbase
#endif  // OB_OBSERVER_OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_H
