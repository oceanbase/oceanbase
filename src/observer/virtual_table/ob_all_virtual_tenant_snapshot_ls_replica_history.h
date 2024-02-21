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

#include "observer/virtual_table/ob_iterate_private_virtual_table.h"
#include "storage/ls/ob_ls_meta_package.h"

namespace oceanbase {
namespace observer {

class ObAllVirtualTenantSnapshotLSReplicaHistory : public ObIteratePrivateVirtualTable
{
public:
  ObAllVirtualTenantSnapshotLSReplicaHistory() {}
  virtual ~ObAllVirtualTenantSnapshotLSReplicaHistory() {}
  virtual int try_convert_row(const ObNewRow *input_row, ObNewRow *&row) override;

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
  int decode_hex_string_to_package_(const ObString& hex_str,
                                    ObLSMetaPackage& ls_meta_package);

private:
  static const int64_t LS_META_BUFFER_SIZE = 16 * 1024;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantSnapshotLSReplicaHistory);
};

}  // namespace observer
}  // namespace oceanbase
#endif  // OB_OBSERVER_OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_H
