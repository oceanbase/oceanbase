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

#ifndef OB_ALL_VIRTUAL_WEAK_READ_STAT_H_
#define OB_ALL_VIRTUAL_WEAK_READ_STAT_H_


#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "share/ob_define.h"
#include "storage/tx/wrs/ob_tenant_weak_read_stat.h"
#include "storage/tx/wrs/ob_tenant_weak_read_service.h"

namespace oceanbase
{
namespace transaction{
  class ObTenantWeakReadStat;
}
namespace observer
{
class ObAllVirtualWeakReadStat : public common::ObVirtualTableScannerIterator
{
enum columns{TENANT_ID, SERVER_IP, SERVER_PORT, SERVER_VERSION, SERVER_VERSION_DELTA,
  LOCAL_CLUSTER_VERSION, LOCAL_CLUSTER_VERSION_DELTA, TOTAL_PART_COUNT, VALID_INNER_PART_COUNT,
  VALID_USER_PART_COUNT, CLUSTER_MASTER_IP, CLUSTER_MASTER_PORT, CLUSTER_HEART_BEAT_TS,
  CLUSTER_HEART_BEAT_COUNT, CLUSTER_HEART_BEAT_SUCC_TS, CLUSTER_HEART_BEAT_SUCC_COUNT,
  SELF_CHECK_TS, LOCAL_CURRENT_TS, IN_CLUSTER_SERVICE, IS_CLUSTER_MASTER, CLUSTER_MASTER_EPOCH,
  CLUSTER_SERVERS_COUNT, CLUSTER_SKIPPED_SERVERS_COUNT, CLUSTER_VERSION_GEN_TS, CLUSTER_VERSION,
  CLUSTER_VERSION_DELTA, MIN_CLUSTER_VERSION, MAX_CLUSTER_VERSION};
public:
ObAllVirtualWeakReadStat();
~ObAllVirtualWeakReadStat();
void reset();
int inner_get_next_row(common::ObNewRow *&row);
protected:
bool start_to_read_;
};

} //observer
} //oceanbase
#endif /* OB_ALL_VIRTUAL_WEAK_READ_STAT_H_ */
