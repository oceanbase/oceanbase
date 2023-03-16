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

#ifndef OCEANBASE_SHARE_OB_TABLET_CHECKSUM_OPERATOR_H_
#define OCEANBASE_SHARE_OB_TABLET_CHECKSUM_OPERATOR_H_

#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "common/ob_zone.h"
#include "common/ob_tablet_id.h"
#include "share/ob_tablet_replica_checksum_operator.h"

namespace oceanbase
{
namespace share
{
// Memory item for __all_tablet_checksum table
//
// The data in __all_tablet_checksum table, is sync from
// __all_tablet_replica_checksum table. This table will be
// sync to standby cluster from primary cluster, for these
// two cluster checksum verifying.
struct ObTabletChecksumItem
{
public:
  ObTabletChecksumItem() 
    : tenant_id_(OB_INVALID_TENANT_ID), tablet_id_(), ls_id_(), data_checksum_(-1), 
      row_count_(0), snapshot_version_(0), replica_type_(0), column_meta_() {}
  virtual ~ObTabletChecksumItem() = default;

  void reset();
  bool is_valid() const;
  bool is_same_tablet(const ObTabletChecksumItem &item) const;
  int compare_tablet(const ObTabletReplicaChecksumItem &replica_item) const;
  int verify_tablet_checksum(const ObTabletReplicaChecksumItem &replica_item) const;
  int assign(const ObTabletReplicaChecksumItem &replica_item);
  ObTabletChecksumItem &operator =(const ObTabletChecksumItem &other);

  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(ls_id), K_(data_checksum), K_(row_count), 
    K_(snapshot_version), K_(replica_type), K_(column_meta));
  
  uint64_t tenant_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  int64_t data_checksum_;
  int64_t row_count_;
  int64_t snapshot_version_;
  int replica_type_;
  ObTabletReplicaReportColumnMeta column_meta_;
};

// CRUD operation to __all_tablet_checksum table
class ObTabletChecksumOperator
{
public:
  // range get tablet checksum
  // @snapshot_version:
  //   if equals to 0, means get all snapshot_version
  //   if greater than 0, means get specified snapshot_version
  //   else, invalid argument
  static int load_tablet_checksum_items(
      common::ObISQLClient &sql_client,
      const ObTabletLSPair &start_pair,
      const int64_t batch_cnt,
      const uint64_t tenant_id,
      const int64_t snapshot_version,
      common::ObIArray<ObTabletChecksumItem> &items);
  // multi get tablet checksum
  static int load_tablet_checksum_items(
      common::ObISQLClient &sql_client,
      const common::ObIArray<ObTabletLSPair> &pairs,
      const uint64_t tenant_id,
      common::ObIArray<ObTabletChecksumItem> &items);
  static int load_tablet_checksum_items(
      common::ObISQLClient &sql_client,
      const common::ObSqlString &sql,
      const uint64_t tenant_id,
      common::ObIArray<ObTabletChecksumItem> &items);
  static int insert_tablet_checksum_item(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObTabletChecksumItem &item);
  static int insert_tablet_checksum_items(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      common::ObIArray<ObTabletChecksumItem> &items);
  static int update_tablet_checksum_items(
      common::ObISQLClient &sql_client, 
      const uint64_t tenant_id,
      common::ObIArray<ObTabletChecksumItem> &items);

private:
  static int construct_load_sql_str_(
      const uint64_t tenant_id,
      const ObTabletLSPair &start_pair,
      const int64_t batch_cnt,
      const int64_t snapshot_version,
      common::ObSqlString &sql);
  static int construct_load_sql_str_(
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletLSPair> &pairs,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObSqlString &sql);
  static int insert_or_update_tablet_checksum_items_(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      common::ObIArray<ObTabletChecksumItem> &items,
      const bool is_update);

private:
  const static int64_t MAX_BATCH_COUNT = 999;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_TABLET_CHECKSUM_OPERATOR_H_
