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

#ifndef OCEANBASE_SHARE_OB_TABLET_REPLICA_CHECKSUM_OPERATOR_H_
#define OCEANBASE_SHARE_OB_TABLET_REPLICA_CHECKSUM_OPERATOR_H_

#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_table_schema.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/ob_column_checksum_error_operator.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObAddr;
class ObTabletID;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class ObTabletReplica;

struct ObTabletReplicaReportColumnMeta
{
public:
  ObTabletReplicaReportColumnMeta();
  ~ObTabletReplicaReportColumnMeta();
  void reset();
  bool is_valid() const;
  int init(const common::ObIArray<int64_t> &column_checksums);
  int assign(const ObTabletReplicaReportColumnMeta &other);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_size() const;
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  int check_checksum(const ObTabletReplicaReportColumnMeta &other, const int64_t pos, bool &is_equal) const;
  int check_all_checksums(const ObTabletReplicaReportColumnMeta &other, bool &is_equal) const;
  int check_equal(const ObTabletReplicaReportColumnMeta &other, bool &is_equal) const;
  int get_column_checksum(const int64_t pos, int64_t &checksum) const;
  int64_t get_string(char *buf, const int64_t buf_len) const;
  int64_t get_string_length() const;
  TO_STRING_KV(K_(compat_version), K_(checksum_method), K_(checksum_bytes), K_(column_checksums));

public:
  static const int64_t MAX_OCCUPIED_BYTES = 4000 * 8 + 11;
  static const int64_t DEFAULT_COLUMN_CNT = 64;
  static const int64_t MAGIC_NUMBER = static_cast<int64_t>(0x636865636B636F6CL); // cstirng of "checkcol"
  int8_t compat_version_;
  int8_t checksum_method_;
  int8_t checksum_bytes_;
  common::ObSEArray<int64_t, DEFAULT_COLUMN_CNT> column_checksums_;
  bool is_inited_;
};

struct ObTabletReplicaChecksumItem
{
public:
  ObTabletReplicaChecksumItem();
  virtual ~ObTabletReplicaChecksumItem() { reset(); };
  void reset();
  bool is_key_valid() const;
  bool is_valid() const;
  bool is_same_tablet(const ObTabletReplicaChecksumItem &other) const;
  int verify_checksum(const ObTabletReplicaChecksumItem &other) const;
  int assign_key(const ObTabletReplicaChecksumItem &other);
  ObTabletReplicaChecksumItem &operator =(const ObTabletReplicaChecksumItem &other);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(server), K_(row_count),
      K_(snapshot_version), K_(data_checksum), K_(column_meta));

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObAddr server_;
  int64_t row_count_;
  int64_t snapshot_version_;
  int64_t data_checksum_;
  ObTabletReplicaReportColumnMeta column_meta_;
};

// Operator for __all_tablet_replica_checksum
class ObTabletReplicaChecksumOperator
{
public:
  static int batch_get(
      const uint64_t tenant_id,
      const ObTabletLSPair &start_pair,
      const int64_t batch_cnt,
      const int64_t snapshot_version,
      common::ObISQLClient &sql_proxy,
      common::ObIArray<ObTabletReplicaChecksumItem> &items);
  static int batch_get(
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletLSPair> &pairs,
      common::ObISQLClient &sql_proxy,
      common::ObIArray<ObTabletReplicaChecksumItem> &items);
  static int batch_get(
      const uint64_t tenant_id,
      const common::ObSqlString &sql,
      common::ObISQLClient &sql_proxy,
      common::ObIArray<ObTabletReplicaChecksumItem> &items);
  static int batch_update_with_trans(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletReplicaChecksumItem> &item);
  static int batch_remove_with_trans(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const common::ObIArray<share::ObTabletReplica> &tablet_replicas);
  static int remove_residual_checksum(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObAddr &server,
      const int64_t limit,
      int64_t &affected_rows);

  static int check_column_checksum(
      const uint64_t tenant_id,
      const schema::ObTableSchema &data_table_schema,
      const schema::ObTableSchema &index_table_schema,
      const int64_t global_snapshot_version,
      common::ObMySQLProxy &sql_proxy);

  static int set_column_meta_with_hex_str(
      const ObString &hex_str,
      ObTabletReplicaReportColumnMeta &column_meta);

  static int get_visible_column_meta(
      const ObTabletReplicaReportColumnMeta &column_meta,
      common::ObIAllocator &allocator,
      common::ObString &column_meta_visible_str);

  static int get_hex_column_meta(
      const ObTabletReplicaReportColumnMeta &column_meta,
      common::ObIAllocator &allocator,
      common::ObString &column_meta_hex_str);

private:
  static int batch_insert_or_update_with_trans_(
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletReplicaChecksumItem> &items,
      common::ObMySQLTransaction &trans,
      const bool is_update);

  static int inner_batch_insert_or_update_by_sql_(
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletReplicaChecksumItem> &items,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObISQLClient &sql_client,
      const bool is_update);

  static int inner_batch_remove_by_sql_(
      const uint64_t tenant_id,
      const common::ObIArray<share::ObTabletReplica> &tablet_replicas,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObMySQLTransaction &trans);

  static int inner_batch_get_by_sql_(
      const uint64_t tenant_id,
      const common::ObSqlString &sql,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObTabletReplicaChecksumItem> &items);

  static int construct_batch_get_sql_str_(
      const uint64_t tenant_id,
      const ObTabletLSPair &start_pair,
      const int64_t batch_cnt,
      const int64_t snapshot_version,
      common::ObSqlString &sql);

  static int construct_batch_get_sql_str_(
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletLSPair> &pairs,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObSqlString &sql);

  static int inner_init_tablet_pair_map_(
      const ObIArray<ObTabletLSPair> &pairs,
      hash::ObHashMap<ObTabletLSPair, bool> &pair_map);

  static int construct_tablet_replica_checksum_items_(
      common::sqlclient::ObMySQLResult &res,
      common::ObIArray<ObTabletReplicaChecksumItem> &items);

  static int construct_tablet_replica_checksum_item_(
      common::sqlclient::ObMySQLResult &res,
      ObTabletReplicaChecksumItem &item);

  static int check_global_index_column_checksum(
      const uint64_t tenant_id,
      const schema::ObTableSchema &data_table_schema,
      const schema::ObTableSchema &index_table_schema,
      const int64_t global_snapshot_version,
      common::ObMySQLProxy &sql_proxy);

  static int check_local_index_column_checksum(
      const uint64_t tenant_id,
      const schema::ObTableSchema &data_table_schema,
      const schema::ObTableSchema &index_table_schema,
      const int64_t global_snapshot_version,
      common::ObMySQLProxy &sql_proxy);

  // get column checksum_sum from items and store result in map
  // KV of @column_ckm_sum_map is: <column_id, column_checksum_sum>
  static int get_column_checksum_sum_map_(
      const schema::ObTableSchema &table_schema,
      const int64_t global_snapshot_version,
      common::hash::ObHashMap<int64_t, int64_t> &column_ckm_sum_map,
      const common::ObIArray<ObTabletReplicaChecksumItem> &items);

  // get column checksum from item and store result in map
  // KV of @column_ckm_map is: <column_id, column_checksum>
  static int get_column_checksum_map_(
      const schema::ObTableSchema &table_schema,
      const int64_t global_snapshot_version,
      common::hash::ObHashMap<int64_t, int64_t> &column_ckm_map,
      const ObTabletReplicaChecksumItem &item);

  static int get_tablet_replica_checksum_items_(
      const uint64_t tenant_id,
      common::ObMySQLProxy &mysql_proxy,
      const schema::ObTableSchema &table_schema,
      common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<ObTabletReplicaChecksumItem> &items);

  static int find_checksum_item_by_id_(
      const common::ObTabletID &tablet_id,
      common::ObIArray<ObTabletReplicaChecksumItem> &items,
      const int64_t global_snapshot_version,
      int64_t &idx);

  static int get_table_all_tablet_id_(
      const schema::ObTableSchema &table_schema,
      common::ObIArray<common::ObTabletID> &schema_tablet_ids);

  static int need_verify_checksum_(
      const int64_t global_snapshot_version,
      bool &need_verify,
      common::ObIArray<common::ObTabletID> &schema_tablet_ids,
      common::ObIArray<ObTabletReplicaChecksumItem> &items);

  static int compare_column_checksum_(
      const schema::ObTableSchema &data_table_schema,
      const schema::ObTableSchema &index_table_schema,
      const common::hash::ObHashMap<int64_t, int64_t> &data_column_ckm_map,
      const common::hash::ObHashMap<int64_t, int64_t> &index_column_ckm_map,
      int64_t &check_cnt,
      ObColumnChecksumErrorInfo &ckm_error_info);

  static void print_detail_tablet_replica_checksum(
      const common::ObIArray<ObTabletReplicaChecksumItem> &items);

private:
  const static int64_t MAX_BATCH_COUNT = 99;
};

} // share
} // oceanbase

#endif // OCEANBASE_SHARE_OB_TABLET_REPLICA_CHECKSUM_OPERATOR_H_