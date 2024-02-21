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
#include "share/scn.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/compaction/ob_tenant_medium_checker.h"

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
  int assign(const ObTabletReplicaChecksumItem &other);
  int set_tenant_id(const uint64_t tenant_id);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(server), K_(row_count),
      K_(compaction_scn), K_(data_checksum), K_(column_meta));

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObAddr server_;
  int64_t row_count_;
  SCN compaction_scn_;
  int64_t data_checksum_;
  ObTabletReplicaReportColumnMeta column_meta_;
};

// Operator for __all_tablet_replica_checksum
class ObTabletReplicaChecksumOperator
{
public:
  // Get a batch of checksum_items
  // Default: checksum_items' compaction_scn = @compaction_scn
  // If include_larger_than = true: checksum_items' compaction_scn >= @compaction_scn
  static int batch_get(
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletLSPair> &pairs,
      const SCN &compaction_scn,
      common::ObISQLClient &sql_proxy,
      common::ObIArray<ObTabletReplicaChecksumItem> &items,
      int64_t &tablet_items_cnt,
      const bool include_larger_than,
      const int32_t group_id,
      const bool with_order_by_field);
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
  static int get_tablets_replica_checksum(
      const uint64_t tenant_id,
      const ObIArray<compaction::ObTabletCheckInfo> &pairs,
      ObIArray<ObTabletReplicaChecksumItem> &tablet_replica_checksum_items);
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
      const int32_t group_id,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObTabletReplicaChecksumItem> &items,
      int64_t &tablet_items_cnt);

  template<typename T>
  static int construct_batch_get_sql_str_(
      const uint64_t tenant_id,
      const SCN &compaction_scn,
      const common::ObIArray<T> &pairs,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObSqlString &sql,
      const bool include_larger_than,
      const bool with_compaction_scn,
      const bool with_order_by_field);

  static int construct_tablet_replica_checksum_items_(
      common::sqlclient::ObMySQLResult &res,
      common::ObIArray<ObTabletReplicaChecksumItem> &items,
      int64_t &tablet_items_cnt);

  static int construct_tablet_replica_checksum_item_(
      common::sqlclient::ObMySQLResult &res,
      ObTabletReplicaChecksumItem &item);

public:
  // get column checksum from item and store result in map
  // KV of @column_ckm_map is: <column_id, column_checksum>
  static int get_tablet_replica_checksum_items(
      const uint64_t tenant_id,
      common::ObMySQLProxy &mysql_proxy,
      const SCN &compaction_scn,
      const common::ObIArray<ObTabletLSPair> &tablet_pairs,
      common::ObIArray<ObTabletReplicaChecksumItem> &items);
private:
  const static int64_t MAX_BATCH_COUNT = 120;
  const static int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
};

template<typename T>
int ObTabletReplicaChecksumOperator::construct_batch_get_sql_str_(
    const uint64_t tenant_id,
    const SCN &compaction_scn,
    const ObIArray<T> &pairs,
    const int64_t start_idx,
    const int64_t end_idx,
    ObSqlString &sql,
    const bool include_larger_than,
    const bool with_compaction_scn,
    const bool with_order_by_field)
{
  int ret = OB_SUCCESS;
  const int64_t pairs_cnt = pairs.count();
  if (start_idx < 0 || end_idx > pairs_cnt || start_idx > end_idx ||
      pairs_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KR(ret), K(start_idx), K(end_idx), K(pairs_cnt));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' AND (tablet_id, ls_id) IN ((",
      OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id))) {
    SHARE_LOG(WARN, "fail to assign sql", KR(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    ObSqlString order_by_sql;
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const T &pair = pairs.at(idx);
      if (OB_UNLIKELY(!pair.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "invalid tablet_ls_pair", KR(ret), K(tenant_id), K(pair));
      } else if (OB_FAIL(sql.append_fmt(
          "'%lu', %ld%s",
          pair.get_tablet_id().id(),
          pair.get_ls_id().id(),
          ((idx == end_idx - 1) ? "))" : "), (")))) {
        SHARE_LOG(WARN,"fail to assign sql", KR(ret), K(tenant_id), K(pair));
      } else if (OB_FAIL(order_by_sql.append_fmt(
          ",%ld",
          pair.get_tablet_id().id()))) {
        SHARE_LOG(WARN, "fail to assign sql", KR(ret), K(tenant_id), K(pair));
      }
    } // end of for
    if (OB_SUCC(ret) && with_compaction_scn) {
      if (OB_FAIL(sql.append_fmt(" AND compaction_scn %s %ld",
          include_larger_than ? ">=" : "=", compaction_scn.get_val_for_inner_table_field()))) {
        SHARE_LOG(WARN, "fail to assign sql", KR(ret), K(tenant_id), K(compaction_scn));
      }
    }
    if (OB_FAIL(ret) || !with_order_by_field) {
    } else if (OB_FAIL(sql.append_fmt(" ORDER BY FIELD(tablet_id%s)", order_by_sql.string().ptr()))) {
      SHARE_LOG(WARN, "fail to assign sql string", KR(ret), K(tenant_id), K(compaction_scn), K(pairs_cnt));
    }
  }
  return ret;
}


} // share
} // oceanbase

#endif // OCEANBASE_SHARE_OB_TABLET_REPLICA_CHECKSUM_OPERATOR_H_
