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

#ifndef OCEANBASE_SHARE_OB_SSTABLE_CHECKSUM_OPERATOR_H_
#define OCEANBASE_SHARE_OB_SSTABLE_CHECKSUM_OPERATOR_H_

#include "lib/net/ob_addr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {
namespace share {
// sstable data checksum item
struct ObSSTableDataChecksumItem {
public:
  ObSSTableDataChecksumItem();
  virtual ~ObSSTableDataChecksumItem() = default;
  void reset();
  bool is_key_valid() const;
  bool is_valid() const;
  bool is_same_table(const ObSSTableDataChecksumItem& other) const;
  TO_STRING_KV(K_(tenant_id), K_(data_table_id), K_(sstable_id), K_(partition_id), K_(sstable_type), K_(server),
      K_(row_checksum), K_(data_checksum), K_(row_count), K_(snapshot_version), K_(replica_type));
  uint64_t tenant_id_;
  uint64_t data_table_id_;
  uint64_t sstable_id_;
  int64_t partition_id_;
  int sstable_type_;
  common::ObAddr server_;
  int64_t row_checksum_;
  int64_t data_checksum_;
  int64_t row_count_;
  int64_t snapshot_version_;
  int replica_type_;
};

class ObSSTableDataChecksumOperator {
public:
  ObSSTableDataChecksumOperator() = default;
  virtual ~ObSSTableDataChecksumOperator() = default;
  static int get_checksum(ObSSTableDataChecksumItem& item, common::ObISQLClient& proxy);
  static int get_checksum(const uint64_t data_table_id, const uint64_t sstable_id, const int64_t partition_id,
      const int sstable_type, common::ObIArray<ObSSTableDataChecksumItem>& items, common::ObISQLClient& sql_proxy);
  static int need_verify_checksum(const share::schema::ObTableSchema& data_table_schema,
      const int64_t global_snapshot_version, int64_t& snapshot_version, bool& need_verify,
      common::ObISQLClient& sql_proxy);
  static int get_major_checksums(const uint64_t start_tenant_id, const uint64_t start_data_table_id,
      const uint64_t start_sstable_id, const int64_t start_partition_id, const int64_t batch_cnt,
      common::ObIArray<ObSSTableDataChecksumItem>& items, common::ObISQLClient& sql_proxy);
  static int get_replicas(const uint64_t data_table_id, const uint64_t sstable_id, const int64_t partition_id,
      const int sstable_type, common::ObIArray<common::ObAddr>& replicas, common::ObISQLClient& sql_proxy);
  static int batch_report_checksum(
      const common::ObIArray<ObSSTableDataChecksumItem>& items, common::ObISQLClient& proxy);
  static int batch_remove_checksum(
      const common::ObIArray<ObSSTableDataChecksumItem>& items, common::ObISQLClient& proxy);
  static int check_table_checksum(
      const uint64_t data_table_id, const uint64_t index_id, common::ObISQLClient& sql_proxy, bool& is_checksum_valid);

private:
  static int fill_one_item(const ObSSTableDataChecksumItem& item, share::ObDMLSqlSplicer& dml);
  static int get_checksum(const common::ObSqlString& sql, common::ObIArray<ObSSTableDataChecksumItem>& items,
      common::ObISQLClient& sql_proxy);
  static int remove_one_item(const ObSSTableDataChecksumItem& item, common::ObISQLClient& sql_proxy);
};

struct ObSSTableColumnChecksumItem {
public:
  ObSSTableColumnChecksumItem()
      : tenant_id_(common::OB_INVALID_ID),
        data_table_id_(common::OB_INVALID_ID),
        index_id_(common::OB_INVALID_ID),
        partition_id_(common::OB_INVALID_ID),
        sstable_type_(-1),
        column_id_(common::OB_INVALID_ID),
        server_(),
        column_checksum_(0),
        checksum_method_(0),
        snapshot_version_(0),
        replica_type_(-1),
        major_version_(-1)
  {}
  ~ObSSTableColumnChecksumItem() = default;
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != data_table_id_ &&
           common::OB_INVALID_ID != data_table_id_ && partition_id_ >= 0 && sstable_type_ >= 0 && column_id_ >= 0 &&
           server_.is_valid() && snapshot_version_ >= 0 && checksum_method_ >= 0 && replica_type_ >= 0 &&
           major_version_ >= 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(data_table_id), K_(index_id), K_(partition_id), K_(sstable_type), K_(column_id),
      K_(server), K_(column_checksum), K_(snapshot_version), K_(replica_type), K_(major_version));
  uint64_t tenant_id_;
  uint64_t data_table_id_;
  uint64_t index_id_;
  int64_t partition_id_;
  int sstable_type_;
  int64_t column_id_;
  common::ObAddr server_;
  int64_t column_checksum_;
  int64_t checksum_method_;
  int64_t snapshot_version_;
  int replica_type_;
  int64_t major_version_;
};

class ObSSTableColumnChecksumOperator {
public:
  ObSSTableColumnChecksumOperator() = default;
  virtual ~ObSSTableColumnChecksumOperator() = default;
  static int batch_report_checksum(
      const common::ObIArray<ObSSTableColumnChecksumItem>& items, common::ObMySQLProxy& proxy);
  static int batch_remove_checksum(
      const common::ObIArray<ObSSTableColumnChecksumItem>& items, common::ObMySQLProxy& proxy);
  static int get_checksum_method(const uint64_t data_table_id, const uint64_t index_id, const int64_t major_version,
      int64_t& checksum_method, common::ObMySQLProxy& sql_proxy);
  static int check_column_checksum(const share::schema::ObTableSchema& data_table_schema,
      const share::schema::ObTableSchema& index_table_schema, const int64_t global_snapshot_version,
      common::ObMySQLProxy& sql_proxy);

private:
  static int fill_one_item(const ObSSTableColumnChecksumItem& item, share::ObDMLSqlSplicer& dml);
  static int remove_one_item(const ObSSTableColumnChecksumItem& item, common::ObMySQLProxy& sql_proxy);
  static int get_checksum(const uint64_t tenant_id, const common::ObSqlString& sql,
      common::ObIArray<ObSSTableColumnChecksumItem>& items, common::ObMySQLProxy& sql_proxy);

private:
  static int get_table_column_checksum(const share::schema::ObTableSchema& data_table_schema,
      const share::schema::ObTableSchema& index_table_schema, const int64_t global_snapshot_version,
      common::ObMySQLProxy& mysql_proxy, int64_t& snapshot_version,
      common::hash::ObHashMap<int64_t, int64_t>& column_checksum_map);
};

struct ObSSTableChecksumItem {
public:
  ObSSTableChecksumItem() : data_checksum_(), column_checksum_()
  {}
  ~ObSSTableChecksumItem()
  {
    reset();
  }
  void reset();
  TO_STRING_KV(K_(data_checksum), K_(column_checksum));
  ObSSTableDataChecksumItem data_checksum_;
  common::ObArray<ObSSTableColumnChecksumItem> column_checksum_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_SSTABLE_CHECKSUM_OPERATOR_H_
