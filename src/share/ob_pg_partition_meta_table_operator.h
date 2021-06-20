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

#ifndef OCEANBASE_OBSERVER_OB_PG_PARTITION_META_TABLE_UPDATER_OPERATOR_H_
#define OCEANBASE_OBSERVER_OB_PG_PARTITION_META_TABLE_UPDATER_OPERATOR_H_

#include "lib/net/ob_addr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_table_schema.h"
#include "share/partition_table/ob_partition_info.h"

namespace oceanbase {
namespace share {
struct ObPGPartitionMTUpdateItem {
public:
  ObPGPartitionMTUpdateItem()
  {
    reset();
  }
  ~ObPGPartitionMTUpdateItem()
  {
    reset();
  }
  bool is_key_valid() const;
  bool is_valid() const;
  void reset();
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t partition_id_;
  common::ObAddr svr_ip_;
  int role_;
  int64_t row_count_;
  int64_t data_size_;
  int64_t data_version_;
  int64_t required_size_;
  ObReplicaStatus status_;
  common::ObReplicaType replica_type_;
  int data_checksum_;

  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(partition_id), K_(svr_ip), K_(role), K_(row_count), K_(data_size),
      K_(data_version), K_(required_size), K_(status), K_(replica_type), K_(data_checksum));
};

class ObPGPartitionMTUpdateOperator {
public:
  ObPGPartitionMTUpdateOperator() = default;
  virtual ~ObPGPartitionMTUpdateOperator() = default;
  static int batch_update(const common::ObIArray<ObPGPartitionMTUpdateItem>& items, common::ObMySQLProxy& proxy);
  static int batch_delete(const common::ObIArray<ObPGPartitionMTUpdateItem>& items, common::ObMySQLProxy& proxy);

  static int get_checksum(ObPGPartitionMTUpdateItem& item, common::ObMySQLProxy& proxy);
  static int get_checksum(const uint64_t data_table_id, const uint64_t sstable_id, const int64_t partition_id,
      const int sstable_type, common::ObIArray<ObPGPartitionMTUpdateItem>& items, common::ObMySQLProxy& sql_proxy);
  static int get_replicas(const uint64_t data_table_id, const uint64_t sstable_id, const int64_t partition_id,
      const int sstable_type, common::ObIArray<common::ObAddr>& replicas, common::ObMySQLProxy& sql_proxy);
  static int check_table_checksum(
      const uint64_t data_table_id, const uint64_t index_id, common::ObMySQLProxy& sql_proxy, bool& is_checksum_valid);

private:
  static int fill_one_item_(const ObPGPartitionMTUpdateItem& item, share::ObDMLSqlSplicer& dml);
  static int remove_one_item_(const ObPGPartitionMTUpdateItem& item, common::ObMySQLProxy& sql_proxy);
  static int get_checksum(const common::ObSqlString& sql, common::ObIArray<ObPGPartitionMTUpdateItem>& items,
      common::ObMySQLProxy& sql_proxy);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_PG_PARTITION_META_TABLE_UPDATER_OPERATOR_H_
