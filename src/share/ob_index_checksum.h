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

#ifndef OCEANBASE_SHARE_OB_INDEX_CHECKSUM_H_
#define OCEANBASE_SHARE_OB_INDEX_CHECKSUM_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase {
namespace share {

struct ObIndexChecksumItem {
  ObIndexChecksumItem()
      : execution_id_(common::OB_INVALID_ID),
        tenant_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        partition_id_(common::OB_INVALID_ID),
        column_id_(common::OB_INVALID_ID),
        task_id_(common::OB_INVALID_ID),
        checksum_(0),
        checksum_method_(0)
  {}
  ~ObIndexChecksumItem(){};
  bool is_valid() const
  {
    return common::OB_INVALID_ID != execution_id_ && common::OB_INVALID_ID != tenant_id_ &&
           common::OB_INVALID_ID != table_id_ && common::OB_INVALID_ID != partition_id_ &&
           common::OB_INVALID_ID != column_id_ && 0 != checksum_method_;
  }
  TO_STRING_KV(K_(execution_id), K_(tenant_id), K_(table_id), K_(partition_id), K_(column_id), K_(task_id),
      K_(checksum), K_(checksum_method));
  uint64_t execution_id_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t column_id_;
  uint64_t task_id_;
  int64_t checksum_;
  int64_t checksum_method_;
};

class ObIndexChecksumOperator {
public:
  static int update_checksum(
      const common::ObIArray<ObIndexChecksumItem>& checksum_items, common::ObMySQLProxy& sql_proxy);
  static int get_partition_column_checksum(const uint64_t execution_id, const uint64_t table_id,
      const int64_t partition_id, common::hash::ObHashMap<int64_t, int64_t>& column_checksums,
      common::ObMySQLProxy& sql_proxy);
  static int get_table_column_checksum(const uint64_t execution_id, const uint64_t table_id,
      common::hash::ObHashMap<int64_t, int64_t>& column_checksums, common::ObMySQLProxy& sql_proxy);
  static int check_column_checksum(const uint64_t execution_id, const uint64_t data_table_id,
      const uint64_t index_table_id, bool& is_equal, common::ObMySQLProxy& sql_proxy);
  static int get_checksum_method(
      const uint64_t execution_id, const uint64_t table_id, int64_t& checksum_method, common::ObMySQLProxy& sql_proxy);

private:
  static int fill_one_item(const ObIndexChecksumItem& item, share::ObDMLSqlSplicer& dml);
  static int get_column_checksum(const common::ObSqlString& sql,
      common::hash::ObHashMap<int64_t, int64_t>& column_checksum_map, common::ObMySQLProxy& sql_proxy);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_INDEX_CHECKSUM_H_
