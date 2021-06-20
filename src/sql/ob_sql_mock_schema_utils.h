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

#ifndef OB_SQL_MOCK_SCHEMA_UTILS_H
#define OB_SQL_MOCK_SCHEMA_UTILS_H

#include "share/schema/ob_schema_getter_guard.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace share {}
namespace sql {
class ObSQLMockSchemaUtils {
public:
  static int mock_rowid_index(
      const share::schema::ObTableSchema* base_table_schema, const share::schema::ObTableSchema*& rowid_index_schema);
  static int mock_rowid_column(share::schema::ObTableSchema& table_schema);

  static uint64_t get_rowid_index_table_id(const uint64_t table_id);

  static uint64_t get_baseid_from_rowid_index_id(const uint64_t mock_index_id);

  static void pop_mock_table();

  static const common::ObIArray<uint64_t>& get_all_mocked_tables();

  static int add_mock_table(const uint64_t table_id);

  static bool is_mock_table(const uint64_t table_id);

  static bool is_mock_index(const uint64_t index_id);

  static bool is_mock_index(const uint64_t index_id, uint64_t& base_table_id);

  static bool contain_mock_index(const uint64_t table_id);

  static int reset_mock_table();

  static int prepare_mocked_schemas(const common::ObIArray<uint64_t>& mock_rowid_tables);

  static const char* get_rowid_index_name();

private:
  static int add_mock_index(const uint64_t table_id);
};

class ObSQLMockedTables {
private:
  // hit 25 bits
  // normal table id, high 24 bits is tenant_id, then 1 bit to determine this is table group id
  // so here we use 25 bits to specify this is a mocked rowid index table id
  const static uint64_t ROWID_INDEX_HIGH_BITS = 0xFEFDFC8;

public:
  const static uint64_t ROWID_INDEX_MASK = (ROWID_INDEX_HIGH_BITS << 39UL);

public:
  struct ObMockedRowIDIndexInfo {
    uint64_t org_table_id_;
    uint64_t mocked_table_id_;

    ObMockedRowIDIndexInfo(const uint64_t org_table_id, const uint64_t mocked_table_id)
        : org_table_id_(org_table_id), mocked_table_id_(mocked_table_id)
    {}

    ObMockedRowIDIndexInfo() : org_table_id_(OB_INVALID_ID), mocked_table_id_(OB_INVALID_ID)
    {}

    bool operator==(const ObMockedRowIDIndexInfo& other) const
    {
      return other.org_table_id_ == org_table_id_ && other.mocked_table_id_ == mocked_table_id_;
    }

    bool operator!=(const ObMockedRowIDIndexInfo& other) const
    {
      return !(other == *this);
    }

    TO_STRING_KV(K_(org_table_id), K_(mocked_table_id));
  };
  typedef common::ObSEArray<uint64_t, 8> MockTableIDArray;
  typedef common::ObSEArray<ObMockedRowIDIndexInfo, 8> MockRowIDIdxArray;
  const static int64_t MOCKED_TABLE_IDENTIFIER = 0;

public:
  ObSQLMockedTables() : table_ids_()
  {}
  virtual ~ObSQLMockedTables()
  {
    reset();
  }

  void reset()
  {
    table_ids_.reset();
    rowid_idx_ids_.reset();
  }

  int add_table(const uint64_t table_id)
  {
    return add_var_to_array_no_dup(table_ids_, table_id);
  }

  int add_index(const ObMockedRowIDIndexInfo& index_id)
  {
    return add_var_to_array_no_dup(rowid_idx_ids_, index_id);
  }

  void pop_mock_table_and_index()
  {
    table_ids_.pop_back();
    rowid_idx_ids_.pop_back();
  }

  const common::ObIArray<uint64_t>& get_table_ids() const
  {
    return table_ids_;
  }

  const common::ObIArray<ObMockedRowIDIndexInfo>& get_index_ids() const
  {
    return rowid_idx_ids_;
  }

private:
  MockTableIDArray table_ids_;
  MockRowIDIdxArray rowid_idx_ids_;
};

class ObSQLMockSchemaGuard {
public:
  ObSQLMockSchemaGuard();
  ~ObSQLMockSchemaGuard();
};
}  // end namespace sql
}  // end namespace oceanbase
#endif  // !OB_SQL_MOCK_SCHEMA_UTILS_H