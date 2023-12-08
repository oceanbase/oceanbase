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
 *
 * OBCDC HBase Util
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_HBASE_MODE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_HBASE_MODE_H_

#include "lib/hash/ob_hashset.h"                // ObHashSet
#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap

namespace oceanbase
{
namespace datadict
{
class ObDictTableMeta;
}
namespace share
{
namespace schema
{
class ObTableSchema;
} // namespace schema
} // namespace share

namespace libobcdc
{

class ObLogHbaseUtil
{
public:
  ObLogHbaseUtil();
  virtual ~ObLogHbaseUtil();

public:
  // Determine if the table is an hbase model,
  // if yes, join; otherwise do nothing
  //
  // Determine if the table is an hbase model:
  // 1. table_name contains $
  // 2. contains four columns K, Q, T, V
  // 3. T is of type bigint
  // Note: All of the above conditions are not necessarily met for an hbase table
  template<class TABLE_SCHEMA>
  int add_hbase_table_id(const TABLE_SCHEMA &table_schema)
  {
    int ret = OB_SUCCESS;

    bool is_hbase_mode_table = false;
    const uint64_t table_id = table_schema.get_table_id();
    const char *table_name = table_schema.get_table_name();

    if (OB_FAIL(filter_hbase_mode_table_(table_schema, is_hbase_mode_table))) {
      OBLOG_LOG(ERROR, "filter_hbase_mode_table_ fail", KR(ret), K(table_id), K(table_name), K(is_hbase_mode_table));
    } else if (! is_hbase_mode_table) {
      OBLOG_LOG(INFO, "[IS_NOT_HBASE_TABLE]", K(table_name), K(table_id), K(is_hbase_mode_table));
    } else if (OB_FAIL(table_id_set_.set_refactored(table_id))) {
      OBLOG_LOG(ERROR, "add_table_id into table_id_set_ fail", KR(ret), K(table_name), K(table_id));
    } else {
      OBLOG_LOG(INFO, "[HBASE] add_table_id into table_id_set_ succ", K(table_name), K(table_id));
    }

    return ret;
  }

  // Determine if conversion is required
  // table exists and is a T column
  int judge_hbase_T_column(const uint64_t table_id,
      const uint64_t column_id,
      bool &chosen);

  int is_hbase_table(const uint64_t table_id,
      bool &chosen);

public:
  int init();
  void destroy();

private:
  static const int64_t HBASE_TABLE_COLUMN_COUNT = 4;
  const char *K_COLUMN = "K";
  const char *Q_COLUMN = "Q";
  const char *T_COLUMN = "T";
  const char *V_COLUMN = "V";

  static const int64_t DEFAULT_TABLE_SET_SIZE = 1024;
  typedef common::hash::ObHashSet<uint64_t> HbaseTableIDSet;

  struct TableID
  {
    uint64_t table_id_;

    TableID(const uint64_t table_id) :
      table_id_(table_id)
    {}

    int64_t hash() const
    {
      return static_cast<int64_t>(table_id_);
    }

    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }

    bool operator== (const TableID &other) const
    {
      return table_id_ == other.table_id_;
    }

    void reset()
    {
      table_id_ = common::OB_INVALID_ID;
    }

    TO_STRING_KV(K_(table_id));
  };

  typedef common::ObLinearHashMap<TableID, uint64_t/*column id*/> ColumnIDMap;

private:
  int filter_hbase_mode_table_(const oceanbase::share::schema::ObTableSchema &table_schema,
      bool &is_hbase_mode_table);
  int filter_hbase_mode_table_(const oceanbase::datadict::ObDictTableMeta &table_meta,
      bool &is_hbase_mode_table);
  template <class COLUMN_SCHEMA>
  int match_column_name_(const COLUMN_SCHEMA &col_schema,
      const int column_flag_size,
      int *column_flag,
      bool &is_T_column_bigint_type,
      uint64_t &column_id);

  template<class TABLE_SCHEMA>
  int judge_and_add_hbase_table_(const TABLE_SCHEMA &table_schema,
      const bool is_T_column_bigint_type,
      const uint64_t column_id,
      const int column_flag_size,
      const int *column_flag,
      bool &is_hbase_mode_table);

private:
  bool            inited_;

  HbaseTableIDSet table_id_set_;
  ColumnIDMap     column_id_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogHbaseUtil);
};

}
}
#endif
