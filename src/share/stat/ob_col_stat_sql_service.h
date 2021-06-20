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

#ifndef _OB_COL_STAT_SQL_READER_H_
#define _OB_COL_STAT_SQL_READER_H_

#include "lib/allocator/ob_allocator.h"
#include "share/stat/ob_column_stat.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase {
namespace share {
class ObDMLSqlSplicer;
}
namespace common {
struct ObPartitionKey;
class ObServerConfig;
class ObTableStat;

struct ObStatConverterInfo {
  ObStatConverterInfo()
      : output_column_with_tenant_ids_(),
        column_ids_(),
        is_vt_mapping_(false),
        use_real_tenant_id_(false),
        real_table_id_(common::OB_INVALID_ID),
        tenant_id_col_id_(UINT64_MAX),
        vt_result_converter_(),
        schema_guard_()
  {}

  ObArray<bool> output_column_with_tenant_ids_;
  ObArray<uint64_t> column_ids_;
  ObArray<uint64_t> real_column_ids_;
  ObArray<const share::schema::ObColumnSchemaV2*> col_schemas_;
  bool is_vt_mapping_;
  bool use_real_tenant_id_;
  uint64_t real_table_id_;
  uint64_t tenant_id_col_id_;
  sql::ObVirtualTableResultConverter vt_result_converter_;
  share::schema::ObSchemaGetterGuard schema_guard_;
};

class ObTableColStatSqlService {
  const static int64_t STATISTIC_WEAK_READ_TIMEOUT = 500000;

public:
  ObTableColStatSqlService();
  ~ObTableColStatSqlService();

  int init(common::ObMySQLProxy* proxy, common::ObServerConfig* config);
  int fetch_column_stat(const ObColumnStat::Key& key, ObColumnStat& stat);
  int fetch_batch_stat(const uint64_t table_id, const ObIArray<uint64_t>& partition_id,
      const ObIArray<uint64_t>& column_id, ObIAllocator& allocator, common::ObIArray<ObColumnStat*>& stats,
      const bool use_pure_id = true);
  int fetch_table_stat(
      const common::ObPartitionKey& key, ObIArray<std::pair<ObPartitionKey, ObTableStat>>& all_part_stats);
  /**
   * item in stats must not NULL, or will return OB_ERR_UNEXPECTED
   */
  int insert_or_update_column_stats(const ObIArray<ObColumnStat*>& stats);
  int insert_or_update_column_stat(const ObColumnStat& stat);

  int fill_column_stat(const uint64_t table_id, common::sqlclient::ObMySQLResult& result, ObColumnStat& stat,
      int64_t idx, ObStatConverterInfo& stat_converter_info);
  int fill_table_stat(common::sqlclient::ObMySQLResult& result, int64_t& partition_id, ObTableStat& stat);

private:
  // serialize data to hex-str. append \0 if return OB_SUCCESS
  //@pos, buf pos used.
  //@cstr_pos begin of hex-str.
  int serialize_to_hex_cstr(const common::ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, int64_t& cstr_pos);
  int deserialize_hex_cstr(char* buf, int64_t buf_len, common::ObObj& obj);
  int deserialize_hex_cstr(char* buf, int64_t buf_len, ObIAllocator& allocator, common::ObObj& obj);
  int get_compressed_llc_bitmap(
      ObIAllocator& allocator, const char* bitmap_buf, int64_t bitmap_size, char*& comp_buf, int64_t& comp_size);
  int get_decompressed_llc_bitmap(
      ObIAllocator& allocator, const char* comp_buf, int64_t comp_size, char*& bitmap_buf, int64_t& bitmap_size);
  int fill_dml_splicer(const ObColumnStat& stat, share::ObDMLSqlSplicer& dml_splicer);

  int init_vt_converter(uint64_t tenant_id, uint64_t ref_table_id, ObStatConverterInfo& stat_converter_info);
  int init_vt_converter(
      uint64_t tenant_id, uint64_t ref_table_id, uint64_t column_id, ObStatConverterInfo& stat_converter_info);
  int init_vt_converter(uint64_t tenant_id, uint64_t ref_table_id, const ObIArray<uint64_t>& column_ids,
      ObStatConverterInfo& stat_converter_info);
  int get_mapping_columns_info(const share::schema::ObTableSchema* vt_table_schema,
      const share::schema::ObTableSchema* real_table_schema, const ObIArray<uint64_t>& column_ids,
      ObStatConverterInfo& stat_converter_info);

private:
  static const char* bitmap_compress_lib_name;

private:
  bool inited_;
  common::ObMySQLProxy* mysql_proxy_;
  lib::ObMutex mutex_;
  common::ObServerConfig* config_;
};  // end of class ObColumnStat

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OB_COL_STAT_SQL_READER_H_ */
