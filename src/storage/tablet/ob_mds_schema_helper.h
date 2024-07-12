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

#ifndef OCEANBASE_STORAGE_OB_MDS_SCHEMA_HELPER
#define OCEANBASE_STORAGE_OB_MDS_SCHEMA_HELPER

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "storage/ob_i_store.h"
#include "storage/ob_storage_schema.h"
#include "storage/access/ob_table_read_info.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
}
}

namespace common
{
class ObObjMeta;
class ObIAllocator;
}

namespace storage
{
class ObMdsSchemaHelper
{
private:
  ObMdsSchemaHelper();
public:
  ~ObMdsSchemaHelper();
  ObMdsSchemaHelper(const ObMdsSchemaHelper&) = delete;
  ObMdsSchemaHelper &operator=(const ObMdsSchemaHelper&) = delete;
public:
  static ObMdsSchemaHelper &get_instance();
  int init();
  void reset();
  const share::schema::ObTableSchema *get_table_schema() const;
  const ObStorageSchema *get_storage_schema() const;
  const ObRowkeyReadInfo *get_rowkey_read_info() const;
private:
  static int build_table_schema(
      const uint64_t tenant_id,
      const int64_t database_id,
      const uint64_t table_id,
      const char *table_name,
      share::schema::ObTableSchema &table_schema);
  static int build_rowkey_read_info(
      common::ObIAllocator &allocator,
      const ObStorageSchema &storage_schema,
      ObRowkeyReadInfo &rowkey_read_info);
  static int build_column_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t column_id,
      const char *column_name,
      const int64_t schema_version,
      const int64_t rowkey_position,
      const common::ObOrderType &order_in_rowkey,
      const common::ObObjMeta &meta_type,
      const int64_t data_length,
      share::schema::ObColumnSchemaV2 &column_schema);
public:
  static constexpr int64_t MDS_ROW_COLUMN_CNT = 4;
  static constexpr int64_t MDS_MULTI_VERSION_ROW_COLUMN_CNT = MDS_ROW_COLUMN_CNT + ObMultiVersionExtraRowkeyIds::MAX_EXTRA_ROWKEY;
  static constexpr uint64_t MDS_TYPE_IDX = 0;
  static constexpr uint64_t UDF_KEY_IDX = 1;
  static constexpr uint64_t SNAPSHOT_IDX = 2;
  static constexpr uint64_t SEQ_NO_IDX = 3;
  static constexpr uint64_t META_INFO_IDX = 4;
  static constexpr uint64_t USER_DATA_IDX = 5;
  static constexpr int64_t MDS_SCHEMA_VERSION = 9527; // for mds schema change
  static constexpr uint64_t MDS_TABLE_ID = 88888; // TODO(@bowen.gbw): choose another value?
private:
  static constexpr const char *MDS_TABLE_NAME = "mds_table";
  static constexpr const char *MDS_TYPE_COLUMN_NAME = "mds_type";
  static constexpr const char *UDF_KEY_COLUMN_NAME = "udf_key";
  static constexpr const char *META_INFO_COLUMN_NAME = "meta_info";
  static constexpr const char *USER_DATA_COLUMN_NAME = "user_data";
  static constexpr uint64_t MDS_TYPE_DATA_LENGTH = 1;
  static constexpr uint64_t UDF_KEY_DATA_LENGTH = 1024;
  static constexpr uint64_t META_INFO_DATA_LENGTH = 1024;
  static constexpr uint64_t USER_DATA_DATA_LENGTH = common::OB_MAX_VARCHAR_LENGTH;
  static constexpr int64_t DATABASE_ID = 1;
  static constexpr int64_t ROWKEY_COLUMN_NUM = 2;
  static constexpr int64_t COLUMN_SCHEMA_VERSION = 1;
  static constexpr uint64_t MDS_TYPE_COLUMN_ID = common::OB_APP_MIN_COLUMN_ID;
  static constexpr uint64_t UDF_KEY_COLUMN_ID = common::OB_APP_MIN_COLUMN_ID + 1;
  static constexpr uint64_t META_INFO_COLUMN_ID = common::OB_APP_MIN_COLUMN_ID + 2;
  static constexpr uint64_t USER_DATA_COLUMN_ID = common::OB_APP_MIN_COLUMN_ID + 3;
private:
  bool is_inited_;
  ObArenaAllocator allocator_;
  share::schema::ObTableSchema table_schema_;
  ObStorageSchema storage_schema_;
  ObRowkeyReadInfo rowkey_read_info_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_SCHEMA_HELPER
