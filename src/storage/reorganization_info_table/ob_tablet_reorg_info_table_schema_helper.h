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

#ifndef OCEANBASE_STORAGE_OB_TABLET_REORGANIZATION_INFO_TABLE_SCHEMA_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_REORGANIZATION_INFO_TABLE_SCHEMA_HELPER

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
/**
 * @brief Using for Member Table in sstable store
 *
 * The sstables it iterates produce are looked like the
 * following table:
 *
 * │  Column 0  │  Column 1         │  Column 1    │        Hidden Column       │       Column 2                   │
 * ├────────────┼───────────────────│──────────────│──────────────┬─────────────┼──────────────────────────────────┤
 * │    Key     │    Key            │     Key      │ TransVersion │ SqlSequence │              Value               │
 * ├────────────┼───────────────────│───────────── │──────────────┼─────────────┼──────────────────────────────────|
 * │ tablet_id_1│reorganization_scn1│   transfer   |     -1       │     0       │     serialization of data1       │
 * │ tablet_id_2│reorganization_scn2│    split     |     -1       │     0       │     serialization of data2       │
 * │ tablet_id_3│reorganization_scn3│   transfer   |     -1       │     0       │     serialization of data3       │
 * │    ...     │   ...             │    ...       │    ....      │     0       │                ...               │
 * │ INT64_MAX  │                   │              │     -1       │     0       | serialization of pre-process data│
 * └────────────┴───────────────────┴──────────────┴──────────────┴─────────────┴──────────────────────────────────┴
 *
 *
 */

struct ObTabletReorgInfoTableSchemaDef final
{
public:
  ObTabletReorgInfoTableSchemaDef();
  ~ObTabletReorgInfoTableSchemaDef();

  enum class REORG_INFO_TABLE_SCHEMA_COL_IDX : int64_t
  {
    TABLET_ID = 0,
    REORGANIZATION_SCN = 1,
    DATA_TYPE = 2,
    DATA_VALUE = 3,
    MAX_SCHEMA_COL_IDX,
  };
  enum class REORG_INFO_SSTABLE_COL_IDX : int64_t
  {
    TABLET_ID = 0,
    REORGANIZATION_SCN = 1,
    DATA_TYPE = 2,
    TRANS_VERSION = 3,
    SQL_SEQ = 4,
    DATA_VALUE = 5,
    MAX_COLUMN_IDX,
  };
  static int get_table_column_info(
      const int64_t column_idx,
      int64_t &column_id,
      const char *&column_name,
      int64_t &column_length,
      ObObjMeta &meta,
      int64_t &rowkey_position);
public:
  static constexpr const char *REORG_INFO_TABLE_NAME = "reorg_info_table";
  static constexpr int64_t DATABASE_ID = 1;
  static constexpr int64_t ROWKEY_COLUMN_NUM = 3;
  static constexpr int64_t COLUMN_SCHEMA_VERSION = 1;
  static constexpr int64_t REORG_INFO_ROW_COLUMN_CNT = 4;
  static constexpr int64_t REORG_INFO_MULTI_VERSION_ROW_COLUMN_CNT = REORG_INFO_ROW_COLUMN_CNT
      + ObMultiVersionExtraRowkeyIds::MAX_EXTRA_ROWKEY;
  static constexpr int64_t REORG_INFO_SCHEMA_VERSION = 1;
  static constexpr uint64_t REORG_INFO_TABLE_ID = 90000;
  static constexpr int64_t TRANS_VERSION_COLUMN_ID = 3;
  static constexpr uint64_t DATA_VALUE_COLUMN_LENGTH = 1024; //1k
private:
  static constexpr const uint64_t TABLE_COLUMN_ID[] = {
      OB_APP_MIN_COLUMN_ID,
      OB_APP_MIN_COLUMN_ID + 1,
      OB_APP_MIN_COLUMN_ID + 2,
      OB_APP_MIN_COLUMN_ID + 3};
  static constexpr const char *TABLE_COLUMN_NAMES[] = {"tablet_id", "reorgnization_scn", "data_type", "data_value"};
  static constexpr const uint64_t COLUMN_DATA_LENGTH[] = {16, 16, 16, DATA_VALUE_COLUMN_LENGTH};
};

class ObTabletReorgInfoTableSchemaHelper
{
public:
  static ObTabletReorgInfoTableSchemaHelper &get_instance();
  ObTabletReorgInfoTableSchemaHelper();
  ~ObTabletReorgInfoTableSchemaHelper();
  ObTabletReorgInfoTableSchemaHelper(const ObTabletReorgInfoTableSchemaHelper&) = delete;
  ObTabletReorgInfoTableSchemaHelper &operator=(const ObTabletReorgInfoTableSchemaHelper&) = delete;

  int init();
  void reset();
  const share::schema::ObTableSchema *get_table_schema() const;
  const ObStorageSchema *get_storage_schema() const;
  const ObRowkeyReadInfo *get_rowkey_read_info() const;
  int get_column_ids(
      const bool is_query,
      common::ObIArray<uint64_t> &column_ids);

private:
  int build_table_schema_(
      const uint64_t tenant_id,
      const int64_t database_id,
      const uint64_t table_id,
      const char *table_name,
      share::schema::ObTableSchema &table_schema);
  int build_rowkey_read_info_(
      common::ObIAllocator &allocator,
      const ObStorageSchema &storage_schema,
      ObRowkeyReadInfo &rowkey_read_info);
  int build_column_schema_(
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
  int build_column_ids_(
      const share::schema::ObTableSchema &table_schema);
private:
  bool is_inited_;
  ObArenaAllocator allocator_;
  share::schema::ObTableSchema table_schema_;
  ObStorageSchema storage_schema_;
  ObRowkeyReadInfo rowkey_read_info_;
  common::ObSEArray<uint64_t, static_cast<int64_t>(ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX::MAX_COLUMN_IDX)> column_ids_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MEMBER_TABLE_SCHEMA_HELPER
