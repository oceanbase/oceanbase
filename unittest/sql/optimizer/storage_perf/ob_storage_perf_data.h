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

#ifndef OCEANBASE_UNITEST_BLOCKSSTABLE_OB_ROW_GENERATE_
#define OCEANBASE_UNITEST_BLOCKSSTABLE_OB_ROW_GENERATE_

#include "ob_storage_perf_schema.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_i_store.h"
#include "storage/ob_ss_store.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ob_partition_storage.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/mockcontainer/ob_restore_schema.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_malloc.h"
#include "share/ob_errno.h"
#include "common/object/ob_object.h"
#include "common/object/ob_obj_type.h"
#include "ob_storage_perf_config.h"

#ifndef INT24_MIN
#define INT24_MIN     (-8388607 - 1)
#endif
#ifndef INT24_MAX
#define INT24_MAX     (8388607)
#endif
#ifndef UINT24_MAX
#define UINT24_MAX    (16777215U)
#endif

namespace oceanbase
{

namespace storageperf
{

class ObStoragePerfData
{
public:
  ObStoragePerfData();
  ~ObStoragePerfData();
  int init(ObStoragePerfConfig *config,
           share::schema::MockSchemaService *schema_service,
           blocksstable::ObStorageCacheSuite *cache_suite,
           sql::ObRestoreSchema *restore_schema);
  storage::ObPartitionStorage *get_partition_storage() {return &storage_;}

  void set_update_schema(const char *uschema) {
    update_schema_ = uschema;
  }

private:
  int init_data_file();
  int init_partition_storage();
  int backup_sstable_meta(blocksstable::ObSSTable &sstable);
  int update_to_memtable();

private:
  char data_file_path_[common::OB_MAX_FILE_NAME_LENGTH];
  char log_dir_path_[common::OB_MAX_FILE_NAME_LENGTH];
  char sstable_meta_path_[common::OB_MAX_FILE_NAME_LENGTH];

  storage::ObSSStore ssstore_;
  memtable::ObMemtable memtable_;
  blocksstable::ObSSTable *sstable_;
  blocksstable::ObPartitionMeta meta_;
  storage::ObPartitionStorage storage_;
  blocksstable::ObDataFile data_file_;
  blocksstable::ObMacroBlockMetaImage image_;
  blocksstable::ObBaseStorageLogger logger_;
  blocksstable::ObStorageCacheSuite *cache_suite_;
  share::schema::MockSchemaService *schema_service_;
  sql::ObRestoreSchema *restore_schema_;
  ObStoragePerfConfig *config_;
  bool is_inited_;

  const char *update_schema_;
};

class ObRowGenerate
{
public:
  ObRowGenerate();
  ~ObRowGenerate();
  void reuse();
  int init(const share::schema::ObTableSchema &src_schema);
  int init(const share::schema::ObTableSchema &src_schema, common::ObArenaAllocator *allocator);
  int get_next_row(const storage::ObStoreRow *&row);
  int get_next_row(const int64_t seed, const storage::ObStoreRow *&row, bool old_value = true);
  int get_rowkey_by_seed(const int64_t seed, common::ObRowkey &rowkey);
  int check_one_row(const storage::ObStoreRow &row, bool &exist);
  void reset() { seed_ = 0; column_list_.reset(); }

private:
  int generate_one_row(storage::ObStoreRow& row, int64_t seed, bool old_value = true);
  int set_obj(const common::ObObjType &column_type, const uint64_t column_id, const int64_t seed, common::ObObj &obj, bool old_value = true);
  int compare_obj(const common::ObObjType &column_type, const int64_t value, const common::ObObj obj, bool &exist);
  int get_seed(const common::ObObjType &column_type, const common::ObObj obj, int64_t &seed);

private:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator *p_allocator_;
  share::schema::ObTableSchema schema_;
  storage::ObColDescArray column_list_;
  int64_t seed_;
  bool is_inited_;
  bool is_reused_;
};

}//storage
}//oceanbase

#endif //OCEANBASE_UNITEST_BLOCKSSTABLE_OB_ROW_GENERATE_
