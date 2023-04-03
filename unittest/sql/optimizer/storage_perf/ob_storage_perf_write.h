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

#ifndef OB_TOOLS_STORAGE_PERF_WRITE_H_
#define OB_TOOLS_STORAGE_PERF_WRITE_H_

#include "share/schema/ob_table_schema.h"
#include "share/ob_tenant_mgr.h"
#include "storage/blocksstable/ob_sstable.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_tenant_mgr.h"
#include "lib/file/file_directory_utils.h"
#include "ob_storage_perf_config.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/ob_macro_block_marker.h"
#include "storage/ob_partition_meta.h"
#include "ob_storage_perf_schema.h"

namespace oceanbase
{

namespace storageperf
{

class ObStoragePerfWrite
{
public:
  ObStoragePerfWrite();
  virtual ~ObStoragePerfWrite();
  int init(ObStoragePerfConfig *config,
           const int64_t thread_no,
           ObRestoreSchema *restore_schema,
           share::schema::MockSchemaService *schema_service);
  int create_sstable(blocksstable::ObStorageCacheSuite &cache_suite, int64_t &used_macro_num);
  int close_sstable(void);
  void cleanup_sstable();

private:
  int init_data_file();
  int store_sstable_meta(const blocksstable::ObSSTable &sstable);

private:
  char data_file_path_[MAX_PATH_SIZE];
  char log_dir_path_[MAX_PATH_SIZE];
  char sstable_meta_path_[MAX_PATH_SIZE];

  blocksstable::ObMacroBlockMetaImage image_;
  blocksstable::ObBaseStorageLogger logger_;
  blocksstable::ObDataFile data_file_;
  blocksstable::ObMacroBlockMarker marker_;

  sql::ObRestoreSchema *restore_schema_;
  share::schema::MockSchemaService *schema_service_;
  ObStoragePerfConfig *config_;
  int64_t row_count_;
  int64_t thread_no_;
  bool is_inited_;
};

}//end namespace storageperf
}//end namespace oceanbase

#endif //OB_TOOLS_STORAGE_PERF_WRITE_H_
