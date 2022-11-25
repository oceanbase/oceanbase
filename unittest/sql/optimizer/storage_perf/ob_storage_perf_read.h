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

#ifndef OB_TOOLS_STORAGE_PERF_READ_H_
#define OB_TOOLS_STORAGE_PERF_READ_H_

#include "ob_storage_perf_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_tenant_mgr.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ob_ss_store.h"
#include "share/ob_tenant_mgr.h"
#include "lib/file/file_directory_utils.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/ob_macro_block_marker.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/ob_partition_meta.h"
#include "storage/ob_partition_component_factory.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_base_storage.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/allocator/page_arena.h"
#include "storage/memtable/ob_memtable.h"
#include "ob_storage_perf_data.h"
#include "ob_storage_perf_config.h"
#include <pthread.h>
#include <cstdio>


namespace oceanbase
{

namespace storageperf
{

class FakePartitionMeta : public blocksstable::ObBaseStorageMeta
{
public:
  FakePartitionMeta() {}
  virtual ~FakePartitionMeta() {}
  int initialize(blocksstable::ObDataFile *data_file,
                 blocksstable::ObBaseStorageLogger *logger);
  void destroy();
  int add_macro_block(const int64_t block_index);
  int read_check_point();
  int do_check_point();
  int replay(const int64_t log_seq_num, const int64_t subcmd,
             const char *buf,const int64_t len);
  int parse(const int64_t subcmd, const char *buf,
     const int64_t len, FILE *stream);
  int get_entry_block(int64_t &entry_block) const;
  int mark_macro_block(blocksstable::ObMacroBlockMarkerHelper &helper) const;
private:
  static const int64_t DEFAULT_PARTITION_META_SIZE = 256 * 1024LL;
  DISALLOW_COPY_AND_ASSIGN(FakePartitionMeta);
  ObArray<int64_t> blocks_;
};

struct ObStoragePerfWaitEvent
{
  uint64_t total_waits_;
  uint64_t total_timeouts_;
  uint64_t time_waited_micro_;
  uint64_t max_wait_;
  double average_wait_;
  char name_[MAX_PATH_SIZE];

  ObStoragePerfWaitEvent(const char *name);
  ObStoragePerfWaitEvent operator - (const ObStoragePerfWaitEvent &event);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
};
//TOTO:distinguish index micro read or normal micro read
struct ObStoragePerfStatistics
{
  uint64_t index_cache_hit_;
  uint64_t index_cache_miss_;
  uint64_t row_cache_hit_;
  uint64_t row_cache_miss_;
  uint64_t block_cache_hit_;
  uint64_t block_cache_miss_;
  uint64_t bf_cache_hit_;
  uint64_t bf_cache_miss_;
  uint64_t io_read_count_;
  uint64_t io_read_size_;
  uint64_t io_read_delay_;
  uint64_t io_read_queue_delay_;
  uint64_t io_read_cb_alloc_delay_;
  uint64_t io_read_cb_process_delay_;
  uint64_t io_read_prefetch_micro_cnt_;
  uint64_t io_read_prefetch_micro_size_;
  uint64_t io_read_uncomp_micro_cnt_;
  uint64_t io_read_uncomp_micro_size_;


  ObStoragePerfWaitEvent db_file_data_read_;
  ObStoragePerfWaitEvent db_file_data_index_read_;
  ObStoragePerfWaitEvent kv_cache_bucket_lock_wait_;
  ObStoragePerfWaitEvent io_queue_lock_wait_;
  ObStoragePerfWaitEvent io_controller_cond_wait_;
  ObStoragePerfWaitEvent io_processor_cond_wait_;


  ObStoragePerfStatistics();
  ObStoragePerfStatistics operator - (const ObStoragePerfStatistics &statics);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
};

struct ObSingleRowSpeed
{
  uint64_t sum_duation_;
  uint64_t count_;

  ObSingleRowSpeed();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t get_avg();
};

class ObUpdateRowIter : public ObNewRowIterator
{
public:
  ObUpdateRowIter();
  void reset();
  int init(ObRowGenerate *generate);
  int get_next_row(ObNewRow *&row);
private:
  int64_t seed_;
  bool is_inited_;
  ObRowGenerate *row_generate_;
  ObNewRow row_;
  ObArenaAllocator allocator_;
};

class ObStoragePerfRead
{
public:
  ObStoragePerfRead();
  virtual ~ObStoragePerfRead();
  int init(ObStoragePerfConfig *config,
           const int64_t thread_no,
           blocksstable::ObStorageCacheSuite *cache_suite_,
           ObRestoreSchema *restore_schema,
           MockSchemaService *schema_service,
           storage::ObPartitionStorage *storage,
           pthread_barrier_t *barrier);
  int flush_cache_or_not();
  int single_get_speed();
  int update_to_memtable();
  int multi_get_speed();
  int scan_speed();

  void print_obj(const ObObj &obj) {
    if (obj.get_type_class() == ObIntTC) {
      std::cout << obj.get_int() <<" | ";
    } else if (obj.get_type_class() == ObStringTC) {
      int64_t len = obj.get_string().length();
      char buf[len + 1];
      memset(buf, 0, len + 1);
      obj.get_string().to_string(buf, len + 1);
      std::cout << buf <<" | ";
    } else if (obj.get_type_class() == ObNumberTC) {
      std::cout << obj.get_number().format() <<" | ";
    } else if (obj.get_type_class() == ObDoubleTC) {
      std::cout << obj.get_double() <<" | ";
    } else if (obj.get_type_class() == ObUIntTC) {
      std::cout << obj.get_uint64() <<" | ";
    } else {
      std::cout << obj.get_int() <<" | ";
    }
  }

  void print_row(ObNewRow *row, int64_t &count) {
    std::cout << "Result Row "<< count <<" : " << "| ";
    for (int64_t i = 0; i < row->get_count(); i++) {
      print_obj(row->get_cell(i));
    }
    std::cout << std::endl;
    ++count;
  }

  int assign_read_cols(const ObIArray<int64_t> &read_cols) {
    return read_cols_.assign(read_cols);
  }
private:
  enum QueryType {
    SINGLE_GET = 0,
    MULTI_GET = 1,
    SCAN = 2,
  };
private:
  int init_data_file();
  int init_partition_storage();
  int backup_sstable_meta(blocksstable::ObSSTable &sstable);
  int set_trans_desc(oceanbase::transaction::ObTransDesc &trans_desc);
  int destroy(void);
  void set_statistics(ObStoragePerfStatistics &statics);
  int set_global_stat(ObStoragePerfStatistics &statics);
  bool is_row_cache_hit(const ObStoragePerfStatistics &statics, const QueryType type);
  bool is_block_cache_hit(const ObStoragePerfStatistics &statics, const QueryType type);
  bool is_read_one_micro_block(const ObStoragePerfStatistics &statics, const QueryType type);
  bool is_read_two_micro_block(const ObStoragePerfStatistics &statics, const QueryType type);
  int get_ret() { return ret_; }
  void set_ret(int ret) { ret_ = ret; }

private:
  char data_file_path_[MAX_PATH_SIZE];
  char log_dir_path_[MAX_PATH_SIZE];
  char sstable_meta_path_[MAX_PATH_SIZE];
  char perf_stat_path_[MAX_PATH_SIZE];
  char *string_buf_;
  static const int64_t MAX_BUF_LENGTH = 8192;

  FILE *pfile_;
  storage::ObSSStore ssstore_;
  memtable::ObMemtable memtable_;
  blocksstable::ObSSTable *sstable_;
  storage::ObPartitionComponentFactory cp_fty_;
  blocksstable::ObPartitionMeta meta_;
  storage::ObPartitionStorage *storage_;
  ObRestoreSchema *restore_schema_;
  MockSchemaService *schema_service_;

  FakePartitionMeta pmeta_;
  blocksstable::ObMacroBlockMetaImage image_;
  blocksstable::ObBaseStorageLogger logger_;
  blocksstable::ObDataFile data_file_;
  blocksstable::ObMacroBlockMarker marker_;

  ObStoragePerfConfig *config_;
  blocksstable::ObStorageCacheSuite *cache_suite_;

  pthread_barrier_t *barrier_;
  common::ObArenaAllocator allocator_;
  common::ObArray<std::pair<uint64_t, common::ObDiagnoseTenantInfo*> > tenant_dis_;

  int64_t thread_no_;
  bool is_inited_;

  ObArray<int64_t> read_cols_;
  int ret_;
};

}//end namespace storageperf
}//end namespace oceanbase

#endif //OB_TOOLS_STORAGE_PERF_READ_H_
