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

#include "ob_storage_perf_read.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/stat/ob_di_cache.h"
#include "lib/container/ob_se_array_iterator.h"
#define private public
#define protected public
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include <stdio.h>
#include <iostream>


#define EVENT_TGET(stat_no)                                      \
    ({                                                            \
         int64_t ret = 0;                                            \
         oceanbase::common::ObDiagnoseTenantInfo *session_info            \
           = oceanbase::common::ObDiagnoseTenantInfo::get_local_diagnose_info();   \
         if (NULL != session_info) {                                \
           if (oceanbase::common::stat_no < oceanbase::common::ObStatEventIds::STAT_EVENT_ADD_END) {    \
             oceanbase::common::ObStatEventAddStat *stat                  \
                 = session_info->get_add_stat_stats().get(                \
                                     ::oceanbase::common::stat_no);    \
             if (NULL != stat) {                                       \
               ret = stat->get_stat_value();                           \
             }                                                         \
           }                                                         \
         }                                                           \
         ret;                                                        \
       })

#define GLOBAL_EVENT_GET(stat_no)             \
  ({                                                \
      int64_t ret = 0;                              \
      ObStatEventAddStat *stat = tenant_info->get_add_stat_stats().get(stat_no); \
      if (NULL != stat) {         \
        ret = stat->stat_value_;   \
      }   \
      ret; \
   })

#define GLOBAL_WAIT_GET(stat_no, wait_event)             \
  ({                                                \
      ObWaitEventStat *stat = tenant_info->get_event_stats().get(stat_no); \
      if (NULL != stat) {         \
        wait_event.total_waits_ = stat->total_waits_;   \
        wait_event.total_timeouts_ = stat->total_timeouts_; \
        wait_event.time_waited_micro_ = stat->time_waited_; \
        wait_event.max_wait_ = stat->max_wait_;   \
      }   \
   })

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share;
using namespace memtable;
using namespace share::schema;
using namespace transaction;

namespace storageperf
{

int FakePartitionMeta::initialize(blocksstable::ObDataFile *data_file,
               blocksstable::ObBaseStorageLogger *logger)
{
  int ret = OB_SUCCESS;
  if(NULL == data_file || NULL == logger) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is NULL", K(data_file), K(logger));
  } else if (OB_FAIL(ObBaseStorageMeta::init(data_file, logger))) {
    STORAGE_LOG(WARN, "fail to init storage meta", K(ret));
  } else if(OB_FAIL(logger->register_redo_module(blocksstable::OB_REDO_LOG_PARTITION, this))) {
    STORAGE_LOG(WARN, "fail to register redo module", K(ret));
  }
  return ret;
}

void FakePartitionMeta::destroy()
{
  ObBaseStorageMeta::destroy();
}

int FakePartitionMeta::add_macro_block(const int64_t block_index)
{
  return blocks_.push_back(block_index);
}

int FakePartitionMeta::read_check_point()
{
  return OB_SUCCESS;
}

int FakePartitionMeta::do_check_point()
{
  return OB_SUCCESS;
}

int FakePartitionMeta::replay(
    const int64_t log_seq_num,
    const int64_t subcmd,
    const char *buf,
    const int64_t len)
{
  UNUSED(log_seq_num);
  UNUSED(subcmd);
  UNUSED(buf);
  UNUSED(len);
  return OB_SUCCESS;
}

int FakePartitionMeta::parse(
   const int64_t subcmd,
   const char *buf,
   const int64_t len,
   FILE *stream)
{
  UNUSED(subcmd);
  UNUSED(buf);
  UNUSED(len);
  UNUSED(stream);
  return OB_SUCCESS;
}

int FakePartitionMeta::get_entry_block(int64_t &entry_block) const
{
  entry_block = 0;
  return OB_SUCCESS;
}

int FakePartitionMeta::mark_macro_block(ObMacroBlockMarkerHelper &helper) const
{
  int ret = OB_SUCCESS;

  ret = helper.set_block(ObMacroBlockCommonHeader::PartitionMeta, blocks_);
  return ret;
}

ObStoragePerfWaitEvent::ObStoragePerfWaitEvent(const char *name)
  : total_waits_(0)
  , total_timeouts_(0)
  , time_waited_micro_(0)
  , max_wait_(0)
  , average_wait_(0.0)
{
  strcpy(name_, name);
}

ObStoragePerfWaitEvent ObStoragePerfWaitEvent::operator - (const ObStoragePerfWaitEvent &event)
{
  ObStoragePerfWaitEvent res(event.name_);
  res.total_waits_ = total_waits_ - event.total_waits_;
  res.total_timeouts_ = total_timeouts_ - event.total_timeouts_;
  res.time_waited_micro_ = time_waited_micro_ - event.time_waited_micro_;
  res.max_wait_ = max_wait_;
  if (res.total_waits_ != 0)
    res.average_wait_ = static_cast<double>(res.time_waited_micro_) / static_cast<double>(res.total_waits_);
  else
    res.average_wait_ = 0.0;
  return res;
}

void ObStoragePerfWaitEvent::reset()
{
  total_waits_ = 0;
  total_timeouts_ = 0;
  time_waited_micro_ = 0;
  max_wait_ = 0;
  average_wait_ = 0.0;
}

int64_t ObStoragePerfWaitEvent::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos,
                 "PerfStat: %s:\n"
                 "PerfStat: total_waits_ = %lu, total_timeouts_ = %lu\n"
                 "PerfStat: time_waited_micro_ = %lu, average_wait_ = %.2f, max_wait_ = %lu\n",
                 name_,
                 total_waits_, total_timeouts_,
                 time_waited_micro_, average_wait_, max_wait_);
  return pos;
}

ObStoragePerfStatistics::ObStoragePerfStatistics()
    : index_cache_hit_(0)
    , index_cache_miss_(0)
    , row_cache_hit_(0)
    , row_cache_miss_(0)
    , block_cache_hit_(0)
    , block_cache_miss_(0)
    , bf_cache_hit_(0)
    , bf_cache_miss_(0)
    , io_read_count_(0)
    , io_read_size_(0)
    , io_read_delay_(0)
    , io_read_queue_delay_(0)
    , io_read_cb_alloc_delay_(0)
    , io_read_cb_process_delay_(0)
    , io_read_prefetch_micro_cnt_(0)
    , io_read_prefetch_micro_size_(0)
    , io_read_uncomp_micro_cnt_(0)
    , io_read_uncomp_micro_size_(0)
    , db_file_data_read_("db_file_data_read_")
    , db_file_data_index_read_("db_file_data_index_read_")
    , kv_cache_bucket_lock_wait_("kv_cache_bucket_lock_wait_")
    , io_queue_lock_wait_("io_queue_lock_wait_")
    , io_controller_cond_wait_("io_controller_cond_wait_")
    , io_processor_cond_wait_("io_processor_cond_wait_")
{
}

void ObStoragePerfStatistics::reset()
{
  index_cache_hit_ = 0;
  index_cache_miss_ = 0;
  row_cache_hit_ = 0;
  row_cache_miss_ = 0;
  block_cache_hit_ = 0;
  block_cache_miss_ = 0;
  io_read_count_ = 0;
  io_read_size_ = 0;
  io_read_delay_ = 0;
  io_read_queue_delay_ = 0;
  io_read_cb_alloc_delay_ = 0;
  io_read_cb_process_delay_ = 0;
  io_read_prefetch_micro_cnt_ = 0;
  io_read_prefetch_micro_size_ = 0;
  io_read_uncomp_micro_cnt_ = 0;
  io_read_uncomp_micro_size_ = 0;
  db_file_data_read_.reset();
  db_file_data_index_read_.reset();
  kv_cache_bucket_lock_wait_.reset();
  io_queue_lock_wait_.reset();
  io_controller_cond_wait_.reset();
  io_processor_cond_wait_.reset();
}

ObStoragePerfStatistics ObStoragePerfStatistics::operator - (const ObStoragePerfStatistics &statics)
{
  ObStoragePerfStatistics res;
  res.index_cache_hit_ = index_cache_hit_ - statics.index_cache_hit_;
  res.index_cache_miss_ = index_cache_miss_ - statics.index_cache_miss_;
  res.row_cache_hit_ = row_cache_hit_ - statics.row_cache_hit_;
  res.row_cache_miss_ = row_cache_miss_ - statics.row_cache_miss_;
  res.block_cache_hit_ = block_cache_hit_ - statics.block_cache_hit_;
  res.block_cache_miss_ = block_cache_miss_ - statics.block_cache_miss_;
  res.bf_cache_hit_ = bf_cache_hit_ - statics.bf_cache_hit_;
  res.bf_cache_miss_ = bf_cache_miss_ - statics.bf_cache_miss_;
  res.io_read_count_ = io_read_count_ - statics.io_read_count_;
  res.io_read_size_ = io_read_size_ - statics.io_read_size_;
  res.io_read_delay_ = io_read_delay_ - statics.io_read_delay_;
  res.io_read_queue_delay_ = io_read_queue_delay_ - statics.io_read_queue_delay_;
  res.io_read_cb_alloc_delay_ = io_read_cb_alloc_delay_ - statics.io_read_cb_alloc_delay_;
  res.io_read_cb_process_delay_ = io_read_cb_process_delay_ - statics.io_read_cb_process_delay_;
  res.io_read_prefetch_micro_cnt_ = io_read_prefetch_micro_cnt_ - statics.io_read_prefetch_micro_cnt_;
  res.io_read_prefetch_micro_size_ = io_read_prefetch_micro_size_ - statics.io_read_prefetch_micro_size_;
  res.io_read_uncomp_micro_cnt_ = io_read_uncomp_micro_cnt_ - statics.io_read_uncomp_micro_cnt_;
  res.io_read_uncomp_micro_size_ = io_read_uncomp_micro_size_ - statics.io_read_uncomp_micro_size_;
  res.db_file_data_read_ = db_file_data_read_ - statics.db_file_data_read_;
  res.db_file_data_index_read_ = db_file_data_index_read_ - statics.db_file_data_index_read_;
  res.kv_cache_bucket_lock_wait_ = kv_cache_bucket_lock_wait_ - statics.kv_cache_bucket_lock_wait_;
  res.io_queue_lock_wait_ = io_queue_lock_wait_ - statics.io_queue_lock_wait_;
  res.io_controller_cond_wait_ = io_controller_cond_wait_ - statics.io_controller_cond_wait_;
  res.io_processor_cond_wait_ = io_processor_cond_wait_ - statics.io_processor_cond_wait_;

  return res;
}

int64_t ObStoragePerfStatistics::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  uint64_t total_deply = io_read_delay_ + io_read_queue_delay_
        + io_read_cb_alloc_delay_ + 8 * io_read_cb_process_delay_;

  uint64_t avg_deply = io_read_count_ == 0 ? total_deply : total_deply / io_read_count_;


  databuff_printf(buf, buf_len, pos,
                  "PerfStat: index_cache_hit_ = %lu , index_cache_miss_ = %lu ,\n"
                  "PerfStat: row_cache_hit_ = %lu , row_cache_miss_ = %lu ,\n"
                  "PerfStat: block_cache_hit_ = %lu , block_cache_miss_ = %lu ,\n"
                  "PerfStat: bf_cache_hit_ = %lu , bf_cache_miss_ = %lu ,\n"
                  "PerfStat: io_read_count_ = %lu , io_read_size_ = %lu , io_read_delay_ = %lu, io_read_queue_delay = %lu, \n"
                  "PerfStat: io_read_cb_alloc_delay = %lu, io_read_cb_process_delay_ = %lu ,\n"
                  "PerfStat: io_read_prefetch_micro_cnt = %lu, io_read_prefetch_micro_size_ = %lu, \n"
                  "PerfStat: io_read_uncomp_micro_cnt_ = %lu, io_read_uncomp_micro_size_ = %lu, \n"
                  "PerfStat: total_delay = %lu, avg_delay = %lu, \n",
                  index_cache_hit_, index_cache_miss_,
                  row_cache_hit_, row_cache_miss_,
                  block_cache_hit_, block_cache_miss_,
                  bf_cache_hit_, bf_cache_miss_,
                  io_read_count_, io_read_size_, io_read_delay_, io_read_queue_delay_,
                  io_read_cb_alloc_delay_, io_read_cb_process_delay_,
                  io_read_prefetch_micro_cnt_, io_read_prefetch_micro_size_,
                  io_read_uncomp_micro_cnt_, io_read_uncomp_micro_size_, total_deply, avg_deply);
  pos += db_file_data_read_.to_string(buf + pos, buf_len - pos);
  pos += db_file_data_index_read_.to_string(buf + pos, buf_len - pos);
  pos += kv_cache_bucket_lock_wait_.to_string(buf + pos, buf_len - pos);
  pos += io_queue_lock_wait_.to_string(buf + pos, buf_len- pos);
  pos += io_controller_cond_wait_.to_string(buf + pos, buf_len - pos);
  pos += io_processor_cond_wait_.to_string(buf + pos, buf_len - pos);
  return pos;
}

ObSingleRowSpeed::ObSingleRowSpeed()
  : sum_duation_(0)
  , count_(0)
{

}

int64_t ObSingleRowSpeed::to_string(char *buf, const int64_t buf_len) const {
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "sum_duation_ = %lu us, count_ = %lu , avg_speed = %.4Lf us/row",
                  sum_duation_, count_, sum_duation_/static_cast<long double>(count_));
  return pos;
}

int64_t ObSingleRowSpeed::get_avg() {
  int64_t ret = 0;
  if(count_ > 0) {
    ret = sum_duation_/count_;
  }
  return ret;
}

ObUpdateRowIter::ObUpdateRowIter()
  : seed_(0)
  , is_inited_(false)
  , row_generate_(NULL)
  , allocator_(ObModIds::TEST)
{

}

int ObUpdateRowIter::init(ObRowGenerate *generate)
{
  int ret = OB_SUCCESS;
  if(is_inited_){
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice");
  } else if(NULL == generate){
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row generate is NULL");
  } else {
    row_generate_ = generate;
    is_inited_ = true;
  }
  return ret;
}

void ObUpdateRowIter::reset()
{
  seed_ = 0;
  is_inited_ = false;
  row_generate_ = NULL;
  allocator_.reset();
}

int ObUpdateRowIter::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *store_row = NULL;
  allocator_.reuse();
  bool old_value = false;
  int64_t tmp_seed = 0;

  if(0 == seed_ % 2) {//get old row value
    old_value = true;
    tmp_seed = seed_/2;
  } else {
    old_value = false;
    tmp_seed = (seed_ - 1)/2;
  }

  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "please init first");
  } else if(OB_SUCCESS != (ret = row_generate_->get_next_row(tmp_seed, store_row, old_value))){
    STORAGE_LOG(WARN, "fail to get next row form row generate", K(ret));
  } else if(OB_SUCCESS != (ret = ob_write_row(allocator_, store_row->row_val_, row_))){
    STORAGE_LOG(WARN, "fail to write ObNewRow", K(ret));
  } else {
    //STORAGE_LOG(INFO, " ", K(*store_row), K(row_));
    row = &row_;
    ++seed_;
    if(0 == seed_ % (update_rows_num * 2)){
      ret = OB_ITER_END;
    }
  }
  return ret;
}

ObStoragePerfRead::ObStoragePerfRead()
 : pfile_(NULL)
 , sstable_(NULL)
 , schema_service_(NULL)
 , config_(NULL)
 , cache_suite_(NULL)
 , barrier_(NULL)
 , allocator_()
 , tenant_dis_()
 , thread_no_(-1)
 , is_inited_(false)
 , ret_(OB_SUCCESS)
{

}

ObStoragePerfRead::~ObStoragePerfRead()
{
  destroy();
}

int ObStoragePerfRead::init(ObStoragePerfConfig *config,
                            const int64_t thread_no,
                            ObStorageCacheSuite *cache_suite,
                            ObRestoreSchema *restore_schema,
                            MockSchemaService *schema_service,
                            ObPartitionStorage *storage,
                            pthread_barrier_t *barrier)
{
  int ret = OB_SUCCESS;

  if(is_inited_){
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice");
  } else if(NULL == config || NULL == cache_suite || NULL == schema_service){
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is NULL", K(config), K(cache_suite));
  } else if (NULL == (string_buf_ = static_cast<char*>(ob_malloc(MAX_BUF_LENGTH)))) {
    STORAGE_LOG(WARN, "allocate string buf failed");
  } else {
    config_ = config;
    thread_no_ = thread_no;
    cache_suite_ = cache_suite;
    restore_schema_ = restore_schema;
    schema_service_ = schema_service;
    storage_ = storage;
    barrier_ = barrier;
    is_inited_ = true;
  }
  return ret;
}

int ObStoragePerfRead::update_to_memtable()
{
  int ret = OB_SUCCESS;
  ObMemtableCtxFactory mem_ctx_fty;
  ObSEArray<uint64_t, 512>column_ids;
  ObSEArray<uint64_t, 512>update_column_ids;
  for(int64_t i = 0; i < update_column_num; ++i){//no all column in memtable
    column_ids.push_back(OB_APP_MIN_COLUMN_ID + i);
  }
  for(int64_t i = rowkey_column_count; i < update_column_num; ++i){
    update_column_ids.push_back(OB_APP_MIN_COLUMN_ID + i);
  }

  ObRowGenerate row_generate;
  ObUpdateRowIter insert_iter;
  int64_t affected_rows;
  const int64_t total_update_row_count = config_->get_partition_size() * 1024L * row_count_per_macro/2 * config_->get_write_to_memtable_percent() / 100;//TODO:const
  ObSchemaGetterGuard *schema_guard;
  const ObTableSchema *schema = NULL;

  if(OB_FAIL(restore_schema_->parse_from_file("./storage_perf_update.schema", schema_guard))) {
    STORAGE_LOG(WARN, "fail to init schema", K(ret));
  } else {
 //   schema_service_ = restore_schema_.schema_service_;
 //   schema_service_->get_schema_guard(schema_guard, INT64_MAX);
    schema_guard->get_table_schema(combine_id(tenant_id, pure_id+1), schema);
    if (schema == NULL) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "schema is NULL");
    } else if(OB_SUCCESS != (ret = row_generate.init(*schema))) {
      STORAGE_LOG(WARN, "fail to init table schema");
    } else if(OB_SUCCESS != (ret = insert_iter.init(&row_generate))) {
      STORAGE_LOG(WARN, "fail to init ObUpdateRowIter", K(ret));
    }
  }

  for(int64_t i = 0; OB_SUCC(ret) && i < total_update_row_count; i += update_rows_num) {
    ObStoreCtx ins_ctx;
    ins_ctx.mem_ctx_ = mem_ctx_fty.alloc();
    ins_ctx.mem_ctx_->trans_begin();
    ins_ctx.mem_ctx_->sub_trans_begin(1, query_timeout + ObTimeUtility::current_time());

    ObDMLBaseParam dml_param;
    dml_param.timeout_ = ObTimeUtility::current_time() + query_timeout;
    dml_param.schema_version_ = 0;

    if(OB_FAIL(storage_->update_rows(ins_ctx, dml_param, column_ids, update_column_ids,  &insert_iter, affected_rows))) {
      STORAGE_LOG(WARN, "fail to insert row", K(ret));
      ob_print_mod_memory_usage();
    }
    ins_ctx.mem_ctx_->trans_end(true, 1);
    mem_ctx_fty.free(ins_ctx.mem_ctx_);
  }
  return ret;
}

int ObStoragePerfRead::single_get_speed()
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    //ObTenantStatEstGuard session_guard(1, 123*(thread_no_+1));
    ObTenantStatEstGuard tenant_guard(1);
    ObRowGenerate row_generate;
    ObMemtableCtxFactory mem_ctx_fty;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *schema = NULL;
    if (OB_FAIL(schema_service_->get_schema_guard(schema_guard, INT64_MAX))) {
      STORAGE_LOG(WARN, "failed to get schema guard");
    } else if (OB_FAIL(schema_guard.get_table_schema(combine_id(tenant_id, pure_id), schema))) {
      STORAGE_LOG(WARN, "failed to get table schema");
    } else if (schema == NULL) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "schema is NULL");
    } else if(OB_SUCCESS != (ret = row_generate.init(*schema))) {
      STORAGE_LOG(WARN, "fail to init table schema");
    } else {
      int64_t repeat_times = config_->get_single_get_times();
      while (OB_SUCC(ret) && repeat_times--) {
        //ctx
        ObStoreCtx get_ctx;
        if (OB_ISNULL(get_ctx.mem_ctx_ = mem_ctx_fty.alloc())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "mem ctx is null");
        } else if (OB_FAIL(get_ctx.mem_ctx_->trans_begin())) {
          STORAGE_LOG(WARN, "failed to begin trans");
        } else if (OB_FAIL(get_ctx.mem_ctx_->sub_trans_begin(1, query_timeout + ObTimeUtility::current_time()))) {
          STORAGE_LOG(WARN, "failed to begin sub strans");
        } else {
          //param
          ObTransDesc trans_desc;
          ObTableScanParam scan_param;
          scan_param.pkey_.init(combine_id(tenant_id, pure_id), 1, 1);
//          scan_param.pkey_.table_id_ = combine_id(tenant_id, pure_id);
//          scan_param.pkey_.partition_idx_ = 1;
//          scan_param.pkey_.partition_cnt_ = 1;
          for(int64_t i = 0; OB_SUCC(ret) && i < read_cols_.count(); ++i){
            if (OB_FAIL(scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + read_cols_.at(i)))) {
              STORAGE_LOG(WARN, "failed to push back");
            }
          }
          if (OB_SUCC(ret)) {
            scan_param.index_id_ = combine_id(tenant_id, pure_id);
            //row to store rowkey
            //get range
            ObNewRange range;
            scan_param.timeout_ = ObTimeUtility::current_time() + query_timeout;
            scan_param.reserved_cell_count_ = OB_MAX_COLUMN_NUMBER;
            scan_param.for_update_ = false;
            scan_param.for_update_wait_timeout_ = -1;
            scan_param.schema_version_ = 0;

            if(OB_FAIL(set_trans_desc(trans_desc))){
              STORAGE_LOG(WARN, "fail to set trans", K(ret));
            } else {
              scan_param.trans_desc_ = &trans_desc;
              //get one row
              ObNewRowIterator *get_iter = NULL;
              ObNewRow *row = NULL;
              ObStoragePerfStatistics bef_statics;
              ObStoragePerfStatistics aft_statics;
              ObStoragePerfStatistics res_statics;
              ObStoragePerfStatistics out_bef_statics;
              ObStoragePerfStatistics out_aft_statics;
              ObStoragePerfStatistics out_res_statics;

              out_bef_statics.reset();
              out_aft_statics.reset();
              out_res_statics.reset();

              ObRowkey rk;
              ObNewRange range;
              if (OB_FAIL(row_generate.get_rowkey_by_seed(rand() % config_->get_range_, rk))) {
                //          if(OB_FAIL(row_generate.get_next_row(rand() % config_->get_range_, range_row))){
                STORAGE_LOG(WARN, "fail to get next row", K(ret));
              } else {
                range.table_id_ = combine_id(tenant_id, pure_id);
                range.start_key_ = rk;
                range.end_key_ = rk;
                range.border_flag_.set_inclusive_start();
                range.border_flag_.set_inclusive_end();
                scan_param.key_ranges_.reset();
                if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
                  STORAGE_LOG(WARN, "fail to push back", K(ret));
                }
              }

              srand(static_cast<unsigned int>(ObTimeUtility::current_time()));

              int64_t total_time = 0;
              int64_t row_count = 0;
              for(int64_t i = 0; OB_SUCC(ret) && i < config_->get_total_single_row_count(); ++i) {
                out_bef_statics.reset();
                out_aft_statics.reset();
                out_res_statics.reset();
                pthread_barrier_wait(barrier_);
                if (OB_FAIL(set_global_stat(out_bef_statics))) {
                  STORAGE_LOG(WARN, "fail to set global stat", K(ret));
                } else {
                  int64_t single_begin = ObTimeUtility::current_time();
                  if(OB_FAIL(storage_->table_scan(scan_param, get_iter))){
                    STORAGE_LOG(WARN, "fail to table scan", K(ret));
                  } else if(OB_FAIL(get_iter->get_next_row(row))){
                    STORAGE_LOG(WARN, "fail to get next row", K(ret));
                  } else if(NULL == row){
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "fail to get next row");
                  } else {
                    if (config_->print_row_) {
                      print_row(row, row_count);
                    }
                    int64_t t = ObTimeUtility::current_time() - single_begin;
                    pthread_barrier_wait(barrier_);
                    if (OB_FAIL(set_global_stat(out_aft_statics))) {
                      STORAGE_LOG(WARN, "fail to set global stat", K(ret));
                    } else {
                      std::cout << t << std::endl;
                      total_time += t;
                      if (config_->print_perf_stat_) {
                        out_res_statics = out_aft_statics - out_bef_statics;
                        int64_t pos = out_res_statics.to_string(string_buf_, MAX_BUF_LENGTH);
                        if (pos < MAX_BUF_LENGTH && pos >= 0) {
                          string_buf_[pos] = '\0';
                          fprintf(stderr, "PerfStat: --- %ld iteration ---\n", i );
                          fprintf(stderr, "PerfStat: runtime = %ld\n", t);
                          fprintf(stderr, "%s\n\n", string_buf_);
                          fflush(stderr);
                        }
                      }
                      if(NULL != get_iter) {//ignore success
                        if(OB_FAIL(storage_->revert_scan_iter(get_iter))){
                          STORAGE_LOG(WARN,"fail to revert inter", K(ret));
                        }
                      }
                      if (OB_SUCC(ret)) {
                        if (OB_FAIL(flush_cache_or_not())) {
                          STORAGE_LOG(WARN,"fail to flush cache", K(ret));
                        }
                      }
                    }
                  }
                }
              }
              pthread_barrier_wait(barrier_);
              if (OB_FAIL(get_ctx.mem_ctx_->trans_end(true, 1))) {
                STORAGE_LOG(WARN,"fail to end trans", K(ret));
              } else {
                mem_ctx_fty.free(get_ctx.mem_ctx_);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObStoragePerfRead::multi_get_speed()
{
  int ret = OB_SUCCESS;

  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    //ObSessionStatEstGuard session_guard(1, 124 * (thread_no_ + 1));
    ObTenantStatEstGuard tenant_guard(1);
    ObRowGenerate row_generate;
    ObArenaAllocator allocator(ObModIds::TEST);
    ObMemtableCtxFactory mem_ctx_fty;

    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *schema = NULL;
    schema_service_->get_schema_guard(schema_guard, INT64_MAX);
    if (OB_FAIL(schema_guard.get_table_schema(combine_id(tenant_id, pure_id), schema))) {
      STORAGE_LOG(WARN, "failed to get table schema", K(ret));
    } else if (schema == NULL) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "schema is NULL");
    } else if(OB_SUCCESS != (ret = row_generate.init(*schema, &allocator))){
      STORAGE_LOG(WARN, "fail to init table schema");
    } else {
      int64_t repeat_times = config_->get_multi_get_times();
      while (OB_SUCC(ret) && repeat_times--) {
        //ctx
        ObStoreCtx get_ctx;
        if (OB_ISNULL(get_ctx.mem_ctx_ = mem_ctx_fty.alloc())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "failed to alloc mem ctx", K(ret));
        } else if (OB_FAIL(get_ctx.mem_ctx_->trans_begin())) {
          STORAGE_LOG(WARN, "failed to begin trans", K(ret));
        } else if (OB_FAIL(get_ctx.mem_ctx_->sub_trans_begin(1, query_timeout + ObTimeUtility::current_time()))) {
          STORAGE_LOG(WARN, "failed to begin sub trans", K(ret));
        } else {
          //param
          ObTransDesc trans_desc;
          ObTableScanParam scan_param;
          scan_param.pkey_.init(combine_id(tenant_id, pure_id), 1, 1);
//          scan_param.pkey_.table_id_ = combine_id(tenant_id, pure_id);
//          scan_param.pkey_.partition_idx_ = 1;
//          scan_param.pkey_.partition_cnt_ = 1;
          //        STORAGE_LOG(WARN, "[SCHEMA]", K(schema->get_column_count()), K(read_cols_));
          for(int64_t i = 0; i < read_cols_.count(); ++i) {
            scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + read_cols_.at(i));
          }
          scan_param.index_id_ = combine_id(tenant_id, pure_id);
          scan_param.scan_flag_.flag_ = 0;

          scan_param.timeout_ = ObTimeUtility::current_time() + query_timeout;
          scan_param.reserved_cell_count_ = OB_MAX_COLUMN_NUMBER;
          scan_param.for_update_ = false;
          scan_param.for_update_wait_timeout_ = -1;
          scan_param.schema_version_ = 0;

          ObNewRange range;
          ObRowkey rowkey;
          srand(static_cast<unsigned int>(ObTimeUtility::current_time()));
          scan_param.key_ranges_.reset();
          for(int64_t i = 0; OB_SUCC(ret) && i < config_->get_total_multi_row_count(); ++i) {
            range.reset();
            rowkey.reset();
            //row to store rowkey
            if(OB_FAIL(row_generate.get_rowkey_by_seed(rand() % config_->get_range_, rowkey))){
              STORAGE_LOG(WARN, "fail to get next row", K(ret));
            } else {
              range.border_flag_.set_inclusive_start();
              range.border_flag_.set_inclusive_end();
              range.table_id_ = combine_id(tenant_id, pure_id);
              range.start_key_ = rowkey;
              range.end_key_ = rowkey;
              scan_param.key_ranges_.push_back(range);
            }
          }
          if (OB_SUCC(ret)) {
            if(OB_FAIL(set_trans_desc(trans_desc))){
              STORAGE_LOG(WARN, "fail to set trans", K(ret));
            } else {
              scan_param.trans_desc_ = &trans_desc;

              ObStoragePerfStatistics bef_statics;
              ObStoragePerfStatistics aft_statics;
              ObStoragePerfStatistics res_statics;
              ObStoragePerfStatistics out_bef_statics;
              ObStoragePerfStatistics out_aft_statics;
              ObStoragePerfStatistics out_res_statics;
              ObSingleRowSpeed row_cache_speed;
              ObSingleRowSpeed block_cache_speed;
              ObSingleRowSpeed read_one_micro_block_speed;
              ObSingleRowSpeed read_two_micro_block_speed;
              ObSingleRowSpeed other_speed;
              int64_t one_run_begin = 0;

              const int multi_get_run = config_->get_multi_get_run();
              for (int run = 0; OB_SUCC(ret) && run < multi_get_run; ++run) {

                pthread_barrier_wait(barrier_);
                ObNewRowIterator *get_iter = NULL;
                ObNewRow *row = NULL;

                int64_t row_count = 0;
                int64_t real_row_count = 0;

                out_bef_statics.reset();
                out_aft_statics.reset();
                out_res_statics.reset();
                if (OB_FAIL(set_global_stat(out_bef_statics))) {
                  STORAGE_LOG(WARN, "failed to get global stat", K(ret));
                } else {
                  one_run_begin = ObTimeUtility::current_time();
                  if(OB_FAIL(storage_->table_scan(scan_param, get_iter))) {
                    STORAGE_LOG(WARN, "fail to table scan", K(ret));
                  }
                  for(real_row_count = 0; OB_SUCCESS == ret; ++real_row_count){
                    if(OB_FAIL(get_iter->get_next_row(row))){
                      STORAGE_LOG(WARN, "fail to get next row", K(ret), K(real_row_count));
                    } else if(NULL == row) {
                      ret = OB_ERR_UNEXPECTED;
                      STORAGE_LOG(WARN, "fail to get next row");
                    } else {
                      if (config_->print_row_) {
                        print_row(row, row_count);
                      }
                    }
                  }
                  if (OB_ITER_END != ret) {
                    STORAGE_LOG(WARN,"fail to scan", K(ret));
                  } else {
                    ret = OB_SUCCESS;
                    int64_t one_run_end = ObTimeUtility::current_time();
                    if (OB_FAIL(set_global_stat(out_aft_statics))) {
                      STORAGE_LOG(WARN, "failed to get global stat", K(ret));
                    } else {
                      if(NULL != get_iter) {//igonre ret
                        if(OB_FAIL(storage_->revert_scan_iter(get_iter))){
                          STORAGE_LOG(WARN,"fail to revert inter", K(ret));
                        }
                      }
                      if (OB_SUCC(ret)) {
                        std::cout << real_row_count - 1 << "," << one_run_end - one_run_begin << std::endl;
                        if (config_->print_perf_stat_) {
                          out_res_statics = out_aft_statics - out_bef_statics;
                          int64_t pos = out_res_statics.to_string(string_buf_, MAX_BUF_LENGTH);
                          if (pos < MAX_BUF_LENGTH && pos >= 0) {
                            string_buf_[pos] = '\0';
                            fprintf(stderr, "PerfStat: --- %ld iteration, %d run ---\n", config_->get_multi_get_times() - repeat_times, run);
                            fprintf(stderr, "PerfStat: runtime = %ld, end = %ld, begin = %ld\n", one_run_end - one_run_begin, one_run_end, one_run_begin);
                            fprintf(stderr, "%s\n\n", string_buf_);
                            fflush(stderr);
                          }
                        }
                        if (OB_FAIL(get_ctx.mem_ctx_->trans_end(true, 1))) {
                          STORAGE_LOG(WARN,"fail to revert inter", K(ret));
                        } else {
                          mem_ctx_fty.free(get_ctx.mem_ctx_);
                          if (OB_FAIL(flush_cache_or_not())) {
                            STORAGE_LOG(WARN,"fail to flush cache", K(ret));
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        row_generate.reuse();
      }
    }
  }
  return ret;
}

int ObStoragePerfRead::flush_cache_or_not()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && config_->flush_block_index_cache_) {
    if (OB_FAIL(ObKVGlobalCache::get_instance().erase_cache(OB_SYS_TENANT_ID, "block_index_cache"))) {
      STORAGE_LOG(WARN, "failed to flush cache", K(ret));
    }
  }
  if (OB_SUCC(ret) && config_->flush_block_cache_) {
    if (OB_FAIL(ObKVGlobalCache::get_instance().erase_cache(OB_SYS_TENANT_ID, "user_block_cache"))) {
      STORAGE_LOG(WARN, "failed to flush cache", K(ret));
    }
  }
  if (OB_SUCC(ret) && config_->flush_row_cache_) {
    if (OB_FAIL(ObKVGlobalCache::get_instance().erase_cache(OB_SYS_TENANT_ID, "user_row_cache"))) {
      STORAGE_LOG(WARN, "failed to flush cache", K(ret));
    }
  }
  if (OB_SUCC(ret) && config_->flush_bf_cache_) {
    if (OB_FAIL(ObKVGlobalCache::get_instance().erase_cache(OB_SYS_TENANT_ID, "bf_cache"))) {
      STORAGE_LOG(WARN, "failed to flush cache", K(ret));
    }
  }
  return ret;
}

int ObStoragePerfRead::scan_speed()
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    ObSessionStatEstGuard session_guard(1, 125 * (thread_no_ + 1));
    ObRowGenerate row_generate;
    ObRowGenerate row_generate2;
    ObArenaAllocator allocator(ObModIds::TEST);
    ObMemtableCtxFactory mem_ctx_fty;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *schema = NULL;
    schema_service_->get_schema_guard(schema_guard, INT64_MAX);
    if (OB_FAIL(schema_guard.get_table_schema(combine_id(tenant_id, pure_id), schema))) {
      STORAGE_LOG(WARN, "failed to get table schema", K(ret));
    } else if (schema == NULL) {
      STORAGE_LOG(WARN, "schema is NULL");
    } else if(OB_SUCCESS != (ret = row_generate.init(*schema))){
      STORAGE_LOG(WARN, "fail to init table schema");
    } else if(OB_SUCCESS != (ret = row_generate2.init(*schema))){
      STORAGE_LOG(WARN, "fail to init table schema");
    } else {
      int64_t repeat_times = config_->get_scan_times();
      while (OB_SUCC(ret) && repeat_times--) {
        //ctx
        ObStoreCtx get_ctx;
        if (OB_ISNULL(get_ctx.mem_ctx_ = mem_ctx_fty.alloc())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "fail to alloc mem ctx", K(ret));
        } else if (OB_FAIL(get_ctx.mem_ctx_->trans_begin())) {
          STORAGE_LOG(WARN, "fail to begin trans", K(ret));
        } else if (OB_FAIL(get_ctx.mem_ctx_->sub_trans_begin(1, query_timeout + ObTimeUtility::current_time()))) {
          STORAGE_LOG(WARN, "fail to begin trans", K(ret));
        } else {
          //param
          ObTransDesc trans_desc;
          ObTableScanParam scan_param;
          scan_param.pkey_.init(combine_id(tenant_id, pure_id), 1, 1);
//          scan_param.pkey_.table_id_ = combine_id(tenant_id, pure_id);
//          scan_param.pkey_.partition_idx_ = 1;
//          scan_param.pkey_.partition_cnt_ = 1;
          for(int64_t i = 0; i < read_cols_.count(); ++i){
            scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + read_cols_.at(i));
          }
          scan_param.index_id_ = combine_id(tenant_id, pure_id);
          scan_param.scan_flag_.whole_macro_scan_ = 0 == config_->get_scan_use_cache() ? 0 : 1;

          scan_param.timeout_ = ObTimeUtility::current_time() + query_timeout;
          scan_param.reserved_cell_count_ = OB_MAX_COLUMN_NUMBER;
          scan_param.for_update_ = false;
          scan_param.for_update_wait_timeout_ = -1;
          scan_param.schema_version_ = 0;

          if(OB_FAIL(set_trans_desc(trans_desc))){
            STORAGE_LOG(WARN, "fail to set trans", K(ret));
          } else {
            scan_param.trans_desc_ = &trans_desc;
            int64_t total_row_count = config_->get_partition_size() * 1024L * row_count_per_macro/2;
            ObNewRange range;
            ObRowkey start_rowkey;
            ObRowkey end_rowkey;
            int64_t start_seed = 0;
            int64_t end_seed = config_->get_total_scan_row_count()-1;
            range.reset();
            start_rowkey.reset();
            end_rowkey.reset();
            //row to store rowkey
            if(OB_FAIL(row_generate.get_rowkey_by_seed(start_seed, start_rowkey))){
              STORAGE_LOG(WARN, "fail to get next row", K(ret));
            } else if(OB_FAIL(row_generate2.get_rowkey_by_seed(end_seed, end_rowkey))){
              STORAGE_LOG(WARN, "fail to get next row", K(ret));
            } else {
              //get range
              range.border_flag_.set_inclusive_start();
              range.border_flag_.set_inclusive_end();
              range.table_id_ = combine_id(tenant_id, pure_id);
              range.start_key_ = start_rowkey;
              range.end_key_ = end_rowkey;
              scan_param.key_ranges_.reset();
              scan_param.key_ranges_.push_back(range);

              const int scan_run = config_->get_scan_run();
              for (int run = 0; OB_SUCC(ret) && run < scan_run; ++run) {
                //get one row
                ObNewRowIterator *get_iter = NULL;
                ObNewRow *row = NULL;
                int64_t real_count = 0;

                ObStoragePerfStatistics out_bef_statics;
                ObStoragePerfStatistics out_aft_statics;
                ObStoragePerfStatistics out_res_statics;
                ObSingleRowSpeed row_cache_speed;
                ObSingleRowSpeed block_cache_speed;
                ObSingleRowSpeed read_one_micro_block_speed;
                ObSingleRowSpeed read_two_micro_block_speed;
                ObSingleRowSpeed other_speed;
                pthread_barrier_wait(barrier_);
                if (config_->print_perf_stat_) {
                  if (thread_no_ == 0) {
                    out_bef_statics.reset();
                    out_aft_statics.reset();
                    out_res_statics.reset();
                    if (OB_FAIL(set_global_stat(out_bef_statics))) {
                      STORAGE_LOG(WARN, "fail to get global stat", K(ret));
                    }
                  }
                }
                if (OB_SUCC(ret)) {
                  int64_t total_scan_time = 0;
                  int64_t row_count = 1;
                  const int64_t begin = ObTimeUtility::current_time();
                  if(OB_FAIL(storage_->table_scan(scan_param, get_iter))){
                    STORAGE_LOG(WARN, "fail to table scan", K(ret));
                  }
                  for(int64_t j = 0; OB_SUCC(ret) && j < end_seed - start_seed + 1; ++j, ++real_count) {
                    if(OB_FAIL(get_iter->get_next_row(row))) {
                      if (OB_ITER_END != ret) {
                        STORAGE_LOG(WARN, "fail to get next row", K(ret), K(j), K(end_seed),
                            K(start_seed), K(total_row_count), K(start_rowkey), K(end_rowkey));
                      }
                    } else if(NULL == row){
                      ret = OB_ERR_UNEXPECTED;
                      STORAGE_LOG(WARN, "fail to get next row");
                    } else {
                      if (config_->print_row_) {
                        print_row(row, row_count);
                      }
                    }
                  }
                  if (OB_SUCC(ret)) {
                    total_scan_time = ObTimeUtility::current_time() - begin;
                    std::cout << real_count << "," << total_scan_time << std::endl;
                    if (config_->print_perf_stat_) {
                      if (thread_no_ == 0) {
                        ObStoragePerfConfig::print_bianque_timestamp("scan_end_time", stderr);
                        set_global_stat(out_aft_statics);
                        out_res_statics = out_aft_statics - out_bef_statics;
                        int64_t pos = out_res_statics.to_string(string_buf_, MAX_BUF_LENGTH);
                        if (pos < MAX_BUF_LENGTH && pos >= 0) {
                          string_buf_[pos] = '\0';
                          fprintf(stderr, "PerfStat: --- scan %ld iteration %d run ---\n", config_->get_multi_get_times() - repeat_times, run);
                          fprintf(stderr, "PerfStat: runtime = %ld\n", total_scan_time);
                          fprintf(stderr, "%s\n\n", string_buf_);
                          fflush(stderr);
                        }
                      }
                    }

                    if(OB_ITER_END != get_iter->get_next_row(row)){
                      STORAGE_LOG(WARN, "fail to get the end");
                    } else {
                      if(NULL != get_iter) {
                        if(OB_FAIL(storage_->revert_scan_iter(get_iter))){
                          STORAGE_LOG(WARN,"fail to revert inter", K(ret));
                        }
                      }
                      if (OB_SUCC(ret)) {
                        if(real_count != end_seed - start_seed + 1) {
                          STORAGE_LOG(WARN, "scan count not match", K(real_count), K(end_seed - start_seed + 1));
                        } else if (OB_FAIL(flush_cache_or_not())) {
                          STORAGE_LOG(WARN, "failed to flush cache", K(ret));
                        }
                        pthread_barrier_wait(barrier_);
                      }
                    }
                  }
                }
              }
            }
          }
          if (OB_FAIL(get_ctx.mem_ctx_->trans_end(true, 1))) {
            STORAGE_LOG(WARN, "failed to end trans", K(ret));
          } else {
            mem_ctx_fty.free(get_ctx.mem_ctx_);
          }
        }
      }
    }
  }
  return ret;
}

int ObStoragePerfRead::init_partition_storage()
{
  int ret = OB_SUCCESS;
  //ObPartitionKey pkey(combine_id(tenant_id, pure_id), 0, 1);
  ObBaseStorage base_storage;
  meta_.table_id_ = combine_id(tenant_id, pure_id);
  meta_.index_table_count_ = 1;
  memtable_.set_version(ObVersion(2));//TODO:const

  if(OB_FAIL(init_data_file())){
    STORAGE_LOG(WARN, "fail to init data file", K(ret));
  } else if(OB_FAIL(ssstore_.init(&data_file_, &cp_fty_, meta_, cache_suite_, NULL))){
    STORAGE_LOG(WARN, "fail to init ssstore", K(ret));
  } else if (OB_FAIL(ssstore_.create_new_sstable(sstable_, 1))){
    STORAGE_LOG(WARN, "fail to create sstable");
  } else if(OB_SUCCESS != (ret = backup_sstable_meta(*sstable_))){
    STORAGE_LOG(WARN, "fail to backup sstable meta", K(ret));
  } else if(!sstable_->is_valid()){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is invalid");
  } else if(OB_FAIL(ssstore_.add_sstable(sstable_->get_meta().index_id_, sstable_))){
    STORAGE_LOG(WARN, "fail to add sstable", K(ret));
  //} else if(OB_FAIL(memtable_.init(pkey))) {
  //  STORAGE_LOG(WARN, "fail to init mem table", K(ret));
  //} else if(OB_FAIL(storage_->init(pkey, pkey, &cp_fty_, &base_storage, schema_service_,
  //                                 (transaction::ObTransService *)0x2, NULL))){
  //  STORAGE_LOG(WARN, "fail to init partitin storage", K(ret));
  //} else if(OB_FAIL(storage_->add_store(ssstore_, false))){
  //  STORAGE_LOG(WARN, "fail to add store", K(ret));
  //} else if (OB_FAIL(storage_->add_store(memtable_, false))){
  //  STORAGE_LOG(WARN, "fail to add memtable");
  //} else if (OB_FAIL(update_to_memtable())) {
  //  STORAGE_LOG(WARN, "fail to update data to memtable", K(ret));
  }

  return ret;
}

int ObStoragePerfRead::backup_sstable_meta(ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int64_t size = 0;
  int64_t pos = 0;
  char *buf = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);

  if(-1 == (fd = open(sstable_meta_path_, O_RDONLY, 0777))){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to open meta file", K(sstable_meta_path_), K(fd));
  } else if(0 == (size = lseek(fd, 0, SEEK_END))){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get file length", K(size), K(sstable_meta_path_));
  } else if(0 != lseek(fd, 0, SEEK_SET)){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to reset lseek");
  } else if(NULL == (buf = static_cast<char *>(allocator.alloc(size)))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocator memory", K(size), K(ret));
  } else if (size != read(fd, buf, size)){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "fail to read meta");
  } else if(OB_SUCCESS != (ret = sstable.deserialize(allocator_, buf, size, pos))){
    STORAGE_LOG(WARN, "fail to deserialize sstable", K(ret));
  } else {
    STORAGE_LOG(INFO, "backup sstable meta success");
    close(fd);
  }
  return ret;
}

int ObStoragePerfRead::init_data_file()
{
  int ret = OB_SUCCESS;
  ObBlockFile::FileLocation  location;
  ObBlockFile::FileSpec spec;
  location.disk_no_ = 1;
  location.install_sequence_ = 1;

  if (static_cast<int64_t>(strlen(data_file_path_)) > common::OB_MAX_FILE_NAME_LENGTH){
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "data file is too long", K(strlen(data_file_path_)));
  } else {
    strcpy(location.path_, data_file_path_);
    spec.macro_block_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
    spec.data_file_size_ = config_->get_partition_size() * 1024L * 1024L * 1024L;//GB
  }

  if(OB_SUCC(ret)){
    if (OB_SUCCESS != (ret = data_file_.open(location, &image_))) {
      STORAGE_LOG(WARN, "open data file error.", K(ret));
    } else if (OB_SUCCESS != (ret = image_.initialize(&data_file_, &logger_))) {
        STORAGE_LOG(WARN, "initialize macro block meta error.", K(ret));
    } else if (OB_SUCCESS != (ret = pmeta_.initialize(&data_file_, &logger_))) {
      STORAGE_LOG(WARN, "initialize partition meta error.", K(ret));
    } else if (OB_SUCCESS != (ret = logger_.init(data_file_, log_dir_path_, 1024L*1024L*1024L))) {
      STORAGE_LOG(WARN, "initialize commit logger", K(ret));
    } else if (OB_SUCCESS != (ret = logger_.replay())) {
      STORAGE_LOG(WARN, "replay commit log error.", K(ret));
    } else if (OB_SUCCESS != (ret = marker_.initialize(&data_file_, &image_))) {
      STORAGE_LOG(WARN, "initialize macro block marker error.", K(ret));
    } else {
      for (int64_t i = 0; i < data_file_.get_total_macro_block_cnt(); ++i) {
        const ObMacroBlockMeta *meta = image_.get_meta_ptr(i);
        if (NULL != meta && meta->attr_ == ObMacroBlockCommonHeader::SSTableData) {
          ret = pmeta_.add_macro_block(i);
          if (ret) return ret;
        }
      }
      if (OB_SUCCESS != (ret = marker_.register_storage_meta(&image_))) {
        STORAGE_LOG(WARN, "register macro block meta error.", K(ret));
      }  else if (OB_SUCCESS != (ret = marker_.register_storage_meta(&pmeta_))) {
        STORAGE_LOG(WARN, "register macro block meta error.", K(ret));
      } else if (OB_SUCCESS != (ret = marker_.mark_init())) {
        STORAGE_LOG(WARN, "build first free list error.", K(ret));
      }
    }
  }
  return ret;
}

int ObStoragePerfRead::set_trans_desc(ObTransDesc &trans_desc)
{
  int ret = OB_SUCCESS;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 9021);
  ObTransID trans_id;
  ObStartTransParam trans_param;
  trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
  trans_param.set_type(ObTransType::TRANS_NORMAL);
  trans_param.set_isolation(ObTransIsolation::READ_COMMITED);
  int64_t snapshot_version = 2;
  if (OB_SUCCESS != (ret = trans_desc.set_trans_id(trans_id))) {
    STORAGE_LOG(WARN, "set trans_id error", K(ret));
  } else if (OB_SUCCESS != (ret = trans_desc.set_snapshot_version(snapshot_version))) {
    STORAGE_LOG(WARN, "set snapshot_version error", K(snapshot_version), K(ret));
  } else if (OB_SUCCESS != (ret = trans_desc.set_trans_param(trans_param))) {
    STORAGE_LOG(WARN, "set trans_param error", K(ret));
  } else {
    trans_desc.inc_sql_no();
  }
  return ret;
}

int ObStoragePerfRead::set_global_stat(ObStoragePerfStatistics &statics)
{
  int ret = OB_SUCCESS;
  ObDiagnoseTenantInfo *tenant_info = NULL;
  if (OB_FAIL(ObDIGlobalTenantCache::get_instance().get_all_stat_event(allocator_, tenant_dis_))) {
    STORAGE_LOG(WARN, "failed to get stat event", K(ret));
  } else {
    for (int64_t i = 0; i < tenant_dis_.count(); ++i) {
      if (tenant_dis_.at(i).first == 1) {
        tenant_info = tenant_dis_.at(i).second;
        break;
      }
    }
    if (tenant_info == NULL) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tenant_info not found", K(ret));
    } else {
      statics.row_cache_hit_ = GLOBAL_EVENT_GET(ObStatEventIds::ROW_CACHE_HIT);
      statics.row_cache_miss_ = GLOBAL_EVENT_GET(ObStatEventIds::ROW_CACHE_MISS);
      statics.block_cache_hit_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOCK_CACHE_HIT);
      statics.block_cache_miss_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOCK_CACHE_MISS);
      statics.bf_cache_hit_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOOM_FILTER_CACHE_HIT);
      statics.bf_cache_miss_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOOM_FILTER_CACHE_MISS);
      statics.io_read_count_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_COUNT);
      statics.io_read_size_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_BYTES);
      statics.io_read_delay_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_DELAY);
      statics.io_read_queue_delay_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_QUEUE_DELAY);
      statics.io_read_cb_alloc_delay_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_CB_ALLOC_DELAY);
      statics.io_read_cb_process_delay_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_CB_PROCESS_DELAY);
      statics.io_read_prefetch_micro_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
      statics.io_read_prefetch_micro_size_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES);
      statics.io_read_uncomp_micro_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_UNCOMP_MICRO_COUNT);
      statics.io_read_uncomp_micro_size_ = GLOBAL_EVENT_GET(ObStatEventIds::IO_READ_UNCOMP_MICRO_BYTES);
    }
  }

  //io_read_queue_delay -> cb_alloc_delay -> read_delay -> cb_process_delay
  if (OB_SUCC(ret)) {
    allocator_.reuse();
    tenant_dis_.reuse();
    if (OB_FAIL(ObDIGlobalTenantCache::get_instance().get_all_wait_event(allocator_, tenant_dis_))) {
      STORAGE_LOG(WARN, "failed to get stat event", K(ret));
    } else {
      for (int64_t i = 0; i < tenant_dis_.count(); ++i) {
        if (tenant_dis_.at(i).first == 1) {
          tenant_info = tenant_dis_.at(i).second;
          break;
        }
      }
      if (tenant_info == NULL) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tenant_info not found");
      } else {
        GLOBAL_WAIT_GET(ObWaitEventIds::DB_FILE_DATA_READ, statics.db_file_data_read_);
        GLOBAL_WAIT_GET(ObWaitEventIds::DB_FILE_DATA_INDEX_READ,statics.db_file_data_index_read_);
        GLOBAL_WAIT_GET(ObWaitEventIds::KV_CACHE_BUCKET_LOCK_WAIT,statics.kv_cache_bucket_lock_wait_);
        GLOBAL_WAIT_GET(ObWaitEventIds::IO_QUEUE_LOCK_WAIT, statics.io_queue_lock_wait_);
        GLOBAL_WAIT_GET(ObWaitEventIds::IO_CONTROLLER_COND_WAIT, statics.io_controller_cond_wait_);
        GLOBAL_WAIT_GET(ObWaitEventIds::IO_PROCESSOR_COND_WAIT, statics.io_processor_cond_wait_);
        allocator_.reuse();
        tenant_dis_.reuse();
      }
    }
  }
  return ret;
}

void ObStoragePerfRead::set_statistics(ObStoragePerfStatistics &statics)
{
  statics.row_cache_hit_ = EVENT_TGET(ObStatEventIds::ROW_CACHE_HIT);
  statics.row_cache_miss_ = EVENT_TGET(ObStatEventIds::ROW_CACHE_MISS);
  statics.block_cache_hit_ = EVENT_TGET(ObStatEventIds::BLOCK_CACHE_HIT);
  statics.block_cache_miss_ = EVENT_TGET(ObStatEventIds::BLOCK_CACHE_MISS);
  statics.io_read_count_ = EVENT_TGET(ObStatEventIds::IO_READ_COUNT);
  statics.io_read_size_ = EVENT_TGET(ObStatEventIds::IO_READ_BYTES);
  statics.io_read_delay_ = EVENT_TGET(ObStatEventIds::IO_READ_DELAY);
  statics.io_read_queue_delay_ = EVENT_TGET(ObStatEventIds::IO_READ_QUEUE_DELAY);
  statics.io_read_cb_alloc_delay_ = EVENT_TGET(ObStatEventIds::IO_READ_CB_ALLOC_DELAY);
  statics.io_read_cb_process_delay_ = EVENT_TGET(ObStatEventIds::IO_READ_CB_PROCESS_DELAY);
  statics.io_read_prefetch_micro_cnt_ = EVENT_TGET(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
  statics.io_read_prefetch_micro_size_ = EVENT_TGET(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES);
  statics.io_read_uncomp_micro_cnt_ = EVENT_TGET(ObStatEventIds::IO_READ_UNCOMP_MICRO_COUNT);
  statics.io_read_uncomp_micro_size_ = EVENT_TGET(ObStatEventIds::IO_READ_UNCOMP_MICRO_BYTES);
}

bool ObStoragePerfRead::is_row_cache_hit(const ObStoragePerfStatistics &statics, const QueryType type)
{
  bool ret = false;
  if(SCAN == type) {
    ret = 0 == statics.row_cache_hit_ && 0 == statics.block_cache_miss_ && 0 == statics.block_cache_hit_;
  } else {
    ret = statics.row_cache_hit_ > 0;
  }
  return ret;
}

bool ObStoragePerfRead::is_block_cache_hit(const ObStoragePerfStatistics &statics, const QueryType type)
{
  bool ret = false;
  if(SCAN == type) {
    ret = 0 == statics.row_cache_hit_ && 0 == statics.block_cache_miss_ && 1 == statics.block_cache_hit_ && 1 == statics.io_read_count_;
  } else if(MULTI_GET == type){
    ret =  0 == statics.row_cache_hit_ && 1 == statics.block_cache_hit_ && 0 == statics.io_read_delay_;
  } else {
    ret =  0 == statics.row_cache_hit_ && statics.block_cache_hit_ > 0 && 0 == statics.io_read_count_;
  }
  return ret;
}

bool ObStoragePerfRead::is_read_one_micro_block(const ObStoragePerfStatistics &statics, const QueryType type) {
  bool ret = false;
  if(SCAN == type) {
    ret = 0 == statics.row_cache_hit_ && 0 == statics.block_cache_hit_ && 1 == statics.io_read_count_ && 0 != statics.io_read_delay_;
  } else if(MULTI_GET == type){
    ret = 0 == statics.row_cache_hit_ && 1 == statics.io_read_count_ && 0 != statics.io_read_delay_;
  } else {
    ret = 0 == statics.row_cache_hit_ && 1 == statics.io_read_count_ && 0 != statics.io_read_delay_;
  }
  return ret;
}

bool ObStoragePerfRead::is_read_two_micro_block(const ObStoragePerfStatistics &statics, const QueryType type) {
  bool ret = false;
  if(SCAN == type) {
    ret = 0 == statics.row_cache_hit_ && 2 == statics.io_read_count_ && 0 != statics.io_read_delay_;
  } else if(MULTI_GET == type){
    ret = 0 == statics.row_cache_hit_ && 2 == statics.io_read_count_ && 0 != statics.io_read_delay_;
  } else {
    ret = 0 == statics.row_cache_hit_ && 2 == statics.io_read_count_ && 0 != statics.io_read_delay_;
  }
  return ret;
}

int ObStoragePerfRead::destroy()
{
  int ret = OB_SUCCESS;

  logger_.write_check_point();
  data_file_.close();
  image_.destroy();
  pmeta_.destroy();
  marker_.destroy();
  logger_.destroy();

  return ret;
}

}//end namespace storageperf
}//end namespace oceanbase
