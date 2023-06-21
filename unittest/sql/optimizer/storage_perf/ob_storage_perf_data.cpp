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

#include "lib/number/ob_number_v2.h"
#include "ob_storage_perf_data.h"
#include "ob_storage_perf_read.h"
#include "storage/ob_base_storage.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/blocksstable/ob_block_file.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

#define VARIABLE_BUF_LEN 128

#define COMPARE_VALUE(obj_get_fun, medium_type, value, exist) \
{ \
  medium_type tmp_value = 0; \
  tmp_value = obj.obj_get_fun(); \
  exist = tmp_value == static_cast<medium_type>(value) ? true : false; \
  if(!exist){ \
    STORAGE_LOG(WARN, "value is different", K(tmp_value), K(value)); \
  } \
}

#define COMPARE_NUMBER(allocator, obj_get_fun, value, exist) \
{ \
  ObNumber number; \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator->alloc(VARIABLE_BUF_LEN)))){ \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    STORAGE_LOG(WARN, "fail to alloc memory"); \
  } else { \
    sprintf(buf, "%ld", value); \
  } \
  if(OB_SUCC(ret)){ \
    if(OB_SUCCESS != (ret = number.from(buf, *allocator))){ \
      STORAGE_LOG(WARN, "fail to format num", K(ret)); \
    } else if(number != obj.obj_get_fun()){ \
      exist = false; \
      STORAGE_LOG(WARN, "row value is different", K(obj)); \
    } \
  } \
}

#define SET_VALUE(rowkey_pos, obj_set_fun, type, seed, value) \
{ \
  if (rowkey_pos > 0) { \
    obj.obj_set_fun(static_cast<type>(seed)); \
  } else { \
    obj.obj_set_fun(static_cast<type>(value)); \
  } \
}

#define SET_CHAR(allocator, rowkey_pos, obj_set_fun, seed, value) \
{ \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator->alloc(VARIABLE_BUF_LEN)))){ \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    STORAGE_LOG(WARN, "fail to alloc memory"); \
  } else if(rowkey_pos > 0){ \
    sprintf(buf, "%064ld", seed); \
  } else { \
    sprintf(buf, "%064ld", value); \
  } \
  if(OB_SUCC(ret)){ \
    ObString str; \
    str.assign_ptr(buf, static_cast<int32_t>(strlen(buf))); \
    obj.obj_set_fun(str); \
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI); \
  } \
}

#define SET_NUMBER(allocator, rowkey_pos, obj_set_fun, seed, value) \
{ \
  ObNumber number; \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator->alloc(VARIABLE_BUF_LEN)))){ \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    STORAGE_LOG(WARN, "fail to alloc memory"); \
  } else if(rowkey_pos > 0){ \
    sprintf(buf, "%064ld", seed); \
  } else { \
    sprintf(buf, "%064ld", value); \
  } \
  if(OB_SUCC(ret)){ \
    if(OB_SUCCESS != (ret = number.from(buf, *allocator))){ \
      STORAGE_LOG(WARN, "fail to format num", K(ret)); \
    } else { \
      obj.obj_set_fun(number); \
    } \
  } \
}

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace number;
using namespace memtable;
namespace storageperf
{

ObStoragePerfData::ObStoragePerfData()
  : sstable_(NULL),
    config_(NULL),
    is_inited_(false),
    update_schema_("./storage_perf_update.schema")
{
}

ObStoragePerfData::~ObStoragePerfData()
{
}

int ObStoragePerfData::init(ObStoragePerfConfig *config,
                            MockSchemaService *schema_service,
                            ObStorageCacheSuite *cache_suite,
                            ObRestoreSchema *restore_schema)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice");
  } else if (NULL == config) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is NULL");
  } else {
    config_ = config;
    schema_service_ = schema_service;
    cache_suite_ = cache_suite;
    restore_schema_ = restore_schema;
    int n1 = snprintf(data_file_path_, MAX_PATH_SIZE, "%s/%ld", config_->get_sstable_data_dir(), 1L);
    int n2 = snprintf(log_dir_path_, MAX_PATH_SIZE, "%s/%ld/", config_->get_slog_dir(), 1L);
    int n3 = snprintf(sstable_meta_path_, MAX_PATH_SIZE, "%s/%ld", config_->get_sstable_meta_dir(), 1L);
    if(n1 > MAX_PATH_SIZE || n1 < 0
        || n2 > MAX_PATH_SIZE || n2 < 0
        || n3 > MAX_PATH_SIZE || n3 < 0){
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "fail to create data file path");
    } else if(OB_FAIL(init_partition_storage())){
      STORAGE_LOG(WARN, "fail to init partition storage", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObStoragePerfData::init_data_file()
{
  int ret = OB_SUCCESS;
  ObBlockFile::FileLocation location;
  ObBlockFile::FileSpec spec;
  location.disk_no_ = 1;
  location.install_sequence_ = 1;

  int n1 = snprintf(location.path_, common::OB_MAX_FILE_NAME_LENGTH, "%s",
              data_file_path_);
  if (n1 < 0 || n1 > OB_MAX_FILE_NAME_LENGTH) {
    STORAGE_LOG(WARN, "failed to copy data path");
  } else {
    spec.macro_block_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
    spec.data_file_size_ = config_->get_partition_size() * 1024L * 1024L * 1024L;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(data_file_.open(location, &image_))) {
      STORAGE_LOG(WARN, "open data file error.", K(ret));
    } else if (OB_FAIL(image_.initialize(&data_file_, &logger_))) {
      STORAGE_LOG(WARN, "initialize macro block meta error.", K(ret));
    } else if (OB_FAIL(logger_.init(data_file_, log_dir_path_, 1024L*1024L*1024L))) {
      STORAGE_LOG(WARN, "init logger failed, ", K(ret));
    } else if (OB_FAIL(logger_.replay())) {
      STORAGE_LOG(WARN, "replay storage log failed, ", K(ret));
    }
  }
  return ret;
}

int ObStoragePerfData::init_partition_storage()
{
  int ret = OB_SUCCESS;
  //ObPartitionKey pkey(combine_id(tenant_id, pure_id), 0, 1);
  ObBaseStorage base_storage;
  meta_.table_id_ = combine_id(tenant_id, pure_id);
  meta_.index_table_count_ = 1;
  memtable_.set_version(ObVersion(2));

  if (OB_FAIL(init_data_file())) {
    STORAGE_LOG(WARN, "failed init data file", K(ret));
  } else if (OB_FAIL(ssstore_.init(&data_file_, &cp_fty_, meta_, cache_suite_, NULL))) {
    STORAGE_LOG(WARN, "failed to init ssstore", K(ret));
  } else if (OB_FAIL(ssstore_.create_new_sstable(sstable_, meta_.index_table_count_))){
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
  //} else if(OB_FAIL(storage_.init(pkey, pkey, &cp_fty_, &base_storage, schema_service_,
  //                                (transaction::ObTransService *)0x2, NULL))){
  //  STORAGE_LOG(WARN, "fail to init partitin storage", K(ret));
  //} else if(OB_FAIL(storage_.add_store(ssstore_, false))){
  //  STORAGE_LOG(WARN, "fail to add store", K(ret));
  //} else if (OB_FAIL(storage_.add_store(memtable_, false))){
  //  STORAGE_LOG(WARN, "fail to add memtable");
  //} else if (0 != config_->get_write_to_memtable_percent() && OB_FAIL(update_to_memtable())) {
  //  STORAGE_LOG(WARN, "fail to update data to memtable", K(ret));
  }
  return ret;
}

int ObStoragePerfData::backup_sstable_meta(ObSSTable &sstable)
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

int ObStoragePerfData::update_to_memtable()
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

    if(OB_FAIL(storage_.update_rows(ins_ctx, dml_param, column_ids, update_column_ids,  &insert_iter, affected_rows))) {
      STORAGE_LOG(WARN, "fail to insert row", K(ret));
      ob_print_mod_memory_usage();
    }
    ins_ctx.mem_ctx_->trans_end(true, 1);
    mem_ctx_fty.free(ins_ctx.mem_ctx_);
  }
  return ret;
}



ObRowGenerate::ObRowGenerate()
  : allocator_(ObModIds::TEST)
  , p_allocator_(NULL)
  , schema_()
  , seed_(0)
  , is_inited_(false)
  , is_reused_(false)
{

}

ObRowGenerate::~ObRowGenerate()
{

}

void ObRowGenerate::reuse()
{
  return p_allocator_->reuse();
}

int ObRowGenerate::init(const share::schema::ObTableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if(is_inited_){
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "already inited");
  } else if (!src_schema.is_valid()) {
    STORAGE_LOG(WARN, "schema is invalid.", K(src_schema));
  } else if (OB_SUCCESS != (ret = src_schema.get_column_ids(column_list_))) {
      STORAGE_LOG(WARN, "fail to get column ids.", K(ret));
  } else if (OB_SUCCESS != (ret = schema_.assign(src_schema))) {
    STORAGE_LOG(WARN, "fail to assign schema", K(ret));
  } else {
    p_allocator_ = &allocator_;
    is_inited_ = true;
    is_reused_ = true;
  }
  return ret;
}

int ObRowGenerate::init(const share::schema::ObTableSchema &src_schema, ObArenaAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if(is_inited_){
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "already inited");
  } else if (!src_schema.is_valid()) {
    STORAGE_LOG(WARN, "schema is invalid.", K(src_schema));
  } else if (OB_SUCCESS != (ret = src_schema.get_column_ids(column_list_))) {
      STORAGE_LOG(WARN, "fail to get column ids.", K(ret));
  } else if (OB_SUCCESS != (ret = schema_.assign(src_schema))) {
    STORAGE_LOG(WARN, "fail to assign schema", K(ret));
  } else {
    p_allocator_ = allocator;
    is_inited_ = true;
    is_reused_ = false;
  }
  return ret;
}

int ObRowGenerate::get_next_row(const ObStoreRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *objs = NULL;
  char *buf;
  ObStoreRow *res_row = NULL;

  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else if(NULL == (buf = static_cast<char *>(p_allocator_->alloc(sizeof(ObStoreRow))))) {
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  }

  else if(NULL == (objs = static_cast<ObObj*>(p_allocator_->alloc(sizeof(ObObj)*schema_.get_column_count())))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory");
  } else {
    res_row = new(buf)ObStoreRow;
    res_row->row_val_.cells_ = objs;
    res_row->row_val_.count_ = schema_.get_column_count();
    if (OB_SUCCESS != (ret = generate_one_row(*res_row, seed_))) {
      STORAGE_LOG(WARN, "fail to generate one row.", K(ret), K(row));
    }
  }

  if(OB_SUCC(ret)) {
    row = res_row;
    ++ seed_;
    if(is_reused_){
      p_allocator_->reuse();
    }
  }
  return ret;
}

int ObRowGenerate::get_rowkey_by_seed(const int64_t seed, common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  ObObj *objs = NULL;
  if(NULL == (objs = static_cast<ObObj*>(p_allocator_->alloc(sizeof(ObObj)*schema_.get_rowkey_column_num())))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory");
  } else {
    row.row_val_.cells_ = objs;
    row.row_val_.count_ = schema_.get_rowkey_column_num();
    if(!is_inited_){
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "should init first");
    } else if (OB_SUCCESS != (ret = generate_one_row(row, seed))) {
      STORAGE_LOG(WARN, "fail to generate one row.", K(ret), K(row));
    } else {
      rowkey.assign(objs, schema_.get_rowkey_column_num());
    }
  }
  if(is_reused_){
    p_allocator_->reuse();
  }
  return ret;
}


int ObRowGenerate::get_next_row(const int64_t seed, const ObStoreRow *&row, bool old_value)
{
  int ret = OB_SUCCESS;
  ObObj *objs = NULL;
  char *buf = NULL;
  ObStoreRow *res_row = NULL;

  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else if(NULL == (buf = static_cast<char *>(p_allocator_->alloc(sizeof(ObStoreRow))))) {
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if(NULL == (objs = static_cast<ObObj*>(p_allocator_->alloc(sizeof(ObObj)*schema_.get_column_count())))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory");
  } else {
    res_row = new(buf)ObStoreRow;
    res_row->row_val_.cells_ = objs;
    res_row->row_val_.count_ = schema_.get_column_count();
    if (OB_SUCCESS != (ret = generate_one_row(*res_row, seed, old_value))) {
      STORAGE_LOG(WARN, "fail to generate one row.", K(ret), K(row));
    }
  }
  if(OB_SUCC(ret)) {
    row = res_row;
  }

  if(OB_SUCCESS == ret && is_reused_){
    p_allocator_->reuse();
  }
  return ret;
}

int ObRowGenerate::generate_one_row(ObStoreRow &row, int64_t seed, bool old_value)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else if (schema_.get_rowkey_column_num() > row.row_val_.count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.",
        K(schema_.get_column_count()), K(row.row_val_.count_));
  } else {
    row.flag_.set_flag(ObDmlFlag::DF_INSERT);
    row.reset_dml();
    for (int64_t i = 0; OB_SUCC(ret) && i < row.row_val_.count_; ++i) {
      const uint64_t column_id = column_list_.at(i).col_id_;
      ObObjType column_type = column_list_.at(i).col_type_.get_type();
      if (OB_SUCCESS != (ret = set_obj(column_type, column_id, seed, row.row_val_.cells_[i], old_value))) {
        STORAGE_LOG(WARN, "fail to set obj.", K(ret), K(i), K(seed));
      } else {
        if (ObVarcharType == column_type || ObCharType == column_type || ObHexStringType == column_type) {
        } else if (ObRawType == column_type) {
           row.row_val_.cells_[i].set_collation_type(CS_TYPE_BINARY);
           row.row_val_.cells_[i].set_collation_level(CS_LEVEL_IMPLICIT);
        } else if (ObNullType == column_type) {
           row.row_val_.cells_[i].set_collation_type(CS_TYPE_BINARY);
           row.row_val_.cells_[i].set_collation_level(CS_LEVEL_IGNORABLE);
        } else {
           row.row_val_.cells_[i].set_collation_type(CS_TYPE_BINARY);
           row.row_val_.cells_[i].set_collation_level(CS_LEVEL_NUMERIC);
        }
      }
    }
  }
  return ret;
}

int ObRowGenerate::set_obj(const ObObjType &column_type,
                           const uint64_t column_id,
                           const int64_t seed,
                           ObObj &obj,
                           bool old_value)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    const int64_t rowkey_pos = schema_.get_column_schema(column_id)->get_rowkey_position();
    int64_t value = 0;
    if(old_value) {
      value = seed * column_type + column_id;
    } else {
      value = seed * column_type + column_id + 1;
    }
    int64_t tmp_seed = seed;
    switch(column_type) {
    case ObNullType:
      if(rowkey_pos > 0){
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "ObNULLType should not be rowkey column");
      } else {
        obj.set_null();
      }
      break;
    case ObTinyIntType:
      SET_VALUE(rowkey_pos, set_tinyint, int8_t, seed, value);
      break;
    case ObSmallIntType:
      SET_VALUE(rowkey_pos, set_smallint, int16_t, seed, value);
      break;
    case ObMediumIntType:{
      if(value < INT24_MIN){
        value = INT24_MIN;
      } else if(value > INT24_MAX){
        value = INT24_MAX;
      }
      if(seed < INT24_MIN){
        tmp_seed = INT24_MIN;
      } else if(seed > INT24_MAX){
        tmp_seed = INT24_MAX;
      }
      SET_VALUE(rowkey_pos, set_mediumint, int32_t, tmp_seed, value);
      break;
    }
    case ObInt32Type:
      SET_VALUE(rowkey_pos, set_int32, int32_t, seed, value);
      break;
    case ObIntType:
      SET_VALUE(rowkey_pos, set_int, int64_t, seed, value);
      break;
    case ObUTinyIntType:
      SET_VALUE(rowkey_pos, set_utinyint, uint8_t, seed, value);
      break;
    case ObUSmallIntType:
      SET_VALUE(rowkey_pos, set_usmallint, uint16_t, seed, value);
      break;
    case ObUMediumIntType:{
      if(value > UINT24_MAX){
        value = UINT24_MAX;
      }
      if(seed > UINT24_MAX){
        tmp_seed = UINT24_MAX;
      }
      SET_VALUE(rowkey_pos, set_umediumint, uint32_t, tmp_seed, value);
      break;
    }
    case ObUInt32Type:
      SET_VALUE(rowkey_pos, set_uint32, uint32_t, seed, value);
      break;
    case ObUInt64Type:
      SET_VALUE(rowkey_pos, set_uint64, uint64_t, seed, value);
      break;
    case ObFloatType:
      SET_VALUE(rowkey_pos, set_float, float, seed, value);
      break;
    case ObUFloatType:
      SET_VALUE(rowkey_pos, set_ufloat, float, seed, value);
      break;
    case ObDoubleType:
      SET_VALUE(rowkey_pos, set_double, double, seed, value);
      break;
    case ObUDoubleType:
      SET_VALUE(rowkey_pos, set_udouble, double, seed, value);
      break;
    case ObNumberType: {
      SET_NUMBER(p_allocator_, rowkey_pos, set_number, seed, value);
      break;
    }
    case ObUNumberType: {
      SET_NUMBER(p_allocator_, rowkey_pos, set_unumber, seed, value);
      break;
    }
    case ObDateType:
      SET_VALUE(rowkey_pos, set_date, int32_t, seed, value);
      break;
    case ObDateTimeType:
      SET_VALUE(rowkey_pos, set_datetime, int64_t, seed, value);
      break;
    case ObTimestampType:
      SET_VALUE(rowkey_pos, set_timestamp, int64_t, seed, value);
      break;
    case ObTimeType:
      SET_VALUE(rowkey_pos, set_time, int64_t, seed, value);
      break;
    case ObYearType:
      SET_VALUE(rowkey_pos, set_year, uint8_t, seed, value);
      break;
    case ObVarcharType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_varchar, seed, value);
      break;
    }
    case ObCharType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_char, seed, value);
      break;
    }
    case ObRawType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_raw, seed, value);
      break;
    }
    case ObHexStringType: {
      char *buf = NULL;
      if(NULL == (buf = reinterpret_cast<char*>(p_allocator_->alloc(VARIABLE_BUF_LEN)))){
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory");
      } else if(rowkey_pos > 0){
        sprintf(buf, "%064ld", seed);
      } else {
        sprintf(buf, "%064ld", value);
      }
      if(OB_SUCC(ret)){
        ObString str;
        str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
        obj.set_hex_string(str);
      }
      break;
    }
    case ObExtendType:
      if(rowkey_pos > 0){
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "ObExtendType should not be rowkey column");
      } else {
        obj.set_nop_value();
      }
      break;
    default:
      STORAGE_LOG(WARN, "not support this data type.", K(column_type));
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObRowGenerate::check_one_row(const ObStoreRow& row, bool &exist)
{
  int ret = OB_SUCCESS;
  int64_t seed = -1;

  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    //first get seed from rowkey
    ObObjType column_type;
    int64_t pos = -1;
    uint64_t column_id = 0;
    ret = OB_ENTRY_NOT_EXIST;
    for(int64_t i = 0; i < column_list_.count(); ++ i){
      column_id = column_list_.at(i).col_id_;
      if(schema_.get_column_schema(column_id)->get_rowkey_position() > 0){
        column_type = schema_.get_column_schema(column_id)->get_data_type();
        pos = i;
        ret = OB_SUCCESS;
        break;
      }
    }
    if(OB_SUCC(ret)){
      ObObj obj = row.row_val_.cells_[pos];
      if(OB_SUCCESS != (ret = get_seed(column_type, obj, seed))){
        STORAGE_LOG(WARN, "fail to get seed.", K(ret));
      }
    }
  }
  //second compare the value
  if(OB_SUCC(ret)){
    ObObjType column_type;
    int64_t value = 0;
    uint64_t column_id = 0;
    for(int64_t i = 0; i < column_list_.count(); ++ i){
      column_id = column_list_.at(i).col_id_;
      if(0 == schema_.get_column_schema(column_id)->get_rowkey_position()){
        column_type = schema_.get_column_schema(column_id)->get_data_type();
        value = seed * static_cast<uint8_t>(column_type) + column_id;
        if(OB_SUCCESS != (ret = compare_obj(column_type, value, row.row_val_.cells_[i], exist))){
          STORAGE_LOG(WARN, "compare obobj error", K(ret));
        }
        if(!exist){
          break;
        }
      }
    }
  }
  return ret;
}

int ObRowGenerate::compare_obj(const ObObjType &column_type, const int64_t value, const ObObj obj, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = true;

  switch(column_type) {
  case ObNullType:
    break;
  case ObTinyIntType:
    COMPARE_VALUE(get_tinyint, int8_t, value, exist);
    break;
  case ObSmallIntType:
    COMPARE_VALUE(get_smallint, int16_t, value, exist);
    break;
  case ObMediumIntType:
    COMPARE_VALUE(get_mediumint, int32_t, value, exist);
    break;
  case ObInt32Type:
    COMPARE_VALUE(get_int32, int32_t, value, exist);
    break;
  case ObIntType:
    COMPARE_VALUE(get_int, int64_t, value, exist);
    break;
  case ObUTinyIntType:
    COMPARE_VALUE(get_utinyint, uint8_t, value, exist);
    break;
  case ObUSmallIntType:
    COMPARE_VALUE(get_usmallint, uint16_t, value, exist);
    break;
  case ObUMediumIntType:
    COMPARE_VALUE(get_umediumint, uint32_t, value, exist);
    break;
  case ObUInt32Type:
    COMPARE_VALUE(get_uint32, uint32_t, value, exist);
    break;
  case ObUInt64Type:
    COMPARE_VALUE(get_uint64, uint64_t, value, exist);
    break;
  case ObFloatType:
    COMPARE_VALUE(get_float, float, value, exist);
    break;
  case ObUFloatType:
    COMPARE_VALUE(get_ufloat, float, value, exist);
    break;
  case ObDoubleType:
    COMPARE_VALUE(get_double, double, value, exist);
    break;
  case ObUDoubleType:
    COMPARE_VALUE(get_udouble, double, value, exist);
    break;
  case ObNumberType:
    COMPARE_NUMBER(p_allocator_, get_number, value, exist);
    break;
  case ObUNumberType:
    COMPARE_NUMBER(p_allocator_, get_unumber, value, exist);
    break;
  case ObDateType:
    COMPARE_VALUE(get_date, int32_t, value, exist);
    break;
  case ObDateTimeType:
    COMPARE_VALUE(get_datetime, int64_t, value, exist);
    break;
  case ObTimestampType:
    COMPARE_VALUE(get_timestamp, int64_t, value, exist);
    break;
  case ObTimeType:
    COMPARE_VALUE(get_time, int64_t, value, exist);
    break;
  case ObYearType:
    COMPARE_VALUE(get_year, uint8_t, value, exist);
    break;
  case ObVarcharType:
  case ObCharType:
  case ObRawType:
  {
    char *buf = NULL;
    if(NULL == (buf = reinterpret_cast<char*>(p_allocator_->alloc(VARIABLE_BUF_LEN)))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory");
    } else {
      sprintf(buf, "%064ld", value);
    }
    if(OB_SUCC(ret)){
      ObString str;
      str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
      if(str != obj.get_string()){
        exist = false;
        STORAGE_LOG(WARN, "row value is different", K(str), K(obj));
      }
    }
    break;
  }
  case ObHexStringType:
  {
    char *buf = NULL;
    if(NULL == (buf = reinterpret_cast<char*>(p_allocator_->alloc(VARIABLE_BUF_LEN)))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory");
    } else {
      sprintf(buf, "%064ld", value);//not change this
    }
    if(OB_SUCC(ret)){
      ObString str;
      str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
      if(str != obj.get_string()){
        exist = false;
        STORAGE_LOG(WARN, "row value is different", K(str), K(obj));
      }
    }
    break;
  }
  case ObExtendType:
    //just check value to OP_NOP
    if(obj.get_ext() != ObActionFlag::OP_NOP){
      exist = false;
      STORAGE_LOG(WARN, "row value is different", K(obj.get_ext()), K(static_cast<int64_t>(value)));
    }
    break;
  default:
    STORAGE_LOG(WARN, "don't support this data type.", K(column_type));
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObRowGenerate::get_seed(const ObObjType &column_type, const ObObj obj, int64_t &seed)
{
  int ret = OB_SUCCESS;
  switch(column_type) {
  case ObTinyIntType:
    seed = static_cast<int64_t>(obj.get_tinyint());
    break;
  case ObSmallIntType:
    seed = static_cast<int64_t>(obj.get_smallint());
    break;
  case ObMediumIntType:
    seed = static_cast<int64_t>(obj.get_mediumint());
    break;
  case ObInt32Type:
    seed = static_cast<int64_t>(obj.get_int32());
    break;
  case ObIntType:
    seed = obj.get_int();
    break;
  case ObUTinyIntType:
    seed = static_cast<int64_t>(obj.get_utinyint());
    break;
  case ObUSmallIntType:
    seed = static_cast<int64_t>(obj.get_usmallint());
    break;
  case ObUMediumIntType:
    seed = static_cast<int64_t>(obj.get_umediumint());
    break;
  case ObUInt32Type:
    seed = static_cast<int64_t>(obj.get_uint32());
    break;
  case ObUInt64Type:
    seed = static_cast<int64_t>(obj.get_uint64());
    break;
  case ObFloatType:
  case ObUFloatType:
    seed = static_cast<int64_t>(obj.get_float());
    break;
  case ObDoubleType:
  case ObUDoubleType:
    seed = static_cast<int64_t>(obj.get_double());
    break;
  case ObNumberType: {
    const char *value = obj.get_number().format();
    seed = static_cast<int64_t>(strtoll(value, NULL, 10));
    break;
  }
  case ObUNumberType: {
    const char *value = obj.get_unumber().format();
    seed = static_cast<int64_t>(strtoll(value, NULL, 10));
    break;
  }
  case ObDateType:
    seed = static_cast<int64_t>(obj.get_date());
    break;
  case ObDateTimeType:
    seed = static_cast<int64_t>(obj.get_datetime());
    break;
  case ObTimestampType:
    seed = static_cast<int64_t>(obj.get_timestamp());
    break;
  case ObTimeType:
    seed = static_cast<int64_t>(obj.get_time());
    break;
  case ObYearType:
    seed = static_cast<int64_t>(obj.get_year());
    break;
  case ObVarcharType:
  case ObCharType:
  case ObHexStringType:
  case ObRawType:
    seed = static_cast<int64_t>(strtoll(obj.get_string().ptr(), NULL, 10));
    break;
  case ObExtendType:
  default:
    STORAGE_LOG(WARN, "don't support this data type.", K(column_type));
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

}//blocksstable
}//oceanbase
