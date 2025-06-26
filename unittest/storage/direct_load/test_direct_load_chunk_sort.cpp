/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "../unittest/storage/blocksstable/ob_data_file_prepare.h"
#include "../unittest/storage/blocksstable/ob_row_generate.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "share/ob_order_perserving_encoder.h"
#include "storage/direct_load/ob_direct_load_compare.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"
#include "storage/direct_load/ob_direct_load_mem_chunk.h"

namespace oceanbase
{
using namespace storage;
using namespace share::schema;
using namespace sql;
namespace unittest
{

static ObSimpleMemLimitGetter getter;
class RandomBase
{
public:
  virtual int64_t generate() = 0;
};

class Sequence : public RandomBase
{
public:
  Sequence(int64_t start) : start_(start) {}
  int64_t generate() override {return start_++;}
private:
  int64_t start_;
};

class Random : public RandomBase
{
public:
  Random(int64_t min, int64_t max) : min_(min), max_(max) {}
  int64_t generate() override {return ObRandom::rand(min_, max_);}
private:
  int64_t min_;
  int64_t max_;
};

class Unique128 : public RandomBase
{
public:
  Unique128() {}
  int64_t generate() override {return ObRandom::rand(0, 127);}
};

class PowerLay : public RandomBase
{
public:
  PowerLay(double alpha = 5, int64_t x_min = 1) : alpha_(alpha), x_min_(x_min) {}
  int64_t generate() override
  {
    double u = ObRandom::rand(0, 127) / 128.0;
    int64_t random_integer = static_cast<int64_t>(x_min_ * std::pow(1.0 - u, -1.0 / (alpha_ - 1.0)));
    return random_integer;
  }

private:
    double alpha_;
    int64_t x_min_;
};

class RowGenerate
{
public:
  RowGenerate() : rand_(nullptr), rowkey_column_count_(0) {}
  ~RowGenerate() {}
  int init(const share::schema::ObTableSchema &src_schema, RandomBase *rand, bool is_multi_version_row = false)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(rand_ = rand)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", KR(ret));
    } else if (OB_FAIL(row_generate_.init(src_schema, is_multi_version_row))) {
      STORAGE_LOG(WARN, "fail to init row generate", KR(ret));
    } else if (direct_load_datum_row_.init(src_schema.get_column_count(), &allocator_)) {
      STORAGE_LOG(WARN, "fail to init datum row", KR(ret));
    } else {
      rowkey_column_count_ = src_schema.get_rowkey_column_num();
      direct_load_datum_row_.seq_no_ = 0;
      external_row_.tablet_id_ = 1;
    }
    return ret;
  }
  int generate_row(ObDirectLoadConstExternalMultiPartitionRow &const_row)
  {
    int ret = OB_SUCCESS;
    datum_row_.storage_datums_ = direct_load_datum_row_.storage_datums_;
    datum_row_.count_ = direct_load_datum_row_.count_;
    if (OB_FAIL(row_generate_.get_next_row(rand_->generate(), datum_row_))) {
      STORAGE_LOG(WARN, "fail to generate row", KR(ret));
    } else {
      direct_load_datum_row_.seq_no_++;
      if (OB_FAIL(external_row_.external_row_.from_datum_row(direct_load_datum_row_, rowkey_column_count_))) {
        STORAGE_LOG(WARN, "fail to from datum row", KR(ret));
      } else {
        const_row = external_row_;
      }
    }
    return ret;
  }
private:
  ObArenaAllocator allocator_;
  RandomBase *rand_;
  ObRowGenerate row_generate_;
  int64_t rowkey_column_count_;
  ObDirectLoadDatumRow direct_load_datum_row_;
  ObDatumRow datum_row_;
  ObDirectLoadExternalMultiPartitionRow external_row_;
};

class TestChunkSort : public ::testing::Test
{
public:
  static int prepare_schecma(int64_t rowkey_count, int64_t column_num, int64_t str_rowkey_count,
                             int64_t int_rowkey_count, ObTableSchema &table_schema);

  static int prepare_utils(const ObTableSchema &table_schema, ObArenaAllocator &allocator,
                           ObStorageDatumUtils &datum_util, ObArray<share::ObEncParam> &enc_params,
                           ObDirectLoadExternalMultiPartitionRowCompare &compare);
  static int sort_test(int64_t test_row_num, RandomBase *rand, ObTableSchema &table_schema,
                       ObArray<share::ObEncParam> &enc_params,
                       ObDirectLoadExternalMultiPartitionRowCompare &compare,
                       ObArenaAllocator &allocator, int64_t same_rowkey_num = 0);

private:
  static int init_table_id_enc_params(const ObTableSchema &table_schema,
                                      ObArray<share::ObEncParam> &enc_params);
  static int init_primary_key_enc_param(const ObTableSchema &table_schema,
                                        ObArray<share::ObEncParam> &enc_params);
  static int init_seq_no_enc_param(const ObTableSchema &table_schema,
                                   ObArray<share::ObEncParam> &enc_params);

public:
  typedef ObDirectLoadMemChunk<ObDirectLoadConstExternalMultiPartitionRow,
                             ObDirectLoadExternalMultiPartitionRowCompare>
  ChunkType;

public:
  TestChunkSort() {}
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }
  int init_tenant_mgr()
  {
    int ret = OB_SUCCESS;
    ObAddr self;
    obrpc::ObSrvRpcProxy rpc_proxy;
    obrpc::ObCommonRpcProxy rs_rpc_proxy;
    share::ObRsMgr rs_mgr;
    self.set_ip_addr("127.0.0.1", 8086);
    rpc::frame::ObReqTransport req_transport(NULL, NULL);
    const int64_t ulmt = 128LL << 30;
    const int64_t llmt = 128LL << 30;
    ret = getter.add_tenant(OB_SYS_TENANT_ID, ulmt, llmt);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = getter.add_tenant(OB_SERVER_TENANT_ID, ulmt, llmt);
    EXPECT_EQ(OB_SUCCESS, ret);
    lib::set_memory_limit(128LL << 32);
    return ret;
  }
public:
  ObArenaAllocator allocator_;
};

int TestChunkSort::prepare_schecma(int64_t rowkey_count, int64_t column_num, int64_t str_rowkey_count,
                             int64_t int_rowkey_count, ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 column;
  int64_t table_id = 3001;
  // init table schema
  table_schema.reset();
  if (rowkey_count != (str_rowkey_count + int_rowkey_count)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(rowkey_count), K(str_rowkey_count), K(int_rowkey_count));
  } else  if (OB_FAIL((table_schema.set_table_name("test_adaptive_aqs_sort")))) {
    STORAGE_LOG(WARN, "fail to set table name", KR(ret));
  } else {
    table_schema.set_tenant_id(1);
    table_schema.set_tablegroup_id(1);
    table_schema.set_database_id(1);
    table_schema.set_table_id(table_id);
    table_schema.set_tablet_id(1);
    table_schema.set_rowkey_column_num(rowkey_count);
    table_schema.set_max_used_column_id(column_num);
  }

  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  //init rowkey
  int64_t str_rowkey_count_left = str_rowkey_count;
  int64_t int_rowkey_count_left = int_rowkey_count;
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_count; ++i) {
    if (ObRandom::rand(0, str_rowkey_count_left + int_rowkey_count_left -1) < str_rowkey_count_left) {
      str_rowkey_count_left--;
      ObObjType obj_type = ObVarcharType;
      column.reset();
      column.set_table_id(table_id);
      column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
      sprintf(name, "test%020ld", i);
      column.set_data_type(obj_type);
      column.set_data_length(32);
      column.set_rowkey_position(i + 1);
      column.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
      if (OB_FAIL(column.set_column_name(name))) {
        STORAGE_LOG(WARN, "fail to set column name", KR(ret));
      } else if (OB_FAIL(table_schema.add_column(column))) {
        STORAGE_LOG(WARN, "fail to add column", KR(ret));
      }
    } else {
      int_rowkey_count_left--;
      ObObjType obj_type = ObIntType;
      column.reset();
      column.set_table_id(table_id);
      column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
      sprintf(name, "test%020ld", i);
      column.set_data_type(obj_type);
      column.set_rowkey_position(i + 1);
      column.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
      if (OB_FAIL(column.set_column_name(name))) {
        STORAGE_LOG(WARN, "fail to set column name", KR(ret));
      } else if (OB_FAIL(table_schema.add_column(column))) {
        STORAGE_LOG(WARN, "fail to add column", KR(ret));
      }
    }
  }
  //int normal key
  for (int64_t i = rowkey_count; OB_SUCC(ret) && i < column_num; ++i) {
    ObObjType obj_type = ObVarcharType;
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    column.set_data_type(obj_type);
    column.set_data_length(32);
    column.set_rowkey_position(0);
    column.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    if (OB_FAIL(column.set_column_name(name))) {
      STORAGE_LOG(WARN, "fail to set column name", KR(ret));
    } else if (OB_FAIL(table_schema.add_column(column))) {
      STORAGE_LOG(WARN, "fail to add column", KR(ret));
    }
  }
  return ret;
}

int TestChunkSort::init_table_id_enc_params(const ObTableSchema &table_schema, ObArray<share::ObEncParam> &enc_params)
{
  int ret = OB_SUCCESS;
  share::ObEncParam param;
  param.type_ = ObUInt64Type;
  param.cs_type_ = CS_TYPE_BINARY;
  param.is_var_len_ = false;
  param.is_memcmp_ = false;
  param.is_nullable_ = true; // unused
#if OB_USE_MULTITARGET_CODE
  int tmp_ret = OB_SUCCESS;
  tmp_ret = OB_E(EventTable::EN_DISABLE_ENCODESORTKEY_OPT) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    param.is_simdopt_ = false;
  }
#else
  param.is_simdopt_ = false;
#endif
  int64_t odr = 0;
  int64_t np = 0; // o or 1 is all ok
  // null pos: null first -> 0, nulls last -> 1
  // order: asc -> 0, desc -> 1
  param.is_null_first_ = (np == 0);
  param.is_asc_ = (odr == 0);
  if (OB_FAIL(enc_params.push_back(param))) {
    STORAGE_LOG(WARN, "fail to push back enc param", KR(ret));
  }
  return ret;
}

int TestChunkSort::init_primary_key_enc_param(const ObTableSchema &table_schema, ObArray<share::ObEncParam> &enc_params)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> col_descs;
  if (OB_FAIL(table_schema.get_column_ids(col_descs))) {
    STORAGE_LOG(WARN, "fail to get column ids", KR(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < table_schema.get_rowkey_column_num(); i++) {
    share::ObEncParam param;
    param.type_ = col_descs[i].col_type_.get_type();
    param.cs_type_ = col_descs[i].col_type_.get_collation_type();
    param.is_var_len_ = true;
    param.is_memcmp_ = false;
    param.is_nullable_ = true; // unused
#if OB_USE_MULTITARGET_CODE
    int tmp_ret = OB_SUCCESS;
    tmp_ret = OB_E(EventTable::EN_DISABLE_ENCODESORTKEY_OPT) OB_SUCCESS;
    if (OB_SUCCESS != tmp_ret) {
      param.is_simdopt_ = false;
    }
#else
    param.is_simdopt_ = false;
#endif
    int64_t odr = col_descs[i].col_order_;
    int64_t np = 0; // o or 1 is all ok
    // null pos: null first -> 0, nulls last -> 1
    // order: asc -> 0, desc -> 1
    if (odr != 0 && odr != 1) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", KR(ret), K(odr));
    } else if (np != 0 && np != 1) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", KR(ret), K(np));
    } else {
      param.is_null_first_ = (np == 0);
      param.is_asc_ = (odr == 0);
      if (OB_FAIL(enc_params.push_back(param))) {
        STORAGE_LOG(WARN, "fail to push back enc param", KR(ret));
      }
    }
  }
  return ret;
}

int TestChunkSort::init_seq_no_enc_param(const ObTableSchema &table_schema, ObArray<share::ObEncParam> &enc_params)
{
  int ret = OB_SUCCESS;
  share::ObEncParam param;
  param.type_ = ObUInt64Type;
  param.cs_type_ = CS_TYPE_BINARY;
  param.is_var_len_ = false;
  param.is_memcmp_ = false;
  param.is_nullable_ = true; // unused
#if OB_USE_MULTITARGET_CODE
  int tmp_ret = OB_SUCCESS;
  tmp_ret = OB_E(EventTable::EN_DISABLE_ENCODESORTKEY_OPT) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    param.is_simdopt_ = false;
  }
#else
  param.is_simdopt_ = false;
#endif
  int64_t odr = 0;
  int64_t np = 0; // o or 1 is all ok
  // null pos: null first -> 0, nulls last -> 1
  // order: asc -> 0, desc -> 1
  param.is_null_first_ = (np == 0);
  param.is_asc_ = (odr == 0);
  if (OB_FAIL(enc_params.push_back(param))) {
    STORAGE_LOG(WARN, "fail to push back enc param", KR(ret));
  }
  return ret;
}

int TestChunkSort::prepare_utils(const ObTableSchema &table_schema,
                                       ObArenaAllocator &allocator, ObStorageDatumUtils &datum_util,
                                       ObArray<share::ObEncParam> &enc_params,
                                       ObDirectLoadExternalMultiPartitionRowCompare &compare)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> col_descs;
  sql::ObLoadDupActionType dup_action = sql::ObLoadDupActionType::LOAD_REPLACE;
  bool ignore_seq_no = false;
  //prepare util
  if (OB_FAIL(table_schema.get_column_ids(col_descs))) {
    STORAGE_LOG(WARN, "fail to get column ids", KR(ret));
  } else if (OB_FAIL(datum_util.init(col_descs, table_schema.get_rowkey_column_num(), false, allocator))) {
    STORAGE_LOG(WARN, "fail to init datum util", KR(ret));
  //prepare compare
  } else if (OB_FAIL(compare.init(datum_util, dup_action, ignore_seq_no))) {
    STORAGE_LOG(WARN, "fail to init compare", KR(ret));
  //prepare enc_params
  } else if (OB_FAIL(init_table_id_enc_params(table_schema, enc_params))) {
    STORAGE_LOG(WARN, "fail to init enc params", KR(ret));
  } else if (OB_FAIL(init_primary_key_enc_param(table_schema, enc_params))) {
    STORAGE_LOG(WARN, "fail to init enc params", KR(ret));
  } else if (OB_FAIL(init_seq_no_enc_param(table_schema, enc_params))) {
    STORAGE_LOG(WARN, "fail to init enc params", KR(ret));
  }
  return ret;
}

void TestChunkSort::SetUp()
{
  int ret = OB_SUCCESS;
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  // init file
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, getter.add_tenant(1, 8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L));
  if (OB_FAIL(ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size))) {
    ASSERT_EQ(OB_INIT_TWICE, ret);
    ret = OB_SUCCESS;
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, init_tenant_mgr());
  ASSERT_EQ(OB_SUCCESS, common::ObClockGenerator::init());

  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTimerService *timer_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_new(timer_service));
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_start(timer_service));
  tenant_ctx.set(timer_service);

  ObTenantEnv::set_tenant(&tenant_ctx);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestChunkSort::TearDown()
{
  common::ObClockGenerator::destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObTimerService *timer_service = MTL(ObTimerService *);
  ASSERT_NE(nullptr, timer_service);
  timer_service->stop();
  timer_service->wait();
  timer_service->destroy();
}

int TestChunkSort::sort_test(int64_t test_row_num, RandomBase *rand,
                                   ObTableSchema &table_schema,
                                   ObArray<share::ObEncParam> &enc_params,
                                   ObDirectLoadExternalMultiPartitionRowCompare &compare,
                                   ObArenaAllocator &allocator,
                                   int64_t same_rowkey_num)
{
  int ret = OB_SUCCESS;
  RowGenerate generate;
  if (OB_FAIL(generate.init(table_schema, rand))) {
    STORAGE_LOG(WARN, "fail to init row generate", KR(ret));
  } else {
    ChunkType chunk;
    if (OB_FAIL(chunk.init(table_schema.get_tenant_id(), 512 * 1024LL * 1024LL))) {
      STORAGE_LOG(WARN, "fail to init chunk", KR(ret));
    } else {
      //prepare same row key prefix
      ObDirectLoadConstExternalMultiPartitionRow const_row;
      ObDirectLoadConstExternalMultiPartitionRow *prefix_row = nullptr;
      if (OB_FAIL(generate.generate_row(const_row))) {
        STORAGE_LOG(WARN, "fail to generate row", KR(ret));
      } else {
        const int64_t item_size =
          sizeof(ObDirectLoadConstExternalMultiPartitionRow) + const_row.get_deep_copy_size();
        char *buf = nullptr;
        if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(item_size)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to allocate memory", KR(ret), K(item_size));
        } else {
          prefix_row = new (buf) ObDirectLoadConstExternalMultiPartitionRow();
          int64_t buf_pos = sizeof(ObDirectLoadConstExternalMultiPartitionRow);
          if (OB_FAIL(prefix_row->deep_copy(const_row, buf, item_size, buf_pos))) {
            STORAGE_LOG(WARN, "fail to deep copy row", KR(ret));
          }
        }
      }

      for (int i = 0; OB_SUCC(ret) && i < test_row_num; i++) {
        if (OB_FAIL(generate.generate_row(const_row))) {
          STORAGE_LOG(WARN, "fail to generate row", KR(ret));
        } else {
          for (int64_t k = 0; k < same_rowkey_num; k++) {
            const_row.rowkey_datum_array_.datums_[k] = prefix_row->rowkey_datum_array_.datums_[k];
          }
          if (OB_FAIL(chunk.add_item(const_row))) {
            if (OB_BUF_NOT_ENOUGH == ret) {
              ret = OB_SUCCESS;
              if (OB_FAIL(chunk.sort(compare, enc_params))) {
                STORAGE_LOG(WARN, "fail to sort chunk", KR(ret));
              } else if (FALSE_IT(chunk.reuse())) {
              } else if (OB_FAIL(chunk.add_item(const_row))) {
                STORAGE_LOG(WARN, "fail to add item", KR(ret));
              }
            }
          }
        }
      }
      if (OB_SUCCESS == ret && chunk.get_size() > 0) {
          if (OB_FAIL(chunk.sort(compare, enc_params))) {
            STORAGE_LOG(WARN, "fail to sort chunk", KR(ret));
          }
      }
    }
  }
  return ret;
}

TEST_F(TestChunkSort, test_seq)
{
  const int64_t test_row_num = 1000000;
  int64_t rowkey_count = 4;
  int64_t column_num = 4;
  int64_t str_rowkey_count = 4;
  int64_t int_rowkey_count = 0;
  Sequence rand(0);

  ObTableSchema table_schema;
  ObArenaAllocator allocator;
  ObStorageDatumUtils datum_util;
  ObArray<share::ObEncParam> enc_params;
  ObDirectLoadExternalMultiPartitionRowCompare compare;
  ASSERT_EQ(OB_SUCCESS, prepare_schecma(rowkey_count, column_num, str_rowkey_count,
                                        int_rowkey_count, table_schema));
  ASSERT_EQ(OB_SUCCESS, prepare_utils(table_schema, allocator, datum_util, enc_params, compare));
  ASSERT_EQ(OB_SUCCESS, sort_test(test_row_num, &rand, table_schema, enc_params, compare, allocator));
}

TEST_F(TestChunkSort, test_random)
{
  const int64_t test_row_num = 1000000;
  int64_t rowkey_count = 4;
  int64_t column_num = 4;
  int64_t str_rowkey_count = 4;
  int64_t int_rowkey_count = 0;
  Random rand = Random(0, test_row_num-1);
  ObTableSchema table_schema;
  ObArenaAllocator allocator;
  ObStorageDatumUtils datum_util;
  ObArray<share::ObEncParam> enc_params;
  ObDirectLoadExternalMultiPartitionRowCompare compare;
  ASSERT_EQ(OB_SUCCESS, prepare_schecma(rowkey_count, column_num, str_rowkey_count,
                                        int_rowkey_count, table_schema));
  ASSERT_EQ(OB_SUCCESS, prepare_utils(table_schema, allocator, datum_util, enc_params, compare));
  ASSERT_EQ(OB_SUCCESS, sort_test(test_row_num, &rand, table_schema, enc_params, compare, allocator));
}

TEST_F(TestChunkSort, test_unique128)
{
  const int64_t test_row_num = 1000000;
  int64_t rowkey_count = 4;
  int64_t column_num = 4;
  int64_t str_rowkey_count = 4;
  int64_t int_rowkey_count = 0;
  Unique128 rand;
  ObTableSchema table_schema;
  ObArenaAllocator allocator;
  ObStorageDatumUtils datum_util;
  ObArray<share::ObEncParam> enc_params;
  ObDirectLoadExternalMultiPartitionRowCompare compare;
  ASSERT_EQ(OB_SUCCESS, prepare_schecma(rowkey_count, column_num, str_rowkey_count,
                                        int_rowkey_count, table_schema));
  ASSERT_EQ(OB_SUCCESS, prepare_utils(table_schema, allocator, datum_util, enc_params, compare));
  ASSERT_EQ(OB_SUCCESS, sort_test(test_row_num, &rand, table_schema, enc_params, compare, allocator));
}

TEST_F(TestChunkSort, test_rowlay)
{
  const int64_t test_row_num = 1000000;
  PowerLay rand;
  int64_t rowkey_count = 4;
  int64_t column_num = 4;
  int64_t str_rowkey_count = 4;
  int64_t int_rowkey_count = 0;
  ObTableSchema table_schema;
  ObArenaAllocator allocator;
  ObStorageDatumUtils datum_util;
  ObArray<share::ObEncParam> enc_params;
  ObDirectLoadExternalMultiPartitionRowCompare compare;
  ASSERT_EQ(OB_SUCCESS, prepare_schecma(rowkey_count, column_num, str_rowkey_count,
                                        int_rowkey_count, table_schema));
  ASSERT_EQ(OB_SUCCESS, prepare_utils(table_schema, allocator, datum_util, enc_params, compare));
  ASSERT_EQ(OB_SUCCESS, sort_test(test_row_num, &rand, table_schema, enc_params, compare, allocator));
}

TEST_F(TestChunkSort, test_random_less_rowkey)
{
  //The number of rowkeys is one
  const int64_t test_row_num = 1000000;
  int64_t rowkey_count = 1;
  int64_t column_num = 4;
  int64_t str_rowkey_count = 1;
  int64_t int_rowkey_count = 0;
  Random rand = Random(0, test_row_num-1);
  ObTableSchema table_schema;
  ObArenaAllocator allocator;
  ObStorageDatumUtils datum_util;
  ObArray<share::ObEncParam> enc_params;
  ObDirectLoadExternalMultiPartitionRowCompare compare;
  ASSERT_EQ(OB_SUCCESS, prepare_schecma(rowkey_count, column_num, str_rowkey_count,
                                        int_rowkey_count, table_schema));
  ASSERT_EQ(OB_SUCCESS, prepare_utils(table_schema, allocator, datum_util, enc_params, compare));
  ASSERT_EQ(OB_SUCCESS,
            sort_test(test_row_num, &rand, table_schema, enc_params, compare, allocator));
}

TEST_F(TestChunkSort, test_random_int)
{
  //the type of rowkey is int
  const int64_t test_row_num = 1000000;
  int64_t rowkey_count = 4;
  int64_t column_num = 4;
  int64_t str_rowkey_count = 0;
  int64_t int_rowkey_count = 4;
  Random rand = Random(0, test_row_num-1);
  ObTableSchema table_schema;
  ObArenaAllocator allocator;
  ObStorageDatumUtils datum_util;
  ObArray<share::ObEncParam> enc_params;
  ObDirectLoadExternalMultiPartitionRowCompare compare;
  ASSERT_EQ(OB_SUCCESS, prepare_schecma(rowkey_count, column_num, str_rowkey_count,
                                        int_rowkey_count, table_schema));
  ASSERT_EQ(OB_SUCCESS, prepare_utils(table_schema, allocator, datum_util, enc_params, compare));
  ASSERT_EQ(OB_SUCCESS,
            sort_test(test_row_num, &rand, table_schema, enc_params, compare, allocator));
}

TEST_F(TestChunkSort, test_random_prefix)
{
  //all row have the same prefix column
  const int64_t test_row_num = 1000000;
  int64_t rowkey_count = 4;
  int64_t column_num = 4;
  int64_t str_rowkey_count = 4;
  int64_t int_rowkey_count = 0;
  Random rand = Random(0, test_row_num-1);
  ObTableSchema table_schema;
  ObArenaAllocator allocator;
  ObStorageDatumUtils datum_util;
  ObArray<share::ObEncParam> enc_params;
  ObDirectLoadExternalMultiPartitionRowCompare compare;
  ASSERT_EQ(OB_SUCCESS, prepare_schecma(rowkey_count, column_num, str_rowkey_count,
                                        int_rowkey_count, table_schema));
  ASSERT_EQ(OB_SUCCESS, prepare_utils(table_schema, allocator, datum_util, enc_params, compare));
  ASSERT_EQ(OB_SUCCESS,
            sort_test(test_row_num, &rand, table_schema, enc_params, compare, allocator, 3));
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_direct_load_adaptive_aqs_sort.log");
  OB_LOGGER.set_file_name("test_direct_load_adaptive_aqs_sort.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}