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

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/ob_tmp_file.h"
#include "storage/blocksstable/ob_tmp_file_store.h"
#include "storage/blocksstable/ob_tmp_file_cache.h"
#include "ob_row_generate.h"
#include "ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
static ObSimpleMemLimitGetter getter;

namespace unittest
{

static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;

struct BufHeader
{
public:
  BufHeader()
    : data_size_(0), start_row_(0)
  {}
  virtual ~BufHeader() {}
  int serialize(char *buf, const int64_t buf_len, int64_t &pos);
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t data_size_;
  int64_t start_row_;
};

int BufHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, data_size_))) {
    STORAGE_LOG(WARN, "fail to serialize data size", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, start_row_))) {
    STORAGE_LOG(WARN, "fail to serialize start row", K(ret));
  }
  return ret;
}

int BufHeader::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &data_size_))) {
    STORAGE_LOG(WARN, "fail to decode data size", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &start_row_))) {
    STORAGE_LOG(WARN, "fail to decode start row", K(ret));
  }
  return ret;
}

int64_t BufHeader::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(1L);
  size += serialization::encoded_length_i64(1L);
  return size;
}

class TestTmpFileStress : public share::ObThreadPool
{
public:
  TestTmpFileStress();
  TestTmpFileStress(ObTenantBase *tenant_ctx);
  virtual ~TestTmpFileStress();
  int init(const int fd, const bool is_write, const int64_t thread_cnt, ObTableSchema *table_schema,
      const bool is_plain_data, const bool is_big_file);
  virtual void run1();
private:
  void prepare_data(char *buf, const int64_t macro_block_size);
  void prepare_plain_data(const int64_t buf_size, char *buf, ObIArray<int64_t> &size_array);
  void prepare_one_buffer(const int64_t macro_block_size, const int64_t start_index, char *buf, int64_t &end_index);
  void check_data(const char *buf, const int64_t buf_len);
  void check_plain_data(const char *read_buf, const char *right_buf, const int64_t buf_len);
  void write_data(const int64_t macro_block_size);
  void write_plain_data(char *&buf, const int64_t macro_block_size);
  void read_data(const int64_t macro_block_size);
  void read_plain_data(const char *buf, const int64_t macro_block_size);
private:
  static const int64_t BUF_COUNT = 16;
  int64_t thread_cnt_;
  int64_t size_;
  int fd_;
  bool is_write_;
  bool is_big_file_;
  ObTableSchema *table_schema_;
  bool is_plain_;
  ObTenantBase *tenant_ctx_;
};

TestTmpFileStress::TestTmpFileStress()
  : thread_cnt_(0), size_(OB_SERVER_BLOCK_MGR.get_macro_block_size()), fd_(0),
    is_write_(false), is_big_file_(false), table_schema_(NULL), is_plain_(false)
{
}

TestTmpFileStress::TestTmpFileStress(ObTenantBase *tenant_ctx)
  : thread_cnt_(0), size_(OB_SERVER_BLOCK_MGR.get_macro_block_size()), fd_(0),
    is_write_(false), is_big_file_(false), table_schema_(NULL), is_plain_(false),
    tenant_ctx_(tenant_ctx)
{
}

TestTmpFileStress::~TestTmpFileStress()
{
}

int TestTmpFileStress::init(const int fd, const bool is_write,
    const int64_t thread_cnt, ObTableSchema *table_schema,
    const bool is_plain, const bool is_big_file)
{
  int ret = OB_SUCCESS;
  if (thread_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(thread_cnt));
  } else {
    thread_cnt_ = thread_cnt;
    fd_ = fd;
    is_write_ = is_write;
    table_schema_ = table_schema;
    is_plain_ = is_plain;
    is_big_file_ = is_big_file;
    if (!is_big_file_) {
      size_ = 16L * 1024L;
    }
    set_thread_count(static_cast<int32_t>(thread_cnt));
  }
  return ret;
}

void TestTmpFileStress::prepare_one_buffer(const int64_t macro_block_size, const int64_t start_index, char *buf, int64_t &end_index)
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  BufHeader header;
  ObArenaAllocator allocator;
  ObRowGenerate row_generate;
  int64_t buf_pos = header.get_serialize_size();
  int64_t header_pos = 0;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.cells_ = cells;
  row.row_val_.count_ = TEST_COLUMN_CNT;
  header.start_row_ = start_index;
  const int64_t buf_capacity = macro_block_size;
  ASSERT_EQ(OB_SUCCESS, row_generate.init(*table_schema_, &allocator));
  for (int64_t i = start_index; OB_SUCC(ret) && buf_pos < buf_capacity; ++i) {
    ret = row_generate.get_next_row(i, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (buf_pos + row.get_serialize_size() <= buf_capacity) {
      ASSERT_EQ(OB_SUCCESS, row.serialize(buf, buf_capacity, buf_pos));
    } else {
      end_index = i;
      break;
    }
  }
  header.data_size_ = buf_pos;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, buf_capacity, header_pos));
}

void TestTmpFileStress::prepare_data(char *buf, const int64_t macro_block_size)
{
  const int64_t macro_block_buffer_count = BUF_COUNT;
  int64_t buf_pos = 0;
  int64_t start_index = 0;
  for (int64_t i = 0; i < macro_block_buffer_count; ++i) {
    int64_t end_index = 0;
    prepare_one_buffer(macro_block_size, start_index, buf + buf_pos, end_index);
    buf_pos += macro_block_size;
    start_index = end_index;
  }
}

void TestTmpFileStress::prepare_plain_data(const int64_t buf_capacity, char *buf,
    ObIArray<int64_t> &size_array)
{
  ObRandom random;
  int64_t left_size = buf_capacity;
  int8_t data = 0;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  while (left_size > 0) {
    if (left_size < macro_block_size) {
      memset(buf, data, left_size);
      ASSERT_EQ(OB_SUCCESS, size_array.push_back(left_size));
      left_size = 0;
    } else {
      const int64_t rand_data = random.get(0, macro_block_size);
      memset(buf, data, rand_data);
      left_size -= rand_data;
      buf += rand_data;
      ASSERT_EQ(OB_SUCCESS, size_array.push_back(rand_data));
    }
    ++data;
  }
}

void TestTmpFileStress::check_plain_data(const char *read_buf, const char *right_buf, const int64_t buf_len)
{
  int cmp = memcmp(read_buf, right_buf, buf_len);
  ASSERT_EQ(0, cmp);
}

void TestTmpFileStress::check_data(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t header_pos = 0;
  int64_t data_pos = 0;
  const char *data = NULL;
  BufHeader header;
  ObArenaAllocator allocator;
  ObRowGenerate row_generate;
  ASSERT_EQ(OB_SUCCESS, row_generate.init(*table_schema_, &allocator));
  const int64_t serialize_size = header.get_serialize_size();
  ObStoreRow lhs_row;
  ObStoreRow rhs_row;
  ObObj lhs_cells[TEST_COLUMN_CNT];
  ObObj rhs_cells[TEST_COLUMN_CNT];
  lhs_row.row_val_.cells_ = lhs_cells;
  lhs_row.row_val_.count_ = TEST_COLUMN_CNT;
  rhs_row.row_val_.cells_ = rhs_cells;
  rhs_row.row_val_.count_ = TEST_COLUMN_CNT;
  ret = header.deserialize(buf, buf_len, header_pos);
  const int64_t data_len = header.data_size_;
  int64_t i = header.start_row_;
  ASSERT_EQ(OB_SUCCESS, ret);
  data = buf + header_pos;
  while (data_pos < data_len - serialize_size) {
    ret = lhs_row.deserialize(data, data_len, data_pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate.get_next_row(i, rhs_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(lhs_row.row_val_ == rhs_row.row_val_);
    ++i;
  }
}

void TestTmpFileStress::write_data(const int64_t macro_block_size)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObRowGenerate row_generate;
  ObTmpFileIOInfo io_info;
  row_generate.reset();
  ret = row_generate.init(*table_schema_, &allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info.fd_ = fd_;
  io_info.size_ = macro_block_size;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  char *buf = new char[BUF_COUNT * macro_block_size];
  const int64_t timeout_ms = 5000;
  prepare_data(buf, macro_block_size);
  for (int64_t i = 0; i < BUF_COUNT; ++i) {
    io_info.buf_ = buf + i * macro_block_size;
    check_data(io_info.buf_, macro_block_size);
    ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

void TestTmpFileStress::write_plain_data(char *&buf, const int64_t macro_block_size)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> size_array;
  ObTmpFileIOInfo io_info;
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info.fd_ = fd_;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  buf = new char[BUF_COUNT * macro_block_size];
  const int64_t timeout_ms = 5000;
  int64_t sum_size = 0;
  prepare_plain_data(BUF_COUNT * macro_block_size, buf, size_array);
  for (int64_t i = 0; i < size_array.count(); ++i) {
    io_info.buf_ = buf + sum_size;
    io_info.size_ = size_array.at(i);
    ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
    ASSERT_EQ(OB_SUCCESS, ret);
    sum_size += size_array.at(i);
  }
  ret = ObTmpFileManager::get_instance().sync(fd_, timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTmpFileStress::read_data(const int64_t macro_block_size)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ms = 5000;
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  io_info.fd_ = fd_;
  io_info.size_ = macro_block_size;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  char *buf = new char[macro_block_size];
  for (int64_t i = 0; i < BUF_COUNT; ++i) {
    io_info.buf_ = buf;
    ret = ObTmpFileManager::get_instance().read(io_info, timeout_ms, handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(macro_block_size, handle.get_data_size());
    check_data(handle.get_buffer(), handle.get_data_size());
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  handle.reset();
}

void TestTmpFileStress::read_plain_data(const char *read_buf, const int64_t macro_block_size)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ms = 5000;
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  io_info.fd_ = fd_;
  io_info.size_ = macro_block_size;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  char *buf = new char[BUF_COUNT * macro_block_size];
  int64_t offset = 0;
  for (int64_t i = 0; i < BUF_COUNT; ++i) {
    io_info.buf_ = buf + i * macro_block_size;
    offset = i * macro_block_size;
    ret = ObTmpFileManager::get_instance().pread(io_info, offset, timeout_ms, handle);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  offset += macro_block_size;
  ret = ObTmpFileManager::get_instance().pread(io_info, offset, timeout_ms, handle);
  ASSERT_EQ(OB_ITER_END, ret);
  check_plain_data(read_buf, buf, BUF_COUNT * macro_block_size);
  handle.reset();
}

void TestTmpFileStress::run1()
{
  ObTenantEnv::set_tenant(tenant_ctx_);
  if (is_plain_) {
    char *buf = NULL;
    write_plain_data(buf, size_);
    read_plain_data(buf, size_);
  } else {
    if (is_write_) {
      write_data(size_);
    } else {
      read_data(size_);
    }
  }
}

class TestMultiTmpFileStress : public share::ObThreadPool
{
public:
  TestMultiTmpFileStress();
  TestMultiTmpFileStress(ObTenantBase *tenant_ctx);
  virtual ~TestMultiTmpFileStress();
  int init(const int64_t file_cnt, const int64_t dir_id, const int64_t thread_cnt,
      ObTableSchema *table_schema, const bool is_plain_data, const bool is_big_file);
  virtual void run1();
private:
  void run_plain_case();
  void run_normal_case();
private:
  int64_t file_cnt_;
  int64_t dir_id_;
  int64_t thread_cnt_perf_file_;
  ObTableSchema *table_schema_;
  bool is_big_file_;
  bool is_plain_data_;
  ObTenantBase *tenant_ctx_;
};

TestMultiTmpFileStress::TestMultiTmpFileStress()
  : file_cnt_(0),
    dir_id_(-1),
    thread_cnt_perf_file_(0),
    table_schema_(NULL),
    is_big_file_(false),
    is_plain_data_(false)
{
}
TestMultiTmpFileStress::TestMultiTmpFileStress(ObTenantBase *tenant_ctx)
  : file_cnt_(0),
    dir_id_(-1),
    thread_cnt_perf_file_(0),
    table_schema_(NULL),
    is_big_file_(false),
    is_plain_data_(false),
    tenant_ctx_(tenant_ctx)
{
}

TestMultiTmpFileStress::~TestMultiTmpFileStress()
{
}

int TestMultiTmpFileStress::init(const int64_t file_cnt,
                                   const int64_t dir_id,
                                   const int64_t thread_cnt,
                                   ObTableSchema *table_schema,
                                   const bool is_plain_data,
                                   const bool is_big_file)
{
  int ret = OB_SUCCESS;
  if (file_cnt < 0 || thread_cnt < 0 || NULL == table_schema) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(file_cnt), K(thread_cnt),
        KP(table_schema));
  } else {
    file_cnt_ = file_cnt;
    dir_id_ = dir_id;
    thread_cnt_perf_file_ = thread_cnt;
    table_schema_ = table_schema;
    is_big_file_ = is_big_file;
    is_plain_data_ = is_plain_data;
    set_thread_count(static_cast<int32_t>(file_cnt));
  }
  return ret;
}

void TestMultiTmpFileStress::run_plain_case()
{
  int ret = OB_SUCCESS;
  int64_t fd = 0;
  TestTmpFileStress test(tenant_ctx_);
  ret = ObTmpFileManager::get_instance().open(fd, dir_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(fd, true, thread_cnt_perf_file_, table_schema_, is_plain_data_, is_big_file_);
  ASSERT_EQ(OB_SUCCESS, ret);
  test.start();
  test.wait();
  ret = ObTmpFileManager::get_instance().remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMultiTmpFileStress::run_normal_case()
{
  int ret = OB_SUCCESS;
  int64_t fd = 0;
  const int64_t timeout_ms = 5000;
  TestTmpFileStress test_write(tenant_ctx_);
  TestTmpFileStress test_read(tenant_ctx_);
  ret = ObTmpFileManager::get_instance().open(fd, dir_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "open file success", K(fd));
  ret = test_write.init(fd, true, thread_cnt_perf_file_, table_schema_, is_plain_data_, is_big_file_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test_read.init(fd, false, thread_cnt_perf_file_, table_schema_, is_plain_data_, is_big_file_);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_write.start();
  test_write.wait();
  ret = ObTmpFileManager::get_instance().sync(fd, timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_read.start();
  test_read.wait();
  ret = ObTmpFileManager::get_instance().remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMultiTmpFileStress::run1()
{
  ObTenantEnv::set_tenant(tenant_ctx_);
  if (is_plain_data_) {
    run_plain_case();
  } else {
    run_normal_case();
  }
}

class TestTmpFile : public TestDataFilePrepare
{
public:
  TestTmpFile();
  virtual ~TestTmpFile();
  virtual void SetUp();
  virtual void TearDown();
protected:
  ObTableSchema table_schema_;
private:
  void prepare_schema();
};

TestTmpFile::TestTmpFile()
  : TestDataFilePrepare(&getter, "TestTmpFile", 2 * 1024 * 1024, 2048)
{
}

TestTmpFile::~TestTmpFile()
{
}

void TestTmpFile::prepare_schema()
{
  ObColumnSchemaV2 column;
  int64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_macro_file"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if(obj_type == common::ObIntType){
      column.set_rowkey_position(1);
    } else if(obj_type == common::ObVarcharType) {
      column.set_rowkey_position(2);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  ObTmpFileManager::get_instance().destroy();
}

void TestTmpFile::SetUp()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TestDataFilePrepare::SetUp();
  prepare_schema();

  ret = getter.add_tenant(1,
                          8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  ret = ObTmpFileManager::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  static ObTenantBase tenant_ctx(1);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
}

void TestTmpFile::TearDown()
{
  table_schema_.reset();
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObTmpFileStore::get_instance().destroy();
  TestDataFilePrepare::TearDown();
}

TEST_F(TestTmpFile, test_big_file)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTmpFileManager::get_instance().open(fd, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t write_size = macro_block_size * 512;
  char *write_buf = (char *)malloc(write_size);
  for (int64_t i = 0; i < write_size; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  char *read_buf = (char *)malloc(write_size);
  io_info.fd_ = fd;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
  write_time = ObTimeUtility::current_time() - write_time;
  io_info.buf_ = read_buf;

  io_info.size_ = write_size;
  ret = ObTmpFileManager::get_instance().aio_read(io_info, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(handle.size_ < handle.expect_read_size_);
  ASSERT_EQ(OB_SUCCESS, handle.wait(timeout_ms));
  ASSERT_EQ(write_size, handle.get_data_size());
  int cmp = memcmp(handle.get_buffer(), write_buf, handle.get_data_size());
  ASSERT_EQ(0, cmp);

  io_info.size_ = macro_block_size;
  ret = ObTmpFileManager::get_instance().pread(io_info, 100, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 100, handle.get_data_size());
  ASSERT_EQ(0, cmp);

  io_info.size_ = write_size;
  int64_t read_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().pread(io_info, 0, timeout_ms, handle);
  read_time = ObTimeUtility::current_time() - read_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(write_size, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf, write_size);
  ASSERT_EQ(0, cmp);

  io_info.size_ = 200;
  ret = ObTmpFileManager::get_instance().pread(io_info, 200, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(200, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 200, 200);
  ASSERT_EQ(0, cmp);

  free(write_buf);
  free(read_buf);

  STORAGE_LOG(INFO, "test_big_file");
  STORAGE_LOG(INFO, "io time", K(write_time), K(read_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd);
}

TEST_F(TestTmpFile, test_multi_small_file_single_thread_read_write)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;
  const int64_t file_cnt = 4;
  const bool is_plain_data = false;
  const bool is_big_file = false;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;

  STORAGE_LOG(INFO, "test_multi_small_file_single_thread_read_write");
  STORAGE_LOG(INFO, "io time", K(io_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}

TEST_F(TestTmpFile, test_multi_small_file_multi_thread_read_write )
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 4;
  const int64_t file_cnt = 4;
  const bool is_plain_data = false;
  const bool is_big_file = false;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;

  STORAGE_LOG(INFO, "test_multi_small_file_multi_thread_read_write");
  STORAGE_LOG(INFO, "io time", K(io_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}

TEST_F(TestTmpFile, test_inner_read_offset_and_seek)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTmpFileManager::get_instance().open(fd, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  char *write_buf = new char [macro_block_size + 256];
  for (int i = 0; i < macro_block_size + 256; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  char *read_buf = new char [macro_block_size + 256];
  io_info.fd_ = fd;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = macro_block_size + 256;
  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info.buf_ = read_buf;


  int64_t read_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().read(io_info, timeout_ms, handle);
  read_time = ObTimeUtility::current_time() - read_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size + 256, handle.get_data_size());
  int cmp = memcmp(handle.get_buffer(), write_buf, macro_block_size + 256);
  ASSERT_EQ(0, cmp);


  io_info.size_ = 200;
  ret = ObTmpFileManager::get_instance().read(io_info, timeout_ms, handle);
  ASSERT_EQ(OB_ITER_END, ret);


  ret = ObTmpFileManager::get_instance().seek(fd, 0, ObTmpFile::SET_SEEK);
  ASSERT_EQ(OB_SUCCESS, ret);

  io_info.size_ = 201;
  ret = ObTmpFileManager::get_instance().read(io_info, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(201, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf, 201);
  ASSERT_EQ(0, cmp);


  ret = ObTmpFileManager::get_instance().seek(fd, 199, ObTmpFile::CUR_SEEK);
  ASSERT_EQ(OB_SUCCESS, ret);

  io_info.size_ = 199;
  ret = ObTmpFileManager::get_instance().read(io_info, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(199, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 400, 199);
  ASSERT_EQ(0, cmp);


  STORAGE_LOG(INFO, "test_inner_read_offset_and_seek");
  STORAGE_LOG(INFO, "io time", K(write_time), K(read_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd);
}

TEST_F(TestTmpFile, test_single_file_single_thread_read_write)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;
  const int64_t file_cnt = 1;
  const bool is_plain_data = false;
  const bool is_big_file = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;


  STORAGE_LOG(INFO, "test_single_file_single_thread_read_write");
  STORAGE_LOG(INFO, "io time", K(io_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}

TEST_F(TestTmpFile, test_aio_read_and_write)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTmpFileManager::get_instance().open(fd, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  char *write_buf = new char [macro_block_size + 256];
  for (int i = 0; i < macro_block_size + 256; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  char *read_buf = new char [macro_block_size + 256];


  io_info.fd_ = fd;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = macro_block_size + 256;
  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().aio_write(io_info, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = handle.wait(timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  handle.reset();

  io_info.buf_ = read_buf;

  int64_t read_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().read(io_info, timeout_ms, handle);
  read_time = ObTimeUtility::current_time() - read_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size + 256, handle.get_data_size());
  int cmp = memcmp(handle.get_buffer(), write_buf, macro_block_size + 256);
  ASSERT_EQ(0, cmp);


  ret = ObTmpFileManager::get_instance().seek(fd, 100, ObTmpFile::SET_SEEK);
  ASSERT_EQ(OB_SUCCESS, ret);


  io_info.size_ = macro_block_size;
  ret = ObTmpFileManager::get_instance().aio_read(io_info, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = handle.wait(timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 100, macro_block_size);
  ASSERT_EQ(0, cmp);
  handle.reset();


  io_info.size_ = macro_block_size;
  ret = ObTmpFileManager::get_instance().aio_pread(io_info, 0, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = handle.wait(timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf, macro_block_size);
  ASSERT_EQ(0, cmp);
  handle.reset();


  STORAGE_LOG(INFO, "test_aio_read_and_write");
  STORAGE_LOG(INFO, "io time", K(write_time), K(read_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd);
}

TEST_F(TestTmpFile, test_100_small_files)
{
  int ret = OB_SUCCESS;
  int64_t dir = 0;
  int64_t fd = 0;
  int count = 100;
  const int64_t timeout_ms = 5000;
  TestTmpFileStress test_write(MTL_CTX());
  TestTmpFileStress test_read(MTL_CTX());
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  while (count--) {
    ret = ObTmpFileManager::get_instance().open(fd, dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "open file success", K(fd));
    ret = test_write.init(fd, true, 1, &table_schema_, false, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = test_read.init(fd, false, 1, &table_schema_, false, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    test_write.start();
    test_write.wait();
    ret = ObTmpFileManager::get_instance().sync(fd, timeout_ms);
    ASSERT_EQ(OB_SUCCESS, ret);
    test_read.start();
    test_read.wait();
  }

  STORAGE_LOG(INFO, "test_1000_small_files");
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
  count = 100;
  while (count--) {
    ret = ObTmpFileManager::get_instance().remove(count);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestTmpFile, test_single_file_multi_thread_read_write)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 4;
  const int64_t file_cnt = 1;
  const bool is_plain_data = false;
  const bool is_big_file = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;


  STORAGE_LOG(INFO, "test_single_file_multi_thread_read_write");
  STORAGE_LOG(INFO, "io time", K(io_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}

TEST_F(TestTmpFile, test_multi_file_single_thread_read_write)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;
  const int64_t file_cnt = 4;
  const bool is_plain_data = false;
  const bool is_big_file = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;


  STORAGE_LOG(INFO, "test_multi_file_single_thread_read_write");
  STORAGE_LOG(INFO, "io time", K(io_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}

TEST_F(TestTmpFile, test_multi_file_multi_thread_read_write)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 4;
  const int64_t file_cnt = 4;
  const bool is_plain_data = false;
  const bool is_big_file = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;


  STORAGE_LOG(INFO, "test_multi_file_multi_thread_read_write");
  STORAGE_LOG(INFO, "io time", K(io_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}

TEST_F(TestTmpFile, test_write_not_macro_size)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;
  const int64_t file_cnt = 1;
  const bool is_plain_data = true;
  const bool is_big_file = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;


  STORAGE_LOG(INFO, "test_write_not_macro_size");
  STORAGE_LOG(INFO, "io time", K(io_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}

TEST_F(TestTmpFile, test_write_less_than_macro_block_size)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTmpFileManager::get_instance().open(fd, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  char *write_buf = new char [256];
  for (int i = 0; i < 256; ++i) {
    write_buf[i] = static_cast<char>(i);
  }
  char *read_buf = new char [256];
  io_info.fd_ = fd;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 256;
  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info.buf_ = read_buf;


  int64_t read_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().pread(io_info, 0, timeout_ms, handle);
  read_time = ObTimeUtility::current_time() - read_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(256, handle.get_data_size());
  int cmp = memcmp(handle.get_buffer(), write_buf, 256);
  ASSERT_EQ(0, cmp);


  io_info.size_ = 255;
  ret = ObTmpFileManager::get_instance().pread(io_info, 0, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(255, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf, 255);
  ASSERT_EQ(0, cmp);


  ret = ObTmpFileManager::get_instance().pread(io_info, 20, timeout_ms, handle);
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(256 - 20, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 20, 256 - 20);
  ASSERT_EQ(0, cmp);


  io_info.size_ = 20;
  ret = ObTmpFileManager::get_instance().pread(io_info, 40, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(20, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 40, handle.get_data_size());
  ASSERT_EQ(0, cmp);


  io_info.size_ = 100;
  ret = ObTmpFileManager::get_instance().pread(io_info, 156, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 156, handle.get_data_size());
  ASSERT_EQ(0, cmp);


  ret = ObTmpFileManager::get_instance().pread(io_info, 256, timeout_ms, handle);
  ASSERT_EQ(OB_ITER_END, ret);


  STORAGE_LOG(INFO, "test_write_less_than_macro_block_size");
  STORAGE_LOG(INFO, "io time", K(write_time), K(read_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd);
}

TEST_F(TestTmpFile, test_write_more_than_one_macro_block)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTmpFileManager::get_instance().open(fd, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  char *write_buf = new char [macro_block_size + 256];
  for (int i = 0; i < macro_block_size + 256; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  char *read_buf = new char [macro_block_size + 256];
  io_info.fd_ = fd;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = macro_block_size + 256;
  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info.buf_ = read_buf;

  int64_t read_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().pread(io_info, 0, timeout_ms, handle);
  read_time = ObTimeUtility::current_time() - read_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size + 256, handle.get_data_size());
  int cmp = memcmp(handle.get_buffer(), write_buf, macro_block_size + 256);
  ASSERT_EQ(0, cmp);

  io_info.size_ = 200;
  ret = ObTmpFileManager::get_instance().pread(io_info, 200, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(200, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 200, 200);
  ASSERT_EQ(0, cmp);

  io_info.size_ = macro_block_size;
  ret = ObTmpFileManager::get_instance().pread(io_info, 200, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 200, macro_block_size);
  ASSERT_EQ(0, cmp);

  io_info.size_ = macro_block_size;
  ret = ObTmpFileManager::get_instance().pread(io_info, 400, timeout_ms, handle);
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(macro_block_size + 256 - 400, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 400, macro_block_size + 256 - 400);
  ASSERT_EQ(0, cmp);

  io_info.size_ = 100;
  ret = ObTmpFileManager::get_instance().pread(io_info, macro_block_size, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + macro_block_size, handle.get_data_size());
  ASSERT_EQ(0, cmp);

  io_info.size_ = 100;
  ret = ObTmpFileManager::get_instance().pread(io_info, macro_block_size + 10, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + macro_block_size + 10, handle.get_data_size());
  ASSERT_EQ(0, cmp);


  io_info.size_ = 200;
  ret = ObTmpFileManager::get_instance().pread(io_info, macro_block_size + 100, timeout_ms, handle);
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(156, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + macro_block_size + 100, handle.get_data_size());
  ASSERT_EQ(0, cmp);

  ret = ObTmpFileManager::get_instance().pread(io_info, macro_block_size + 256, timeout_ms, handle);
  ASSERT_EQ(OB_ITER_END, ret);


  STORAGE_LOG(INFO, "test_write_more_than_one_macro_block");
  STORAGE_LOG(INFO, "io time", K(write_time), K(read_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd);
}

TEST_F(TestTmpFile, test_single_dir_two_file)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd_1 = -1;
  int64_t fd_2 = -1;
  const int64_t macro_block_size = 64 * 1024;
  ObTmpFileIOInfo io_info1;
  ObTmpFileIOInfo io_info2;
  ObTmpFileIOHandle handle1;
  ObTmpFileIOHandle handle2;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *write_buf = new char [macro_block_size + 256];
  for (int i = 0; i < macro_block_size + 256; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  char *read_buf = new char [macro_block_size + 256];

  ret = ObTmpFileManager::get_instance().open(fd_1, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info1.fd_ = fd_1;
  io_info1.tenant_id_ = 1;
  io_info1.io_desc_.set_wait_event(2);
  io_info1.buf_ = write_buf;
  io_info1.size_ = macro_block_size + 256;

  ret = ObTmpFileManager::get_instance().open(fd_2, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info2.fd_ = fd_2;
  io_info2.tenant_id_ = 1;
  io_info2.io_desc_.set_wait_event(2);
  io_info2.buf_ = write_buf;
  io_info2.size_ = macro_block_size + 256;

  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info1, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);

  write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info2, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);


  io_info1.buf_ = read_buf;
  int64_t read_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().pread(io_info1, 0, timeout_ms, handle1);
  read_time = ObTimeUtility::current_time() - read_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size + 256, handle1.get_data_size());
  int cmp = memcmp(handle1.get_buffer(), write_buf, macro_block_size + 256);
  ASSERT_EQ(0, cmp);


  io_info2.buf_ = read_buf;
  read_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().pread(io_info2, 0, timeout_ms, handle2);
  read_time = ObTimeUtility::current_time() - read_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size + 256, handle2.get_data_size());
  cmp = memcmp(handle2.get_buffer(), write_buf, macro_block_size + 256);
  ASSERT_EQ(0, cmp);

  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObTmpFileManager::get_instance().remove(fd_1);
  ObTmpFileManager::get_instance().remove(fd_2);
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
}

/*TEST_F(TestTmpFile, test_iter_end)
{
  int old_ret = OB_SUCCESS;
  int new_ret = OB_SUCCESS;
  int64_t new_dir = -1;
  int64_t new_fd = -1;
  int old_fd = -1;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  ObTmpFileIOInfo new_io_info;
  ObTmpFileIOHandle new_handle;
  ObMacroFileIOInfo old_io_info;
  ObMacroFileIOHandle old_handle;
  new_ret = ObTmpFileManager::get_instance().alloc_dir(new_dir);
  ASSERT_EQ(OB_SUCCESS, new_ret);
  new_ret = ObTmpFileManager::get_instance().open(new_fd, new_dir);
  ASSERT_EQ(OB_SUCCESS, new_ret);
  old_ret = ObMacroFileManager::get_instance().open(old_fd);
  ASSERT_EQ(OB_SUCCESS, old_ret);
  char *write_buf = new char [macro_block_size + 256];
  for (int i = 0; i < macro_block_size + 256; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  char *read_buf = new char [macro_block_size + 256];

  new_io_info.fd_ = new_fd;
  new_io_info.tenant_id_ = 1;
  new_io_info.io_desc_.set_wait_event(2);
  new_io_info.buf_ = write_buf;
  new_io_info.size_ = macro_block_size + 256;

  old_io_info.fd_ = old_fd;
  old_io_info.tenant_id_ = 1;
  old_io_info.io_desc_.set_wait_event(2);
  old_io_info.buf_ = write_buf;
  old_io_info.size_ = macro_block_size + 256;

  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  new_ret = ObTmpFileManager::get_instance().write(new_io_info, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  old_ret = ObMacroFileManager::get_instance().write(old_io_info, timeout_ms);
  ASSERT_EQ(OB_SUCCESS, new_ret);
  ASSERT_EQ(old_ret, new_ret);

  new_io_info.buf_ = read_buf;
  old_io_info.buf_ = read_buf;


  int64_t read_time = ObTimeUtility::current_time();
  new_ret = ObTmpFileManager::get_instance().pread(new_io_info, 0, timeout_ms, new_handle);
  read_time = ObTimeUtility::current_time() - read_time;
  old_ret = ObMacroFileManager::get_instance().pread(old_io_info, 0,timeout_ms, old_handle);
  ASSERT_EQ(OB_SUCCESS, new_ret);
  ASSERT_EQ(old_ret, new_ret);

  ASSERT_EQ(macro_block_size + 256, new_handle.get_data_size());
  int cmp = memcmp(new_handle.get_buffer(), write_buf, macro_block_size + 256);
  ASSERT_EQ(0, cmp);
  new_ret = ObTmpFileManager::get_instance().pread(new_io_info, macro_block_size + 256, timeout_ms,
      new_handle);
  old_ret = ObMacroFileManager::get_instance().pread(old_io_info, macro_block_size + 256,
      timeout_ms, old_handle);
  ASSERT_EQ(OB_ITER_END, new_ret);
  ASSERT_EQ(OB_ITER_END, old_ret);
  ASSERT_EQ(old_ret, new_ret);
}*/

TEST_F(TestTmpFile, test_single_dir_multi_file)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;
  const int64_t file_cnt = 4;
  const bool is_plain_data = false;
  const bool is_big_file = false;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;


  STORAGE_LOG(INFO, "test_single_dir_multi_file");
  STORAGE_LOG(INFO, "io time", K(io_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}

TEST_F(TestTmpFile, test_drop_tenant_file)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 4;
  const int64_t file_cnt = 4;
  const bool is_plain_data = false;
  const bool is_big_file = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, thread_cnt, &table_schema_, is_plain_data, is_big_file);
  ASSERT_EQ(OB_SUCCESS, ret);
  test.start();
  test.wait();
  ASSERT_EQ(0, ObTmpFileManager::get_instance().files_.map_.size());
  ASSERT_EQ(1, ObTmpFileStore::get_instance().tenant_file_stores_.size());


  ret = ObTmpFileManager::get_instance().remove_tenant_file(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, ObTmpFileManager::get_instance().files_.map_.size());
  ASSERT_EQ(0, ObTmpFileStore::get_instance().tenant_file_stores_.size());

  int64_t fd = 0;
  int count = 100;
  const int64_t timeout_ms = 5000;
  TestTmpFileStress test_write(MTL_CTX());
  TestTmpFileStress test_read(MTL_CTX());
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  while (count--) {
    ret = ObTmpFileManager::get_instance().open(fd, dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "open file success", K(fd));
    ret = test_write.init(fd, true, 1, &table_schema_, false, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = test_read.init(fd, false, 1, &table_schema_, false, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    test_write.start();
    test_write.wait();
    ret = ObTmpFileManager::get_instance().sync(fd, timeout_ms);
    ASSERT_EQ(OB_SUCCESS, ret);
    test_read.start();
    test_read.wait();
  }

  ASSERT_EQ(100, ObTmpFileManager::get_instance().files_.map_.size());
  ASSERT_EQ(1, ObTmpFileStore::get_instance().tenant_file_stores_.size());


  ret = ObTmpFileManager::get_instance().remove_tenant_file(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, ObTmpFileManager::get_instance().files_.map_.size());
  ASSERT_EQ(0, ObTmpFileStore::get_instance().tenant_file_stores_.size());
}

TEST_F(TestTmpFile, test_handle_double_wait)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTmpFileManager::get_instance().open(fd, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  char *write_buf = new char [256];
  for (int i = 0; i < 256; ++i) {
    write_buf[i] = static_cast<char>(i);
  }
  char *read_buf = new char [256];
  io_info.fd_ = fd;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 256;
  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info.buf_ = read_buf;


  int64_t read_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().pread(io_info, 0, timeout_ms, handle);
  read_time = ObTimeUtility::current_time() - read_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(256, handle.get_data_size());
  int cmp = memcmp(handle.get_buffer(), write_buf, 256);
  ASSERT_EQ(0, cmp);

  ASSERT_EQ(OB_SUCCESS, handle.wait(timeout_ms));

  STORAGE_LOG(INFO, "test_handle_double_wait");
  STORAGE_LOG(INFO, "io time", K(write_time), K(read_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd);
}

TEST_F(TestTmpFile, test_sql_workload)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTmpFileManager::get_instance().open(fd, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t blk_cnt = 16;
  int64_t write_size = macro_block_size * blk_cnt;
  char *write_buf = (char *)malloc(write_size);
  for (int64_t i = 0; i < write_size; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  char *read_buf = (char *)malloc(write_size);


  io_info.fd_ = fd;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();

  const int cnt = 1;
  const int64_t sql_read_size = 64 * 1024;
  const int64_t sql_cnt = write_size / sql_read_size;

  for (int i = 0; i < cnt; i++) {
    for (int64_t j = 0; j < sql_cnt; j++) {
      io_info.size_ = sql_read_size;
      io_info.buf_ = write_buf + j * sql_read_size;
      ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  write_time = ObTimeUtility::current_time() - write_time;


  io_info.buf_ = read_buf;

  io_info.size_ = macro_block_size;
  ret = ObTmpFileManager::get_instance().pread(io_info, 100, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_block_size, handle.get_data_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + 100, handle.get_data_size());
  ASSERT_EQ(0, cmp);


  io_info.size_ = write_size;
  int64_t read_time = ObTimeUtility::current_time();

  ret = ObTmpFileManager::get_instance().seek(fd, 0, ObTmpFile::SET_SEEK);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int i = 0; i < cnt; i++) {
    for (int64_t j = 0; j < sql_cnt; j++) {
      io_info.size_ = sql_read_size;
      io_info.buf_ = read_buf + j * sql_read_size;
      ret = ObTmpFileManager::get_instance().read(io_info, timeout_ms, handle);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(sql_read_size, handle.get_data_size());
      cmp = memcmp(handle.get_buffer(), write_buf + j * sql_read_size, sql_read_size);
      ASSERT_EQ(0, cmp);
    }
  }
  read_time = ObTimeUtility::current_time() - read_time;

  io_info.size_ = 200;
  ret = ObTmpFileManager::get_instance().pread(io_info, 200, timeout_ms, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(200, handle.get_data_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 200, 200);
  ASSERT_EQ(0, cmp);

  free(write_buf);
  free(read_buf);


  STORAGE_LOG(INFO, "test_sql_workload");
  STORAGE_LOG(INFO, "io time", K((write_size * cnt) / (1024*1024*1024)), K(write_time), K(read_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd);
}

TEST_F(TestTmpFile, test_page_buddy)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator  allocator;
  ObTmpFilePageBuddy page_buddy_1;

  ret = page_buddy_1.init(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  uint8_t page_nums = 64;
  uint8_t alloced_page_nums = 64;
  uint8_t start_page_id = 255;
  ASSERT_EQ(true, page_buddy_1.is_empty());
  ret = page_buddy_1.alloc(page_nums, start_page_id, alloced_page_nums);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, page_buddy_1.is_empty());

  uint8_t start_page_id_2 = 255;
  ret = page_buddy_1.alloc(page_nums, start_page_id_2, alloced_page_nums);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, page_buddy_1.is_empty());

  page_buddy_1.free(start_page_id + 63, page_nums -63);
  page_buddy_1.free(start_page_id_2 + 1, page_nums - 1);
  page_nums = 63;
  page_buddy_1.free(start_page_id, page_nums);
  page_nums = 1;
  page_buddy_1.free(start_page_id_2, page_nums);
  STORAGE_LOG(INFO, "page buddy", K(page_buddy_1));
  ASSERT_EQ(true, page_buddy_1.is_empty());

  ObTmpFilePageBuddy page_buddy_2;
  ret = page_buddy_2.init(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, page_buddy_2.is_empty());
  start_page_id = 0;
  ret = page_buddy_2.alloc_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, page_buddy_2.is_empty());

  int32_t free_nums = 252 - 129;
  page_buddy_2.free(start_page_id + 129, free_nums);
  free_nums = 127;
  page_buddy_2.free(start_page_id + 2, free_nums);
  free_nums = 2;
  page_buddy_2.free(start_page_id, free_nums);
  STORAGE_LOG(INFO, "page buddy", K(page_buddy_2));
  ASSERT_EQ(true, page_buddy_2.is_empty());

  for (int32_t i = 1; i < 129; i++) {
    ObTmpFilePageBuddy page_buddy_3;
    int32_t page_num_2 = i;
    ret = page_buddy_3.init(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = page_buddy_3.alloc(page_num_2, start_page_id, alloced_page_nums);
    ASSERT_EQ(OB_SUCCESS, ret);
    page_buddy_3.free(start_page_id, alloced_page_nums);
    STORAGE_LOG(INFO, "page buddy", K(page_buddy_3));
    ASSERT_EQ(true, page_buddy_3.is_empty());
    STORAGE_LOG(INFO, "page buddy", K(page_buddy_3));
  }

  ObTmpFilePageBuddy page_buddy_4;
  ret = page_buddy_4.init(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, page_buddy_4.is_empty());

  page_nums = 2;
  alloced_page_nums = -1;
  start_page_id = -1;
  ASSERT_EQ(true, page_buddy_4.is_empty());
  ret = page_buddy_4.alloc(page_nums, start_page_id, alloced_page_nums);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(alloced_page_nums, page_nums);
  ASSERT_EQ(false, page_buddy_4.is_empty());
}

TEST_F(TestTmpFile, test_tmp_file_sync)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTmpFileManager::get_instance().open(fd, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t write_size = 16*1024;
  char *write_buf = (char *)malloc(write_size);
  for (int64_t i = 0; i < write_size; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  io_info.fd_ = fd;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_group_id(THIS_WORKER.get_group_id());
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  const int64_t timeout_ms = 5000;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  free(write_buf);

  STORAGE_LOG(INFO, "test_tmp_file_sync");
  STORAGE_LOG(INFO, "io time", K(write_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  ASSERT_EQ(1, store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.size());
  ObTmpFileManager::get_instance().sync(fd, 5000);
  ASSERT_EQ(0, store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.size());

  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd);
}

TEST_F(TestTmpFile, test_tmp_file_sync_same_block)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd1, fd2 = -1;
  const int64_t timeout_ms = 5000;
  ObTmpFileIOHandle handle;
  ObTmpFileIOInfo io_info;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_group_id(THIS_WORKER.get_group_id());
  io_info.io_desc_.set_wait_event(2);
  int64_t write_size = 16 *1024;
  char *write_buf = (char *)malloc(write_size);
  for (int64_t i = 0; i < write_size; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;

  ret = ObTmpFileManager::get_instance().alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObTmpFileManager::get_instance().open(fd1, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info.fd_ = fd1;
  int64_t write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObTmpFileManager::get_instance().open(fd2, dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  io_info.fd_ = fd2;
  write_time = ObTimeUtility::current_time();
  ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);

  free(write_buf);

  STORAGE_LOG(INFO, "test_tmp_file_sync_same_block");
  STORAGE_LOG(INFO, "io time", K(write_time));
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);
  ASSERT_EQ(1, store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.size());
  ObTmpFileManager::get_instance().sync(fd1, 5000);
  ASSERT_EQ(1, store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.size());
  ObTmpFileManager::get_instance().sync(fd2, 5000);
  ASSERT_EQ(0, store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.size());

  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  ObTmpFileManager::get_instance().remove(fd1);
  ObTmpFileManager::get_instance().remove(fd2);
}

TEST_F(TestTmpFile, test_tmp_file_wash)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ms = 5000;
  int count = 64 * 0.8;
  int64_t dir = -1;
  int64_t fd = -1;
  ObTmpFileIOHandle handle;
  ObTmpFileIOInfo io_info, io_info_2;
  io_info.tenant_id_ = 1;
  io_info.io_desc_.set_group_id(THIS_WORKER.get_group_id());
  io_info.io_desc_.set_wait_event(2);
  int64_t write_size = 1024 *1024;
  char *write_buf = (char *)malloc(write_size);
  for (int64_t i = 0; i < write_size; ++i) {
    write_buf[i] = static_cast<char>(i % 256);
  }
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;

  io_info_2 = io_info;

  int64_t write_size_2 = 2016 *1024;
  char *write_buf_2 = (char *)malloc(write_size_2);
  for (int64_t i = 0; i < write_size_2; ++i) {
    write_buf_2[i] = static_cast<char>(i % 256);
  }
  io_info_2.buf_ = write_buf_2;
  io_info_2.size_ = write_size_2;

  STORAGE_LOG(INFO, "test_tmp_file_wash");
  ObTmpTenantFileStoreHandle store_handle;
  OB_TMP_FILE_STORE.get_store(1, store_handle);

  for (int64_t i=0; i<count; i++) {
    ret = ObTmpFileManager::get_instance().alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ObTmpFileManager::get_instance().open(fd, dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (i == count/2) {
      // This macro block will be freed immediately because its memory has been exhausted.
      io_info_2.fd_ = fd;
      ret = ObTmpFileManager::get_instance().write(io_info_2, timeout_ms);
      ASSERT_EQ(OB_SUCCESS, ret);
    } else {
      io_info.fd_ = fd;
      ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }

  int64_t oldest_id = -1;
  int64_t oldest_time = INT64_MAX;
  int64_t newest_time = -1;
  int64_t newest_id = -1;
  int64_t used_up_id = -1;
  ObTmpTenantMemBlockManager::TmpMacroBlockMap::iterator iter;
  for (iter = store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.begin();
      iter != store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.end(); ++iter) {
    int64_t alloc_time = iter->second->get_alloc_time();
    if (alloc_time < oldest_time) {
      oldest_id = iter->first;
      oldest_time = alloc_time;
    }
    if (alloc_time > newest_time) {
      newest_id = iter->first;
      newest_time = alloc_time;
    }
  }
  ObTmpMacroBlock* wash_block;
  ret = store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.get_refactored(newest_id, wash_block);
  ASSERT_EQ(OB_SUCCESS, ret);
  wash_block->alloc_time_ = wash_block->alloc_time_ - 60 * 1000000L;

  ObArray<ObTmpMacroBlock*> free_blocks;
  // 1 macro block has been disked immediately because its memory has been exhausted.
  ASSERT_EQ(count-1, store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.size());

  for (int64_t i=0; i< 3; i++) {
    ret = ObTmpFileManager::get_instance().alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ObTmpFileManager::get_instance().open(fd, dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    io_info.fd_ = fd;
    ret = ObTmpFileManager::get_instance().write(io_info, timeout_ms);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  store_handle.get_tenant_store()->tmp_mem_block_manager_.cleanup();

  std::chrono::milliseconds(50);
  ret = store_handle.get_tenant_store()->tmp_mem_block_manager_.wait_write_finish(oldest_id, ObTmpTenantMemBlockManager::get_default_timeout_ms());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = store_handle.get_tenant_store()->tmp_mem_block_manager_.wait_write_finish(newest_id, ObTmpTenantMemBlockManager::get_default_timeout_ms());
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.get_refactored(oldest_id, wash_block);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.get_refactored(newest_id, wash_block);
  ASSERT_NE(OB_SUCCESS, ret);
  ASSERT_EQ(count, store_handle.get_tenant_store()->tmp_mem_block_manager_.t_mblk_map_.size());

  free(write_buf);
  free(write_buf_2);

  store_handle.get_tenant_store()->print_block_usage();
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

  count = 64 * 0.8 + 3;
  while (count--) {
    ret = ObTmpFileManager::get_instance().remove(count);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}


}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tmp_file.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tmp_file.log", true, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
