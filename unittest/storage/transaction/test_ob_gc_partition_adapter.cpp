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
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "common/ob_partition_key.h"
#include "storage/transaction/ob_gc_partition_adapter.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace transaction;
namespace unittest {

class MyConnectionPool : public ObISQLConnectionPool {
public:
  MyConnectionPool()
  {}
  ~MyConnectionPool()
  {}

public:
  // sql string escape
  int escape(const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
  {
    UNUSEDx(from, from_size, to, to_size, out_size);
    return OB_SUCCESS;
  }

  // acquired connection must be released
  int acquire(ObISQLConnection*& conn, void* client_addr)
  {
    UNUSEDx(conn, client_addr);
    return OB_SUCCESS;
  }
  int release(ObISQLConnection* conn, const bool success)
  {
    UNUSEDx(conn, success);
    return OB_SUCCESS;
  }
  int on_client_inactive(void* client_addr)
  {
    UNUSEDx(client_addr);
    return OB_SUCCESS;
  }
};

class MyPartitionInfo {
public:
  MyPartitionInfo() : tenant_id_(0), table_id_(0), partition_id_(0)
  {}
  MyPartitionInfo(const int64_t tenant_id, const int64_t table_id, const int64_t partition_id)
      : tenant_id_(tenant_id), table_id_(table_id), partition_id_(partition_id)
  {}
  explicit MyPartitionInfo(const ObPartitionKey& pkey)
      : tenant_id_(pkey.get_tenant_id()), table_id_(pkey.get_table_id()), partition_id_(pkey.get_partition_id())
  {}
  ~MyPartitionInfo()
  {}
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_table_id() const
  {
    return table_id_;
  }
  int64_t get_partition_id() const
  {
    return partition_id_;
  }
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(partition_id));

private:
  int64_t tenant_id_;
  int64_t table_id_;
  int64_t partition_id_;
};

class MyResult : public ObMySQLResult {
public:
  MyResult() : index_(-1)
  {}
  ~MyResult()
  {}

public:
  void reset()
  {
    array_.reset();
    index_ = -1;
  }
  int push(const MyPartitionInfo& info)
  {
    return array_.push_back(info);
  }
  void rewind()
  {
    index_ = -1;
  }
  TO_STRING_KV(K_(array), K_(index));

public:
  int close()
  {
    return OB_SUCCESS;
  }
  int next()
  {
    int ret = OB_SUCCESS;
    index_++;
    if (array_.count() <= index_) {
      ret = OB_ITER_END;
    }
    return ret;
  }

  int print_info() const
  {
    return OB_SUCCESS;
  }

  int get_int(const int64_t col_idx, int64_t& int_val) const
  {
    int ret = OB_SUCCESS;
    if (0 > index_) {
      ret = OB_ERR_UNEXPECTED;
    } else if (array_.count() <= index_) {
      ret = OB_ITER_END;
    } else {
      const MyPartitionInfo& info = array_.at(index_);
      if (0 == col_idx) {
        int_val = info.get_tenant_id();
      } else if (1 == col_idx) {
        int_val = info.get_table_id();
      } else if (2 == col_idx) {
        int_val = info.get_partition_id();
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }
  int get_uint(const int64_t col_idx, uint64_t& int_val) const
  {
    UNUSEDx(col_idx, int_val);
    return OB_NOT_SUPPORTED;
  }
  int get_datetime(const int64_t col_idx, int64_t& datetime) const
  {
    UNUSEDx(col_idx, datetime);
    return OB_NOT_SUPPORTED;
  }
  int get_date(const int64_t col_idx, int32_t& date) const
  {
    UNUSEDx(col_idx, date);
    return OB_NOT_SUPPORTED;
  }
  int get_time(const int64_t col_idx, int64_t& time) const
  {
    UNUSEDx(col_idx, time);
    return OB_NOT_SUPPORTED;
  }
  int get_year(const int64_t col_idx, uint8_t& year) const
  {
    UNUSEDx(col_idx, year);
    return OB_NOT_SUPPORTED;
  }
  int get_bool(const int64_t col_idx, bool& bool_val) const
  {
    UNUSEDx(col_idx, bool_val);
    return OB_NOT_SUPPORTED;
  }
  int get_varchar(const int64_t col_idx, common::ObString& varchar_val) const
  {
    UNUSEDx(col_idx, varchar_val);
    return OB_NOT_SUPPORTED;
  }
  int get_raw(const int64_t col_idx, common::ObString& raw_val) const
  {
    UNUSEDx(col_idx, raw_val);
    return OB_NOT_SUPPORTED;
  }
  int get_float(const int64_t col_idx, float& float_val) const
  {
    UNUSEDx(col_idx, float_val);
    return OB_NOT_SUPPORTED;
  }
  int get_double(const int64_t col_idx, double& double_val) const
  {
    UNUSEDx(col_idx, double_val);
    return OB_NOT_SUPPORTED;
  }
  int get_timestamp(const int64_t col_idx, const common::ObTimeZoneInfo* tz_info, int64_t& int_val) const
  {
    UNUSEDx(col_idx, tz_info, int_val);
    return OB_NOT_SUPPORTED;
  }
  int get_otimestamp_value(const int64_t col_idx, const common::ObTimeZoneInfo& tz_info, const common::ObObjType type,
      common::ObOTimestampData& otimestamp_val) const
  {
    UNUSEDx(col_idx, tz_info, type, otimestamp_val);
    return OB_NOT_SUPPORTED;
  }
  int get_timestamp_tz(
      const int64_t col_idx, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const
  {
    UNUSEDx(col_idx, tz_info, otimestamp_val);
    return OB_NOT_SUPPORTED;
  }
  int get_timestamp_ltz(
      const int64_t col_idx, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const
  {
    UNUSEDx(col_idx, tz_info, otimestamp_val);
    return OB_NOT_SUPPORTED;
  }
  int get_timestamp_nano(
      const int64_t col_idx, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const
  {
    UNUSEDx(col_idx, tz_info, otimestamp_val);
    return OB_NOT_SUPPORTED;
  }
  int get_interval_ym(const int64_t col_idx, common::ObIntervalYMValue& val) const
  {
    UNUSEDx(col_idx, val);
    return OB_NOT_SUPPORTED;
  }
  int get_interval_ds(const int64_t col_idx, common::ObIntervalDSValue& val) const
  {
    UNUSEDx(col_idx, val);
    return OB_NOT_SUPPORTED;
  }
  int get_nvarchar2(const int64_t col_idx, ObString& val) const
  {
    UNUSEDx(col_idx, val);
    return OB_NOT_SUPPORTED;
  }
  int get_nchar(const int64_t col_idx, ObString& val) const
  {
    UNUSEDx(col_idx, val);
    return OB_NOT_SUPPORTED;
  }
  int get_int(const char* col_name, int64_t& int_val) const
  {
    UNUSEDx(col_name, int_val);
    return OB_NOT_SUPPORTED;
  }
  int get_uint(const char* col_name, uint64_t& int_val) const
  {
    UNUSEDx(col_name, int_val);
    return OB_NOT_SUPPORTED;
  }
  int get_datetime(const char* col_name, int64_t& datetime) const
  {
    UNUSEDx(col_name, datetime);
    return OB_NOT_SUPPORTED;
  }
  int get_date(const char* col_name, int32_t& date) const
  {
    UNUSEDx(col_name, date);
    return OB_NOT_SUPPORTED;
  }
  int get_time(const char* col_name, int64_t& time) const
  {
    UNUSEDx(col_name, time);
    return OB_NOT_SUPPORTED;
  }
  int get_year(const char* col_name, uint8_t& year) const
  {
    UNUSEDx(col_name, year);
    return OB_NOT_SUPPORTED;
  }
  int get_bool(const char* col_name, bool& bool_val) const
  {
    UNUSEDx(col_name, bool_val);
    return OB_NOT_SUPPORTED;
  }
  int get_varchar(const char* col_name, common::ObString& varchar_val) const
  {
    UNUSEDx(col_name, varchar_val);
    return OB_NOT_SUPPORTED;
  }
  int get_raw(const char* col_name, common::ObString& raw_val) const
  {
    UNUSEDx(col_name, raw_val);
    return OB_NOT_SUPPORTED;
  }
  int get_float(const char* col_name, float& float_val) const
  {
    UNUSEDx(col_name, float_val);
    return OB_NOT_SUPPORTED;
  }
  int get_double(const char* col_name, double& double_val) const
  {
    UNUSEDx(col_name, double_val);
    return OB_NOT_SUPPORTED;
  }
  int get_timestamp(const char* col_name, const common::ObTimeZoneInfo* tz_info, int64_t& int_val) const
  {
    UNUSEDx(col_name, tz_info, int_val);
    return OB_NOT_SUPPORTED;
  }
  int get_otimestamp_value(const char* col_name, const common::ObTimeZoneInfo& tz_info, const common::ObObjType type,
      common::ObOTimestampData& otimestamp_val) const
  {
    UNUSEDx(col_name, tz_info, type, otimestamp_val);
    return OB_NOT_SUPPORTED;
  }
  int get_timestamp_tz(
      const char* col_name, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const
  {
    UNUSEDx(col_name, tz_info, otimestamp_val);
    return OB_NOT_SUPPORTED;
  }
  int get_timestamp_ltz(
      const char* col_name, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const
  {
    UNUSEDx(col_name, tz_info, otimestamp_val);
    return OB_NOT_SUPPORTED;
  }
  int get_timestamp_nano(
      const char* col_name, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const
  {
    UNUSEDx(col_name, tz_info, otimestamp_val);
    return OB_NOT_SUPPORTED;
  }
  virtual int get_type(const int64_t col_idx, ObObjMeta& type) const override
  {
    UNUSEDx(col_idx, type);
    return OB_NOT_SUPPORTED;
  }
  virtual int get_obj(const int64_t col_idx, ObObj& obj, const common::ObTimeZoneInfo* tz_info = NULL,
      common::ObIAllocator* allocator = NULL) const override
  {
    UNUSEDx(col_idx, obj, tz_info, allocator);
    return OB_NOT_SUPPORTED;
  }
  virtual int get_type(const char* col_name, ObObjMeta& type) const override
  {
    UNUSEDx(col_name, type);
    return OB_NOT_SUPPORTED;
  }
  virtual int get_obj(const char* col_name, ObObj& obj) const override
  {
    UNUSEDx(col_name, obj);
    return OB_NOT_SUPPORTED;
  }
  int get_interval_ym(const char* col_name, common::ObIntervalYMValue& val) const
  {
    UNUSEDx(col_name, val);
    return OB_NOT_SUPPORTED;
  }
  int get_interval_ds(const char* col_name, common::ObIntervalDSValue& val) const
  {
    UNUSEDx(col_name, val);
    return OB_NOT_SUPPORTED;
  }
  int get_nvarchar2(const char* col_name, ObString& val) const
  {
    UNUSEDx(col_name, val);
    return OB_NOT_SUPPORTED;
  }
  int get_nchar(const char* col_name, ObString& val) const
  {
    UNUSEDx(col_name, val);
    return OB_NOT_SUPPORTED;
  }

private:
  int inner_get_number(const int64_t col_idx, common::number::ObNumber& nmb_val, IAllocator& allocator) const
  {
    UNUSEDx(col_idx, nmb_val, allocator);
    return OB_NOT_SUPPORTED;
  }
  int inner_get_number(const char* col_name, common::number::ObNumber& nmb_val, IAllocator& allocator) const
  {
    UNUSEDx(col_name, nmb_val, allocator);
    return OB_NOT_SUPPORTED;
  }

  int inner_get_urowid(const int64_t, common::ObURowIDData&, common::ObIAllocator&) const
  {
    return OB_NOT_SUPPORTED;
  }

  int inner_get_urowid(const char*, common::ObURowIDData&, common::ObIAllocator&) const
  {
    return OB_NOT_SUPPORTED;
  }

  int inner_get_lob_locator(const char*, common::ObLobLocator*&, common::ObIAllocator&) const
  {
    return OB_NOT_SUPPORTED;
  }
  int inner_get_lob_locator(const int64_t, common::ObLobLocator*&, common::ObIAllocator&) const
  {
    return OB_NOT_SUPPORTED;
  }

private:
  ObSEArray<MyPartitionInfo, 16> array_;
  int64_t index_;
};

class MyResultHandler : public ObISQLResultHandler {
public:
  explicit MyResultHandler(MyResult& my_result) : my_result_(my_result)
  {}
  ~MyResultHandler()
  {}

public:
  ObMySQLResult* mysql_result()
  {
    return &my_result_;
  }

private:
  MyResult& my_result_;
};

class MyProxy : public ObMySQLProxy {
public:
  MyProxy()
  {}
  ~MyProxy()
  {}

public:
  MyResult& get_result()
  {
    return my_result_;
  }

public:
  int read(ReadResult& res, const uint64_t tenant_id, const char* sql) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(tenant_id, sql);
    my_result_.rewind();
    MyResultHandler* my_handler = NULL;
    if (OB_FAIL(res.create_handler(my_handler, my_result_))) {
      TRANS_LOG(ERROR, "create handler failed", K(ret));
    }
    return ret;
  }

private:
  MyResult my_result_;
};

class TestObGcPartitionAdapter : public ::testing::Test {
public:
  virtual void SetUp()
  {
    GCTX.split_schema_version_ = OB_INVALID_VERSION;
  }
  virtual void TearDown()
  {}
};

//////////////////////basic function test//////////////////////////////////////////

TEST_F(TestObGcPartitionAdapter, check_partition_exist)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  MyProxy my_proxy;
  ObGCPartitionAdapter gc_adapter;
  EXPECT_EQ(OB_SUCCESS, gc_adapter.init(&my_proxy));
  EXPECT_EQ(OB_SUCCESS, gc_adapter.start());

  MyResult& my_result = my_proxy.get_result();
  const int64_t tenant_id1 = 1001;
  const int64_t tenant_id2 = 1002;
  const int64_t pure_id = 50011;
  const int64_t table_id1 = combine_id(tenant_id1, pure_id);
  const int64_t table_id2 = combine_id(tenant_id2, pure_id);
  const int64_t partition_id = 0;
  const int64_t partition_cnt = 0;
  const ObPartitionKey pkey1(table_id1, partition_id, partition_cnt);
  const ObPartitionKey pkey2(table_id1, partition_id + 1, partition_cnt);
  const ObPartitionKey pkey3(table_id2, partition_id, partition_cnt);
  const ObPartitionKey pkey4(table_id2, partition_id, partition_cnt + 1);
  bool exist1 = false;
  bool exist2 = false;
  bool exist3 = false;
  bool exist4 = false;

  EXPECT_EQ(OB_SUCCESS, my_result.push(MyPartitionInfo(pkey1)));
  EXPECT_EQ(OB_SUCCESS, my_result.push(MyPartitionInfo(pkey3)));

  EXPECT_EQ(OB_EAGAIN, gc_adapter.check_partition_exist(pkey1, exist1));
  EXPECT_EQ(OB_EAGAIN, gc_adapter.check_partition_exist(pkey2, exist2));
  EXPECT_EQ(OB_EAGAIN, gc_adapter.check_partition_exist(pkey3, exist3));
  EXPECT_EQ(OB_EAGAIN, gc_adapter.check_partition_exist(pkey4, exist4));
  sleep(1);
  EXPECT_EQ(OB_SUCCESS, gc_adapter.check_partition_exist(pkey1, exist1));
  EXPECT_EQ(OB_SUCCESS, gc_adapter.check_partition_exist(pkey2, exist2));
  EXPECT_EQ(OB_SUCCESS, gc_adapter.check_partition_exist(pkey3, exist3));
  EXPECT_EQ(OB_SUCCESS, gc_adapter.check_partition_exist(pkey4, exist4));
  EXPECT_TRUE(exist1);
  EXPECT_FALSE(exist2);
  EXPECT_TRUE(exist3);
  EXPECT_TRUE(exist4);

  gc_adapter.stop();
  gc_adapter.wait();
}

//////////////////////////boundary test/////////////////////////////////////////

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_gc_partition_adapter.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
