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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/vector_index/ob_plugin_vector_index_serialize.h"
#undef private
#undef protected


namespace oceanbase {
namespace common {
class TestVectorIndexSerialize : public ::testing::Test
{
public:
  TestVectorIndexSerialize()
  {}
  ~TestVectorIndexSerialize()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestVectorIndexSerialize);
};

struct TestOStreamCbParam : public share::ObOStreamBuf::CbParam {
  TestOStreamCbParam()
    : total_size_(0)
  {}
  virtual ~TestOStreamCbParam() {}
  int64_t total_size_;
};

class SerializeCallback {
public:
  SerializeCallback(int ret = OB_SUCCESS)
    : ret_(ret)
  {}
  int operator()(const char* data, const int64_t data_size, share::ObOStreamBuf::CbParam &cb_param) {
    if (OB_SUCCESS == ret_) {
      ObString str(data_size, data);
      LOG_INFO("output", K(str));
      TestOStreamCbParam* param = dynamic_cast<TestOStreamCbParam*>(&cb_param);
      param->total_size_ += data_size;
    }
    return ret_;
  }
private:
  int ret_;
};

static const char* data_str = "unittest: test plugin vector index serialize";

TEST_F(TestVectorIndexSerialize, serialize)
{
  const int MAX_BUF_SIZE = 8LL;
  char data[MAX_BUF_SIZE] = {0};
  TestOStreamCbParam cb_param;
  SerializeCallback callback;
  share::ObOStreamBuf::Callback func = callback;
  share::ObOStreamBuf streambuf(data, MAX_BUF_SIZE, cb_param, func);
  std::ostream out(&streambuf);
  out.write(data_str, strlen(data_str));
  streambuf.check_finish();
  ASSERT_EQ(OB_SUCCESS, streambuf.get_error_code());
  ASSERT_EQ(strlen(data_str), cb_param.total_size_);
}

TEST_F(TestVectorIndexSerialize, serialize_failed)
{
  const int MAX_BUF_SIZE = 8LL;
  char data[MAX_BUF_SIZE] = {0};
  TestOStreamCbParam cb_param;
  SerializeCallback callback(OB_ERR_UNEXPECTED);
  share::ObOStreamBuf::Callback func = callback;
  share::ObOStreamBuf streambuf(data, MAX_BUF_SIZE, cb_param, func);
  std::ostream out(&streambuf);
  out.write(data_str, strlen(data_str));
  ASSERT_EQ(OB_ERR_UNEXPECTED, streambuf.get_error_code());
  ASSERT_EQ(0, cb_param.total_size_);
}

struct TestIStreamCbParam : public share::ObIStreamBuf::CbParam {
  TestIStreamCbParam()
    : cur_(0)
  {}
  virtual ~TestIStreamCbParam() {}
  int64_t cur_;
};

class DeserializeCallback {
public:
  DeserializeCallback(int ret = OB_SUCCESS)
    : ret_(ret)
  {}
  int operator()(char*& data, const int64_t data_size, int64_t &read_size, share::ObIStreamBuf::CbParam &cb_param) {
    if (OB_SUCCESS == ret_) {
      TestIStreamCbParam* param = dynamic_cast<TestIStreamCbParam*>(&cb_param);
      read_size = MIN(strlen(data_str) - param->cur_, data_size);
      if (read_size) {
        data = const_cast<char*>(data_str + param->cur_);
        param->cur_ += read_size;
        ObString str(read_size, data);
        LOG_INFO("input", K(str));
      }
    }
    return ret_;
  }
private:
  int ret_;
};

TEST_F(TestVectorIndexSerialize, deserialize)
{
  const int MAX_BUF_SIZE = 8LL;
  char data[MAX_BUF_SIZE] = {0};
  TestIStreamCbParam cb_param;
  DeserializeCallback callback;
  share::ObIStreamBuf::Callback func = callback;
  share::ObIStreamBuf streambuf(data, MAX_BUF_SIZE, cb_param, func);
  std::istream in(&streambuf);
  char result[1024] = {0};
  in.read(result, strlen(data_str));
  ASSERT_EQ(OB_SUCCESS, streambuf.get_error_code());
  ASSERT_EQ(strlen(data_str), strlen(result));
}

TEST_F(TestVectorIndexSerialize, deserialize_failed)
{
  const int MAX_BUF_SIZE = 8LL;
  char data[MAX_BUF_SIZE] = {0};
  share::ObIStreamBuf::CbParam cb_param;
  DeserializeCallback callback(OB_ERR_UNEXPECTED);
  share::ObIStreamBuf::Callback func = callback;
  share::ObIStreamBuf streambuf(data, MAX_BUF_SIZE, cb_param, func);
  std::istream in(&streambuf);
  char result[1024] = {0};
  in.read(result, strlen(data_str));
  ASSERT_EQ(OB_ERR_UNEXPECTED, streambuf.get_error_code());
  ASSERT_EQ(0, strlen(result));
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}