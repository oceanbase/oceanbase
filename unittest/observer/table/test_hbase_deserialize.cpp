/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <gtest/gtest_prod.h>
#include "lib/allocator/page_arena.h"
#include "share/table/ob_table.h"
#include "share/table/ob_table_object.h"
#include "share/table/ob_table_rpc_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::table;

class TestHbaseDeserialize : public ::testing::Test
{
public:
  TestHbaseDeserialize() = default;
  virtual ~TestHbaseDeserialize() = default;

private:
  FRIEND_TEST(TestHbaseDeserialize, reject_invalid_cell_counts);
  FRIEND_TEST(TestHbaseDeserialize, accept_valid_cell_layouts);
  FRIEND_TEST(TestHbaseDeserialize, reject_invalid_key_indexes);
  FRIEND_TEST(TestHbaseDeserialize, reject_invalid_cfrows_metadata);
  FRIEND_TEST(TestHbaseDeserialize, reject_missing_declared_cells);
  FRIEND_TEST(TestHbaseDeserialize, accept_valid_cfrows_metadata);
  FRIEND_TEST(TestHbaseDeserialize, reject_invalid_top_level_request_counts);
  FRIEND_TEST(TestHbaseDeserialize, reject_full_request_invalid_metadata);
  FRIEND_TEST(TestHbaseDeserialize, accept_valid_batch_request);
  FRIEND_TEST(TestHbaseDeserialize, reject_malformed_request_and_continue);

  enum { BUF_SIZE = 32768 };

  struct CfRowsEncodeSpec
  {
    int64_t declared_key_idx_count_;
    const int64_t *key_indexes_;
    int64_t encoded_key_idx_count_;
    int64_t declared_cell_count_count_;
    const int64_t *cell_counts_;
    int64_t encoded_cell_count_count_;
    int64_t encoded_nested_cell_count_;
    int64_t nested_cell_num_;
    int64_t nested_obj_count_;
  };

  struct RequestEncodeSpec
  {
    int64_t declared_key_count_;
    int64_t encoded_key_count_;
    int64_t declared_cfrows_count_;
    const CfRowsEncodeSpec *cfrows_specs_;
    int64_t encoded_cfrows_count_;
  };

  int append_unis_payload(char *buf,
                          const int64_t buf_len,
                          int64_t &pos,
                          const char *payload,
                          const int64_t payload_len);
  int encode_cell_body(char *buf,
                       const int64_t buf_len,
                       int64_t &pos,
                       const int64_t cell_num,
                       const int64_t encoded_obj_count);
  int encode_cell(char *buf,
                  const int64_t buf_len,
                  int64_t &pos,
                  const int64_t cell_num,
                  const int64_t encoded_obj_count);
  int encode_cfrows_body(char *buf,
                         const int64_t buf_len,
                         int64_t &pos,
                         const int64_t declared_key_idx_count,
                         const int64_t *key_indexes,
                         const int64_t encoded_key_idx_count,
                         const int64_t declared_cell_count_count,
                         const int64_t *cell_counts,
                         const int64_t encoded_cell_count_count,
                         const int64_t encoded_nested_cell_count,
                         const int64_t nested_cell_num,
                         const int64_t nested_obj_count);
  int encode_cfrows(char *buf,
                    const int64_t buf_len,
                    int64_t &pos,
                    const int64_t declared_key_idx_count,
                    const int64_t *key_indexes,
                    const int64_t encoded_key_idx_count,
                    const int64_t declared_cell_count_count,
                    const int64_t *cell_counts,
                    const int64_t encoded_cell_count_count,
                    const int64_t encoded_nested_cell_count,
                    const int64_t nested_cell_num,
                    const int64_t nested_obj_count);
  int encode_request(char *buf,
                     const int64_t buf_len,
                     int64_t &pos,
                     const RequestEncodeSpec &spec);
  int encode_request(char *buf,
                     const int64_t buf_len,
                     int64_t &pos,
                     const int64_t declared_cell_count,
                     const int64_t encoded_nested_cell_count,
                     const int64_t nested_cell_num,
                     const int64_t nested_obj_count);
  int prepare_keys(ObArenaAllocator &allocator,
                   const int64_t key_count,
                   ObFixedArray<ObObj, ObIAllocator> &keys);

  DISALLOW_COPY_AND_ASSIGN(TestHbaseDeserialize);
};

int TestHbaseDeserialize::append_unis_payload(char *buf,
                                              const int64_t buf_len,
                                              int64_t &pos,
                                              const char *payload,
                                              const int64_t payload_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_ISNULL(payload) || payload_len < 0 || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, 1))) {
  } else if (OB_FAIL(serialization::encode_fixed_bytes_i64(buf, buf_len, pos, payload_len))) {
  } else if (payload_len > buf_len - pos) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    MEMCPY(buf + pos, payload, payload_len);
    pos += payload_len;
  }
  return ret;
}

int TestHbaseDeserialize::encode_cell_body(char *buf,
                                           const int64_t buf_len,
                                           int64_t &pos,
                                           const int64_t cell_num,
                                           const int64_t encoded_obj_count)
{
  int ret = OB_SUCCESS;
  ObObj qualifier;
  ObObj timestamp;
  ObObj value;
  ObObj ttl;
  qualifier.set_varbinary(ObString::make_string("qualifier"));
  timestamp.set_int(123456);
  value.set_varbinary(ObString::make_string("value"));
  ttl.set_int(86400);
  const ObObj *objects[] = {&qualifier, &timestamp, &value, &ttl};

  if (encoded_obj_count < 0 || encoded_obj_count > ARRAYSIZEOF(objects)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, cell_num))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < encoded_obj_count; ++i) {
      if (OB_FAIL(ObTableSerialUtil::serialize(buf, buf_len, pos, *objects[i]))) {
      }
    }
  }
  return ret;
}

int TestHbaseDeserialize::encode_cell(char *buf,
                                      const int64_t buf_len,
                                      int64_t &pos,
                                      const int64_t cell_num,
                                      const int64_t encoded_obj_count)
{
  int ret = OB_SUCCESS;
  char payload[512];
  int64_t payload_pos = 0;
  if (OB_FAIL(encode_cell_body(payload, sizeof(payload), payload_pos, cell_num, encoded_obj_count))) {
  } else if (OB_FAIL(append_unis_payload(buf, buf_len, pos, payload, payload_pos))) {
  }
  return ret;
}

int TestHbaseDeserialize::encode_cfrows_body(char *buf,
                                             const int64_t buf_len,
                                             int64_t &pos,
                                             const int64_t declared_key_idx_count,
                                             const int64_t *key_indexes,
                                             const int64_t encoded_key_idx_count,
                                             const int64_t declared_cell_count_count,
                                             const int64_t *cell_counts,
                                             const int64_t encoded_cell_count_count,
                                             const int64_t encoded_nested_cell_count,
                                             const int64_t nested_cell_num,
                                             const int64_t nested_obj_count)
{
  int ret = OB_SUCCESS;
  const ObString real_table_name = ObString::make_string("htable$cf");
  if (encoded_key_idx_count < 0
      || encoded_cell_count_count < 0
      || encoded_nested_cell_count < 0
      || encoded_nested_cell_count > 16
      || (encoded_key_idx_count > 0 && OB_ISNULL(key_indexes))
      || (encoded_cell_count_count > 0 && OB_ISNULL(cell_counts))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, real_table_name))) {
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, declared_key_idx_count))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < encoded_key_idx_count; ++i) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, key_indexes[i]))) {
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, declared_cell_count_count))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < encoded_cell_count_count; ++i) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, cell_counts[i]))) {
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < encoded_nested_cell_count; ++i) {
      if (OB_FAIL(encode_cell(buf, buf_len, pos, nested_cell_num, nested_obj_count))) {
      }
    }
  }
  return ret;
}

int TestHbaseDeserialize::encode_cfrows(char *buf,
                                        const int64_t buf_len,
                                        int64_t &pos,
                                        const int64_t declared_key_idx_count,
                                        const int64_t *key_indexes,
                                        const int64_t encoded_key_idx_count,
                                        const int64_t declared_cell_count_count,
                                        const int64_t *cell_counts,
                                        const int64_t encoded_cell_count_count,
                                        const int64_t encoded_nested_cell_count,
                                        const int64_t nested_cell_num,
                                        const int64_t nested_obj_count)
{
  int ret = OB_SUCCESS;
  char payload[BUF_SIZE];
  int64_t payload_pos = 0;
  if (OB_FAIL(encode_cfrows_body(payload,
                                sizeof(payload),
                                payload_pos,
                                declared_key_idx_count,
                                key_indexes,
                                encoded_key_idx_count,
                                declared_cell_count_count,
                                cell_counts,
                                encoded_cell_count_count,
                                encoded_nested_cell_count,
                                nested_cell_num,
                                nested_obj_count))) {
  } else if (OB_FAIL(append_unis_payload(buf, buf_len, pos, payload, payload_pos))) {
  }
  return ret;
}

int TestHbaseDeserialize::encode_request(char *buf,
                                         const int64_t buf_len,
                                         int64_t &pos,
                                         const RequestEncodeSpec &spec)
{
  int ret = OB_SUCCESS;
  char payload[BUF_SIZE];
  int64_t payload_pos = 0;
  const ObString credential;
  const ObString table_name = ObString::make_string("htable");
  const uint64_t option_flag = 0;
  const ObTableOperationType::Type op_type = ObTableOperationType::INSERT_OR_UPDATE;
  const OHOperationType hbase_op_type = OHOperationType::PUT;
  const char *row_keys[] = {"row-0", "row-1"};

  if (spec.encoded_key_count_ < 0
      || spec.encoded_key_count_ > ARRAYSIZEOF(row_keys)
      || spec.encoded_cfrows_count_ < 0
      || spec.encoded_cfrows_count_ > 16
      || (spec.encoded_cfrows_count_ > 0 && OB_ISNULL(spec.cfrows_specs_))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode(payload, sizeof(payload), payload_pos, credential))) {
  } else if (OB_FAIL(serialization::encode(payload, sizeof(payload), payload_pos, table_name))) {
  } else if (OB_FAIL(serialization::encode(payload, sizeof(payload), payload_pos, option_flag))) {
  } else if (OB_FAIL(serialization::encode(payload, sizeof(payload), payload_pos, op_type))) {
  } else if (OB_FAIL(serialization::encode_vi64(
                 payload, sizeof(payload), payload_pos, spec.declared_key_count_))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < spec.encoded_key_count_; ++i) {
      ObObj key;
      key.set_varbinary(ObString::make_string(row_keys[i]));
      if (OB_FAIL(ObTableSerialUtil::serialize(payload, sizeof(payload), payload_pos, key))) {
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(serialization::encode_vi64(
               payload, sizeof(payload), payload_pos, spec.declared_cfrows_count_))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < spec.encoded_cfrows_count_; ++i) {
      const CfRowsEncodeSpec &cfrows_spec = spec.cfrows_specs_[i];
      if (OB_FAIL(encode_cfrows(payload,
                                sizeof(payload),
                                payload_pos,
                                cfrows_spec.declared_key_idx_count_,
                                cfrows_spec.key_indexes_,
                                cfrows_spec.encoded_key_idx_count_,
                                cfrows_spec.declared_cell_count_count_,
                                cfrows_spec.cell_counts_,
                                cfrows_spec.encoded_cell_count_count_,
                                cfrows_spec.encoded_nested_cell_count_,
                                cfrows_spec.nested_cell_num_,
                                cfrows_spec.nested_obj_count_))) {
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(serialization::encode(payload, sizeof(payload), payload_pos, hbase_op_type))) {
    } else if (OB_SUCC(ret)
               && OB_FAIL(append_unis_payload(buf, buf_len, pos, payload, payload_pos))) {
    }
  }
  return ret;
}

int TestHbaseDeserialize::encode_request(char *buf,
                                         const int64_t buf_len,
                                         int64_t &pos,
                                         const int64_t declared_cell_count,
                                         const int64_t encoded_nested_cell_count,
                                         const int64_t nested_cell_num,
                                         const int64_t nested_obj_count)
{
  const int64_t key_idx = 0;
  const CfRowsEncodeSpec cfrows_spec = {
      1,
      &key_idx,
      1,
      1,
      &declared_cell_count,
      1,
      encoded_nested_cell_count,
      nested_cell_num,
      nested_obj_count};
  const RequestEncodeSpec request_spec = {1, 1, 1, &cfrows_spec, 1};
  return encode_request(buf, buf_len, pos, request_spec);
}

int TestHbaseDeserialize::prepare_keys(ObArenaAllocator &allocator,
                                       const int64_t key_count,
                                       ObFixedArray<ObObj, ObIAllocator> &keys)
{
  int ret = OB_SUCCESS;
  keys.set_allocator(&allocator);
  if (OB_FAIL(keys.prepare_allocate(key_count))) {
  } else {
    for (int64_t i = 0; i < key_count; ++i) {
      if (i == 0) {
        keys.at(i).set_varbinary(ObString::make_string("row-0"));
      } else {
        keys.at(i).set_varbinary(ObString::make_string("row-1"));
      }
    }
  }
  return ret;
}

TEST_F(TestHbaseDeserialize, reject_invalid_cell_counts)
{
  const int64_t invalid_counts[] = {-1, 0, 1, 2, 5, INT64_MAX};
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_counts); ++i) {
    char buf[BUF_SIZE];
    int64_t data_len = 0;
    const int64_t encoded_obj_count = invalid_counts[i] == -1 ? 3 : 0;
    ASSERT_EQ(OB_SUCCESS,
              encode_cell_body(buf, sizeof(buf), data_len, invalid_counts[i], encoded_obj_count));

    ObArenaAllocator allocator;
    ObHCell cell;
    cell.set_allocator(&allocator);
    int64_t pos = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, cell.deserialize_(buf, data_len, pos));
    EXPECT_EQ(0, cell.count());
  }
}

TEST_F(TestHbaseDeserialize, accept_valid_cell_layouts)
{
  const int64_t valid_counts[] = {3, 4};
  for (int64_t i = 0; i < ARRAYSIZEOF(valid_counts); ++i) {
    char buf[BUF_SIZE];
    int64_t data_len = 0;
    ASSERT_EQ(OB_SUCCESS,
              encode_cell_body(buf, sizeof(buf), data_len, valid_counts[i], valid_counts[i]));

    ObArenaAllocator allocator;
    ObHCell cell;
    cell.set_allocator(&allocator);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, cell.deserialize_(buf, data_len, pos));
    EXPECT_EQ(data_len, pos);
    EXPECT_EQ(valid_counts[i] + 1, cell.count());

    ObObj obj;
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_K, obj));
    EXPECT_TRUE(obj.is_null());
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_Q, obj));
    EXPECT_EQ(0, ObString::make_string("qualifier").compare(obj.get_string()));
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_T, obj));
    EXPECT_EQ(123456, obj.get_int());
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_V, obj));
    EXPECT_EQ(0, ObString::make_string("value").compare(obj.get_string()));
    if (valid_counts[i] == 4) {
      ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_TTL, obj));
      EXPECT_EQ(86400, obj.get_int());
    } else {
      EXPECT_EQ(nullptr, cell.get_cell_obj(ObHTableConstants::COL_IDX_TTL));
    }
  }
}

TEST_F(TestHbaseDeserialize, reject_invalid_key_indexes)
{
  const int64_t invalid_indexes[] = {-1, 1, 2, INT64_MAX};
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_indexes); ++i) {
    const int64_t cell_count = 1;
    char buf[BUF_SIZE];
    int64_t data_len = 0;
    ASSERT_EQ(OB_SUCCESS,
              encode_cfrows_body(buf,
                                 sizeof(buf),
                                 data_len,
                                 1,
                                 &invalid_indexes[i],
                                 1,
                                 1,
                                 &cell_count,
                                 1,
                                 0,
                                 3,
                                 3));

    ObArenaAllocator allocator;
    ObFixedArray<ObObj, ObIAllocator> keys;
    ASSERT_EQ(OB_SUCCESS, prepare_keys(allocator, 1, keys));
    ObHCfRows rows;
    rows.deserialize_alloc_ = &allocator;
    rows.rows_.set_allocator(&allocator);
    rows.set_keys(&keys);
    int64_t pos = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, rows.deserialize_(buf, data_len, pos));
    EXPECT_EQ(0, rows.count());
    EXPECT_EQ(0, rows.cell_count_);
  }
}

TEST_F(TestHbaseDeserialize, reject_invalid_cfrows_metadata)
{
  const int64_t key_idx = 0;
  const int64_t negative_cell_count = -1;
  const int64_t zero_cell_count = 0;
  const int64_t oversized_cell_count = UINT32_MAX;
  const int64_t overflow_key_indexes[] = {0, 0};
  const int64_t overflow_cell_counts[] = {INT64_MAX, 1};

  struct MetadataCase
  {
    int64_t declared_key_idx_count_;
    const int64_t *key_indexes_;
    int64_t encoded_key_idx_count_;
    int64_t declared_cell_count_count_;
    const int64_t *cell_counts_;
    int64_t encoded_cell_count_count_;
  };
  const MetadataCase cases[] = {
      {1, &key_idx, 1, 1, &negative_cell_count, 1},
      {1, &key_idx, 1, 1, &zero_cell_count, 1},
      {1, &key_idx, 1, 1, &oversized_cell_count, 1},
      {1, &key_idx, 1, 0, nullptr, 0},
      {1, &key_idx, 1, -1, nullptr, 0},
      {-1, nullptr, 0, 0, nullptr, 0},
      {0, nullptr, 0, 0, nullptr, 0},
      {INT64_MAX, nullptr, 0, 0, nullptr, 0},
      {2, overflow_key_indexes, 2, 2, overflow_cell_counts, 2}};

  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    char buf[BUF_SIZE];
    int64_t data_len = 0;
    ASSERT_EQ(OB_SUCCESS,
              encode_cfrows_body(buf,
                                 sizeof(buf),
                                 data_len,
                                 cases[i].declared_key_idx_count_,
                                 cases[i].key_indexes_,
                                 cases[i].encoded_key_idx_count_,
                                 cases[i].declared_cell_count_count_,
                                 cases[i].cell_counts_,
                                 cases[i].encoded_cell_count_count_,
                                 0,
                                 3,
                                 3));

    ObArenaAllocator allocator;
    ObFixedArray<ObObj, ObIAllocator> keys;
    ASSERT_EQ(OB_SUCCESS, prepare_keys(allocator, 1, keys));
    ObHCfRows rows;
    rows.deserialize_alloc_ = &allocator;
    rows.rows_.set_allocator(&allocator);
    rows.set_keys(&keys);
    int64_t pos = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, rows.deserialize_(buf, data_len, pos));
    EXPECT_EQ(0, rows.count());
    EXPECT_EQ(0, rows.cell_count_);
  }
}

TEST_F(TestHbaseDeserialize, reject_missing_declared_cells)
{
  const int64_t key_idx = 0;
  const int64_t declared_cell_count = 2;
  char buf[BUF_SIZE];
  int64_t data_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            encode_cfrows_body(buf,
                               sizeof(buf),
                               data_len,
                               1,
                               &key_idx,
                               1,
                               1,
                               &declared_cell_count,
                               1,
                               1,
                               3,
                               3));

  ObArenaAllocator allocator;
  ObFixedArray<ObObj, ObIAllocator> keys;
  ASSERT_EQ(OB_SUCCESS, prepare_keys(allocator, 1, keys));
  ObHCfRows rows;
  rows.deserialize_alloc_ = &allocator;
  rows.rows_.set_allocator(&allocator);
  rows.set_keys(&keys);
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, rows.deserialize_(buf, data_len, pos));
  EXPECT_EQ(data_len, pos);
  EXPECT_EQ(0, rows.cell_count_);
}

TEST_F(TestHbaseDeserialize, accept_valid_cfrows_metadata)
{
  const int64_t key_indexes[] = {1, 0};
  const int64_t cell_counts[] = {1, 2};
  char buf[BUF_SIZE];
  int64_t data_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            encode_cfrows_body(buf,
                               sizeof(buf),
                               data_len,
                               ARRAYSIZEOF(key_indexes),
                               key_indexes,
                               ARRAYSIZEOF(key_indexes),
                               ARRAYSIZEOF(cell_counts),
                               cell_counts,
                               ARRAYSIZEOF(cell_counts),
                               3,
                               3,
                               3));

  ObArenaAllocator allocator;
  ObFixedArray<ObObj, ObIAllocator> keys;
  ASSERT_EQ(OB_SUCCESS, prepare_keys(allocator, 2, keys));
  ObHCfRows rows;
  rows.deserialize_alloc_ = &allocator;
  rows.rows_.set_allocator(&allocator);
  rows.set_keys(&keys);
  rows.now_ms_ = 999;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, rows.deserialize_(buf, data_len, pos));
  EXPECT_EQ(data_len, pos);
  ASSERT_EQ(2, rows.count());
  EXPECT_EQ(3, rows.cell_count_);
  EXPECT_EQ(1, rows.get_cf_row(0).key_index_);
  EXPECT_EQ(1, rows.get_cf_row(0).cells_.count());
  EXPECT_EQ(0, rows.get_cf_row(1).key_index_);
  EXPECT_EQ(2, rows.get_cf_row(1).cells_.count());

  ObObj row_key;
  ASSERT_EQ(OB_SUCCESS,
            rows.get_cf_row(0).get_cell(0).get_cell_obj(ObHTableConstants::COL_IDX_K, row_key));
  EXPECT_EQ(0, ObString::make_string("row-1").compare(row_key.get_string()));
  ASSERT_EQ(OB_SUCCESS,
            rows.get_cf_row(1).get_cell(0).get_cell_obj(ObHTableConstants::COL_IDX_K, row_key));
  EXPECT_EQ(0, ObString::make_string("row-0").compare(row_key.get_string()));
}

TEST_F(TestHbaseDeserialize, reject_invalid_top_level_request_counts)
{
  struct TopLevelCase
  {
    int64_t declared_key_count_;
    int64_t encoded_key_count_;
    int64_t declared_cfrows_count_;
    int64_t encoded_cfrows_count_;
  };
  const int64_t over_uint32 = static_cast<int64_t>(UINT32_MAX) + 1;
  const TopLevelCase cases[] = {
      {-1, 0, 1, 0},
      {0, 0, 1, 0},
      {UINT32_MAX, 0, 1, 0},
      {over_uint32, 0, 1, 0},
      {1, 1, -1, 0},
      {1, 1, 0, 0},
      {1, 1, UINT32_MAX, 0},
      {1, 1, over_uint32, 0}};

  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    const RequestEncodeSpec request_spec = {
        cases[i].declared_key_count_,
        cases[i].encoded_key_count_,
        cases[i].declared_cfrows_count_,
        nullptr,
        cases[i].encoded_cfrows_count_};
    char buf[BUF_SIZE];
    int64_t data_len = 0;
    ASSERT_EQ(OB_SUCCESS, encode_request(buf, sizeof(buf), data_len, request_spec));

    ObArenaAllocator allocator;
    ObHbaseRpcRequest request;
    request.set_deserialize_allocator(&allocator);
    int64_t pos = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, request.deserialize(buf, data_len, pos));
  }

  char valid_buf[BUF_SIZE];
  int64_t valid_len = 0;
  ASSERT_EQ(OB_SUCCESS, encode_request(valid_buf, sizeof(valid_buf), valid_len, 1, 1, 3, 3));
  ObHbaseRpcRequest request_without_allocator;
  int64_t valid_pos = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            request_without_allocator.deserialize(valid_buf, valid_len, valid_pos));
}

TEST_F(TestHbaseDeserialize, reject_full_request_invalid_metadata)
{
  const int64_t key_idx = 0;
  const int64_t negative_key_idx = -1;
  const int64_t out_of_range_key_idx = 1;
  const int64_t larger_out_of_range_key_idx = 2;
  const int64_t one_cell = 1;
  const int64_t zero_cells = 0;
  const int64_t negative_cells = -1;
  const int64_t oversized_cells = UINT32_MAX;
  const int64_t two_cells = 2;
  const int64_t mismatched_cell_counts[] = {1, 1};
  const CfRowsEncodeSpec cases[] = {
      {1, &negative_key_idx, 1, 1, &one_cell, 1, 0, 3, 3},
      {1, &out_of_range_key_idx, 1, 1, &one_cell, 1, 0, 3, 3},
      {1, &larger_out_of_range_key_idx, 1, 1, &one_cell, 1, 0, 3, 3},
      {-1, nullptr, 0, 0, nullptr, 0, 0, 3, 3},
      {0, nullptr, 0, 0, nullptr, 0, 0, 3, 3},
      {UINT32_MAX, nullptr, 0, 0, nullptr, 0, 0, 3, 3},
      {1, &key_idx, 1, -1, nullptr, 0, 0, 3, 3},
      {1, &key_idx, 1, 0, nullptr, 0, 0, 3, 3},
      {1, &key_idx, 1, 2, mismatched_cell_counts, 2, 0, 3, 3},
      {1, &key_idx, 1, 1, &zero_cells, 1, 0, 3, 3},
      {1, &key_idx, 1, 1, &negative_cells, 1, 0, 3, 3},
      {1, &key_idx, 1, 1, &oversized_cells, 1, 0, 3, 3},
      {1, &key_idx, 1, 1, &two_cells, 1, 1, 3, 3},
      {1, &key_idx, 1, 1, &one_cell, 1, 1, 0, 0}};

  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    const RequestEncodeSpec request_spec = {1, 1, 1, &cases[i], 1};
    char buf[BUF_SIZE];
    int64_t data_len = 0;
    ASSERT_EQ(OB_SUCCESS, encode_request(buf, sizeof(buf), data_len, request_spec));

    ObArenaAllocator allocator;
    ObHbaseRpcRequest request;
    request.set_deserialize_allocator(&allocator);
    int64_t pos = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, request.deserialize(buf, data_len, pos));
  }
}

TEST_F(TestHbaseDeserialize, accept_valid_batch_request)
{
  const int64_t key_indexes[] = {1, 0, 1};
  const int64_t cell_counts[] = {1, 1, 1};
  const CfRowsEncodeSpec cfrows_spec = {
      ARRAYSIZEOF(key_indexes),
      key_indexes,
      ARRAYSIZEOF(key_indexes),
      ARRAYSIZEOF(cell_counts),
      cell_counts,
      ARRAYSIZEOF(cell_counts),
      3,
      4,
      4};
  const RequestEncodeSpec request_spec = {2, 2, 1, &cfrows_spec, 1};
  char buf[BUF_SIZE];
  int64_t data_len = 0;
  ASSERT_EQ(OB_SUCCESS, encode_request(buf, sizeof(buf), data_len, request_spec));

  ObArenaAllocator allocator;
  ObHbaseRpcRequest request;
  request.set_deserialize_allocator(&allocator);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, request.deserialize(buf, data_len, pos));
  EXPECT_EQ(data_len, pos);
  ASSERT_EQ(2, request.keys_.count());
  ASSERT_EQ(1, request.cf_rows_.count());
  const ObHCfRows &rows = request.cf_rows_.at(0);
  ASSERT_EQ(ARRAYSIZEOF(key_indexes), rows.count());
  EXPECT_EQ(ARRAYSIZEOF(cell_counts), rows.cell_count_);

  const char *expected_row_keys[] = {"row-1", "row-0", "row-1"};
  for (int64_t i = 0; i < rows.count(); ++i) {
    const ObHCfRow &row = rows.get_cf_row(i);
    ASSERT_EQ(key_indexes[i], row.key_index_);
    ASSERT_EQ(1, row.cells_.count());
    const ObHCell &cell = row.get_cell(0);
    EXPECT_EQ(ObHTableConstants::COL_IDX_TTL + 1, cell.count());

    ObObj obj;
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_K, obj));
    EXPECT_EQ(0, ObString::make_string(expected_row_keys[i]).compare(obj.get_string()));
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_Q, obj));
    EXPECT_EQ(0, ObString::make_string("qualifier").compare(obj.get_string()));
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_T, obj));
    EXPECT_EQ(123456, obj.get_int());
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_V, obj));
    EXPECT_EQ(0, ObString::make_string("value").compare(obj.get_string()));
    ASSERT_EQ(OB_SUCCESS, cell.get_cell_obj(ObHTableConstants::COL_IDX_TTL, obj));
    EXPECT_EQ(86400, obj.get_int());
  }
}

TEST_F(TestHbaseDeserialize, reject_malformed_request_and_continue)
{
  ObArenaAllocator allocator;
  const int64_t key_idx = 0;
  const int64_t cell_count = 1;
  const CfRowsEncodeSpec poc_cfrows_spec = {
      1, &key_idx, 1, 1, &cell_count, 1, 1, 3, 3};
  const RequestEncodeSpec poc_request_spec = {0, 0, 1, &poc_cfrows_spec, 1};
  char poc_buf[BUF_SIZE];
  int64_t poc_len = 0;
  ASSERT_EQ(OB_SUCCESS, encode_request(poc_buf, sizeof(poc_buf), poc_len, poc_request_spec));
  ObHbaseRpcRequest poc_request;
  poc_request.set_deserialize_allocator(&allocator);
  int64_t poc_pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, poc_request.deserialize(poc_buf, poc_len, poc_pos));

  char malformed_buf[BUF_SIZE];
  int64_t malformed_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            encode_request(malformed_buf, sizeof(malformed_buf), malformed_len, 1, 1, -1, 3));

  ObHbaseRpcRequest malformed_request;
  malformed_request.set_deserialize_allocator(&allocator);
  int64_t malformed_pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            malformed_request.deserialize(malformed_buf, malformed_len, malformed_pos));

  char truncated_buf[BUF_SIZE];
  int64_t truncated_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            encode_request(truncated_buf, sizeof(truncated_buf), truncated_len, 2, 1, 3, 3));
  ObHbaseRpcRequest truncated_request;
  truncated_request.set_deserialize_allocator(&allocator);
  int64_t truncated_pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            truncated_request.deserialize(truncated_buf, truncated_len, truncated_pos));
  EXPECT_TRUE(truncated_request.credential_.empty());

  char valid_buf[BUF_SIZE];
  int64_t valid_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            encode_request(valid_buf, sizeof(valid_buf), valid_len, 1, 1, 3, 3));
  ObHbaseRpcRequest valid_request;
  valid_request.set_deserialize_allocator(&allocator);
  int64_t valid_pos = 0;
  ASSERT_EQ(OB_SUCCESS, valid_request.deserialize(valid_buf, valid_len, valid_pos));
  EXPECT_EQ(valid_len, valid_pos);
  EXPECT_TRUE(valid_request.credential_.empty());
  ASSERT_EQ(1, valid_request.cf_rows_.count());
  ASSERT_EQ(1, valid_request.cf_rows_.at(0).count());
  EXPECT_EQ(1, valid_request.cf_rows_.at(0).get_cf_row(0).cells_.count());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_hbase_deserialize.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
