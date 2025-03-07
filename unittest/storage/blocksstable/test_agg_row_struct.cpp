// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <utility>
#include <string>
#include <cstring>
#include <sstream>
#define protected public
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#include "lib/ob_errno.h"
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"
#include "storage/blocksstable/index_block/ob_index_block_aggregator.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
using namespace std;

namespace unittest
{

class TestAggRow : public ::testing::Test
{
public:
  TestAggRow();
  virtual ~TestAggRow();
  virtual void SetUp();
  virtual void TearDown();
  void prepare_agg_row_writer(ObAggRowWriter &row_writer);
  void set_datum(ObStorageDatum &datum, const std::string &hexString, const int64_t len, const uint32_t pack);
  void prepare_skip_index_col_metas(ObIArray<oceanbase::blocksstable::ObSkipIndexColMeta> &metas);
  int check_serialized_aggregated_row(const ObIArray<oceanbase::blocksstable::ObSkipIndexColMeta> &full_agg_metas,
                                      const char *agg_row_buf,
                                      const int64_t agg_row_len,
                                      const int64_t row_count);

protected:
  ObArenaAllocator allocator_;
};

TestAggRow::TestAggRow()
    : allocator_()
{
}
TestAggRow::~TestAggRow()
{
}

void TestAggRow::SetUp()
{
}

void TestAggRow::TearDown()
{
}

int TestAggRow::check_serialized_aggregated_row(
    const ObIArray<oceanbase::blocksstable::ObSkipIndexColMeta> &full_agg_metas,
    const char *agg_row_buf,
    const int64_t agg_row_len,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  ObStorageDatum tmp_datum;
  ObStorageDatum tmp_null_datum;
  ObAggRowReader agg_reader;
  agg_reader.reset();
  uint64_t agg_crc = ob_crc64(agg_row_buf, agg_row_len);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(agg_reader.init(agg_row_buf, agg_row_len))) {
    LOG_WARN("Fail to init agg row reader", K(ret), KP(agg_row_buf), K(agg_row_len));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas.count(); ++i) {
      tmp_datum.reuse();
      tmp_null_datum.reuse();
      const ObSkipIndexColMeta &idx_col_meta = full_agg_metas.at(i);
      if (OB_FAIL(agg_reader.read(idx_col_meta, tmp_datum))) {
        LOG_WARN("Fail to read aggregated data", K(ret), K(idx_col_meta));
      } else if (OB_UNLIKELY(tmp_datum.is_ext())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected ext agg datum", K(ret), K(tmp_datum), K(idx_col_meta));
      } else if (tmp_datum.is_null()) {
        ObSkipIndexColMeta null_col_meta(idx_col_meta.col_idx_, SK_IDX_NULL_COUNT);
        if (OB_FAIL(agg_reader.read(null_col_meta, tmp_null_datum))) {
          LOG_WARN("Fail to read aggregated null", K(ret), K(idx_col_meta));
        } else if (tmp_null_datum.is_ext()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null count datum", K(ret), K(tmp_null_datum), K(idx_col_meta));
        } else if (tmp_null_datum.is_null()) {
          // do nothing.
        } else if (tmp_null_datum.get_int() > row_count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Unexpected null count datum out row count", K(ret),
              K(tmp_null_datum), K(row_count), K(null_col_meta), K(tmp_null_datum.get_int()));
          ob_abort();
        } else if (tmp_null_datum.get_int() < row_count) {
          // do nothing.
        }
      }
    }
  }
  return ret;
}


void TestAggRow::set_datum(ObStorageDatum &datum, const std::string& hexString, const int64_t len, const uint32_t pack)
{
  int ret = OB_SUCCESS;
  char * buffer = static_cast<char *>(allocator_.alloc(40));
  ASSERT_NE(nullptr, buffer);
  std::istringstream iss(hexString);
  std::string hexByte;
  size_t i = 0;
  // split the hex string with spaces
  if (len > 0) {
    while (iss >> hexByte) {
      if (i >= len) {
        std::cerr << "Buffer size is smaller than the number of hex values." << std::endl;
        return;
      }
      // read hexadecimal string, convert to int, and write into buffer
      unsigned int byteValue;
      std::stringstream ss;
      ss << std::hex << hexByte;
      ss >> byteValue;
      buffer[i++] = static_cast<char>(byteValue);
    }
  }
  // set ptr
  const char * ptr = buffer;
  datum.ptr_ = ptr;
  // set desc
  datum.pack_ = pack;
}

void TestAggRow::prepare_skip_index_col_metas(ObIArray<oceanbase::blocksstable::ObSkipIndexColMeta> & metas)
{
  int ret = OB_SUCCESS;
  ret = metas.reserve(21);
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 0
  ret = metas.push_back(ObSkipIndexColMeta(0, ObSkipIndexColType(0)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(0, ObSkipIndexColType(1)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(0, ObSkipIndexColType(2)));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 1
  ret = metas.push_back(ObSkipIndexColMeta(1, ObSkipIndexColType(0)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(1, ObSkipIndexColType(1)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(1, ObSkipIndexColType(2)));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 2
  ret = metas.push_back(ObSkipIndexColMeta(2, ObSkipIndexColType(0)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(2, ObSkipIndexColType(1)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(2, ObSkipIndexColType(2)));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 3
  ret = metas.push_back(ObSkipIndexColMeta(3, ObSkipIndexColType(0)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(3, ObSkipIndexColType(1)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(3, ObSkipIndexColType(2)));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 4
  ret = metas.push_back(ObSkipIndexColMeta(4, ObSkipIndexColType(0)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(4, ObSkipIndexColType(1)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(4, ObSkipIndexColType(2)));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 7
  ret = metas.push_back(ObSkipIndexColMeta(7, ObSkipIndexColType(0)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(7, ObSkipIndexColType(1)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(7, ObSkipIndexColType(2)));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 8
  ret = metas.push_back(ObSkipIndexColMeta(8, ObSkipIndexColType(0)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(8, ObSkipIndexColType(1)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = metas.push_back(ObSkipIndexColMeta(8, ObSkipIndexColType(2)));
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestAggRow::prepare_agg_row_writer(ObAggRowWriter & row_writer)
{
  int ret = OB_SUCCESS;

  /* ---------------------------------- col_meta_list ---------------------------------- */
  row_writer.col_meta_list_.set_allocator(&allocator_);
  ret = row_writer.col_meta_list_.reserve(21);
  // col 0
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(0, ObSkipIndexColType(0)), 0));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(0, ObSkipIndexColType(1)), 1));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(0, ObSkipIndexColType(2)), 2));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 1
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(1, ObSkipIndexColType(0)), 3));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(1, ObSkipIndexColType(1)), 4));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(1, ObSkipIndexColType(2)), 5));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 2
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(2, ObSkipIndexColType(0)), 6));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(2, ObSkipIndexColType(1)), 7));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(2, ObSkipIndexColType(2)), 8));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 3
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(3, ObSkipIndexColType(0)), 9));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(3, ObSkipIndexColType(1)), 10));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(3, ObSkipIndexColType(2)), 11));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 4
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(4, ObSkipIndexColType(0)), 12));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(4, ObSkipIndexColType(1)), 13));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(4, ObSkipIndexColType(2)), 14));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 7
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(7, ObSkipIndexColType(0)), 15));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(7, ObSkipIndexColType(1)), 16));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(7, ObSkipIndexColType(2)), 17));
  ASSERT_EQ(OB_SUCCESS, ret);
  // col 8
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(8, ObSkipIndexColType(0)), 18));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(8, ObSkipIndexColType(1)), 19));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_writer.col_meta_list_.push_back(std::make_pair(ObSkipIndexColMeta(8, ObSkipIndexColType(2)), 20));
  ASSERT_EQ(OB_SUCCESS, ret);

  /* ---------------------------------- agg_datums ---------------------------------- */
  ObStorageDatum * storage_datums = static_cast<ObStorageDatum *>(allocator_.alloc(sizeof(ObStorageDatum) * 21));
  ASSERT_NE(nullptr, storage_datums);

  // col 0
  set_datum(storage_datums[0], "0xf2	0xcc	0xe9	0xce	0xff	0xff	0xff	0xff", 8, 8);
  set_datum(storage_datums[1], "0xf2	0xcc	0xe9	0xce	0xff	0xff	0xff	0xff", 8, 8);
  set_datum(storage_datums[2], "0x00	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 8, 8);
  // col 1
  set_datum(storage_datums[3], "0x73	0x74	0x72	0x75	0x67	0x67	0x6c	0x65  0x73	0x20	0x61	0x72	0x6d	0x65	0x72	0x20  0x63	0x61	0x77	0x73	0x20	0x61	0x70	0x70  0x6c	0x79	0x20", 27, 27);
  set_datum(storage_datums[4], "0x73	0x74	0x72	0x75	0x67	0x67	0x6c	0x65  0x73	0x20	0x61	0x72	0x6d	0x65	0x72	0x20  0x63	0x61	0x77	0x73	0x20	0x61	0x70	0x70  0x6c	0x79	0x20", 27, 27);
  set_datum(storage_datums[5], "0x00	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 8, 8);
  // col 2
  set_datum(storage_datums[6], "0xc5	0xbe	0xdb	0x19	0x00	0x00	0x00	0x00", 8, 8);
  set_datum(storage_datums[7], "0xc5	0xbe	0xdb	0x19	0x00	0x00	0x00	0x00", 8, 8);
  set_datum(storage_datums[8], "0x00	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 8, 8);
  // col 3
  set_datum(storage_datums[9], "0x63	0x6f	0x6d	0x70	0x72	0x65	0x68	0x65  0x6e	0x64	0x69	0x6e	0x67	0x20	0x64	0x72  0x65	0x73	0x73	0x6d	0x61	0x6b	0x65	0x72  0x27	0x73	0x20	0x65	0x6c", 29, 29);
  set_datum(storage_datums[10], "0x63	0x6f	0x6d	0x70	0x72	0x65	0x68	0x65  0x6e	0x64	0x69	0x6e	0x67	0x20	0x64	0x72  0x65	0x73	0x73	0x6d	0x61	0x6b	0x65	0x72  0x27	0x73	0x20	0x65	0x6c", 29, 29);
  set_datum(storage_datums[11], "0x00	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 8, 8);
  // col 4
  set_datum(storage_datums[12], "0x80	0xac	0x3c	0x2e	0x72	0x00	0x00	0x00", 8, 8);
  set_datum(storage_datums[13], "0x80	0xac	0x3c	0x2e	0x72	0x00	0x00	0x00", 8, 8);
  set_datum(storage_datums[14], "0x00	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 8, 8);
  // col 7
  set_datum(storage_datums[15], "0x19	0x07	0x00	0x00	0x00	0x00	0x00	0x00  0x03	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 16, 1073741840);
  set_datum(storage_datums[16], "0x19	0x07	0x00	0x00	0x00	0x00	0x00	0x00  0x03	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 16, 1073741840);
  set_datum(storage_datums[17], "0x00	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 8, 8);
  // col 8
  set_datum(storage_datums[18], "", 0, 2147483648);
  set_datum(storage_datums[19], "", 0, 2147483648);
  set_datum(storage_datums[20], "0x01	0x00	0x00	0x00	0x00	0x00	0x00	0x00", 8, 8);

  const ObStorageDatum * const_datums = storage_datums;
  row_writer.agg_datums_ = const_datums;

  /* ---------------------------------- others ---------------------------------- */
  row_writer.column_count_ = 21;
  row_writer.col_meta_list_.set_allocator(&allocator_);
  ret = row_writer.calc_serialize_agg_buf_size();
  ASSERT_EQ(OB_SUCCESS, ret);
  row_writer.is_inited_ = true;
}

TEST_F(TestAggRow, test_agg_row_serialize_arm)
{
  int ret = OB_SUCCESS;

  ObAggRowWriter row_writer;
  prepare_agg_row_writer(row_writer);

  int64_t estimate_size = row_writer.get_serialize_data_size();
  char * buf = static_cast<char *>(allocator_.alloc(estimate_size));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, 0, estimate_size);
  int64_t write_pos = 0;
  ret = row_writer.write_agg_data(buf, estimate_size, write_pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  common::ObFixedArray<oceanbase::blocksstable::ObSkipIndexColMeta, common::ObIAllocator> metas;
  metas.set_allocator(&allocator_);
  prepare_skip_index_col_metas(metas);

  ObAggRowReader row_reader;
  ret = row_reader.init(buf, write_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStorageDatum * read_datums = static_cast<ObStorageDatum *>(allocator_.alloc(sizeof(ObStorageDatum) * 21));
  ASSERT_NE(nullptr, read_datums);
  ret = row_reader.read(metas[0], read_datums[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[1], read_datums[1]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[2], read_datums[2]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[3], read_datums[3]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[4], read_datums[4]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[5], read_datums[5]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[6], read_datums[6]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[7], read_datums[7]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[8], read_datums[8]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[9], read_datums[9]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[10], read_datums[10]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[11], read_datums[11]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[12], read_datums[12]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[13], read_datums[13]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[14], read_datums[14]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[15], read_datums[15]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[16], read_datums[16]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[17], read_datums[17]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[18], read_datums[18]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[19], read_datums[19]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_reader.read(metas[20], read_datums[20]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = check_serialized_aggregated_row(metas, buf, write_pos, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestAggRow, test_agg_row)
{
  int ret = OB_SUCCESS;
  int test_cnt = 10;
  bool has[10][5];
  memset(has, false, sizeof(has));
  ObArray<ObSkipIndexColMeta> agg_cols;
  ObArray<bool> is_null;
  ObDatumRow agg_row;
  OK(agg_row.init(test_cnt));
  int cnt = 0;
  while (cnt < test_cnt) {
    ObSkipIndexColMeta col_meta;
    uint32_t col_idx = rand() % 5;
    uint32_t col_type = rand() % ObSkipIndexColType::SK_IDX_MAX_COL_TYPE;
    if (!has[col_idx][col_type]) {
      col_meta.col_idx_ = col_idx;
      col_meta.col_type_ = col_type;
      OK(agg_cols.push_back(col_meta));
      bool null = (rand() % 5 == 0);
      if (null) {
        agg_row.storage_datums_[cnt].set_null();
      } else {
        agg_row.storage_datums_[cnt].set_int(cnt);
      }
      OK(is_null.push_back(null));
      has[col_idx][col_type] = true;
      ++cnt;
    }
  }

  ObAggRowWriter row_writer;
  OK(row_writer.init(agg_cols, agg_row, allocator_));
  int64_t buf_size = row_writer.get_serialize_data_size();
  char *buf = reinterpret_cast<char *>(allocator_.alloc(buf_size));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, 0, buf_size);
  int64_t pos = 0;
  OK(row_writer.write_agg_data(buf, buf_size, pos));
  ASSERT_GE(buf_size, pos);

  ObAggRowReader row_reader;
  OK(row_reader.init(buf, buf_size));
  for (int i = 0; i < test_cnt; ++i) {
    ObDatum datum;
    OK(row_reader.read(agg_cols.at(i), datum));
    if (is_null.at(i)) {
      ASSERT_TRUE(datum.is_null());
    } else {
      int64_t data = datum.get_int();
      ASSERT_EQ(data, i);
    }
  }
  row_reader.reset();
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_agg_row_struct.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_agg_row_struct.log", true);
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
