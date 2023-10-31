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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#define protected public

#include "storage/ob_partition_range_spliter.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;

static const int64_t ROWKEY_COLUMN_NUM = 1;

static int get_number(const char *str, const char *&endpos, int64_t &num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "str can not been null", KR(ret));
  } else {
    char *pos = nullptr;
    errno = 0;
    num = ::strtoll(str, &pos, 10);
    if (pos == nullptr || pos == str || errno != 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "fail to strtoll", KR(ret), K(str), K(errno), K(pos));
    } else {
      endpos = pos;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMockSSTableV2;

class ObMockSSTableSecMetaIterator : public ObSSTableSecMetaIterator
{
public:
  ObMockSSTableSecMetaIterator() : sstable_(nullptr), macro_block_idx_(0) {}
  virtual ~ObMockSSTableSecMetaIterator() = default;

  int get_next(ObDataMacroBlockMeta &macro_meta) override;

private:
  ObMockSSTableV2 *sstable_;
  int macro_block_idx_;
};

class ObMockSSTableV2 : public ObSSTable
{
public:
  ObMockSSTableV2() = default;
  virtual ~ObMockSSTableV2() = default;
  int scan_secondary_meta(ObIAllocator &allocator, ObSSTableSecMetaIterator *&meta_iter);
  int add_macro_block_meta(const int64_t endkey);
  int from(const ObString &str);
  void reset() { endkeys_.reset(); }
private:
  ObSEArray<ObStorageDatum, 64> endkeys_;
};

int ObMockSSTableSecMetaIterator::get_next(ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret));
  } else if (macro_block_idx_ >= sstable_->endkeys_.count()) {
    ret = OB_ITER_END;
  } else {
    ObDataMacroBlockMeta &meta = dynamic_cast<ObDataMacroBlockMeta &>(macro_meta);
    ObStorageDatum &obj = sstable_->endkeys_.at(macro_block_idx_);
    meta.end_key_.assign(&obj, ROWKEY_COLUMN_NUM);
    meta.val_.rowkey_count_ = ROWKEY_COLUMN_NUM;
    meta.val_.column_count_ = ROWKEY_COLUMN_NUM + 1;
    meta.val_.micro_block_count_ = 1;
    meta.val_.occupy_size_ = 100;
    meta.val_.original_size_ = 100;
    meta.val_.data_zsize_ = 100;
    meta.val_.logic_id_.tablet_id_ = 1;
    meta.val_.logic_id_.logic_version_ = 1;
    meta.val_.macro_id_.set_block_index(100);
    meta.val_.version_ = ObDataBlockMetaVal::DATA_BLOCK_META_VAL_VERSION;
    meta.val_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    meta.val_.row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    ++macro_block_idx_;
  }
  return ret;
}

int ObMockSSTableV2::add_macro_block_meta(const int64_t endkey)
{
  int ret = OB_SUCCESS;
  if (!endkeys_.empty()) {
    const ObStorageDatum &last = endkeys_.at(endkeys_.count() - 1);
    if (last.get_int() >= endkey) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument endkey", KR(ret), K(last), K(endkey));
    }
  }
  if (OB_SUCC(ret)) {
    ObStorageDatum new_obj;
    new_obj.set_int(endkey);
    if (OB_FAIL(endkeys_.push_back(new_obj))) {
      STORAGE_LOG(WARN, "failed to push endkey", KR(ret));
    }
  }
  return ret;
}

int ObMockSSTableV2::from(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument empty str", KR(ret));
  } else {
    endkeys_.reset();
    const char *pos1 = str.ptr(), *pos2 = nullptr;
    int64_t num = 0;
    while (OB_SUCC(ret) && *pos1 != '\0') {
      if (OB_FAIL(get_number(pos1, pos2, num))) {
        STORAGE_LOG(WARN, "failed to get number", KR(ret), K(pos1));
      } else if (OB_FAIL(add_macro_block_meta(num))) {
        STORAGE_LOG(WARN, "failed to add macro meta", KR(ret), K(num));
      } else {
        while (*pos2 != '\0' && (*pos2 == ' ' || *pos2 == ','))
          ++pos2;
        pos1 = pos2;
        pos2 = nullptr;
      }
    }
  }
  return ret;
}

int ObMockSSTableV2::scan_secondary_meta(ObIAllocator &allocator, ObSSTableSecMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObMockSSTableSecMetaIterator *iter = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMockSSTableSecMetaIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory", KR(ret));
  } else if (OB_ISNULL(iter = new (buf) ObMockSSTableSecMetaIterator())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null pointer of secondary meta iterator", KR(ret));
  } else {
    iter->sstable_ = this;
    iter->is_inited_ = true;
    meta_iter = iter;
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(iter)) {
      iter->~ObMockSSTableSecMetaIterator();
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMockPartitionMajorSSTableRangeSpliter : public ObPartitionMajorSSTableRangeSpliter
{
public:
  int scan_major_sstable_secondary_meta(const ObDatumRange &scan_range,
                                        ObSSTableSecMetaIterator *&meta_iter) override;
};

int ObMockPartitionMajorSSTableRangeSpliter::scan_major_sstable_secondary_meta(
  const ObDatumRange &scan_range, ObSSTableSecMetaIterator *&meta_iter)
{
  UNUSED(scan_range);
  int ret = OB_SUCCESS;
  ObMockSSTableV2 *sstable = dynamic_cast<ObMockSSTableV2 *>(major_sstable_);
  if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null sstable", KR(ret));
  } else if (OB_FAIL(sstable->scan_secondary_meta(*allocator_, meta_iter))) {
    STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

class TestPartitionMajorSSTableRangeSliter : public ::testing::Test
{
  static const int64_t MAX_BUF_LENGTH = 1024;
public:
  TestPartitionMajorSSTableRangeSliter() : buf_(nullptr), is_inited_(false)
  {
    major_sstable_.meta_ = &sstable_meta_;
    major_sstable_.addr_.set_none_addr();
    major_sstable_.meta_->is_inited_ = true;
  }
  virtual ~TestPartitionMajorSSTableRangeSliter() {}

public:
  virtual void SetUp();
  virtual void TearDown();

private:
  void set_major_sstable_meta(int64_t macro_block_count, int64_t occupy_size, int64_t row_count)
  {
    major_sstable_.meta_->basic_meta_.data_macro_block_count_ = macro_block_count;
    major_sstable_.meta_->basic_meta_.occupy_size_ = occupy_size;
    major_sstable_.meta_->basic_meta_.row_count_ = row_count;
    major_sstable_.addr_.set_none_addr();
    major_sstable_.meta_cache_.data_macro_block_count_ = macro_block_count;
    major_sstable_.meta_->is_inited_ = true;
  }
  int set_major_sstable_macro_blocks(const ObString &str);
  void reset_major_sstable()
  {
    set_major_sstable_meta(0, 0, 0);
    major_sstable_.reset();
  }

private:
  int check_ranges_result(const ObIArray<ObStoreRange> &ranges, const ObString &result, bool &equal);
  void inner_test_split_ranges(int64_t tablet_size, int64_t macro_block_count, int64_t row_count,
                               const ObString &ranges_str, const ObString &split_ranges);

private:
  common::ObArenaAllocator allocator_;
  ObMockSSTableV2 major_sstable_;
  ObSSTableMeta sstable_meta_;
  char *buf_;
  ObSEArray<ObColDesc, 3> col_descs_;
  ObTableReadInfo idx_read_info_;
  bool is_inited_;
};

void TestPartitionMajorSSTableRangeSliter::SetUp()
{
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  if (!is_inited_) {
    OB_SERVER_BLOCK_MGR.super_block_.body_.macro_block_size_ = 1;

    // major sstable
    major_sstable_.set_table_type(ObITable::MAJOR_SSTABLE);
    major_sstable_.meta_->basic_meta_.data_macro_block_count_ = 0;
    major_sstable_.meta_->basic_meta_.occupy_size_ = 0;
    major_sstable_.meta_->basic_meta_.row_count_ = 0;
    major_sstable_.valid_for_reading_ = true;
    major_sstable_.addr_.set_none_addr();
    major_sstable_.meta_cache_.data_macro_block_count_ = 0;
    major_sstable_.meta_->is_inited_ = true;

    ObColDesc col_desc;
    col_desc.col_id_ = 1;
    col_desc.col_type_.set_int();
    col_descs_.reset();
    idx_read_info_.reset();
    ASSERT_EQ(OB_SUCCESS, col_descs_.push_back(col_desc));
    ASSERT_EQ(OB_SUCCESS, col_descs_.push_back(col_desc));
    ASSERT_EQ(OB_SUCCESS, storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(col_descs_));
    ASSERT_EQ(OB_SUCCESS, idx_read_info_.init(allocator_, 2, 1, lib::is_oracle_mode(), col_descs_, nullptr/*storage_cols_index*/));

    // buf
    buf_ = static_cast<char *>(allocator_.alloc(MAX_BUF_LENGTH));
    ASSERT_NE(nullptr, buf_);

    is_inited_ = true;
  }
}

void TestPartitionMajorSSTableRangeSliter::TearDown()
{
}

int TestPartitionMajorSSTableRangeSliter::set_major_sstable_macro_blocks(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(major_sstable_.from(str))) {
    STORAGE_LOG(WARN, "failed to get macro blocks from str", KR(ret), K(str));
  }
  return ret;
}

int TestPartitionMajorSSTableRangeSliter::check_ranges_result(const ObIArray<ObStoreRange> &ranges, const ObString &result, bool &equal)
{
  int ret = OB_SUCCESS;
  if (ranges.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(ranges), K(result));
  } else if (OB_UNLIKELY(!ranges.at(0).get_start_key().is_min() || !ranges.at(0).is_left_open())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected ranges", KR(ret), K(ranges));
  } else if (OB_UNLIKELY(!ranges.at(ranges.count() - 1).get_end_key().is_max() ||
                         !ranges.at(ranges.count() - 1).is_right_open())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected ranges", KR(ret), K(ranges));
  } else if (ranges.count() == 1 || result.empty()) {
    equal = (ranges.count() == 1 && result.empty());
  } else {
    bool is_equal = false;
    int64_t length = 0;
    for (int64_t i = 1; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObStoreRange &prev_range = ranges.at(i - 1);
      const ObStoreRange &cur_range = ranges.at(i);
      if (OB_UNLIKELY(!prev_range.is_right_closed() || !cur_range.is_left_open())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected range", K(ret), K(i), K(prev_range), K(cur_range));
      } else if (OB_FAIL(cur_range.get_start_key().equal(prev_range.get_end_key(), is_equal))) {
        STORAGE_LOG(WARN, "failed to compare rowkeys", K(ret), K(i), K(prev_range), K(cur_range));
      } else if (OB_UNLIKELY(!is_equal)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ranges", K(ret), K(i), K(prev_range), K(cur_range));
      } else {
        int64_t val = cur_range.get_start_key().key_.obj_ptr_[0].get_int();
        length += ::snprintf(buf_ + length, MAX_BUF_LENGTH - length, "%ld,", val);
      }
    }
    // STORAGE_LOG(DEBUG, "ranges result", KR(ret), K(buf_), K(result));
    if (OB_SUCC(ret)) {
      if (length > 0) {
        --length;  // 去掉末尾的','
      }
      buf_[length] = '\0';
      equal = (length == result.length() && ::memcmp(buf_, result.ptr(), length) == 0);
      if (!equal) {
        STORAGE_LOG(DEBUG, "ranges result", KR(ret), K(buf_));
      }
    }
  }
  return ret;
}

void TestPartitionMajorSSTableRangeSliter::inner_test_split_ranges(
    int64_t tablet_size, int64_t macro_block_count, int64_t row_count,
    const ObString &macro_block_str, const ObString &split_ranges)
{
  bool equal = false;
  common::ObSEArray<common::ObStoreRange, 64> range_array;
  ObMockPartitionMajorSSTableRangeSpliter range_spliter;

  // set macro block meta
  int64_t occupy_size = macro_block_count * OB_SERVER_BLOCK_MGR.get_macro_block_size();
  set_major_sstable_meta(macro_block_count, occupy_size, row_count);
  if (macro_block_count > 0) {
    ASSERT_EQ(OB_SUCCESS, set_major_sstable_macro_blocks(macro_block_str));
  }

  ASSERT_EQ(OB_SUCCESS, range_spliter.init(idx_read_info_, &major_sstable_, tablet_size, allocator_));
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_array));
  ASSERT_EQ(OB_SUCCESS, check_ranges_result(range_array, split_ranges, equal));
  ASSERT_TRUE(equal);
}

TEST_F(TestPartitionMajorSSTableRangeSliter, test_tablet_size_0)
{
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges(0/*tablet_size*/, 0/*macro_block_count*/, 0/*row_count*/,
    "", ""));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges(0, 7, 700,
    "100,200,300,400,500,600,700", ""));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges(0, 20, 200000,
    "10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,"
    "110000,120000,130000,140000,150000,160000,170000,180000,190000,200000",
    ""));
}

TEST_F(TestPartitionMajorSSTableRangeSliter, test_split_ranges)
{
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges(1, 0/*macro_block_count*/, 0/*row_count*/,
    "", ""));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges(2, 7, 700,
    "100,200,300,400,500,600,700",
    "200,400,600"));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges(1, 7, 1400,
    "200,400,600,800,1000,1200,1400",
    "200,400,600,800,1000,1200"));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges(1, 64, 12800,
    "200,400,600,800,1000,1200,1400,1600,1800,2000,"
    "2200,2400,2600,2800,3000,3200,3400,3600,3800,4000,"
    "4200,4400,4600,4800,5000,5200,5400,5600,5800,6000,"
    "6200,6400,6600,6800,7000,7200,7400,7600,7800,8000,"
    "8200,8400,8600,8800,9000,9200,9400,9600,9800,10000,"
    "10200,10400,10600,10800,11000,11200,11400,11600,11800,12000,"
    "12200,12400,12600,12800",
    "200,400,600,800,1000,1200,1400,1600,1800,2000,"
    "2200,2400,2600,2800,3000,3200,3400,3600,3800,4000,"
    "4200,4400,4600,4800,5000,5200,5400,5600,5800,6000,"
    "6200,6400,6600,6800,7000,7200,7400,7600,7800,8000,"
    "8200,8400,8600,8800,9000,9200,9400,9600,9800,10000,"
    "10200,10400,10600,10800,11000,11200,11400,11600,11800,12000,"
    "12200,12400,12600"));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges(1, 65, 13000,
    "200,400,600,800,1000,1200,1400,1600,1800,2000,"
    "2200,2400,2600,2800,3000,3200,3400,3600,3800,4000,"
    "4200,4400,4600,4800,5000,5200,5400,5600,5800,6000,"
    "6200,6400,6600,6800,7000,7200,7400,7600,7800,8000,"
    "8200,8400,8600,8800,9000,9200,9400,9600,9800,10000,"
    "10200,10400,10600,10800,11000,11200,11400,11600,11800,12000,"
    "12200,12400,12600,12800,13000",
    "400,800,1200,1600,2000,2400,2800,3200,3600,4000,"
    "4400,4800,5200,5600,6000,6400,6800,7200,7600,8000,"
    "8400,8800,9200,9600,10000,10400,10800,11200,11600,12000,"
    "12400,12800"));
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_partition_major_sstable_range_spliter.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_partition_major_sstable_range_spliter.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
