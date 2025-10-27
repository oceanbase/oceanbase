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

#define private public
#define protected public

#include "storage/access/ob_index_skip_scanner.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"

namespace oceanbase
{

using namespace common;
using namespace storage;
using namespace share::schema;

namespace blocksstable
{

#define DEFAULT_ROKEY_CNT 4
#define DEFAULT_COLUMN_CNT 5
#define DEFAULT_GROUP_IDX 1234
#define DEFAULT_LOG_LEVEL "TRACE"
class TestIndexSkipScanner : public TestIndexBlockDataPrepare
{
public:
  TestIndexSkipScanner();
  virtual ~TestIndexSkipScanner();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override;
  virtual void TearDown() override;
  virtual void prepare_schema() override;
  virtual void insert_data(ObMacroBlockWriter &data_writer) override;
  void prepare_ranges(
      const bool is_whole_range,
      const bool is_false_range,
      const int64_t start_row_idx,
      const int64_t end_row_idx,
      const int64_t prefix_cnt,
      const int64_t suffix_cnt,
      const int64_t skip_row_idx);
  int get_next_row_with_skip_fiter(ObSSTableRowScanner<> &scanner, const ObDatumRow *&prow, int64_t &access_cnt);
  void test_one_case();
  bool is_whole_range_;
  bool is_false_range_;
  int64_t start_row_idx_;
  int64_t end_row_idx_;
  int64_t prefix_cnt_;
  int64_t suffix_cnt_;
  int64_t skip_row_idx_;
  ObDatumRow scan_row1_;
  ObDatumRow scan_row2_;
  ObDatumRange scan_range_;
  ObDatumRow skip_row1_;
  ObDatumRow skip_row2_;
  ObDatumRange skip_range_;
  ObArenaAllocator range_alloc_;
};

TestIndexSkipScanner::TestIndexSkipScanner()
  : TestIndexBlockDataPrepare("Test Index Skip Scanner",
                              MAJOR_MERGE,
                              false,
                              OB_DEFAULT_MACRO_BLOCK_SIZE,
                              10000,
                              100000,
                              FLAT_ROW_STORE,
                              100,
                              10)
{
}

TestIndexSkipScanner::~TestIndexSkipScanner()
{
}

void TestIndexSkipScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestIndexSkipScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestIndexSkipScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);

  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestIndexSkipScanner::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestIndexSkipScanner::prepare_schema()
{
  // init table schema
  uint64_t table_id = TEST_TABLE_ID;
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_index_skip_scanner"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(DEFAULT_ROKEY_CNT);
  table_schema_.set_max_used_column_id(ObHexStringType);
  table_schema_.set_block_size(2 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(row_store_type_);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  table_schema_.set_micro_index_clustered(false);
  index_schema_.reset();

  // init column schema
  ObObjType types[DEFAULT_COLUMN_CNT] =
    {ObIntType, ObIntType, ObIntType, ObIntType, ObIntType};
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObColumnSchemaV2 column;
  for (int64_t i = 0; i < DEFAULT_COLUMN_CNT; ++i) {
    ObObjType obj_type = types[i];
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if (i < DEFAULT_ROKEY_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

#define SET_CHAR_OBJ(allocator, value, obj) \
{ \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator.alloc(VARIABLE_BUF_LEN)))){ \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    STORAGE_LOG(WARN, "fail to alloc memory"); \
  } else { \
    snprintf(buf, VARIABLE_BUF_LEN, "%064ld", value); \
  } \
  if (OB_SUCC(ret)){ \
    ObString str; \
    str.assign_ptr(buf, static_cast<int32_t>(strlen(buf))); \
    obj.set_varchar(str); \
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI); \
  } \
}


void TestIndexSkipScanner::insert_data(ObMacroBlockWriter &data_writer)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  int ret = OB_SUCCESS;
  row_cnt_ = 0;
  ObDatumRow row;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  const ObDmlFlag dml = DF_INSERT;

  storage::ObColDescArray column_list;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(column_list));
  int64_t rows_per_mirco_block = rows_per_mirco_block_;
  int64_t rows_per_macro_block = rows_per_mirco_block_ * mirco_blocks_per_macro_block_;

  while (true) {
    if (row_cnt_ >= max_row_cnt_) {
      break;
    }

    range_alloc_.reuse();
    int64_t value = 0;
    int64_t tmp_value = row_cnt_;
    char str_buf[VARIABLE_BUF_LEN];
    ObObj obj;
    for (int64_t i = DEFAULT_ROKEY_CNT - 1; i >= 0; --i) {
      ObObjType column_type = column_list.at(i).col_type_.get_type();
      const int64_t base = 0 == i ? 1 : std::pow(10, i) / 2;
      value = tmp_value % base;
      tmp_value = tmp_value / base;
      if (ObIntType == column_type) {
        obj.set_int(value);
      } else if (ObVarcharType == column_type) {
        SET_CHAR_OBJ(range_alloc_, value, obj);
      }
      ASSERT_EQ(OB_SUCCESS, row.storage_datums_[i].from_obj_enhance(obj));
    }
    for (int64_t i = DEFAULT_ROKEY_CNT; i < DEFAULT_COLUMN_CNT; ++i) {
      value = row_cnt_;
      ObObjType column_type = column_list.at(i).col_type_.get_type();
      if (ObIntType == column_type) {
        obj.set_int(value);
      } else if (ObVarcharType == column_type) {
        SET_CHAR_OBJ(range_alloc_, value, obj);
      }
      ASSERT_EQ(OB_SUCCESS, row.storage_datums_[i].from_obj_enhance(obj));
    }

    convert_to_multi_version_row(row, table_schema_, SNAPSHOT_VERSION, dml, multi_row);
    multi_row.mvcc_row_flag_.set_uncommitted_row(false);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
    if (row_cnt_ == 0) {
      ObDatumRowkey &start_key = data_writer.last_key_;
      ASSERT_EQ(OB_SUCCESS, start_key.deep_copy(start_key_, allocator_));
    }
    if (row_cnt_ == max_row_cnt_ - 1) {
      ObDatumRowkey &end_key = data_writer.last_key_;
      ASSERT_EQ(OB_SUCCESS, end_key.deep_copy(end_key_, allocator_));
    }
    if ((row_cnt_ + 1) % rows_per_mirco_block_ == 0) {
      OK(data_writer.build_micro_block());
    }
    if ((row_cnt_ + 1) % rows_per_macro_block == 0) {
      OK(data_writer.try_switch_macro_block());
    }
    ++row_cnt_;
  }
  oceanbase::common::ObLogger::get_logger().set_log_level(DEFAULT_LOG_LEVEL);
}

void TestIndexSkipScanner::prepare_ranges(
    const bool is_whole_range,
    const bool is_false_range,
    const int64_t start_row_idx,
    const int64_t end_row_idx,
    const int64_t prefix_cnt,
    const int64_t suffix_cnt,
    const int64_t skip_row_idx)
{
  int ret = OB_SUCCESS;
  scan_row1_.reset();
  scan_row2_.reset();
  scan_range_.reset();
  skip_row1_.reset();
  skip_row2_.reset();
  skip_range_.reset();
  range_alloc_.reuse();

  storage::ObColDescArray column_list;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(column_list));

  if (is_whole_range) {
    scan_range_.set_whole_range();
  } else if (is_false_range) {
    scan_range_.start_key_.set_max_rowkey();
    scan_range_.end_key_.set_min_rowkey();
  } else {
    ASSERT_EQ(OB_SUCCESS, scan_row1_.init(range_alloc_, DEFAULT_ROKEY_CNT));
    ASSERT_EQ(OB_SUCCESS, scan_row2_.init(range_alloc_, DEFAULT_ROKEY_CNT));

    int64_t value1, value2;
    int64_t tmp_value1 = start_row_idx, tmp_value2 = end_row_idx;
    ObObj obj1, obj2;
    for (int64_t i = DEFAULT_ROKEY_CNT - 1; i >= 0; --i) {
      const ObObjType column_type = column_list.at(i).col_type_.get_type();
      const int64_t base = 0 == i ? 1 : std::pow(10, i) / 2;
      value1 = tmp_value1 % base;
      value2 = tmp_value2 % base;
      tmp_value1 = tmp_value1 / base;
      tmp_value2 = tmp_value2 / base;
      if (ObIntType == column_type) {
        obj1.set_int(value1);
        obj2.set_int(value2);
      } else if (ObVarcharType == column_type) {
        SET_CHAR_OBJ(range_alloc_, value1, obj1);
        SET_CHAR_OBJ(range_alloc_, value2, obj2);
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, scan_row1_.storage_datums_[i].from_obj_enhance(obj1));
      ASSERT_EQ(OB_SUCCESS, scan_row2_.storage_datums_[i].from_obj_enhance(obj2));
    }
    scan_range_.start_key_.assign(scan_row1_.storage_datums_, DEFAULT_ROKEY_CNT);
    scan_range_.end_key_.assign(scan_row2_.storage_datums_, DEFAULT_ROKEY_CNT);
  }
  scan_range_.group_idx_ = DEFAULT_GROUP_IDX;

  ASSERT_TRUE(prefix_cnt > 0 && prefix_cnt < DEFAULT_ROKEY_CNT && suffix_cnt >= 0 && prefix_cnt + suffix_cnt <= DEFAULT_ROKEY_CNT);
  ASSERT_EQ(OB_SUCCESS, skip_row1_.init(range_alloc_, suffix_cnt));
  ASSERT_EQ(OB_SUCCESS, skip_row2_.init(range_alloc_, suffix_cnt));
  int64_t value1;
  int64_t tmp_value1 = skip_row_idx;
  ObObj obj1, obj2;
  for (int64_t i = suffix_cnt - 1; i >= 0; --i) {
    const int64_t col_idx = prefix_cnt + i;
    const ObObjType column_type = column_list.at(col_idx).col_type_.get_type();
    const int64_t base = 0 == col_idx ? 1 : std::pow(10, col_idx) / 2;
    value1 = tmp_value1 % base;
    tmp_value1 = tmp_value1 / base;
    if (ObIntType == column_type) {
      obj1.set_int(value1);
      obj2.set_int(value1);
    } else if (ObVarcharType == column_type) {
      SET_CHAR_OBJ(range_alloc_, value1, obj1);
      SET_CHAR_OBJ(range_alloc_, value1, obj2);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, skip_row1_.storage_datums_[i].from_obj_enhance(obj1));
    ASSERT_EQ(OB_SUCCESS, skip_row2_.storage_datums_[i].from_obj_enhance(obj2));
  }
  skip_range_.start_key_.assign(skip_row1_.storage_datums_, suffix_cnt);
  skip_range_.end_key_.assign(skip_row2_.storage_datums_, suffix_cnt);
  skip_range_.group_idx_ = scan_range_.group_idx_;
}

int TestIndexSkipScanner::get_next_row_with_skip_fiter(ObSSTableRowScanner<> &scanner, const ObDatumRow *&prow, int64_t &access_cnt)
{
  int ret = OB_SUCCESS;
  const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();
  const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
  bool filtered = true;
  while (OB_SUCC(ret) && filtered) {
    if (OB_SUCC(scanner.inner_get_next_row(prow))) {
      access_cnt++;
      int cmp_ret_left = 0, cmp_ret_right = 0;
      for (int64_t i = prefix_cnt_; OB_SUCC(ret) && filtered && i < prefix_cnt_ + suffix_cnt_; ++i) {
        const int64_t skip_col_idx = i - prefix_cnt_;
        if (OB_FAIL(cmp_funcs.at(i).compare(prow->storage_datums_[i], skip_range_.start_key_.datums_[skip_col_idx], cmp_ret_left))) {
        } else if (OB_FAIL(cmp_funcs.at(i).compare(prow->storage_datums_[i], skip_range_.start_key_.datums_[skip_col_idx], cmp_ret_right))) {
        } else {
          filtered = (cmp_ret_left < 0 || (0 == cmp_ret_left && skip_range_.is_left_open())) ||
                     (cmp_ret_right > 0 || (0 == cmp_ret_right && skip_range_.is_right_open()));
        }
      }
    }
  }
  return ret;
}

void TestIndexSkipScanner::test_one_case()
{
  prepare_ranges(is_whole_range_, is_false_range_, start_row_idx_, end_row_idx_, prefix_cnt_, suffix_cnt_, skip_row_idx_);
  scan_range_.set_left_closed(); scan_range_.set_right_closed();
  skip_range_.set_left_closed(); skip_range_.set_right_closed();

  STORAGE_LOG(INFO, "after prepare ranges", K_(start_row_idx), K_(end_row_idx), K_(skip_row_idx), K_(prefix_cnt), K_(suffix_cnt),
      K_(scan_range), K_(skip_range));

  const ObDatumRow *prow = nullptr;
  const ObDatumRow *skip_prow = nullptr;
  ObSSTableRowScanner<> scanner;
  ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<2,2>> skip_scanner;

  iter_param_.ss_rowkey_prefix_cnt_ = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.init(iter_param_, context_, &sstable_, &scan_range_));
  iter_param_.set_skip_scan_range(skip_range_);
  iter_param_.ss_rowkey_prefix_cnt_ = prefix_cnt_;
  context_.alloc_skip_scan_factory();
  ASSERT_EQ(OB_SUCCESS, skip_scanner.init(iter_param_, context_, &sstable_, &scan_range_));


  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  int64_t access_cnt = 0, output_cnt = 0;
  const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();
  const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
  while (OB_SUCC(ret)) {
    int skip_ret = skip_scanner.inner_get_next_row(skip_prow);
    ret = get_next_row_with_skip_fiter(scanner, prow, access_cnt);
    STORAGE_LOG(INFO, "check ret", K(skip_ret), K(ret), K(access_cnt), KPC(prow));
    ASSERT_EQ(skip_ret, ret);
    ASSERT_TRUE(OB_ITER_END == ret || OB_SUCCESS == ret);
    if (OB_SUCCESS == ret) {
      output_cnt++;
      for (int64_t i = 0; i < DEFAULT_COLUMN_CNT; ++i) {
        ASSERT_EQ(OB_SUCCESS,
          cmp_funcs.at(i).compare(skip_prow->storage_datums_[i], prow->storage_datums_[i], cmp_ret));
        if (0 != cmp_ret) {
          STORAGE_LOG(INFO, "check row", KPC(skip_prow), KPC(prow), K(i));
        }
        ASSERT_TRUE(0 == cmp_ret);
      }
    } else {
      ASSERT_EQ(end_row_idx_ - start_row_idx_ + 1, access_cnt);
    }
  }
  STORAGE_LOG(INFO, "after get rows", K_(start_row_idx), K_(end_row_idx), K_(skip_row_idx),
      K(access_cnt), K(output_cnt));
}

TEST_F(TestIndexSkipScanner, test_false_range)
{
  bool is_reverse_scan = false;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  start_row_idx_ = 0;
  end_row_idx_ = -1;
  is_whole_range_ = false;
  is_false_range_ = true;
  prefix_cnt_ = 1;
  suffix_cnt_ = 1;
  skip_row_idx_ = 0;

  test_one_case();

  destroy_query_param();
}

TEST_F(TestIndexSkipScanner, test_whole_range)
{
  bool is_reverse_scan = false;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  start_row_idx_ = 0;
  end_row_idx_ = row_cnt_ - 1;
  is_whole_range_ = true;
  is_false_range_ = false;
  prefix_cnt_ = 3;
  suffix_cnt_ = 1;
  skip_row_idx_ = 90000;

  test_one_case();

  destroy_query_param();
}

TEST_F(TestIndexSkipScanner, test_random)
{
  bool is_reverse_scan = false;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  is_whole_range_ = false;
  is_false_range_ = false;

  for (int64_t i = 0; i < 3; ++i) {
    start_row_idx_ = ObRandom::rand(0, row_cnt_ - 1);
    end_row_idx_ = ObRandom::rand(0, row_cnt_ - 1);
    if (start_row_idx_ > end_row_idx_) {
      int64_t temp = start_row_idx_;
      start_row_idx_ = end_row_idx_;
      end_row_idx_ = temp;
    }
    prefix_cnt_ = i + 1;
    suffix_cnt_ = 1;
    skip_row_idx_ = ObRandom::rand(0, row_cnt_ - 1);

    test_one_case();
  }
  destroy_query_param();
}

TEST_F(TestIndexSkipScanner, test_border)
{
  bool is_reverse_scan = false;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  is_whole_range_ = true;
  is_false_range_ = false;

  start_row_idx_ = 0;
  end_row_idx_ = row_cnt_ - 1;
  for (int64_t i = 0; i <= row_cnt_; i += 100 * rows_per_mirco_block_) {
    prefix_cnt_ = ObRandom::rand(0, 1) + 1;
    suffix_cnt_ = 1;
    skip_row_idx_ = i;

    test_one_case();
  }
  destroy_query_param();
}

TEST_F(TestIndexSkipScanner, test_false_range_reverse)
{
  bool is_reverse_scan = true;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  start_row_idx_ = 0;
  end_row_idx_ = -1;
  is_whole_range_ = false;
  is_false_range_ = true;
  prefix_cnt_ = 1;
  suffix_cnt_ = 1;
  skip_row_idx_ = 0;

  test_one_case();

  destroy_query_param();
}

TEST_F(TestIndexSkipScanner, test_whole_range_reverse)
{
  bool is_reverse_scan = true;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  start_row_idx_ = 0;
  end_row_idx_ = row_cnt_ - 1;
  is_whole_range_ = true;
  is_false_range_ = false;
  prefix_cnt_ = 3;
  suffix_cnt_ = 1;
  skip_row_idx_ = 90000;

  test_one_case();

  destroy_query_param();
}

TEST_F(TestIndexSkipScanner, test_random_reverse)
{
  bool is_reverse_scan = true;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  is_whole_range_ = false;
  is_false_range_ = false;

  for (int64_t i = 0; i < 3; ++i) {
    start_row_idx_ = ObRandom::rand(0, row_cnt_ - 1);
    end_row_idx_ = ObRandom::rand(0, row_cnt_ - 1);
    if (start_row_idx_ > end_row_idx_) {
      int64_t temp = start_row_idx_;
      start_row_idx_ = end_row_idx_;
      end_row_idx_ = temp;
    }
    prefix_cnt_ = i + 1;
    suffix_cnt_ = 1;
    skip_row_idx_ = ObRandom::rand(0, row_cnt_ - 1);

    test_one_case();
  }
  destroy_query_param();
}

TEST_F(TestIndexSkipScanner, test_border_reverse)
{
  bool is_reverse_scan = true;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  is_whole_range_ = true;
  is_false_range_ = false;

  start_row_idx_ = 0;
  end_row_idx_ = row_cnt_ - 1;
  for (int64_t i = 0; i <= row_cnt_; i += 100 * rows_per_mirco_block_) {
    prefix_cnt_ = ObRandom::rand(0, 1) + 1;
    suffix_cnt_ = 1;
    skip_row_idx_ = i;

    test_one_case();
  }
  destroy_query_param();
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_index_skip_scanner.log*");
  OB_LOGGER.set_file_name("test_index_skip_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level(DEFAULT_LOG_LEVEL);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}