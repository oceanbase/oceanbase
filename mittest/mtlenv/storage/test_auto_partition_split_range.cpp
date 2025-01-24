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

#define private public
#define protected public

#include "storage/test_tablet_helper.h"
#include "storage/test_dml_common.h"
#include "src/share/ob_partition_split_query.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;

namespace unittest
{

enum TEST_RANGE_TYPE
{
  EMPTY_ORIGIN_RANGE = 0,
  EMPTY_SPLIT_RANGE,
  ONLY_SET_SPLIT_START_KEY,
  ONLY_SET_SPLIT_END_KEY,
  ORIGIN_INSIDE_SPLIT,
  ORIGIN_OUTSIDE_SPLIT,
  ORIGIN_INCLIDE_SPLIT_START,
  ORIGIN_INCLUDE_SPLIT_END,
  MAX_RANGE_TYPE
};

enum CHECK_DATUM_MIN_TYPE
{
  LEFT = 0,
  RIGHT,
  BOTH,
  MAX_TYPE
};

typedef ObSEArray<share::schema::ObColDesc, 2> ColDescArray;

class FakeObTableScanRange : public ::testing::Test
{
public:
  FakeObTableScanRange();
  ~FakeObTableScanRange()
  {}

  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();

  int do_split_store_range(const int64_t split_start_val, const int64_t split_end_val);
  int do_split_datum_range(int64_t split_start_val, int64_t split_end_val);
  int gen_datum_range(const int64_t start_val, const int64_t end_val, const int64_t key_cnt);
  int check_datum_range_result(int64_t expected_start_val, int64_t expected_end_val, bool is_left_closed,  bool is_right_closed, bool &is_equal);
  int check_datum_min_column(const int64_t src_datum_cnt, const int64_t split_datum_cnt, const int64_t expected_left_val, const int64_t expected_right_val, int check_type);
  int set_datum_key(const bool is_left, const bool is_right, int64_t value);

  void set_need_set_src_range(bool value) { need_set_src_range_ = value; }
  void set_need_set_split_start_key(bool value) { need_set_split_start_key_ = value; }
  void set_need_set_split_end_key(bool value) { need_set_split_end_key_ = value; }
  void set_split_type(ObTabletSplitType type) { split_info_.split_type_ = type; }
  void set_left_closed() { datum_range_.set_left_closed(); }
  void set_right_closed() { datum_range_.set_right_closed(); }
  void set_left_open() { datum_range_.set_left_open(); }
  void set_right_open() { datum_range_.set_right_open(); }

public:
  static constexpr int64_t TEST_LS_ID = 100;
  static constexpr int64_t TEST_TENANT_ID = 1;

private:
  int init_table_datum_desc();
  int init_table_col_descs();
  int gen_datum_rowkey(const int64_t key_val, const int64_t key_cnt, blocksstable::ObDatumRowkey &rowkey);

private:
  ObArenaAllocator allocator_;
  static ObArenaAllocator static_allocator_;
  ColDescArray col_descs_;
  blocksstable::ObDatumRange datum_range_;                       // src datum range
  blocksstable::ObStorageDatumUtils datum_utils_;
  storage::ObTabletSplitTscInfo split_info_;
  ObTabletHandle tablet_handle_;
  ObPartitionSplitQuery split_query_;
  bool need_set_src_range_;
  bool need_set_split_start_key_;
  bool need_set_split_end_key_;
};

ObArenaAllocator FakeObTableScanRange::static_allocator_;

void FakeObTableScanRange::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  // create tablet
  ObTabletID tablet_id(1001);
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, static_allocator_));
}

void FakeObTableScanRange::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);
  MockTenantModuleEnv::get_instance().destroy();
}

void FakeObTableScanRange::SetUp()
{
  int ret = OB_SUCCESS;
  ret = init_table_col_descs();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = init_table_datum_desc();
  ASSERT_EQ(OB_SUCCESS, ret);

   // get ls
  ObLSHandle handle;
  const ObLSID ls_id(TEST_LS_ID);
  ObLS *ls = NULL;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_svr);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  ObTabletID tablet_id(1001);
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle_));

  need_set_src_range_ = false;
  need_set_split_start_key_ = false;
  need_set_split_end_key_ = false;
  split_info_.split_type_ = ObTabletSplitType::MAX_TYPE;
  split_info_.split_cnt_ = 1;
  split_info_.src_tablet_handle_ = tablet_handle_;
  split_info_.partkey_is_rowkey_prefix_ = true;
}

void FakeObTableScanRange::TearDown()
{
  col_descs_.reset();
  datum_utils_.reset();
}

FakeObTableScanRange::FakeObTableScanRange()
{
}

int FakeObTableScanRange::do_split_datum_range(int64_t split_start_val, int64_t split_end_val)
{
  int ret = OB_SUCCESS;

  int64_t key_cnt = 1;
  if (need_set_split_start_key_) {
    if (OB_FAIL(gen_datum_rowkey(split_start_val, key_cnt, split_info_.start_partkey_))) {
      STORAGE_LOG(WARN, "fail to gen start rowkey", K(ret));
    }
  }

  if (OB_SUCC(ret) && need_set_split_end_key_) {
    if (OB_FAIL(gen_datum_rowkey(split_end_val, key_cnt, split_info_.end_partkey_))) {
      STORAGE_LOG(WARN, "fail to gen end rowkey", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bool is_empty_range = false;
    if (OB_FAIL(split_query_.get_tablet_split_range(*tablet_handle_.get_obj(), datum_utils_, split_info_, allocator_, datum_range_, is_empty_range))) {
      STORAGE_LOG(WARN, "fail to do split range", K(ret));
    }
  }

  return ret;
}

int FakeObTableScanRange::init_table_col_descs()
{
  int ret = OB_SUCCESS;

  share::schema::ObColDesc col_desc;
  for (int64_t i = 0; i < 2; i++) { // two columns
    col_desc.col_type_.set_int32();
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + i;
    // col_desc.col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (OB_FAIL(col_descs_.push_back(col_desc))) {
      STORAGE_LOG(WARN, "fail to push back col_descs_", K(ret));
    }
  }
  return ret;
}

int FakeObTableScanRange::init_table_datum_desc()
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc;
  ObSEArray<ObColDesc, 2> col_descs;
  const bool is_oracle_mode = false;
  // two columns
  for (int64_t i = 0; i < 2; i++) {
    col_desc.col_type_.set_int32();
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + i;
    if (OB_FAIL(col_descs.push_back(col_desc))) {
      STORAGE_LOG(WARN, "fail to push back col_desc", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum_utils_.init(col_descs, 2, is_oracle_mode, allocator_))) {
      STORAGE_LOG(WARN, "fail to init datum_utils", K(ret));
    }
  }
  return ret;
}

int FakeObTableScanRange::gen_datum_rowkey(const int64_t key_val, const int64_t key_cnt, blocksstable::ObDatumRowkey &datum_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *key_val_obj = NULL;
  ObRowkey rowkey;
  if (NULL == (key_val_obj = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * key_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "out of memory", K(ret));
  } else {
    for (int64_t i = 0; i < key_cnt; ++i) {
      key_val_obj[i].set_int(key_val);
      // key_val_obj[i].set_min();
    }
    rowkey.assign(key_val_obj, key_cnt);
    if (OB_FAIL(datum_rowkey.from_rowkey(rowkey, allocator_))) {
      STORAGE_LOG(WARN, "fail to from rowkey", K(ret));
    }
  }
  return ret;
}



int FakeObTableScanRange::gen_datum_range(const int64_t start_val, const int64_t end_val, const int64_t key_cnt)
{
  int ret = OB_SUCCESS;
  datum_range_.reset();
  if (start_val <= end_val && need_set_src_range_) {
    blocksstable::ObDatumRowkey start_key;
    blocksstable::ObDatumRowkey end_key;
    if (OB_FAIL(gen_datum_rowkey(start_val, key_cnt, start_key))) {
      STORAGE_LOG(WARN, "Failed to gen start rowkey", K(ret));
    } else if (OB_FAIL(gen_datum_rowkey(end_val, key_cnt, end_key))) {
      STORAGE_LOG(WARN, "Failed to gen end rowkey", K(ret));
    } else {
      datum_range_.set_start_key(start_key);
      datum_range_.set_end_key(end_key);
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(start_val), K(end_val), K(need_set_src_range_));
  }
  return ret;
}

int FakeObTableScanRange::set_datum_key(const bool is_left, const bool is_right, int64_t value)
{
  int ret = OB_SUCCESS;
  int64_t key_cnt = 1;
  blocksstable::ObDatumRowkey datum_key;
  if (OB_FAIL(gen_datum_rowkey(value, key_cnt, datum_key))) {
    STORAGE_LOG(WARN, "Failed to gen rowkey", K(ret));
  } else if (!datum_range_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid range", K(ret));
  } else if (is_left) {
    datum_range_.set_start_key(datum_key);
  } else if (is_right) {
    datum_range_.set_end_key(datum_key);
  }
  return ret;
}

int FakeObTableScanRange::check_datum_range_result(
    int64_t expected_left_val,
    int64_t expected_right_val,
    bool is_left_closed,  // 1:  close; 0:  open
    bool is_right_closed,
    bool &is_equal)
{
  int ret = OB_SUCCESS;
  int64_t key_cnt = 1;
  blocksstable::ObDatumRowkey datum_start_key;
  blocksstable::ObDatumRowkey datum_end_key;

  if (OB_FAIL(gen_datum_rowkey(expected_left_val, key_cnt, datum_start_key))) {
    STORAGE_LOG(WARN, "Failed to gen start rowkey", K(ret));
  } else if (OB_FAIL(gen_datum_rowkey(expected_right_val, key_cnt, datum_end_key))) {
    STORAGE_LOG(WARN, "Failed to gen end rowkey", K(ret));
  } else if (!datum_range_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid range", K(ret));
  } else {
    const blocksstable::ObDatumRowkey &range_start_rowkey = datum_range_.get_start_key();
    const blocksstable::ObDatumRowkey &range_end_rowkey = datum_range_.get_end_key();

    if (OB_FAIL(range_start_rowkey.equal(datum_start_key, datum_utils_, is_equal))) {
      STORAGE_LOG(WARN, "Failed to compare start key", K(ret));
    } else if (is_left_closed != datum_range_.is_left_closed()) {
      is_equal = false;
    } else if (is_equal && OB_FAIL(range_end_rowkey.equal(datum_end_key, datum_utils_, is_equal))) {
      STORAGE_LOG(WARN, "Failed to compare end rowkey", K(ret));
    } else if (is_right_closed != datum_range_.is_right_closed()) {
      is_equal = false;
    }
  }
  return ret;
}

int FakeObTableScanRange::check_datum_min_column(
    const int64_t src_datum_cnt,
    const int64_t split_datum_cnt,
    const int64_t expected_left_val,
    const int64_t expected_right_val,
    int check_type)
{
  int ret = OB_SUCCESS;
  if (split_datum_cnt > src_datum_cnt) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "split datum cnt should not large than src datum cnt", K(ret));
  } else if (datum_range_.is_valid()) {
    const blocksstable::ObDatumRowkey &range_start_rowkey = datum_range_.get_start_key();
    const blocksstable::ObDatumRowkey &range_end_rowkey = datum_range_.get_end_key();
    if (src_datum_cnt != range_start_rowkey.get_datum_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "datum cnt after split not equal to src datum", K(ret));
    } else if (src_datum_cnt != range_end_rowkey.get_datum_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "datum cnt after split not equal to src datum", K(ret));
    }
    // start key
    if (check_type == CHECK_DATUM_MIN_TYPE::BOTH || check_type == CHECK_DATUM_MIN_TYPE::LEFT) {
      for (int64_t i = 0; OB_SUCC(ret) && i < range_start_rowkey.get_datum_cnt(); ++i) {
        if (i < split_datum_cnt) {
          int64_t datum_left = range_start_rowkey.get_datum(i).get_int();
          if (datum_left != expected_left_val) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "datum left value not as expected", K(ret), K(expected_left_val), K(datum_left));
          }
        } else if (!range_start_rowkey.get_datum(i).is_min()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "ext datum column not equal to min value.", K(ret), K(range_start_rowkey.get_datum(i).get_int()));
        }
      }
    }
    // end key
    if (check_type == CHECK_DATUM_MIN_TYPE::BOTH || check_type == CHECK_DATUM_MIN_TYPE::RIGHT) {
      for (int64_t i = 0; OB_SUCC(ret) && i < range_end_rowkey.get_datum_cnt(); ++i) {
        if (i < split_datum_cnt) {
          int64_t datum_right = range_end_rowkey.get_datum(i).get_int();
          if (datum_right != expected_right_val) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "datum right value not as expected", K(ret), K(expected_right_val), K(datum_right));
          }
        } else if (!range_end_rowkey.get_datum(i).is_min()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "ext datum column not equal to min value.", K(ret));
        }
      }
    }
  }
  return ret;
}


/*
  ATENTION!!!
    split range is left closed right open. like: [1, 10)
*/

// set empty origin key
TEST_F(FakeObTableScanRange, test_empty_origin_range)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set valid split start key
  set_need_set_split_end_key(true);      // set valid split end key
  set_need_set_src_range(false);         // set empty origin range
  set_split_type(ObTabletSplitType::RANGE);

  // datum range
  int64_t origin_start_val = 1;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 2;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = do_split_datum_range(split_start_val, split_end_val);
  // error
  ASSERT_NE(OB_SUCCESS, ret);
}

// set empty split range key
TEST_F(FakeObTableScanRange, test_empty_split_range)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(false);    // set empty split start key
  set_need_set_split_end_key(false);      // set empty split end key
  set_need_set_src_range(true);           // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 2;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 1;

  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = do_split_datum_range(split_start_val, split_end_val);
  // error
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret); // empty key is invalid， return -4016
}

// only set split start key
TEST_F(FakeObTableScanRange, test_only_set_split_start_key)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);     // set valid split start key
  set_need_set_split_end_key(false);      // set valid split end key
  set_need_set_src_range(true);           // set empty origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 2;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = do_split_datum_range(split_start_val, split_end_val);
  // error
  ASSERT_NE(OB_SUCCESS, ret);
}

// only set split end key
TEST_F(FakeObTableScanRange, test_only_set_split_end_key)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(false);    // set empty split start key
  set_need_set_split_end_key(false);      // set empty split end key
  set_need_set_src_range(true);           // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 2;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  // error
  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_NE(OB_SUCCESS, ret);
}

// origin range = (1,5)
// split range = [2,4)
// result = [2,4)
TEST_F(FakeObTableScanRange, test_origin_range_outside_split_range)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 2;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 1;

  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_open();
  set_right_open();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  // check result

  // [2,4)
  int64_t expected_left_val = 2;
  int64_t expected_right_val = 4;
  bool is_left_closed = true;
  bool is_right_closed = false;
  bool is_equal = false;

  ret = check_datum_range_result(expected_left_val, expected_right_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_equal);
}

// origin range = (2,4)
// split range = [1,5)
// result = (2,4)
TEST_F(FakeObTableScanRange, test_origin_range_inside_split_range_01)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 4;
  int64_t split_start_val = 1;
  int64_t split_end_val = 5;
  int64_t origin_key_cnt = 1;

  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_open();
  set_right_open();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // (2,4)
  int64_t expected_start_val = 2;
  int64_t expected_end_val = 4;
  bool is_left_closed = false;
  bool is_right_closed = false;
  bool is_equal = false;

  ret = check_datum_range_result(expected_start_val, expected_end_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_equal);
}

// origin range = [2,4]
// split range = [1,5)
// result = [2,4]
TEST_F(FakeObTableScanRange, test_origin_range_inside_split_range_02)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 4;
  int64_t split_start_val = 1;
  int64_t split_end_val = 5;
  int64_t origin_key_cnt = 1;

  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_closed();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // [2,4]
  int64_t expected_start_val = 2;
  int64_t expected_end_val = 4;
  bool is_left_closed = true;
  bool is_right_closed = true;
  bool is_equal = false;

  ret = check_datum_range_result(expected_start_val, expected_end_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_equal);
}

// ori range = (2,4)
// split range = [1,3)
// result = (2,3)
TEST_F(FakeObTableScanRange, test_origin_range_include_split_end_key_01)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 4;
  int64_t split_start_val = 1;
  int64_t split_end_val = 3;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_open();
  set_right_open();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // (2,3)
  int64_t expected_start_val = 2;
  int64_t expected_end_val = 3;
  bool is_left_closed = false;
  bool is_right_closed = false;
  bool is_equal = false;
  ret = check_datum_range_result(expected_start_val, expected_end_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_equal);
}

// origin range = [2,4]
// split range = [1,3)
// result = [2,3)
TEST_F(FakeObTableScanRange, test_origin_range_include_split_end_key_02)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 4;
  int64_t split_start_val = 1;
  int64_t split_end_val = 3;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_closed();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // [2,3)
  int64_t expected_start_val = 2;
  int64_t expected_end_val = 3;
  bool is_left_closed = true;
  bool is_right_closed = false;
  bool is_equal = false;
  ret = check_datum_range_result(expected_start_val, expected_end_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_equal);
}

// origin range = (1,3)
// split range = [2,4)
// result = [2,3)
TEST_F(FakeObTableScanRange, test_origin_range_include_split_start_key_01)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 3;
  int64_t split_start_val = 2;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 1;

  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_open();
  set_right_open();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t expected_start_val = 2;
  int64_t expected_end_val = 3;
  bool is_left_closed = true;
  bool is_right_closed = false;
  bool is_equal = false;

  ret = check_datum_range_result(expected_start_val, expected_end_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_equal);
}

// origin range = [1,3]
// split range = [2,4)
// result = [2,3]
TEST_F(FakeObTableScanRange, test_origin_range_include_split_start_key_02)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 3;
  int64_t split_start_val = 2;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 1;

  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_closed();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // [2,3]
  int64_t expected_start_val = 2;
  int64_t expected_end_val = 3;
  bool is_left_closed = true;
  bool is_right_closed = true;
  bool is_equal = false;

  ret = check_datum_range_result(expected_start_val, expected_end_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_equal);
}

// src range = [2,5]
// split range = [5,6)
// result = [5]
TEST_F(FakeObTableScanRange, test_split_range_result_only_left_key)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 5;
  int64_t split_end_val = 6;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_closed();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // [5]
  int64_t expected_left_val = 5;
  int64_t expected_right_val = 5;
  bool is_left_closed = true;
  bool is_right_closed = true;
  bool is_equal = false;

  ret = check_datum_range_result(expected_left_val, expected_right_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_equal);
}

// src range = [2,5]
// split range = [1,2)
// result = [empty]
TEST_F(FakeObTableScanRange, test_split_range_result_only_right_key)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 1;
  int64_t split_end_val = 2;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_closed();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t expected_left_val = 2;
  int64_t expected_right_val = 2;
  bool is_left_closed = true;
  bool is_right_closed = true;
  bool is_equal = false;

  ret = check_datum_range_result(expected_left_val, expected_right_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret); // empty range
}

// src range = (2,5)
// split range = [1,2)
// result = []
TEST_F(FakeObTableScanRange, test_split_range_empty_case_01)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 1;
  int64_t split_end_val = 2;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_open();
  set_right_open();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t expected_left_val = 2;
  int64_t expected_right_val = 2;
  bool is_left_closed = true;
  bool is_right_closed = true;
  bool is_equal = false;

  ret = check_datum_range_result(expected_left_val, expected_right_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret); // empty range
}

// src range = (2,5)
// split range = [5,6)
// result = []
TEST_F(FakeObTableScanRange, test_split_range_empty_case_02)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 5;
  int64_t split_end_val = 6;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_open();
  set_right_open();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t expected_left_val = 2;
  int64_t expected_right_val = 2;
  bool is_left_closed = true;
  bool is_right_closed = true;
  bool is_equal = false;

  ret = check_datum_range_result(expected_left_val, expected_right_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// src range = (2,2]
// split range = [2,3)
// result = []
TEST_F(FakeObTableScanRange, test_split_range_empty_case_03)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 2;
  int64_t split_start_val = 2;
  int64_t split_end_val = 3;
  int64_t origin_key_cnt = 1;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  // attention !!! because ObNewRange need start key not equal to end key in (] case
  // here modify start key to 2
  ret = set_datum_key(true, false, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_open();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t expected_left_val = 2;
  int64_t expected_right_val = 2;
  bool is_left_closed = true;
  bool is_right_closed = true;
  bool is_equal = false;

  ret = check_datum_range_result(expected_left_val, expected_right_val, is_left_closed, is_right_closed, is_equal);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}


// src range = [(1,1), (5,5)]
// split range = [2,4)
// result = [(2,min),(3,min)]
TEST_F(FakeObTableScanRange, test_split_column_cnt_not_match_with_rowkey_cnt_both_min)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 2;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 2;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_closed();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t expected_left_val = 2;
  int64_t expected_right_val = 4;
  int64_t split_column_cnt = 1;

  ret = check_datum_min_column(origin_key_cnt, split_column_cnt, expected_left_val, expected_right_val, CHECK_DATUM_MIN_TYPE::BOTH);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(FakeObTableScanRange, test_split_column_cnt_not_match_with_rowkey_cnt_left_min)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 1;
  int64_t origin_end_val = 4;
  int64_t split_start_val = 2;
  int64_t split_end_val = 5;
  int64_t origin_key_cnt = 2;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_closed();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t expected_left_val = 2;
  int64_t expected_right_val = 4;
  int64_t split_column_cnt = 1;
  ret = check_datum_min_column(origin_key_cnt, split_column_cnt, expected_left_val, expected_right_val, CHECK_DATUM_MIN_TYPE::LEFT);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(FakeObTableScanRange, test_split_column_cnt_not_match_with_rowkey_right_min)
{
  int ret = OB_SUCCESS;
  set_need_set_split_start_key(true);    // set split start key
  set_need_set_split_end_key(true);      // set split end key
  set_need_set_src_range(true);          // set valid origin range
  set_split_type(ObTabletSplitType::RANGE);

  int64_t origin_start_val = 2;
  int64_t origin_end_val = 5;
  int64_t split_start_val = 1;
  int64_t split_end_val = 4;
  int64_t origin_key_cnt = 2;
  // datum range
  ret = gen_datum_range(origin_start_val, origin_end_val, origin_key_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  set_left_closed();
  set_right_closed();

  ret = do_split_datum_range(split_start_val, split_end_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t expected_left_val = 2;
  int64_t expected_right_val = 4;
  int64_t split_column_cnt = 1;

  ret = check_datum_min_column(origin_key_cnt, split_column_cnt, expected_left_val, expected_right_val, CHECK_DATUM_MIN_TYPE::RIGHT);
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_auto_partition_split_range.log");
  OB_LOGGER.set_file_name("test_auto_partition_split_range.log", true);
  OB_LOGGER.set_log_level("WARN");
  CLOG_LOG(INFO, "begin unittest: test_auto_partition_split_range");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
