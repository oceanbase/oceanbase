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
#include "mock_ob_iterator.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase
{
namespace common
{
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

ObMockIterator::ObMockIterator(bool reverse)
    : rows_(),
      cursor_(0),
      reverse_(reverse),
      trans_id_(888),
      allocator_("StTemp")
{
}

ObMockIterator::~ObMockIterator()
{
  reset();
}

void ObMockIterator::setup_start_cursor()
{
  if (reverse_) {
    cursor_ = rows_.count() - 1;
  } else {
    cursor_ = 0;
  }
}

void ObMockIterator::advance()
{
  if (reverse_) {
    --cursor_;
  } else {
    ++cursor_;
  }
}

bool ObMockIterator::end_of_row() const
{
  if (reverse_) {
    return cursor_ < 0;
  } else {
    return cursor_ >= rows_.count();
  }
}

int ObMockIterator::get_next_row(ObStoreRow *&row)
{
  int ret = OB_SUCCESS;
  if (end_of_row()) {
    row = NULL;
    ret = OB_ITER_END;
  } else {
    row = rows_[cursor_];
    advance();
  }
  return ret;
}

int ObMockIterator::get_next_row(const ObStoreRow *&row)
{
  return get_next_row(const_cast<ObStoreRow *&>(row));
}

int ObMockIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (end_of_row()) {
    row = NULL;
    ret = OB_ITER_END;
  } else {
    row = &rows_[cursor_]->row_val_;
    advance();
  }
  return ret;
}

int ObMockIterator::get_row(const int64_t idx, const ObStoreRow *&row) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= rows_.count()) {
    STORAGE_LOG(WARN, "failed to idx", K(idx), K(rows_.count()));
    row = NULL;
    ret = OB_ITER_END;
  } else {
    row = rows_[idx];
  }
  return ret;
}

int ObMockIterator::get_row(const int64_t idx, ObStoreRow *&row) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= rows_.count()) {
    STORAGE_LOG(WARN, "failed to idx", K(idx), K(rows_.count()));
    row = NULL;
    ret = OB_ITER_END;
  } else {
    row = rows_[idx];
  }
  return ret;
}

int ObMockIterator::get_row(const int64_t idx, ObNewRow *&row) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= rows_.count()) {
    STORAGE_LOG(WARN, "failed to idx", K(idx), K(rows_.count()));
    row = NULL;
    ret = OB_ITER_END;
  } else {
    row = &rows_[idx]->row_val_;
  }
  return ret;
}

// deep copy
int ObMockIterator::add_row(ObStoreRow *row)
{
  int ret = OB_SUCCESS;
  ObStoreRow *row_clone = NULL;
  if (NULL == row) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    ObRowStoreType row_type = FLAT_ROW_STORE;
    if (OB_FAIL(malloc_store_row(allocator_, row->row_val_.count_, row_clone, row_type))) {
      STORAGE_LOG(WARN, "failed to malloc store row", K(ret));
    } else {
      row_clone->flag_ = row->flag_;
      row_clone->from_base_ = row->from_base_;
      row_clone->row_type_flag_ = row->row_type_flag_;
      row_clone->is_get_ = row->is_get_;
      row_clone->scan_index_ = row->scan_index_;
      if (row->row_type_flag_.is_uncommitted_row()) { // make all row point to the same trans_id
        row_clone->trans_id_ = row->trans_id_.is_valid() ? row->trans_id_ : trans_id_;
      }
      row_clone->row_val_.count_ = row->row_val_.count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < row->row_val_.count_; ++i) {
        if (OB_SUCCESS != (ret = ob_write_obj(allocator_, row->row_val_.cells_[i],
                     row_clone->row_val_.cells_[i]))) {
          STORAGE_LOG(WARN, "ob write obj error.", K(ret) ,K(row));
        }
      }

      if  (OB_SUCC(ret)) {
        if (OB_SUCCESS != (ret = rows_.push_back(row_clone))) {
          STORAGE_LOG(WARN, "add row failed", K(ret), K(row));
        } else {
          setup_start_cursor();
        }
      }
    }

    if (OB_SUCCESS != ret && row_clone != NULL) {
      free_store_row(allocator_, row_clone);
    }
  }
  return ret;
}

void ObMockIterator::reset_iter()
{
  setup_start_cursor();
}

void ObMockIterator::reset()
{
  cursor_ = 0;
  for (int64_t i = 0; i < rows_.count(); ++i) {
    ObStoreRow *row = rows_[i];
    free_store_row(allocator_, row);
  }
  rows_.reset();
  allocator_.clear();
}

int ObMockIterator::from(const char *cstr, char escape, uint16_t* col_id_array, int64_t *result_col_id_array)
{
  return from(ObString::make_string(cstr), escape, col_id_array, result_col_id_array);
}

int ObMockIterator::from(const ObString &str, char escape, uint16_t *col_id_array, int64_t *result_col_id_array)
{
  int ret = OB_SUCCESS;
  ObMockIteratorBuilder builder;
  ObArenaAllocator buffer("StTemp");
  if (OB_SUCCESS != (ret != builder.init(&buffer, escape))) {
    STORAGE_LOG(WARN, "init builder failed");
  } else {
    if (OB_ISNULL(result_col_id_array)) {
      ret = builder.parse(str, *this, col_id_array);
    } else {
      ret = builder.parse_with_specified_col_ids(str, *this, col_id_array, result_col_id_array);
    }

  }
  buffer.clear();
  return ret;
}

bool ObMockIterator::equals(const ObNewRow &r1, const ObNewRow &r2)
{
  bool bool_ret = true;
  int64_t idx = 0;
  if (!r1.cells_ || !r2.cells_) {
    bool_ret = false;
  } else if (r1.count_ != r2.count_) {
    bool_ret = false;
  } else {
    for (idx = 0; idx < r1.count_ && bool_ret; idx++) {
      if (r1.cells_[idx].get_type() != r2.cells_[idx].get_type()) {
        bool_ret = false;
      } else if (r1.cells_[idx].is_null() && r2.cells_[idx].is_null()) {
        continue;
      } else if (r1.cells_[idx].is_null() || r2.cells_[idx].is_null()) {
        bool_ret = false;
      } else if (r1.cells_[idx] != r2.cells_[idx]) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

bool ObMockIterator::equals(uint16_t *col_id1, uint16_t *col_id2, const int64_t col_cnt)
{
  bool bool_ret = true;
  int64_t idx = 0;
  if (OB_ISNULL(col_id1) || OB_ISNULL(col_id2)) {
    bool_ret = false;
  } else {
    for (idx = 0; idx < col_cnt && bool_ret; idx++) {
      if (col_id1[idx] != col_id2[idx]) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

bool ObMockIterator::equals(const ObStoreRow &r1, const ObStoreRow &r2,
    const bool cmp_multi_version_row_flag, const bool cmp_is_get_and_scan_index)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  STORAGE_LOG(INFO, "compare two rows", K(r1), K(r2));
  if (r1.flag_ != r2.flag_) {
    STORAGE_LOG(WARN, "flag not equals", K(r1), K(r2));
  } else if (!cmp_multi_version_row_flag && (r1.flag_.is_not_exist() || r1.flag_.is_delete())) {
    STORAGE_LOG(DEBUG, "flag is not row exist, return true", K(r1), K(r2));
    bool_ret = true;
  } else if (cmp_multi_version_row_flag && r1.row_type_flag_.flag_ != r2.row_type_flag_.flag_) {
    STORAGE_LOG(WARN, "row_type_flag not equals", K(r1), K(r2));
    bool_ret = false;
  } else if (cmp_is_get_and_scan_index
      && (r1.is_get_ != r2.is_get_ || r1.scan_index_ != r2.scan_index_)) {
    STORAGE_LOG(WARN, "is_get or scan_index not equals", K(r1), K(r2));
    bool_ret = false;
  } else if (r1.is_sparse_row_ != r2.is_sparse_row_){
    STORAGE_LOG(WARN, "is_sparse_row not equals", K(r1), K(r2));
    bool_ret = false;
  } else {
    bool_ret = equals(r1.row_val_, r2.row_val_);
    STORAGE_LOG(INFO, "compare two rows", K(bool_ret), K(r1), K(r2));
  }
  if (bool_ret && r1.is_sparse_row_){ // both are sparse row
    bool_ret = equals(r1.column_ids_, r2.column_ids_, r1.row_val_.count_);
  }
  return bool_ret;
}

bool ObMockIterator::equals(int64_t idx, ObNewRow &other_row) const
{
  bool bool_ret = false;
  const ObStoreRow *this_row = NULL;
  if (OB_SUCCESS != get_row(idx, this_row)) {
    STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid idx");
    bool_ret = false;
  } else {
    bool_ret = equals(this_row->row_val_, other_row);
    if (!bool_ret) {
      STORAGE_LOG(INFO, "new row is not equal",
                  K(idx), K(this_row->row_val_), K(other_row));
    }
  }
  return bool_ret;
}

bool ObMockIterator::equals(int64_t idx, ObStoreRow &other_row) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  const ObStoreRow *this_row = NULL;
  if (OB_SUCCESS != get_row(idx, this_row)) {
    STORAGE_LOG(WARN, "invalid idx");
    bool_ret = false;
  } else {
    bool_ret = equals(*this_row, other_row);
    if (!bool_ret) {
      STORAGE_LOG(WARN, "store row is not equal",
                  K(idx), K(this_row->row_val_), K(other_row));
    }
  }
  return bool_ret;
}

bool ObMockIterator::equals(int64_t idx, const ObStoreRow &other_row) const
{
  return equals(idx, const_cast<ObStoreRow &>(other_row));
}

///////////////////////////////////////////////////////////////////////////
bool ObMockIteratorBuilder::is_static_inited_ = false;
ObHashMap<ObString, ObMockIteratorBuilder::ObParseFunc>
ObMockIteratorBuilder::str_to_obj_parse_func_;
ObHashMap<ObString, ObMockIteratorBuilder::ObParseFunc>
ObMockIteratorBuilder::str_to_info_parse_func_;
ObHashMap<ObString, ObObjMeta*>
ObMockIteratorBuilder::str_to_obj_type_;
ObHashMap<ObString, int64_t> ObMockIteratorBuilder::str_to_flag_;
ObHashMap<ObString, ObDmlFlag> ObMockIteratorBuilder::str_to_dml_;
ObHashMap<ObString, bool> ObMockIteratorBuilder::str_to_base_;
ObHashMap<ObString, bool> ObMockIteratorBuilder::str_to_is_get_;
ObHashMap<ObString, transaction::ObTransID> ObMockIteratorBuilder::str_to_trans_id_;
ObHashMap<ObString, uint8_t> ObMockIteratorBuilder::str_to_multi_version_row_flag_;
transaction::ObTransID ObMockIteratorBuilder::trans_id_list_[ObMockIteratorBuilder::TRANS_ID_NUM];
const char *ObMockIteratorBuilder::STR_NOP = "nop";
const char *ObMockIteratorBuilder::STR_NULL = "null";
const char *ObMockIteratorBuilder::STR_MAX = "max";
const char *ObMockIteratorBuilder::STR_MIN = "min";
const char *ObMockIteratorBuilder::STR_MIN_2_TRANS = "min2";
const char *ObMockIteratorBuilder::STR_MAGIC = "magic";
ObObjMeta ObMockIteratorBuilder::INT_TYPE;
ObObjMeta ObMockIteratorBuilder::BIGINT_TYPE;
ObObjMeta ObMockIteratorBuilder::VAR_TYPE;
ObObjMeta ObMockIteratorBuilder::LOB_TYPE;
ObObjMeta ObMockIteratorBuilder::TS_TYPE;
ObObjMeta ObMockIteratorBuilder::NU_TYPE;

int ObMockIteratorBuilder::parse_varchar(ObIAllocator *allocator,
                                         const ObString &word,
                                         ObStoreRow &row,
                                         int64_t &idx)
{
  OB_ASSERT(allocator);
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (idx >= row.row_val_.count_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else if (NULL == (buf = static_cast<char *>(allocator->alloc(word.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObObj obj_tmp;
    obj_tmp.set_varchar(word);
    // obj_tmp.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    obj_tmp.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    ret = ob_write_obj(*allocator, obj_tmp, row.row_val_.cells_[idx++]);
  }
  return ret;
}

int ObMockIteratorBuilder::parse_lob(ObIAllocator *allocator,
                                         const ObString &word,
                                         ObStoreRow &row,
                                         int64_t &idx)
{
  OB_ASSERT(allocator);
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (idx >= row.row_val_.count_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else if (NULL == (buf = static_cast<char *>(allocator->alloc(sizeof(ObLobCommon))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    char str[MAX_DATA_LENGTH];
    int64_t val = 0;
    char *end_ptr = NULL;

    snprintf(str, MAX_DATA_LENGTH, "%.*s", word.length(), word.ptr());
    errno = 0;
    if (2 < word.length() && 0 == strncasecmp(str, "0x", 2)) {
      val = strtol(str, &end_ptr, 16);
    } else if (1 < word.length() && 0 == strncasecmp(str, "0", 1)) {
      val = strtol(str, &end_ptr, 8);
    } else {
      val = strtol(str, &end_ptr, 10);
    }
    if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
        || (errno != 0 && val == 0)
        || (INT_MAX < val || INT_MIN > val)) {
      STORAGE_LOG(WARN, "strtol fail", K(val), K(word), K(errno));
      ret = OB_NUMERIC_OVERFLOW;
    } else if (str == end_ptr) {
      STORAGE_LOG(WARN, "no digits found");
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    } else if (val > 48) {
      ret = OB_NUMERIC_OVERFLOW;
      STORAGE_LOG(WARN, "too big lob index cnt", K(val), K(word));
    } else {
      ObLobCommon *lob_data = nullptr;
      const int64_t block_size = 1 << 20;
      lob_data = new (buf) ObLobCommon();
      ObObj obj_tmp;
      obj_tmp.set_lob_value(ObLongTextType, lob_data, lob_data->get_handle_size(val * block_size));
      obj_tmp.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      ret = ob_write_obj(*allocator, obj_tmp, row.row_val_.cells_[idx++]);
    }
  }
  return ret;
}

/*
int ObMockIteratorBuilder::parse_bool(ObIAllocator *allocator,
                                      const ObString &word, ObStoreRow &row, int64_t &idx)
{
  UNUSED(allocator);
  OB_ASSERT(idx < row.row_val_.count_);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  ObObj &obj = row.row_val_.cells_[idx++];
  static const int64_t len_true = strlen("true");
  static const int64_t len_false = strlen("false");
  if (word.length() == len_true
      && 0 == strncasecmp(word.ptr(), "true", len_true)) {
    obj.set_bool(true);
  } else if (word.length() == len_false
             && 0 == strncasecmp(word.ptr(), "false", len_false)) {
    obj.set_bool(false);
  } else if (word.length() == 1
             && 0 == strncasecmp(word.ptr(), "1", 1)) {
    obj.set_bool(true);
  } else if (word.length() == 1
             && 0 == strncasecmp(word.ptr(), "0", 1)) {
    obj.set_bool(false);
  } else {
    ret = OB_ERROR;
  }

  return ret;
}
*/

int ObMockIteratorBuilder::parse_timestamp(ObIAllocator *allocator,
                                           const ObString &word,
                                           ObStoreRow &row,
                                           int64_t &idx)
{
  UNUSED(allocator);

  int ret = OB_SUCCESS;
  int64_t usec = 0;
  if (idx >= row.row_val_.count_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else if (OB_SUCCESS != (ret = ObTimeUtility2::str_to_usec(word, usec))) {
    STORAGE_LOG(WARN, "str to microsecond failed", K(word), K(usec), K(ret));
  } else {
    row.row_val_.cells_[idx++].set_timestamp(usec);
  }
  return ret;
}

int ObMockIteratorBuilder::parse_int(ObIAllocator *allocator,
                                     const ObString &word,
                                     ObStoreRow &row,
                                     int64_t &idx)
{
  UNUSED(allocator);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  if (idx >= row.row_val_.count_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    char str[MAX_DATA_LENGTH];
    int64_t val = 0;
    char *end_ptr = NULL;

    snprintf(str, MAX_DATA_LENGTH, "%.*s", word.length(), word.ptr());
    errno = 0;
    if (2 < word.length() && 0 == strncasecmp(str, "0x", 2)) {
      val = strtol(str, &end_ptr, 16);
    } else if (1 < word.length() && 0 == strncasecmp(str, "0", 1)) {
      val = strtol(str, &end_ptr, 8);
    } else {
      val = strtol(str, &end_ptr, 10);
    }

    if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
        || (errno != 0 && val == 0)
        || (INT_MAX < val || INT_MIN > val)) {
      STORAGE_LOG(WARN, "strtol fail", K(val), K(word), K(errno));
      ret = OB_NUMERIC_OVERFLOW;
    } else {
      if (str == end_ptr) {
        STORAGE_LOG(WARN, "no digits found");
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      } else {
        row.row_val_.cells_[idx++].set_int32(static_cast<int32_t>(val));
      }
    }
  }

  return ret;
}

int ObMockIteratorBuilder::parse_bigint(ObIAllocator *allocator,
                                     const ObString &word,
                                     ObStoreRow &row,
                                     int64_t &idx)
{
  UNUSED(allocator);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  if (idx >= row.row_val_.count_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    char str[MAX_DATA_LENGTH];
    int64_t val = 0;
    char *end_ptr = NULL;

    snprintf(str, MAX_DATA_LENGTH, "%.*s", word.length(), word.ptr());
    errno = 0;
    if (2 < word.length() && 0 == strncasecmp(str, "0x", 2)) {
      val = strtoll(str, &end_ptr, 16);
    } else if (1 < word.length() && 0 == strncasecmp(str, "0", 1)) {
      val = strtoll(str, &end_ptr, 8);
    } else {
      val = strtoll(str, &end_ptr, 10);
    }

    if ((errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN))
        || (errno != 0 && val == 0)) {
      STORAGE_LOG(WARN, "strtoll fail", K(val), K(word), K(errno));
      ret = OB_NUMERIC_OVERFLOW;
    } else {
      if (str == end_ptr) {
        STORAGE_LOG(WARN, "no digits found");
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      } else {
        row.row_val_.cells_[idx++].set_int(val);
      }
    }
  }

  return ret;
}

int ObMockIteratorBuilder::parse_number(ObIAllocator *allocator,
                                        const ObString &word,
                                        ObStoreRow &row,
                                        int64_t &idx)
{
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  number::ObNumber nmb;
  if (idx >= row.row_val_.count_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else if (OB_SUCCESS != (ret = nmb.from(word.ptr(), word.length(), *allocator))) {
    STORAGE_LOG(WARN, "parse number failed", K(ret), K(word));
  } else {
    row.row_val_.cells_[idx++].set_number(nmb);
  }
  return ret;
}

int ObMockIteratorBuilder::parse_dml(ObIAllocator *allocator,
                                     const ObString &word,
                                     ObStoreRow &row,
                                     int64_t &idx)
{
  UNUSEDx(allocator, word, row, idx);
  return OB_SUCCESS;
}

int ObMockIteratorBuilder::parse_first_dml(ObIAllocator *allocator,
                                           const ObString &word,
                                           ObStoreRow &row,
                                           int64_t &idx)
{
  UNUSEDx(allocator, word, row, idx);
  return OB_SUCCESS;
}

int ObMockIteratorBuilder::parse_flag(ObIAllocator *allocator,
                                      const ObString &word,
                                      ObStoreRow &row,
                                      int64_t &idx)
{
  UNUSED(allocator);
  UNUSED(idx);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  int64_t flag = 0;
  if (OB_SUCCESS != str_to_flag_.get_refactored(word, flag)) {
    STORAGE_LOG(WARN, "failed to parse flag", K(word));
    ret = OB_HASH_NOT_EXIST;
  } else {
    row.flag_ = (ObDmlFlag)flag;
  }
  return ret;
}

int ObMockIteratorBuilder::parse_base(ObIAllocator *allocator,
                                      const ObString &word,
                                      ObStoreRow &row,
                                      int64_t &idx)
{
  UNUSED(allocator);
  UNUSED(idx);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  if (OB_SUCCESS != str_to_base_.get_refactored(word, row.from_base_)) {
    STORAGE_LOG(WARN, "failed to parse from base", K(word));
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

int ObMockIteratorBuilder::parse_is_get(ObIAllocator *allocator,
                                        const ObString &word,
                                        ObStoreRow &row,
                                        int64_t &idx)
{
  UNUSED(allocator);
  UNUSED(idx);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  if (OB_SUCCESS != str_to_is_get_.get_refactored(word, row.is_get_)) {
    STORAGE_LOG(WARN, "failed to parse is_get", K(word));
    ret = OB_HASH_NOT_EXIST;
  } else {
    STORAGE_LOG(DEBUG, "parse is_get", K(row));
  }
  return ret;
}

int ObMockIteratorBuilder::parse_trans_id(ObIAllocator *allocator,
                                        const ObString &word,
                                        ObStoreRow &row,
                                        int64_t &idx)
{
  UNUSED(allocator);
  UNUSED(idx);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  if (OB_SUCCESS != str_to_trans_id_.get_refactored(word, row.trans_id_)) {
    STORAGE_LOG(WARN, "failed to parse is_get", K(word));
    ret = OB_HASH_NOT_EXIST;
  } else {
    STORAGE_LOG(DEBUG, "parse str_to_trans_id_", K(row), K(word), K(row.trans_id_));
  }
  return ret;
}

int ObMockIteratorBuilder::parse_scan_index(ObIAllocator *allocator,
                                            const ObString &word,
                                            ObStoreRow &row,
                                            int64_t &idx)
{
  UNUSED(allocator);
  UNUSED(idx);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());
  int ret = OB_SUCCESS;
  char str[MAX_DATA_LENGTH];
  int64_t val = 0;
  char *end_ptr = NULL;

  snprintf(str, MAX_DATA_LENGTH, "%.*s", word.length(), word.ptr());
  errno = 0;
  if (2 < word.length() && 0 == strncasecmp(str, "0x", 2)) {
    val = strtoll(str, &end_ptr, 16);
  } else if (1 < word.length() && 0 == strncasecmp(str, "0", 1)) {
    val = strtoll(str, &end_ptr, 8);
  } else {
    val = strtoll(str, &end_ptr, 10);
  }

  if ((errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN))
      || (errno != 0 && val == 0)) {
    STORAGE_LOG(WARN, "strtoll fail", K(val), K(word), K(errno));
    ret = OB_NUMERIC_OVERFLOW;
  } else {
    if (str == end_ptr) {
      STORAGE_LOG(WARN, "no digits found");
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    } else {
      row.scan_index_ = val;
      STORAGE_LOG(DEBUG, "parse scan_index", K(row));
    }
  }
  return ret;
}

int ObMockIteratorBuilder::parse_multi_version_row_flag(ObIAllocator *allocator,
                                                        const ObString &word,
                                                        ObStoreRow &row,
                                                        int64_t &idx)
{
  UNUSED(allocator);
  UNUSED(idx);
  OB_ASSERT(NULL != word.ptr() && 0 <= word.length());

  int ret = OB_SUCCESS;
  if (OB_SUCCESS != str_to_multi_version_row_flag_.get_refactored(word, row.row_type_flag_.flag_)) {
    STORAGE_LOG(WARN, "failed to parse multi version row flag", K(word));
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

// init static var
int ObMockIteratorBuilder::static_init()
{
  int ret = OB_SUCCESS;

  INT_TYPE.set_int32();
  BIGINT_TYPE.set_int();
  NU_TYPE.set_number();
  TS_TYPE.set_timestamp();
  VAR_TYPE.set_varchar();
  VAR_TYPE.set_collation_level(CS_LEVEL_IMPLICIT);
  VAR_TYPE.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  LOB_TYPE.set_type_simple(ObLongTextType);
  LOB_TYPE.set_collation_level(CS_LEVEL_IMPLICIT);
  LOB_TYPE.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

  for (int i = 1; i <= TRANS_ID_NUM; ++i) {
    trans_id_list_[i - 1] = transaction::ObTransID(i);
  }

  if (OB_SUCCESS != (ret = str_to_obj_parse_func_.create(
                               cal_next_prime(TYPE_NUM * 10),
                               ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(WARN, "out of memory");
  } else if (OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("int"),
                 ObMockIteratorBuilder::parse_int)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("bigint"),
                 ObMockIteratorBuilder::parse_bigint)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("varchar"),
                 ObMockIteratorBuilder::parse_varchar)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("lob"),
                 ObMockIteratorBuilder::parse_lob)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("var"),
                 ObMockIteratorBuilder::parse_varchar)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("ts"),
                 ObMockIteratorBuilder::parse_timestamp)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("timestamp"),
                 ObMockIteratorBuilder::parse_timestamp)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("num"),
                 ObMockIteratorBuilder::parse_number)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("number"),
                 ObMockIteratorBuilder::parse_number)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "obj parse func hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init obj parse func hashtable");
  }

  if (OB_SUCCESS != (ret = str_to_obj_type_.create(
                               cal_next_prime(TYPE_NUM * 10),
                               ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(WARN, "out of memory");
  } else if (OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("int"), &INT_TYPE)
             || OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("bigint"), &BIGINT_TYPE)
             || OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("varchar"), &VAR_TYPE)
             || OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("var"), &VAR_TYPE)
             || OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("lob"), &LOB_TYPE)
             || OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("ts"), &TS_TYPE)
             || OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("timestamp"), &TS_TYPE)
             || OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("num"), &NU_TYPE)
             || OB_SUCCESS != str_to_obj_type_.set_refactored(
                 ObString::make_string("number"), &NU_TYPE)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "obj type hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init obj type hashtable");
  }

  if (OB_SUCCESS != (ret = str_to_info_parse_func_.create(
                               cal_next_prime(INFO_NUM),
                               ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(DEBUG, "out of memory");
  } else if (OB_SUCCESS != str_to_info_parse_func_.set_refactored(
                 ObString::make_string("flag"),
                 ObMockIteratorBuilder::parse_flag)
             || OB_SUCCESS != str_to_info_parse_func_.set_refactored(
                 ObString::make_string("dml"),
                 ObMockIteratorBuilder::parse_dml)
             || OB_SUCCESS != str_to_info_parse_func_.set_refactored(
                 ObString::make_string("first_dml"),
                 ObMockIteratorBuilder::parse_first_dml)
             || OB_SUCCESS != str_to_info_parse_func_.set_refactored(
                 ObString::make_string("multi_version_row_flag"),
                 ObMockIteratorBuilder::parse_multi_version_row_flag)
             || OB_SUCCESS != str_to_info_parse_func_.set_refactored(
                 ObString::make_string("is_get"),
                 ObMockIteratorBuilder::parse_is_get)
             || OB_SUCCESS != str_to_info_parse_func_.set_refactored(
                 ObString::make_string("scan_index"),
                 ObMockIteratorBuilder::parse_scan_index)
             || OB_SUCCESS != str_to_info_parse_func_.set_refactored(
                 ObString::make_string("from_base"),
                 ObMockIteratorBuilder::parse_base)
             || OB_SUCCESS != str_to_info_parse_func_.set_refactored(
                 ObString::make_string("trans_id"),
                 ObMockIteratorBuilder::parse_trans_id)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "info parse func hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init info parse func hashtable");
  }

  if (ret == OB_SUCCESS
      && OB_SUCCESS != (ret = str_to_flag_.create(
                                  cal_next_prime(FLAG_NUM * 2),
                                  ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(WARN, "out of memory");
  } else if (OB_SUCCESS != str_to_flag_.set_refactored(
                 ObString::make_string("RF_ROW_DOES_NOT_EXIST"),
                 ObDmlFlag::DF_NOT_EXIST)
             || OB_SUCCESS != str_to_flag_.set_refactored(
                 ObString::make_string("EMPTY"),
                 ObDmlFlag::DF_NOT_EXIST)
             || OB_SUCCESS != str_to_flag_.set_refactored(
                 ObString::make_string("RF_ROW_EXIST"),
                 ObDmlFlag::DF_INSERT)
             || OB_SUCCESS != str_to_flag_.set_refactored(
                 ObString::make_string("EXIST"),
                 ObDmlFlag::DF_INSERT)
             || OB_SUCCESS != str_to_flag_.set_refactored(
                 ObString::make_string("UPDATE"),
                 ObDmlFlag::DF_UPDATE)
             || OB_SUCCESS != str_to_flag_.set_refactored(
                 ObString::make_string("INSERT"),
                 ObDmlFlag::DF_INSERT)
             || OB_SUCCESS != str_to_flag_.set_refactored(
                 ObString::make_string("LOCK"),
                 ObDmlFlag::DF_LOCK)
             || OB_SUCCESS != str_to_flag_.set_refactored(
                 ObString::make_string("DELETE"),
                 ObDmlFlag::DF_DELETE)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "flag hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init flag hashtable");
  }

  if (ret == OB_SUCCESS
      && OB_SUCCESS != (ret = str_to_dml_.create(
                                  cal_next_prime(DML_NUM * 2),
                                  ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(WARN, "out of memory");
  } else if (OB_SUCCESS != str_to_dml_.set_refactored(
                 ObString::make_string("INSERT"), DF_INSERT)
             || OB_SUCCESS != str_to_dml_.set_refactored(
                 ObString::make_string("UPDATE"), DF_UPDATE)
             || OB_SUCCESS != str_to_dml_.set_refactored(
                 ObString::make_string("DELETE"), DF_DELETE)
             || OB_SUCCESS != str_to_dml_.set_refactored(
                 ObString::make_string("LOCK"), DF_LOCK)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "dml hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init flag hashtable");
  }

  if (ret == OB_SUCCESS
      && OB_SUCCESS != (ret = str_to_base_.create(
                                  cal_next_prime(BASE_NUM * 2),
                                  ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(WARN, "out of memory");
  } else if (OB_SUCCESS != str_to_base_.set_refactored(
                 ObString::make_string("TRUE"), true)
             || OB_SUCCESS != str_to_base_.set_refactored(
                 ObString::make_string("FALSE"), false)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "from_base hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init from_base hashtable");
  }

  if (ret == OB_SUCCESS
      && OB_SUCCESS != (ret = str_to_is_get_.create(
                                  cal_next_prime(BASE_NUM * 2),
                                  ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(WARN, "out of memory");
  } else if (OB_SUCCESS != str_to_is_get_.set_refactored(
                 ObString::make_string("TRUE"), true)
             || OB_SUCCESS != str_to_is_get_.set_refactored(
                 ObString::make_string("FALSE"), false)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "is_get hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init is_get hashtable");
  }

  if (ret == OB_SUCCESS
      && OB_SUCCESS != (ret = str_to_trans_id_.create(
                                  cal_next_prime(BASE_NUM * 3),
                                  ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(WARN, "out of memory");
  } else if (OB_SUCCESS != str_to_trans_id_.set_refactored(
          ObString::make_string("trans_id_1"), trans_id_list_[0])
      || OB_SUCCESS != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_2"), trans_id_list_[1])
      || OB_SUCCESS != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_3"), trans_id_list_[2])
      || OB_SUCCESS != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_4"), trans_id_list_[3])
      || OB_SUCCESS != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_5"), trans_id_list_[4])
      || OB_SUCCESS
          != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_6"), trans_id_list_[5])
      || OB_SUCCESS
          != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_7"), trans_id_list_[6])
      || OB_SUCCESS
          != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_8"), trans_id_list_[7])
      || OB_SUCCESS
          != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_9"), trans_id_list_[8])
      || OB_SUCCESS
          != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_10"), trans_id_list_[9])
      || OB_SUCCESS
          != str_to_trans_id_.set_refactored(
              ObString::make_string("trans_id_0"), transaction::ObTransID(0))) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "str_to_trans_id_ hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init str_to_trans_id_ hashtable");
  }

  if (ret == OB_SUCCESS
      && OB_SUCCESS != (ret = str_to_multi_version_row_flag_.create(
      cal_next_prime(MULTI_VERSION_ROW_FLAG_NUM * 2), ObModIds::OB_HASH_BUCKET))) {
    STORAGE_LOG(WARN, "out of memory");
  } else if (OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("N"), 0)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("C"), 8)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("L"), 32)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("CL"), 40)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("F"), 1)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("CF"), 9)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("LF"), 33)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("CLF"), 41)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("U"), 2)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("LU"), 34)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("LG"), 48)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("FU"), 3)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("ULF"), 35)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("SCF"), 13)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("SC"), 12)
      || OB_SUCCESS != str_to_multi_version_row_flag_.set_refactored(ObString::make_string("SF"), 5)
      ) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "multi version row flag hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init multi version row flag hashtable");
  }

  // for sstable row
  if (OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("table_type"),
                 ObMockIteratorBuilder::parse_bigint)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("start_scn"),
                 ObMockIteratorBuilder::parse_bigint)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("end_scn"),
                 ObMockIteratorBuilder::parse_bigint)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("max_ver"),
                 ObMockIteratorBuilder::parse_bigint)
             || OB_SUCCESS != str_to_obj_parse_func_.set_refactored(
                 ObString::make_string("upper_ver"),
                 ObMockIteratorBuilder::parse_bigint)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "obj parse func hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init obj parse func hashtable");
  }

  if (OB_SUCCESS != str_to_obj_type_.set_refactored(
                  ObString::make_string("table_type"), &BIGINT_TYPE)
              || OB_SUCCESS != str_to_obj_type_.set_refactored(
                  ObString::make_string("start_scn"), &BIGINT_TYPE)
              || OB_SUCCESS != str_to_obj_type_.set_refactored(
                  ObString::make_string("end_scn"), &BIGINT_TYPE)
              || OB_SUCCESS != str_to_obj_type_.set_refactored(
                  ObString::make_string("max_ver"), &BIGINT_TYPE)
              || OB_SUCCESS != str_to_obj_type_.set_refactored(
                  ObString::make_string("upper_ver"), &BIGINT_TYPE)) {
    ret = OB_INIT_FAIL;
    STORAGE_LOG(WARN, "obj type hashtable insert failed");
  } else {
    STORAGE_LOG(DEBUG, "init obj type hashtable");
  }

  if (OB_SUCC(ret)) {
    is_static_inited_ = true;
  }

  return ret;
}

int ObMockIteratorBuilder::init(ObIAllocator *allocator, char escape)
{
  int ret = OB_SUCCESS;
  allocator_ = allocator ? : global_default_allocator;
  escape_ = escape;

  if (is_static_inited_) {
    is_inited_ = true;
  } else if (OB_SUCCESS == (ret = static_init())) {
    is_inited_ = true;
  } else {
    STORAGE_LOG(WARN, "init static hashtable failed", K(ret));
  }

  return ret;
}

int ObMockIteratorBuilder::parse(
    const ObString &str,
    ObMockIterator &iter,
    uint16_t *col_id_array_list)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t pos = 0;
    int64_t obj_num = 0;
    ObSEArray<ObParseFunc, DEF_COL_NUM> header;
    iter.reset();
    ret = parse_header(str, pos, header, obj_num, iter);
    if (OB_ITER_END == ret) {
      ret = OB_EMPTY_RESULT;
      STORAGE_LOG(WARN, "no data input");
    } else if (OB_FAIL(ret)) {
      //OB_ASSERT(false);
      // will not happen
      STORAGE_LOG(WARN, "parse header error", K(ret));
    } else if (0 == obj_num) {
      ret = OB_EMPTY_RESULT;
      STORAGE_LOG(WARN, "no data(such as int, var) col", K(str));
    } else if (OB_FAIL(iter.set_column_cnt(obj_num))) {
      STORAGE_LOG(WARN, "set column cnt failed", K(ret), K(obj_num));
    } else {
      STORAGE_LOG(TRACE, "get header success", K(header.count()), K(obj_num));
      int idx = 0;
      while (OB_SUCC(ret)) {
        ObStoreRow *row = NULL;
        if (OB_FAIL(malloc_store_row(*allocator_, obj_num, row, FLAT_ROW_STORE))) {
          STORAGE_LOG(WARN, "failed to malloc store row", K(ret));
        } else  {
          // set default value
          row->flag_.set_flag(ObDmlFlag::DF_INSERT);
          for (int64_t i = 0; i < obj_num; ++i) {
            row->row_val_.cells_[i].set_nop_value();
          }
          // parse one row
          ret = parse_row(str, pos, header, col_id_array_list, *row);
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            STORAGE_LOG(INFO, "parse successfully");
            break;
          } else if (OB_FAIL(ret)) {
            STORAGE_LOG(WARN, "failed to get row", K(ret), K(pos));
          } else {
            iter.add_row(row);
          }
        }

      } // end of while
    } // end of else
  } // end of is_inited_'s else
  return ret;
}

int ObMockIteratorBuilder::parse_header(const ObString &str,
                                        int64_t &pos,
                                        ObIArray<ObParseFunc> &header,
                                        int64_t &obj_num,
                                        ObMockIterator &iter)
{
  OB_ASSERT(is_inited_);

  int ret = OB_SUCCESS;
  int64_t ext = NOT_EXT;
  obj_num = 0;
  while (OB_SUCCESS == ret && pos < str.length()) {
    char buf[MAX_DATA_LENGTH];
    ObString word;
    word.assign_buffer(buf, MAX_DATA_LENGTH);
    ret = get_next_word(str, pos, word, ext);
    if (OB_FAIL(ret)) {
      // will be OB_ITER_END only
      STORAGE_LOG(WARN, "fail", K(ret), K(str), K(pos), K(word), K(obj_num));
    } else if (obj_num >= OB_ROW_MAX_COLUMNS_COUNT) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "too many columns", K(ret), K(obj_num));
    } else if (EXT_END == ext) {
      // header parse end
      break;
    } else {
      ObParseFunc fp = NULL;
      ObObjMeta *type = NULL;
      // tolower, as case must be ignore in following hash get function
      for (int64_t i = 0; i < word.length(); i++) {
        /*
        if (word.ptr()[i] >= 'A' && word.ptr()[i] <= 'Z') {
          word.ptr()[i] = static_cast<char>(word.ptr()[i] - 'A' + 'a');
        }
        */
        word.ptr()[i] = static_cast<char>(tolower(word.ptr()[i]));
      }
      //STORAGE_LOG(WARN, "chaser debug", K(word), K(ret));
      // match data col, such as var, int, num...
      if (OB_SUCCESS == str_to_obj_parse_func_.get_refactored(word, fp) && NULL != fp) {
        if (OB_SUCCESS != str_to_obj_type_.get_refactored(word, type)) {
          STORAGE_LOG(WARN, "get obj type from hashmap failed", K(ret));
        } else if (NULL == type) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "type is null", K(ret));
        } else {
          iter.set_column_type(obj_num, *type);
          ret = header.push_back(fp);
          ++obj_num;
        }
        // match row info, such as dml, flag and from_base
      } else if (OB_SUCCESS == str_to_info_parse_func_.get_refactored(word, fp)
                 && NULL != fp) {
        ret = header.push_back(fp);
      } else {
        ret = OB_OBJ_TYPE_ERROR;
      }
    }
  }
  return ret;
}

int ObMockIteratorBuilder::parse_with_specified_col_ids(
    const ObString &str,
    ObMockIterator &iter,
    uint16_t *col_id_array_list/* = nullptr*/,
    int64_t *result_col_id_array/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t pos = 0;
    int64_t obj_num = 0;
    ObSEArray<ObParseFunc, DEF_COL_NUM> header;
    iter.reset();
    ret = parse_header(str, pos, header, obj_num, iter);
    if (OB_ITER_END == ret) {
      ret = OB_EMPTY_RESULT;
      STORAGE_LOG(WARN, "no data input");
    } else if (OB_FAIL(ret)) {
      //OB_ASSERT(false);
      // will not happen
      STORAGE_LOG(WARN, "parse header error", K(ret));
    } else if (0 == obj_num) {
      ret = OB_EMPTY_RESULT;
      STORAGE_LOG(WARN, "no data(such as int, var) col");
    } else if (OB_FAIL(iter.set_column_cnt(obj_num))) {
      STORAGE_LOG(WARN, "set column cnt failed", K(ret), K(obj_num));
    } else {
      STORAGE_LOG(TRACE, "get header success", K(header.count()), K(obj_num));
      bool init_col_id_flag = false;
      if (OB_NOT_NULL(col_id_array_list) && OB_NOT_NULL(result_col_id_array)) {
        init_col_id_flag = true;
      }
      int idx = 0;
      int col_id_pos = 0;
      while (OB_SUCC(ret)) {
        ObStoreRow *row = NULL;
        if (OB_FAIL(malloc_store_row(*allocator_, obj_num, row))) {
          STORAGE_LOG(WARN, "failed to malloc store row", K(ret));
        } else  {
          // set default value
          row->flag_.set_flag(ObDmlFlag::DF_INSERT);
          for (int64_t i = 0; i < obj_num; ++i) {
            row->row_val_.cells_[i].set_nop_value();
          }
          // parse one row
          ret = parse_row(str, pos, header, nullptr, *row);
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            STORAGE_LOG(INFO, "parse successfully");
            break;
          } else if (OB_FAIL(ret)) {
            STORAGE_LOG(WARN, "failed to get row", K(ret), K(pos));
          } else {
            if (init_col_id_flag) {
              row->is_sparse_row_ = true;
              row->column_ids_ = col_id_array_list + col_id_pos;
              row->row_val_.count_ = result_col_id_array[idx];
              col_id_pos += result_col_id_array[idx]; // move forward
              STORAGE_LOG(TRACE, "get row successfully", K(idx), K(*row), K(row->trans_id_));
              ++idx;
            }
            iter.add_row(row);
          }
        }

      } // end of while
    } // end of else
  } // end of is_inited_'s else
  return ret;
}

int ObMockIteratorBuilder::parse_row(const ObString &str,
                                     int64_t &pos,
                                     const ObIArray<ObParseFunc> &header,
                                     const uint16_t *col_id_array_list,
                                     ObStoreRow &row)
{
  OB_ASSERT(is_inited_);

  int ret = OB_SUCCESS;
  int64_t idx = 0;
  int64_t ext = NOT_EXT;
  int64_t col_id_idx = 0;
  // i <= header.count() so row end is included
  for (int64_t i = 0; OB_SUCC(ret) && i <= header.count(); ++i) {
    char buf[MAX_DATA_LENGTH];
    ObString word;
    ObParseFunc fp = NULL;
    word.assign_buffer(buf, MAX_DATA_LENGTH);
    if (OB_SUCCESS != (ret = get_next_word(str, pos, word, ext))) {
      // must be OB_ITER_END only
      break;
    } else if (EXT_END == ext) {
      // check col num if flag is RF_ROW_EXIST
      break;
    } else if (OB_SUCCESS != (ret = header.at(i, fp))) {
      STORAGE_LOG(WARN, "invalid array index");
    } else {
      //STORAGE_LOG(DEBUG, "parsing word", K(word));
      switch (ext) {
        case EXT_NOP:
          if (row.is_sparse_row_) { // move_forward
            ++col_id_idx;
          } else {
            row.row_val_.cells_[idx++].set_nop_value();
          }
          break;
        case EXT_NULL:
          row.row_val_.cells_[idx++].set_null();
          break;
        case EXT_MAX:
          if (ObMockIteratorBuilder::parse_int != fp && ObMockIteratorBuilder::parse_bigint != fp) {
            row.row_val_.cells_[idx++].set_max_value();
          } else {
            row.row_val_.cells_[idx++].set_int(INT64_MAX);
          }
          break;
        case EXT_MIN:
          if (ObMockIteratorBuilder::parse_int != fp && ObMockIteratorBuilder::parse_bigint != fp) {
            row.row_val_.cells_[idx++].set_min_value();
          } else {
            row.row_val_.cells_[idx++].set_int(-INT64_MAX);
          }
          break;
        case EXT_MIN_2_TRANS:
          if (ObMockIteratorBuilder::parse_int != fp && ObMockIteratorBuilder::parse_bigint != fp) {
            row.row_val_.cells_[idx++].set_min_value();
          } else {
            row.row_val_.cells_[idx++].set_int(-(INT64_MAX - 7));
          }
          break;
        case EXT_GHOST:
          row.row_val_.cells_[idx++].set_int(ObGhostRowUtil::GHOST_NUM);
          break;
        case NOT_EXT:
          // use parse func to parse a word
          ret = (*fp)(allocator_, word, row, idx);
          if (OB_ARRAY_OUT_OF_RANGE == ret) {
            STORAGE_LOG(WARN, "data col out of range, you may missing '\\n'?");
          } else if (OB_FAIL(ret)) {
            STORAGE_LOG(WARN, "failed to parse word", K(ret), K(word));
          }
          break;
        default:
          OB_ASSERT(false);
      } // end of switch
      if (OB_SUCC(ret) && row.is_sparse_row_) {
        if (EXT_NOP != ext && parse_flag != fp && parse_multi_version_row_flag != fp && parse_trans_id != fp) {
          row.column_ids_[idx - 1] = col_id_array_list[col_id_idx++];
        }
      }
    } // end of else
  } // end of for
  if (OB_SUCC(ret) && row.is_sparse_row_) {
    row.row_val_.count_ = idx;
  }
  return ret;
}

int ObMockIteratorBuilder::get_next_word(const ObString &str,
                                         int64_t &pos,
                                         ObString &word,
                                         int64_t &ext)
{
  OB_ASSERT(is_inited_);
  int ret = OB_SUCCESS;
  int state = STATE_BEGIN;
  const char *str_ptr = str.ptr();
  ext = NOT_EXT;
  while (OB_SUCCESS == ret && state != STATE_END) {
    if (pos >= str.length()) {
      ret = OB_ITER_END;
      break;
    }

    switch (state) {
      case STATE_BEGIN:
        if (is_row_end(str_ptr[pos])) {
          word.write(&str_ptr[pos], 1);
          ext = EXT_END;
          state = STATE_END;
        } else if (is_space(str_ptr[pos])) {
          // do nothing
        } else if (is_quote(str_ptr[pos])) {
          state = STATE_WORD_WITH_QUOTE;
        } else {
          ret = write_next_char(str, pos, word);
          state = STATE_WORD_WITHOUT_QUOTE;
        }
        break;
      case STATE_WORD_WITH_QUOTE:
        if (is_quote(str_ptr[pos])) {
          state = STATE_END;
        } else {
          ret = write_next_char(str, pos, word);
        }
        break;
      case STATE_WORD_WITHOUT_QUOTE:
        if (is_row_end(str_ptr[pos])) {
          // row end need to be fallback
          --pos;
          ext = get_ext(word);
          state = STATE_END;
        } else if (is_space(str_ptr[pos])) {
          ext = get_ext(word);
          state = STATE_END;
        } else {
          ret = write_next_char(str, pos, word);
        }
        break;
      default:
        OB_ASSERT(false);
    } // end of switch
    ++pos;
  } // end of while
  //STORAGE_LOG(DEBUG, "get word", K(word), K(pos));

  return ret;
}

int ObMockIteratorBuilder::get_ext(const common::ObString &word)
{
  OB_ASSERT(word.length() > 0 && NULL != word.ptr());
  int ext = NOT_EXT;
  if (0 == word.case_compare(STR_NOP)) {
    ext = EXT_NOP;
  } else if (0 == word.case_compare(STR_NULL)) {
    ext = EXT_NULL;
  } else if (0 == word.case_compare(STR_MAX)) {
    ext = EXT_MAX;
  } else if (0 == word.case_compare(STR_MIN)) {
    ext = EXT_MIN;
  } else if (0 == word.case_compare(STR_MIN_2_TRANS)) {
    ext = EXT_MIN_2_TRANS;
  } else if (0 == word.case_compare(STR_MAGIC)) {
    ext = EXT_GHOST;
  } else {
    ext = NOT_EXT;
  }
  return ext;
}

// write one char into word buffer
int ObMockIteratorBuilder::write_next_char(const ObString &str,
                                           int64_t &pos,
                                           ObString &word)
{
  int ret = OB_SUCCESS;
  const char *str_ptr = str.ptr();
  char c = '\0';
  if (pos >= str.length() || NULL == str_ptr) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    // handle escape
    if (escape_ == str_ptr[pos]) {
      ++pos;
      if (pos < str.length()) {
        ret = handle_escape(str, pos, c);
      } else {
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
    } else {
      c = str_ptr[pos];
    }
  }

  if (OB_SUCCESS == ret && 1 != word.write(&c, 1)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    STORAGE_LOG(WARN, "out of buffer");
  }
  return ret;
}

int ObMockIteratorBuilder::handle_escape(const ObString &str, int64_t &pos, char &c)
{
  OB_ASSERT(pos < str.length());

  int ret = OB_SUCCESS;
  const char *str_ptr = str.ptr();
  switch (str_ptr[pos]) {
    case 'n':
      c = '\n';
      break;
    case 't':
      c = '\t';
      break;
    case 'r':
      c = '\r';
      break;
    default:
      c = str_ptr[pos];
      break;
  }
  return ret;
}

MockObNewRowIterator::MockObNewRowIterator()
{
  allocator_.set_label(ObModIds::OB_TRANS_CHECK);
}

MockObNewRowIterator::~MockObNewRowIterator()
{
  allocator_.free();
}

void MockObNewRowIterator::reset()
{
  allocator_.free();
  return iter_.reset();
}

bool MockObNewRowIterator::equals(const ObNewRow &r1, const ObNewRow &r2)
{
  bool bool_ret = true;
  int64_t idx = 0;
  if (!r1.cells_ || !r2.cells_) {
    bool_ret = false;
  } else if (r1.count_ != r2.count_) {
    bool_ret = false;
  } else {
    for (idx = 0; idx < r1.count_ && bool_ret; idx++) {
      bool_ret = (r1.cells_[idx] == r2.cells_[idx]);
    }
  }
  return bool_ret;
}

OB_DEF_SERIALIZE(MockObNewRowIterator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, iter_.count()))) {
    TRANS_LOG(WARN, "MockObNewRowIterator serialize error", K(ret), K(iter_.count()));
  } else {
    ObStoreRow *row = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_.count(); i++) {
      if (OB_SUCCESS != (ret = get_row(i, row))) {
        TRANS_LOG(WARN, "MockObNewRowIterator get row error", K(ret), K(i));
      } else if (OB_SUCCESS != (ret = row->serialize(buf, buf_len, pos))) {
        TRANS_LOG(WARN, "MockObNewRowIterator serialize ObStoreRow error", K(ret), K(row->row_val_));
      } else {
        TRANS_LOG(DEBUG, "MockObNewRowIterator serialize ObStoreRow success", K(row->row_val_));
      }
    }
  }

  return ret;
}

OB_DEF_DESERIALIZE(MockObNewRowIterator)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;

  reset();

  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    TRANS_LOG(WARN, "MockObNewRowIterator deserialize row size error", K(ret), K(count));
  } else {
    TRANS_LOG(INFO, "MockObNewRowIterator deserialize iter count", K(count));
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObStoreRow row;
      ObObj *ptr = (ObObj*)allocator_.alloc(sizeof(ObObj) * OB_ROW_MAX_COLUMNS_COUNT);
      if (NULL == ptr) {
        TRANS_LOG(WARN, "MockObNewRowIterator alloc ObObj error", K(ret));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        row.row_val_.assign(ptr, OB_ROW_MAX_COLUMNS_COUNT);
        if (OB_SUCCESS != (ret = row.deserialize(buf, data_len, pos))) {
          TRANS_LOG(WARN, "MockObNewRowIterator deserialize ObNewRow error", K(ret));
        } else {
          if (OB_SUCCESS != (ret = add_row(&row))) {
            TRANS_LOG(WARN, "MockObNewRowIterator deserialize add row error", K(ret), K(row.row_val_));
          } else {
            TRANS_LOG(DEBUG, "MockObNewRowIterator deserialize add row success", K(row.row_val_));
          }
        }
      }
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(MockObNewRowIterator)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  len += serialization::encoded_length_vi64(iter_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < iter_.count(); i++) {
    ObStoreRow *row = NULL;
    if (OB_SUCCESS != (ret = get_row(i, row))) {
      TRANS_LOG(WARN, "MockObNewRowIterator get_serialize size get row error", K(ret), K(i));
    } else {
      len += row->get_serialize_size();
    }
  }

  return len;
}

int ObMockDirectReadIterator::init(ObStoreRowIterator *iter,
                                   common::ObIAllocator &alloc,
                                   const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRow *row = nullptr;
  if (OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "Invalid null argument", K(ret));
  } else if (typeid(*iter) != typeid(storage::ObSSTableRowWholeScanner)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "iterator must be ObSSTableRowWholeScanner", K(ret));
  } else if (OB_FAIL(iter->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "Fail to get next row", K(ret));
    }
  } else {
    int64_t column_cnt = read_info.get_request_count();
    row_iter_ = iter;
    scanner_ = (static_cast<storage::ObSSTableRowWholeScanner *>(iter))->micro_scanner_;
    reader_ = scanner_->reader_;
    current_ = 0;
    end_ = scanner_->last_;
    row_.init(alloc, column_cnt);
    read_info_ = &read_info;
    sstable_row_.capacity_ = read_info.get_request_count();
    sstable_row_.row_val_.cells_ = reinterpret_cast<common::ObObj *>(alloc.alloc(sizeof(ObObj) * column_cnt));
    sstable_row_.row_val_.count_ = read_info.get_request_count();
    const ObIArray<ObColDesc> &cols_desc = read_info.get_columns_desc();
    for (int64_t i = 0; i < cols_desc.count(); i++) {
      sstable_row_.row_val_.cells_[i].set_type(cols_desc.at(i).col_type_.get_type());
    }
  }

  return ret;
}

int ObMockDirectReadIterator::reset_scanner()
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRow *row = nullptr;
  if (OB_ISNULL(row_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "Invalid null iter", K(ret));
  } else if (OB_FAIL(row_iter_->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "Fail to get next row", K(ret));
    }
  } else {
    current_ = 0;
    end_ = scanner_->last_;
  }

  return ret;
}

int ObMockDirectReadIterator::get_next_row(const storage::ObStoreRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;

  while (OB_SUCC(ret)) {
    if (end_of_block()) {
      scanner_->current_ = -1;
      if (OB_FAIL(reset_scanner())) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "Fail to get next row", K(ret));
        }
      }
    } else if (OB_FAIL(reader_->get_row(current_, row_))) {
      TRANS_LOG(WARN, "get row failed", K(ret));
    } else {
      current_++;
      break;
    }
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObColDesc> &cols_desc = read_info_->get_columns_desc();
    row = &sstable_row_;
    int64_t count = row_.get_column_count();
    sstable_row_.trans_id_ = row_.trans_id_;
    sstable_row_.row_type_flag_ = row_.mvcc_row_flag_;
    sstable_row_.flag_ = row_.row_flag_;
    sstable_row_.row_val_.count_ = count;
    for (int64_t i = 0; i < count; i++) {
      if (row_.storage_datums_[i].is_nop()) {
        sstable_row_.row_val_.cells_[i].set_nop_value();
      } else if (row_.storage_datums_[i].is_null()) {
        sstable_row_.row_val_.cells_[i].set_null();
      } else {
        row_.storage_datums_[i].to_obj(sstable_row_.row_val_.cells_[i], cols_desc.at(i).col_type_);
      }
    }

  }

  return ret;
}

} // namespace unitest
} // namespace oceanbase
