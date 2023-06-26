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

#ifndef OCEANBASE_UNITTEST_MOCK_ITERATOR_H_
#define OCEANBASE_UNITTEST_MOCK_ITERATOR_H_

#include "common/object/ob_object.h"
#include "storage/ob_i_store.h"
#include "common/row/ob_row_store.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_time_utility2.h"
#include "lib/string/ob_string.h"
#include "storage/ob_i_store.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/access/ob_sstable_row_whole_scanner.h"

namespace oceanbase
{
using namespace storage;
namespace common
{
class ObMockIterator : public storage::ObStoreRowIterator
{
public:
  static const int64_t DEF_ROW_NUM = 512;
public:
  ObMockIterator(bool reverse = false);
  virtual ~ObMockIterator();

  int64_t count() const { return rows_.count(); }
  // get row at idx, do not move iter
  int get_row(const int64_t idx, const storage::ObStoreRow *&row) const;
  int get_row(const int64_t idx, storage::ObStoreRow *&row) const;
  int get_row(const int64_t idx, common::ObNewRow *&row) const;
  // get row and move iter to the next
  int get_next_row(const storage::ObStoreRow *&row);
  int get_next_row(storage::ObStoreRow *&row);
  int get_next_row(common::ObNewRow *&row);

  // destory data
  void reset();
  // rewind iter
  void reset_iter();

  int add_row(storage::ObStoreRow *row);
  int from(
      const char *cstr,
      char escape = '\\',
      uint16_t* col_id_array = nullptr,
      int64_t *result_col_id_array = nullptr);
  int from(
      const common::ObString &str,
      char escape = '\\',
      uint16_t* col_id_array = nullptr,
      int64_t *result_col_id_array = nullptr);

  static bool equals(const common::ObNewRow &r1, const common::ObNewRow &r2);
  static bool equals(const storage::ObStoreRow &r1, const storage::ObStoreRow &r2,
      const bool cmp_multi_version_row_flag = false, const bool cmp_is_get_and_scan_index = false);
  static bool equals(uint16_t *col_id1, uint16_t *col_id2, const int64_t col_cnt);
  bool equals(int64_t idx, common::ObNewRow &row) const;
  bool equals(int64_t idx, storage::ObStoreRow &row) const;
  bool equals(int64_t idx, const storage::ObStoreRow &row) const;
  bool equals(common::ObNewRow &other_row) const { return equals(0, other_row); }
  bool equals(storage::ObStoreRow &other_row) const { return equals(0, other_row); }
  bool equals(const storage::ObStoreRow &other_row) const { return equals(0, other_row); }

  int set_column_cnt(const int64_t column_cnt)
  {
    int ret = common::OB_SUCCESS;
    if (column_cnt > common::OB_ROW_MAX_COLUMNS_COUNT || column_cnt < 0) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      column_cnt_ = column_cnt;
    }
    return ret;
  }

  int set_column_type(const int64_t col_idx, const common::ObObjMeta &type)
  {
    int ret = common::OB_SUCCESS;
    if (col_idx >= common::OB_ROW_MAX_COLUMNS_COUNT || col_idx < 0) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      metas_[col_idx] = type;
    }
    return ret;
  }

  template<typename T>
  bool equals(T &other_iter, const bool cmp_multi_version_row_flag = false,
      const bool cmp_is_get_and_scan_index = false)
  {
    bool bool_ret = true;
    int ret1 = common::OB_SUCCESS;
    int ret2 = common::OB_SUCCESS;
    const storage::ObStoreRow *other_row = NULL;
    storage::ObStoreRow *this_row = NULL;
    int64_t idx = 0;

    while (bool_ret) {
      ret1 = get_next_row(this_row);
      ret2 = other_iter.get_next_row(other_row);
      STORAGE_LOG(DEBUG, "compare row", K(ret1), K(*this_row), K(ret2), K(*other_row));
      if (ret1 == ret2) {
        if (common::OB_SUCCESS == ret1 && this_row && other_row) {
          bool_ret = ObMockIterator::equals(*this_row, *other_row,
              cmp_multi_version_row_flag, cmp_is_get_and_scan_index);
          STORAGE_LOG(DEBUG, "compare row", K(bool_ret), K(ret1), K(*this_row), K(ret2), K(*other_row));
          if (this_row->trans_id_ != other_row->trans_id_) {
            STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "not equal trans_id", K(*this_row), K(this_row->trans_id_),
                K(*other_row), K(other_row->trans_id_));
            bool_ret = false;
          }
        } else {
          // must be OB_ITER_END
          OB_ASSERT(common::OB_ITER_END == ret1);
          break;
        }
      } else {
        bool_ret = false;
      }
      idx++;
    }

    if (!bool_ret) {
      STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "iter is not equal",
                  K(idx), K(ret1), K(ret2),
                  "this_row", this_row ? to_cstring(*this_row): "null",
                  "other_row", other_row ? to_cstring(*other_row) : "null");
    } else {
      STORAGE_LOG(INFO, "iter is equal",
          K(idx), K(ret1), K(ret2),
          "this_row", this_row ? to_cstring(*this_row): "null",
          "other_row", other_row ? to_cstring(*other_row) : "null");
    }

    return bool_ret;
  }

  int64_t get_column_cnt() const { return column_cnt_; }
  common::ObObjMeta *get_column_type() { return metas_; }

private:
  void setup_start_cursor();
  void advance();
  bool end_of_row() const;

  common::ObSEArray<storage::ObStoreRow *, DEF_ROW_NUM> rows_;
  int64_t column_cnt_;
  common::ObObjMeta metas_[common::OB_ROW_MAX_COLUMNS_COUNT];
  int64_t cursor_;
  bool reverse_;
  transaction::ObTransID trans_id_;
  common::ObArenaAllocator allocator_;
};

template<typename T, typename ROW_TYPE>
class ObMockRowIterator : public T
{
public:
  ObMockRowIterator(bool reverse = false) : iter_(reverse) {}
  virtual ~ObMockRowIterator() {}
  int get_next_row(ROW_TYPE *&row) { return iter_.get_next_row(row); }
  int get_row(const int64_t idx, ROW_TYPE *&row) { return iter_.get_row(idx, row); }
  void reset() { return iter_.reset(); }
  void reset_iter() { return iter_.reset_iter(); }
  int from(const char *cstr, char escape = '\\') { return iter_.from(cstr, escape); }
  int from(const common::ObString &str, char escape = '\\') { return iter_.from(str, escape); }
  bool equals(ROW_TYPE &row) const { return iter_.equals(row); }
  bool equals(int64_t idx, ROW_TYPE &row) const { return iter_.equals(idx, row); }
  // compare to an iterator, rewind the iter before call this function
  // to be add
  bool equals(T &other_iter)
  {
    bool bool_ret = true;
    int ret1 = common::OB_SUCCESS;
    int ret2 = common::OB_SUCCESS;
    ROW_TYPE *other_row = NULL;
    ROW_TYPE *this_row = NULL;
    int64_t idx = 0;

    while (bool_ret) {
      ret1 = get_next_row(this_row);
      ret2 = other_iter.get_next_row(other_row);
      if (ret1 == ret2) {
        if (common::OB_SUCCESS == ret1 && this_row && other_row) {
          bool_ret = ObMockIterator::equals(*this_row, *other_row);
        } else {
          // must be OB_ITER_END
          OB_ASSERT(common::OB_ITER_END == ret1);
          break;
        }
      } else {
        bool_ret = false;
      }
      idx++;
    }

    if (!bool_ret) {
      STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "iter is not equal",
                  K(idx),
                  "this_row", to_cstring(*this_row),
                  "other_row", to_cstring(*other_row));
    }

    return bool_ret;
  }
private:
  ObMockIterator iter_;
};
typedef ObMockRowIterator<storage::ObStoreRowIterator, const storage::ObStoreRow>
ObMockStoreRowIterator;
typedef ObMockRowIterator<storage::ObQueryRowIterator, storage::ObStoreRow> ObMockQueryRowIterator;
typedef ObMockRowIterator<common::ObNewRowIterator, common::ObNewRow> ObMockNewRowIterator;

// init
// parse -> parse->header
//       -> parse->row -> parse_int(parse_varchar..)
class ObMockIteratorBuilder
{
public:
  static const int64_t MAX_DATA_LENGTH = 4096;
  static const int64_t DEF_COL_NUM     = 16;
  static const int TYPE_NUM = 6;
  static const int INFO_NUM = 2;
  static const int FLAG_NUM = 5;
  static const int DML_NUM = 6;
  static const int BASE_NUM = 2;
  static const int MULTI_VERSION_ROW_FLAG_NUM = 5;
  static const int TRANS_ID_NUM = 10;

  static const int64_t STATE_BEGIN = 0;
  static const int64_t STATE_WORD_WITH_QUOTE = 1;
  static const int64_t STATE_WORD_WITHOUT_QUOTE = 2;
  static const int64_t STATE_END = 3;

  static const int64_t NOT_EXT = 0;
  static const int64_t EXT_NOP = 1;
  static const int64_t EXT_MAX = 2;
  static const int64_t EXT_MIN = 3;
  static const int64_t EXT_NULL = 4;
  static const int64_t EXT_END = 5;
  static const int64_t EXT_MIN_2_TRANS = 6; // for second_uncommitted_trans
  static const int64_t EXT_GHOST = 7;

  static const char CHAR_ROW_END = '\n';
  static const char CHAR_QUOTE = '\'';
  static const char *STR_NOP;
  static const char *STR_NULL;
  static const char *STR_MAX;
  static const char *STR_MIN;
  static const char *STR_MIN_2_TRANS; // for second_uncommitted_trans
  static const char *STR_MAGIC;

  static common::ObObjMeta INT_TYPE;
  static common::ObObjMeta BIGINT_TYPE;
  static common::ObObjMeta VAR_TYPE;
  static common::ObObjMeta LOB_TYPE;
  static common::ObObjMeta TS_TYPE;
  static common::ObObjMeta NU_TYPE;

  typedef int (*ObParseFunc)(common::ObIAllocator *,
                             const common::ObString &,
                             storage::ObStoreRow &,
                             int64_t &);
public:
  ObMockIteratorBuilder() : is_inited_(false),
                            allocator_(NULL),
                            escape_('\\') {}
  ~ObMockIteratorBuilder() {}

  int init(common::ObIAllocator *allocator = NULL, char escape = '\\');
  int parse(
      const common::ObString &str,
      ObMockIterator &iter,
      uint16_t *col_id_array_list = nullptr);
  int parse_with_specified_col_ids(
      const ObString &str,
      ObMockIterator &iter,
      uint16_t *col_id_array_list = nullptr,
      int64_t *result_col_id_array = nullptr);
private:
  static int static_init();
  static int parse_varchar(common::ObIAllocator *allocator,
                           const common::ObString &word,
                           storage::ObStoreRow &row,
                           int64_t &idx);
  static int parse_lob(common::ObIAllocator *allocator,
                       const common::ObString &word,
                       storage::ObStoreRow &row,
                       int64_t &idx);
  /*
  static int parse_bool(common::ObIAllocator *allocator,
                        const common::ObString &word, storage::ObStoreRow &row, int64_t &idx);
                        */
  static int parse_timestamp(common::ObIAllocator *allocator,
                             const common::ObString &word,
                             storage::ObStoreRow &row,
                             int64_t &idx);
  static int parse_int(common::ObIAllocator *allocator,
                       const common::ObString &word,
                       storage::ObStoreRow &row,
                       int64_t &idx);
  static int parse_bigint(common::ObIAllocator *allocator,
                          const common::ObString &word,
                          storage::ObStoreRow &row,
                          int64_t &idx);
  static int parse_number(common::ObIAllocator *allocator,
                          const common::ObString &word,
                          storage::ObStoreRow &row,
                          int64_t &idx);
  static int parse_dml(common::ObIAllocator *allocator,
                       const common::ObString &word,
                       storage::ObStoreRow &row,
                       int64_t &idx);
  static int parse_first_dml(common::ObIAllocator *allocator,
                             const common::ObString &word,
                             storage::ObStoreRow &row,
                             int64_t &idx);
  static int parse_flag(common::ObIAllocator *allocator,
                        const common::ObString &word,
                        storage::ObStoreRow &row,
                        int64_t &idx);
  static int parse_base(common::ObIAllocator *allocator,
                        const common::ObString &word,
                        storage::ObStoreRow &row,
                        int64_t &idx);
  static int parse_multi_version_row_flag(common::ObIAllocator *allocator,
                                          const common::ObString &word,
                                          storage::ObStoreRow &row,
                                          int64_t &idx);
  static int parse_is_get(common::ObIAllocator *allocator,
                          const common::ObString &word,
                          storage::ObStoreRow &row,
                          int64_t &idx);
  static int parse_scan_index(common::ObIAllocator *allocator,
                              const common::ObString &word,
                              storage::ObStoreRow &row,
                              int64_t &idx);
  static int parse_trans_id(common::ObIAllocator *allocator,
                              const common::ObString &word,
                              storage::ObStoreRow &row,
                              int64_t &idx);

  int parse_header(const common::ObString &str,
                   int64_t &pos,
                   common::ObIArray<ObParseFunc> &header,
                   int64_t &obj_num,
                   ObMockIterator &iter);
  int parse_row(const common::ObString &str,
                int64_t &pos,
                const common::ObIArray<ObParseFunc> &header,
                const uint16_t *col_id_array_list,
                storage::ObStoreRow &row);
  int get_next_word(const common::ObString &str,
                    int64_t &pos,
                    common::ObString &word,
                    int64_t &ext);
  int write_next_char(const common::ObString &str,
                      int64_t &pos,
                      common::ObString &word);
  int handle_escape(const common::ObString &str, int64_t &pos, char &c);
  // return NOT_EXT, EXT_NOP, EXT_MIN, EXT_MAX
  int get_ext(const common::ObString &word);

  inline bool is_space(char c) { return isspace(c); }
  inline bool is_row_end(char c) { return (CHAR_ROW_END == c); }
  inline bool is_escape(char c) { return (escape_ == c); }
  inline bool is_quote(char c) { return (CHAR_QUOTE == c); }

  //inline bool is_row_end(const common::ObString &word);
public:
  static transaction::ObTransID trans_id_list_[TRANS_ID_NUM];
private:
  static bool is_static_inited_;
  // hash ObString to obj parse func , such as parse_int ...
  static common::hash::ObHashMap<common::ObString, ObParseFunc> str_to_obj_parse_func_;
  // hash ObString to row info parse func , such as parse_dml ...
  static common::hash::ObHashMap<common::ObString, ObParseFunc> str_to_info_parse_func_;
  static common::hash::ObHashMap<common::ObString, common::ObObjMeta*> str_to_obj_type_;
  static common::hash::ObHashMap<common::ObString, int64_t> str_to_flag_;
  static common::hash::ObHashMap<common::ObString, blocksstable::ObDmlFlag> str_to_dml_;
  static common::hash::ObHashMap<common::ObString, bool> str_to_base_;
  static common::hash::ObHashMap<common::ObString, bool> str_to_is_get_;
  static common::hash::ObHashMap<common::ObString, uint8_t> str_to_multi_version_row_flag_;
  static common::hash::ObHashMap<ObString, transaction::ObTransID> str_to_trans_id_;

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;
  char escape_;
};

class MockObNewRowIterator: public ObNewRowIterator
{
  OB_UNIS_VERSION(1);
public:
  MockObNewRowIterator();
  ~MockObNewRowIterator();
  static bool equals(const common::ObNewRow &r1, const common::ObNewRow &r2);
  void reset();
  int from(const char *cstr, char escape = '\\') { return iter_.from(cstr, escape); }
  int from(const common::ObString &str, char escape = '\\') { return iter_.from(str, escape); }
  int get_next_row(ObNewRow *&row) { return iter_.get_next_row(row); }
  int get_row(const int64_t idex, storage::ObStoreRow *&row) const { return iter_.get_row(idex, row); }
  int64_t count() const { return iter_.count(); }
  int add_row(storage::ObStoreRow *row) { return iter_.add_row(row); }

private:
  ObMockIterator iter_;
  common::PageArena<char> allocator_;
};

class ObMockDirectReadIterator : public storage::ObStoreRowIterator
{
public:
  ObMockDirectReadIterator()
      : current_(-1),
      end_(-1),
      row_iter_(nullptr),
      scanner_(nullptr),
      reader_(nullptr)
  {}
  virtual ~ObMockDirectReadIterator() {};
  int get_next_row(const storage::ObStoreRow *&row);
  int init(ObStoreRowIterator *iter,
           common::ObIAllocator &alloc,
           const ObITableReadInfo &read_info);
  int reset_scanner();
  bool end_of_block() {
    return current_ == -1 ||
        current_ > end_;
  }

public:
  int64_t current_;
  int64_t end_;
  ObStoreRowIterator *row_iter_;
  ObIMicroBlockRowScanner *scanner_;
  ObIMicroBlockReader *reader_;
  ObDatumRow row_;
  const ObITableReadInfo *read_info_;
  ObStoreRow sstable_row_;
};

} // namespace unittest
} // namespace oceanbase
#endif // OCEANBASE_UNITTEST_MOCK_ITERATOR_H_
