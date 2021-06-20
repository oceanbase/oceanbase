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

#ifndef OCEANBASE_SQL_RESOLVER_EXPR_RAW_EXPR_
#define OCEANBASE_SQL_RESOLVER_EXPR_RAW_EXPR_
#include "share/ob_define.h"
#include "lib/container/ob_bit_set.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/list/ob_obj_store.h"
#include "lib/rc/context.h"
#include "common/ob_range.h"
#include "common/ob_accuracy.h"
#include "sql/resolver/expr/ob_expr.h"
#include "sql/resolver/expr/ob_const_expr.h"
#include "sql/resolver/expr/ob_var_expr.h"
#include "sql/resolver/expr/ob_op_expr.h"
#include "sql/resolver/expr/ob_column_ref_expr.h"
#include "sql/resolver/expr/ob_case_op_expr.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/expr/ob_expr_info_flag.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/ob_worker.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
namespace oceanbase {
namespace share {
namespace schema {
class ObSchemaGetterGuard;
}
}  // namespace share
namespace sql {
class ObStmt;
class ObSQLSessionInfo;
class ObExprOperator;
class ObRawExprFactory;
class ObSelectStmt;
extern ObRawExpr* USELESS_POINTER;

// ObSqlBitSet is a simple bitset, in order to avoid memory explosure
// ObBitSet is too large just for a simple bitset
const static int64_t DEFAULT_SQL_BITSET_SIZE = 32;
template <int64_t N = DEFAULT_SQL_BITSET_SIZE, typename FlagType = int64_t, bool auto_free = false>
class ObSqlBitSet {
public:
  typedef uint32_t BitSetWord;

  ObSqlBitSet() : block_allocator_(NULL), bit_set_word_array_(NULL), desc_()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(init_block_allocator())) {
      SQL_RESV_LOG(WARN, "failed to init block allocator", K(ret));
    } else if (OB_ISNULL(block_allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret));
    } else {
      int64_t words_size = sizeof(BitSetWord) * MAX_BITSETWORD;
      if (OB_ISNULL(bit_set_word_array_ = (BitSetWord*)block_allocator_->alloc(words_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(WARN, "failed to alloc memory", K(ret));
      } else {
        MEMSET(bit_set_word_array_, 0, words_size);
        desc_.cap_ = static_cast<int16_t>(MAX_BITSETWORD);
        desc_.len_ = 0;
        desc_.inited_ = true;
      }
    }
  }

  ObSqlBitSet(const ObSqlBitSet<N, FlagType, auto_free>& other)
      : block_allocator_(NULL), bit_set_word_array_(NULL), desc_()
  {
    int ret = OB_SUCCESS;
    desc_.inited_ = false;
    if (!other.is_valid()) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not intied", K(ret));
    } else if (OB_FAIL(init_block_allocator())) {
      SQL_RESV_LOG(WARN, "failed to init block allocator", K(ret));
    } else if (OB_ISNULL(block_allocator_)) {
      SQL_RESV_LOG(WARN, "block_allocator_ is null", K(ret));
    } else {
      int64_t cap = other.bitset_word_count() * 2;
      if (cap <= 0) {
        cap = MAX_BITSETWORD;
      }
      int64_t words_size = sizeof(BitSetWord) * cap;
      if (OB_ISNULL(bit_set_word_array_ = (BitSetWord*)block_allocator_->alloc(words_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(WARN, "failed to alloc memory", K(ret));
      } else {
        MEMSET(bit_set_word_array_, 0, words_size);
        for (int64_t i = 0; i < other.bitset_word_count(); i++) {
          bit_set_word_array_[i] = other.get_bitset_word(i);
        }
        desc_.len_ = static_cast<int16_t>(other.bitset_word_count());
        desc_.cap_ = static_cast<int16_t>(cap);
        desc_.inited_ = true;
      }
    }
  }
  explicit ObSqlBitSet(const int64_t bit_size) : block_allocator_(NULL), bit_set_word_array_(NULL), desc_()
  {
    int ret = OB_SUCCESS;
    if (bit_size < 0) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret));
    } else if (OB_FAIL(init_block_allocator())) {
      SQL_RESV_LOG(WARN, "failed to init block allocator", K(ret));
    } else {
      int64_t bitset_word_cnt = (bit_size <= N ? MAX_BITSETWORD : ((bit_size - 1) / PER_BITSETWORD_BITS + 1));
      int64_t words_size = sizeof(BitSetWord) * bitset_word_cnt;
      if (OB_ISNULL(bit_set_word_array_ = (BitSetWord*)block_allocator_->alloc(words_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(WARN, "failed to alloc memory", K(ret));
      } else {
        MEMSET(bit_set_word_array_, 0, words_size);
        desc_.len_ = 0;
        desc_.cap_ = static_cast<int16_t>(bitset_word_cnt);
        desc_.inited_ = true;
      }
    }
  }

  ~ObSqlBitSet()
  {
    destroy();
  }

  void reset()
  {
    reuse();
  }

  void reuse()
  {
    if (is_valid()) {
      if (NULL != bit_set_word_array_ && desc_.len_ > 0) {
        MEMSET(bit_set_word_array_, 0, desc_.len_ * sizeof(BitSetWord));
      }
      desc_.len_ = 0;
    }
  }

  void destroy()
  {
    if (is_valid()) {
      if (auto_free) {
        // auto_free = true, context allocator is used,
        // free memory is just useless
        // do nothing
      } else {
        // free memory
        if (NULL != block_allocator_ && NULL != bit_set_word_array_) {
          block_allocator_->free(bit_set_word_array_);
        }
        if (NULL != block_allocator_) {
          block_allocator_->reset();
          ob_free(block_allocator_);
        }
        block_allocator_ = NULL;
        bit_set_word_array_ = NULL;
      }
      desc_.len_ = 0;
      desc_.cap_ = 0;
      desc_.inited_ = false;
    }
  }

  int64_t bitset_word_count() const
  {
    return static_cast<int64_t>(desc_.len_);
  }
  int64_t bit_count() const
  {
    return static_cast<int64_t>(desc_.len_) * PER_BITSETWORD_BITS;
  }
  bool is_empty() const
  {
    return 0 == num_members();
  }
  bool is_valid() const
  {
    return desc_.inited_;
  }
  void clear_all()
  {
    if (!is_valid()) {
      // do nothing
    } else {
      MEMSET(bit_set_word_array_, 0, desc_.len_ * sizeof(BitSetWord));
    }
  }
  BitSetWord get_bitset_word(int64_t index) const
  {
    BitSetWord word = 0;
    if (!is_valid()) {
      SQL_RESV_LOG(INFO, "not inited");
    } else if (index < 0 || index >= desc_.len_) {
      SQL_RESV_LOG(INFO, "bitmap word index exceeds scope", K(index), K(desc_.len_));
    } else {
      word = bit_set_word_array_[index];
    }
    return word;
  }

  int64_t num_members() const
  {
    int64_t num = 0;
    BitSetWord word = 0;
    if (!is_valid()) {
      // do nothing
    } else {
      for (int64_t i = 0; i < desc_.len_; i++) {
        word = bit_set_word_array_[i];
        if (0 == word) {
          // do nothing
        } else {
          word = (word & UINT32_C(0x55555555)) + ((word >> 1) & UINT32_C(0x55555555));
          word = (word & UINT32_C(0x33333333)) + ((word >> 2) & UINT32_C(0x33333333));
          word = (word & UINT32_C(0x0f0f0f0f)) + ((word >> 4) & UINT32_C(0x0f0f0f0f));
          word = (word & UINT32_C(0x00ff00ff)) + ((word >> 8) & UINT32_C(0x00ff00ff));
          word = (word & UINT32_C(0x0000ffff)) + ((word >> 16) & UINT32_C(0x0000ffff));
          num += (int64_t)word;
        }
      }
    }
    return num;
  }
  bool has_member(int64_t index) const
  {
    bool bool_ret = false;
    if (!is_valid()) {
      // do nothing
    } else if (OB_UNLIKELY(index < 0)) {
      SQL_RESV_LOG(INFO, "negative bitmap member not allowed");
    } else {
      int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
      if (pos >= desc_.len_) {
        // dp nothing
      } else {
        bool_ret = (bit_set_word_array_[pos] & ((BitSetWord)1 << (index & PER_BITSETWORD_MASK))) != 0;
      }
    }
    return bool_ret;
  }
  int add_member(int64_t index)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else if (OB_UNLIKELY(index < 0)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "negative bitmap member not allowed", K(ret), K(index));
    } else {
      int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
      if (OB_UNLIKELY(pos >= desc_.cap_)) {
        int64_t new_word_cnt = pos * 2;
        if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
          SQL_RESV_LOG(WARN, "failed to alloc new buf", K(ret));
        }
      }
      if (pos >= desc_.len_) {
        desc_.len_ = static_cast<int16_t>(pos) + 1;
      }
      if (OB_SUCC(ret)) {
        bit_set_word_array_[pos] |= ((BitSetWord)1 << (index & PER_BITSETWORD_MASK));
      }
    }
    return ret;
  }
  int del_member(int64_t index)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = common::OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else if (OB_UNLIKELY(index < 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "negative bitmap member not allowed", K(ret), K(index));
    } else {
      int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
      if (OB_UNLIKELY(pos >= desc_.len_)) {
        // do nothing
      } else {
        bit_set_word_array_[pos] &= ~((BitSetWord)1 << (index & PER_BITSETWORD_MASK));
      }
    }
    return ret;
  }

  int do_mask(int64_t begin_index, int64_t end_index)
  {
    int ret = OB_SUCCESS;
    int64_t max_bit_count = static_cast<int64_t>(desc_.len_) * PER_BITSETWORD_BITS;
    if (!is_valid()) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else if (begin_index < 0 || begin_index >= max_bit_count || end_index < 0 || end_index >= max_bit_count) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(begin_index), K(end_index), K(max_bit_count));
    } else {
      int64_t begin_word = begin_index / PER_BITSETWORD_BITS;
      int64_t end_word = end_index / PER_BITSETWORD_BITS;
      int64_t begin_pos = begin_index % PER_BITSETWORD_BITS;
      int64_t end_pos = end_index % PER_BITSETWORD_BITS;

      for (int64_t i = 0; i < begin_word; i++) {
        bit_set_word_array_[i] = 0;
      }
      for (int64_t i = desc_.len_ - 1; i > end_word; i--) {
        bit_set_word_array_[i] = 0;
      }
      for (int64_t i = 0; i < begin_pos; i++) {
        bit_set_word_array_[begin_word] &= ~((BitSetWord)1 << i);
      }
      for (int64_t i = 1 + end_pos; i < PER_BITSETWORD_BITS; i++) {
        bit_set_word_array_[end_word] &= ~((BitSetWord)1 << i);
      }
    }
    return ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  int add_members(const ObSqlBitSet<M, FlagType2, auto_free2>& other)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = common::OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else if (OB_ISNULL(bit_set_word_array_)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(bit_set_word_array_));
    } else {
      int64_t this_count = desc_.len_;
      int64_t that_count = other.bitset_word_count();

      if (this_count < that_count) {
        if (desc_.cap_ >= that_count) {
          // do nothing
        } else {
          int64_t new_word_cnt = that_count * 2;
          if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
            SQL_RESV_LOG(WARN, "failed to alloc new buffer", K(ret));
          }
        }
      }
      if (that_count >= desc_.len_) {
        desc_.len_ = static_cast<int16_t>(that_count);
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        for (int64_t i = 0; i < that_count; i++) {
          bit_set_word_array_[i] |= other.get_bitset_word(i);
        }
      }
    }
    return ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  int add_members2(const ObSqlBitSet<M, FlagType2, auto_free2>& other)
  {
    return add_members(other);
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  int del_members(const ObSqlBitSet<M, FlagType2, auto_free2>& other)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = common::OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else {
      for (int64_t i = 0; i < desc_.len_; i++) {
        bit_set_word_array_[i] &= ~(other.get_bitset_word(i));
      }
    }
    return ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  int del_members2(const ObSqlBitSet<M, FlagType2, auto_free2>& other)
  {
    return del_members(other);
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool is_subset(const ObSqlBitSet<M, FlagType2, auto_free2>& other) const
  {
    bool bool_ret = true;
    if (!is_valid()) {
      // do nothing
    } else if (is_empty()) {
      bool_ret = true;
    } else if (other.is_empty()) {
      bool_ret = false;
    } else if (desc_.len_ > other.bitset_word_count()) {
      bool_ret = false;
    } else {
      for (int64_t i = 0; bool_ret && i < desc_.len_; i++) {
        if ((bit_set_word_array_[i] & (~(other.get_bitset_word(i)))) != 0) {
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool is_subset2(const ObSqlBitSet<M, FlagType2, auto_free2>& other) const
  {
    return is_subset(other);
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool is_superset(const ObSqlBitSet<M, FlagType2, auto_free2>& other) const
  {
    bool bool_ret = false;
    if (!is_valid()) {
      // do nothing
    } else if (is_empty()) {
      bool_ret = false;
    } else if (other.is_empty()) {
      bool_ret = true;
    } else if (desc_.len_ < other.bitset_word_count()) {
      bool_ret = false;
    } else {
      bool_ret = true;
      for (int64_t i = 0; bool_ret && i < other.bitset_word_count(); i++) {
        if (((~bit_set_word_array_[i]) & other.get_bitset_word(i)) != 0) {
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool is_superset2(const ObSqlBitSet<M, FlagType2, auto_free2>& other) const
  {
    return is_superset(other);
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool overlap(const ObSqlBitSet<M, FlagType2, auto_free2>& other) const
  {
    bool bool_ret = false;
    if (!is_valid()) {
      // do nothing
    } else {
      int64_t min_bitset_word_count = std::min(static_cast<int64_t>(desc_.len_), other.bitset_word_count());
      for (int64_t i = 0; !bool_ret && i < min_bitset_word_count; i++) {
        if ((bit_set_word_array_[i] & (other.get_bitset_word(i))) != 0) {
          bool_ret = true;
        }
      }
    }
    return bool_ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool overlap2(const ObSqlBitSet<M, FlagType2, auto_free2>& other) const
  {
    return overlap(other);
  }

  int to_array(ObIArray<int64_t>& arr) const
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else {
      arr.reuse();
      int64_t num = num_members();
      int64_t count = 0;
      int64_t max_bit_count = desc_.len_ * PER_BITSETWORD_BITS;
      for (int64_t i = 0; OB_SUCC(ret) && count < num && i < max_bit_count; i++) {
        if (has_member(i)) {
          if (OB_FAIL(arr.push_back(i))) {
            SQL_RESV_LOG(WARN, "failed to push back element", K(ret));
          } else {
            count++;
          }
        }
      }  // for end
    }
    return ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool equal(const ObSqlBitSet<M, FlagType2, auto_free2>& other) const
  {
    bool bool_ret = true;
    if (!is_valid() || !other.is_valid()) {
      bool_ret = false;
    } else if (bitset_word_count() != other.bitset_word_count()) {
      bool_ret = false;
    } else {
      for (int64_t i = 0; bool_ret && i < bitset_word_count(); i++) {
        if (get_bitset_word(i) != other.get_bitset_word(i)) {
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }

  bool operator==(const ObSqlBitSet<N, FlagType, auto_free>& other) const
  {
    return this->equal(other);
  }

  bool operator!=(const ObSqlBitSet<N, FlagType, auto_free>& other) const
  {
    return !(*this == other);
  }

  template <int64_t M, typename FlagType2, bool auto_free2, int64_t L, typename FlagType3, bool auto_free3>
  int intersect(const ObSqlBitSet<M, FlagType2, auto_free2>& left, const ObSqlBitSet<L, FlagType3, auto_free3>& right)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid() || !left.is_valid() || !right.is_valid()) {
      ret = common::OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else if (OB_ISNULL(bit_set_word_array_)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(bit_set_word_array_));
    } else {
      reset();
      int64_t that_count =
          left.bitset_word_count() < right.bitset_word_count() ? left.bitset_word_count() : right.bitset_word_count();
      if (desc_.cap_ < that_count) {
        int64_t new_word_cnt = that_count * 2;
        if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
          SQL_RESV_LOG(WARN, "failed to alloc new buffer", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        for (int64_t i = 0; i < that_count; i++) {
          bit_set_word_array_[i] = left.get_bitset_word(i) & right.get_bitset_word(i);
        }
        desc_.len_ = static_cast<int16_t>(that_count);
      }
    }
    return ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2, int64_t L, typename FlagType3, bool auto_free3>
  int except(const ObSqlBitSet<M, FlagType2, auto_free2>& left, const ObSqlBitSet<L, FlagType3, auto_free3>& right)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid() || !left.is_valid() || !right.is_valid()) {
      ret = common::OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else if (OB_ISNULL(bit_set_word_array_)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(bit_set_word_array_));
    } else {
      reset();
      int64_t that_count = left.bitset_word_count();
      if (desc_.cap_ < that_count) {
        int64_t new_word_cnt = that_count * 2;
        if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
          SQL_RESV_LOG(WARN, "failed to alloc new buffer", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        int64_t i = 0;
        for (; i < left.bitset_word_count() && i < right.bitset_word_count(); i++) {
          bit_set_word_array_[i] = left.get_bitset_word(i) ^ (left.get_bitset_word(i) & right.get_bitset_word(i));
        }
        for (; i < left.bitset_word_count(); i++) {
          bit_set_word_array_[i] = left.get_bitset_word(i);
        }
        desc_.len_ = static_cast<int16_t>(that_count);
      }
    }
    return ret;
  }

  static int databuff_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const int64_t& obj)
  {
    return common::databuff_print_obj(buf, buf_len, pos, obj);
  }

  static int databuff_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const ObExprInfoFlag& flag)
  {
    return common::databuff_printf(buf, buf_len, pos, "\"%s\"", get_expr_info_flag_str(flag));
  }

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    J_ARRAY_START();
    int64_t num = num_members();
    int64_t count = 0;
    for (int64_t i = 0; i < bit_count(); i++) {
      if (has_member(i)) {
        if (count != 0 && count < num) {
          J_COMMA();
        }
        FlagType flag = static_cast<FlagType>(i);
        if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, flag))) {
          SQL_RESV_LOG(WARN, "databuff print obj failed", K(ret));
        }
        ++count;
      }
    }
    J_ARRAY_END();
    return pos;
  }

  ObSqlBitSet<N, FlagType, auto_free>& operator=(const ObSqlBitSet<N, FlagType, auto_free>& other)
  {
    int ret = OB_SUCCESS;
    if (!other.is_valid() || !is_valid()) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "invalid bitset", K(is_valid()), K(other.is_valid()));
    } else if (&other == this) {
      // do nothing
    } else {
      if (other.bitset_word_count() > desc_.cap_) {
        int64_t new_word_cnt = other.bitset_word_count() * 2;
        if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
          SQL_RESV_LOG(WARN, "failed to alloc new buf", K(ret));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < other.bitset_word_count(); i++) {
        bit_set_word_array_[i] = other.get_bitset_word(i);
      }
      desc_.len_ = static_cast<int16_t>(other.bitset_word_count());
    }
    if (OB_FAIL(ret)) {
      // error happened, set inited flag to be false
      desc_.inited_ = false;
    }
    return *this;
  }

private:
  int alloc_new_buf(int64_t word_cnt)
  {
    int ret = OB_SUCCESS;
    int64_t words_size = sizeof(BitSetWord) * word_cnt;
    BitSetWord* new_buf = NULL;
    ObIAllocator* allocator = get_block_allocator();
    if (!is_valid()) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else if (OB_ISNULL(allocator)) {
      SQL_RESV_LOG(WARN, "invalid allocator", K(ret));
    } else if (OB_ISNULL(new_buf = (BitSetWord*)allocator->alloc(words_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(WARN, "failed to alloc memory", K(ret));
    } else {
      MEMSET(new_buf, 0, words_size);
      MEMCPY(new_buf, bit_set_word_array_, desc_.len_ * sizeof(BitSetWord));
      // set word array to new buffer
      allocator->free(bit_set_word_array_);
      bit_set_word_array_ = new_buf;
      desc_.cap_ = static_cast<int16_t>(word_cnt);
    }
    return ret;
  }

  ObIAllocator* get_block_allocator()
  {
    ObIAllocator* ret_alloc = NULL;
    if (!is_valid()) {
      // do nothing
    } else {
      ret_alloc = block_allocator_;
    }
    return ret_alloc;
  }

  int init_block_allocator()
  {
    int ret = OB_SUCCESS;
    if (is_valid()) {
      ret = OB_INIT_TWICE;
      SQL_RESV_LOG(WARN, "init twice", K(ret));
    } else if (auto_free) {
      block_allocator_ = &CURRENT_CONTEXT.get_arena_allocator();
    } else {
      void* alloc_buf = NULL;
      if (OB_ISNULL(alloc_buf = ob_malloc(sizeof(ObArenaAllocator), ObModIds::OB_BIT_SET))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        block_allocator_ = new (alloc_buf) ObArenaAllocator(ObModIds::OB_BIT_SET);
      }
    }
    return ret;
  }

private:
  static const int64_t PER_BITSETWORD_BITS = 32;
  static const int64_t PER_BITSETWORD_MOD_BITS = 5;
  static const int64_t PER_BITSETWORD_MASK = PER_BITSETWORD_BITS - 1;
  static const int64_t MAX_BITSETWORD = ((N <= 0 ? DEFAULT_SQL_BITSET_SIZE : N) - 1) / PER_BITSETWORD_BITS + 1;

  struct SqlBitSetDesc {
    int16_t len_;
    int16_t cap_;
    bool inited_;

    SqlBitSetDesc() : len_(0), cap_(0), inited_(false)
    {}
  };

private:
  ObIAllocator* block_allocator_;
  BitSetWord* bit_set_word_array_;
  SqlBitSetDesc desc_;
};

typedef ObSqlBitSet<96, ObExprInfoFlag, true> ObExprInfo;

enum AccessNameType {
  UNKNOWN = -1,
  SYS_FUNC,
  DLL_UDF,
  PL_UDF,
  PL_VAR,
  DB_NS,
  PKG_NS,
  REC_ELEM,
  TYPE_METHOD,
  CURSOR_ATTR,
  UDT_NS,
  LOCAL_TYPE,
  PKG_TYPE,
};

class ObRawExpr;
class ObSysFunRawExpr;
class ObObjAccessIdent {
public:
  ObObjAccessIdent()
      : type_(UNKNOWN), access_name_(), access_index_(common::OB_INVALID_INDEX), sys_func_expr_(NULL), params_()
  {}
  ObObjAccessIdent(const common::ObString& name, int64_t index = common::OB_INVALID_INDEX)
      : type_(UNKNOWN), access_name_(name), access_index_(index), sys_func_expr_(NULL), params_()
  {}
  virtual ~ObObjAccessIdent()
  {}

  int assign(const ObObjAccessIdent& other)
  {
    type_ = other.type_;
    access_name_ = other.access_name_;
    access_index_ = other.access_index_;
    sys_func_expr_ = other.sys_func_expr_;
    return params_.assign(other.params_);
  }
  ObObjAccessIdent& operator=(const ObObjAccessIdent& other)
  {
    assign(other);
    return *this;
  }

public:
  inline void set_type(AccessNameType type)
  {
    type_ = type;
  }
  inline AccessNameType get_type()
  {
    return type_;
  }
  inline void set_sys_func()
  {
    type_ = SYS_FUNC;
  }
  inline void set_dll_udf()
  {
    type_ = DLL_UDF;
  }
  inline void set_db_ns()
  {
    type_ = DB_NS;
  }
  inline void set_pkg_ns()
  {
    type_ = PKG_NS;
  }
  inline void set_rec_elem()
  {
    type_ = REC_ELEM;
  }
  inline void set_type_method()
  {
    type_ = TYPE_METHOD;
  }
  inline bool is_sys_func() const
  {
    return SYS_FUNC == type_;
  }
  inline bool is_dll_udf() const
  {
    return DLL_UDF == type_;
  }
  inline bool is_db_ns() const
  {
    return DB_NS == type_;
  }
  inline bool is_rec_elem() const
  {
    return REC_ELEM == type_;
  }
  inline bool is_type_method() const
  {
    return TYPE_METHOD == type_;
  }
  inline bool is_local_type() const
  {
    return LOCAL_TYPE == type_;
  }
  inline bool is_type() const
  {
    return is_local_type();
  }

  int extract_params(int64_t level, common::ObIArray<ObRawExpr*>& params) const;
  int replace_params(ObRawExpr* from, ObRawExpr* to);

  TO_STRING_KV(K_(access_name), K_(access_index), K_(type), K_(params));

  AccessNameType type_;
  common::ObString access_name_;
  int64_t access_index_;
  ObSysFunRawExpr* sys_func_expr_;
  // x/y/m/n are parameters of a.f, but the param_level of x/y is 0, and m/n is 1.
  common::ObSEArray<std::pair<ObRawExpr*, int64_t>, 4, common::ModulePageAllocator, true> params_;
};

class ObColumnRefRawExpr;
class ObQualifiedName {
public:
  ObQualifiedName()
      : database_name_(),
        tbl_name_(),
        col_name_(),
        is_star_(false),
        ref_expr_(NULL),
        parents_expr_info_(),
        parent_aggr_level_(-1),
        access_idents_(),
        is_access_root_(true)
  {}
  virtual ~ObQualifiedName()
  {}

  int assign(const ObQualifiedName& other)
  {
    database_name_ = other.database_name_;
    tbl_name_ = other.tbl_name_;
    col_name_ = other.col_name_;
    is_star_ = other.is_star_;
    ref_expr_ = other.ref_expr_;
    parents_expr_info_ = other.parents_expr_info_;
    parent_aggr_level_ = other.parent_aggr_level_;
    is_access_root_ = other.is_access_root_;
    return access_idents_.assign(other.access_idents_);
  }
  ObQualifiedName& operator=(const ObQualifiedName& other)
  {
    assign(other);
    return *this;
  }

  void format_qualified_name(common::ObNameCaseMode mode);
  inline bool is_sys_func() const
  {
    return 1 == access_idents_.count() && access_idents_.at(0).is_sys_func();
  }
  inline bool is_dll_udf() const
  {
    return false;
  }
  inline bool is_access_root() const
  {
    return is_access_root_;
  }

  int replace_access_ident_params(ObRawExpr* from, ObRawExpr* to);

  TO_STRING_KV(N_DATABASE_NAME, database_name_, N_TABLE_NAME, tbl_name_, N_COLUMN, col_name_, K_(is_star), K_(ref_expr),
      K_(parents_expr_info), K_(parent_aggr_level), K_(access_idents), K_(is_access_root));

public:
  common::ObString database_name_;
  common::ObString tbl_name_;  // used for package name for UDF
  common::ObString col_name_;  // used for function name for UDF
  bool is_star_;
  ObColumnRefRawExpr* ref_expr_;
  ObExprInfo parents_expr_info_;
  int64_t parent_aggr_level_;
  // all identifiers will be stored here, like a/f/c in a.f(x,y).c
  common::ObSEArray<ObObjAccessIdent, 4, common::ModulePageAllocator, true> access_idents_;
  bool is_access_root_;  // qualified name is recursive:
                         // a(b(c)) => b(c) => c, a(b(c)) is root.
};

// bug 6349933: for most of the cases, 8 tables should be more than enough
typedef ObSqlBitSet<8, int64_t, true> ObRelIds;
typedef ObSqlBitSet<common::OB_MAX_SUBQUERY_LAYER_NUM, int64_t, true> ObExprLevels;

/**
 * @brief The ExprCopyPolicy enum
 * Share expr: column, query, aggregation, window function,
               pseudo column, and expr with IS_SHARED_REF
 * COPY_REF_DEFAULT: shallow copy for share expr, deep copy for other expr.
 * COPY_REF_SHARE: deep copy for share expr too.
 * e.g. min(c1),
 *   default mode: return expr pointer directly.
 *   share mode: deep copy min, shallow copy c1, return the copied min expr.
 * every share expr can deep copy themselves, can not deep coy others.
 */
enum ExprCopyPolicy {
  COPY_REF_DEFAULT = 0,
  COPY_REF_SHARED = 1 << 0,
};

struct OrderItem {
  OrderItem()
  {
    reset();
  }
  explicit OrderItem(ObRawExpr* expr) : expr_(expr), order_type_(default_asc_direction())
  {}
  OrderItem(ObRawExpr* expr, ObOrderDirection order_type) : expr_(expr), order_type_(order_type)
  {}

public:
  virtual ~OrderItem()
  {}
  void reset()
  {
    expr_ = NULL;
    order_type_ = default_asc_direction();
  }
  inline bool operator==(const OrderItem& other) const
  {
    return (expr_ == other.expr_ && order_type_ == other.order_type_);
  }
  inline bool operator!=(const OrderItem& other) const
  {
    return !(*this == other);
  }
  uint64_t hash(uint64_t seed) const
  {
    if (NULL != expr_) {
      seed = common::do_hash(*expr_, seed);
    }
    seed = common::do_hash(order_type_, seed);

    return seed;
  }

  bool is_null_first() const
  {
    return NULLS_FIRST_ASC == order_type_ || NULLS_FIRST_DESC == order_type_;
  }

  bool is_ascending() const
  {
    return NULLS_FIRST_ASC == order_type_ || NULLS_LAST_ASC == order_type_;
  }

  bool is_descending() const
  {
    return NULLS_FIRST_DESC == order_type_ || NULLS_LAST_DESC == order_type_;
  }

  int deep_copy(ObRawExprFactory& expr_factory, const OrderItem& other, const uint64_t copy_types,
      bool use_new_allocator = false);

  static const char* order_type2name(const ObOrderDirection direction)
  {
    const char* name = NULL;
    switch (direction) {
      case NULLS_FIRST_ASC:
        name = N_NULLS_FIRST_ASC;
        break;
      case NULLS_LAST_ASC:
        name = N_NULLS_LAST_ASC;
        break;
      case NULLS_FIRST_DESC:
        name = N_NULLS_FIRST_DESC;
        break;
      case NULLS_LAST_DESC:
        name = N_NULLS_LAST_DESC;
        break;
      default:
        break;
    }
    return name;
  }

  TO_STRING_KV(N_EXPR, expr_, N_ASCENDING, order_type2name(order_type_));

  ObRawExpr* expr_;
  ObOrderDirection order_type_;
};

class ObQueryRefRawExpr;
struct ObExprEqualCheckContext {
  ObExprEqualCheckContext()
      : override_const_compare_(false),
        override_column_compare_(false),
        override_query_compare_(false),
        err_code_(common::OB_SUCCESS),
        param_expr_()
  {}
  virtual ~ObExprEqualCheckContext()
  {}
  struct ParamExprPair {
    ParamExprPair(int64_t param_idx, const ObRawExpr* expr) : param_idx_(param_idx), expr_(expr)
    {}
    ParamExprPair() : param_idx_(-1), expr_(NULL)
    {}
    TO_STRING_KV(K_(param_idx), K_(expr));
    int64_t param_idx_;
    const ObRawExpr* expr_;
  };
  inline int add_param_pair(int64_t param_idx, const ObRawExpr* expr)
  {
    return param_expr_.push_back(ParamExprPair(param_idx, expr));
  }

  // only compare the result type of two columns
  virtual bool compare_column(const ObColumnRefRawExpr& left, const ObColumnRefRawExpr& right);

  virtual bool compare_const(const ObConstRawExpr& left, const ObConstRawExpr& right);

  virtual bool compare_query(const ObQueryRefRawExpr& left, const ObQueryRefRawExpr& right);

  void reset()
  {
    override_const_compare_ = false;
    override_column_compare_ = false;
    override_query_compare_ = false;
    err_code_ = OB_SUCCESS;
    param_expr_.reset();
  }
  bool override_const_compare_;
  bool override_column_compare_;
  bool override_query_compare_;
  int err_code_;
  // when compare with T_QUESTIONMARK, as T_QUESTIONMARK is unkown, record this first.
  common::ObSEArray<ParamExprPair, 3, common::ModulePageAllocator, true> param_expr_;
};

struct ObExprParamCheckContext : ObExprEqualCheckContext {
  ObExprParamCheckContext() : ObExprEqualCheckContext(), context_(NULL)
  {
    override_column_compare_ = false;
    override_const_compare_ = true;
    override_query_compare_ = false;
  }
  virtual ~ObExprParamCheckContext()
  {}
  int init(const ObQueryCtx* context);
  bool compare_const(const ObConstRawExpr& left, const ObConstRawExpr& right) override;

  int get_calc_expr(const int64_t param_idx, const ObRawExpr*& expr);

  int is_pre_calc_item(const ObConstRawExpr& const_expr, bool& is_calc);

  const ObQueryCtx* context_;
};

enum ObVarType {
  INVALID_VAR = -1,
  SYS_VAR = 0,
  USER_VAR = 1,
};

struct ObVarInfo final {
  OB_UNIS_VERSION(1);

public:
  ObVarInfo() : type_(INVALID_VAR), name_()
  {}
  int deep_copy(common::ObIAllocator& allocator, ObVarInfo& var_info) const;
  bool operator==(const ObVarInfo& other) const
  {
    return (type_ == other.type_ && name_ == other.name_);
  }
  TO_STRING_KV(K_(type), K_(name));
  ObVarType type_;
  common::ObString name_;
};

class ObQueryRefRawExpr;
struct ObSubQueryInfo {
  ObSubQueryInfo()
  {
    sub_query_ = NULL;
    ref_expr_ = NULL;
  }
  TO_STRING_KV(K_(ref_expr));

  const ParseNode* sub_query_;
  ObQueryRefRawExpr* ref_expr_;
  ObExprInfo parents_expr_info_;
};
class ObAggFunRawExpr;
class ObPseudoColumnRawExpr;
class ObOpRawExpr;
class ObWinFunRawExpr;
class ObUserVarIdentRawExpr;
template <typename ExprFactoryT>
struct ObResolveContext {
  struct ObAggResolveLinkNode : public common::ObDLinkBase<ObAggResolveLinkNode> {
    ObAggResolveLinkNode() : is_win_agg_(false)
    {}
    bool is_win_agg_;
  };
  ObResolveContext(ExprFactoryT& expr_factory, const common::ObTimeZoneInfo* tz_info, const common::ObNameCaseMode mode)
      : expr_factory_(expr_factory),
        stmt_(NULL),
        connection_charset_(common::CHARSET_INVALID),
        dest_collation_(common::CS_TYPE_INVALID),
        tz_info_(tz_info),
        case_mode_(mode),
        columns_(NULL),
        sys_vars_(NULL),
        sub_query_info_(NULL),
        aggr_exprs_(NULL),
        win_exprs_(NULL),
        op_exprs_(NULL),
        is_extract_param_type_(true),
        param_list_(NULL),
        prepare_param_count_(0),
        external_param_info_(NULL),
        current_scope_(T_NONE_SCOPE),
        is_win_agg_(false),
        schema_checker_(NULL),
        session_info_(NULL),
        query_ctx_(NULL),
        is_for_pivot_(false),
        is_for_dynamic_sql_(false),
        is_for_dbms_sql_(false)
  {}

  ExprFactoryT& expr_factory_;
  ObStmt* stmt_;
  common::ObCharsetType connection_charset_;
  common::ObCollationType dest_collation_;
  const common::ObTimeZoneInfo* tz_info_;
  common::ObNameCaseMode case_mode_;
  ObExprInfo parents_expr_info_;
  common::ObIArray<ObQualifiedName>* columns_;
  common::ObIArray<ObVarInfo>* sys_vars_;
  common::ObIArray<ObSubQueryInfo>* sub_query_info_;
  common::ObIArray<ObAggFunRawExpr*>* aggr_exprs_;
  common::ObIArray<ObWinFunRawExpr*>* win_exprs_;
  common::ObIArray<ObOpRawExpr*>* op_exprs_;
  common::ObIArray<ObUserVarIdentRawExpr*>* user_var_exprs_;
  bool is_extract_param_type_;
  const ParamStore* param_list_;
  int64_t prepare_param_count_;
  common::ObIArray<std::pair<ObRawExpr*, ObConstRawExpr*>>* external_param_info_;  // for anonymous + ps
  ObStmtScope current_scope_;
  common::ObArenaAllocator local_allocator_;
  typedef common::ObDList<ObAggResolveLinkNode> ObAggResolveLink;
  ObAggResolveLink agg_resolve_link_;
  bool is_win_agg_;
  ObSchemaChecker* schema_checker_;       // we use checker to get udf function name.
  const ObSQLSessionInfo* session_info_;  // we use to get tenant id
  share::schema::ObSchemaGetterGuard* schema_guard_;
  ObQueryCtx* query_ctx_;
  bool is_for_pivot_;
  bool is_for_dynamic_sql_;
  bool is_for_dbms_sql_;
};

typedef ObResolveContext<ObRawExprFactory> ObExprResolveContext;
class ObRawExprVisitor;
struct ObHiddenColumnItem;

class ObRawExpr : virtual public jit::expr::ObExpr {
public:
  friend sql::ObExpr* ObStaticEngineExprCG::get_rt_expr(const ObRawExpr& raw_expr);
  friend sql::ObExpr* ObExprOperator::get_rt_expr(const ObRawExpr& raw_expr) const;

  explicit ObRawExpr(ObItemType expr_type = T_INVALID)
      : ObExpr(expr_type),
        magic_num_(0x13572468),
        info_(),
        rel_ids_(),
        expr_levels_(),
        inner_alloc_(NULL),
        expr_factory_(NULL),
        expr_level_(-1),
        is_explicited_reference_(false),
        ref_count_(0),
        is_for_generated_column_(false),
        rt_expr_(NULL),
        extra_(0),
        orig_expr_(NULL),
        is_calculated_(false),
        is_deterministic_(true)
  {}

  explicit ObRawExpr(common::ObIAllocator& alloc, ObItemType expr_type = T_INVALID)
      : ObExpr(alloc, expr_type),
        magic_num_(0x13572468),
        info_(),
        rel_ids_(),
        expr_levels_(),
        inner_alloc_(&alloc),
        expr_factory_(NULL),
        expr_level_(-1),
        is_explicited_reference_(false),
        ref_count_(0),
        is_for_generated_column_(false),
        rt_expr_(NULL),
        extra_(0),
        orig_expr_(NULL),
        is_calculated_(false),
        is_deterministic_(true)
  {}
  virtual ~ObRawExpr();
  int assign(const ObRawExpr& other);
  int deep_copy(
      ObRawExprFactory& expr_factory, const ObRawExpr& other, const uint64_t types, bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs) = 0;
  virtual void clear_child() = 0;

  virtual void reset();
  /// whether is the same expression.
  /// Compare the two expression tree.
  bool has_generalized_column() const;
  bool has_enum_set_column() const;
  bool has_specified_pseudocolumn() const;
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const = 0;

  bool is_generalized_column() const;
  void unset_result_flag(uint32_t result_flag);
  // void set_op_factory(ObExprOperatorFactory &factory) { op_factory_ = &factory; }
  void set_allocator(common::ObIAllocator& alloc);
  void set_expr_factory(ObRawExprFactory& factory)
  {
    expr_factory_ = &factory;
  }
  ObRawExprFactory* get_expr_factory()
  {
    return expr_factory_;
  }

  void set_expr_info(const ObExprInfo& info);
  int add_flag(int32_t flag);
  int add_flags(const ObExprInfo& flags);
  int add_child_flags(const ObExprInfo& flags);
  bool has_flag(ObExprInfoFlag flag) const;
  int clear_flag(int32_t flag);
  bool has_const_or_const_expr_flag() const;
  bool has_hierarchical_query_flag() const;
  const ObExprInfo& get_expr_info() const;
  ObExprInfo& get_expr_info();

  void set_relation_ids(const ObRelIds& rel_ids);
  int add_relation_id(int64_t rel_idx);
  int add_relation_ids(const ObRelIds& rel_ids);
  ObRelIds& get_relation_ids();
  const ObRelIds& get_relation_ids() const;

  int add_expr_levels(const ObExprLevels& expr_levels)
  {
    return expr_levels_.add_members(expr_levels);
  }
  ObExprLevels& get_expr_levels()
  {
    return expr_levels_;
  }
  const ObExprLevels& get_expr_levels() const
  {
    return expr_levels_;
  }

  // implemented base on get_param_count() and get_param_expr() interface,
  // children are visited in get_param_expr() return order.
  int preorder_accept(ObRawExprVisitor& visitor);
  int postorder_accept(ObRawExprVisitor& visitor);
  int postorder_replace(ObRawExprVisitor& visitor);

  virtual int do_visit(ObRawExprVisitor& visitor) = 0;

  // skip visit child for expr visitor. (ObSetIterRawExpr)
  virtual bool skip_visit_child() const
  {
    return false;
  }

  virtual int64_t get_param_count() const = 0;
  virtual const ObRawExpr* get_param_expr(int64_t index) const = 0;
  virtual ObRawExpr*& get_param_expr(int64_t index) = 0;
  virtual int64_t get_output_column() const
  {
    return -1;
  }

  inline bool is_not_null() const
  {
    return get_result_type().has_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  }
  inline bool is_auto_increment() const
  {
    return get_result_type().has_result_flag(OB_MYSQL_AUTO_INCREMENT_FLAG);
  }
  inline bool is_rand_func_expr() const
  {
    return has_flag(IS_RAND_FUNC);
  }
  inline bool is_obj_access_expr() const
  {
    return T_OBJ_ACCESS_REF == get_expr_type();
  }
  inline bool is_assoc_index_expr() const
  {
    return T_FUN_PL_ASSOCIATIVE_INDEX == get_expr_type();
  }
  void set_alias_column_name(const common::ObString& alias_name)
  {
    alias_column_name_ = alias_name;
  }
  const common::ObString& get_alias_column_name() const
  {
    return alias_column_name_;
  }
  inline const common::ObString& get_root_alias_column_name() const
  {
    return OB_NOT_NULL(orig_expr_) ? orig_expr_->get_root_alias_column_name() : alias_column_name_;
  }
  inline ObRawExpr* get_orig_expr() const
  {
    return orig_expr_;
  }
  inline const ObRawExpr* get_root_orig_expr() const
  {
    return OB_NOT_NULL(orig_expr_) ? orig_expr_->get_root_orig_expr() : this;
  }
  int set_expr_name(const common::ObString& expr_name);
  const common::ObString& get_expr_name() const
  {
    return expr_name_;
  }
  inline void set_expr_level(int32_t expr_level)
  {
    expr_level_ = expr_level;
  }
  inline int32_t get_expr_level() const
  {
    return expr_level_;
  }
  inline uint64_t hash(uint64_t seed) const
  {
    seed = common::do_hash(type_, seed);
    seed = common::do_hash(expr_class_, seed);
    seed = result_type_.hash(seed);
    seed = hash_internal(seed);
    return seed;
  }
  inline bool is_type_to_str_expr() const
  {
    return (T_FUN_ENUM_TO_STR == type_ || T_FUN_SET_TO_STR == type_ || T_FUN_ENUM_TO_INNER_TYPE == type_ ||
            T_FUN_SET_TO_INNER_TYPE == type_);
  }
  virtual uint64_t hash_internal(uint64_t seed) const = 0;

  int get_name(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type = EXPLAIN_UNINITIALIZED) const;
  int get_type_and_length(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  virtual int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const = 0;

  // post-processing for expressions
  int formalize(const ObSQLSessionInfo* my_session);
  int pull_relation_id_and_levels(int32_t cur_stmt_level);
  int extract_info();
  int deduce_type(const ObSQLSessionInfo* my_session = NULL);
  inline ObExprInfo& get_flags()
  {
    return info_;
  }
  int set_enum_set_values(const common::ObIArray<common::ObString>& values);
  const common::ObIArray<common::ObString>& get_enum_set_values() const
  {
    return enum_set_values_;
  }
  bool is_explicited_reference() const
  {
    return is_explicited_reference_;
  }
  void set_explicited_reference()
  {
    ref_count_++;
    is_explicited_reference_ = true;
  }
  void clear_explicited_referece()
  {
    ref_count_ = 0;
    is_explicited_reference_ = false;
  }
  int64_t get_ref_count() const
  {
    return ref_count_;
  }
  bool is_for_generated_column() const
  {
    return is_for_generated_column_;
  }
  void set_for_generated_column()
  {
    is_for_generated_column_ = true;
  }
  void clear_for_generated_column()
  {
    is_for_generated_column_ = false;
  }
  void set_rt_expr(sql::ObExpr* expr)
  {
    rt_expr_ = expr;
  }
  void reset_rt_expr()
  {
    rt_expr_ = NULL;
  }
  void set_extra(uint64_t extra)
  {
    extra_ = extra;
  }
  void set_is_calculated(bool is_calculated)
  {
    is_calculated_ = is_calculated;
  }
  uint64_t get_extra() const
  {
    return extra_;
  }
  bool is_calculated() const
  {
    return is_calculated_;
  }
  void set_orig_expr(ObRawExpr* expr)
  {
    orig_expr_ = expr;
  }
  bool is_deterministic() const
  {
    return is_deterministic_;
  }
  void set_is_deterministic(bool is_deterministic)
  {
    is_deterministic_ = is_deterministic;
  }
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_,
      K_(expr_level), K_(expr_levels), K_(enum_set_values), K_(is_explicited_reference), K_(ref_count),
      K_(is_for_generated_column), K_(extra), K_(is_calculated));

public:
  uint32_t magic_num_;

protected:
  static const int64_t COMMON_MULTI_NUM = 16;
  static const int64_t COMMON_ENUM_SET_VALUE_NUM = 4;

protected:
  ObExprInfo info_;   // flags
  ObRelIds rel_ids_;  // related table idx
  // means the raw expr contain which level variables(column, aggregate expr, set expr or subquery expr)
  ObExprLevels expr_levels_;
  common::ObIAllocator* inner_alloc_;
  ObRawExprFactory* expr_factory_;
  common::ObString alias_column_name_;
  int32_t expr_level_;
  common::ObArray<common::ObString> enum_set_values_;  // string_map
  common::ObString expr_name_;
  // for column expr, agg expr, window function expr and query ref exprs
  bool is_explicited_reference_;
  int64_t ref_count_;
  bool is_for_generated_column_;
  sql::ObExpr* rt_expr_;

  uint64_t extra_;
  ObRawExpr* orig_expr_;   // orig raw expr before pre cast of pre calc.
  bool is_calculated_;     // for code gerenation in static engine.
  bool is_deterministic_;  // expr is deterministic, given the same inputs, returns the same result
private:
  DISALLOW_COPY_AND_ASSIGN(ObRawExpr);
};

inline void ObRawExpr::set_allocator(ObIAllocator& alloc)
{
  inner_alloc_ = &alloc;
  result_type_.set_allocator(&alloc);
}

inline void ObRawExpr::unset_result_flag(uint32_t result_flag)
{
  result_type_.unset_result_flag(result_flag);
}
inline void ObRawExpr::set_relation_ids(const ObRelIds& rel_ids)
{
  rel_ids_ = rel_ids;
}

inline int ObRawExpr::add_relation_id(int64_t rel_idx)
{
  int ret = common::OB_SUCCESS;
  if (rel_idx < 0) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = rel_ids_.add_member(rel_idx);
  }
  return ret;
}

inline int ObRawExpr::add_relation_ids(const ObRelIds& rel_ids)
{
  return rel_ids_.add_members(rel_ids);
}

inline int ObRawExpr::add_flag(int32_t flag)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(info_.add_member(flag))) {
    // add_member will print log,here no need
  } else if (flag <= IS_INFO_MASK_END) {
    if (OB_FAIL(info_.add_member(CNT_INFO_MASK_BEGIN + flag))) {
      // add_member will print log,here no need
    }
  } else {
  }
  return ret;
}

inline int ObRawExpr::clear_flag(int32_t flag)
{
  return info_.del_member(flag);
}

inline bool ObRawExpr::has_const_or_const_expr_flag() const
{
  return has_flag(IS_CONST) || has_flag(IS_CONST_EXPR);
}

inline bool ObRawExpr::has_hierarchical_query_flag() const
{
  return has_flag(CNT_PRIOR) || has_flag(CNT_LEVEL) || has_flag(CNT_CONNECT_BY_ISLEAF) ||
         has_flag(CNT_CONNECT_BY_ISCYCLE) || has_flag(CNT_CONNECT_BY_ROOT) || has_flag(CNT_SYS_CONNECT_BY_PATH);
  ;
}

inline int ObRawExpr::add_flags(const ObExprInfo& flags)
{
  return info_.add_members(flags);
}

inline bool ObRawExpr::has_flag(ObExprInfoFlag flag) const
{
  return info_.has_member(flag);
}
inline bool ObRawExpr::same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context) const
{
  UNUSED(check_context);
  return (get_expr_type() == expr.get_expr_type() && get_result_type() == expr.get_result_type());
}

inline void ObRawExpr::set_expr_info(const ObExprInfo& info)
{
  info_ = info;
}
inline ObExprInfo& ObRawExpr::get_expr_info()
{
  return info_;
}
inline const ObExprInfo& ObRawExpr::get_expr_info() const
{
  return info_;
}
inline ObRelIds& ObRawExpr::get_relation_ids()
{
  return rel_ids_;
}
inline const ObRelIds& ObRawExpr::get_relation_ids() const
{
  return rel_ids_;
}

////////////////////////////////////////////////////////////////
class ObTerminalRawExpr : public ObRawExpr {
public:
  explicit ObTerminalRawExpr(ObItemType expr_type = T_INVALID) : ObRawExpr(expr_type)
  {}
  explicit ObTerminalRawExpr(common::ObIAllocator& alloc, ObItemType expr_type = T_INVALID)
      : ObRawExpr(alloc, expr_type)
  {}
  virtual ~ObTerminalRawExpr()
  {}
  virtual void clear_child()
  {}

  virtual int64_t get_param_count() const
  {
    return 0;
  }
  virtual const ObRawExpr* get_param_expr(int64_t index) const
  {
    UNUSED(index);
    return NULL;
  }
  virtual ObRawExpr*& get_param_expr(int64_t index);
  virtual uint64_t hash_internal(uint64_t seed) const
  {
    return seed;
  }

protected:
private:
  DISALLOW_COPY_AND_ASSIGN(ObTerminalRawExpr);
};

////////////////////////////////////////////////////////////////
class ObConstRawExpr : public ObTerminalRawExpr, public jit::expr::ObConstExpr {
public:
  ObConstRawExpr() : is_date_unit_(false) /*: precalc_expr_(NULL)*/
  {
    ObExpr::set_expr_class(ObExpr::EXPR_CONST);
  }
  ObConstRawExpr(common::ObIAllocator& alloc)
      : ObExpr(alloc), ObTerminalRawExpr(alloc), ObConstExpr(), is_date_unit_(false)
  {
    ObExpr::set_expr_class(ObExpr::EXPR_CONST);
  }
  ObConstRawExpr(const oceanbase::common::ObObj& val, ObItemType expr_type = T_INVALID)
      : ObExpr(expr_type), ObTerminalRawExpr(expr_type), ObConstExpr(), is_date_unit_(false)
  {
    set_value(val);
    set_expr_class(ObExpr::EXPR_CONST);
  }
  virtual ~ObConstRawExpr()
  {}
  int assign(const ObConstRawExpr& other);

  int deep_copy(ObRawExprFactory& expr_factory, const ObConstRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  void set_value(const oceanbase::common::ObObj& val);
  void set_literal_prefix(const common::ObString& name);
  void set_expr_obj_meta(const common::ObObjMeta& meta)
  {
    obj_meta_ = meta;
  }
  const common::ObObjMeta& get_expr_obj_meta() const
  {
    return obj_meta_;
  }
  const common::ObString& get_literal_prefix() const
  {
    return literal_prefix_;
  }
  void set_is_date_unit();
  void reset_is_date_unit();
  bool is_date_unit()
  {
    return true == is_date_unit_;
  }
  virtual void reset();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int do_visit(ObRawExprVisitor& visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;
  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  DECLARE_VIRTUAL_TO_STRING;

private:
  common::ObString literal_prefix_;  // used in compile phase.
  common::ObObjMeta obj_meta_;
  bool is_date_unit_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConstRawExpr);
};

////////////////////////////////////////////////////////////////
class ObVarRawExpr : public ObTerminalRawExpr, public jit::expr::ObVarExpr {
public:
  ObVarRawExpr()
  {
    ObExpr::set_expr_class(ObExpr::EXPR_VAR);
  }
  ObVarRawExpr(common::ObIAllocator& alloc)
      : ObExpr(alloc), ObTerminalRawExpr(alloc), ObVarExpr(), result_type_assigned_(false)
  {
    ObExpr::set_expr_class(ObExpr::EXPR_VAR);
  }
  ObVarRawExpr(ObItemType expr_type = T_INVALID)
      : ObExpr(expr_type), ObTerminalRawExpr(expr_type), ObVarExpr(), result_type_assigned_(false)
  {
    set_expr_class(ObExpr::EXPR_VAR);
  }
  virtual ~ObVarRawExpr()
  {}

  int assign(const ObVarRawExpr& other);

  int deep_copy(ObRawExprFactory& expr_factory, const ObVarRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int do_visit(ObRawExprVisitor& visitor) override;

  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  void set_result_type_assigned(bool v)
  {
    result_type_assigned_ = v;
  }
  bool get_result_type_assigned()
  {
    return result_type_assigned_;
  }

private:
  bool result_type_assigned_;
  DISALLOW_COPY_AND_ASSIGN(ObVarRawExpr);
};

////////////////////////////////////////////////////////////////
class ObUserVarIdentRawExpr : public ObConstRawExpr {
public:
  ObUserVarIdentRawExpr() : is_contain_assign_(false), query_has_udf_(false)
  {}
  ObUserVarIdentRawExpr(common::ObIAllocator& alloc)
      : ObConstRawExpr(alloc), is_contain_assign_(false), query_has_udf_(false)
  {}
  ObUserVarIdentRawExpr(const oceanbase::common::ObObj& val, ObItemType expr_type = T_INVALID)
      : ObConstRawExpr(val, expr_type), is_contain_assign_(false), query_has_udf_(false)
  {}
  virtual ~ObUserVarIdentRawExpr()
  {}
  int assign(const ObUserVarIdentRawExpr& other);

  int deep_copy(ObRawExprFactory& expr_factory, const ObUserVarIdentRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  virtual void reset();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int do_visit(ObRawExprVisitor& visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;

  bool get_is_contain_assign() const
  {
    return is_contain_assign_;
  }
  void set_is_contain_assign(bool is_contain_assign)
  {
    is_contain_assign_ = is_contain_assign;
  }
  bool get_query_has_udf() const
  {
    return query_has_udf_;
  }
  void set_query_has_udf(bool query_has_udf)
  {
    query_has_udf_ = query_has_udf;
  }
  bool is_same_variable(const ObObj& obj) const;
  DECLARE_VIRTUAL_TO_STRING;

private:
  bool is_contain_assign_;
  bool query_has_udf_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObUserVarIdentRawExpr);
};

////////////////////////////////////////////////////////////////
class ObLogicalOperator;
class ObQueryRefRawExpr : public ObTerminalRawExpr {
public:
  ObQueryRefRawExpr()
      : ObTerminalRawExpr(),
        ref_id_(common::OB_INVALID_ID),
        output_column_(0),
        is_set_(false),
        is_cursor_(false),
        ref_type_(OB_STMT)
  {
    ref_stmt_ = NULL;
    ref_operator_ = NULL;
    set_expr_class(ObExpr::EXPR_QUERY_REF);
  }

  ObQueryRefRawExpr(common::ObIAllocator& alloc)
      : ObTerminalRawExpr(alloc),
        ref_id_(common::OB_INVALID_ID),
        output_column_(0),
        is_set_(false),
        is_cursor_(false),
        ref_type_(OB_STMT)
  {
    ref_stmt_ = NULL;
    ref_operator_ = NULL;
    set_expr_class(ObExpr::EXPR_QUERY_REF);
  }
  ObQueryRefRawExpr(int64_t id, ObItemType expr_type = T_INVALID)
      : ObTerminalRawExpr(expr_type),
        ref_id_(id),
        output_column_(0),
        is_set_(false),
        is_cursor_(false),
        ref_type_(OB_STMT)
  {
    ref_stmt_ = NULL;
    ref_operator_ = NULL;
    set_expr_class(ObExpr::EXPR_QUERY_REF);
  }
  virtual ~ObQueryRefRawExpr()
  {}
  int assign(const ObQueryRefRawExpr& other);
  int deep_copy(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObQueryRefRawExpr& other);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  int64_t get_ref_id() const;
  void set_ref_id(int64_t id);
  ObSelectStmt* get_ref_stmt();
  const ObSelectStmt* get_ref_stmt() const;
  void set_ref_stmt(ObSelectStmt* ref_stmt)
  {
    ref_stmt_ = ref_stmt;
    ref_type_ = OB_STMT;
  }
  ObLogicalOperator* get_ref_operator()
  {
    return (OB_LOGICAL_OPERATOR == ref_type_ ? ref_operator_ : NULL);
  }
  const ObLogicalOperator* get_ref_operator() const
  {
    return (OB_LOGICAL_OPERATOR == ref_type_ ? ref_operator_ : NULL);
  }
  void set_ref_operator(ObLogicalOperator* ref_opeator)
  {
    ref_operator_ = ref_opeator;
    ref_type_ = OB_LOGICAL_OPERATOR;
  }
  bool is_ref_stmt() const
  {
    return OB_STMT == ref_type_;
  }
  bool is_ref_operator() const
  {
    return OB_LOGICAL_OPERATOR == ref_type_;
  }
  void set_output_column(int64_t output_column);
  int64_t get_output_column() const;
  int add_column_type(const ObExprResType& type)
  {
    return column_types_.push_back(type);
  }
  const common::ObIArray<ObExprResType>& get_column_types() const
  {
    return column_types_;
  }
  common::ObIArray<ObExprResType>& get_column_types()
  {
    return column_types_;
  }
  void set_is_set(bool is_set)
  {
    is_set_ = is_set;
  }
  bool is_set() const
  {
    return is_set_;
  }
  void set_cursor(bool is_cursor)
  {
    is_cursor_ = is_cursor;
  }
  bool is_cursor() const
  {
    return is_cursor_;
  }
  virtual void reset();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int do_visit(ObRawExprVisitor& visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const
  {
    return common::do_hash(ref_id_, seed);
  }

  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_, N_ID,
      ref_id_, K_(expr_level), K_(expr_levels), K_(output_column), K_(is_set), K_(is_cursor), K_(column_types),
      K_(enum_set_values));

private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryRefRawExpr);
  int64_t ref_id_;
  union {
    ObSelectStmt* ref_stmt_;
    ObLogicalOperator* ref_operator_;
  };
  enum RefType { OB_STMT = 0, OB_LOGICAL_OPERATOR = 1 };
  int64_t output_column_;
  bool is_set_;
  bool is_cursor_;
  common::ObSEArray<ObExprResType, 64, common::ModulePageAllocator, true> column_types_;
  RefType ref_type_;
};

inline int64_t ObQueryRefRawExpr::get_ref_id() const
{
  return ref_id_;
}
inline void ObQueryRefRawExpr::set_ref_id(int64_t id)
{
  ref_id_ = id;
}

inline void ObQueryRefRawExpr::set_output_column(int64_t output_column)
{
  output_column_ = output_column;
}

inline int64_t ObQueryRefRawExpr::get_output_column() const
{
  return output_column_;
}

////////////////////////////////////////////////////////////////
class ObColumnRefRawExpr : public ObTerminalRawExpr, public jit::expr::ObColumnRefExpr {
public:
  ObColumnRefRawExpr()
      : ObExpr(),
        ObTerminalRawExpr(),
        jit::expr::ObColumnRefExpr(),
        table_id_(common::OB_INVALID_ID),
        column_id_(common::OB_INVALID_ID),
        database_name_(),
        table_name_(),
        synonym_name_(),
        synonym_db_name_(),
        column_name_(),
        column_flags_(0),
        dependant_expr_(NULL),
        is_lob_column_(false),
        is_unpivot_mocked_column_(false),
        is_hidden_(false),
        real_expr_(nullptr)
  {
    set_expr_class(ObExpr::EXPR_COLUMN_REF);
  }

  ObColumnRefRawExpr(common::ObIAllocator& alloc)
      : ObExpr(alloc),
        ObTerminalRawExpr(alloc),
        ObColumnRefExpr(alloc),
        table_id_(common::OB_INVALID_ID),
        column_id_(common::OB_INVALID_ID),
        database_name_(),
        table_name_(),
        synonym_name_(),
        synonym_db_name_(),
        column_name_(),
        column_flags_(0),
        dependant_expr_(NULL),
        is_lob_column_(false),
        is_unpivot_mocked_column_(false),
        is_hidden_(false),
        real_expr_(nullptr)
  {
    set_expr_class(ObExpr::EXPR_COLUMN_REF);
  }

  ObColumnRefRawExpr(uint64_t first_id, uint64_t second_id, ObItemType expr_type = T_INVALID)
      : ObExpr(expr_type),
        ObTerminalRawExpr(expr_type),
        ObColumnRefExpr(expr_type),
        table_id_(first_id),
        column_id_(second_id),
        database_name_(),
        table_name_(),
        synonym_name_(),
        synonym_db_name_(),
        column_name_(),
        column_flags_(0),
        dependant_expr_(NULL),
        is_lob_column_(false),
        is_unpivot_mocked_column_(false),
        is_hidden_(false),
        real_expr_(nullptr)
  {
    set_expr_class(ObExpr::EXPR_COLUMN_REF);
  }

  virtual ~ObColumnRefRawExpr()
  {}
  int assign(const ObColumnRefRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObColumnRefRawExpr& other, bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  uint64_t get_table_id() const;
  uint64_t get_column_id() const;
  uint64_t& get_table_id();
  uint64_t& get_column_id();
  void set_ref_id(uint64_t table_id, uint64_t column_id);
  void set_table_id(uint64_t table_id);
  void set_column_attr(const common::ObString& table_name, const common::ObString& column_name);
  inline void set_table_name(const common::ObString& table_name)
  {
    table_name_ = table_name;
  }
  inline common::ObString& get_table_name()
  {
    return table_name_;
  }
  inline const common::ObString& get_table_name() const
  {
    return table_name_;
  }
  inline void set_synonym_name(const common::ObString& synonym_name)
  {
    synonym_name_ = synonym_name;
  }
  inline common::ObString& get_synonym_name()
  {
    return synonym_name_;
  }
  inline const common::ObString& get_synonym_name() const
  {
    return synonym_name_;
  }
  inline void set_synonym_db_name(const common::ObString& synonym_db_name)
  {
    synonym_db_name_ = synonym_db_name;
  }
  inline common::ObString& get_synonym_db_name()
  {
    return synonym_db_name_;
  }
  inline const common::ObString& get_synonym_db_name() const
  {
    return synonym_db_name_;
  }
  inline void set_column_name(const common::ObString& column_name)
  {
    column_name_ = column_name;
  }
  inline common::ObString& get_column_name()
  {
    return column_name_;
  }
  inline const common::ObString& get_column_name() const
  {
    return column_name_;
  }
  inline void set_database_name(const common::ObString& db_name)
  {
    database_name_ = db_name;
  }
  inline const common::ObString& get_database_name() const
  {
    return database_name_;
  }
  inline common::ObString& get_database_name()
  {
    return database_name_;
  }
  inline int64_t get_cte_generate_column_projector_offset() const
  {
    return get_column_id();
  }
  virtual void reset();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int do_visit(ObRawExprVisitor& visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;
  inline bool is_generated_column() const
  {
    return share::schema::ObSchemaUtils::is_generated_column(column_flags_);
  }
  inline bool is_default_expr_v2_column() const
  {
    return share::schema::ObSchemaUtils::is_default_expr_v2_column(column_flags_);
  }
  inline bool is_virtual_generated_column() const
  {
    return share::schema::ObSchemaUtils::is_virtual_generated_column(column_flags_);
  }
  inline bool is_stored_generated_column() const
  {
    return share::schema::ObSchemaUtils::is_stored_generated_column(column_flags_);
  }
  inline bool is_fulltext_column() const
  {
    return share::schema::ObSchemaUtils::is_fulltext_column(column_flags_);
  }
  inline bool is_cte_generated_column() const
  {
    return share::schema::ObSchemaUtils::is_cte_generated_column(column_flags_);
  }
  inline bool has_generated_column_deps() const
  {
    return column_flags_ & GENERATED_DEPS_CASCADE_FLAG;
  }
  inline bool is_table_part_key_column() const
  {
    return column_flags_ & TABLE_PART_KEY_COLUMN_FLAG;
  }
  void set_column_flags(uint64_t column_flags)
  {
    column_flags_ = column_flags;
  }
  inline uint64_t get_column_flags() const
  {
    return column_flags_;
  }
  inline const ObRawExpr* get_dependant_expr() const
  {
    return dependant_expr_;
  }
  inline ObRawExpr* get_dependant_expr()
  {
    return dependant_expr_;
  }
  inline void set_dependant_expr(ObRawExpr* expr)
  {
    dependant_expr_ = expr;
  }
  bool is_lob_column() const
  {
    return is_lob_column_;
  }
  void set_lob_column(bool is_lob_column)
  {
    is_lob_column_ = is_lob_column;
  }
  bool is_unpivot_mocked_column() const
  {
    return is_unpivot_mocked_column_;
  }
  void set_unpivot_mocked_column(const bool value)
  {
    is_unpivot_mocked_column_ = value;
  }
  bool is_hidden_column() const
  {
    return is_hidden_;
  }
  void set_hidden_column(const bool value)
  {
    is_hidden_ = value;
  }

  inline ObRawExpr* get_real_expr()
  {
    return real_expr_;
  }
  inline void set_real_expr(ObRawExpr* expr)
  {
    real_expr_ = expr;
  }

  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;

  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_, N_TID,
      table_id_, N_CID, column_id_, K_(database_name), K_(table_name), K_(synonym_name), K_(synonym_db_name),
      K_(column_name), K_(expr_level), K_(expr_levels), K_(column_flags), K_(enum_set_values), K_(is_lob_column),
      K_(is_unpivot_mocked_column), K_(is_hidden));

private:
  DISALLOW_COPY_AND_ASSIGN(ObColumnRefRawExpr);
  uint64_t table_id_;
  uint64_t column_id_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString synonym_name_;
  common::ObString synonym_db_name_;
  common::ObString column_name_;
  uint64_t column_flags_;          // same as flags in ObColumnSchemaV2
  ObRawExpr* dependant_expr_;      // TODO
  bool is_lob_column_;             // TODO add lob column
  bool is_unpivot_mocked_column_;  // used for unpivot
  bool is_hidden_;                 // used for print hidden column
  ObRawExpr* real_expr_;           // for oracle virtual table that is mapping a real table
};

inline void ObColumnRefRawExpr::set_ref_id(uint64_t table_id, uint64_t column_id)
{
  table_id_ = table_id;
  column_id_ = column_id;
}

inline void ObColumnRefRawExpr::set_table_id(uint64_t table_id)
{
  table_id_ = table_id;
}

inline uint64_t ObColumnRefRawExpr::get_table_id() const
{
  return table_id_;
}

inline uint64_t ObColumnRefRawExpr::get_column_id() const
{
  return column_id_;
}

inline uint64_t& ObColumnRefRawExpr::get_table_id()
{
  return table_id_;
}

inline uint64_t& ObColumnRefRawExpr::get_column_id()
{
  return column_id_;
}

////////////////////////////////////////////////////////////////
class ObSetOpRawExpr : public ObTerminalRawExpr {
public:
  ObSetOpRawExpr() : ObTerminalRawExpr(), idx_(-1)
  {
    set_expr_class(ObExpr::EXPR_SET_OP);
  }
  ObSetOpRawExpr(common::ObIAllocator& alloc) : ObTerminalRawExpr(alloc), idx_(-1)
  {
    set_expr_class(ObExpr::EXPR_SET_OP);
  }
  virtual ~ObSetOpRawExpr()
  {}
  virtual int do_visit(ObRawExprVisitor& visitor) override;
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs) override;
  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  int deep_copy(
      ObRawExprFactory& expr_factory, const ObSetOpRawExpr& other, const uint64_t copy_types, bool use_new_allocator);

  int assign(const ObSetOpRawExpr& other);

  void set_idx(int64_t idx)
  {
    idx_ = idx;
  }
  int64_t get_idx() const
  {
    return idx_;
  }
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_,
      K_(expr_level), K_(expr_levels), K_(idx));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSetOpRawExpr);
  int64_t idx_;
};

////////////////////////////////////////////////////////////////
class ObAliasRefRawExpr : public ObRawExpr {
public:
  ObAliasRefRawExpr() : ObRawExpr(), ref_expr_(NULL)
  {
    set_expr_class(ObExpr::EXPR_ALIAS_REF);
  }
  ObAliasRefRawExpr(common::ObIAllocator& alloc) : ObRawExpr(alloc), ref_expr_(NULL), project_index_(OB_INVALID_INDEX)
  {
    set_expr_class(ObExpr::EXPR_ALIAS_REF);
  }

  virtual ~ObAliasRefRawExpr()
  {}
  int assign(const ObAliasRefRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObAliasRefRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  const ObRawExpr* get_ref_expr() const;
  ObRawExpr* get_ref_expr();
  void set_ref_expr(ObRawExpr* ref_expr)
  {
    ref_expr_ = ref_expr;
  }
  bool is_ref_query_output() const
  {
    return ref_expr_->is_query_ref_expr() && project_index_ != OB_INVALID_INDEX;
  }
  int64_t get_project_index() const
  {
    return project_index_;
  }
  void set_query_output(ObQueryRefRawExpr* query_ref, int64_t project_index)
  {
    ref_expr_ = query_ref;
    project_index_ = project_index;
  }
  virtual void clear_child() override
  {
    ref_expr_ = NULL;
    project_index_ = OB_INVALID_INDEX;
  }
  virtual int64_t get_param_count() const override
  {
    return 1;
  }
  virtual const ObRawExpr* get_param_expr(int64_t index) const override;
  virtual ObRawExpr*& get_param_expr(int64_t index) override;
  virtual int do_visit(ObRawExprVisitor& visitor) override;
  virtual uint64_t hash_internal(uint64_t seed) const;
  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_, N_VALUE,
      ref_expr_, K_(enum_set_values));

private:
  DISALLOW_COPY_AND_ASSIGN(ObAliasRefRawExpr);
  ObRawExpr* ref_expr_;
  int64_t project_index_;  // project index of the subquery
};

////////////////////////////////////////////////////////////////
class ObNonTerminalRawExpr : public ObRawExpr {
public:
  ObNonTerminalRawExpr() : ObRawExpr(), op_(NULL), input_types_()
  {}
  ObNonTerminalRawExpr(common::ObIAllocator& alloc) : ObRawExpr(alloc), op_(NULL), input_types_()
  {}
  virtual ~ObNonTerminalRawExpr()
  {
    free_op();
  }

  int assign(const ObNonTerminalRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObNonTerminalRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  virtual void reset()
  {
    free_op();
    input_types_.reset();
  }

  virtual ObExprOperator* get_op();
  void free_op();
  /*
   * store the target type of paramters.
   */
  int set_input_types(const ObIExprResTypes& input_types);

  inline const ObExprResTypes& get_input_types()
  {
    return input_types_;
  }
  inline int64_t get_input_types_count() const
  {
    return input_types_.count();
  }

  virtual uint64_t hash_internal(uint64_t seed) const
  {
    return seed;
  }

protected:
  // data members
  ObExprOperator* op_;
  ObExprResTypes input_types_;
  DISALLOW_COPY_AND_ASSIGN(ObNonTerminalRawExpr);
};

////////////////////////////////////////////////////////////////
class ObOpRawExpr : public ObNonTerminalRawExpr, public jit::expr::ObOpExpr {
public:
  ObOpRawExpr()
      : ObExpr(),
        ObNonTerminalRawExpr(),
        ObOpExpr(),
        exprs_(),
        subquery_key_(T_WITH_NONE),
        deduce_type_adding_implicit_cast_(true)
  {
    set_expr_class(ObExpr::EXPR_OPERATOR);
  }

  ObOpRawExpr(common::ObIAllocator& alloc)
      : ObExpr(alloc),
        ObNonTerminalRawExpr(alloc),
        ObOpExpr(alloc),
        exprs_(),
        subquery_key_(T_WITH_NONE),
        deduce_type_adding_implicit_cast_(true)
  {
    set_expr_class(ObExpr::EXPR_OPERATOR);
  }

  ObOpRawExpr(ObRawExpr* first_expr, ObRawExpr* second_expr, ObItemType type);  // binary op
  virtual ~ObOpRawExpr()
  {}
  int assign(const ObOpRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObOpRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  int set_param_expr(ObRawExpr* expr);                                                        // unary op
  int set_param_exprs(ObRawExpr* first_expr, ObRawExpr* second_expr);                         // binary op
  int set_param_exprs(ObRawExpr* first_expr, ObRawExpr* second_expr, ObRawExpr* third_expr);  // triple op
  int add_param_expr(ObRawExpr* expr);
  int remove_param_expr(int64_t index);
  int replace_param_expr(int64_t index, ObRawExpr* expr);

  bool deduce_type_adding_implicit_cast() const
  {
    return deduce_type_adding_implicit_cast_;
  }
  void set_deduce_type_adding_implicit_cast(const bool v)
  {
    deduce_type_adding_implicit_cast_ = v;
  }

  void set_expr_type(ObItemType type);

  common::ObIArray<ObRawExpr*>& get_param_exprs()
  {
    return exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_param_exprs() const
  {
    return exprs_;
  }

  int64_t get_param_count() const;
  const ObRawExpr* get_param_expr(int64_t index) const;
  ObRawExpr*& get_param_expr(int64_t index);
  virtual int64_t get_output_column() const
  {
    return T_OP_ROW == get_expr_type() ? get_param_count() : -1;
  }

  virtual void clear_child();
  virtual void reset();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  // used for jit expr
  virtual int64_t get_children_count() const
  {
    return exprs_.count();
  }
  // used for jit expr
  virtual int get_children(jit::expr::ExprArray& jit_exprs) const;

  void set_subquery_key(ObSubQueryKey& key)
  {
    subquery_key_ = key;
  }
  ObSubQueryKey get_subquery_key()
  {
    return subquery_key_;
  }

  virtual int do_visit(ObRawExprVisitor& visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;

  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  int get_subquery_comparison_name(
      const common::ObString& symbol, char* buf, int64_t buf_len, int64_t& pos, ExplainType type) const;
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_,
      K_(expr_levels), N_CHILDREN, exprs_);

protected:
  common::ObSEArray<ObRawExpr*, COMMON_MULTI_NUM, common::ModulePageAllocator, true> exprs_;
  ObSubQueryKey subquery_key_;

  bool deduce_type_adding_implicit_cast_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOpRawExpr);
};

inline const ObRawExpr* ObOpRawExpr::get_param_expr(int64_t index) const
{
  const ObRawExpr* expr = NULL;
  if (index >= 0 && index < exprs_.count()) {
    expr = exprs_.at(index);
  }
  return expr;
}

// here must return in two branch, because ret value is a *&
inline ObRawExpr*& ObOpRawExpr::get_param_expr(int64_t index)
{
  if (index >= 0 && index < exprs_.count()) {
    return exprs_.at(index);
  } else {
    return USELESS_POINTER;
  }
}

inline int ObOpRawExpr::add_param_expr(ObRawExpr* expr)
{
  return exprs_.push_back(expr);
}

inline int ObOpRawExpr::remove_param_expr(int64_t index)
{
  int ret = common::OB_SUCCESS;
  if (index < 0 || index >= exprs_.count()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = exprs_.remove(index);
  }
  return ret;
}

inline int ObOpRawExpr::replace_param_expr(int64_t index, ObRawExpr* expr)
{
  int ret = common::OB_SUCCESS;
  if (index < 0 || index >= exprs_.count()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ObRawExpr*& target_expr = exprs_.at(index);
    if (OB_NOT_NULL(expr) && expr != target_expr) {
      expr->set_orig_expr(target_expr);
    }
    target_expr = expr;
  }
  return ret;
}

inline int64_t ObOpRawExpr::get_param_count() const
{
  return exprs_.count();
}

inline uint64_t ObOpRawExpr::hash_internal(uint64_t seed) const
{
  uint64_t hash_value = seed;
  for (int64_t i = 0; i < exprs_.count(); ++i) {
    if (NULL != exprs_.at(i)) {
      hash_value = common::do_hash(*(exprs_.at(i)), hash_value);
    }
  }
  return hash_value;
}

////////////////////////////////////////////////////////////////
class ObCaseOpRawExpr : public ObNonTerminalRawExpr, public jit::expr::ObCaseOpExpr {
public:
  ObCaseOpRawExpr()
      : ObExpr(),
        ObNonTerminalRawExpr(),
        ObCaseOpExpr(),
        arg_expr_(NULL),
        when_exprs_(),
        then_exprs_(),
        default_expr_(NULL),
        is_decode_func_(false)
  {
    set_expr_class(ObExpr::EXPR_CASE_OPERATOR);
  }
  ObCaseOpRawExpr(common::ObIAllocator& alloc)
      : ObExpr(alloc),
        ObNonTerminalRawExpr(alloc),
        ObCaseOpExpr(),
        arg_expr_(NULL),
        when_exprs_(),
        then_exprs_(),
        default_expr_(NULL),
        is_decode_func_(false)
  {
    set_expr_class(ObExpr::EXPR_CASE_OPERATOR);
  }
  virtual ~ObCaseOpRawExpr()
  {}
  int assign(const ObCaseOpRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObCaseOpRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  const ObRawExpr* get_arg_param_expr() const;
  const ObRawExpr* get_default_param_expr() const;
  const ObRawExpr* get_when_param_expr(int64_t index) const;
  const ObRawExpr* get_then_param_expr(int64_t index) const;
  ObRawExpr*& get_arg_param_expr();
  inline common::ObIArray<ObRawExpr*>& get_when_param_exprs()
  {
    return when_exprs_;
  }
  inline common::ObIArray<ObRawExpr*>& get_then_param_exprs()
  {
    return then_exprs_;
  }
  ObRawExpr*& get_default_param_expr();
  ObRawExpr*& get_when_param_expr(int64_t index);
  ObRawExpr*& get_then_param_expr(int64_t index);
  void set_arg_param_expr(ObRawExpr* expr);
  void set_default_param_expr(ObRawExpr* expr);
  int add_when_param_expr(ObRawExpr* expr);
  int add_then_param_expr(ObRawExpr* expr);
  int replace_when_param_expr(int64_t index, ObRawExpr* expr);
  int replace_then_param_expr(int64_t index, ObRawExpr* expr);
  int replace_param_expr(int64_t index, ObRawExpr* new_expr);
  int64_t get_when_expr_size() const;
  int64_t get_then_expr_size() const;
  bool is_arg_case() const
  {
    return NULL != arg_expr_;
  }
  bool is_decode_func() const
  {
    return is_decode_func_;
  }

  virtual void clear_child();
  virtual void reset();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int do_visit(ObRawExprVisitor& visitor) override;

  virtual int64_t get_param_count() const;
  virtual const ObRawExpr* get_param_expr(int64_t index) const;
  virtual ObRawExpr*& get_param_expr(int64_t index);

  // used for jit
  virtual int64_t get_children_count() const
  {
    return get_param_count();
  }

  virtual int get_children(jit::expr::ExprArray& jit_exprs) const;

  virtual uint64_t hash_internal(uint64_t seed) const;

  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_,
      K_(expr_levels), N_ARG_CASE, arg_expr_, N_DEFAULT, default_expr_, N_WHEN, when_exprs_, N_THEN, then_exprs_,
      N_DECODE, is_decode_func_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCaseOpRawExpr);
  ObRawExpr* arg_expr_;
  common::ObSEArray<ObRawExpr*, COMMON_MULTI_NUM, common::ModulePageAllocator, true> when_exprs_;
  common::ObSEArray<ObRawExpr*, COMMON_MULTI_NUM, common::ModulePageAllocator, true> then_exprs_;
  ObRawExpr* default_expr_;
  bool is_decode_func_;
};

inline const ObRawExpr* ObCaseOpRawExpr::get_arg_param_expr() const
{
  return arg_expr_;
}
inline const ObRawExpr* ObCaseOpRawExpr::get_default_param_expr() const
{
  return default_expr_;
}
inline const ObRawExpr* ObCaseOpRawExpr::get_when_param_expr(int64_t index) const
{
  ObRawExpr* expr = NULL;
  if (OB_LIKELY(index >= 0 && index < when_exprs_.count())) {
    expr = when_exprs_.at(index);
  } else {
  }
  return expr;
}
inline const ObRawExpr* ObCaseOpRawExpr::get_then_param_expr(int64_t index) const
{
  ObRawExpr* expr = NULL;
  if (OB_LIKELY(index >= 0 || index < then_exprs_.count())) {
    expr = then_exprs_.at(index);
  } else {
  }
  return expr;
}
inline ObRawExpr*& ObCaseOpRawExpr::get_arg_param_expr()
{
  return arg_expr_;
}
inline ObRawExpr*& ObCaseOpRawExpr::get_default_param_expr()
{
  return default_expr_;
}

// here must return in two branch, because ret value is a *&
inline ObRawExpr*& ObCaseOpRawExpr::get_when_param_expr(int64_t index)
{
  if (OB_LIKELY(index >= 0 && index < when_exprs_.count())) {
    return when_exprs_.at(index);
  } else {
    return USELESS_POINTER;
  }
}

// here must return in two branch, because ret value is a *&
inline ObRawExpr*& ObCaseOpRawExpr::get_then_param_expr(int64_t index)
{
  if (OB_LIKELY(index >= 0 && index < then_exprs_.count())) {
    return then_exprs_.at(index);
  } else {
    return USELESS_POINTER;
  }
}
inline void ObCaseOpRawExpr::set_arg_param_expr(ObRawExpr* expr)
{
  arg_expr_ = expr;
}
inline void ObCaseOpRawExpr::set_default_param_expr(ObRawExpr* expr)
{
  default_expr_ = expr;
}
inline int ObCaseOpRawExpr::add_when_param_expr(ObRawExpr* expr)
{
  int ret = when_exprs_.push_back(expr);
  return ret;
}
inline int ObCaseOpRawExpr::add_then_param_expr(ObRawExpr* expr)
{
  int ret = then_exprs_.push_back(expr);
  return ret;
}

inline int ObCaseOpRawExpr::replace_when_param_expr(int64_t index, ObRawExpr* expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY((index < 0 || index >= when_exprs_.count()))) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    when_exprs_.at(index) = expr;
  }
  return ret;
}

inline int ObCaseOpRawExpr::replace_then_param_expr(int64_t index, ObRawExpr* expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY((index < 0 || index >= then_exprs_.count()))) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    then_exprs_.at(index) = expr;
  }
  return ret;
}

inline int64_t ObCaseOpRawExpr::get_when_expr_size() const
{
  return when_exprs_.count();
}
inline int64_t ObCaseOpRawExpr::get_then_expr_size() const
{
  return then_exprs_.count();
}
inline int64_t ObCaseOpRawExpr::get_param_count() const
{
  return when_exprs_.count() + then_exprs_.count() + (NULL == arg_expr_ ? 0 : 1) + (NULL == default_expr_ ? 0 : 1);
}
inline uint64_t ObCaseOpRawExpr::hash_internal(uint64_t seed) const
{
  if (arg_expr_) {
    seed = common::do_hash(*arg_expr_, seed);
  }

  for (int64_t i = 0; i < when_exprs_.count(); ++i) {
    if (OB_LIKELY(NULL != when_exprs_.at(i))) {
      seed = common::do_hash(*(when_exprs_.at(i)), seed);
    }
  }
  for (int64_t i = 0; i < then_exprs_.count(); ++i) {
    if (OB_LIKELY(NULL != then_exprs_.at(i))) {
      seed = common::do_hash(*(then_exprs_.at(i)), seed);
    }
  }

  if (NULL != default_expr_) {
    seed = common::do_hash(*default_expr_, seed);
  }

  seed = common::do_hash(is_decode_func_, seed);

  return seed;
}

////////////////////////////////////////////////////////////////
class ObAggFunRawExpr : public ObRawExpr {
public:
  ObAggFunRawExpr()
      : ObRawExpr(),
        real_param_exprs_(),
        push_down_sum_expr_(NULL),
        push_down_count_expr_(NULL),
        push_down_synopsis_expr_(NULL),
        distinct_(false),
        order_items_(),
        separator_param_expr_(NULL),
        udf_meta_(),
        linear_inter_expr_(NULL),
        is_nested_aggr_(false)
  {
    set_expr_class(ObExpr::EXPR_AGGR);
    order_items_.set_label(common::ObModIds::OB_SQL_AGGR_FUNC_ARR);
  }
  ObAggFunRawExpr(common::ObIAllocator& alloc)
      : ObRawExpr(alloc),
        real_param_exprs_(),
        push_down_sum_expr_(NULL),
        push_down_count_expr_(NULL),
        push_down_synopsis_expr_(NULL),
        distinct_(false),
        order_items_(),
        separator_param_expr_(NULL),
        udf_meta_(),
        linear_inter_expr_(NULL),
        is_nested_aggr_(false)
  {
    set_expr_class(ObExpr::EXPR_AGGR);
    order_items_.set_label(common::ObModIds::OB_SQL_AGGR_FUNC_ARR);
  }
  ObAggFunRawExpr(const common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true>& real_param_exprs,
      bool is_distinct, ObItemType expr_type = T_INVALID)
      : ObRawExpr(expr_type),
        real_param_exprs_(real_param_exprs),
        push_down_sum_expr_(NULL),
        push_down_count_expr_(NULL),
        push_down_synopsis_expr_(NULL),
        distinct_(is_distinct),
        order_items_(),
        separator_param_expr_(NULL),
        udf_meta_(),
        linear_inter_expr_(NULL),
        is_nested_aggr_(false)
  {
    set_expr_class(ObExpr::EXPR_AGGR);
    order_items_.set_label(common::ObModIds::OB_SQL_AGGR_FUNC_ARR);
  }
  virtual ~ObAggFunRawExpr()
  {}
  int assign(const ObAggFunRawExpr& other);

  int deep_copy(ObRawExprFactory& expr_factory, const ObAggFunRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  int add_real_param_expr(ObRawExpr* expr);
  int replace_real_param_expr(int64_t index, ObRawExpr* expr);
  int replace_param_expr(int64_t index, ObRawExpr* expr);
  void set_push_down_sum_expr(ObRawExpr* expr);
  void set_push_down_count_expr(ObRawExpr* expr);
  bool contain_nested_aggr() const;
  bool is_param_distinct() const;
  void set_param_distinct(bool is_distinct);
  void set_separator_param_expr(ObRawExpr* separator_param_expr);
  void set_linear_inter_expr(ObRawExpr* linear_inter_expr);
  bool is_nested_aggr() const;
  void set_in_nested_aggr(bool is_nested);
  int add_order_item(const OrderItem& order_item);
  virtual void clear_child();
  virtual void reset();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int64_t get_param_count() const
  {
    return real_param_exprs_.count() + order_items_.count();
  }
  virtual const ObRawExpr* get_param_expr(int64_t index) const;
  virtual ObRawExpr*& get_param_expr(int64_t index);
  inline int64_t get_real_param_count() const
  {
    return real_param_exprs_.count();
  }
  inline const common::ObIArray<ObRawExpr*>& get_real_param_exprs() const
  {
    return real_param_exprs_;
  }
  inline common::ObIArray<ObRawExpr*>& get_real_param_exprs_for_update()
  {
    return real_param_exprs_;
  }
  virtual int do_visit(ObRawExprVisitor& visitor) override;
  inline ObRawExpr*& get_push_down_sum_expr()
  {
    return push_down_sum_expr_;
  }
  inline ObRawExpr*& get_push_down_count_expr()
  {
    return push_down_count_expr_;
  }
  inline ObRawExpr*& get_push_down_synopsis_expr()
  {
    return push_down_synopsis_expr_;
  }
  inline ObRawExpr* get_separator_param_expr() const
  {
    return separator_param_expr_;
  }
  inline ObRawExpr* get_linear_inter_expr() const
  {
    return linear_inter_expr_;
  }
  inline const common::ObIArray<OrderItem>& get_order_items() const
  {
    return order_items_;
  }
  inline common::ObIArray<OrderItem>& get_order_items_for_update()
  {
    return order_items_;
  }

  virtual uint64_t hash_internal(uint64_t seed) const
  {
    for (int64_t i = 0; i < real_param_exprs_.count(); ++i) {
      if (OB_LIKELY(NULL != real_param_exprs_.at(i))) {
        seed = common::do_hash(*real_param_exprs_.at(i), seed);
      }
    }
    seed = common::do_hash(distinct_, seed);
    for (int64_t i = 0; i < order_items_.count(); ++i) {
      seed = common::do_hash(order_items_.at(i), seed);
    }
    if (NULL != separator_param_expr_) {
      seed = common::do_hash(*separator_param_expr_, seed);
    }
    return seed;
  }

  // set udf meta to this expr
  int set_udf_meta(const share::schema::ObUDF& udf);
  const share::schema::ObUDFMeta get_udf_meta()
  {
    return udf_meta_;
  }

  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  const char* get_name_dblink(ObItemType expr_type) const;
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_,
      K_(expr_level), K_(expr_levels), N_CHILDREN, real_param_exprs_, N_DISTINCT, distinct_, N_ORDER_BY, order_items_,
      N_SEPARATOR_PARAM_EXPR, separator_param_expr_, K_(udf_meta), N_LINEAR_INTER_EXPR, linear_inter_expr_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAggFunRawExpr);
  // real_param_exprs_.count() == 0 means '*'
  common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true> real_param_exprs_;
  ObRawExpr* push_down_sum_expr_;
  ObRawExpr* push_down_count_expr_;
  ObRawExpr* push_down_synopsis_expr_;
  bool distinct_;
  // used for group_concat/rank/percent rank/dense rank/cume dist
  common::ObArray<OrderItem, common::ModulePageAllocator, true> order_items_;
  ObRawExpr* separator_param_expr_;
  // use for udf function info
  share::schema::ObUDFMeta udf_meta_;
  // a pre allocated expr used to compute linear interpolation
  // not contain any sharable expr, no need to deep copy
  ObRawExpr* linear_inter_expr_;
  bool is_nested_aggr_;
};

inline int ObAggFunRawExpr::add_real_param_expr(ObRawExpr* expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(NULL == expr)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = real_param_exprs_.push_back(expr);
  }
  return ret;
}

inline int ObAggFunRawExpr::replace_real_param_expr(int64_t index, ObRawExpr* expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= real_param_exprs_.count())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(NULL == expr)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ObRawExpr*& target_expr = real_param_exprs_.at(index);
    target_expr = expr;
  }
  return ret;
}

inline int ObAggFunRawExpr::replace_param_expr(int64_t index, ObRawExpr* expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= get_param_count())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(NULL == expr)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (index >= real_param_exprs_.count()) {
    ObRawExpr*& target_expr = order_items_.at(index - real_param_exprs_.count()).expr_;
    target_expr = expr;
  } else {
    ObRawExpr*& target_expr = real_param_exprs_.at(index);
    target_expr = expr;
  }
  return ret;
}

inline void ObAggFunRawExpr::set_push_down_sum_expr(ObRawExpr* expr)
{
  push_down_sum_expr_ = expr;
}
inline void ObAggFunRawExpr::set_push_down_count_expr(ObRawExpr* expr)
{
  push_down_count_expr_ = expr;
}
inline bool ObAggFunRawExpr::is_param_distinct() const
{
  return distinct_;
}
inline bool ObAggFunRawExpr::is_nested_aggr() const
{
  return is_nested_aggr_;
}
inline bool ObAggFunRawExpr::contain_nested_aggr() const
{
  bool ret = false;
  for (int64 i = 0; !ret && i < get_param_count(); i++) {
    if (get_param_expr(i)->has_flag(CNT_AGG)) {
      ret = true;
    } else { /*do nothing.*/
    }
  }
  return ret;
}
inline void ObAggFunRawExpr::set_param_distinct(bool is_distinct)
{
  distinct_ = is_distinct;
}
inline void ObAggFunRawExpr::set_in_nested_aggr(bool is_nested)
{
  is_nested_aggr_ = is_nested;
}
inline void ObAggFunRawExpr::set_separator_param_expr(ObRawExpr* separator_param_expr)
{
  separator_param_expr_ = separator_param_expr;
}
inline void ObAggFunRawExpr::set_linear_inter_expr(ObRawExpr* linear_inter_expr)
{
  linear_inter_expr_ = linear_inter_expr;
}
inline int ObAggFunRawExpr::add_order_item(const OrderItem& order_item)
{
  return order_items_.push_back(order_item);
}

////////////////////////////////////////////////////////////////
// for normal system function, func_name_ is used to distinguish them.
// for special system function, ObRawExpr::type_ can be reset. Such function may not need name
class ObSysFunRawExpr : public ObOpRawExpr {
public:
  ObSysFunRawExpr(common::ObIAllocator& alloc) : ObOpRawExpr(alloc), func_name_(), operator_id_(common::OB_INVALID_ID)
  {
    set_expr_class(ObExpr::EXPR_SYS_FUNC);
  }
  ObSysFunRawExpr() : ObOpRawExpr(), func_name_(), operator_id_(common::OB_INVALID_ID)
  {
    set_expr_class(ObExpr::EXPR_SYS_FUNC);
  }
  virtual ~ObSysFunRawExpr()
  {}
  virtual int assign(const ObSysFunRawExpr& other);
  virtual int deep_copy(ObRawExprFactory& expr_factory, const ObSysFunRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  void set_func_name(const common::ObString& name);
  const common::ObString& get_func_name() const;
  virtual void clear_child();
  int check_param_num();
  virtual ObExprOperator* get_op();
  virtual void reset();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int do_visit(ObRawExprVisitor& visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const
  {
    uint64_t hash_ret = common::do_hash(func_name_, seed);
    return hash_ret;
  }

  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  int get_cast_type_name(char* buf, int64_t buf_len, int64_t& pos) const;
  int get_column_conv_name(char* buf, int64_t buf_len, int64_t& pos, ExplainType type) const;
  void set_op_id(int64_t operator_id)
  {
    operator_id_ = operator_id;
  }
  int64_t get_op_id() const
  {
    return operator_id_;
  }

  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_,
      K_(expr_levels), N_FUNC, func_name_, N_CHILDREN, exprs_, K_(enum_set_values));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSysFunRawExpr);
  common::ObString func_name_;
  uint64_t operator_id_;
};

inline void ObSysFunRawExpr::set_func_name(const common::ObString& name)
{
  func_name_ = name;
}
inline const common::ObString& ObSysFunRawExpr::get_func_name() const
{
  return func_name_;
}

class ObSequenceRawExpr : public ObSysFunRawExpr {
public:
  ObSequenceRawExpr(common::ObIAllocator& alloc) : ObSysFunRawExpr(alloc), name_(), action_(), sequence_id_(0)
  {}
  ObSequenceRawExpr() : ObSysFunRawExpr(), name_(), action_(), sequence_id_(0)
  {}
  virtual ~ObSequenceRawExpr() = default;
  virtual int assign(const ObSequenceRawExpr& other);
  virtual int deep_copy(ObRawExprFactory& expr_factory, const ObSequenceRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  int set_sequence_meta(const common::ObString& name, const common::ObString& action, uint64_t sequence_id);
  const common::ObString& get_name()
  {
    return name_;
  }
  const common::ObString& get_action()
  {
    return action_;
  }
  uint64_t get_sequence_id() const
  {
    return sequence_id_;
  }
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const override;
  virtual int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const override;

private:
  common::ObString name_;    // sequence object name
  common::ObString action_;  // NEXTVAL or CURRVAL
  uint64_t sequence_id_;
};

class ObNormalDllUdfRawExpr : public ObSysFunRawExpr {
public:
  ObNormalDllUdfRawExpr(common::ObIAllocator& alloc) : ObSysFunRawExpr(alloc), udf_meta_(), udf_attributes_()
  {}
  ObNormalDllUdfRawExpr() : ObSysFunRawExpr(), udf_meta_(), udf_attributes_()
  {}
  virtual ~ObNormalDllUdfRawExpr()
  {}
  virtual int assign(const ObNormalDllUdfRawExpr& other);
  virtual int deep_copy(ObRawExprFactory& expr_factory, const ObNormalDllUdfRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  int set_udf_meta(const share::schema::ObUDF& udf);
  int add_udf_attribute_name(const common::ObString& name);
  int add_udf_attribute(const ObRawExpr* expr, const ParseNode* node);
  const share::schema::ObUDFMeta& get_udf_meta() const
  {
    return udf_meta_;
  }
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const override;

private:
  // for udf function info
  share::schema::ObUDFMeta udf_meta_;
  common::ObSEArray<common::ObString, 16> udf_attributes_;  // name of input expr
};

class ObPLSQLCodeSQLErrmRawExpr : public ObSysFunRawExpr {
public:
  ObPLSQLCodeSQLErrmRawExpr(common::ObIAllocator& alloc) : ObSysFunRawExpr(alloc), is_sqlcode_(true)
  {}
  ObPLSQLCodeSQLErrmRawExpr() : ObSysFunRawExpr(), is_sqlcode_(true)
  {}
  virtual ~ObPLSQLCodeSQLErrmRawExpr()
  {}
  virtual int assign(const ObPLSQLCodeSQLErrmRawExpr& other);
  virtual int deep_copy(ObRawExprFactory& expr_factory, const ObPLSQLCodeSQLErrmRawExpr& other,
      const uint64_t copy_types, bool use_new_allocator = false);
  virtual ObExprOperator* get_op() override;
  void set_is_sqlcode(bool is_sqlcode)
  {
    is_sqlcode_ = is_sqlcode;
  }
  bool get_is_sqlcode()
  {
    return is_sqlcode_;
  }
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_,
      K_(expr_level), K_(expr_levels), K_(is_sqlcode), N_CHILDREN, exprs_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPLSQLCodeSQLErrmRawExpr);

private:
  bool is_sqlcode_;
};

class ObObjAccessRawExpr : public ObOpRawExpr {
public:
  ObObjAccessRawExpr(common::ObIAllocator& alloc)
      : ObOpRawExpr(alloc), get_attr_func_(0), func_name_(), var_indexs_(), for_write_(false)
  {}
  ObObjAccessRawExpr() : ObOpRawExpr(), get_attr_func_(0), func_name_(), var_indexs_(), for_write_(false)
  {}
  virtual ~ObObjAccessRawExpr()
  {}
  int assign(const ObObjAccessRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObObjAccessRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const override;

  const common::ObIArray<int64_t>& get_var_indexs() const
  {
    return var_indexs_;
  }
  void set_get_attr_func_addr(uint64_t get_attr_func)
  {
    get_attr_func_ = get_attr_func;
  }
  uint64_t get_get_attr_func_addr() const
  {
    return get_attr_func_;
  }
  void set_func_name(const common::ObString& func_name)
  {
    func_name_ = func_name;
  }
  const common::ObString& get_func_name() const
  {
    return func_name_;
  }
  bool for_write() const
  {
    return for_write_;
  }
  void set_write(bool for_write)
  {
    for_write_ = for_write;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObObjAccessRawExpr);
  uint64_t get_attr_func_;
  common::ObString func_name_;
  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> var_indexs_;
  bool for_write_;
};

enum ObMultiSetType {
  MULTISET_TYPE_INVALID = -1,
  MULTISET_TYPE_UNION,
  MULTISET_TYPE_INTERSECT,
  MULTISET_TYPE_EXCEPT,
  MULTISET_TYPE_SUBMULTISET,
  MULTISET_TYPE_MEMBER_OF,
  MULTISET_TYPE_IS_SET,
  MULTISET_TYPE_EMPTY,
};

enum ObMultiSetModifier {
  MULTISET_MODIFIER_INVALID = -1,
  MULTISET_MODIFIER_ALL,
  MULTISET_MODIFIER_DISTINCT,
  MULTISET_MODIFIER_NOT,
};

class ObMultiSetRawExpr : public ObOpRawExpr {
public:
  ObMultiSetRawExpr(common::ObIAllocator& alloc)
      : ObOpRawExpr(alloc),
        ms_modifier_(ObMultiSetModifier::MULTISET_MODIFIER_INVALID),
        ms_type_(ObMultiSetType::MULTISET_TYPE_INVALID)
  {}
  ObMultiSetRawExpr()
      : ObOpRawExpr(),
        ms_modifier_(ObMultiSetModifier::MULTISET_MODIFIER_INVALID),
        ms_type_(ObMultiSetType::MULTISET_TYPE_INVALID)
  {}

  virtual ~ObMultiSetRawExpr()
  {}

  int assign(const ObMultiSetRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObMultiSetRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const override;

  inline ObMultiSetModifier get_multiset_modifier() const
  {
    return ms_modifier_;
  }
  inline ObMultiSetType get_multiset_type() const
  {
    return ms_type_;
  }

  void set_multiset_modifier(ObMultiSetModifier modifier)
  {
    ms_modifier_ = modifier;
  }
  void set_multiset_type(ObMultiSetType type)
  {
    ms_type_ = type;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiSetRawExpr);
  ObMultiSetModifier ms_modifier_;
  ObMultiSetType ms_type_;
};

class ObCollPredRawExpr : public ObMultiSetRawExpr {
public:
  ObCollPredRawExpr(common::ObIAllocator& alloc) : ObMultiSetRawExpr(alloc)
  {}
  virtual ~ObCollPredRawExpr()
  {}

  int assign(const ObCollPredRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObCollPredRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCollPredRawExpr);
};

class ObFunMatchAgainst : public ObNonTerminalRawExpr {
public:
  ObFunMatchAgainst()
      : ObNonTerminalRawExpr(),
        mode_flag_(NATURAL_LANGUAGE_MODE),
        match_columns_(NULL),
        real_column_(NULL),
        search_key_(NULL),
        /*search_tree_(NULL)*/
        fulltext_filter_(NULL)
  {
    set_expr_class(ObExpr::EXPR_DOMAIN_INDEX);
  }
  ObFunMatchAgainst(common::ObIAllocator& alloc)
      : ObNonTerminalRawExpr(alloc),
        mode_flag_(NATURAL_LANGUAGE_MODE),
        match_columns_(NULL),
        real_column_(NULL),
        search_key_(NULL),
        /*search_tree_(NULL)*/
        fulltext_filter_(NULL)
  {
    set_expr_class(ObExpr::EXPR_DOMAIN_INDEX);
  }

  virtual ~ObFunMatchAgainst()
  {}
  virtual int do_visit(ObRawExprVisitor& visitor) override;
  virtual uint64_t hash_internal(uint64_t seed) const;
  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;
  inline void set_mode_flag(ObMatchAgainstMode mode_flag)
  {
    mode_flag_ = mode_flag;
  }
  inline ObMatchAgainstMode get_mode_flag() const
  {
    return mode_flag_;
  }
  inline void set_match_columns(ObRawExpr* match_columns)
  {
    match_columns_ = match_columns;
  }
  inline const ObRawExpr* get_match_columns() const
  {
    return match_columns_;
  }
  inline ObRawExpr* get_match_columns()
  {
    return match_columns_;
  }
  inline void set_search_key(ObRawExpr* search_key)
  {
    search_key_ = search_key;
  }
  inline const ObRawExpr* get_search_key() const
  {
    return search_key_;
  }
  inline ObRawExpr* get_search_key()
  {
    return search_key_;
  }
  inline void set_real_column(ObColumnRefRawExpr* real_column)
  {
    real_column_ = real_column;
  }
  inline const ObColumnRefRawExpr* get_real_column() const
  {
    return real_column_;
  }
  //  inline void set_search_tree(ObRawExpr *search_tree) { search_tree_ = search_tree; }
  //  inline const ObRawExpr *get_search_tree() const { return search_tree_; }
  inline void set_fulltext_filter(ObRawExpr* fulltext_key)
  {
    fulltext_filter_ = fulltext_key;
  }
  inline const ObRawExpr* get_fulltext_filter() const
  {
    return fulltext_filter_;
  }
  inline ObRawExpr* get_fulltext_filter()
  {
    return fulltext_filter_;
  }
  int64_t get_param_count() const
  {
    return 2;
  }
  const ObRawExpr* get_param_expr(int64_t index) const;
  ObRawExpr*& get_param_expr(int64_t index);
  void clear_child();
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  VIRTUAL_TO_STRING_KV(
      N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_, K_(mode_flag));

private:
  DISALLOW_COPY_AND_ASSIGN(ObFunMatchAgainst);
  ObMatchAgainstMode mode_flag_;
  ObRawExpr* match_columns_;
  ObColumnRefRawExpr* real_column_;
  ObRawExpr* search_key_;
  ObRawExpr* fulltext_filter_;
};

class ObSetIterRawExpr : public ObNonTerminalRawExpr {
public:
  ObSetIterRawExpr() : ObNonTerminalRawExpr(), left_iter_(NULL), right_iter_(NULL)
  {
    set_expr_class(ObExpr::EXPR_DOMAIN_INDEX);
  }
  ObSetIterRawExpr(common::ObIAllocator& alloc) : ObNonTerminalRawExpr(alloc), left_iter_(NULL), right_iter_(NULL)
  {
    set_expr_class(ObExpr::EXPR_DOMAIN_INDEX);
  }

  virtual ~ObSetIterRawExpr()
  {}
  virtual int do_visit(ObRawExprVisitor& visitor) override;
  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  int64_t get_param_count() const
  {
    return 2;
  }
  const ObRawExpr* get_param_expr(int64_t index) const;
  ObRawExpr*& get_param_expr(int64_t index);
  void set_left_expr(ObRawExpr* left_iter)
  {
    left_iter_ = left_iter;
  }
  void set_right_expr(ObRawExpr* right_iter)
  {
    right_iter_ = right_iter;
  }
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const
  {
    UNUSED(expr);
    UNUSED(check_context);
    return false;
  }
  inline void clear_child()
  {
    left_iter_ = NULL;
    right_iter_ = NULL;
  }
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSetIterRawExpr);
  ObRawExpr* left_iter_;
  ObRawExpr* right_iter_;
};

class ObRowIterRawExpr : public ObTerminalRawExpr {
public:
  ObRowIterRawExpr() : ObTerminalRawExpr(), iter_idx_(common::OB_INVALID_INDEX)
  {
    set_expr_class(ObExpr::EXPR_DOMAIN_INDEX);
  }
  ObRowIterRawExpr(common::ObIAllocator& alloc) : ObTerminalRawExpr(alloc), iter_idx_(common::OB_INVALID_INDEX)
  {
    set_expr_class(ObExpr::EXPR_DOMAIN_INDEX);
  }

  virtual ~ObRowIterRawExpr()
  {}
  virtual int do_visit(ObRawExprVisitor& visitor) override;
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const
  {
    UNUSED(expr);
    UNUSED(check_context);
    return false;
  }
  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  inline void set_iter_idx(int64_t iter_idx)
  {
    iter_idx_ = iter_idx;
  }
  inline int64_t get_iter_idx() const
  {
    return iter_idx_;
  }
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_, K_(iter_idx));

private:
  DISALLOW_COPY_AND_ASSIGN(ObRowIterRawExpr);
  int64_t iter_idx_;
};

////////////////////////////////////////////////////////////////
// eg :
//  partition by class order by score rows between 5 preceding and 5 following : WINDOW_ROWS
//  partition by class order by score range between 5 preceding and 5 following : WINDOW_RANGE
// new
enum WindowType {
  WINDOW_ROWS,
  WINDOW_RANGE,
  WINDOW_MAX,
};
enum BoundType {
  BOUND_UNBOUNDED,
  BOUND_CURRENT_ROW,
  BOUND_INTERVAL,
};

enum BoundExprIdx { BOUND_EXPR_ADD = 0, BOUND_EXPR_SUB, BOUND_EXPR_MAX };

struct Bound {
  OB_UNIS_VERSION_V(1);

public:
  Bound()
      : type_(BOUND_UNBOUNDED),
        is_preceding_(false),
        is_nmb_literal_(false),
        interval_expr_(NULL),
        date_unit_expr_(NULL)
  {
    MEMSET(exprs_, 0, sizeof(ObRawExpr*) * BOUND_EXPR_MAX);
  }

  int deep_copy(
      ObRawExprFactory& expr_factory, const Bound& other, const uint64_t copy_types, bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);

  BoundType type_;
  bool is_preceding_;
  bool is_nmb_literal_;
  ObRawExpr* interval_expr_;
  ObRawExpr* date_unit_expr_;
  ;

  ObRawExpr* exprs_[BOUND_EXPR_MAX];
  TO_STRING_KV(K_(type), K_(is_preceding), K_(is_nmb_literal), KP_(interval_expr), K_(date_unit_expr));
};

////////////////////////////////////////////////////////////////
struct ObFrame {
public:
  ObFrame() : win_type_(WINDOW_MAX), is_between_(false)
  {}
  inline void set_window_type(WindowType win_type)
  {
    win_type_ = win_type;
  }
  inline void set_is_between(bool is_between)
  {
    is_between_ = is_between;
  }
  inline void set_upper(const Bound& upper)
  {
    upper_ = upper;
  }
  inline void set_lower(const Bound& lower)
  {
    lower_ = lower;
  }
  inline WindowType get_window_type()
  {
    return win_type_;
  }
  inline bool is_between()
  {
    return is_between_;
  }
  inline Bound& get_upper()
  {
    return upper_;
  }
  inline Bound& get_lower()
  {
    return lower_;
  }

  int assign(const ObFrame& other);

  WindowType win_type_;
  bool is_between_;
  Bound upper_;
  Bound lower_;
};

struct ObWindow : public ObFrame {
public:
  ObWindow() : has_frame_orig_(false)
  {
    partition_exprs_.set_label(common::ObModIds::OB_SQL_WINDOW_FUNC);
    order_items_.set_label(common::ObModIds::OB_SQL_WINDOW_FUNC);
  }
  inline int set_partition_exprs(const common::ObIArray<ObRawExpr*>& exprs)
  {
    return partition_exprs_.assign(exprs);
  }
  inline int set_order_items(const common::ObIArray<OrderItem>& items)
  {
    return order_items_.assign(items);
  }
  inline void set_win_name(common::ObString& win_name)
  {
    win_name_ = win_name;
  }
  inline void set_has_frame_orig(int64_t has_frame_orig)
  {
    has_frame_orig_ = has_frame_orig;
  }
  inline common::ObString& get_win_name()
  {
    return win_name_;
  }
  inline bool has_frame_orig()
  {
    return has_frame_orig_;
  }
  inline bool has_order_items()
  {
    return order_items_.count() > 0;
  }
  inline const common::ObIArray<ObRawExpr*>& get_partition_exprs() const
  {
    return partition_exprs_;
  }
  inline common::ObIArray<ObRawExpr*>& get_partition_exprs()
  {
    return partition_exprs_;
  }
  inline const common::ObIArray<OrderItem>& get_order_items() const
  {
    return order_items_;
  }
  inline common::ObIArray<OrderItem>& get_order_items()
  {
    return order_items_;
  }

  int assign(const ObWindow& other);

  common::ObArray<ObRawExpr*, common::ModulePageAllocator, true> partition_exprs_;
  common::ObArray<OrderItem, common::ModulePageAllocator, true> order_items_;
  // used in resolver
  common::ObString win_name_;
  bool has_frame_orig_;
};

class ObWinFunRawExpr : public ObRawExpr, public ObWindow {
public:
  ObWinFunRawExpr()
      : ObRawExpr(),
        ObWindow(),
        func_type_(T_MAX),
        is_distinct_(false),
        is_ignore_null_(false),
        is_from_first_(false),
        agg_expr_(NULL)
  {
    set_expr_class(ObExpr::EXPR_WINDOW);
  }
  ObWinFunRawExpr(common::ObIAllocator& alloc)
      : ObRawExpr(alloc),
        ObWindow(),
        func_type_(T_MAX),
        is_distinct_(false),
        is_ignore_null_(false),
        is_from_first_(false),
        agg_expr_(NULL)
  {
    set_expr_class(ObExpr::EXPR_WINDOW);
  }
  virtual ~ObWinFunRawExpr()
  {}

  int assign(const ObWinFunRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObWinFunRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  int replace_param_expr(int64_t partition_expr_index, ObRawExpr* expr);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  inline void set_func_type(ObItemType func_type)
  {
    func_type_ = func_type;
  }
  inline void set_is_distinct(bool is_distinct)
  {
    is_distinct_ = is_distinct;
  }
  inline void set_is_ignore_null(bool is_ignore_null)
  {
    is_ignore_null_ = is_ignore_null;
  }
  inline void set_is_from_first(bool is_from_first)
  {
    is_from_first_ = is_from_first;
  }
  inline int set_func_params(const common::ObIArray<ObRawExpr*>& params)
  {
    return func_params_.assign(params);
  }
  inline void set_agg_expr(ObAggFunRawExpr* agg_expr)
  {
    agg_expr_ = agg_expr;
  }
  inline ObItemType get_func_type() const
  {
    return func_type_;
  }
  inline bool is_distinct()
  {
    return is_distinct_;
  }
  inline bool is_ignore_null()
  {
    return is_ignore_null_;
  }
  inline bool is_from_first()
  {
    return is_from_first_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_func_params() const
  {
    return func_params_;
  }
  inline common::ObIArray<ObRawExpr*>& get_func_params()
  {
    return func_params_;
  }
  inline ObAggFunRawExpr* get_agg_expr()
  {
    return agg_expr_;
  }
  inline ObAggFunRawExpr* get_agg_expr() const
  {
    return agg_expr_;
  }

  virtual void clear_child();
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int64_t get_param_count() const
  {
    int64_t cnt = (agg_expr_ != NULL ? agg_expr_->get_param_count() : 0) + func_params_.count() +
                  partition_exprs_.count() + order_items_.count() + (upper_.interval_expr_ != NULL ? 1 : 0) +
                  (lower_.interval_expr_ != NULL ? 1 : 0);
    for (int64_t i = 0; i < 2; ++i) {
      const Bound* bound = 0 == i ? &upper_ : &lower_;
      for (int64_t j = 0; j < BOUND_EXPR_MAX; ++j) {
        if (NULL != bound->exprs_[j]) {
          cnt++;
        }
      }
    }
    return cnt;
  }
  virtual const ObRawExpr* get_param_expr(int64_t index) const;
  virtual ObRawExpr*& get_param_expr(int64_t index);
  virtual int do_visit(ObRawExprVisitor& visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;

  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_,
      K_(expr_level), K_(expr_levels), K_(func_type), K_(is_distinct), K_(func_params), K_(partition_exprs),
      K_(order_items), K_(win_type), K_(is_between), K_(upper), K_(lower), KPC_(agg_expr));

public:
  common::ObString sort_str_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObWinFunRawExpr);
  ObItemType func_type_;
  bool is_distinct_;
  bool is_ignore_null_;
  bool is_from_first_;
  common::ObArray<ObRawExpr*, common::ModulePageAllocator, true> func_params_;
  ObAggFunRawExpr* agg_expr_;
};

////////////////////////////////////////////////////////////////
class ObPseudoColumnRawExpr : public ObTerminalRawExpr {
public:
  ObPseudoColumnRawExpr() : ObTerminalRawExpr(), table_id_(common::OB_INVALID_ID)
  {
    set_expr_class(ObExpr::EXPR_PSEUDO_COLUMN);
  }
  ObPseudoColumnRawExpr(common::ObIAllocator& alloc) : ObTerminalRawExpr(alloc), table_id_(common::OB_INVALID_ID)
  {
    set_expr_class(ObExpr::EXPR_PSEUDO_COLUMN);
  }
  virtual ~ObPseudoColumnRawExpr(){};
  int assign(const ObPseudoColumnRawExpr& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObPseudoColumnRawExpr& other, const uint64_t copy_types,
      bool use_new_allocator = false);
  virtual int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);
  virtual bool same_as(const ObRawExpr& expr, ObExprEqualCheckContext* check_context = NULL) const;

  virtual int do_visit(ObRawExprVisitor& visitor) override;
  virtual uint64_t hash_internal(uint64_t seed) const;
  int get_name_internal(char* buf, const int64_t buf_len, int64_t& pos, ExplainType type) const;
  bool is_hierarchical_query_type() const
  {
    return type_ == T_LEVEL || type_ == T_CONNECT_BY_ISCYCLE || type_ == T_CONNECT_BY_ISLEAF;
  }
  bool is_cte_query_type() const
  {
    return T_CTE_SEARCH_COLUMN == type_ || T_CTE_CYCLE_COLUMN == type_;
  }
  void set_cte_cycle_value(ObRawExpr* v, ObRawExpr* d_v)
  {
    cte_cycle_value_ = v;
    cte_cycle_default_value_ = d_v;
  };
  void get_cte_cycle_value(ObRawExpr*& v, ObRawExpr*& d_v)
  {
    v = cte_cycle_value_;
    d_v = cte_cycle_default_value_;
  };
  void set_table_id(int64_t table_id)
  {
    table_id_ = table_id;
  }
  int64_t get_table_id() const
  {
    return table_id_;
  }

  VIRTUAL_TO_STRING_KV(
      N_ITEM_TYPE, type_, N_RESULT_TYPE, result_type_, N_EXPR_INFO, info_, N_REL_ID, rel_ids_, N_TABLE_ID, table_id_);

private:
  ObRawExpr* cte_cycle_value_;
  ObRawExpr* cte_cycle_default_value_;
  int64_t table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPseudoColumnRawExpr);
};
/// visitor interface
class ObRawExprVisitor {
public:
  ObRawExprVisitor()
  {}
  virtual ~ObRawExprVisitor()
  {}

  // OP types: constants, ? etc.
  virtual int visit(ObConstRawExpr& expr) = 0;
  // OP types: ObObjtypes
  virtual int visit(ObVarRawExpr& expr) = 0;
  // OP types: subquery, cell index
  virtual int visit(ObQueryRefRawExpr& expr) = 0;
  // OP types: identify, table.column
  virtual int visit(ObColumnRefRawExpr& expr) = 0;
  // unary OP types: exists, not, negative, positive
  // binary OP types: +, -, *, /, >, <, =, <=>, IS, IN etc.
  // triple OP types: like, not like, btw, not btw
  // multi OP types: and, or, ROW
  virtual int visit(ObOpRawExpr& expr) = 0;
  // OP types: case, arg case
  virtual int visit(ObCaseOpRawExpr& expr) = 0;
  // OP types: aggregate functions e.g. max, min, avg, count, sum
  virtual int visit(ObAggFunRawExpr& expr) = 0;
  // OP types: system functions
  virtual int visit(ObSysFunRawExpr& expr) = 0;
  virtual int visit(ObSetOpRawExpr& expr) = 0;
  virtual int visit(ObAliasRefRawExpr& expr)
  {
    UNUSED(expr);
    return common::OB_SUCCESS;
  }
  virtual int visit(ObFunMatchAgainst& expr)
  {
    UNUSED(expr);
    return common::OB_SUCCESS;
  }
  virtual int visit(ObSetIterRawExpr& expr)
  {
    UNUSED(expr);
    return common::OB_SUCCESS;
  }
  virtual int visit(ObRowIterRawExpr& expr)
  {
    UNUSED(expr);
    return common::OB_SUCCESS;
  }
  virtual int visit(ObWinFunRawExpr& expr)
  {
    UNUSED(expr);
    return common::OB_SUCCESS;
  }
  virtual int visit(ObPseudoColumnRawExpr& expr)
  {
    UNUSED(expr);
    return common::OB_SUCCESS;
  }
  virtual bool skip_child(ObRawExpr& expr)
  {
    UNUSED(expr);
    return false;
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprVisitor);
};

class ObRawExprFactory {
public:
  explicit ObRawExprFactory(common::ObIAllocator& alloc) : allocator_(alloc), expr_store_(alloc)
  {}
  ~ObRawExprFactory()
  {
    if (!THIS_WORKER.has_req_flag()) {
      destory();
    }
  }
  //~ObRawExprFactory() { }

  template <typename ExprType>
  inline int create_raw_expr(ObItemType expr_type, ExprType*& raw_expr)
  {
    int ret = common::OB_SUCCESS;
    void* ptr = allocator_.alloc(sizeof(ExprType));
    raw_expr = NULL;
    if (OB_UNLIKELY(NULL == ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "no more memory to create raw expr");
    } else {
      raw_expr = new (ptr) ExprType(allocator_);
      raw_expr->set_allocator(allocator_);
      raw_expr->set_expr_factory(*this);
      raw_expr->set_expr_type(expr_type);
      if (OB_FAIL(expr_store_.store_obj(raw_expr))) {
        SQL_RESV_LOG(WARN, "store raw expr failed", K(ret));
        raw_expr->~ExprType();
        raw_expr = NULL;
      } else {
        SQL_RESV_LOG(DEBUG, "create_raw_expr", K(expr_type), "expr_type", get_type_name(expr_type), K(lbt()));
      }
    }
    return ret;
  }

  inline void destory()
  {
    DLIST_FOREACH_NORET(node, expr_store_.get_obj_list())
    {
      if (node != NULL && node->get_obj() != NULL) {
        node->get_obj()->~ObRawExpr();
      }
    }
    expr_store_.destory();
  }

  inline common::ObIAllocator& get_allocator()
  {
    return allocator_;
  }
  TO_STRING_KV("", "");

private:
  common::ObIAllocator& allocator_;
  common::ObObjStore<ObRawExpr*, common::ObIAllocator&, true> expr_store_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRawExprFactory);
};

class ObRawExprPointer {
public:
  ObRawExprPointer();

  virtual ~ObRawExprPointer();
  int get(ObRawExpr*& expr);
  int set(ObRawExpr* expr);
  int add_ref(ObRawExpr** expr);
  TO_STRING_KV("", "");

private:
  common::ObSEArray<ObRawExpr**, 1> expr_group_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_RESOLVER_EXPR_RAW_EXPR_
