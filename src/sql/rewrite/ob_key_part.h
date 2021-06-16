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

#ifndef OCEANBASE_SQL_REWRITE_OB_KEY_PART_
#define OCEANBASE_SQL_REWRITE_OB_KEY_PART_

#include "share/ob_define.h"
#include "lib/objectpool/ob_tc_factory.h"
#include "lib/container/ob_array_serialization.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "sql/engine/expr/ob_expr_res_type.h"

namespace oceanbase {
namespace sql {
class ObQueryRange;

enum ObKeyPartType { T_NORMAL_KEY = 0, T_LIKE_KEY };

class ObKeyPartId {
  OB_UNIS_VERSION(1);

public:
  ObKeyPartId(uint64_t data_table_id = common::OB_INVALID_ID, uint64_t column_id = common::OB_INVALID_ID)
      : table_id_(data_table_id), column_id_(column_id)
  {}

  inline uint64_t hash() const
  {
    uint64_t hash_code = 0;
    hash_code = common::murmurhash(&table_id_, sizeof(table_id_), hash_code);
    hash_code = common::murmurhash(&column_id_, sizeof(column_id_), hash_code);
    return hash_code;
  }

  inline bool operator==(const ObKeyPartId& other) const
  {
    return (table_id_ == other.table_id_) && (column_id_ == other.column_id_);
  }

  inline bool operator!=(const ObKeyPartId& other) const
  {
    return !(*this == other);
  }

  TO_STRING_KV(N_TID, table_id_, N_CID, column_id_);
  uint64_t table_id_;
  uint64_t column_id_;
};

class ObKeyPartPos {
  OB_UNIS_VERSION(1);

public:
  ObKeyPartPos(common::ObIAllocator& alloc, int64_t offset = -1)
      : offset_(offset), column_type_(alloc), enum_set_values_()
  {}

  ObKeyPartPos(int64_t offset, ObExprResType type) : offset_(offset), column_type_(type), enum_set_values_()
  {}

  inline bool operator==(const ObKeyPartPos& other) const
  {
    bool is_equal = true;
    is_equal = ((offset_ == other.offset_) && (column_type_ == other.column_type_) &&
                (enum_set_values_.count() == other.enum_set_values_.count()));
    for (int64_t i = 0; is_equal && i < enum_set_values_.count(); ++i) {
      is_equal = enum_set_values_.at(i) == other.enum_set_values_.at(i);
    }
    return is_equal;
  }

  inline bool operator!=(const ObKeyPartPos& other) const
  {
    return !(*this == other);
  }
  int set_enum_set_values(common::ObIAllocator& allocator, const common::ObIArray<common::ObString>& enum_set_values);
  inline const common::ObIArray<common::ObString>& get_enum_set_values() const
  {
    return enum_set_values_;
  }
  TO_STRING_KV(N_OFFSET, offset_, N_COLUMN_TYPE, column_type_, N_ENUM_SET_VALUES, enum_set_values_);

  int64_t offset_;
  ObExprResType column_type_;
  common::ObSArray<common::ObString> enum_set_values_;
};

struct ObNormalKeyPart {
  ObNormalKeyPart()
      : start_(), include_start_(false), end_(), include_end_(false), always_true_(false), always_false_(false)
  {}
  bool operator==(const ObNormalKeyPart& other) const
  {
    return (start_ == other.start_) && (end_ == other.end_) && (include_start_ == other.include_start_) &&
           (include_end_ == other.include_end_) && (always_false_ == other.always_false_) &&
           (always_true_ == other.always_true_);
  }
  // normal key type
  common::ObObj start_;
  bool include_start_;
  common::ObObj end_;
  bool include_end_;
  bool always_true_;
  bool always_false_;
};

struct ObLikeKeyPart {
  common::ObObj pattern_;
  common::ObObj escape_;
};

class ObKeyPart : public common::ObDLinkBase<ObKeyPart> {
  OB_UNIS_VERSION_V(1);

public:
  ObKeyPart(common::ObIAllocator& allocator, uint64_t data_table_id = common::OB_INVALID_ID,
      uint64_t column_id = common::OB_INVALID_ID, int32_t offset = -1)
      : allocator_(allocator),
        pos_(allocator, offset),
        null_safe_(false),
        key_type_(T_NORMAL_KEY),
        normal_keypart_(),
        item_next_(NULL),
        or_next_(NULL),
        and_next_(NULL)
  {
    id_.table_id_ = data_table_id;
    id_.column_id_ = column_id;
  }

  inline bool operator<=(const ObKeyPart& other) const
  {
    return pos_.offset_ <= other.pos_.offset_;
  }
  inline bool operator>(const ObKeyPart& other) const;

  bool has_or()
  {
    return NULL != or_next_;
  }

  bool has_and()
  {
    return NULL != and_next_;
  }

  bool has_intersect(const ObKeyPart* other) const;
  bool can_union(const ObKeyPart* other) const;
  bool equal_to(const ObKeyPart* other);

  bool normal_key_is_equal(const ObKeyPart* other)
  {
    bool ret = false;
    if (other != NULL) {
      ret = (!is_question_mark() && !(other->is_question_mark())) && (other->is_normal_key()) && (is_normal_key()) &&
            (id_ == other->id_) && (pos_ == other->pos_) &&
            (normal_keypart_->start_ == other->normal_keypart_->start_) &&
            (normal_keypart_->end_ == other->normal_keypart_->end_) &&
            (normal_keypart_->include_start_ == other->normal_keypart_->include_start_) &&
            (normal_keypart_->include_end_ == other->normal_keypart_->include_end_) && (NULL == item_next_) &&
            (NULL == other->item_next_);
    }
    return ret;
  }

  int intersect(ObKeyPart* other, bool contain_row);
  void link_gt(ObKeyPart* and_next);

  void cut_and_next_ptr()
  {
    ObKeyPart* cur = this;
    while (NULL != cur) {
      cur->and_next_ = NULL;
      cur = cur->or_next_;
    }
  }

  inline bool is_question_mark() const
  {
    bool bret = false;
    if (is_like_key()) {
      bret = like_keypart_->pattern_.is_unknown() || like_keypart_->escape_.is_unknown();
    } else if (is_normal_key()) {
      bret = normal_keypart_->start_.is_unknown() || normal_keypart_->end_.is_unknown();
    }
    return bret;
  }

  bool is_equal_condition() const
  {
    bool bret = false;
    for (const ObKeyPart* cur_key = this; !bret && cur_key != NULL; cur_key = cur_key->item_next_) {
      if (cur_key->is_always_false() || cur_key->is_always_true()) {
        bret = false;
      } else if (cur_key->is_normal_key()) {
        if (!cur_key->normal_keypart_->include_end_ || !cur_key->normal_keypart_->include_start_) {
          bret = false;
        } else if (cur_key->normal_keypart_->start_.get_type() != cur_key->normal_keypart_->end_.get_type()) {
          bret = false;
        } else if (cur_key->normal_keypart_->start_.is_unknown() && cur_key->normal_keypart_->end_.is_unknown()) {
          bret = (cur_key->normal_keypart_->start_.get_unknown() == cur_key->normal_keypart_->end_.get_unknown());
        } else if (0 == cur_key->normal_keypart_->start_.compare(cur_key->normal_keypart_->end_)) {
          bret = true;
        }
      }
    }
    return bret;
  }
  bool is_range_condition() const
  {
    bool bret = false;
    for (const ObKeyPart* cur_key = this; !bret && cur_key != NULL; cur_key = cur_key->item_next_) {
      if (cur_key->is_always_false() || cur_key->is_always_true()) {
        bret = false;
      } else if (cur_key->is_normal_key()) {
        bret = true;
      }
    }
    return bret;
  }
  bool is_always_false_condition() const
  {
    bool bret = false;
    for (const ObKeyPart* cur_key = this; !bret && cur_key != NULL; cur_key = cur_key->item_next_) {
      if (cur_key->is_always_false()) {
        bret = true;
      }
    }
    return bret;
  }
  void set_normal_start(ObKeyPart* other)
  {
    if (NULL != other && other->is_normal_key() && is_normal_key()) {
      this->id_ = other->id_;
      this->pos_ = other->pos_;
      this->normal_keypart_->start_ = other->normal_keypart_->start_;
      this->normal_keypart_->include_start_ = other->normal_keypart_->include_start_;
    }
  }
  void set_normal_end(ObKeyPart* other)
  {
    if (NULL != other && other->is_normal_key() && is_normal_key()) {
      this->id_ = other->id_;
      this->pos_ = other->pos_;
      this->normal_keypart_->end_ = other->normal_keypart_->end_;
      this->normal_keypart_->include_end_ = other->normal_keypart_->include_end_;
    }
  }
  inline bool is_always_true() const
  {
    return is_normal_key() && normal_keypart_->always_true_;
  }
  inline bool is_always_false() const
  {
    return is_normal_key() && normal_keypart_->always_false_;
  }
  inline bool is_normal_key() const
  {
    return T_NORMAL_KEY == key_type_ && normal_keypart_ != NULL;
  }
  inline bool is_like_key() const
  {
    return T_LIKE_KEY == key_type_ && like_keypart_ != NULL;
  }
  int create_normal_key();
  int create_like_key();
  inline ObNormalKeyPart* get_normal_key()
  {
    ObNormalKeyPart* normal_key = NULL;
    if (T_NORMAL_KEY == key_type_) {
      normal_key = normal_keypart_;
    }
    return normal_key;
  }
  inline ObLikeKeyPart* get_like_key()
  {
    ObLikeKeyPart* like_key = NULL;
    if (T_LIKE_KEY == key_type_) {
      like_key = like_keypart_;
    }
    return like_key;
  }
  int formalize_keypart(bool contain_row);
  int cast_value_type(const common::ObDataTypeCastParams& dtc_params, bool contain_row);
  virtual void reset();

  // copy all except next_ pointer
  int deep_node_copy(const ObKeyPart& other);
  ObKeyPart* general_or_next();
  ObKeyPart* cut_general_or_next();
  DECLARE_TO_STRING;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKeyPart);

public:
  common::ObIAllocator& allocator_;
  ObKeyPartId id_;
  ObKeyPartPos pos_;
  bool null_safe_;
  ObKeyPartType key_type_;
  union {
    // normal key type
    ObNormalKeyPart* normal_keypart_;
    // like expr type
    ObLikeKeyPart* like_keypart_;
  };
  // list member
  ObKeyPart* item_next_;
  ObKeyPart* or_next_;
  ObKeyPart* and_next_;
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_REWRITE_OB_KEY_PART_
