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

#include "lib/allocator/ob_malloc.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/serialization.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "common/rowkey/ob_rowkey_info.h"
#include "common/object/ob_object.h"
#include "common/object/ob_obj_type.h"
#include "common/object/ob_obj_compare.h"

namespace oceanbase
{
namespace common
{

using namespace lib;

ObObj ObRowkey::MIN_OBJECT = ObObj::make_min_obj();
ObObj ObRowkey::MAX_OBJECT = ObObj::make_max_obj();

ObRowkey ObRowkey::MIN_ROWKEY(&ObRowkey::MIN_OBJECT, 1);
ObRowkey ObRowkey::MAX_ROWKEY(&ObRowkey::MAX_OBJECT, 1);

int ObRowkey::to_store_rowkey(ObStoreRowkey &store_rowkey) const
{
  int ret = OB_SUCCESS;
  bool contain_min_max = false;

  for(int64_t i = 0; !contain_min_max && i < obj_cnt_; i++) {
    if (obj_ptr_[i].is_min_value() || obj_ptr_[i].is_max_value()) {
      contain_min_max = true;
    }
  }

  if (contain_min_max) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "cannot convert min/max ObRowkey to ObStoreRowkey!", K(ret), K(*this));
  } else if (OB_FAIL(store_rowkey.assign(obj_ptr_, obj_cnt_))) {
    COMMON_LOG(ERROR, "Failed to assign store rowkey", K(ret), K(*this));
  }

  return ret;
}

void ObRowkey::destroy(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(obj_ptr_)) {
    allocator.free(obj_ptr_);
    obj_ptr_ = nullptr;
  }
  obj_cnt_ = 0;
}

int ObRowkey::equal(const ObRowkey &rhs, bool &is_equal) const
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (obj_ptr_ == rhs.obj_ptr_) {
    is_equal = (obj_cnt_ == rhs.obj_cnt_);
  } else if (obj_cnt_ != rhs.obj_cnt_){
    is_equal = false;
  } else {
    ObObjTypeClass tc = ObMaxTC;
    for (int64_t i = 0; is_equal && i < obj_cnt_ && OB_SUCC(ret); ++i) {
      const ObObj &obj = obj_ptr_[i];
      const ObObj &rhs_obj = rhs.obj_ptr_[i];
      tc = obj.get_type_class();
      if (ObMaxTC <= tc) {
        COMMON_LOG(WARN, "invalid type class", K(tc));
        ret = OB_ERR_UNEXPECTED;
      } else if (tc != rhs_obj.get_type_class()) {
        is_equal = false;
      } else {
        switch(tc) {
        case ObNullTC:
          break;
        case ObIntTC:
        case ObUIntTC:
        case ObDateTimeTC:
        case ObTimeTC:
        case ObExtendTC:
        case ObBitTC:
        case ObEnumSetTC:
          is_equal = (obj.v_.int64_ == rhs_obj.v_.int64_);
          break;
        case ObDateTC:
          is_equal = (obj.v_.date_ == rhs_obj.v_.date_);
          break;
        case ObYearTC:
          is_equal = (obj.v_.year_ == rhs_obj.v_.year_);
          break;
        case ObFloatTC:
          is_equal = (obj.v_.float_ == rhs_obj.v_.float_);
          break;
        case ObDoubleTC:
          {
            if (lib::is_mysql_mode() && obj.is_fixed_double() && rhs_obj.is_fixed_double()) {
              is_equal = ObObjCmpFuncs::fixed_double_cmp(obj, rhs_obj) == 0;
            } else {
              is_equal = (obj.v_.double_ == rhs_obj.v_.double_);
            }
          }
          break;
        case ObOTimestampTC:
          is_equal = (obj.time_ctx_.desc_ == rhs_obj.time_ctx_.desc_ && obj.v_.datetime_ == rhs_obj.v_.datetime_);
          break;
        case ObStringTC:
        case ObRawTC:
         {
          bool cmp_padding_space = false;
          if (obj.val_len_ != rhs_obj.val_len_
              || 0 != MEMCMP(obj.v_.string_, rhs_obj.v_.string_, obj.val_len_)) {
            if (lib::is_oracle_mode() && (obj.is_varying_len_char_type()
                                          || rhs_obj.is_varying_len_char_type())) {
              cmp_padding_space = true;
            }
            is_equal = (ObCharset::strcmpsp(obj.get_collation_type(), obj.v_.string_, obj.val_len_,
                rhs_obj.v_.string_, rhs_obj.val_len_, cmp_padding_space) == 0);
            }
          }
          break;
        case ObNumberTC:
          if (obj.nmb_desc_.se_ != rhs_obj.nmb_desc_.se_
              || obj.nmb_desc_.len_ != rhs_obj.nmb_desc_.len_
              || (0
                  != MEMCMP(obj.v_.nmb_digits_, rhs_obj.v_.nmb_digits_,
                      sizeof(uint32_t) * obj.nmb_desc_.len_))) {
            is_equal = false;
          }
          break;
        case ObIntervalTC:
          is_equal = obj.is_interval_ym() ? (obj.get_interval_ym() == rhs_obj.get_interval_ym())
                                     : (obj.get_interval_ds() == rhs_obj.get_interval_ds());
          break;
        case ObRowIDTC:
          if (ob_is_urowid(obj.get_type()) && ob_is_urowid(rhs_obj.get_type())) {
            is_equal = obj.get_urowid() == rhs_obj.get_urowid();
          } else {
            COMMON_LOG(WARN, "not support rowid type for now",
                       K(obj.get_type()),
                       K(obj.get_type()));
            ret = OB_ERR_UNEXPECTED;
          }
          break;
        case ObLobTC:
          COMMON_LOG(ERROR, "lob type rowkey not support", K(tc));
          break;
        default:
          COMMON_LOG(WARN, "not_supported type class", K(tc));
          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }
    }
  }
  return ret;
}

// TODO by fengshuo.fs: remove this function
bool ObRowkey::simple_equal(const ObRowkey &rhs) const
{
  bool ret = true;
  if (obj_ptr_ == rhs.obj_ptr_) {
    ret = (obj_cnt_ == rhs.obj_cnt_);
  } else if (obj_cnt_ != rhs.obj_cnt_){
    ret = false;
  } else {
    ObObjTypeClass tc = ObMaxTC;
    for (int64_t i = 0; ret && i < obj_cnt_; ++i) {
      const ObObj &obj = obj_ptr_[i];
      const ObObj &rhs_obj = rhs.obj_ptr_[i];
      tc = obj.get_type_class();
      if (ObMaxTC <= tc) {
        COMMON_LOG(WARN, "invalid type class", K(tc));
        common::right_to_die_or_duty_to_live();
      } else if (tc != rhs_obj.get_type_class()) {
        ret = false;
      } else {
        switch(tc) {
        case ObNullTC:
          break;
        case ObIntTC:
        case ObUIntTC:
        case ObDateTimeTC:
        case ObTimeTC:
        case ObExtendTC:
        case ObBitTC:
        case ObEnumSetTC:
          ret = (obj.v_.int64_ == rhs_obj.v_.int64_);
          break;
        case ObDateTC:
          ret = (obj.v_.date_ == rhs_obj.v_.date_);
          break;
        case ObYearTC:
          ret = (obj.v_.year_ == rhs_obj.v_.year_);
          break;
        case ObFloatTC:
          ret = (obj.v_.float_ == rhs_obj.v_.float_);
          break;
        case ObDoubleTC:
          {
            if (lib::is_mysql_mode() && obj.is_fixed_double() && rhs_obj.is_fixed_double()) {
              ret = ObObjCmpFuncs::fixed_double_cmp(obj, rhs_obj) == 0;
            } else {
              ret = (obj.v_.double_ == rhs_obj.v_.double_);
            }
          }
          break;
        case ObOTimestampTC:
          ret = (obj.time_ctx_.desc_ == rhs_obj.time_ctx_.desc_ && obj.v_.datetime_ == rhs_obj.v_.datetime_);
          break;
        case ObStringTC:
        case ObRawTC:
         {
          bool cmp_padding_space = false;
          if (obj.val_len_ != rhs_obj.val_len_
              || 0 != MEMCMP(obj.v_.string_, rhs_obj.v_.string_, obj.val_len_)) {
            if (lib::is_oracle_mode() && (obj.is_varying_len_char_type()
                                          || rhs_obj.is_varying_len_char_type())) {
              cmp_padding_space = true;
            }
            ret = (ObCharset::strcmpsp(obj.get_collation_type(), obj.v_.string_, obj.val_len_,
                rhs_obj.v_.string_, rhs_obj.val_len_, cmp_padding_space) == 0);
            }
          }
          break;
        case ObNumberTC:
          if (obj.nmb_desc_.se_ != rhs_obj.nmb_desc_.se_
              || obj.nmb_desc_.len_ != rhs_obj.nmb_desc_.len_
              || (0
                  != MEMCMP(obj.v_.nmb_digits_, rhs_obj.v_.nmb_digits_,
                      sizeof(uint32_t) * obj.nmb_desc_.len_))) {
            ret = false;
          }
          break;
        case ObIntervalTC:
          ret = obj.is_interval_ym() ? (obj.get_interval_ym() == rhs_obj.get_interval_ym())
                                     : (obj.get_interval_ds() == rhs_obj.get_interval_ds());
          break;
        case ObRowIDTC:
          if (ob_is_urowid(obj.get_type()) && ob_is_urowid(rhs_obj.get_type())) {
            ret = obj.get_urowid() == rhs_obj.get_urowid();
          } else {
            COMMON_LOG(WARN, "not support rowid type for now",
                       K(obj.get_type()),
                       K(obj.get_type()));
            common::right_to_die_or_duty_to_live();
          }
          break;
        case ObLobTC:
          COMMON_LOG(ERROR, "lob type rowkey not support", K(tc));
          break;
        default:
          COMMON_LOG(WARN, "not_supported type class", K(tc));
          common::right_to_die_or_duty_to_live();
          break;
        }
      }
    }
  }
  return ret;
}

int ObRowkey::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_legal())) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, obj_cnt_))) {
    COMMON_LOG(WARN, "encode object count failed.",
               KP(buf), K(buf_len), K(pos), K_(obj_cnt), K(ret));
  } else if (OB_FAIL(serialize_objs(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "serialize objects failed.",
               KP(buf), K(buf_len), K(pos), K_(obj_cnt), K(ret));
  }
  return ret;
}

int ObRowkey::deserialize(const char *buf, const int64_t buf_len, int64_t &pos, bool check_zero  /*= false*/)
{
  int ret = OB_SUCCESS;
  int64_t obj_cnt = 0;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &obj_cnt))) {
    COMMON_LOG(WARN, "decode object count failed.",
               KP(buf), K(buf_len), K(pos), K(obj_cnt), K(ret));
  } else if (obj_cnt > 0) {
    // If there is no assign external ptr, then dynamically assign
    if (NULL == obj_ptr_) {
      if (OB_ISNULL(obj_ptr_ = static_cast<ObObj *>
          (this_worker().get_sql_arena_allocator().alloc(sizeof(ObObj) * obj_cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN,
            "failed to allocate memory for decode ObRowKey",
            K(ret),
            K(obj_cnt),
            K(sizeof(ObObj)));
      }
    }
    // Otherwise, check whether the number of reserved primary keys meets the requirements
    else if (obj_cnt_ < obj_cnt) {
      COMMON_LOG(ERROR, "preserved obj count is not enough", K(obj_cnt_), K(obj_cnt), K(obj_ptr_));
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      // Use the reserved obj array
    }
    if (OB_SUCC(ret)) {
      obj_cnt_ = obj_cnt;
      if (OB_FAIL(deserialize_objs(buf, buf_len, pos))) {
        COMMON_LOG(WARN, "decode objects failed.",
                   KP(buf), K(buf_len), K(pos), K(obj_cnt), K(ret));
      }
    }
  } else if (check_zero && obj_cnt == 0) {
    // Prevent the reserved obj array not aware the obj is empty when obj_cnt = 0
    obj_cnt_ = obj_cnt;
  }
  return ret;
}

int64_t ObRowkey::get_serialize_size(void) const
{
  int64_t size = serialization::encoded_length_vi64(obj_cnt_);
  size += get_serialize_objs_size();
  return size;
}

int ObRowkey::serialize_objs(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_legal())) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(ret));
  }
  for (int64_t i = 0; i < obj_cnt_ && OB_SUCCESS == ret; ++i) {
    if (OB_FAIL(obj_ptr_[i].serialize(buf, buf_len, pos))) {
      COMMON_LOG(WARN, "serialize object failed.",
                 K(i), KP(buf), K(buf_len), K(pos), K(ret));
    }
  }

  return ret;
}

int64_t ObRowkey::get_serialize_objs_size(void) const
{
  int64_t total_size = 0;
  for (int64_t i = 0; i < obj_cnt_ ; ++i) {
    total_size += obj_ptr_[i].get_serialize_size();
  }
  return total_size;
}

int ObRowkey::deserialize_objs(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (NULL == obj_ptr_) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "obj array is NULL", K(ret));
  } else {
    for (int64_t i = 0; i < obj_cnt_ && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(obj_ptr_[i].deserialize(buf, buf_len, pos))) {
        COMMON_LOG(WARN, "deserialize object failed.",
                   K(i), KP(buf), K(buf_len), K(pos), K(ret));
      }
    }
  }
  return ret;
}

int64_t ObRowkey::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_ISNULL(obj_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < obj_cnt_; ++i) {
      if (pos < length) {
        if (!obj_ptr_[i].is_max_value() && !obj_ptr_[i].is_min_value()) {
          obj_ptr_[i].print_range_value(buffer, length, pos);
        } else if (obj_ptr_[i].is_min_value()) {
          if (OB_FAIL(databuff_printf(buffer, length, pos, "MIN"))) {
            // do nothing
          }
        } else {
          if (OB_FAIL(databuff_printf(buffer, length, pos, "MAX"))) {
            // do nothing
          }
        }
        if (OB_SUCC(ret)) {
          if (i < obj_cnt_ - 1) {
            if (OB_FAIL(databuff_printf(buffer, length, pos, ","))) {
              // do nothing
            }
          }
        }
      }
    }
  }
  //no way to preserve ret code
  (void)ret;
  return pos;
}

// used for log_tool
int64_t ObRowkey::to_smart_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_ISNULL(obj_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < obj_cnt_; ++i) {
      if (pos < length) {
        if (!obj_ptr_[i].is_max_value() && !obj_ptr_[i].is_min_value()) {
          if (OB_FAIL(obj_ptr_[i].print_smart(buffer, length, pos))) {
            // do nothing
          }
        } else if (obj_ptr_[i].is_min_value()) {
          if (OB_FAIL(databuff_printf(buffer, length, pos, "MIN"))) {
            // do nothing
          }
        } else {
          if (OB_FAIL(databuff_printf(buffer, length, pos, "MAX"))) {
            // do nothing
          }
        }
        if (OB_SUCC(ret)) {
          if (i < obj_cnt_ - 1) {
            if (OB_FAIL(databuff_printf(buffer, length, pos, "   "))) {
              // do nothing
            }
          }
        }
      }
    }
  }
  //no way to preserve ret code
  (void)ret;
  return pos;
}

int ObRowkey::need_transform_to_collation_free(bool &need_transform) const
{
  int ret = OB_SUCCESS;
  need_transform = false;
  if (obj_cnt_ <= 0 || NULL == obj_ptr_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(obj_cnt_), KP(obj_ptr_));
  }

  for (int64_t i = 0; OB_SUCC(ret) && !need_transform && i < obj_cnt_; ++i) {
    if (obj_ptr_[i].is_collation_free_compatible()) {
      need_transform = true;
    }
  }
  return ret;
}

// used for log_tool
int64_t ObRowkey::to_format_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < obj_cnt_; ++i) {
    if (pos < length) {
      if (!obj_ptr_[i].is_max_value() && !obj_ptr_[i].is_min_value()) {
        if (OB_FAIL(obj_ptr_[i].print_format(buffer, length, pos))) {
          COMMON_LOG(WARN, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      } else if (obj_ptr_[i].is_min_value()) {
        if (OB_FAIL(databuff_printf(buffer, length, pos, "MIN"))) {
          COMMON_LOG(WARN, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(buffer, length, pos, "MAX"))) {
          COMMON_LOG(WARN, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (i < obj_cnt_ - 1) {
          if (OB_FAIL(databuff_printf(buffer, length, pos, " "))) {
            COMMON_LOG(WARN, "Failed to print", K(ret));
          }
        }
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(WARN, "buf is not enough", K(length), K(pos), K(ret));
    }
  }
  //no way to preserve ret code
  (void)ret;
  return pos;
}

int64_t ObRowkey::to_plain_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < obj_cnt_; ++i) {
    if (pos < length) {
      if (!obj_ptr_[i].is_max_value() && !obj_ptr_[i].is_min_value()) {
        if (OB_FAIL(obj_ptr_[i].print_plain_str_literal(buffer, length, pos))) {
          COMMON_LOG(WARN, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      } else if (obj_ptr_[i].is_min_value()) {
        if (OB_FAIL(databuff_printf(buffer, length, pos, "MIN"))) {
          COMMON_LOG(WARN, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(buffer, length, pos, "MAX"))) {
          COMMON_LOG(WARN, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (i < obj_cnt_ - 1) {
          if (OB_FAIL(databuff_printf(buffer, length, pos, ","))) {
            COMMON_LOG(WARN, "Failed to print", K(ret));
          }
        }
      }
    } else {
      break;
    }
  }
  //no way to preserve ret code
  (void)ret;
  return pos;
}

int ObRowkey::checksum(ObBatchChecksum &bc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_legal())) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(ret));
  } else if (0 < obj_cnt_ && NULL != obj_ptr_) {
    for (int64_t i = 0; i < obj_cnt_; i++) {
      obj_ptr_[i].checksum(bc);
    }
  }
  return ret;
}

uint64_t ObRowkey::murmurhash(const uint64_t hash) const
{
  int tmp_ret = OB_SUCCESS;
  uint64_t ret = hash;
  if (OB_UNLIKELY(!is_legal())) {
    tmp_ret = OB_INVALID_DATA;
    COMMON_LOG(ERROR, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(tmp_ret));
  } else if (0 < obj_cnt_ && NULL != obj_ptr_) {
    if (is_min_row() || is_max_row()) {
      tmp_ret = obj_ptr_[0].hash(ret, ret);
    } else {
      for (int64_t i = 0; i < obj_cnt_; i++) {
        tmp_ret = obj_ptr_[i].hash(ret, ret);
      }
    }
  }
  return ret;
}

int ObRowkey::get_common_prefix_length(
    const ObRowkey &lhs, const ObRowkey &rhs, int64_t &prefix_len)
{
  int ret = OB_SUCCESS;
  const ObObj *lhs_obj = NULL;
  const ObObj *rhs_obj = NULL;
  int64_t min_obj_cnt = 0;
  prefix_len = 0;

  if (!lhs.is_valid() || !rhs.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(lhs), K(rhs));
  }

  min_obj_cnt = std::min(lhs.get_obj_cnt(), rhs.get_obj_cnt());
  for (int64_t i = 0; OB_SUCC(ret) && i < min_obj_cnt; ++i) {
    lhs_obj = &(lhs.get_obj_ptr()[i]);
    rhs_obj = &(rhs.get_obj_ptr()[i]);
    if (!lhs_obj->is_max_value() && !lhs_obj->is_min_value()
        && !rhs_obj->is_max_value() && !rhs_obj->is_min_value()
        && *lhs_obj == *rhs_obj) {
      ++prefix_len;
    } else {
      break;
    }
  }

  return ret;
}

}
}
