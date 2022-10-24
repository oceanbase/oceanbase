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

#ifndef OCEANBASE_SHARE_OB_CLIENT_FEEDBACK_BASIC_H_
#define OCEANBASE_SHARE_OB_CLIENT_FEEDBACK_BASIC_H_

#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
namespace share
{
//--------------------------serialize---------------------
#define OB_FB_ENCODE_INT(num) \
do { \
  if (OB_SUCC(ret)) { \
    uint64_t tmp_num = static_cast<uint64_t>(num); \
    ret = ::oceanbase::obmysql::ObMySQLUtil::store_length(buf, len, tmp_num, pos); \
  } \
} while(0)

#define OB_FB_ENCODE_STRUCT \
do { \
  if (OB_SUCC(ret)) { \
    ret = serialize_struct(buf, len, pos); \
  } \
} while(0)

#define OB_FB_GET_T (static_cast<T*>(this))
#define OB_FB_GET_CONST_T (static_cast<const T*>(this))

#define OB_FB_ENCODE_STRUCT_CONTENT \
do { \
  if (OB_SUCC(ret)) { \
    ret = OB_FB_GET_CONST_T->serialize_struct_content(buf, len, pos); \
  } \
} while(0)

#define CHECK_SER_INPUT_VALUE_VALID \
do { \
  if (OB_SUCC(ret)) { \
    if (OB_ISNULL(buf) \
        || OB_UNLIKELY(len <= 0) \
        || OB_UNLIKELY(pos < 0)) { \
      ret = common::OB_INVALID_ARGUMENT; \
      SHARE_LOG(WARN, "invalid argument", KP(buf), K(len), K(pos), K(ret)); \
    } else if (OB_UNLIKELY(pos >= len)) { \
      ret = common::OB_SIZE_OVERFLOW; \
      SHARE_LOG(WARN, "buf is no enough", KP(buf), K(len), K(pos), K(ret)); \
    } \
  } \
} while(0)

#define OB_FB_ENCODE_STRUCT_ARRAY(array, count) \
do { \
  OB_FB_ENCODE_INT(count); \
  for (int i = 0; (OB_SUCC(ret)) && (i < count); ++i) { \
    ret = array[i].serialize_struct(buf, len, pos); \
  } \
} while (0)

#define OB_FB_ENCODE_STRING(str_ptr, str_len)                                                  \
  do {                                                                                         \
    if (OB_SUCC(ret)) {                                                                        \
      if (OB_ISNULL(str_ptr) || OB_UNLIKELY(str_len < 0)) {                                    \
        ret = common::OB_INVALID_ARGUMENT;                                                     \
        SHARE_LOG(WARN, "invalid argument", K(str_ptr), K(str_len), K(ret));                   \
      } else {                                                                                 \
        ret = ::oceanbase::obmysql::ObMySQLUtil::store_str_v(buf, len, str_ptr, str_len, pos); \
      }                                                                                        \
    }                                                                                          \
  } while (0)

#define OB_FB_SER_START \
  INIT_SUCC(ret); \
  { \
    ObSeriPosGuard pos_guard(pos, ret); \
    CHECK_SER_INPUT_VALUE_VALID;

#define OB_FB_SER_END \
  } \
  return ret;

//-------------------------deserialize-----------------------

#define OB_FB_DESER_START OB_FB_SER_START
#define OB_FB_DESER_END OB_FB_SER_END

#define OB_FB_DECODE_STRUCT_CONTENT(tmp_buf, tmp_len, tmp_pos) \
do { \
  if (OB_SUCC(ret)) { \
    if (OB_FAIL(OB_FB_GET_T->deserialize_struct_content(tmp_buf, tmp_len, tmp_pos))) { \
      SHARE_LOG(ERROR, "fail to deserialize_struct_content", K(tmp_buf), K(tmp_len), K(tmp_pos), K(ret)); \
    } \
  } \
} while(0)


#define OB_FB_DECODE_INT(num, type) \
do { \
  if (OB_SUCC(ret) && (pos < len)) { \
    uint64_t tmp_num = 0; \
    const char *tmp_buf_start = buf + pos; \
    if (OB_FAIL(::oceanbase::obmysql::ObMySQLUtil::get_length(tmp_buf_start, tmp_num))) { \
      SHARE_LOG(ERROR, "fail to get length", K(pos), K(ret)); \
    } else if (FALSE_IT(pos += (tmp_buf_start - buf - pos))) { \
    } else if (OB_UNLIKELY(pos > len)) { \
      ret = common::OB_ERR_UNEXPECTED; \
      SHARE_LOG(ERROR, "invalid pos or len", K(pos), K(len), K(num), K(ret)); \
    } else { \
      num = static_cast<type>(tmp_num); \
    }\
  } \
} while (0)

#define OB_FB_DECODE_STRUCT_ARRAY(array, type) \
do { \
 int64_t count = 0; \
 OB_FB_DECODE_INT(count, int64_t); \
 for (int64_t i = 0; OB_SUCC(ret) && (i < count); ++i) { \
   type object; \
   if (OB_FAIL(object.deserialize_struct(buf, len, pos))) { \
     SHARE_LOG(ERROR, "fail to deserialize_struct", K(len), K(pos), K(ret)); \
   } else if (OB_UNLIKELY(!object.is_valid())) { \
     ret = common::OB_INVALID_ARGUMENT; \
     SHARE_LOG(ERROR, "invalid argument", K(object), K(ret)); \
   } else if (OB_FAIL(array.push_back(object))) { \
     SHARE_LOG(ERROR, "fail to push back", K(object), K(ret)); \
   } \
 } \
} while (0)

#define OB_FB_DECODE_STRING(str_ptr, max_str_len, dec_str_len)                \
  do {                                                                        \
    if (OB_SUCC(ret)) {                                                       \
      uint64_t tmp_len = 0;                                                   \
      const char *tmp_buf_start = buf + pos;                                  \
      if (OB_ISNULL(str_ptr)) {              \
        ret = common::OB_INVALID_ARGUMENT;                                    \
        SHARE_LOG(ERROR, "invalid argument", K(str_ptr), K(ret));       \
      } else if (OB_UNLIKELY(max_str_len <= 0)) {                       \
        ret = common::OB_INVALID_ARGUMENT;                                    \
        SHARE_LOG(ERROR, "invalid argument", K(max_str_len), K(ret));   \
      } else if (OB_FAIL(::oceanbase::obmysql::ObMySQLUtil::get_length(       \
                     tmp_buf_start, tmp_len))) {                              \
        SHARE_LOG(ERROR, "failed to get string len", K(ret));                 \
      } else if (FALSE_IT(pos += (tmp_buf_start - buf - pos))) {                \
      } else if (OB_UNLIKELY(tmp_len > max_str_len)) {                        \
        ret = common::OB_ERR_UNEXPECTED;                                      \
        SHARE_LOG(ERROR, "invalid str len", K(ret), K(max_str_len),           \
                  K(tmp_len));                                                \
      } else {                                                                \
        MEMCPY(str_ptr, buf + pos, tmp_len);                                  \
        pos += tmp_len;                                                       \
        dec_str_len = tmp_len;                                                \
      }                                                                       \
    }                                                                         \
  } while (0)

static const char *const OB_CLIENT_FEEDBACK = "ob_client_feedback";
static const char *const OB_CLIENT_REROUTE_INFO = "ob_client_reroute_info";

// For compatibility, must not delete or modify ele type,
// only append is allowed.
enum ObFeedbackElementType
{
  MIN_FB_ELE = 0,
#define OB_FB_TYPE_DEF(name) name,
#include "share/client_feedback/ob_feedback_type_define.h"
#undef OB_FB_TYPE_DEF
  MAX_FB_ELE
};

extern const char *get_feedback_element_type_str(const ObFeedbackElementType type);

extern bool is_valid_fb_element_type(const int64_t type);

class ObSeriPosGuard
{
public:
  ObSeriPosGuard(int64_t &pos, int &ret)
    : pos_(pos), orig_pos_(pos), ret_(ret) {}

  ~ObSeriPosGuard()
  {
    if (common::OB_SUCCESS != ret_) {
      pos_ = orig_pos_;
    }
  }

private:
  int64_t &pos_;
  int64_t orig_pos_;
  int &ret_;
};

#define FB_OBJ_DEFINE_METHOD \
  int deserialize_struct_content(char *buf, const int64_t len, int64_t &pos); \
  int serialize_struct_content(char *buf, const int64_t len, int64_t &pos) const; \
  bool is_valid_obj() const;

template <class T>
class ObAbstractFeedbackObject
{
public:
  ObAbstractFeedbackObject(const ObFeedbackElementType type) : type_(type) {}
  virtual ~ObAbstractFeedbackObject() {}

  int serialize_struct(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize_struct(char *buf, const int64_t len, int64_t &pos);

  // observer need serialize, OCJ && obproxy need deserialize
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(char *buf, const int64_t len, int64_t &pos);

  bool is_valid() const {
    bool bret = true;
    if (MIN_FB_ELE != get_type()) {
      bret = is_valid_fb_element_type(get_type());
    }
    if (bret) {
      bret = OB_FB_GET_CONST_T->is_valid_obj();
    }
    return bret;
  }

  bool need_seri_type() const { return MIN_FB_ELE != get_type(); }

  ObFeedbackElementType get_type() const { return type_; }

  TO_STRING_KV("type", get_feedback_element_type_str(type_));

protected:
  // when MIN_FB_ELE == type_ means this class just a common obj without ObFeedbackElementType
  // , so no need seri or deseri type_, just like ObFeedbackReplicaLocation.
  ObFeedbackElementType type_;
};

template <class T>
inline int ObAbstractFeedbackObject<T>::serialize_struct(char *buf, const int64_t len, int64_t &pos) const
{
  OB_FB_SER_START;
  int64_t orig_pos = pos;

  // reserve L
  // reserved one byte is enough for most case
  ++pos;

  // encode V
  OB_FB_ENCODE_STRUCT_CONTENT;

  int64_t content_encode_len = 0;
  int64_t len_encode_len = 0;
  if (OB_SUCC(ret)) {
    content_encode_len = pos - orig_pos - 1;
    len_encode_len = static_cast<int64_t>(obmysql::ObMySQLUtil::get_number_store_len(content_encode_len));
    if (1 == len_encode_len) { // only one byte is enough
      // nothing
    } else {
      if ((orig_pos + len_encode_len + content_encode_len) > len) {
        ret = common::OB_SIZE_OVERFLOW;
      } else {
        // not enough, need copy
        MEMMOVE(buf + len_encode_len, buf + 1, content_encode_len);
      }
    }
    pos = orig_pos;
  }

  // encode L
  OB_FB_ENCODE_INT(content_encode_len);

  if (OB_SUCC(ret)) {
    pos = orig_pos + len_encode_len + content_encode_len;
  }
  OB_FB_SER_END;
}

template <class T>
inline int ObAbstractFeedbackObject<T>::deserialize_struct(char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;
  int64_t struct_len = 0;
  // read struct len
  OB_FB_DECODE_INT(struct_len, int64_t);

  // deseri struct content
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(struct_len <= 0)) {
      ret = common::OB_INVALID_DATA;
      SHARE_LOG(ERROR, "struct_len must > 0", K(pos), K(struct_len), K(len), K(ret));
    } else if ((pos + struct_len) > len) {
      ret = common::OB_INVALID_DATA;
      SHARE_LOG(ERROR, "invalid data buf", K(pos), K(struct_len), K(len), K(ret));
    } else {
      int64_t orig_pos = pos;
      int64_t curr_len = pos + struct_len;

      OB_FB_DECODE_STRUCT_CONTENT(buf, curr_len, pos);

      if (OB_SUCC(ret)) {
        if (pos < orig_pos + struct_len) {
          // means current is old version, just skip
          pos = orig_pos + struct_len;
        } else if (OB_UNLIKELY(pos > orig_pos + struct_len)) {
          // impossible, just for defense
          ret = common::OB_ERR_UNEXPECTED;
          SHARE_LOG(ERROR, "unexpect error", K(pos), K(orig_pos), K(struct_len), K(len), K(ret));
        } else {
          // pos == orig_pos + struct_len, normal case
        }
      }
    }
  }
  OB_FB_DESER_END;
}

template <class T>
int ObAbstractFeedbackObject<T>::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  OB_FB_SER_START;

  // check valid
  if (OB_UNLIKELY(!is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid type", KPC(this), K(ret));
  }

  if (OB_SUCC(ret)) { // seri type if needed
    if (need_seri_type()) {
      const int type_num = static_cast<int>(type_);
      // encode T
      OB_FB_ENCODE_INT(type_num);
    }
  }

  // encode L-V
  OB_FB_ENCODE_STRUCT;

  OB_FB_SER_END;
}

template <class T>
int ObAbstractFeedbackObject<T>::deserialize(char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;

  if (OB_SUCC(ret)) { // deseri type if needed
    if (need_seri_type()) {
      int64_t type = 0;
      // read struct len
      OB_FB_DECODE_INT(type, int64_t);
      if (OB_SUCC(ret)) { // just for defense
        if (type != type_) {
          ret = common::OB_ERR_UNEXPECTED;
          SHARE_LOG(ERROR, "unrecognise type", K(type), K_(type), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(deserialize_struct(buf, len, pos))) {
      SHARE_LOG(ERROR, "fail to deserialize_struct", K(len), K(pos), K(ret));
    }
  }
  OB_FB_DESER_END;
}

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_CLIENT_FEEDBACK_BASIC_H_
