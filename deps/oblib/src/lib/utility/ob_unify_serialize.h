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

#ifndef _OCEABASE_LIB_UTILITY_OB_UNIFY_SERIALIZE_H_
#define _OCEABASE_LIB_UTILITY_OB_UNIFY_SERIALIZE_H_

#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace lib
{

#ifdef ENABLE_SERIALIZATION_CHECK
enum ObSerializationCheckStatus
{
  CHECK_STATUS_WATING = 0,
  CHECK_STATUS_RECORDING = 1,
  CHECK_STATUS_COMPARING = 2
};
static constexpr int MAX_SERIALIZE_RECORD_LENGTH = 256;
struct SerializeDiagnoseRecord
{
  uint8_t encoded_lens[MAX_SERIALIZE_RECORD_LENGTH];
  int count = -1;
  int check_index = -1;
  int flag = CHECK_STATUS_WATING;
};
RLOCAL_EXTERN(SerializeDiagnoseRecord, ser_diag_record);
void begin_record_serialization();
void finish_record_serialization();
void begin_check_serialization();
void finish_check_serialization();
#endif

#define SERIAL_PARAMS char *buf, const int64_t buf_len, int64_t &pos
#define DESERIAL_PARAMS const char *buf, const int64_t data_len, int64_t &pos

#define UNF_UNUSED_SER ({(void)buf; (void)buf_len; (void)pos;})
#define UNF_UNUSED_DES ({(void)buf; (void)data_len; (void)pos;})

#ifndef RPC_WARN
#define RPC_WARN(...) OB_LOG(WARN, __VA_ARGS__)
#endif

#define OB_DEF_SERIALIZE_SIMPLE(CLS)            \
  int CLS::serialize(SERIAL_PARAMS) const

#define OB_DEF_DESERIALIZE_SIMPLE(CLS)          \
  int CLS::deserialize(DESERIAL_PARAMS)

#define OB_DEF_SERIALIZE_SIZE_SIMPLE(CLS)       \
  int64_t CLS::get_serialize_size(void) const

///
// define essential macros used for encode/decode single object
//----------------------------------------------------------------------
#define NS_ ::oceanbase::common::serialization
#define OK_ ::oceanbase::common::OB_SUCCESS

#define OB_UNIS_ENCODE(obj)                                              \
  if (OB_SUCC(ret)) {                                                    \
    if (OB_FAIL(NS_::encode(buf, buf_len, pos, obj))) {                  \
      RPC_WARN("encode object fail",                                     \
               "name", MSTR(obj), K(buf_len), K(pos), K(ret));           \
    }                                                                    \
  }

#define OB_UNIS_DECODE(obj)                                              \
  if (OB_SUCC(ret) && pos < data_len) {                                  \
    if (OB_FAIL(NS_::decode(buf, data_len, pos, obj))) {                 \
      RPC_WARN("decode object fail",                                     \
               "name", MSTR(obj), K(data_len), K(pos), K(ret));          \
    }                                                                    \
  }

#ifdef ENABLE_SERIALIZATION_CHECK

#define IF_TYPE_MATCH(obj, type) \
        std::is_same<type, decltype(obj)>::value || std::is_same<type &, decltype(obj)>::value

#define IF_NEED_TO_CHECK_SERIALIZATION(obj)       \
        !(std::is_const<decltype(obj)>::value ||  \
        IF_TYPE_MATCH(obj, uint8_t) ||            \
        IF_TYPE_MATCH(obj, int8_t) ||             \
        IF_TYPE_MATCH(obj, bool) ||               \
        IF_TYPE_MATCH(obj, char))

#define OB_UNIS_ADD_LEN(obj)                                                                                        \
  {                                                                                                                 \
    int64_t this_len = NS_::encoded_length(obj);                                                                    \
    if (IF_NEED_TO_CHECK_SERIALIZATION(obj)) {                                                                      \
      if (oceanbase::lib::CHECK_STATUS_RECORDING == oceanbase::lib::ser_diag_record.flag &&                         \
          oceanbase::lib::ser_diag_record.count < oceanbase::lib::MAX_SERIALIZE_RECORD_LENGTH) {                    \
        oceanbase::lib::ser_diag_record.encoded_lens[oceanbase::lib::ser_diag_record.count++] =                     \
            static_cast<uint8_t>(this_len);                                                                         \
      } else if (oceanbase::lib::CHECK_STATUS_COMPARING == oceanbase::lib::ser_diag_record.flag &&                  \
                 oceanbase::lib::ser_diag_record.check_index < oceanbase::lib::ser_diag_record.count) {             \
        int ret = OB_ERR_UNEXPECTED;                                                                                \
        int record_len = oceanbase::lib::ser_diag_record.encoded_lens[oceanbase::lib::ser_diag_record.check_index]; \
        if (this_len != record_len) {                                                                               \
          OB_LOG(ERROR, "encoded length not match", "name", MSTR(obj), K(this_len), K(record_len), "value", obj);   \
        }                                                                                                           \
        oceanbase::lib::ser_diag_record.check_index++;                                                              \
      }                                                                                                             \
    }                                                                                                               \
    len += this_len;                                                                                                \
  }
#else
#define OB_UNIS_ADD_LEN(obj)                                             \
  len += NS_::encoded_length(obj)
#endif
//-----------------------------------------------------------------------

// serialize_ no header
#define OB_SERIALIZE_NOHEADER(CLS, PARENT, SUFFIX, PRED, ...)  \
  int CLS::serialize##SUFFIX(SERIAL_PARAMS) const {            \
    int ret = PARENT::serialize(buf, buf_len, pos);    \
    if (OB_SUCC(ret) && (PRED)) {                              \
      LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__);              \
    }                                                          \
    return ret;                                                \
  }

#define OB_DESERIALIZE_NOHEADER(CLS, PARENT, SUFFIX, ...)  \
  int CLS::deserialize##SUFFIX(DESERIAL_PARAMS) {            \
  int ret = PARENT::deserialize(buf, data_len, pos); \
  if (OB_SUCC(ret)) {                                \
    LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__);      \
  }                                                  \
  return ret;                                        \
}

#define OB_SERIALIZE_SIZE_NOHEADER(CLS, PARENT, SUFFIX, PRED, ...)   \
  int64_t CLS::get_serialize_size##SUFFIX(void) const {              \
    int64_t len = PARENT::get_serialize_size();              \
    if (PRED) {                                                      \
      LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__);                   \
    }                                                                \
    return len;                                                      \
  }

#define OB_SERIALIZE_MEMBER_INHERIT(CLS, PARENT, ...)  \
  OB_SERIALIZE_NOHEADER(CLS, PARENT, , true, ##__VA_ARGS__);   \
  OB_DESERIALIZE_NOHEADER(CLS, PARENT, ,##__VA_ARGS__);       \
  OB_SERIALIZE_SIZE_NOHEADER(CLS, PARENT, , true, ##__VA_ARGS__);

struct EmptyUnisStruct
{
  static int serialize(SERIAL_PARAMS) {
    UNF_UNUSED_SER;
    return 0;
  }
  static int deserialize(DESERIAL_PARAMS) {
    UNF_UNUSED_DES;
    return 0;
  }
  static int64_t get_serialize_size() {
    return 0;
  }
};
#define EmptyParent ::oceanbase::lib::EmptyUnisStruct
#define OB_SERIALIZE_MEMBER_SIMPLE(CLS, ...)   OB_SERIALIZE_MEMBER_INHERIT(CLS, EmptyParent, ##__VA_ARGS__)

///
// define serialize/desrialize wrapper which helps hide "version" and
// "length"
//-----------------------------------------------------------------------
#define CHECK_VERSION_LENGTH(CLS, VER, LEN)                              \
  if (OB_SUCC(ret)) {                                                    \
    if (VER != UNIS_VERSION) {                                           \
      ret = ::oceanbase::common::OB_NOT_SUPPORTED;                                   \
      RPC_WARN("object version mismatch", "cls", #CLS, K(ret), K(VER));  \
    } else if (LEN < 0) {                                                \
      ret = ::oceanbase::common::OB_ERR_UNEXPECTED;                                  \
      RPC_WARN("can't decode object with negative length", K(LEN));      \
    } else if (data_len < LEN + pos) {                                   \
      ret = ::oceanbase::common::OB_DESERIALIZE_ERROR;                               \
      RPC_WARN("buf length not enough", K(LEN), K(pos), K(data_len));    \
    }                                                                    \
  }

#ifdef NDEBUG
#define CHECK_SERIALIZE_SIZE(CLS, real_size)
#else
#define CHECK_SERIALIZE_SIZE(CLS, real_size)                    \
  int64_t expect_size = get_serialize_size(); \
  assert(expect_size >= real_size);
#endif

#define OB_UNIS_SERIALIZE(CLS)                                         \
  int CLS::serialize(SERIAL_PARAMS) const                                \
  {                                                                      \
    int ret = OK_;                                                       \
    OB_UNIS_ENCODE(UNIS_VERSION);                                            \
    if (OB_SUCC(ret)) {                                                  \
      int64_t size_nbytes = NS_::OB_SERIALIZE_SIZE_NEED_BYTES;           \
      int64_t pos_bak = (pos += size_nbytes);                            \
      if (OB_SUCC(ret)) {                                               \
        if (OB_FAIL(serialize_(buf, buf_len, pos))) {                   \
          RPC_WARN("serialize fail", K(ret));                           \
        }                                                               \
      }                                                                 \
      int64_t serial_size = pos - pos_bak;                               \
      int64_t tmp_pos = 0;                                               \
      CHECK_SERIALIZE_SIZE(CLS, serial_size);                            \
      if (OB_SUCC(ret)) {                                                \
        ret = NS_::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes,   \
          size_nbytes, tmp_pos, serial_size);                            \
      }                                                                  \
    }                                                                    \
    return ret;                                                          \
  }

#define OB_UNIS_DESERIALIZE(CLS)                                         \
  int CLS::deserialize(DESERIAL_PARAMS) {                               \
    int ret = OK_;                                                       \
    int64_t version = 0;                                                 \
    int64_t len = 0;                                                     \
    if (OB_SUCC(ret)) {                                                 \
      OB_UNIS_DECODE(version);                                         \
      OB_UNIS_DECODE(len);                                             \
      CHECK_VERSION_LENGTH(CLS, version, len);                          \
    }                                                                   \
    if (OB_SUCC(ret)) {                                                 \
      int64_t pos_orig = pos;                                           \
      pos = 0;                                                          \
      if (OB_FAIL(deserialize_(buf + pos_orig, len, pos))) {           \
        RPC_WARN("deserialize_ fail", "slen", len, K(pos), K(ret));    \
      }                                                                 \
      pos = pos_orig + len;                                             \
    }                                                                   \
    return ret;                                                          \
  }

#define OB_UNIS_SERIALIZE_SIZE(CLS)                                      \
  int64_t CLS::get_serialize_size(void) const {                         \
    int64_t len = get_serialize_size_();                                \
    OB_UNIS_ADD_LEN(UNIS_VERSION);                                           \
    len += NS_::OB_SERIALIZE_SIZE_NEED_BYTES;                           \
    return len;                                                          \
  }

//-----------------------------------------------------------------------

///
// macro to declare unis structure, here would be non-implement
// functions in pure class but it's ok coz they wouldn't be invoked
// and the derived class should overwrite these functions.
// -----------------------------------------------------------------------
#define OB_DECLARE_UNIS(VIR,PURE)               \
  VIR int serialize(SERIAL_PARAMS) const PURE;  \
  int serialize_(SERIAL_PARAMS) const;          \
  VIR int deserialize(DESERIAL_PARAMS) PURE;    \
  int deserialize_(DESERIAL_PARAMS);            \
  VIR int64_t get_serialize_size() const PURE;  \
  int64_t get_serialize_size_() const;          \

///
// public entries, define interfaces of manual serialization
//-----------------------------------------------------------------------
#define OB_UNIS_VERSION(VER)                            \
  public: OB_DECLARE_UNIS(,);                           \
private:                                                \
const static int64_t UNIS_VERSION = VER

#define OB_UNIS_VERSION_V(VER)                          \
  public: OB_DECLARE_UNIS(virtual,);                    \
private:                                                \
const static int64_t UNIS_VERSION = VER

#define OB_UNIS_VERSION_PV()                            \
  public: OB_DECLARE_UNIS(virtual,=0); private:

#define OB_DEF_SERIALIZE(CLS, TEMP...)          \
  TEMP OB_UNIS_SERIALIZE(CLS);                  \
  TEMP int CLS::serialize_(SERIAL_PARAMS) const

#define OB_DEF_DESERIALIZE(CLS, TEMP...)        \
  TEMP OB_UNIS_DESERIALIZE(CLS);                \
  TEMP int CLS::deserialize_(DESERIAL_PARAMS)

#define OB_DEF_SERIALIZE_SIZE(CLS, TEMP...)         \
  TEMP OB_UNIS_SERIALIZE_SIZE(CLS);                 \
  TEMP int64_t CLS::get_serialize_size_(void) const

#define OB_SERIALIZE_MEMBER_TEMP_INHERIT(TEMP, CLS, PARENT, ...)        \
  TEMP OB_UNIS_SERIALIZE(CLS);                                         \
  TEMP OB_UNIS_DESERIALIZE(CLS);                                       \
  TEMP OB_UNIS_SERIALIZE_SIZE(CLS);                                    \
  TEMP OB_SERIALIZE_NOHEADER(CLS, PARENT, _, true, ##__VA_ARGS__); \
  TEMP OB_DESERIALIZE_NOHEADER(CLS, PARENT, _,##__VA_ARGS__);     \
  TEMP OB_SERIALIZE_SIZE_NOHEADER(CLS, PARENT, _, true, ##__VA_ARGS__);

#define CAR(a, b) a
#define CDR(a, b) b
#define MY_CLS(CLS) IF_IS_PAREN(CLS, CAR CLS, CLS)
#define BASE_CLS(CLS) IF_IS_PAREN(CLS, CDR CLS, EmptyParent)
#define OB_SERIALIZE_MEMBER_TEMP(TEMP, CLS, ...) OB_SERIALIZE_MEMBER_TEMP_INHERIT(TEMP, MY_CLS(CLS), BASE_CLS(CLS), ##__VA_ARGS__)
#define OB_SERIALIZE_MEMBER(CLS, ...) OB_SERIALIZE_MEMBER_TEMP(, CLS, ##__VA_ARGS__)

/// utility macros to deal with C native array
#define OB_UNIS_ENCODE_ARRAY(objs, objs_count)                  \
  OB_UNIS_ENCODE((objs_count));                                 \
  for (int64_t i = 0; OB_SUCC(ret) && i < (objs_count); ++i) {  \
    OB_UNIS_ENCODE(objs[i]);                                    \
  }

#define OB_UNIS_ADD_LEN_ARRAY(objs, objs_count)   \
    OB_UNIS_ADD_LEN((objs_count));                \
    for (int64_t i = 0; i < (objs_count); ++i) {  \
      OB_UNIS_ADD_LEN(objs[i]);                   \
    }

#define OB_UNIS_DECODE_ARRAY(objs, objs_count)                  \
  for (int64_t i = 0; OB_SUCC(ret) && i < (objs_count); ++i) {  \
    OB_UNIS_DECODE(objs[i]);                                    \
  }

#define OB_UNIS_DEF_SERIALIZE(CLS, ...)  \
  OB_UNIS_SERIALIZE(MY_CLS(CLS));                                  \
  OB_SERIALIZE_NOHEADER(MY_CLS(CLS), BASE_CLS(CLS), _, true, ##__VA_ARGS__);

#define OB_UNIS_DEF_SERIALIZE_SIZE(CLS, ...)  \
  OB_UNIS_SERIALIZE_SIZE(MY_CLS(CLS));                            \
  OB_SERIALIZE_SIZE_NOHEADER(MY_CLS(CLS), BASE_CLS(CLS), _, true, ##__VA_ARGS__);

#define BASE_ADD_LEN(Base) len += BASE_CLS(Base)::get_serialize_size()
#define BASE_SER(Base) if (OB_SUCC(ret) && OB_FAIL(BASE_CLS(Base)::serialize(buf, buf_len, pos))) { \
    RPC_WARN("serialize base failed", K(ret));                           \
  }
#define BASE_DESER(Base) if (OB_SUCC(ret) && OB_FAIL(BASE_CLS(Base)::deserialize(buf, data_len, pos))) { \
    RPC_WARN("deserialize base failed", K(ret));                           \
  }
#define SERIALIZE_SIZE_HEADER(version)          \
  OB_UNIS_ADD_LEN(version);                     \
  len += NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
// Define dummy node to replace obsolete member.
template <int N>
    struct UNFDummy {
  OB_UNIS_VERSION(N);
};
OB_SERIALIZE_MEMBER_TEMP(template<int N>, UNFDummy<N>);

#define OB_SERIALIZE_MEMBER_IF(CLS, PRED, ...)                          \
  OB_UNIS_SERIALIZE(CLS);                                          \
  OB_UNIS_DESERIALIZE(CLS);                                        \
  OB_UNIS_SERIALIZE_SIZE(CLS);                                     \
  OB_SERIALIZE_NOHEADER(CLS, EmptyParent, _, PRED, ##__VA_ARGS__); \
  OB_DESERIALIZE_NOHEADER(CLS, EmptyParent, _,##__VA_ARGS__);      \
  OB_SERIALIZE_SIZE_NOHEADER(CLS, EmptyParent, _, PRED, ##__VA_ARGS__);

inline uint64_t &get_unis_compat_version()
{
  static uint64_t x;
  return x;
}

#define UNIS_VERSION_GUARD(x)
}  // namespace lib
}  // namespace oceanbase

#endif /* _OCEABASE_LIB_UTILITY_OB_UNIFY_SERIALIZE_H_ */
