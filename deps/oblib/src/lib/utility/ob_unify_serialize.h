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

#define UNIS_DEF_HAS_MEMBER(m)                   \
  template <typename _T, typename = void>        \
  struct __unis_has_member : std::false_type {}; \
                                                 \
  template <typename _T>                         \
  struct __unis_has_member<_T, decltype((void)_T::m, void())> : std::true_type {};

#define UNIS_HAS_COMPAT(CLS) \
  __unis_has_member<CLS>     \
  {}

namespace oceanbase {
namespace lib {
inline uint64_t& get_unis_global_compat_version()
{
  static uint64_t global_version;
  return global_version;
}

inline uint64_t& get_unis_compat_version()
{
  static RLOCAL(uint64_t, compat_version);
  return compat_version;
}

class UnisCompatVersionGuard {
public:
  UnisCompatVersionGuard(uint64_t version) : version_(get_unis_compat_version())
  {
    get_unis_compat_version() = version;
  }
  ~UnisCompatVersionGuard()
  {
    get_unis_compat_version() = version_;
  }

private:
  uint64_t version_;
};
}  // namespace lib
}  // namespace oceanbase

#define UNIS_VERSION_GUARD(v) ::oceanbase::lib::UnisCompatVersionGuard __unis_guard(v)

#define SERIAL_PARAMS char *buf, const int64_t buf_len, int64_t &pos
#define DESERIAL_PARAMS const char *buf, const int64_t data_len, int64_t &pos
#define SERIALIZE_SIGNATURE(func) int func(SERIAL_PARAMS) const
#define DESERIALIZE_SIGNATURE(func) int func(DESERIAL_PARAMS)
#define GET_SERIALIZE_SIZE_SIGNATURE(func) int64_t func(void) const
#define SERIALIZE_DISPATCH_NAME serialize_dispatch_
#define DESERIALIZE_DISPATCH_NAME deserialize_dispatch_
#define GET_SERIALIZE_SIZE_DISPATCH_NAME get_serialize_size_dispatch_

#define UNF_UNUSED_SER \
  ({                   \
    (void)buf;         \
    (void)buf_len;     \
    (void)pos;         \
  })
#define UNF_UNUSED_DES \
  ({                   \
    (void)buf;         \
    (void)data_len;    \
    (void)pos;         \
  })

#ifndef RPC_WARN
#define RPC_WARN(...) OB_LOG(WARN, __VA_ARGS__)
#endif

///
// define essential macros used for encode/decode single object
//----------------------------------------------------------------------
#define NS_ ::oceanbase::common::serialization
#define OK_ ::oceanbase::common::OB_SUCCESS

#define OB_UNIS_ENCODE(obj)                                                          \
  if (OB_SUCC(ret)) {                                                                \
    if (OB_FAIL(NS_::encode(buf, buf_len, pos, obj))) {                              \
      RPC_WARN("encode object fail", "name", MSTR(obj), K(buf_len), K(pos), K(ret)); \
    }                                                                                \
  }

#define OB_UNIS_DECODEx(obj)                                                          \
  if (OB_SUCC(ret)) {                                                                 \
    if (OB_FAIL(NS_::decode(buf, data_len, pos, obj))) {                              \
      RPC_WARN("decode object fail", "name", MSTR(obj), K(data_len), K(pos), K(ret)); \
    }                                                                                 \
  }

#define OB_UNIS_DECODE(obj)                                                           \
  if (OB_SUCC(ret) && pos < data_len) {                                               \
    if (OB_FAIL(NS_::decode(buf, data_len, pos, obj))) {                              \
      RPC_WARN("decode object fail", "name", MSTR(obj), K(data_len), K(pos), K(ret)); \
    }                                                                                 \
  }

#define OB_UNIS_ADD_LEN(obj) len += NS_::encoded_length(obj)
//-----------------------------------------------------------------------

/// utility macros to deal with C native array
#define OB_UNIS_ENCODE_ARRAY(objs, objs_count)                 \
  OB_UNIS_ENCODE((objs_count));                                \
  for (int64_t i = 0; OB_SUCC(ret) && i < (objs_count); ++i) { \
    OB_UNIS_ENCODE(objs[i]);                                   \
  }

#define OB_UNIS_ADD_LEN_ARRAY(objs, objs_count) \
  OB_UNIS_ADD_LEN((objs_count));                \
  for (int64_t i = 0; i < (objs_count); ++i) {  \
    OB_UNIS_ADD_LEN(objs[i]);                   \
  }

#define OB_UNIS_DECODE_ARRAY(objs, objs_count)                 \
  for (int64_t i = 0; OB_SUCC(ret) && i < (objs_count); ++i) { \
    OB_UNIS_DECODE(objs[i]);                                   \
  }

///
// define macros deal with parent class
//-----------------------------------------------------------------------
#define UNF_CONCAT_(a, b) a##b
#define UNF_CONCAT(a, b) UNF_CONCAT_(a, b)
#define UNF_IGNORE(...)
#define UNF_uSELF(...) __VA_ARGS__
#define UNF_SAFE_DO(M)                            \
  do {                                            \
    if (OB_SUCC(ret)) {                           \
      if (OB_FAIL((M))) {                         \
        RPC_WARN("fail to execute: " #M, K(ret)); \
      }                                           \
    }                                             \
  } while (0)

#define UNF_MYCLS_ UNF_uSELF(
#define MYCLS_(D, B) uSELF( D
#define UNF_MYCLS(x) UNF_CONCAT(UNF_, MYCLS_ x) )

#define UNF_SBASE_ UNF_IGNORE(
#define SBASE_(D, B) \
  uSELF( UNF_SAFE_DO(B::serialize(buf, buf_len, pos))
#define BASE_SER(x) UNF_CONCAT(UNF_, SBASE_ x) )

#define UNF_DBASE_ UNF_IGNORE(
#define DBASE_(D, B) \
  uSELF( UNF_SAFE_DO(B::deserialize(buf, data_len, pos))
#define BASE_DESER(x) UNF_CONCAT(UNF_, DBASE_ x) )

#define UNF_LBASE_ UNF_IGNORE(
#define LBASE_(D, B) uSELF( len += B::get_serialize_size()
#define BASE_ADD_LEN(x) UNF_CONCAT(UNF_, LBASE_ x) )

///
// define serialize/desrialize wrapper which helps hide "version" and
// "length"
//-----------------------------------------------------------------------
#define CHECK_VERSION_LENGTH(CLS, VER, LEN)                             \
  if (OB_SUCC(ret)) {                                                   \
    if (VER != UNIS_VERSION) {                                          \
      ret = ::oceanbase::common::OB_NOT_SUPPORTED;                      \
      RPC_WARN("object version mismatch", "cls", #CLS, K(ret), K(VER)); \
    } else if (LEN < 0) {                                               \
      ret = ::oceanbase::common::OB_ERR_UNEXPECTED;                     \
      RPC_WARN("can't decode object with negative length", K(LEN));     \
    } else if (data_len < LEN + pos) {                                  \
      ret = ::oceanbase::common::OB_DESERIALIZE_ERROR;                  \
      RPC_WARN("buf length not enough", K(LEN), K(pos), K(data_len));   \
    }                                                                   \
  }

#define CALL_SERIALIZE_(SUFFIX, ...)                                     \
  if (OB_SUCC(ret)) {                                                    \
    if (OB_FAIL(serialize_##SUFFIX(buf, buf_len, pos, ##__VA_ARGS__))) { \
      RPC_WARN("serialize fail", K(ret));                                \
    }                                                                    \
  }

#define CALL_DESERIALIZE_(SLEN, SUFFIX, ...)                                       \
  if (OB_SUCC(ret)) {                                                              \
    int64_t pos_orig = pos;                                                        \
    pos = 0;                                                                       \
    if (OB_FAIL(deserialize_##SUFFIX(buf + pos_orig, SLEN, pos, ##__VA_ARGS__))) { \
      RPC_WARN("deserialize_ fail", "slen", SLEN, K(pos), K(ret));                 \
    }                                                                              \
    pos = pos_orig + SLEN;                                                         \
  }

#define CALL_GET_SERIALIZE_SIZE_(SUFFIX, ...) get_serialize_size_##SUFFIX(__VA_ARGS__)

#define SERIALIZE_HEADER(version) \
  if (OB_SUCC(ret)) {             \
    OB_UNIS_ENCODE(version);      \
  }

// dispatch compatible function depend on current
// get_unis_compat_version().
#define OB_UNIS_DEFINE_DISPATCH()                                   \
  int SERIALIZE_DISPATCH_NAME(SERIAL_PARAMS, std::false_type) const \
  {                                                                 \
    int ret = OK_;                                                  \
    CALL_SERIALIZE_();                                              \
    return ret;                                                     \
  }                                                                 \
  int DESERIALIZE_DISPATCH_NAME(DESERIAL_PARAMS, std::false_type)   \
  {                                                                 \
    int ret = OK_;                                                  \
    CALL_DESERIALIZE_(data_len, );                                  \
    return ret;                                                     \
  }                                                                 \
  int64_t GET_SERIALIZE_SIZE_DISPATCH_NAME(std::false_type) const   \
  {                                                                 \
    return CALL_GET_SERIALIZE_SIZE_();                              \
  }

#define OB_UNIS_DEFINE_COMPAT_DISPATCH()                                                        \
  int SERIALIZE_DISPATCH_NAME(SERIAL_PARAMS, std::true_type) const                              \
  {                                                                                             \
    int ret = OK_;                                                                              \
    if (OB_LIKELY(get_unis_compat_version() == 0 || get_unis_compat_version() > compat_ver_)) { \
      CALL_SERIALIZE_();                                                                        \
    } else {                                                                                    \
      CALL_SERIALIZE_(compat_);                                                                 \
    }                                                                                           \
    return ret;                                                                                 \
  }                                                                                             \
  int DESERIALIZE_DISPATCH_NAME(DESERIAL_PARAMS, std::true_type)                                \
  {                                                                                             \
    int ret = OK_;                                                                              \
    if (OB_LIKELY(get_unis_compat_version() == 0 || get_unis_compat_version() > compat_ver_)) { \
      CALL_DESERIALIZE_(data_len, );                                                            \
    } else {                                                                                    \
      CALL_DESERIALIZE_(data_len, compat_);                                                     \
    }                                                                                           \
    return ret;                                                                                 \
  }                                                                                             \
  int64_t GET_SERIALIZE_SIZE_DISPATCH_NAME(std::true_type) const                                \
  {                                                                                             \
    if (OB_LIKELY(get_unis_compat_version() == 0 || get_unis_compat_version() > compat_ver_)) { \
      return CALL_GET_SERIALIZE_SIZE_();                                                        \
    } else {                                                                                    \
      return CALL_GET_SERIALIZE_SIZE_(compat_);                                                 \
    }                                                                                           \
  }

#ifdef NDEBUG
#define CHECK_SERIALIZE_SIZE(CLS, real_size)
#else
#define CHECK_SERIALIZE_SIZE(CLS, real_size)                                       \
  int64_t expect_size = CALL_GET_SERIALIZE_SIZE_(dispatch_, UNIS_HAS_COMPAT(CLS)); \
  assert(expect_size >= real_size);
#endif

#define OB_UNIS_SERIALIZE(CLS)                                                                             \
  int CLS::serialize(SERIAL_PARAMS) const                                                                  \
  {                                                                                                        \
    int ret = OK_;                                                                                         \
    SERIALIZE_HEADER(UNIS_VERSION);                                                                        \
    if (OB_SUCC(ret)) {                                                                                    \
      int64_t size_nbytes = NS_::OB_SERIALIZE_SIZE_NEED_BYTES;                                             \
      int64_t pos_bak = (pos += size_nbytes);                                                              \
      CALL_SERIALIZE_(dispatch_, UNIS_HAS_COMPAT(CLS));                                                    \
      int64_t serial_size = pos - pos_bak;                                                                 \
      int64_t tmp_pos = 0;                                                                                 \
      CHECK_SERIALIZE_SIZE(CLS, serial_size);                                                              \
      if (OB_SUCC(ret)) {                                                                                  \
        ret = NS_::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes, size_nbytes, tmp_pos, serial_size); \
      }                                                                                                    \
    }                                                                                                      \
    return ret;                                                                                            \
  }

#define DESERIALIZE_HEADER(CLS, version, len) \
  if (OB_SUCC(ret)) {                         \
    OB_UNIS_DECODEx(version);                 \
    OB_UNIS_DECODEx(len);                     \
    CHECK_VERSION_LENGTH(CLS, version, len);  \
  }

#define OB_UNIS_DESERIALIZE(CLS)                             \
  int CLS::deserialize(DESERIAL_PARAMS)                      \
  {                                                          \
    int ret = OK_;                                           \
    int64_t version = 0;                                     \
    int64_t len = 0;                                         \
    DESERIALIZE_HEADER(CLS, version, len);                   \
    CALL_DESERIALIZE_(len, dispatch_, UNIS_HAS_COMPAT(CLS)); \
    return ret;                                              \
  }

#define SERIALIZE_SIZE_HEADER(version) \
  OB_UNIS_ADD_LEN(version);            \
  len += NS_::OB_SERIALIZE_SIZE_NEED_BYTES;

#define OB_UNIS_SERIALIZE_SIZE(CLS)                                          \
  int64_t CLS::get_serialize_size(void) const                                \
  {                                                                          \
    int64_t len = CALL_GET_SERIALIZE_SIZE_(dispatch_, UNIS_HAS_COMPAT(CLS)); \
    SERIALIZE_SIZE_HEADER(UNIS_VERSION);                                     \
    return len;                                                              \
  }

//-----------------------------------------------------------------------

///
// macro to declare unis structure, here would be non-implement
// functions in pure class but it's ok coz they wouldn't be invoked
// and the derived class should overwrite these functions.
// -----------------------------------------------------------------------
#define OB_DECLARE_UNIS(VIR, PURE)             \
  VIR int serialize(SERIAL_PARAMS) const PURE; \
  int serialize_(SERIAL_PARAMS) const;         \
  VIR int deserialize(DESERIAL_PARAMS) PURE;   \
  int deserialize_(DESERIAL_PARAMS);           \
  VIR int64_t get_serialize_size() const PURE; \
  int64_t get_serialize_size_() const;         \
  UNIS_DEF_HAS_MEMBER(compat_ver_);            \
  OB_UNIS_DEFINE_DISPATCH()

#define OB_DECLARE_UNIS_COMPAT()              \
  int serialize_compat_(SERIAL_PARAMS) const; \
  int deserialize_compat_(DESERIAL_PARAMS);   \
  int64_t get_serialize_size_compat_() const; \
  OB_UNIS_DEFINE_COMPAT_DISPATCH()

//-----------------------------------------------------------------------

///
// public entries, define interfaces of manual serialization
//-----------------------------------------------------------------------
#define OB_UNIS_VERSION(VER) \
public:                      \
  OB_DECLARE_UNIS(, );       \
                             \
private:                     \
  const static int64_t UNIS_VERSION = VER

#define OB_UNIS_VERSION_V(VER) \
public:                        \
  OB_DECLARE_UNIS(virtual, );  \
                               \
private:                       \
  const static int64_t UNIS_VERSION = VER

#define OB_UNIS_VERSION_PV()     \
public:                          \
  OB_DECLARE_UNIS(virtual, = 0); \
                                 \
private:

#define OB_DEF_SERIALIZE(CLS, TEMP...) \
  TEMP OB_UNIS_SERIALIZE(CLS);         \
  TEMP int CLS::serialize_(SERIAL_PARAMS) const

#define OB_DEF_DESERIALIZE(CLS, TEMP...) \
  TEMP OB_UNIS_DESERIALIZE(CLS);         \
  TEMP int CLS::deserialize_(DESERIAL_PARAMS)

#define OB_DEF_SERIALIZE_SIZE(CLS, TEMP...) \
  TEMP OB_UNIS_SERIALIZE_SIZE(CLS);         \
  TEMP int64_t CLS::get_serialize_size_(void) const

/// {{{ helper functions
#define OB_DEF_SERIALIZE_COMPAT_(CLS, TEMP...) TEMP int CLS::serialize_compat_(SERIAL_PARAMS) const
#define OB_DEF_DESERIALIZE_COMPAT_(CLS, TEMP...) TEMP int CLS::deserialize_compat_(DESERIAL_PARAMS)
#define OB_DEF_SERIALIZE_SIZE_COMPAT_(CLS, TEMP...) TEMP int64_t CLS::get_serialize_size_compat_(void) const
/// }}}

#define OB_DEF_SERIALIZE_COMPAT(VER, CLS, TEMP...) OB_DEF_SERIALIZE_COMPAT_(CLS, ##TEMP)
#define OB_DEF_DESERIALIZE_COMPAT(VER, CLS, TEMP...) OB_DEF_DESERIALIZE_COMPAT_(CLS, ##TEMP)
#define OB_DEF_SERIALIZE_SIZE_COMPAT(VER, CLS, TEMP...) OB_DEF_SERIALIZE_SIZE_COMPAT_(CLS, ##TEMP)

//-----------------------------------------------------------------------

///
// public entries, define interfaces of list encode/decode members
//-----------------------------------------------------------------------
#define OB_SERIALIZE_MEMBER_COMPAT_TEMP_IF(COMPAT, TEMP, CLS, PRED, ...) \
  OB_DEF_SERIALIZE##COMPAT(UNF_MYCLS(CLS), TEMP)                         \
  {                                                                      \
    int ret = OK_;                                                       \
    UNF_UNUSED_SER;                                                      \
    BASE_SER(CLS);                                                       \
    if (PRED) {                                                          \
      LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__);                        \
    }                                                                    \
    return ret;                                                          \
  }                                                                      \
  OB_DEF_DESERIALIZE##COMPAT(UNF_MYCLS(CLS), TEMP)                       \
  {                                                                      \
    int ret = OK_;                                                       \
    UNF_UNUSED_DES;                                                      \
    BASE_DESER(CLS);                                                     \
    LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__);                          \
    return ret;                                                          \
  }                                                                      \
  OB_DEF_SERIALIZE_SIZE##COMPAT(UNF_MYCLS(CLS), TEMP)                    \
  {                                                                      \
    int64_t len = 0;                                                     \
    BASE_ADD_LEN(CLS);                                                   \
    if (PRED) {                                                          \
      LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__);                       \
    }                                                                    \
    return len;                                                          \
  }

#define OB_SERIALIZE_MEMBER_TEMP_IF(TEMP, CLS, PRED, ...) \
  OB_SERIALIZE_MEMBER_COMPAT_TEMP_IF(, TEMP, CLS, PRED, ##__VA_ARGS__)

#define OB_SERIALIZE_MEMBER_TEMP_IF_COMPAT(VER, TEMP, CLS, PRED, ...) \
  OB_SERIALIZE_MEMBER_COMPAT_TEMP_IF(_COMPAT_, TEMP, CLS, PRED, ##__VA_ARGS__)

#define OB_SERIALIZE_MEMBER_TEMP(TEMP, CLS, ...) OB_SERIALIZE_MEMBER_TEMP_IF(TEMP, CLS, true, ##__VA_ARGS__)

#define OB_SERIALIZE_MEMBER_TEMP_COMPAT(VER, TEMP, CLS, ...) \
  OB_SERIALIZE_MEMBER_TEMP_IF_COMPAT(VER, TEMP, CLS, true, ##__VA_ARGS__)

#define OB_SERIALIZE_MEMBER_IF(CLS, PRED, ...) OB_SERIALIZE_MEMBER_TEMP_IF(, CLS, PRED, ##__VA_ARGS__)

#define OB_SERIALIZE_MEMBER_IF_COMPAT(VER, CLS, PRED, ...) \
  OB_SERIALIZE_MEMBER_TEMP_IF_COMPAT(VER, , CLS, PRED, ##__VA_ARGS__)

#define OB_SERIALIZE_MEMBER(CLS, ...) OB_SERIALIZE_MEMBER_TEMP(, CLS, ##__VA_ARGS__)

#define OB_SERIALIZE_MEMBER_COMPAT(VER, CLS, ...) OB_SERIALIZE_MEMBER_TEMP_COMPAT(VER, , CLS, ##__VA_ARGS__)

#define OB_UNIS_DEF_SERIALIZE(CLS, ...)         \
  OB_DEF_SERIALIZE(UNF_MYCLS(CLS), )            \
  {                                             \
    int ret = OK_;                              \
    UNF_UNUSED_SER;                             \
    BASE_SER(CLS);                              \
    LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__); \
    return ret;                                 \
  }

#define OB_UNIS_DEF_DESERIALIZE(CLS, ...)       \
  OB_DEF_DESERIALIZE(UNF_MYCLS(CLS), )          \
  {                                             \
    int ret = OK_;                              \
    UNF_UNUSED_DES;                             \
    BASE_DESER(CLS);                            \
    LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__); \
    return ret;                                 \
  }

#define OB_UNIS_DEF_SERIALIZE_SIZE(CLS, ...)     \
  OB_DEF_SERIALIZE_SIZE(UNF_MYCLS(CLS), )        \
  {                                              \
    int64_t len = 0;                             \
    BASE_ADD_LEN(CLS);                           \
    LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__); \
    return len;                                  \
  }

//-----------------------------------------------------------------------
// Compatibility not guaranteed, **DONT** use this any more.

#define OB_DEF_SERIALIZE_SIMPLE(CLS) int CLS::serialize(SERIAL_PARAMS) const

#define OB_DEF_DESERIALIZE_SIMPLE(CLS) int CLS::deserialize(DESERIAL_PARAMS)

#define OB_DEF_SERIALIZE_SIZE_SIMPLE(CLS) int64_t CLS::get_serialize_size(void) const

#define OB_SERIALIZE_MEMBER_SIMPLE(CLS, ...)     \
  OB_DEF_SERIALIZE_SIMPLE(CLS)                   \
  {                                              \
    int ret = OK_;                               \
    LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__);  \
    return ret;                                  \
  }                                              \
  OB_DEF_DESERIALIZE_SIMPLE(CLS)                 \
  {                                              \
    int ret = OK_;                               \
    LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__);  \
    return ret;                                  \
  }                                              \
  OB_DEF_SERIALIZE_SIZE_SIMPLE(CLS)              \
  {                                              \
    int64_t len = 0;                             \
    LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__); \
    return len;                                  \
  }

#define OB_SERIALIZE_MEMBER_INHERIT(CLS, PARENT, ...)  \
  OB_DEF_SERIALIZE_SIMPLE(CLS)                         \
  {                                                    \
    int ret = PARENT::serialize(buf, buf_len, pos);    \
    if (OB_SUCC(ret)) {                                \
      LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__);      \
    }                                                  \
    return ret;                                        \
  }                                                    \
  OB_DEF_DESERIALIZE_SIMPLE(CLS)                       \
  {                                                    \
    int ret = PARENT::deserialize(buf, data_len, pos); \
    if (OB_SUCC(ret)) {                                \
      LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__);      \
    }                                                  \
    return ret;                                        \
  }                                                    \
  OB_DEF_SERIALIZE_SIZE_SIMPLE(CLS)                    \
  {                                                    \
    int64_t len = PARENT::get_serialize_size();        \
    LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__);       \
    return len;                                        \
  }

#define UNIS_VER(major, minor, patch) (((uint64_t)major << 32L) + ((uint64_t)minor << 16L) + (uint64_t)patch)
#define OB_UNIS_COMPAT(V)                       \
private:                                        \
  const static uint64_t compat_ver_ = UNIS_##V; \
  OB_DECLARE_UNIS_COMPAT()

// Define dummy node to replace obsolete member.
namespace oceanbase {
namespace lib {
template <int N>
struct UNFDummy {
  OB_UNIS_VERSION(N);
};
OB_SERIALIZE_MEMBER_TEMP(template <int N>, UNFDummy<N>);
}  // namespace lib
}  // namespace oceanbase

#endif /* _OCEABASE_LIB_UTILITY_OB_UNIFY_SERIALIZE_H_ */
