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

#ifndef  OCEANBASE_UNITTEST_MEMTABLE_ROWKEY_BUILDER_H_
#define  OCEANBASE_UNITTEST_MEMTABLE_ROWKEY_BUILDER_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;

class ObStoreRowkeyWrapper
{
public:
  template <class... Args>
  ObStoreRowkeyWrapper(const Args&... args) : obj_cnt_(0)
  {
    fill(args...);
    rowkey_.get_rowkey().assign(objs_, obj_cnt_);
  }
  ObStoreRowkeyWrapper(const uint8_t obj_cnt) : obj_cnt_(obj_cnt) {}
  ObStoreRowkeyWrapper(const int32_t obj_cnt) : obj_cnt_(obj_cnt) {}
  ~ObStoreRowkeyWrapper() {}
public:
  CharArena &get_allocator() { return allocator_; }
  ObObj &get_cur_obj() { return objs_[obj_cnt_]; }
  ObObj &get_obj(int64_t idx) { return objs_[idx]; }
  int64_t get_obj_cnt() const { return obj_cnt_; }
  void add_obj() { obj_cnt_++; }
  ObStoreRowkey &get_rowkey() { return rowkey_; }
  const ObStoreRowkey &get_rowkey() const { return rowkey_; }
private:
  template <class T, class... Args>
  void fill(const T &head, const Args&... args)
  {
    head.build(*this);
    fill(args...);
  }
  void fill() { /*for recursion exit*/ }
private:
  CharArena allocator_;
  mutable ObObj objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
  int64_t obj_cnt_;
  mutable ObStoreRowkey rowkey_;
};

class ObColumnDesc
{
public:
  template <class... Args>
  ObColumnDesc(const Args&... args)
  {
    fill(args...);
  }
  const ObIArray<share::schema::ObColDesc> &get_columns() const { return columns_; }
private:
  template <class... Args>
  void fill(
      const uint64_t col_id,
      const ObObjType col_type,
      const ObCollationType col_collation,
      const Args&... args)
  {
    share::schema::ObColDesc col_desc;
    col_desc.col_id_ = col_id;
    col_desc.col_type_.set_type(col_type);
    col_desc.col_type_.set_collation_type(col_collation);
    columns_.push_back(col_desc);
    fill(args...);
  }
  void fill() { /*for recursion exit*/ }
private:
  ObSEArray<share::schema::ObColDesc, 64> columns_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

#define DEFINE_TYPE_OBJ(classname, ctype, obtype) \
  class classname \
  { \
  public: \
    classname(const ctype v) : v_(v) {} \
    ~classname() {} \
  public: \
    void build(ObStoreRowkeyWrapper &rowkey_wrapper) const \
    { \
      rowkey_wrapper.get_cur_obj().set_##obtype(v_); \
      rowkey_wrapper.add_obj(); \
    } \
  private: \
    const ctype v_; \
  };

#define DEFINE_CHARTYPE_OBJ(classname, obtype, cltype) \
  class classname \
  { \
  public: \
    classname( \
        const char *str, \
        const int64_t len, \
        const ObCollationType cltype = CS_TYPE_UTF8MB4_BIN) \
      : str_(str), len_(len), cltype_(cltype) {} \
    ~classname() {} \
  public: \
    void build(ObStoreRowkeyWrapper &rowkey_wrapper) const \
    { \
      ObString obstr; \
      obstr.assign_ptr(const_cast<char*>(str_), static_cast<int32_t>(len_)); \
      rowkey_wrapper.get_cur_obj().set_##obtype(obstr); \
      rowkey_wrapper.get_cur_obj().set_collation_type(cltype_); \
      rowkey_wrapper.add_obj(); \
    } \
  private: \
    const char *str_; \
    const int64_t len_; \
    const ObCollationType cltype_; \
  };

#define DEFINE_NMBTYPE_OBJ(classname, obtype) \
  class classname \
  { \
  public: \
    classname(const char *str) : str_(str) {} \
    ~classname() {} \
  public: \
    void build(ObStoreRowkeyWrapper &rowkey_wrapper) const \
    { \
      number::ObNumber obnmb; \
      obnmb.from(str_, rowkey_wrapper.get_allocator()); \
      rowkey_wrapper.get_cur_obj().set_##obtype(obnmb); \
      rowkey_wrapper.add_obj(); \
    } \
  private: \
    const char *str_; \
  };

class U
{
public:
  U() {}
  ~U() {}
public:
  void build(ObStoreRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_null();
    rowkey_wrapper.add_obj();
  }
};

class OBMIN
{
public:
  OBMIN() {}
  ~OBMIN() {}
public:
  void build(ObStoreRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_ext(ObObj::MIN_OBJECT_VALUE);
    rowkey_wrapper.add_obj();
  }
};

class OBMAX
{
public:
  OBMAX() {}
  ~OBMAX() {}
public:
  void build(ObStoreRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_ext(ObObj::MAX_OBJECT_VALUE);
    rowkey_wrapper.add_obj();
  }
};

DEFINE_TYPE_OBJ(IT,   int8_t,   tinyint)
DEFINE_TYPE_OBJ(IS,   int16_t,  smallint)
DEFINE_TYPE_OBJ(IM,   int32_t,  mediumint)
DEFINE_TYPE_OBJ(I32,  int32_t,  int32)
DEFINE_TYPE_OBJ(I,    int64_t,  int)

DEFINE_TYPE_OBJ(UIT,   uint8_t,   utinyint)
DEFINE_TYPE_OBJ(UIS,   uint16_t,  usmallint)
DEFINE_TYPE_OBJ(UIM,   uint32_t,  umediumint)
DEFINE_TYPE_OBJ(UI32,  uint32_t,  uint32)
DEFINE_TYPE_OBJ(UI,    uint64_t,  uint64)

DEFINE_TYPE_OBJ(F,  float,  float)
DEFINE_TYPE_OBJ(D,  double, double)

DEFINE_TYPE_OBJ(UF,  float,  ufloat)
DEFINE_TYPE_OBJ(UD,  double, udouble)

DEFINE_NMBTYPE_OBJ(N,   number)
DEFINE_NMBTYPE_OBJ(UN,  unumber)

DEFINE_TYPE_OBJ(T,  int64_t,  datetime)
DEFINE_TYPE_OBJ(TS, int64_t,  timestamp)
DEFINE_TYPE_OBJ(DD, int32_t,  date)
DEFINE_TYPE_OBJ(TT, int64_t,  time)
DEFINE_TYPE_OBJ(YY, uint8_t,  year)

DEFINE_CHARTYPE_OBJ(V,  varchar, ObCollationType)
DEFINE_CHARTYPE_OBJ(C,  char, ObCollationType)
DEFINE_CHARTYPE_OBJ(VB, varbinary, ObCollationType)
DEFINE_CHARTYPE_OBJ(BB,  binary, ObCollationType)

typedef ObStoreRowkeyWrapper RK;
typedef ObColumnDesc CD;

#define INIT_MTK(allocator, mtk, ...) \
{ \
  RK _rkb_(__VA_ARGS__); \
  ObMemtableKey tmp_mtk; \
  int ret = tmp_mtk.encode(&(_rkb_.get_rowkey())); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = tmp_mtk.dup(mtk, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
}

}
}

#endif //OCEANBASE_UNITTEST_MEMTABLE_ROWKEY_BUILDER_H_


