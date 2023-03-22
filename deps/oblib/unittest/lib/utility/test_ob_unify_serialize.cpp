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

#define USING_LOG_PREFIX LIB

#include <stdint.h>
#include <gtest/gtest.h>
#include <iostream>
#include <cstdlib>
#include "lib/utility/ob_unify_serialize.h"
#include "lib/random/ob_random.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObRandom Random;

class TestObUnifySerialize
    : public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }

protected:
  template <class T>
  int64_t do_test(const T &t)
  {
    int64_t pos = 0;
    t.serialize(buf_, LEN, pos);

    T t2;
    int64_t len = pos;
    pos = 0;
    t2.deserialize(buf_, len, pos);
    ASSERT_EQ(t, t2), len;
    return len;
  }

protected:
  const static int64_t LEN = 1024;
  char buf_[LEN];
};

struct CPureTest {
  OB_UNIS_VERSION_PV();
};

struct CPureTestChild {
  OB_UNIS_VERSION_V(1);
};

OB_SERIALIZE_MEMBER(CPureTestChild);

struct CVirtualTest {
  OB_UNIS_VERSION_V(1);
};

struct CEmptyTest {
  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(CEmptyTest);

struct CIntTest
    : public CPureTest
{
  OB_UNIS_VERSION(1);

public:
  CIntTest()
  {
    b_ = false;
    vi8_ = 0;
    vu8_ = 0;
    vi16_ = 0;
    vu16_ = 0;
    vi32_ = 0;
    vu32_ = 0;
    vi64_ = 0;
    vu64_ = 0;
  }

  bool operator==(const CIntTest &other) const
  {
    return b_ == other.b_
        && vi8_ == other.vi8_
        && vu8_ == other.vu8_
        && vi16_ == other.vi16_
        && vu16_ == other.vu16_
        && vi32_ == other.vi32_
        && vu32_ == other.vu32_
        && vi64_ == other.vi64_
        && vu64_ == other.vu64_;
  }

  static const CIntTest MMAX()
  {
    CIntTest t1;
    t1.b_ = true;
    t1.vi8_ = INT8_MAX;
    t1.vu8_ = UINT8_MAX;
    t1.vi16_ = INT16_MAX;
    t1.vu16_ = UINT16_MAX;
    t1.vi32_ = INT32_MAX;
    t1.vu32_ = UINT32_MAX;
    t1.vi64_ = INT64_MAX;
    t1.vu64_ = UINT64_MAX;
    return t1;
  }

  static const CIntTest MMIN()
  {
    CIntTest t1;
    t1.b_ = false;
    t1.vi8_ = INT8_MIN;
    t1.vu8_ = 0;
    t1.vi16_ = INT16_MIN;
    t1.vu16_ = 0;
    t1.vi32_ = INT32_MIN;
    t1.vu32_ = 0;
    t1.vi64_ = INT64_MIN;
    t1.vu64_ = 0;
    return t1;
  }

  static const CIntTest RAND()
  {
    CIntTest t1;
    t1.b_ = Random.get(0, 1) == 0;
    t1.vi8_ = (int8_t) Random.get(0, 127);
    t1.vu8_ = (uint8_t) Random.get(0, 127);
    t1.vi16_ = (int16_t) Random.get(0, 127);
    t1.vu16_ = (uint16_t) Random.get(0, 127);
    t1.vi32_ = (int32_t) Random.get(0, 127);
    t1.vu32_ = (uint32_t) Random.get(0, 127);
    t1.vi64_ = Random.get(0, 127);
    t1.vu64_ = Random.get(0, 127);
    return t1;
  }

public:
  bool b_;
  int8_t vi8_;
  uint8_t vu8_;
  int16_t vi16_;
  uint16_t vu16_;
  int32_t vi32_;
  uint32_t vu32_;
  int64_t vi64_;
  uint64_t vu64_;
}; // end of class TestObUnifySerialize

OB_SERIALIZE_MEMBER(CIntTest, b_, vi8_, vu8_, vi16_, vu16_, vi32_, vu32_, vi64_, vu64_);

struct CDerived
    : public CIntTest
{
  OB_UNIS_VERSION(1);

public:
  CDerived()
  {
    i64_ = 0;
  }

  static const CDerived RAND()
  {
    CDerived n;
    n.CIntTest::RAND();
    n.i64_ = ObRandom::rand(0, 127);
    return n;
  }

private:
  int64_t i64_;
};

OB_SERIALIZE_MEMBER((CDerived, CIntTest), i64_);

struct CCharTest
    : public CPureTest
{
  OB_UNIS_VERSION(1);

public:
  CCharTest()
  {
    memset(buf_, 0, 8);
  }

  bool operator==(const CCharTest &other) const
  {
    return 0 == strcmp(buf_, other.buf_);
  }

  static const CCharTest RAND()
  {
    CCharTest n;
    int64_t rlen = Random.get(0, (int64_t)sizeof (buf_) - 1);
    for (int64_t i = 0; i < rlen; i++) {
      n.buf_[i] = static_cast<char>('a' + Random.get(0, 25));
    }
    n.buf_[rlen] = 0;
    return n;
  }

private:
  char buf_[32];
};

OB_SERIALIZE_MEMBER(CCharTest, buf_);

struct CArrayTest
    : public CPureTest
{
  OB_UNIS_VERSION(1);

public:
  CArrayTest()
  {
    memset(ia_, 0, sizeof (int32_t) * SIZE);
    memset(uia_, 0, sizeof (uint32_t) * SIZE);
    memset(i64a_, 0, sizeof (int64_t) * SIZE);
    memset(ui64a_, 0, sizeof (uint64_t) * SIZE);
  }

  bool operator==(const CArrayTest &other) const
  {
    bool right = true;
    for (int i = 0; i < SIZE; i++) {
      if (other.ia_[i] != ia_[i]) {
        right = false;
        break;
      }
      if (other.uia_[i] != uia_[i]) {
        right = false;
        break;
      }
      if (other.i64a_[i] != i64a_[i]) {
        right = false;
        break;
      }
      if (other.ui64a_[i] != ui64a_[i]) {
        right = false;
        break;
      }
      if (!(other.ita_[i] == ita_[i])) {
        right = false;
        break;
      }
    }
    return right;
  }

  static const CArrayTest RAND()
  {
    CArrayTest n;
    int rlen = SIZE;
    for (int i = 0; i < rlen; i++) {
      n.ia_[i] = (int32_t) Random.get(0, 127);
      n.uia_[i] = (uint32_t) Random.get(0, 127);
      n.i64a_[i] = (int64_t) Random.get(0, 127);
      n.ui64a_[i] = (uint64_t) Random.get(0, 127);
      n.ita_[i] = CIntTest::RAND();
    }
    return n;
  }

private:
  static const int SIZE = 32;
private:
  int32_t ia_[SIZE];
  uint32_t uia_[SIZE];
  int64_t i64a_[SIZE];
  uint64_t ui64a_[SIZE];
  CIntTest ita_[SIZE];
};

OB_SERIALIZE_MEMBER(CArrayTest, ia_, uia_, i64a_, ui64a_, ita_);

struct CEnumTest
    : public CPureTest
{
  OB_UNIS_VERSION(1);

public:
  CEnumTest()
  {
    eval = E0;
  }

  bool operator==(const CEnumTest &other) const {
    return eval == other.eval;
  }

  static const CEnumTest RAND()
  {
    CEnumTest et;
    int rnd = (int) Random.get(0, EMAX-1);
    et.eval = (Eval)rnd;

    return et;
  }

private:
  enum Eval { E0, E1, E2, E3, EMAX } eval;
};

OB_SERIALIZE_MEMBER(CEnumTest, eval);

struct CNested
{
  OB_UNIS_VERSION(1);

public:
  bool operator==(const CNested &other) const
  {
    return t1_ == other.t1_
        && t2_ == other.t2_
        && ct_ == other.ct_
        && at_ == other.at_
        && et_ == other.et_
        && t3_ == other.t3_;
  }

  static const CNested RAND()
  {
    CNested n;
    n.t1_ = n.t1_.RAND();
    n.t2_ = n.t2_.RAND();
    n.ct_ = n.ct_.RAND();
    n.at_ = n.at_.RAND();
    n.et_ = n.et_.RAND();
    n.t3_ = n.t3_.RAND();
    return n;
  }

public:
  CIntTest t1_;
  CIntTest t2_;
  CCharTest ct_;
  CArrayTest at_;
  CEnumTest et_;
  CIntTest t3_;
};

OB_SERIALIZE_MEMBER(CNested, t1_, t2_, ct_, at_, et_, t3_);

struct CNestedAddOne
{
  OB_UNIS_VERSION(1);

public:
  bool operator==(const CNestedAddOne &other) const
  {
    return t1_ == other.t1_
        && t2_ == other.t2_
        && ct_ == other.ct_
        && at_ == other.at_
        && et_ == other.et_
        && t3_ == other.t3_
        && t4_ == other.t4_;
  }

  static const CNestedAddOne RAND()
  {
    CNestedAddOne n;
    n.t1_ = n.t1_.RAND();
    n.t2_ = n.t2_.RAND();
    n.ct_ = n.ct_.RAND();
    n.at_ = n.at_.RAND();
    n.et_ = n.et_.RAND();
    n.t3_ = n.t3_.RAND();
    n.t4_ = n.t4_.RAND();
    return n;
  }

public:
  CIntTest t1_;
  CIntTest t2_;
  CCharTest ct_;
  CArrayTest at_;
  CEnumTest et_;
  CIntTest t3_;
  CIntTest t4_;
};

OB_SERIALIZE_MEMBER(CNestedAddOne, t1_, t2_, ct_, at_, et_, t3_, t4_);

struct CNestedStub
{
  OB_UNIS_VERSION(1);

public:
  bool operator==(const CNestedStub &) const
  {
    return true;
  }

  static const CNestedStub RAND()
  {
    return CNestedStub();
  }

public:
  CEmptyTest t1_;
  CEmptyTest t2_;
  CCharTest ct_;
  CArrayTest at_;
  CEnumTest et_;
  CEmptyTest t3_;
  CEmptyTest t4_;
};

OB_SERIALIZE_MEMBER(CNestedStub, t1_, t2_, ct_, at_, et_, t3_, t4_);

TEST_F(TestObUnifySerialize, CIntTest)
{
  int cnt = 100000;
  while (cnt--) {
    CIntTest t1 = CIntTest::RAND();
    do_test(t1);
  }

  CIntTest t1 = CIntTest::MMAX();
  do_test(t1);

  t1 = CIntTest::MMIN();
  do_test(t1);

  t1 = CIntTest::RAND();
  t1.vi64_ = -1;
  do_test(t1);
}

TEST_F(TestObUnifySerialize, CDerived)
{
  int cnt = 100000;
  while (cnt--) {
    CDerived t1 = CDerived::RAND();
    do_test(t1);
  }
}

TEST_F(TestObUnifySerialize, CCharTest)
{
  int cnt = 100000;
  while (cnt--) {
    CCharTest n = CCharTest::RAND();
    do_test(n);
  }
}

TEST_F(TestObUnifySerialize, CArrayTest)
{
  int cnt = 10000;
  while (cnt--) {
    CArrayTest n = CArrayTest::RAND();
    do_test(n);
  }
}

TEST_F(TestObUnifySerialize, CEnumTest)
{
  int cnt = 100000;
  while (cnt--) {
    CEnumTest n = CEnumTest::RAND();
    do_test(n);
  }
}

TEST_F(TestObUnifySerialize, CNested)
{
  int cnt = 10000;
  while (cnt--) {
    CNested n = CNested::RAND();
    do_test(n);
  }
}

TEST_F(TestObUnifySerialize, CNested2)
{
  int64_t pos = 0;
  CNested n = CNested::RAND();

  EXPECT_EQ(OB_SUCCESS, n.serialize(buf_, LEN, pos));
  EXPECT_EQ(pos, n.get_serialize_size());
}

/*
TEST_F(TestObUnifySerialize, CNestedCompatibility)
{
  // encode a CNested object
  CNested n = CNested::RAND();
  {
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, encode(buf_, LEN, pos, n));

    pos = 0;
    EXPECT_EQ(OB_SUCCESS, decode(buf_, LEN, pos, n));
  }

  // decode as a CNestedAddOne
  {
    int64_t pos = 0;
    CNestedAddOne nn;
    CIntTest t4 = CIntTest::RAND();
    nn.t4_ = t4;
    EXPECT_EQ(OB_SUCCESS, decode(buf_, LEN, pos, nn));
    EXPECT_EQ(t4, nn.t4_);
  }

  // if packet not enough
  {
    int64_t pos = 0;
    CNestedAddOne nn;
    EXPECT_EQ(OB_DESERIALIZE_ERROR, decode(buf_, 0, pos, nn));
    pos = 0;
    EXPECT_EQ(OB_DESERIALIZE_ERROR, decode(buf_, 1, pos, nn));
    pos = 0;
    EXPECT_EQ(OB_DESERIALIZE_ERROR, decode(buf_, 2, pos, nn));
    pos = 0;
    EXPECT_EQ(OB_DESERIALIZE_ERROR, decode(buf_, 3, pos, nn));
    pos = 0;
    EXPECT_EQ(OB_DESERIALIZE_ERROR, decode(buf_, 4, pos, nn));
  }

  // if some members are logical deleted
  {
    CNestedStub ns;
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, decode(buf_, LEN, pos, ns));
    EXPECT_EQ(n.ct_, ns.ct_);
    EXPECT_EQ(n.at_, ns.at_);
    EXPECT_EQ(n.et_, ns.et_);
  }

  // the opposite of previous condition
  {
    CNestedStub ns;
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, encode(buf_, LEN, pos, ns));

    pos = 0;
    CNestedAddOne nn = CNestedAddOne::RAND();
    EXPECT_EQ(OB_SUCCESS, decode(buf_, LEN, pos, nn));
    EXPECT_EQ(nn.ct_, ns.ct_);
    EXPECT_EQ(nn.at_, ns.at_);
    EXPECT_EQ(nn.et_, ns.et_);
  }
}
*/

TEST_F(TestObUnifySerialize, Dummy)
{
  UNFDummy<1> dummy;
  // encode with CNested and decode using dummy.
  CNested n = CNested::RAND();
  int64_t pos = 0;
  int ret = n.serialize(buf_, LEN, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t len = pos;
  pos = 0;
  ret = dummy.deserialize(buf_, len, pos);

  // calculate encoded_length
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t elen = encoded_length(dummy);
  ASSERT_EQ(1 + OB_SERIALIZE_SIZE_NEED_BYTES, elen);  // 1 for version, OB_SERIALIZE_SIZE_NEED_BYTES for data_length
}

struct COptInt
{
  OB_UNIS_VERSION(1);
public:
  COptInt()
      : valid_(true), value_(0)
  {}

  bool operator==(const COptInt &rhs) const {
    return valid_ == rhs.valid_
        && value_ == rhs.value_;
  }

public:
  bool valid_;
  int value_;
};

// if (valid_) then serialize list else nothing.
OB_SERIALIZE_MEMBER_IF(COptInt, (valid_ == true), value_);

TEST_F(TestObUnifySerialize, OptionallySerialize)
{
  // serialize valid_ and value_;
  {
    COptInt st1;
    st1.value_ = 324;
    int64_t pos = 0;
    int ret = st1.serialize(buf_, LEN, pos);
    ASSERT_EQ(OB_SUCCESS, ret);

    COptInt st2;
    const int64_t len = pos;
    pos = 0;
    ret = st2.deserialize(buf_, len, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(len, pos);
    ASSERT_EQ(st1, st2);
  }

  // serialize nothing.
  {
    COptInt st1;
    st1.valid_ = false;
    st1.value_ = 324;
    int64_t pos = 0;
    int ret = st1.serialize(buf_, LEN, pos);
    ASSERT_EQ(OB_SUCCESS, ret);

    COptInt st2;
    const int64_t len = pos;
    pos = 0;
    ret = st2.deserialize(buf_, len, pos);
    cout << st2.valid_ << endl;
    cout << st2.value_ << endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(len, pos);
    ASSERT_EQ(COptInt(), st2);
  }
}

// unis compatibility support for refactor
class CCompat {
  OB_UNIS_VERSION(1);
  //OB_UNIS_COMPAT(VER(2, 2, 3));

public:
  bool operator==(const CCompat &rhs) const {
    return i1_ == rhs.i1_ && i2_ == rhs.i2_;
  }

public:
  int i1_;
  int i2_;
};

//OB_SERIALIZE_MEMBER(CCompat, i1_, i2_);
//OB_SERIALIZE_MEMBER_COMPAT(VER(2, 2, 3), CCompat, i2_, i1_);
/*
class CCompat2 : public CCompat {
  OB_UNIS_VERSION(1);
  OB_UNIS_COMPAT(VER(2, 2, 3));

public:
  CCompat2() {
    at_ = CArrayTest::RAND();
  }

  bool operator==(const CCompat2 &rhs) const {
    return l1_ == rhs.l1_ && l2_ == rhs.l2_ && i1_ == rhs.i1_ && i2_ == rhs.i2_;
  }
  bool operator!=(const CCompat2 &rhs) const {
    return !operator==(rhs);
  }
public:
  CArrayTest at_;
  long l1_;
  long l2_;
};

OB_SERIALIZE_MEMBER((CCompat2, CCompat), at_, l1_, l2_);
OB_SERIALIZE_MEMBER_COMPAT(VER(2, 2, 3), CCompat2, i1_, i2_, at_, l1_, l2_);

class CNoCompat : public CCompat {
  OB_UNIS_VERSION(1);
public:
  CNoCompat() {
    v_ = (int)Random.get(0, 127);
  }
  bool operator==(const CNoCompat &rhs) const {
    return v_ == rhs.v_ && static_cast<const CCompat&>(*this) == static_cast<const CCompat&>(rhs);
  }
private:
  int v_;
};

OB_SERIALIZE_MEMBER((CNoCompat, CCompat), v_);

TEST_F(TestObUnifySerialize, Compat)
{
  using namespace serialization;

  CCompat c1;
  c1.i1_ = 1;
  c1.i2_ = 2;

  {
    UNIS_VERSION_GUARD(UNIS_VER(2, 2, 3));
    do_test(c1);
  }
  do_test(c1);


  // use 2.2.3 to encode, and 2.2.4 to decode.
  // Result: c1.i1_ == c2.i2_ && c1.i2_ == c2.i1_
  {
    int64_t len = 0;
    {
      int64_t pos = 0;
      UNIS_VERSION_GUARD(UNIS_VER(2, 2, 3));
      encode(buf_, LEN, pos, c1);
      len = pos;
    }

    CCompat c2;
    {
      UNIS_VERSION_GUARD(UNIS_VER(2, 2, 4));
      int64_t pos = 0;
      decode(buf_, len, pos, c2);
    }
    EXPECT_EQ(2, c2.i1_);
    EXPECT_EQ(1, c2.i2_);
  }

  {
    // encode and decode with same/different version for refactored class
    // Compat2.
    CCompat2 c;
    c.i1_ = 123;
    c.i2_ = 456;
    c.l1_ = 653;
    c.l2_ = 321;
    {
      UNIS_VERSION_GUARD(UNIS_VER(2, 2, 3));
      int64_t encoded_len = do_test(c);
      {
        // encode with 2.2.3, but decode with 2.2.4
        UNIS_VERSION_GUARD(UNIS_VER(2, 2, 4));
        int64_t pos = 0;
        CCompat2 cc2;
        int ret = decode(buf_, encoded_len, pos, cc2);
        EXPECT_NE(OB_SUCCESS, ret);
        EXPECT_NE(c, cc2);
      }
    }
    {
      UNIS_VERSION_GUARD(UNIS_VER(2, 2, 4));
      int64_t encoded_len = do_test(c);
      {
        // encode with 2.2.4, but decode with 2.2.3
        UNIS_VERSION_GUARD(UNIS_VER(2, 2, 3));
        int64_t pos = 0;
        CCompat2 cc2;
        int ret = decode(buf_, encoded_len, pos, cc2);
        EXPECT_NE(OB_SUCCESS, ret);
        EXPECT_NE(c, cc2);
      }
    }
  }

  {
    CNoCompat cnc;
    do_test(cnc);
  }

}
*/

int main(int argc, char *argv[])
{
  srand((uint32_t)time(NULL));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
