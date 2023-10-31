#define USING_LOG_PREFIX SQL

// #include "lib/wide_integer/ob_wide_integer.h"
#include "share/datum/ob_datum.h"
#include "common/object/ob_object.h"
#include "lib/alloc/alloc_struct.h"
#define private public
#include "lib/wide_integer/ob_wide_integer.h"
#undef private
#include "share/object/ob_obj_cast.h"

#include <gtest/gtest.h>
#include <string>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <type_traits>
#include <utility>
#include <fstream>
#include <random>
#include <chrono>
#include <iterator>

#define CHECK_TOSTR(B)                                                                             \
  do {                                                                                             \
    for (int i = 0; i < vals_##B.size(); i++) {                                                    \
      char buf[256];                                                                               \
      int64_t pos = 0;                                                                             \
      int ret = wide::to_string(vals_##B[i], buf, sizeof(buf), pos);                               \
      ASSERT_EQ(ret, OB_SUCCESS);                                                                  \
      ASSERT_EQ(strs_##B[i], string(buf, pos));                                                    \
    }                                                                                              \
  } while (0)

namespace oceanbase
{
namespace common
{
namespace wide
{
using namespace std;

struct MockAllocator: public ObIAllocator
{
  void* alloc(const int64_t size) override
  {
    return malloc(size);
  }
  void* alloc(const int64_t num, const lib::ObMemAttr &attr) override {
    return malloc(num);
  }
  void free(void *ptr) override {
    std::free(ptr);
  }
};

template<typename T>
T parse_string(std::string input, ObIAllocator &allocator)
{
  ObDecimalInt *decint = nullptr;
  int32_t int_bytes = 0;
  int16_t scale, precision;
  int ret =
    wide::from_string(input.c_str(), input.size(), allocator, scale, precision, int_bytes, decint);
  switch (int_bytes) {
  case sizeof(int64_t):
    return *(decint->int64_v_);
  case sizeof(int32_t):
    return *(decint->int32_v_);
  case sizeof(int128_t): {
    return *(decint->int128_v_);
  }
  case sizeof(int256_t): {
    return *(decint->int256_v_);
  }
  case sizeof(int512_t): {
    return *(decint->int512_v_);
  }
  default:
    OB_ASSERT("abort");
  }
  return T();
}

template<>
int64_t parse_string<int64_t>(std::string input,  ObIAllocator &allocator)
{
  ObDecimalInt *decint = nullptr;
  int32_t int_bytes = 0;
  int16_t scale, precision;
  int ret =
    wide::from_string(input.c_str(), input.size(), allocator, scale, precision, int_bytes, decint);
  if (int_bytes <= sizeof(int32_t)) {
    return *(decint->int32_v_);
  } else {
    return *(decint->int64_v_);
  }
}

bool is_equal_content(std::string lfile, std::string rfile)
{
  std::ifstream lh(lfile);
  std::ifstream rh(rfile);
  EXPECT_TRUE(lh.is_open());
  EXPECT_TRUE(rh.is_open());

  std::istream_iterator<std::string> l_iter(lh);
  std::istream_iterator<std::string> r_iter(rh);

  return std::equal(l_iter, std::istream_iterator<std::string>(), r_iter);
}

template<typename T>
T rand_value(const T min_v, const T max_v)
{
  return min_v;
}

template<>
int64_t rand_value<int64_t>(const int64_t min_v, const int64_t max_v)
{
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> dist(min_v, max_v);
  return dist(rng);
}

class TestWideInteger
{
public:
  void to_string()
  {
    vector<int128_t> vals_128 = {int128_t(-12345678),
                                 int128_t(12345678),
                                 Limits<int128_t>::max(),
                                 Limits<int128_t>::min(),
                                 int128_t{1,1}
                                 };
    vector<string> strs_128 = {"-12345678",
                               "12345678",
                               "170141183460469231731687303715884105727",
                               "-170141183460469231731687303715884105728",
                               "18446744073709551617"};

    vector<int256_t> vals_256 = {int256_t(-123456789),
                                 int256_t(123456789),
                                 Limits<int256_t>::max(),
                                 Limits<int256_t>::min(),
                                 int256_t{1, 1, 2, 2},
                                 int256_t{1, 2, 2, UINT64_MAX}};
    vector<string> strs_256 = {"-123456789",
                "123456789",
                "57896044618658097711785492504343953926634992332820282019728792003956564819967",
                "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
                "12554203470773361528352143580257209759149906847865315000321",
                "-6277101735386680763155224689365789489138712741453078986751"};

    vector<int512_t> vals_512 = {int512_t(-1234567890), int512_t(1234567890),
                                 Limits<int512_t>::max(),
                                 Limits<int512_t>::min()};
    vector<string> strs_512 = {
        "-1234567890", "1234567890",
        "6703903964971298549787012499102923063739682910296196688861780721860882"
        "0150367734884009371490834517138450159290932430254268769414059732849732"
        "16824503042047",
        "-670390396497129854978701249910292306373968291029619668886178072186088"
        "2015036773488400937149083451713845015929093243025426876941405973284973"
        "216824503042048"};

    CHECK_TOSTR(128);
    CHECK_TOSTR(256);
    CHECK_TOSTR(512);
  }

  void op_addsub_without_overflow(){
    int128_t a = 123456789;
    int256_t b = UINT64_MAX;
    int512_t c = UINT64_MAX;

    vector<int128_t> vals_128 = {a + 1, a + UINT64_MAX, a - 1,
                                 a - UINT64_MAX - UINT64_MAX};
    vector<string> strs_128 = {
      "123456790",
      "18446744073833008404",
      "123456788",
      "-36893488147295646441"
    };
    vector<int256_t> vals_256 = {b + 1, b + a, b - 1, b - a, b + b};
    vector<string> strs_256 = {"18446744073709551616", "18446744073833008404",
                                 "18446744073709551614", "18446744073586094826",
                                 "36893488147419103230"};
    vector<int512_t> vals_512 = {c + 1, c + b, c + a, c + c};
    vector<string> strs_512 = {
      "18446744073709551616", "36893488147419103230", "18446744073833008404",
      "36893488147419103230",
    };
    CHECK_TOSTR(128);
    CHECK_TOSTR(256);
    CHECK_TOSTR(512);
  }

  void op_mul_without_overflow()
  {
    int128_t a = 1234567890;
    int256_t b = UINT64_MAX;
    int512_t c = UINT64_MAX;

    vector<int128_t> vals_128 = { a * 1234, a * UINT64_MAX, a * a, a * -1234, a * -a};
    vector<string> strs_128 = {
      "1523456776260",
      "22773757908449605610176642350",
      "1524157875019052100",
      "-1523456776260",
      "-1524157875019052100"};

    vector<int256_t> vals_256 = { b * 123456789, b * b, b * a * a, b * b * -123456789, b * -1234};
    vector<string> strs_256 = {
      "2277375790844960561017664235",
      "340282366920938463426481119284349108225",
      "28115750248405442769487939873324141500",
      "-42010168373378879561227296555971718856471989525",
      "-22763282186957586692910"
    };

    vector<int512_t> vals_512 = {c * c, c * 1234, c * -1234, c * a, c * -a, c * b, c * -b};
    vector<string> strs_512 = {
      "340282366920938463426481119284349108225",
      "22763282186957586692910",
      "-22763282186957586692910",
      "22773757908449605610176642350",
      "-22773757908449605610176642350",
      "340282366920938463426481119284349108225",
      "-340282366920938463426481119284349108225"
    };

    CHECK_TOSTR(128);
    CHECK_TOSTR(256);
    CHECK_TOSTR(512);
  }

  void op_shift()
  {
    int128_t a = UINT64_MAX;
    int256_t b = UINT64_MAX;
    int512_t c = UINT64_MAX;

    vector<int128_t> vals_128 = {
      a >> 1, (a*12345678) >> 64, a >> 128, (-a) >> 2, a << 64, a << 128};
    vector<string> strs_128 = {
      "9223372036854775807",
      "12345677",
      "0",
      "-4611686018427387904",
      "-18446744073709551616",
      "0"
    };

    vector<int256_t> vals_256 = {
      b >> 1, (b * b * b * 12345678) >> 128, (b * b * b * b) >> 256, (-b * b) >> 16, b << 120, b << 256};
    vector<string> strs_256 = {
      "9223372036854775807",
      "227737562482426389738478614",
      "-1",
      "-5192296858534827627967546375798785",
      "24519928653854221732404324438620031064996018894657290240",
      "0"
    };

    vector<int512_t> vals_512 = {
      c >> 2, (c * c * c * c * c) >> 220, (c * c * c * c * c) >> 512,
      (-c) >> 512, (-c * b * a) >> 123, c << 400, c << 512
    };
    vector<string> strs_512 = {
      "4611686018427387903",
      "1267650600228229401153105821696",
      "0",
      "-1",
      "-590295810358705651617",
      "47634102635436893176458235195661356573744321042001064524"
      "278035900673152872968986946564609077560367837960487262708122405616220480360142602240",
      "0"
    };
    CHECK_TOSTR(128);
    CHECK_TOSTR(256);
    CHECK_TOSTR(512);
  }

  void op_div_without_overflow()
  {
    int128_t a = UINT64_MAX;
    int256_t b = UINT64_MAX;
    int512_t c = UINT64_MAX;

    vector<int128_t> vals_128 = {a/123, a%123, a/-123, a%-123, a/int128_t(123), a%(int128_t(123)),
                            a/int128_t(-123), a%int128_t(-123), -a/123, -a%123, -a/-123, -a%-123};
    vector<string> strs_128 = {
      "149973529054549200",
      "15",
      "-149973529054549200",
      "15",
      "149973529054549200",
      "15",
      "-149973529054549200",
      "15",
      "-149973529054549200",
      "-15",
      "149973529054549200",
      "-15"
    };

    vector<int256_t> vals_256 = {
      (b * b * b) / 1234567890123456789,
      (b * b * b) % 1234567890123456789,
      (b * b * b) / -1234567890123456789,
      (b * b * b) % -1234567890123456789,
      (b * b * b) / (a + a + 1234),
      (b * b * b) % (a + a + 1234),
      (b * b * b) / -(a + a + 1234),
      (b * b * b) % -(a + a + 1234),
      -(b * b * b) / -(a + a + 1234),
      -(b * b * b) % -(a + a + 1234)
    };
    vector<string> strs_256 = {
      "5084452451423283485316506379824074720926",
      "526538308263466761",
      "-5084452451423283485316506379824074720926",
      "526538308263466761",
      "170141183460469226022420012902778071229",
      "18446744073474667119",
      "-170141183460469226022420012902778071229",
      "18446744073474667119",
      "170141183460469226022420012902778071229",
      "-18446744073474667119"
    };
    CHECK_TOSTR(128);
    CHECK_TOSTR(256);
  }

  void op_addsub_overflow()
  {
    int128_t res_128;
    int128_t a = UINT64_MAX;
    int128_t max_128 = Limits<int128_t>::max();
    int128_t min_128 = Limits<int128_t>::min();

    int256_t res_256;
    int256_t b = (int256_t(UINT64_MAX) << 64) + UINT64_MAX;
    int256_t max_256 = Limits<int256_t>::max();
    int256_t min_256 = Limits<int256_t>::min();

    int512_t res_512;
    int512_t c = int512_t(UINT64_MAX) << 447;
    int512_t max_512 = Limits<int512_t>::max();
    int512_t min_512 = Limits<int512_t>::min();

    int ret = a.add<CheckOverFlow>((a<<63), res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = a.add<CheckOverFlow>(a, res_128);
    ASSERT_EQ(ret, 0);

    ret = max_128.add<CheckOverFlow>(max_128, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = max_128.add<CheckOverFlow>(min_128, res_128);
    ASSERT_EQ(ret, 0);

    ret = min_128.sub<CheckOverFlow>(1, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = min_128.sub<CheckOverFlow>(-1, res_128);
    ASSERT_EQ(ret, 0);

    ret = b.add<CheckOverFlow>(b << 127, res_256);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = max_256.add<CheckOverFlow>(1, res_256);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = b.add<CheckOverFlow>(123, res_256);
    ASSERT_EQ(ret, 0);
    ret = min_256.sub<CheckOverFlow>(1, res_256);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = min_256.sub<CheckOverFlow>(-1, res_256);
    ASSERT_EQ(ret, 0);

    ret = c.add<CheckOverFlow>(c, res_512);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = c.add<CheckOverFlow>(100, res_512);
    ASSERT_EQ(ret, 0);
    ret = max_512.add<CheckOverFlow>(1, res_512);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = max_512.sub<CheckOverFlow>(1, res_512);
    ASSERT_EQ(ret, 0);
    ret = min_512.add<CheckOverFlow>(-1, res_512);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = min_512.sub<CheckOverFlow>(1, res_512);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
    ret = min_512.add<CheckOverFlow>(1, res_512);
    ASSERT_EQ(ret, 0);
    ret = max_512.sub<CheckOverFlow>(-1, res_512);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
  }

  void op_muldiv128_overflow()
  {
    int128_t res_128;
    int128_t a;

    a = int128_t(1) << 64;
    int ret = a.multiply<CheckOverFlow>(UINT64_MAX, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = int128_t(1) << 65;
    ret = a.multiply<CheckOverFlow>(UINT64_MAX, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = int128_t(1) << 64;
    ret = a.multiply<CheckOverFlow>(int128_t(1)<<64, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = int128_t(2) << 64;
    ret = a.multiply<CheckOverFlow>(int128_t(UINT64_MAX), res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = int128_t(1) << 64;
    ret = a.multiply<CheckOverFlow>(int128_t(UINT64_MAX), res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = int128_t(1) << 64;
    ret = a.multiply<CheckOverFlow>(1234, res_128);
    ASSERT_EQ(ret, 0);
    char buf[256];
    int64_t pos = 0;
    ret = wide::to_string(res_128, buf, sizeof(buf), pos);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ("22763282186957586694144", string(buf, pos));

    a = int128_t(1) << 64;
    ret = a.multiply<CheckOverFlow>(int128_t(1234), res_128);
    pos = 0;
    ret = wide::to_string(res_128, buf, sizeof(buf), pos);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ("22763282186957586694144", string(buf, pos));

    a = -(int128_t(1) << 64);
    ret = a.multiply<CheckOverFlow>(UINT64_MAX, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = -(int128_t(1) << 64);
    ret = a.multiply<CheckOverFlow>((1ULL<<63) + 1, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = -(int128_t(1) << 64);
    ret = a.multiply<CheckOverFlow>(int128_t(UINT64_MAX) << 1,res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = -(int128_t(2) << 64);
    ret = a.multiply<CheckOverFlow>(int128_t(UINT64_MAX), res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = -(int128_t(1) << 64);
    ret = a.multiply<CheckOverFlow>(int128_t(UINT64_MAX), res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = -(int128_t(1) << 64);
    ret = a.multiply<CheckOverFlow>(1234, res_128);
    ASSERT_EQ(ret, 0);
    pos = 0;
    ret = wide::to_string(res_128, buf, sizeof(buf), pos);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ("-22763282186957586694144", string(buf, pos));

    a = -(int128_t(1) << 64);
    ret = a.multiply<CheckOverFlow>(int128_t(1234), res_128);
    ASSERT_EQ(ret, 0);
    pos = 0;
    ret = wide::to_string(res_128, buf, sizeof(buf), pos);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ("-22763282186957586694144", string(buf, pos));

    a = int128_t(1) << 64;
    ret = a.divide<CheckOverFlow>(0, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = int128_t(1) << 64;
    ret = a.divide<CheckOverFlow>(int128_t(0), res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = Limits<int128_t>::min();
    ret = a.divide<CheckOverFlow>(-1, res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);

    a = Limits<int128_t>::min();
    ret = a.divide<CheckOverFlow>(int128_t(-1), res_128);
    ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
  }

  void op_muldiv256_overflow() {
#define chk_overflow(ops_str, lhs, rhs, expected_ret)                          \
  do {                                                                         \
    int ret = (lhs).ops_str <CheckOverFlow>((rhs), res_256);                   \
    ASSERT_EQ(ret, expected_ret);                                              \
  } while (0)

    int256_t res_256;
    chk_overflow(divide, Limits<int256_t>::min(), 0, OB_OPERATE_OVERFLOW);
    chk_overflow(divide, Limits<int256_t>::min(), int128_t(0), OB_OPERATE_OVERFLOW);
    chk_overflow(divide, Limits<int256_t>::min(), int256_t(0), OB_OPERATE_OVERFLOW);
    chk_overflow(divide, Limits<int256_t>::min(), -1, OB_OPERATE_OVERFLOW);
    chk_overflow(divide, Limits<int256_t>::max(), -1, 0);

    chk_overflow(multiply, (int256_t(1) << 254), UINT64_MAX, OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, Limits<int256_t>::min(), -1, OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, Limits<int256_t>::max(), 2, OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, int256_t(1) << 254, -3, OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, (int256_t(1) << 254), int256_t(UINT64_MAX), OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, Limits<int256_t>::min(), int256_t(-1), OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, Limits<int256_t>::max(), int256_t(2), OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, int256_t(1) << 254, int256_t(-3), OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, int256_t(1) << 128, int256_t(3)<<50, 0);
#undef chk_overflow
  }

  void op_muldiv512_overflow() {
#define chk_overflow(ops_str, lhs, rhs, expected_ret)                          \
  do {                                                                         \
    int ret = (lhs).ops_str<CheckOverFlow>((rhs), res_512);                    \
    ASSERT_EQ(ret, expected_ret);                                              \
  } while (0)

    int512_t res_512;
    chk_overflow(divide, Limits<int512_t>::min(), 0, OB_OPERATE_OVERFLOW);
    chk_overflow(divide, Limits<int512_t>::min(), int128_t(0), OB_OPERATE_OVERFLOW);
    chk_overflow(divide, Limits<int512_t>::min(), int256_t(0), OB_OPERATE_OVERFLOW);
    chk_overflow(divide, Limits<int512_t>::min(), -1, OB_OPERATE_OVERFLOW);
    chk_overflow(divide, Limits<int512_t>::max(), -1, 0);

    chk_overflow(multiply, (int512_t(1) << 510), UINT64_MAX, OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, Limits<int512_t>::min(), -1, OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, Limits<int512_t>::max(), 2, OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, int512_t(1) << 510, -3, OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, (int512_t(1) << 510), int256_t(UINT64_MAX), OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, Limits<int512_t>::min(), int256_t(-1), OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, Limits<int512_t>::max(), int256_t(2), OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, int512_t(1) << 510, int256_t(-3), OB_OPERATE_OVERFLOW);
    chk_overflow(multiply, int512_t(1) << 256, int256_t(3)<<200, 0);
  }

  void test_helper_trait()
  {
#define check_trait(trait, expected)                                                               \
  do {                                                                                             \
    bool ret = trait::value;                                                                       \
    ASSERT_EQ(expected, ret);                                                                      \
  } while (0)

    check_trait(IsWideInteger<int32_t>, false);
    check_trait(IsWideInteger<uint32_t>, false);
    check_trait(IsWideInteger<int64_t>, false);
    check_trait(IsWideInteger<uint64_t>, false);

    check_trait(IsWideInteger<int128_t>, true);
    check_trait(IsWideInteger<int256_t>, true);
    check_trait(IsWideInteger<int512_t>, true);
    check_trait(IsWideInteger<int1024_t>, true);

    check_trait(IsIntegral<int32_t>, true);
    check_trait(IsIntegral<uint32_t>, true);
    check_trait(IsIntegral<int64_t>, true);
    check_trait(IsIntegral<uint32_t>, true);
    check_trait(IsIntegral<int128_t>, true);
    check_trait(IsIntegral<int256_t>, true);
    check_trait(IsIntegral<int512_t>, true);
    check_trait(IsIntegral<float>, false);
    check_trait(IsIntegral<double>, false);

    check_trait(SignedConcept<int128_t>, true);
    check_trait(SignedConcept<int256_t>, true);
    check_trait(SignedConcept<int512_t>, true);
    check_trait(SignedConcept<int32_t>, true);
    check_trait(SignedConcept<int64_t>, true);
    check_trait(SignedConcept<uint32_t>, false);
    check_trait(SignedConcept<uint64_t>, false);

    bool ret = std::is_same<PromotedInteger<int32_t>::type, int64_t>::value;
    ASSERT_EQ(true, ret);
    ret = std::is_same<PromotedInteger<int64_t>::type, int128_t>::value;
    ASSERT_EQ(ret, true);
    ret = std::is_same<PromotedInteger<int128_t>::type, int256_t>::value;
    ASSERT_EQ(ret, true);
    ret = std::is_same<PromotedInteger<int256_t>::type, int512_t>::value;
    ASSERT_EQ(ret, true);
  }
  void op_cmp_objects()
  {
    std::vector<std::pair<int, int>> defined_cmp_tuple{
      {4, 4},  {4, 8},  {4, 16},  {4, 32},  {4, 64},
      {8, 4},  {8, 8},  {8, 16},  {8, 32},  {8, 64},
      {16, 4}, {16, 8}, {16, 16}, {16, 32}, {16, 64},
      {32, 4}, {32, 8}, {32, 16}, {32, 32}, {32, 64},
      {64, 4}, {64, 8}, {64, 16}, {64, 32}, {64, 64},
    };
    for (int i = 0; i < defined_cmp_tuple.size(); i++) {
      auto pair = defined_cmp_tuple[i];
      auto cmp_fp = ObDecimalIntCmpSet::get_decint_decint_cmp_func(pair.first, pair.second);
      ASSERT_TRUE(cmp_fp != nullptr);
    }


    // cmp int32

    int128_t v1 = (int128_t(1) << 80) + UINT64_MAX;
    int128_t v2 = (int128_t(1) << 80) + INT64_MAX;

    ObObj lhs_obj, rhs_obj;
    lhs_obj.set_decimal_int(sizeof(int128_t), 0, (ObDecimalInt *)(&v1));
    rhs_obj.set_decimal_int(sizeof(int128_t), 0, (ObDecimalInt *)(&v2));
    int cmp_res = 0;
    int ret = wide::compare(lhs_obj, rhs_obj, cmp_res);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(cmp_res, 1);
  }
  void op_addsub_diff_results()
  {
    std::ifstream int128_result("wide_integer_int128_add.result");
    ASSERT_TRUE(int128_result.is_open());
    MockAllocator allocator;
    while(!int128_result.eof()) {
      std::string lhs_str, rhs_str, result_str;
      int128_result >> lhs_str  >> rhs_str >> result_str;
      int128_t lhs = parse_string<int128_t>(lhs_str, allocator);
      int128_t rhs = parse_string<int128_t>(rhs_str, allocator);
      int128_t expected = parse_string<int128_t>(result_str, allocator);
      int128_t result = lhs + rhs;
      if (result != expected) {
        std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << '\n';
      }
      ASSERT_EQ(expected, result);
    }
    int128_result.close();

    std::ifstream int256_int128_result("wide_integer_int256_int128_add.result");
    ASSERT_TRUE(int256_int128_result.is_open());
    while(!int256_int128_result.eof()) {
      std::string lhs_str, rhs_str, result_str;
      int256_int128_result >> lhs_str >> rhs_str >> result_str;
      int256_t lhs = parse_string<int256_t>(lhs_str, allocator);
      int128_t rhs = parse_string<int128_t>(rhs_str, allocator);
      int256_t expected = lhs + rhs;
      int256_t result = lhs + rhs;
      if (result != expected) {
        std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << '\n';
      }
      ASSERT_EQ(expected, result);
    }
  }

  void op_mul_diff_results()
  {
    // int128 * int64
    std::ifstream int128_int64_result("wide_integer_int128_int64_mul.result");
    ASSERT_TRUE(int128_int64_result.is_open());
    MockAllocator allocator;
    while (!int128_int64_result.eof()) {
      std::string lhs_str, rhs_str, result_str, overflow;
      int128_int64_result >> lhs_str >> rhs_str >> result_str >> overflow;
      int128_t lhs = parse_string<int128_t>(lhs_str, allocator);
      int64_t rhs = parse_string<int64_t>(rhs_str, allocator);
      int128_t result;
      int ret = lhs.multiply<CheckOverFlow>(rhs, result);
      if (overflow == "overflow") {
        if (ret != OB_OPERATE_OVERFLOW) {
          std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << '\n';
        }
        ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
      } else {
        int128_t expected = parse_string<int128_t>(result_str, allocator);
        if (expected != result) {
          std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << '\n';
        }
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(result, expected);
      }
    }
    int128_int64_result.close();

    // int128 * int128
    std::ifstream int128_int128_result("wide_integer_int128_int128_mul.result");
    ASSERT_TRUE(int128_int128_result.is_open());
    while(!int128_int128_result.eof()) {
      std::string lhs_str, rhs_str, result_str, overflow;
      int128_int128_result >> lhs_str >> rhs_str >> result_str >> overflow;
      int128_t lhs = parse_string<int128_t>(lhs_str, allocator);
      int128_t rhs = parse_string<int128_t>(rhs_str, allocator);
      int128_t result;
      int ret = lhs.multiply<CheckOverFlow>(rhs, result);
      if (overflow == "overflow") {
        if (ret != OB_OPERATE_OVERFLOW) {
          std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << '\n';
        }
        ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
      } else {
        ASSERT_EQ(ret, 0);
        int128_t expected = parse_string<int128_t>(result_str, allocator);
        if (expected != result) {
          std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << '\n';
        }
        ASSERT_EQ(result, expected);
      }
    }

    // int256 * int128
    std::ifstream int256_int128_result("wide_integer_int256_int128_mul.result");
    ASSERT_TRUE(int256_int128_result.is_open());
    while (!int256_int128_result.eof()) {
      std::string lhs_str, rhs_str, result_str, overflow;
      int256_int128_result >> lhs_str >> rhs_str >> result_str >> overflow;
      int256_t lhs = parse_string<int256_t>(lhs_str, allocator);
      int128_t rhs = parse_string<int128_t>(rhs_str, allocator);
      int256_t result;
      int ret = lhs.multiply<CheckOverFlow>(rhs, result);
      if (overflow == "overflow") {
        if (ret != OB_OPERATE_OVERFLOW) {
          std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << '\n';
        }
        ASSERT_EQ(ret, OB_OPERATE_OVERFLOW);
      } else {
        int256_t expected = parse_string<int256_t>(result_str, allocator);
        if (expected != result || ret != 0) {
          std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << '\n';
        }
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(result, expected);
      }
    }
  }

  void op_div_diff_results()
  {
    // int128/int64
    std::ifstream int128_int64("wide_integer_int128_int64_div.result");
    ASSERT_TRUE(int128_int64.is_open());
    std::string lhs_str, rhs_str, result_str, rem_str;
    MockAllocator allocator;
    while (!int128_int64.eof()) {
      int128_int64 >> lhs_str >> rhs_str >> result_str >> rem_str;
      int128_t lhs = parse_string<int128_t>(lhs_str, allocator);
      int64_t rhs = parse_string<int64_t>(rhs_str, allocator);
      int128_t expected_quo = parse_string<int128_t>(result_str, allocator);
      int128_t expected_rem = parse_string<int128_t>(rem_str, allocator);
      int128_t result = lhs / rhs;
      int128_t rem = lhs % rhs;
      if (expected_quo != result || expected_rem != rem) {
        std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << ' ' << rem_str << '\n';
      }
      ASSERT_EQ(result, expected_quo);
      ASSERT_EQ(rem, expected_rem);
    }
    int128_int64.close();

    // int128/int128
    std::ifstream int128_int128("wide_integer_int128_int128_div.result");
    ASSERT_TRUE(int128_int128.is_open());
    while (!int128_int128.eof()) {
      int128_int128 >> lhs_str >> rhs_str >> result_str >> rem_str;
      int128_t lhs = parse_string<int128_t>(lhs_str, allocator);
      int128_t rhs = parse_string<int128_t>(rhs_str, allocator);
      int128_t expected_quo = parse_string<int128_t>(result_str, allocator);
      int128_t expected_rem = parse_string<int128_t>(rem_str, allocator);
      int128_t quo = lhs / rhs;
      int128_t rem = lhs % rhs;
      if (expected_quo != quo || expected_rem != rem) {
        std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << ' ' << rem_str << '\n';
      }
      ASSERT_EQ(rem, expected_rem);
      ASSERT_EQ(quo, expected_quo);
    }
    int128_int128.close();

    // int256/int128
    std::ifstream int256_int128("wide_integer_int256_int128_div.result");
    ASSERT_TRUE(int256_int128.is_open());
    while (!int256_int128.eof()) {
      int256_int128 >> lhs_str >> rhs_str >> result_str >> rem_str;
      int256_t lhs = parse_string<int256_t>(lhs_str, allocator);
      int128_t rhs = parse_string<int128_t>(rhs_str, allocator);
      int256_t expected_quo = parse_string<int256_t>(result_str, allocator);
      int256_t expected_rem = parse_string<int256_t>(rem_str, allocator);
      int256_t quo = lhs / rhs;
      int256_t rem = lhs % rhs;
      if (expected_quo != quo || expected_rem != rem) {
        std::cout << lhs_str << ' ' << rhs_str << ' ' << result_str << ' ' << rem_str << '\n';
      }
      ASSERT_EQ(quo, expected_quo);
      ASSERT_EQ(rem, expected_rem);
    }
    int256_int128.close();
  }

  void op_cmp_diff_results()
  {
    std::ifstream cmp_cases("wide_integer_cmp.result");
    ASSERT_TRUE(cmp_cases.is_open());
    std::string lhs_str, rhs_str, res_str;
    ObDecimalInt *lhs_decint = nullptr, *rhs_decint = nullptr;
    int32_t lhs_int_bytes, rhs_int_bytes;
    int ret = 0;
    int cmp_ret = 0;
    std::string cmp_ret_str;
    MockAllocator alloc;
    int16_t scale, precision;
    ObDecimalIntBuilder lhs_bld, rhs_bld;

    while(!cmp_cases.eof()) {
      cmp_cases >> lhs_str >> rhs_str >> res_str;
      ret = wide::from_string(lhs_str.c_str(), lhs_str.size(), alloc, scale, precision, lhs_int_bytes, lhs_decint);
      ASSERT_EQ(ret, 0);
      ret = wide::from_string(rhs_str.c_str(), rhs_str.size(), alloc, scale, precision, rhs_int_bytes, rhs_decint);
      ASSERT_EQ(ret, 0);
      lhs_bld.from(lhs_decint, lhs_int_bytes);
      rhs_bld.from(rhs_decint, rhs_int_bytes);
      ret = wide::compare(lhs_bld, rhs_bld, cmp_ret);
      ASSERT_EQ(ret, 0);
      if (cmp_ret < 0) {
        cmp_ret_str = "lt";
      } else if (cmp_ret > 0) {
        cmp_ret_str = "gt";
      } else {
        cmp_ret_str = "eq";
      }
      ASSERT_EQ(res_str, cmp_ret_str);
    }
  }

  void op_from()
  {
#define check_val_len(v, expected_len)                                                             \
  do {                                                                                             \
    decint = nullptr;                                                                              \
    val_len = 0;                                                                                   \
    int ret = wide::from_integer(v, tmp_alloc, decint, val_len);                                   \
    ASSERT_EQ(ret, 0);                                                                             \
    ASSERT_EQ(val_len, expected_len);                                                              \
  } while (0)
    MockAllocator tmp_alloc;
    ObDecimalInt *decint = nullptr;
    int32_t val_len = 0;
    int32_t v0 = 12, v1 = INT32_MAX;
    uint32_t v2 = INT32_MAX, v3 = UINT32_MAX;
    int64_t k0 = 12, k1 = INT64_MAX;
    uint64_t k2 = INT64_MAX, k3 = UINT64_MAX;

    check_val_len(v0, 4);
    ASSERT_EQ(*reinterpret_cast<int32_t *>(decint), v0);
    check_val_len(v1, 4);
    ASSERT_EQ(*reinterpret_cast<int32_t *>(decint), v1);
    check_val_len(v2, 4);
    ASSERT_EQ(*reinterpret_cast<int32_t *>(decint), v2);
    check_val_len(v3, 8);
    ASSERT_EQ(*reinterpret_cast<int64_t *>(decint), v3);

    check_val_len(k0, 8);
    ASSERT_EQ(*reinterpret_cast<int64_t *>(decint), k0);

    check_val_len(k1, 8);
    ASSERT_EQ(*reinterpret_cast<int64_t *>(decint), k1);

    check_val_len(k2, 8);
    ASSERT_EQ(*reinterpret_cast<int64_t *>(decint), k2);

    check_val_len(k3, 16);
    int128_t expected_k3 = UINT64_MAX;
    ASSERT_EQ(*reinterpret_cast<int128_t *>(decint), expected_k3);

    std::string valstr = "0000123.123456";
    int16_t scale, precision;
    int32_t int_bytes;
    ObDecimalInt *decint2;
    int ret = wide::from_string(valstr.c_str(), valstr.size(), tmp_alloc, scale, precision, int_bytes, decint2);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(int_bytes, 8);
    ASSERT_EQ(scale, 6);
    ASSERT_EQ(precision, 13);
    ASSERT_EQ(*(decint2->int32_v_), 123123456);

    valstr = "000001231234.123456789123456789";
    ret = wide::from_string(valstr.c_str(), valstr.size(), tmp_alloc, scale, precision, int_bytes, decint2);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(int_bytes, 16);
    ASSERT_EQ(scale, 18);
    ASSERT_EQ(precision, 30);
    char buf[256] = {0};
    int64_t length = 0;
    ret = wide::to_string(decint2, int_bytes, scale, buf, sizeof(buf), length);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(std::string(buf, length), "1231234.123456789123456789");
  }
  void op_from_string()
  {
    std::ifstream cases("wide_integer_from_string.result");
    std::string valstr, precsion_str, scale_str;
    MockAllocator alloc;
    ObDecimalInt *decint = nullptr;
    int32_t int_bytes = 0;
    int ret = 0;
    int16_t scale = 0, precision = 0;
    char buf[256] = {0};
    int64_t length = 0;

    while (!cases.eof()) {
      cases >> valstr >> precsion_str >> scale_str;
      int expected_precision = std::atoi(precsion_str.c_str());
      int expected_scale = std::atoi(scale_str.c_str());
      ret = wide::from_string(valstr.c_str(), valstr.size(), alloc, scale, precision, int_bytes,
                              decint);
      if (ret != 0) {
        number::ObNumber tmp_nmb;
        int16_t tmp_prec, tmp_scale;
        int tmp_ret = tmp_nmb.from_sci_opt(valstr.c_str(), valstr.size(), alloc, &tmp_prec, &tmp_prec);
        if (tmp_ret != ret) {
          std::cout << valstr << '\n';
        }
        ASSERT_EQ(tmp_ret, ret);
        continue;
      }
      if (ret != 0) {
        std::cout << valstr << '\n';
      }
      ASSERT_EQ(ret, 0);
      if (precision != expected_precision) {
        ASSERT_EQ(precision, scale + 1);
      }
      ASSERT_EQ(scale, expected_scale);
      length = 0;
      ret = wide::to_string(decint, int_bytes, scale, buf, sizeof(buf), length);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(std::string(buf, length), valstr);
    }
  }
  void op_from_integer()
  {
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<int64_t> int_gen(INT64_MIN, INT64_MAX);
    MockAllocator alloc;
    ObDecimalInt *decint = nullptr;
    int32_t int_bytes = 0;
    for (int i = 0; i < 1000; i++) {
      int64_t v = int_gen(rng);
      int ret = wide::from_integer(v, alloc, decint, int_bytes);
      ASSERT_EQ(ret, 0);
      ASSERT_TRUE(int_bytes <= sizeof(int64_t));
      if (int_bytes == sizeof(int32_t)) {
        ASSERT_TRUE(*(decint->int32_v_) == v);
      } else {
        ASSERT_TRUE(*(decint->int64_v_) == v);
      }
    }
  }

  void op_from_number()
  {
    std::ifstream from_num_cases("wide_integer_from_number.result");
    ASSERT_TRUE(from_num_cases.is_open());
    MockAllocator allocator;
    ObDecimalInt *decint = nullptr;
    int32_t int_bytes = 0;
    number::ObNumber nmb;
    char buf[256] = {0};
    int64_t length = 0;
    std::string nmb_str;
    int ret = 0;
    while (!from_num_cases.eof()) {
      from_num_cases >> nmb_str;
      ret = nmb.from(nmb_str.c_str(), nmb_str.size(), allocator);
      ASSERT_EQ(ret, 0);
      ret = wide::from_number(nmb, allocator, nmb.get_scale(), decint, int_bytes);
      ASSERT_EQ(ret, 0);
      ret = wide::to_string(decint, int_bytes, nmb.get_scale(), buf, sizeof(buf), length);
      ASSERT_EQ(ret, 0);
      std::string nmb_fmt(nmb.format());
      ASSERT_EQ(std::string(buf, length), nmb_fmt);
      length = 0;
    }
  }
  void op_to_number()
  {
    MockAllocator tmp_alloc;
    int16_t scale = 0;
    int16_t precision = 0;
    int32_t int_bytes = 0;
    int ret = 0;
    ObDecimalInt *decint = nullptr;
    std::string valstr;
    std::ifstream cases("wide_integer_to_number.result");
    ASSERT_TRUE(cases.is_open());
    number::ObNumber nmb;
    char data_buf[256] = {0};
    int64_t length = 0;
    while (!cases.eof()) {
      cases >> valstr >> precision >> scale;
      ret = wide::from_string(valstr.c_str(), valstr.size(), tmp_alloc, scale, precision, int_bytes,
                              decint);
      ASSERT_EQ(ret, 0);
      ret = wide::to_number(decint, int_bytes, scale, tmp_alloc, nmb);
      if (ret != 0) {
        std::cout << valstr << '\n';
        std::cout << int_bytes << '\n';
      }
      ASSERT_EQ(ret, 0);
      int ret = nmb.format_v1(data_buf, sizeof(data_buf), length, scale);
      // std::string nmb_str(fmt_str);
      if (ret != 0) {
        std::cout << valstr << '\n';
      }
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(std::string(data_buf, length), valstr);
      length = 0;
    }
    cases.close();
  }
  void perf_calc()
  {
    MockAllocator alloc;
    std::vector<number::ObNumber> lhs_nmbs;
    std::vector<number::ObNumber> rhs_nmbs;
    number::ObNumber nmb0, nmb1, nmb2, nmb3;
    number::ObNumber res;
    int256_t res0;
    int128_t res1;
    nmb0.from("7266892579576111521134811142732427669749408813191128358981921690067753828", alloc);
    nmb1.from("355676015188330345687923", alloc);
    nmb2.from("5691091705679464", alloc);
    nmb3.from("350113974", alloc);

    int256_t l0, r0;
    int128_t l1, r1;
    l0 = parse_string<int256_t>(
      std::string("7266892579576111521134811142732427669749408813191128358981921690067753828"),
      alloc);
    r0 = parse_string<int256_t>(
      std::string("5691091705679464"),
      alloc);
    l1 = parse_string<int128_t>(std::string("355676015188330345687923"), alloc);
    r1 = parse_string<int128_t>(std::string("350113974"), alloc);

    int cnt = 100000000;
    std::cout << "ObNumber Large Add Cost: ";
    auto start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) { nmb0.add(nmb2, res, alloc); }
    auto end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObNumber Medium Add Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) { nmb1.add(nmb3, res, alloc); }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObNumber Large Multiply Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) { nmb0.mul(nmb2, res, alloc); }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObNumber Medium Multiply Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) { nmb1.mul(nmb3, res, alloc); }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObNumber Large Divide Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) { nmb0.div(nmb2, res, alloc); }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObNumber Medium Divide Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) { nmb1.div(nmb3, res, alloc); }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObWideInteger Large Add Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) {
      l0.add<CheckOverFlow>(r0, res0);
    }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObWideInteger Medium Add Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) {
      l1.add<CheckOverFlow>(r1, res1);
    }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObWideInteger Large Multiply Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) {
      l0.multiply<CheckOverFlow>(r0, res0);
    }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObWideInteger Medium Multiply Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) {
      l1.multiply<CheckOverFlow>(r1, res1);
    }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObWideInteger Large Divide Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) {
      l0.divide<CheckOverFlow>(r0, res0);
    }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";

    std::cout << "ObWideInteger Medium Divide Cost: ";
    start = std::chrono::system_clock::now();
    for (volatile int i = 0; i < cnt; i++) { auto res = l1 / r1; }
    end = std::chrono::system_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << "ms\n";
  }
  // TODO: decimal int cmp unittest
  void test_const_value()
  {
    MockAllocator alloc;
    lib::ObMemAttr attr;
    int ret = ObDecimalIntConstValue::init_const_values(alloc, attr);
    ASSERT_EQ(ret, 0);
    std::ofstream out("decimalint_const_val.out");
    ASSERT_TRUE(out.is_open());
    out << "======== MYSQL ========\n";
    char buf[256] = {0};
    int64_t length = 0;
    for (int16_t precision = OB_MIN_DECIMAL_PRECISION; precision <= OB_MAX_DECIMAL_POSSIBLE_PRECISION; ++precision) {
      for (int16_t scale = 0; precision >= scale && scale <= number::ObNumber::MAX_SCALE; ++scale) {
        const ObDecimalInt *decint = ObDecimalIntConstValue::get_min_value(precision);
        int32_t int_bytes = ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
        ret = wide::to_string(decint, int_bytes, scale, buf, sizeof(buf), length);
        ASSERT_EQ(ret, 0);
        // mysql_check_min
        out << "MYSQL_MIN (" << precision << ", " << scale << "): " << std::string(buf, length) << '\n';
        length = 0;
        // mysql_check_max
        decint = ObDecimalIntConstValue::get_max_value(precision);
        ret = wide::to_string(decint, int_bytes, scale, buf, sizeof(buf), length);
        ASSERT_EQ(ret, 0);
        out << "MYSQL_MAX (" << precision << ", " << scale << "): " << std::string(buf, length) << '\n';
        length = 0;
      }
    }

    // oracle
    out << "======== ORACLE ========\n";
    for (int16_t precision = OB_MIN_NUMBER_PRECISION; precision <= OB_MAX_NUMBER_PRECISION; precision++) {
      for (int16_t scale = number::ObNumber::MIN_SCALE; scale <= number::ObNumber::MAX_SCALE; scale++) {
        int16_t delta_scale = ObDecimalIntConstValue::oracle_delta_scale(scale);
        const ObDecimalInt *decint = ObDecimalIntConstValue::get_min_value(precision);
        int32_t int_bytes = ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
        // oracle_min
        ret = wide::to_string(decint, int_bytes, scale, buf, sizeof(buf), length);
        ASSERT_EQ(ret, 0);
        out << "ORACLE_MIN (" << precision << ", " << scale << "): " << std::string(buf, length) << '\n';
        length = 0;

        // oracle_max
        decint = ObDecimalIntConstValue::get_max_value(precision);
        ret = wide::to_string(decint, int_bytes, scale, buf, sizeof(buf), length);
        ASSERT_EQ(ret, 0);
        out << "ORACLE_MAX (" << precision << ", " << scale << "): " << std::string(buf, length) << '\n';
        length = 0;
      } // for end
    } // for end
    ASSERT_TRUE(is_equal_content("decimalint_const_val.out", "nmb_const_value.result"));
  }

  void test_oracle_to_string()
  {
    // TODO enrich test cases
    const int128_t v0{3136633892082024446ULL,
                      5421010862427522ULL}; // 99999999999999999999999999999999998
    const int128_t v1{12919594847110692862ULL,
                      54210108624275221ULL}; // 999999999999999999999999999999999998
    const int256_t v2{4870020673419870206ULL,
                      16114848830623546549ULL,
                      293ULL,
                      0}; // 99999999999999999999999999999999999999998
    const ObDecimalInt *decint0 = reinterpret_cast<const ObDecimalInt *>(&v0);
    int32_t int_bytes0 = sizeof(int128_t);
    int16_t scale0 = 44;
    const ObDecimalInt *decint1 = reinterpret_cast<const ObDecimalInt *>(&v1);
    int32_t int_bytes1 = sizeof(int128_t);
    int16_t scale1 = 45;
    const ObDecimalInt *decint2 = reinterpret_cast<const ObDecimalInt *>(&v2);
    int32_t int_bytes2 = sizeof(int256_t);
    int16_t scale2 = 0;
    int16_t scale3 = 2;
    char buf[256] = {0};
    int64_t length = 0;
    lib::set_compat_mode(lib::Worker::CompatMode::ORACLE);
    int ret = wide::to_string(decint0, int_bytes0, scale0, buf, sizeof(buf), length, true);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(std::string(buf, length), "9.9999999999999999999999999999999998E-10");
    length = 0;
    ret = wide::to_string(decint1, int_bytes1, scale1, buf, sizeof(buf), length, true);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(std::string(buf, length), "1.00000000000000000000000000000000000E-9");
    length = 0;
    ret = wide::to_string(decint2, int_bytes2, scale2, buf, sizeof(buf), length, true);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(std::string(buf, length), "1.0000000000000000000000000000000000E+41");
    length = 0;
    ret = wide::to_string(decint2, int_bytes2, scale3, buf, sizeof(buf), length, true);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(std::string(buf, length), "1.0000000000000000000000000000000000E+39");
    length = 0;

    ObDecimalInt *parsed_decint = nullptr;
    int32_t parsed_int_bytes = 0;
    MockAllocator mock_alloc;
    std::string parse_str0("-.00012345");
    std::string parse_str1("-1.2345e-3");
    std::string parse_str2("1.2345e+18");
    int32_t expected_neg_val = -12345;
    int32_t expected_pos_val = 12345;
    int16_t parsed_scale = 0, parsed_precision = 0;
    ret = wide::from_string(parse_str0.c_str(), parse_str0.size(), mock_alloc, parsed_scale,
                            parsed_precision, parsed_int_bytes, parsed_decint);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(parsed_int_bytes, 4);
    ASSERT_EQ(parsed_scale, 8);
    ASSERT_EQ(*(parsed_decint->int32_v_), expected_neg_val);

    ret = wide::from_string(parse_str1.c_str(), parse_str1.size(), mock_alloc, parsed_scale, parsed_precision, parsed_int_bytes, parsed_decint);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(parsed_int_bytes, 4);
    ASSERT_EQ(parsed_scale, 7);
    ASSERT_EQ(*(parsed_decint->int32_v_), expected_neg_val);

    ret = wide::from_string(parse_str2.c_str(), parse_str2.size(), mock_alloc, parsed_scale, parsed_precision, parsed_int_bytes, parsed_decint);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(parsed_int_bytes, 4);
    ASSERT_EQ(parsed_scale, -14);
    ASSERT_EQ(*(parsed_decint->int32_v_), expected_pos_val);
  }
};

void test_small_decint_to_nmb()
{
  const int64_t constexpr test_cases = 100000;
  MockAllocator tmp_alloc;
  number::ObNumber expect_nmb;
  number::ObNumber res_nmb;
  uint32_t digits[3] = {0};
  int ret = 0;
  int64_t rand_v_list[test_cases] = {0};
  int64_t rand_s_list[test_cases] = {0};
  for (int i = 0; i < test_cases; i++) {
    rand_v_list[i] = rand_value<int64_t>(100000000, 99999999999999999);
    rand_s_list[i] = rand_value<int64_t>(0, 100) % 10;
  }
  auto start = std::chrono::system_clock::now();
  for (int i = 0; i < test_cases; i++) {
    int ret = wide::to_number(rand_v_list[i], rand_s_list[i], tmp_alloc, expect_nmb);
    ASSERT_EQ(ret, OB_SUCCESS);
    ret = wide::to_number(rand_v_list[i], rand_s_list[i], (uint32_t *)digits, 3, res_nmb);
    ASSERT_EQ(ret, OB_SUCCESS);
    if (expect_nmb.compare(res_nmb) != 0) {
      std::cout << "value: " << rand_v_list[i] << ", scale: " << rand_s_list[i] << '\n';
    }
    ASSERT_EQ(expect_nmb, res_nmb);
  }
}


TEST(TestWideInteger, ALL) {
  TestWideInteger test;
  test.op_addsub_without_overflow();
  test.op_mul_without_overflow();
  test.op_div_without_overflow();
  test.op_shift();
  test.op_addsub_overflow();
  test.op_muldiv128_overflow();
  test.op_muldiv256_overflow();
  test.op_muldiv512_overflow();
  test.op_mul_diff_results();
  test.op_cmp_objects();
  test.op_addsub_diff_results();
  test.to_string();
  test.op_from_string();
  test.op_from();
  test.op_to_number();
  test.test_helper_trait();
  test.op_div_diff_results();
  test.op_cmp_diff_results();
  test.op_from_integer();
#ifndef DEBUG
  // test.perf_calc();
#endif
  test.op_from_number();
  test.test_const_value();
  test.test_oracle_to_string();
  test_small_decint_to_nmb();
}


} // endnamespace wide
} // end namespace common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  system("rm -rf test_wide_integer.log");
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_wide_integer.log", true);
  return RUN_ALL_TESTS();
}