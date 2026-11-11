/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <vector>

#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/vector/expr_cmp_func.h"
#include "unittest/share/vector/util.h"


namespace oceanbase
{
namespace common
{

void prepare_datums(std::vector<ObDatum> &datums, const int64_t datum_len, const int64_t datum_cnt)
{
  void *data = std::malloc(datum_cnt * datum_len);
  ASSERT_TRUE(data != nullptr);
  for (int i = 0; i < datum_cnt; i++) {
    char *ptr = (char *)data + (i * datum_len);
    ObDatum d(ptr, datum_len, false);
    datums.push_back(d);
  }
}

sql::ObBitVector *mock_skip(const int64_t cases)
{
  int size = cases / 8 + 1;
  void *data = std::malloc(size);
  // ASSERT_TRUE(data != nullptr);
  std::memset(data, 0, size);
  return reinterpret_cast<sql::ObBitVector *>(data);
}

class ObTestVectorBasicOp: public ::testing::Test
{
private:
  DISALLOW_COPY_AND_ASSIGN(ObTestVectorBasicOp);
};

std::ostream& operator<<(std::ostream &out, const ObIntervalDSValue &v)
{
  out << "<NSEC: " << v.nsecond_ << ", FSEC: " << v.fractional_second_ << ">";
  return out;
}

std::ostream& operator<<(std::ostream &out, const ObOTimestampData &v)
{
  out << "<DESC: " << v.time_ctx_.desc_  << ", US: " << v.time_us_ << ">";
  return out;
}

template<typename T, typename P>
void cmp(std::vector<T> &l, std::vector<T> &r, std::vector<P> &input, std::string name)
{
  if (l != r) {
    std::cout << "COMPARE " << name << '\n';
    ASSERT_EQ(l.size(), r.size());
    for (int i = 0; i < l.size(); i++) {
      if (l[i] != r[i]) {
        std::cout << "case " << i << ": " << input[i] << '\n';
        ASSERT_EQ(l[i], r[i]);
      }
    }
  }
}

static const int test_cases = 50000;

template<typename ValueType, VecValueTypeClass tc>
void test_fixed_length_hash(const ObObjMeta &meta, std::string case_name)
{
  std::vector<ValueType> data;
  for (int i = 0; i < test_cases; i++) {
      data.push_back(RandomData<ValueType>::rand());
  }
  std::vector<uint64_t> seeds;
  for (int i = 0; i < test_cases; i++) {
    seeds.push_back(RandomData<int64_t>::rand());
  }

  sql::ObBitVector *skip = mock_skip(test_cases);

  sql::ObExpr mock_expr;
  mock_expr.obj_meta_ = meta;
  sql::EvalBound bound(static_cast<uint16_t>(test_cases), true);
  std::vector<uint64_t> vec_hash(test_cases, 0);
  std::vector<uint64_t> datum_hash(test_cases, 0);
  auto vec = new ObFixedLengthVector<ValueType, VectorBasicOp<tc>>((char *)data.data(), skip);

  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
    meta.get_type(), meta.get_collation_type(), meta.get_scale(), false, meta.has_lob_header());
  std::vector<ObDatum> datums;
  prepare_datums(datums, sizeof(ValueType), test_cases);
  for (int i = 0; i < test_cases; i++) {
    std::memcpy(const_cast<char *>(datums[i].ptr_), &(data[i]), sizeof(ValueType));
  }

  int ret = vec->murmur_hash(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true);
  ASSERT_EQ(ret, 0);
  basic_funcs->murmur_hash_batch_(datum_hash.data(), datums.data(), true, *skip, test_cases,
                                  seeds.data(), true);
  cmp(datum_hash, vec_hash, data, case_name + " murmurhash");

  ret = vec->murmur_hash_v3(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true);
  ASSERT_EQ(ret, 0);
  basic_funcs->murmur_hash_v2_batch_(datum_hash.data(), datums.data(), true, *skip, test_cases,
                                     seeds.data(), true);
  cmp(datum_hash, vec_hash, data, case_name + " murmurhash_v2");

  ret = vec->default_hash(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true);
  ASSERT_EQ(ret, 0);
  basic_funcs->default_hash_batch_(datum_hash.data(), datums.data(), true, *skip, test_cases,
                                   seeds.data(), true);
  cmp(datum_hash, vec_hash, data, case_name + " default_hash");
}

std::ostream& operator <<(std::ostream &out, const Item &v)
{
  out << "<LEN: " << v.data_len << ", DATA:";
  for (int i = 0; i < v.data_len; i++) {
    out << " " << int64_t(v.data[i]);
  }
  out << ">";
  return out;
}

template<VecValueTypeClass tc>
void test_discrete_hash(const ObObjMeta &meta, std::string case_name)
{
  std::vector<Item> items;
  for (int i = 0; i < test_cases; i++) {
    items.push_back(rand_item<tc>());
  }
  std::vector<int32_t> lens(test_cases, 0);
  std::vector<char *> ptrs(test_cases, nullptr);
  for (int i = 0; i < test_cases; i++) {
    lens[i] = items[i].data_len;
    ptrs[i] = items[i].data;
  }
  std::vector<uint64_t> seeds;
  for (int i = 0; i < test_cases; i++) {
    seeds.push_back(RandomData<int64_t>::rand());
  }
  std::vector<ObDatum> datums;
  for (int i = 0; i < test_cases; i++) {
    ObDatum d;
    d.ptr_ = items[i].data;
    d.len_ = items[i].data_len;
    d.null_ = false;
    datums.push_back(d);
  }
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
    meta.get_type(), meta.get_collation_type(), meta.get_scale(), false, meta.has_lob_header());
  sql::ObBitVector *skip = mock_skip(test_cases);

  std::vector<uint64_t> vec_hash(test_cases, 0);
  std::vector<uint64_t> datum_hash(test_cases, 0);

  sql::ObExpr mock_expr;
  mock_expr.obj_meta_ = meta;
  sql::EvalBound bound(test_cases, true);
  auto vec = new ObDiscreteVector<VectorBasicOp<tc>>(lens.data(), ptrs.data(), skip);

  int ret = vec->murmur_hash(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true);
  ASSERT_EQ(ret, 0);
  basic_funcs->murmur_hash_batch_(datum_hash.data(), datums.data(), true, *skip, test_cases,
                                  seeds.data(), true);
  cmp(vec_hash, datum_hash, items, "murmurhash");

  ret = vec->murmur_hash_v3(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true);
  ASSERT_EQ(ret, 0);
  basic_funcs->murmur_hash_v2_batch_(datum_hash.data(), datums.data(), true, *skip, test_cases,
                                     seeds.data(), true);
  cmp(vec_hash, datum_hash, items, "murmurhash_v2");

  ret = vec->default_hash(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true);
  ASSERT_EQ(ret, 0);
  basic_funcs->default_hash_batch_(datum_hash.data(), datums.data(), true, *skip, test_cases,
                                   seeds.data(), true);
  cmp(vec_hash, datum_hash, items, "default_hash");
}

struct CmpItem
{
  void *l_data;
  void *r_data;
  int32_t l_len;
  int32_t r_len;
  bool l_null;
  bool r_null;
};

template<typename T>
ObDatum get_datum(const T &v, const bool null_v)
{
  ObDatum d;
  d.ptr_ = (const char *)&v;
  d.len_ = sizeof(T);
  d.null_ = null_v;
  return d;
}

template<VecValueTypeClass l_tc, VecValueTypeClass r_tc, typename LType, typename RType>
void test_fixed_length_cmp(const ObObjMeta &l_meta, const ObObjMeta &r_meta, std::string case_name)
{
  std::vector<LType> l_datas(test_cases, LType());
  std::vector<RType> r_datas(test_cases, RType());
  std::vector<CmpItem> items;
  ObDatumCmpFuncType null_first_datum_cmp = ObDatumFuncs::get_nullsafe_cmp_func(
    l_meta.get_type(), r_meta.get_type(), NULL_FIRST, l_meta.get_collation_type(),
    l_meta.get_scale(), false, false);
  ObDatumCmpFuncType null_last_datum_cmp = ObDatumFuncs::get_nullsafe_cmp_func(
    l_meta.get_type(), r_meta.get_type(), NULL_LAST, l_meta.get_collation_type(),
    l_meta.get_scale(), false, false);
  sql::NullSafeRowCmpFunc null_first_row_cmp = nullptr;
  sql::NullSafeRowCmpFunc null_last_row_cmp = nullptr;
  ObPrecision l_prec = wide::ObDecimalIntConstValue::get_max_precision_by_int_bytes(sizeof(LType));
  ObPrecision r_prec = wide::ObDecimalIntConstValue::get_max_precision_by_int_bytes(sizeof(RType));
  const sql::ObDatumMeta l_d_meta(l_meta.get_type(), l_meta.get_collation_type(),
                                  l_meta.get_scale(), l_prec);
  const sql::ObDatumMeta r_d_meta(r_meta.get_type(), r_meta.get_collation_type(),
                                  r_meta.get_scale(), r_prec);
  VectorCmpExprFuncsHelper::get_cmp_set(l_d_meta, r_d_meta, null_first_row_cmp, null_last_row_cmp);
  int datum_cmp_ret = 0, row_cmp_ret = 0;
  for (int i = 0; i < test_cases; i++) {
    LType l_v = RandomData<LType>::rand();
    LType r_v = RandomData<RType>::rand();
    bool l_null = RandomData<bool>::rand();
    bool r_null = RandomData<bool>::rand();
    ObDatum l_datum = get_datum(l_v, l_null);
    ObDatum r_datum = get_datum(r_v, r_null);
    int ret = null_first_datum_cmp(l_datum, r_datum, datum_cmp_ret);
    ASSERT_EQ(ret, 0);
    ret = null_first_row_cmp(l_meta, r_meta, &l_v, sizeof(LType), l_null, &r_v, sizeof(RType),
                             r_null, row_cmp_ret);
    ASSERT_EQ(ret, 0);
    if (datum_cmp_ret != row_cmp_ret) {
      std::cout << case_name << "NULL FIRST CMP\n";
      std::cout << "L: " << l_v << ", R: " << r_v << ", L_NULL: " << l_null
                << ", R_NULL: " << r_null << '\n';
      ASSERT_EQ(datum_cmp_ret, row_cmp_ret);
    }
    ret = null_last_datum_cmp(l_datum, r_datum, datum_cmp_ret);
    ASSERT_EQ(ret, 0);
    ret = null_last_row_cmp(l_meta, r_meta, &l_v, sizeof(LType), l_null, &r_v, sizeof(RType),
                            r_null, row_cmp_ret);
    ASSERT_EQ(ret, 0);
    if (datum_cmp_ret != row_cmp_ret) {
      std::cout << case_name << "NULL LAST CMP\n";
      std::cout << "L: " << l_v << ", R: " << r_v << ", L_NULL: " << l_null
                << ", R_NULL: " << r_null << '\n';
      ASSERT_EQ(datum_cmp_ret, row_cmp_ret);
    }
  }

  // BUGFIX: DIMA-2026011300113428038
  if (case_name == "dec512-dec512") {
    int512_t l_v = -7287090717381220630;
    int512_t r_v = 8915510793597406650;
    bool l_null = false;
    bool r_null = false;
    ObDatum l_datum = get_datum(l_v, l_null);
    ObDatum r_datum = get_datum(r_v, r_null);
    int ret = null_first_datum_cmp(l_datum, r_datum, datum_cmp_ret);
    ASSERT_EQ(ret, 0);
    ret = null_first_row_cmp(l_meta, r_meta, &l_v, sizeof(LType), l_null, &r_v, sizeof(RType),
                             r_null, row_cmp_ret);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(datum_cmp_ret, row_cmp_ret);
  }
}

TEST(ObTestVectorBasicOp, hash_op)
{
  ObObjMeta meta;
  meta.set_int();
  test_fixed_length_hash<int64_t, VEC_TC_INTEGER>(meta, "integer");
  meta.set_uint64();
  test_fixed_length_hash<uint64_t, VEC_TC_UINTEGER>(meta, "uinteger");
  meta.set_float();
  test_fixed_length_hash<float, VEC_TC_FLOAT>(meta, "float");
  meta.set_double();
  test_fixed_length_hash<double, VEC_TC_DOUBLE>(meta, "double");
  meta.set_double();
  for (int scale = 0; scale <= OB_MAX_DOUBLE_FLOAT_SCALE; scale++) {
    meta.set_scale(scale);
    test_fixed_length_hash<double, VEC_TC_FIXED_DOUBLE>(meta, "fixed_double");
  }
  meta.set_timestamp_tz();
  test_fixed_length_hash<ObOTimestampData, VEC_TC_TIMESTAMP_TZ>(meta, "otimestamp");
  meta.set_interval_ds();
  test_fixed_length_hash<ObIntervalDSValue, VEC_TC_INTERVAL_DS>(meta, "intervalds");

  meta.set_decimal_int(0);
  test_fixed_length_hash<int32_t, VEC_TC_DEC_INT32>(meta, "decint_32");
  test_fixed_length_hash<int64_t, VEC_TC_DEC_INT64>(meta, "decint_64");
  test_fixed_length_hash<int128_t, VEC_TC_DEC_INT128>(meta, "decint_128");
  test_fixed_length_hash<int256_t, VEC_TC_DEC_INT256>(meta, "decint_256");
  test_fixed_length_hash<int512_t, VEC_TC_DEC_INT512>(meta, "decint_512");

  meta.set_number();
  test_discrete_hash<VEC_TC_NUMBER>(meta, "number");
  meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  meta.set_varchar();
  test_discrete_hash<VEC_TC_STRING>(meta, "string");
  meta.set_urowid();
  test_discrete_hash<VEC_TC_ROWID>(meta, "rowid");
  meta.set_raw();
  test_discrete_hash<VEC_TC_RAW>(meta, "raw");
}

// 验证utf8mb4_0900_bin的向量化批量hash与单行hash一致地裁剪末尾空格。
// 注意：本unittest二进制没有注册utf8mb4_0900_bin的charset，datum层hash会透传seed，
// 因此0900_bin用例直接对照murmurhash64A参考值；datum层一致性用已注册的utf8mb4_bin验证。
TEST(ObTestVectorBasicOp, utf8mb4_0900_bin_hash_trailing_space)
{
  const char *case_strs[] = {"",
                             "a",
                             "a ",
                             "a  ",
                             "abc",
                             "abc ",
                             "abc  ",
                             " ",
                             "  ",
                             "abc def ",
                             "\xE4\xB8\xAD\xE6\x96\x87",
                             "\xE4\xB8\xAD\xE6\x96\x87 ",
                             "a\xE4\xB8\xAD b ",
                             "   abc   "};
  const int64_t case_cnt = sizeof(case_strs) / sizeof(case_strs[0]);
  std::vector<int32_t> lens(case_cnt, 0);
  std::vector<char *> ptrs(case_cnt, nullptr);
  std::vector<uint64_t> seeds;
  for (int64_t i = 0; i < case_cnt; i++) {
    lens[i] = strlen(case_strs[i]);
    ptrs[i] = const_cast<char *>(case_strs[i]);
    seeds.push_back(i * 2654435761u + 12345);
  }
  int64_t skip_data = 0;
  sql::ObBitVector *skip = sql::to_bit_vector(&skip_data);
  sql::ObExpr mock_expr;
  sql::EvalBound bound(case_cnt, true);

  // helper: trim trailing spaces
  auto trimmed_len = [&case_strs](int64_t i) -> int32_t {
    int32_t len = strlen(case_strs[i]);
    while (len > 0 && case_strs[i][len - 1] == ' ') { len--; }
    return len;
  };

  // 1. utf8mb4_0900_bin: batch and single-row hashes must agree.
  {
    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(CS_TYPE_UTF8MB4_0900_BIN);
    mock_expr.obj_meta_ = meta;
    ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> vec(lens.data(), ptrs.data(), skip);
    std::vector<uint64_t> vec_hash(case_cnt, 0);

    ASSERT_EQ(OB_SUCCESS, vec.murmur_hash_v3(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true));
    for (int64_t i = 0; i < case_cnt; i++) {
      uint64_t row_hash = 0;
      ASSERT_EQ(OB_SUCCESS, vec.murmur_hash_v3_for_one_row(mock_expr, row_hash, i, case_cnt, seeds[i]));
      ASSERT_EQ(murmurhash64A(ptrs[i], trimmed_len(i), seeds[i]), vec_hash[i]) << "case " << i;
      ASSERT_EQ(row_hash, vec_hash[i]) << "case " << i;
    }
    ASSERT_NE(murmurhash64A(ptrs[2], lens[2], seeds[2]), murmurhash64A(ptrs[2], trimmed_len(2), seeds[2]));

    // A NULL switches dispatch to per-row hashing; non-NULL values must keep the same hashes.
    int64_t null_data = 1;
    ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> nullable_vec(lens.data(), ptrs.data(),
                                                                sql::to_bit_vector(&null_data));
    nullable_vec.set_has_null();
    std::vector<uint64_t> nullable_hash(case_cnt, 0);
    ASSERT_EQ(OB_SUCCESS,
              nullable_vec.murmur_hash_v3(mock_expr, nullable_hash.data(), *skip, bound, seeds.data(), true));
    ASSERT_EQ(seeds[0], nullable_hash[0]);
    for (int64_t i = 1; i < case_cnt; i++) { ASSERT_EQ(vec_hash[i], nullable_hash[i]) << "case " << i; }
    ASSERT_EQ(OB_SUCCESS, vec.crc_hash_v3(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true));
    ASSERT_EQ(OB_SUCCESS, nullable_vec.crc_hash_v3(mock_expr, nullable_hash.data(), *skip, bound, seeds.data(), true));
    for (int64_t i = 1; i < case_cnt; i++) { ASSERT_EQ(vec_hash[i], nullable_hash[i]) << "CRC case " << i; }
  }

  // 2. utf8mb4_bin
  // 行为不变：向量化批量hash裁剪末尾空格，与datum层一致。
  {
    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    mock_expr.obj_meta_ = meta;
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(meta.get_type(), meta.get_collation_type(),
                                                                      meta.get_scale(), false, meta.has_lob_header());
    ASSERT_TRUE(nullptr != basic_funcs);
    ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> vec(lens.data(), ptrs.data(), skip);
    std::vector<ObDatum> datums;
    for (int64_t i = 0; i < case_cnt; i++) { datums.push_back(ObDatum(ptrs[i], lens[i], false)); }
    std::vector<uint64_t> vec_hash(case_cnt, 0);
    std::vector<uint64_t> datum_hash(case_cnt, 0);
    basic_funcs->murmur_hash_v2_batch_(datum_hash.data(), datums.data(), true, *skip, case_cnt, seeds.data(), true);
    ASSERT_EQ(OB_SUCCESS, vec.murmur_hash_v3(mock_expr, vec_hash.data(), *skip, bound, seeds.data(), true));
    for (int64_t i = 0; i < case_cnt; i++) { ASSERT_EQ(datum_hash[i], vec_hash[i]) << "case " << i; }
  }
}

TEST(ObTestVectorBasicOp, cmp_op)
{
  ObObjMeta l_meta, r_meta;
  l_meta.set_int();
  r_meta.set_int();
  test_fixed_length_cmp<VEC_TC_INTEGER, VEC_TC_INTEGER, int64_t, int64_t>(l_meta, r_meta,
                                                                          "int-int");
  r_meta.set_uint64();
  test_fixed_length_cmp<VEC_TC_INTEGER, VEC_TC_UINTEGER, int64_t, uint64_t>(l_meta, r_meta,
                                                                            "int-uint");
  l_meta.set_uint64();
  test_fixed_length_cmp<VEC_TC_UINTEGER, VEC_TC_UINTEGER, uint64_t, uint64_t>(l_meta, r_meta,
                                                                              "uint-uint");
  l_meta.set_float(), r_meta.set_float();
  test_fixed_length_cmp<VEC_TC_FLOAT, VEC_TC_FLOAT, float, float>(l_meta, r_meta, "float-float");
  l_meta.set_double(), r_meta.set_double();
  test_fixed_length_cmp<VEC_TC_DOUBLE, VEC_TC_DOUBLE, double, double>(l_meta, r_meta,
                                                                      "double-double");
  l_meta.set_timestamp_tz(), r_meta.set_timestamp_tz();
  test_fixed_length_cmp<VEC_TC_TIMESTAMP_TZ, VEC_TC_TIMESTAMP_TZ, ObOTimestampData,
                        ObOTimestampData>(l_meta, r_meta, "otimestamp-otimestamp");
  l_meta.set_interval_ds(), r_meta.set_interval_ds();
  test_fixed_length_cmp<VEC_TC_INTERVAL_DS, VEC_TC_INTERVAL_DS, ObIntervalDSValue,
                        ObIntervalDSValue>(l_meta, r_meta, "interval-interval");

  l_meta.set_decimal_int(0), r_meta.set_decimal_int(0);
  test_fixed_length_cmp<VEC_TC_DEC_INT32, VEC_TC_DEC_INT32, int32_t, int32_t>(l_meta, r_meta,
                                                                              "dec32-dec32");
  test_fixed_length_cmp<VEC_TC_DEC_INT64, VEC_TC_DEC_INT64, int64_t, int64_t>(l_meta, r_meta,
                                                                              "dec64-dec64");
  test_fixed_length_cmp<VEC_TC_DEC_INT128, VEC_TC_DEC_INT128, int128_t, int128_t>(l_meta, r_meta,
                                                                                  "dec128-dec128");
  test_fixed_length_cmp<VEC_TC_DEC_INT256, VEC_TC_DEC_INT256, int256_t, int256_t>(l_meta, r_meta,
                                                                                  "dec256-dec256");
  test_fixed_length_cmp<VEC_TC_DEC_INT512, VEC_TC_DEC_INT512, int512_t, int512_t>(l_meta, r_meta,
                                                                                  "dec512-dec512");
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
