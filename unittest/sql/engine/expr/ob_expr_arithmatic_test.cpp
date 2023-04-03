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

#define USING_LOG_PREFIX SQL_ENG
#include <iostream>
#include <fstream>
#include <string.h>
#include <gtest/gtest.h>
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_minus.h"
#include "sql/engine/expr/ob_expr_mul.h"
#include "sql/engine/expr/ob_expr_div.h"
#include "sql/engine/expr/ob_expr_mod.h"
#include "sql/engine/expr/ob_expr_bit_and.h"
#include "sql/engine/expr/ob_expr_bit_or.h"
#include "share/object/ob_obj_cast.h"
#include "ob_expr_test_utils.h"
#include "lib/oblog/ob_log.h"
#include "common/object/ob_object.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::common::hash;

class ObExprArithmaticTest: public ::testing::Test
{
public:
  ObExprArithmaticTest();
  virtual ~ObExprArithmaticTest();
  virtual void SetUp();
  virtual void TearDown();

  enum EXPR_TYPE
  {
    NOT_SUPPORTED,
    ADD,
    MINUS,
    MUL,
    DIV,
    MOD,
    BIT_AND,
    BIT_OR,
    EXPR_COUNT
  };

  enum SERVER_TYPE
  {
    MYSQL,
    OB
  };

  static ObArenaAllocator alloc;
  static ObExprAdd add_optr;
  static ObExprMinus minus_optr;
  static ObExprMul mul_optr;
  static ObExprDiv div_optr;
  static ObExprMod mod_optr;
  static ObExprBitAnd bit_and_optr;
  static ObExprBitOr bit_or_optr;

  static ObExprOperator* operators[EXPR_COUNT];

  static int init_arithmetic_test();
  static int lookup_type_from_str(SERVER_TYPE server,
      const char *str, int64_t len, ObObjType &type);
  static int build_obj_from_str(const char *str, int64_t len,
                                ObObjType type,
                                ObObj &obj,
                                ObIAllocator& buf);
  static int get_expr_type_from_str(const char *str, int64_t len, EXPR_TYPE& type);
  static int get_res_type_from_str(char *str, int64_t len, ObObjType &type);
  static bool type_need_fuzzy_cmp(ObObjType type1, ObObjType type2);
  static int truncate_number_precision(number::ObNumber &num,
                                       number::ObNumber &num_trunc,
                                       int16_t scale,
                                       ObIAllocator &buf);

private:
  // disallow copy
  ObExprArithmaticTest(const ObExprArithmaticTest &other);
  ObExprArithmaticTest& operator=(const ObExprArithmaticTest &other);

  static bool map_inited;
  static ObHashMap<ObString, ObObjType> str_to_type_map_ob_;
  static ObHashMap<ObString, ObObjType> str_to_type_map_mysql_;
  static ObHashMap<ObString, EXPR_TYPE> str_to_expr_type_map_;

protected:
  // data members
};

bool ObExprArithmaticTest::map_inited = false;
ObHashMap<ObString, ObObjType> ObExprArithmaticTest::str_to_type_map_ob_;
ObHashMap<ObString, ObObjType> ObExprArithmaticTest::str_to_type_map_mysql_;
ObHashMap<ObString, ObExprArithmaticTest::EXPR_TYPE> ObExprArithmaticTest::str_to_expr_type_map_;
ObExprOperator* ObExprArithmaticTest::operators[EXPR_COUNT] = {NULL};

ObArenaAllocator ObExprArithmaticTest::alloc;
ObExprAdd ObExprArithmaticTest::add_optr(alloc);
ObExprMinus ObExprArithmaticTest::minus_optr(alloc);
ObExprMul ObExprArithmaticTest::mul_optr(alloc);
ObExprDiv ObExprArithmaticTest::div_optr(alloc);
ObExprMod ObExprArithmaticTest::mod_optr(alloc);
ObExprBitAnd ObExprArithmaticTest::bit_and_optr(alloc);
ObExprBitOr ObExprArithmaticTest::bit_or_optr(alloc);

int ObExprArithmaticTest::truncate_number_precision(number::ObNumber &num,
                                                    number::ObNumber &num_trunc,
                                                    int16_t scale,
                                                    ObIAllocator &buf)
{
  int ret = OB_SUCCESS;
  const static int64_t LITERAL_BUF_LEN = 256;
  int64_t pos = 0;
  char num_literal_buf[LITERAL_BUF_LEN];
  if (OB_SUCCESS != (ret = num.format(num_literal_buf, LITERAL_BUF_LEN, pos, -1))) {
    _OB_LOG(WARN, "fail format number");
  } else {
    if (pos < 0 || pos > LITERAL_BUF_LEN - 1) {
      _OB_LOG(WARN, "number literal overflow");
    } else {
      num_literal_buf[pos] = 0;
    }
    if (OB_SUCCESS != (ret = num_trunc.from(30, scale, num_literal_buf, strlen(num_literal_buf), buf))) {
      _OB_LOG(WARN, "number from str failed");
    }
  }
  return ret;
}

bool ObExprArithmaticTest::type_need_fuzzy_cmp(ObObjType type1, ObObjType type2)
{
  bool bool_ret = false;
  if (ObFloatType == type1
      || ObFloatType == type2
      || ObDoubleType == type1
      || ObDoubleType == type2) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}


int ObExprArithmaticTest::get_expr_type_from_str(const char *str, int64_t len, EXPR_TYPE& type)
{
  int ret = OB_SUCCESS;
  if (false == map_inited) {
    ret = OB_NOT_INIT;
  } else {
    ObString str_val(0, static_cast<int32_t>(len), str);
    ret = str_to_expr_type_map_.get_refactored(str_val, type);
    if (OB_HASH_NOT_EXIST == ret) {
      type = NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObExprArithmaticTest::build_obj_from_str(const char *str,
                                             int64_t len,
                                             ObObjType type,
                                             ObObj &obj,
                                             ObIAllocator& buf)
{
  int ret = OB_SUCCESS;
  if (0 == strncmp("null", str, len)) {
    obj.set_null();
  } else {
    ObString str_val(0, static_cast<int32_t>(len), str);
    ObObj str_obj;
    str_obj.set_varchar(str_val);
    ObCastCtx cast_ctx(&buf,
                       NULL,
                       0,
                       CM_NONE,
                       CS_TYPE_INVALID,
                       NULL);
    if (OB_FAIL(ObObjCaster::to_type(type, cast_ctx, str_obj, obj))) {
      _OB_LOG(ERROR, "failed to cast obj, str = %s",str);
    }
  }
  return ret;
}

int ObExprArithmaticTest::lookup_type_from_str(SERVER_TYPE server,
                                               const char *str,
                                               int64_t len,
                                               ObObjType &type)
{
  int ret = OB_SUCCESS;
  if (false == map_inited) {
    ret = OB_NOT_INIT;
  } else {
    ObString key(0, static_cast<int32_t>(len), str);
    if (MYSQL == server) {
      ret = str_to_type_map_mysql_.get_refactored(key, type);
    } else if (OB == server) {
      ret = str_to_type_map_ob_.get_refactored(key, type);
    }
    if (OB_HASH_NOT_EXIST == ret) {
      type = ObMaxType;
    }
  }
  return ret;
}

int ObExprArithmaticTest::init_arithmetic_test()
{
  int ret = OB_SUCCESS;
  if (!map_inited) {

    operators[ADD] = &add_optr;
    operators[MINUS] = &minus_optr;
    operators[MUL] = &mul_optr;
    operators[DIV] = &div_optr;
    operators[MOD] = &mod_optr;
    operators[BIT_AND] = &bit_and_optr;
    operators[BIT_OR] = &bit_or_optr;

    if (OB_SUCCESS != (ret = str_to_type_map_ob_.create(10, ObModIds::OB_HASH_BUCKET))) {
      _OB_LOG(ERROR, "failed to allocate memory ret = %d", ret);
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("tinyint"), ObTinyIntType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("smallint"), ObSmallIntType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("mediumint"), ObMediumIntType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("int32"), ObInt32Type))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("int"), ObIntType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("utinyint"), ObUTinyIntType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("usmallint"), ObUSmallIntType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("umediumint"), ObUMediumIntType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("uint32"), ObUInt32Type))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("uint64"), ObUInt64Type))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("float"), ObFloatType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("double"), ObDoubleType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("varchar"), ObVarcharType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("char"), ObCharType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_ob_.set_refactored(ObString::make_string("number"), ObNumberType))) {
      _OB_LOG(ERROR, "failed to insert type hash");
    }
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = str_to_type_map_mysql_.create(10, ObModIds::OB_HASH_BUCKET))) {
        _OB_LOG(ERROR, "failed to allocate memory ret = %d", ret);
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("tinyint"), ObTinyIntType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("smallint"), ObSmallIntType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("mediumint"), ObMediumIntType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("int"), ObInt32Type))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("bigint"), ObIntType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("tinyint unsigned"),
              ObUTinyIntType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("smallint unsigned"),
              ObUSmallIntType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("mediumint unsigned"),
              ObUMediumIntType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("int unsigned"), ObUInt32Type))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("bigint unsigned"), ObUInt64Type))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("float"), ObFloatType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("double"), ObDoubleType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("varchar"), ObVarcharType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("char"), ObCharType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("number"), ObNumberType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("decimal"), ObNumberType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_type_map_mysql_.set_refactored(ObString::make_string("decimal unsigned"), ObUNumberType))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      }

    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = str_to_expr_type_map_.create(10, ObModIds::OB_HASH_BUCKET))) {
        _OB_LOG(ERROR, "failed to allocate memory ret = %d", ret);
      } else if (OB_SUCCESS != (ret =
          str_to_expr_type_map_.set_refactored(ObString::make_string("add"), ADD))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_expr_type_map_.set_refactored(ObString::make_string("minus"), MINUS))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_expr_type_map_.set_refactored(ObString::make_string("mul"), MUL))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_expr_type_map_.set_refactored(ObString::make_string("div"), DIV))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_expr_type_map_.set_refactored(ObString::make_string("mod"), MOD))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_expr_type_map_.set_refactored(ObString::make_string("bit_and"), BIT_AND))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      } else if (OB_SUCCESS != (ret =
          str_to_expr_type_map_.set_refactored(ObString::make_string("bit_or"), BIT_OR))) {
        _OB_LOG(ERROR, "failed to insert type hash");
      }
    }
  }
  if (OB_SUCCESS == ret) {
    map_inited = true;
    ret = OB_SUCCESS;
  } else {
    OB_ASSERT(false);
    ret = OB_ERROR;
  }
  return ret;
}

int ObExprArithmaticTest::get_res_type_from_str(char *str, int64_t len, ObObjType &type)
{
  int ret = OB_SUCCESS;
  char *space_pos = NULL;
  char *left_brac_pos = NULL;
  int64_t real_type_str_len = 0;
  //normalize type name.
  left_brac_pos = strchr(str, '(');
  space_pos = strchr(str, ' ');
  if (NULL == space_pos) { //one-part type name
    if (NULL != left_brac_pos) { //have bracket
      *left_brac_pos = '\0';  //truncate
      real_type_str_len = static_cast<int64_t>(left_brac_pos - str);
    } else {
      real_type_str_len = len;//no bracket
    }
  } else { //two-part type name
    if (NULL != left_brac_pos) { //have bracket
      memmove(left_brac_pos, space_pos, strlen(space_pos) + 1);//including the '\0'
      real_type_str_len = strlen(str);
    } else { //no bracket
      real_type_str_len = len;
    }
  }
  //we got normalized type name, brackets are trimed
  ret = lookup_type_from_str(MYSQL, str, real_type_str_len, type);
  return ret;
}

ObExprArithmaticTest::ObExprArithmaticTest()
{
  init_arithmetic_test();
}

ObExprArithmaticTest::~ObExprArithmaticTest()
{
}

void ObExprArithmaticTest::SetUp()
{
}

void ObExprArithmaticTest::TearDown()
{
}

#define LINE_BUF_SIZE 1024
TEST_F(ObExprArithmaticTest, arithmatic_test)
{
  int ret = OB_SUCCESS;
  ObMalloc buf;
  //here we begin
  const char *test_file = "./arithmatic.test";
  const char *out_file = "./arithmatic.out";
  //line buffer, we need to adjust the input string so make copy
  char line_buf[LINE_BUF_SIZE] = {0};
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_output(out_file);
  ASSERT_TRUE(of_output.is_open());
  std::string line;

  int64_t column_count = 0;
  int64_t case_count = 0;
  int64_t failed_count = 0;
  bool is_succ = false;
  char *saved_strtok_pos = NULL;
  char *token_start = NULL;
  bool line_valid = true;
  bool line_finished = false;
  bool is_expect_overflow = false;
  bool wrong_type = false;
  EXPR_TYPE expr_type = NOT_SUPPORTED;
  ObObjType opnd1_type = ObMaxType;
  ObObjType opnd2_type = ObMaxType;
  ObObjType res_expect_type = ObMaxType;
  ObObj opnd1_obj;
  ObObj opnd2_obj;
  ObObj res_calc_obj;
  ObObj res_obj;
  ObObj res_check;

  of_output<<"Failed cases:"<<std::endl;
  //do not check ret, because we want to run all the test and see which ones fail
  //also, reset ret each loop
  while (std::getline(if_tests, line)) {
    if (line.size() <= 0 //skip invalid/comment lines
        || '#' == line.at(0)
        || 'v' == line.at(0)) {
      continue;
    }
    ret = OB_SUCCESS;
    ++case_count;
    strncpy(line_buf, line.c_str(), LINE_BUF_SIZE);
    line_buf[LINE_BUF_SIZE - 1] = '\0';   //we are safe to call strtok_r
    line_valid = true;
    line_finished = false;
    is_expect_overflow = false;
    wrong_type = false;
    is_succ = false;
    column_count = 0;
    //start to parse a line
    if (NULL == (token_start = strtok_r(line_buf, "|", &saved_strtok_pos))) {
      _OB_LOG(WARN, "invalid line format found. %s", line_buf);
      line_valid = false;
    } else {
      ++column_count;
      while ((column_count <= 7) && (line_valid) && (!line_finished)) {
        switch (column_count) {
          case 1: {
            expr_type = NOT_SUPPORTED;
            //strtok_r always return null terminated string, safe to call strlen
            if (NOT_SUPPORTED == (ret = get_expr_type_from_str(token_start,
                                                               strlen(token_start), expr_type))) {
              _OB_LOG(WARN, "Not supported operator type found. %s", token_start);
              line_valid = false;
            }
            break;
          }
          case 2: {
            opnd1_type = ObMaxType;
            if (OB_HASH_NOT_EXIST == (ret = lookup_type_from_str(OB, token_start,
                                                              strlen(token_start), opnd1_type))) {
              _OB_LOG(WARN, "Not supported left operand type found. %s", token_start);
              line_valid = false;
            }
            break;
          }
          case 3: {
            opnd2_type = ObMaxType;
            if (OB_HASH_NOT_EXIST == (ret = lookup_type_from_str(OB, token_start,
                                                              strlen(token_start), opnd2_type))) {
              _OB_LOG(WARN, "Not supported right operand type found. %s", token_start);
              line_valid = false;
            }
            break;
          }
          case 4: {
            res_expect_type = ObMaxType;
            if (OB_HASH_NOT_EXIST == (ret = get_res_type_from_str(token_start,
                                                               strlen(token_start), res_expect_type))) {
              _OB_LOG(WARN, "Not supported result type found. %s", token_start);
              line_valid = false;
            }
            break;
          }
          case 5: {
            if(OB_SUCCESS != (ret = build_obj_from_str(token_start,
                                                       strlen(token_start), opnd1_type, opnd1_obj, buf))) {
              _OB_LOG(WARN, "left operand format error");
              line_valid = false;
            }
            break;
          }
          case 6: {
            if(OB_SUCCESS != (ret = build_obj_from_str(token_start,
                                                       strlen(token_start), opnd2_type, opnd2_obj, buf))) {
              _OB_LOG(WARN, "right operand format error");
              line_valid = false;
            }
            break;
          }
          case 7: {
            if (!(is_expect_overflow = (0 == strncmp("overflow", token_start,
                                                     strlen(token_start))))) {
              if(OB_SUCCESS != (ret = build_obj_from_str(token_start,
                                                         strlen(token_start), res_expect_type, res_obj, buf))) {
                _OB_LOG(WARN, "result format error");
                line_valid = false;
              }
            }
            break;
          }
          default: {
            _OB_LOG(WARN, "wrong column count %s", line.c_str());
            line_valid = false;
            break;
          }
        }
        if (NULL == (token_start = strtok_r(NULL, "|", &saved_strtok_pos))) {
          if (7 == column_count) {
            line_finished = true;
          } else {
            _OB_LOG(WARN, "wrong column count %s", line.c_str());
            line_valid = false;
          }
        }
        ++column_count;
      }
    }
    //do specified calculation
    if (OB_SUCCESS == ret && line_valid) {
      if (NOT_SUPPORTED == expr_type) {
        OB_ASSERT(false); //NOT_SUPPORTED should have been excluded in parsing
      } else {
        ObExprCtx expr_ctx(NULL, NULL, NULL, &buf);
        if (OB_SUCCESS != (ret = operators[expr_type]->calc_result2(res_calc_obj, opnd1_obj, opnd2_obj, expr_ctx))) {
          if (is_expect_overflow && OB_DATA_OUT_OF_RANGE == ret) {
            ret = OB_SUCCESS; //overflow is what we expect, so recover
            is_succ = true;
          } else {
            _OB_LOG(WARN, "calc failed %s", line.c_str());
            is_succ = false;
          }
        } else { //ret is success
          if (is_expect_overflow) {
            is_succ = false; //we want overflow
          } else { //expect is not overflow, and ret == success, do the check
            //hack: truncate number scale to 4, follow mysql div default
            if (OB_SUCCESS == ret &&
                (ObNumberType == res_expect_type || ObUNumberType == res_expect_type)) {
              number::ObNumber num;
              number::ObNumber num_trunc;
              if (ObNumberType == res_calc_obj.get_type()) {
                if (OB_SUCCESS != (ret = res_calc_obj.get_number(num))) {
                  _OB_LOG(WARN, "get number failed");
                }
              } else if (ObUNumberType == res_calc_obj.get_type()) {
                if (OB_SUCCESS != (ret = res_calc_obj.get_unumber(num))) {
                  _OB_LOG(WARN, "get unumber failed");
                }
              } else if (!res_calc_obj.is_null()){
                wrong_type = true;
              }

              if (!wrong_type) {
                if (OB_SUCCESS != (truncate_number_precision(num, num_trunc, 4, buf))) {
                  _OB_LOG(WARN, "truncate number failed");
                } else if (ObNumberType == res_calc_obj.get_type()) {
                  res_calc_obj.set_number(num_trunc);
                } else if (ObUNumberType == res_calc_obj.get_type()) {
                  res_calc_obj.set_unumber(num_trunc);
                }
              }
            }
            //end hack
            //type check. Only check typeclass
            if (OB_SUCCESS == ret &&
                (ob_obj_type_class(res_expect_type) != res_calc_obj.get_type_class())
                && !res_calc_obj.is_null()) {
              wrong_type = true;
            }
            if (OB_SUCCESS == ret && !wrong_type) {
              if (type_need_fuzzy_cmp(opnd1_type, opnd2_type)) {
                //res must be double type
                double res = res_obj.get_double();
                double res_calc = res_calc_obj.get_double();
                if (0 == double_cmp_given_precision(res, res_calc, 5)) {
                  is_succ = true;
                } else {
                  is_succ = false;
                }
              } else {
                is_succ = ObObjCmpFuncs::compare_oper_nullsafe(res_obj, res_calc_obj, CS_TYPE_UTF8MB4_BIN, CO_EQ);
              }
            }
          }
        }
      }
      if (!is_succ) {
        ++failed_count;
        of_output<<line<<std::endl<<"# calculated:"<<to_cstring(res_calc_obj)<<std::endl;
      }
    }
  }
  of_output<<"Total\t"<<case_count<<" cases."<<std::endl;
  of_output<<"Failed\t"<<failed_count<<" cases."<<std::endl;
  of_output.close();
  if_tests.close();
  //EXPECT_TRUE(0 == failed_count);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
