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

#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_in.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObExprInTest: public ::testing::Test
{
  public:
    ObExprInTest();
    virtual ~ObExprInTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprInTest(const ObExprInTest &other);
    ObExprInTest& operator=(const ObExprInTest &other);
  protected:
    // data members
};

ObExprInTest::ObExprInTest()
{
}

ObExprInTest::~ObExprInTest()
{
}

void ObExprInTest::SetUp()
{
}

void ObExprInTest::TearDown()
{
}


#define IN_GENPARAM2(params, type1, v1, type2, v2)  \
  params[0].set_##type1(v1);                  \
  params[1].set_##type2(v2);\
  if (CS_LEVEL_INVALID == params[0].get_collation_level()) { \
    params[0].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_LEVEL_INVALID == params[1].get_collation_level()) { \
    params[1].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[0].get_collation_type()) { \
    params[0].set_collation_type(CS_TYPE_BINARY); \
  } \
  if (CS_TYPE_INVALID == params[1].get_collation_type()) { \
    params[1].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM3(params, type1, v1, type2, v2, type3, v3) \
  IN_GENPARAM2(params, type1, v1, type2, v2);                 \
  params[2].set_##type3(v3); \
  if (CS_LEVEL_INVALID == params[2].get_collation_level()) { \
    params[2].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[2].get_collation_type()) {\
    params[2].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM4(params, type1, v1, type2, v2, type3, v3, type4, v4) \
  IN_GENPARAM3(params, type1, v1, type2, v2, type3, v3);                \
  params[3].set_##type4(v4); \
  if (CS_LEVEL_INVALID == params[3].get_collation_level()) { \
    params[3].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[3].get_collation_type()) {\
    params[3].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM5(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5) \
  IN_GENPARAM4(params, type1, v1, type2, v2, type3, v3, type4, v4);     \
  params[4].set_##type5(v5); \
  if (CS_LEVEL_INVALID == params[4].get_collation_level()) { \
    params[4].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[4].get_collation_type()) {\
    params[4].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM6(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6) \
  IN_GENPARAM5(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5); \
  params[5].set_##type6(v6); \
  if (CS_LEVEL_INVALID == params[5].get_collation_level()) { \
    params[5].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[5].get_collation_type()) {\
    params[5].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM7(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7) \
  IN_GENPARAM6(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6); \
  params[6].set_##type7(v7); \
  if (CS_LEVEL_INVALID == params[6].get_collation_level()) { \
    params[6].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[6].get_collation_type()) {\
    params[6].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM8(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8) \
  IN_GENPARAM7(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7); \
  params[7].set_##type8(v8); \
  if (CS_LEVEL_INVALID == params[7].get_collation_level()) { \
    params[7].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[7].get_collation_type()) {\
    params[7].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM9(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9) \
  IN_GENPARAM8(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8); \
  params[8].set_##type9(v9); \
  if (CS_LEVEL_INVALID == params[8].get_collation_level()) { \
    params[8].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[8].get_collation_type()) {\
    params[8].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM10(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10) \
  IN_GENPARAM9(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9); \
  params[9].set_##type10(v10); \
  if (CS_LEVEL_INVALID == params[9].get_collation_level()) { \
    params[9].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[9].get_collation_type()) {\
    params[9].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM11(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11) \
  IN_GENPARAM10(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10); \
  params[10].set_##type11(v11); \
  if (CS_LEVEL_INVALID == params[10].get_collation_level()) { \
    params[10].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[10].get_collation_type()) {\
    params[10].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM12(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12) \
  IN_GENPARAM11(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11); \
  params[11].set_##type12(v12); \
  if (CS_LEVEL_INVALID == params[11].get_collation_level()) { \
    params[11].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[11].get_collation_type()) {\
    params[11].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM13(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13) \
  IN_GENPARAM12(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12); \
  params[12].set_##type13(v13); \
  if (CS_LEVEL_INVALID == params[12].get_collation_level()) { \
    params[12].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[12].get_collation_type()) {\
    params[12].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM14(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14) \
  IN_GENPARAM13(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13); \
  params[13].set_##type14(v14); \
  if (CS_LEVEL_INVALID == params[13].get_collation_level()) { \
    params[13].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[13].get_collation_type()) {\
    params[13].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM15(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15) \
  IN_GENPARAM14(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14); \
  params[14].set_##type15(v15); \
  if (CS_LEVEL_INVALID == params[14].get_collation_level()) { \
    params[14].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[14].get_collation_type()) {\
    params[14].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM16(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16) \
  IN_GENPARAM15(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15); \
  params[15].set_##type16(v16); \
  if (CS_LEVEL_INVALID == params[15].get_collation_level()) { \
    params[15].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[15].get_collation_type()) {\
    params[15].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM17(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17) \
  IN_GENPARAM16(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16); \
  params[16].set_##type17(v17); \
  if (CS_LEVEL_INVALID == params[16].get_collation_level()) { \
    params[16].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[16].get_collation_type()) {\
    params[16].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM18(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18) \
  IN_GENPARAM17(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17); \
  params[17].set_##type18(v18); \
  if (CS_LEVEL_INVALID == params[17].get_collation_level()) { \
    params[17].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[17].get_collation_type()) {\
    params[17].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM19(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19) \
  IN_GENPARAM18(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18); \
  params[18].set_##type19(v19); \
  if (CS_LEVEL_INVALID == params[18].get_collation_level()) { \
    params[18].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[18].get_collation_type()) {\
    params[18].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM20(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20) \
  IN_GENPARAM19(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19); \
  params[19].set_##type20(v20); \
  if (CS_LEVEL_INVALID == params[19].get_collation_level()) { \
    params[19].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[19].get_collation_type()) {\
    params[19].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM21(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20, type21, v21) \
  IN_GENPARAM20(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20); \
  params[20].set_##type21(v21); \
  if (CS_LEVEL_INVALID == params[20].get_collation_level()) { \
    params[20].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[20].get_collation_type()) {\
    params[20].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM22(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20, type21, v21, type22, v22) \
  IN_GENPARAM21(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20, type21, v21); \
  params[21].set_##type22(v22); \
  if (CS_LEVEL_INVALID == params[21].get_collation_level()) { \
    params[21].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[21].get_collation_type()) {\
    params[21].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM23(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20, type21, v21, type22, v22, type23, v23) \
  IN_GENPARAM22(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20, type21, v21, type22, v22); \
  params[22].set_##type23(v23); \
  if (CS_LEVEL_INVALID == params[22].get_collation_level()) { \
    params[22].set_collation_level(CS_LEVEL_SYSCONST); \
  } \
  if (CS_TYPE_INVALID == params[22].get_collation_type()) {\
    params[22].set_collation_type(CS_TYPE_BINARY); \
  }

#define IN_GENPARAM24(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20, type21, v21, type22, v22, type23, v23, type24, v24) \
  IN_GENPARAM23(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13, type14, v14, type15, v15, type16, v16, type17, v17, type18, v18, type19, v19, type20, v20, type21, v21, type22, v22, type23, v23); \
  params[23].set_##type24(v24); \
  if (CS_LEVEL_INVALID == params[23].get_collation_level()) { \
    params[23].set_collation_level(CS_LEVEL_SYSCONST); \
  }\
  if (CS_TYPE_INVALID == params[23].get_collation_type()) {\
    params[23].set_collation_type(CS_TYPE_BINARY); \
  }

int calc_cmp_type2(ObExprResType &type, const ObObj &type1, const ObObj & type2)
{
  int ret = OB_SUCCESS;
  ObObjType cmp_type;
  if (OB_SUCC(ObExprResultTypeUtil::get_relational_cmp_type(cmp_type, type1.get_type(), type2.get_type()))) {
    if (ObMaxType == cmp_type) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "failed to get relational cmp types",K(cmp_type), K(type1), K(type1), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type.set_calc_type(cmp_type);
    if (ob_is_string_type(cmp_type)) {
      ObObjMeta coll_types[2];
      coll_types[0] = type1.get_meta();
      coll_types[1] = type2.get_meta();
      ObArenaAllocator alloc;
      ObExprIn dummy_op(alloc);
      ret = dummy_op.aggregate_charsets_for_comparison(type.get_calc_meta(), coll_types, 2);
    }
  }
  return ret;
}

int calc_result_type2(ObExprResType &type, const ObObj &type1, const ObObj & type2)
{
  int ret = OB_SUCCESS;
  ObExprResType cmp_type;
  if (OB_FAIL(calc_cmp_type2(cmp_type, type1, type2))) {
    SQL_ENG_LOG(WARN, "failed to calc cmp types", K(ret));
  } else {
    type.set_int();
    type.set_calc_collation(cmp_type);
    type.set_calc_type(cmp_type.get_calc_type());
  }
  return ret;
}

int calc_result_typeN(ObExprResType &type, const ObObj *types, const int64_t row_dimension, const int64_t param_num)
{
  int ret = OB_SUCCESS;
  int64_t left_start_idx = 0;
  int64_t right_start_idx = row_dimension;
  int64_t right_element_count = param_num / row_dimension - 1;
  for (int64_t i = 0; OB_SUCC(ret) && i < right_element_count;
       ++i, right_start_idx += row_dimension) {
    ObExprResType tmp_res_type;
    for (int64_t j = 0; OB_SUCC(ret) && j < row_dimension; ++j) {
      if (OB_FAIL(calc_result_type2(tmp_res_type, types[left_start_idx+ j], types[right_start_idx + j]))) {
        SQL_ENG_LOG(WARN, "failed to calc result types", K(ret));
      } else if (OB_FAIL(type.get_row_calc_cmp_types().push_back(tmp_res_type.get_calc_meta()))) {
        SQL_ENG_LOG(WARN, "failed to push back cmp type", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_int();
  }
  return ret;
}

#define R(rtype, stype, cslevel, cstype, res, num, di, pn, ...)          \
  {                                                 \
    int ret = OB_SUCCESS; \
    ObObj vres;                                 \
    ObArenaAllocator alloc;\
    ObExprIn op(alloc);                                    \
    op.set_row_dimension(di);                       \
    op.set_real_param_num(pn);                           \
    int64_t bv;                                        \
    ObObj params[num];                          \
    IN_GENPARAM##num(params, __VA_ARGS__);          \
    ObExprCtx expr_ctx(NULL, NULL, NULL, &buf); \
    ObExprResType tmp_ex_type;  \
    ObCollationLevel tmp_level=cslevel; \
    ObCollationType tmp_type=cstype; \
    if (OB_FAIL(calc_result_typeN(tmp_ex_type, params, di, num))) {\
        SQL_ENG_LOG(WARN, "failed to calc result typeN", K(ret));\
    } else {\
      tmp_ex_type.set_collation_level(tmp_level); \
      tmp_ex_type.set_collation_type(tmp_type); \
      op.set_result_type(tmp_ex_type); \
      ret = op.calc_resultN(vres, params, num, expr_ctx); \
      if (OB_SUCC(ret))                        \
      {                                             \
        ASSERT_EQ(rtype, vres.get_type());          \
        if (ObIntType == rtype)                    \
        {                                           \
          ASSERT_EQ(OB_SUCCESS, vres.get_int(bv)); \
          ASSERT_EQ(res, bv);                       \
        }                                           \
      }                                             \
      else                                          \
      {                                             \
        ASSERT_EQ(res, ret);                        \
      }                                             \
    }\
  } while(0)

TEST_F(ObExprInTest, basic_test)
{
  ObMalloc buf;
  /*
   * same type
   */
  //int
  R(ObIntType, int, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, int, 1, int, 2, int, 2, int, 1, int, 1, int, 2, int, 2, int, 3);
  R(ObIntType, tinyint, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, tinyint, 1, tinyint, 2, tinyint, 2, tinyint, 1, tinyint, 1, tinyint, 2, tinyint, 2, tinyint, 3);
  R(ObIntType, smallint, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, smallint, 1, smallint, 2, smallint, 2, smallint, 1, smallint, 1, smallint, 2, smallint, 2, smallint, 3);
  R(ObIntType, mediumint, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, mediumint, 1, mediumint, 2, mediumint, 2, mediumint, 1, mediumint, 1, mediumint, 2, mediumint, 2, mediumint, 3);
  R(ObIntType, int32, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, int32, 1, int32, 2, int32, 2, int32, 1, int32, 1, int32, 2, int32, 2, int32, 3);
  R(ObIntType, uint64, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, uint64, 1, uint64, 2, uint64, 2, uint64, 1, uint64, 1, uint64, 2, uint64, 2, uint64, 3);
  R(ObIntType, utinyint, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, utinyint, 1, utinyint, 2, utinyint, 2, utinyint, 1, utinyint, 1, utinyint, 2, utinyint, 2, utinyint, 3);
  R(ObIntType, usmallint, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, usmallint, 1, usmallint, 2, usmallint, 2, usmallint, 1, usmallint, 1, usmallint, 2, usmallint, 2, usmallint, 3);
  R(ObIntType, umediumint, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, umediumint, 1, umediumint, 2, umediumint, 2, umediumint, 1, umediumint, 1, umediumint, 2, umediumint, 2, umediumint, 3);
  R(ObIntType, uint32, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, uint32, 1, uint32, 2, uint32, 2, uint32, 1, uint32, 1, uint32, 2, uint32, 2, uint32, 3);

  //float/double
  R(ObIntType, float, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, float, 12345.0f, float, -12345.0f, float, 12345.0f, float, -12345.0f, float, 12345.01f, float, 23451.0f, float, -23451.0f, float, 12345.00f);
  R(ObIntType, ufloat, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, ufloat, 12345.0f, ufloat, 12345.0f, ufloat, 12345.0f, ufloat, 12345.0f, ufloat, 12345.01f, ufloat, 23451.0f, ufloat, 23451.0f, ufloat, 12345.00f);
  R(ObIntType, double, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, double, 12345.0f, double, -12345.0f, double, 12345.0f, double, -12345.0f, double, 12345.01f, double, 23451.0f, double, -23451.0f, double, 12345.00f);
  R(ObIntType, udouble, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, udouble, 12345.0f, udouble, 12345.0f, udouble, 12345.0f, udouble, 12345.0f, udouble, 12345.01f, udouble, 23451.0f, udouble, 23451.0f, udouble, 12345.00f);

  //number
  number::ObNumber nmb1, nmb2, nmb3, nmb4, nmb5, nmb6, nmb7, nmb8;
  ASSERT_EQ(OB_SUCCESS, nmb1.from("789.012", buf));
  ASSERT_EQ(OB_SUCCESS, nmb2.from("89.012", buf));
  ASSERT_EQ(OB_SUCCESS, nmb3.from("789.012", buf));
  ASSERT_EQ(OB_SUCCESS, nmb4.from("89.012", buf));
  ASSERT_EQ(OB_SUCCESS, nmb5.from("789", buf));
  ASSERT_EQ(OB_SUCCESS, nmb6.from("789.011", buf));
  ASSERT_EQ(OB_SUCCESS, nmb7.from("789.0", buf));
  ASSERT_EQ(OB_SUCCESS, nmb8.from("789.0120", buf));

  R(ObIntType, number, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, number, nmb1, number, nmb2, number, nmb3, number, nmb4, number, nmb5, number, nmb6, number, nmb7, number, nmb8);
  R(ObIntType, unumber, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, unumber, nmb1, unumber, nmb2, unumber, nmb3, unumber, nmb4, unumber, nmb5, unumber, nmb6, unumber, nmb7, unumber, nmb8);

//  //time
//  R(ObIntType, 1, 8, 2, 4, datetime, 1, datetime, 2, datetime, 2, datetime, 1, datetime, 1, datetime, 2, datetime, 2, datetime, 3);
//  R(ObIntType, 1, 8, 2, 4, timestamp, 1, timestamp, 2, timestamp, 2, timestamp, 1, timestamp, 1, timestamp, 2, timestamp, 2, timestamp, 3);
//  R(ObIntType, 1, 8, 2, 4, date, 1, date, 2, date, 2, date, 1, date, 1, date, 2, date, 2, date, 3);
//  R(ObIntType, 1, 8, 2, 4, time, 1, time, 2, time, 2, time, 1, time, 1, time, 2, time, 2, time, 3);
//  R(ObIntType, 1, 8, 2, 4, year, 1, year, 2, year, 2, year, 1, year, 1, year, 2, year, 2, year, 3);

  //string
  R(ObIntType, vchar, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, varchar, "1", varchar, "2", varchar, "2", varchar, "1", varchar, "1", varchar, "2", varchar, "2", varchar, "3");
  R(ObIntType, char, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, char, "1", char, "2", char, "2", char, "1", char, "1", char, "2", char, "2", char, "3");
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, varbinary, "1", varbinary, "2", varbinary, "2", varbinary, "1", varbinary, "1", varbinary, "2", varbinary, "2", varbinary, "3");
  R(ObIntType, binary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, binary, "1", binary, "2", binary, "2", binary, "1", binary, "1", binary, "2", binary, "2", binary, "3");

  //bool/ext/unknown/null
  R(ObIntType, bool, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, bool, 1, bool, 1, bool, 1, bool, 1, bool, 0, bool, 0, bool, 0, bool, 0);
//  R(ObIntType, 1, 8, 2, 4, unknown, 1, unknown, 2, unknown, 2, unknown, 1, unknown, 22, unknown, 2, unknown, 2, unknown, 3);
  R(ObNullType, null, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 8, 1, 8, null, , null, , null, , null, , null, , null, , null, , null, );

  /*
   * mixed type
   */
  //int
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, int, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, tinyint, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, smallint, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, mediumint, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, int32, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, uint64, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, utinyint, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, usmallint, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, umediumint, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, uint32, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );

  //float/double
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, float, -12345.0f, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, ufloat, 12345.0f, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, float, -12345.0f, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, udouble, 23451.0f, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );

  //number
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, number, nmb1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  //R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 24, 1, 24, unumber, nmb1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );

////  //time
////  R(ObIntType, 1, 8, 2, 4, datetime, 1, datetime, 2, datetime, 2, datetime, 1, datetime, 1, datetime, 2, datetime, 2, datetime, 3);
////  R(ObIntType, 1, 8, 2, 4, timestamp, 1, timestamp, 2, timestamp, 2, timestamp, 1, timestamp, 1, timestamp, 2, timestamp, 2, timestamp, 3);
////  R(ObIntType, 1, 8, 2, 4, date, 1, date, 2, date, 2, date, 1, date, 1, date, 2, date, 2, date, 3);
////  R(ObIntType, 1, 8, 2, 4, time, 1, time, 2, time, 2, time, 1, time, 1, time, 2, time, 2, time, 3);
////  R(ObIntType, 1, 8, 2, 4, year, 1, year, 2, year, 2, year, 1, year, 1, year, 2, year, 2, year, 3);

  //string
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, varchar, "1", int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, char, "1", int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, varbinary, "1", int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, binary, "1", int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );

  //bool/ext/unknown/null
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, bool, 0, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 22, 1, 22, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
//  R(ObIntType, 1, 23, 1, 23, unknown, 1, int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
  R(ObNullType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 23, 1, 23, null, , int, 2, tinyint, 3, smallint, 4, mediumint, 5, int32, 6, uint64, 7, utinyint, 8, usmallint, 9, umediumint, 10, uint32, 11, float, -12345.0f, ufloat, 12345.0f, double, -12345.0f, udouble, 23451.0f, number, nmb1, unumber, nmb2, varchar, "1", char, "2", varbinary, "3", binary, "4", bool, 0, null, );
}

TEST_F(ObExprInTest, test_bugfix_6134133)
{
  ObMalloc buf;
  R(ObIntType, varbinary, CS_LEVEL_INVALID, CS_TYPE_BINARY, 1, 4, 1, 4, int32, 3, int32, 3, varchar, "3", int32, 3);
}

TEST_F(ObExprInTest, tmp_ob_charset)
{
  ObString y1= "你说你爱了";
  ObCollationType cs_type =  CS_TYPE_UTF8MB4_GENERAL_CI;
  int64_t block_len_limit = 6;
  int64_t byte_num = 0;
  int64_t char_num = 0;
  int ret = ObCharset::fit_string(cs_type,
                                       y1.ptr(),
                                       y1.length(),
                                       block_len_limit,
                                       byte_num,
                                       char_num);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(byte_num == 6);
  ASSERT_TRUE(char_num == 2);

  block_len_limit = 7;
  byte_num = 0;
  char_num = 0;
  ret = ObCharset::fit_string(cs_type,
                                       y1.ptr(),
                                       y1.length(),
                                       block_len_limit,
                                       byte_num,
                                       char_num);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(byte_num == 6);
  ASSERT_TRUE(char_num == 2);


  block_len_limit = 8;
  byte_num = 0;
  char_num = 0;
  ret = ObCharset::fit_string(cs_type,
                                       y1.ptr(),
                                       y1.length(),
                                       block_len_limit,
                                       byte_num,
                                       char_num);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(byte_num == 6);
  ASSERT_TRUE(char_num == 2);


  block_len_limit = 1;
  byte_num = 0;
  char_num = 0;
  ret = ObCharset::fit_string(cs_type,
                                       y1.ptr(),
                                       y1.length(),
                                       block_len_limit,
                                       byte_num,
                                       char_num);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(byte_num == 0);
  ASSERT_TRUE(char_num == 0);



  block_len_limit = 9;
  byte_num = 0;
  char_num = 0;
  ret = ObCharset::fit_string(cs_type,
                                       y1.ptr(),
                                       y1.length(),
                                       block_len_limit,
                                       byte_num,
                                       char_num);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(byte_num == 9);
  ASSERT_TRUE(char_num == 3);

  block_len_limit = 15;
  byte_num = 0;
  char_num = 0;
  ret = ObCharset::fit_string(cs_type,
                                       y1.ptr(),
                                       y1.length(),
                                       block_len_limit,
                                       byte_num,
                                       char_num);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(byte_num == 15);
  ASSERT_TRUE(char_num == 5);

  block_len_limit = 999;
  byte_num = 0;
  char_num = 0;
  ret = ObCharset::fit_string(cs_type,
                                       y1.ptr(),
                                       y1.length(),
                                       block_len_limit,
                                       byte_num,
                                       char_num);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(byte_num == 15);
  ASSERT_TRUE(char_num == 5);

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
