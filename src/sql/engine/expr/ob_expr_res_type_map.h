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

#ifndef OB_EXPR_RES_TYPE_MAP_
#define OB_EXPR_RES_TYPE_MAP_

#include "sql/engine/expr/ob_const_map_initializer.h"
#include "lib/container/ob_bit_set.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase {
namespace sql {

struct ObArithRule {
  ObArithRule() {
    reset();
  }

  void reset() {
    result_type = common::ObMaxType;
    param1_calc_type = common::ObMaxType;
    param2_calc_type = common::ObMaxType;
  }

  bool operator == (const ObArithRule &other)
  {
    return result_type == other.result_type
        && param1_calc_type == other.param1_calc_type
        && param2_calc_type == other.param2_calc_type
        ;
  }

  common::ObObjType result_type;
  common::ObObjType param1_calc_type;
  common::ObObjType param2_calc_type;

  TO_STRING_KV("result_type", ob_obj_type_str(result_type),
               "param1_calc_type", ob_obj_type_str(param1_calc_type),
               "param2_calc_type", ob_obj_type_str(param2_calc_type));
};

template<int D1, int D2>
class ObArithRuleMap {
public:
  ObArithRuleMap() {}
  ~ObArithRuleMap() {}
  const ObArithRule &get_rule(int i, int j) const
  {
    return arith_rule_map_[i][j];
  }
  ObArithRule &get_rule(int i, int j)
  {
    return arith_rule_map_[i][j];
  }
private:
  ObArithRule arith_rule_map_[D1][D2];
};

struct ObArithResultTypeChoice {
  enum {
    FIRST = INT64_MIN,   //use the first param type
    SECOND,              //use the second param type
    PARAM_CHOISE_MAX
  };
  static bool is_valid_choice(int64_t choice)
  {
    return choice < PARAM_CHOISE_MAX;
  }
};

class ObArithFlag {
public:
  ObArithFlag() : flags_(0) {}
  ObArithFlag(uint64_t flags) : flags_(flags) {}
  ObArithFlag(const ObArithFlag &other)
  {
    flags_ = other.flags_;
  }

  int pop()
  {
    int next_bit = -1;
    if (flags_ > 0) {
      next_bit = static_cast<int>(__builtin_ctzll(flags_));
      flags_ &= (flags_ - 1);
    }
    return next_bit;
  }
  TO_STRING_KV(K(flags_));

private:
  uint64_t flags_;
};

class ObArithResultTypeMap {
public:

  enum OP : uint64_t {
    ADD     = 1ULL << 0,
    SUB     = 1ULL << 1,
    MUL     = 1ULL << 2,
    DIV     = 1ULL << 3,
    MOD     = 1ULL << 4,
    ROUND   = 1ULL << 5,
    NANVL   = 1ULL << 6,
    SUM     = 1ULL << 7,
    MAX_OP  = 1ULL << 8,
  };

  constexpr static int TYPE_COUNT = static_cast<int>(common::ObMaxType);
  constexpr static int TC_COUNT = static_cast<int>(common::ObMaxTC);
  constexpr static int OP_CNT = __builtin_ctzll(MAX_OP);
  typedef common::ObFixedBitSet<TYPE_COUNT> TypeBitset;

  typedef bool (*is_type_func)(common::ObObjType type);

  static int flag2bit(uint64_t flag) {
    OB_ASSERT(0ULL != flag);
    return static_cast<int>(__builtin_ctzll(flag));
  }

  class RulesApplyer {
  public:
    RulesApplyer(ObArithResultTypeMap &map, ObArithFlag op_flags,
                 TypeBitset &type1_bitset, TypeBitset &type2_bitset)
      : map_(map),
        op_flags_(op_flags),
        type1_bitset_(type1_bitset),
        type2_bitset_(type2_bitset),
        ret_(common::OB_SUCCESS)
    {
    }
    int get_ret()
    {
      return ret_;
    }

    void update_ret(int ret)
    {
      if (common::OB_SUCCESS == ret_) {
        ret_ = ret;
      }
    }

    inline int set_type(int64_t obj_type_or_choice,
                        common::ObObjType type1,
                        common::ObObjType type2,
                        common::ObObjType &result_type)
    {
      int ret = common::OB_SUCCESS;
      if (result_type != common::ObMaxType) {
        ret = common::OB_INIT_TWICE;
      } else if (obj_type_or_choice >= 0 && obj_type_or_choice < TYPE_COUNT) {
        //是一个明确指定的
        result_type = static_cast<common::ObObjType>(obj_type_or_choice);
      } else if (ObArithResultTypeChoice::is_valid_choice(obj_type_or_choice)) {
        //是一个特殊选项
        switch (obj_type_or_choice) {
        case ObArithResultTypeChoice::FIRST:
          result_type = type1;
          break;
        case ObArithResultTypeChoice::SECOND:
          result_type = type2;
          break;
        default:
          ret = common::OB_ERR_UNEXPECTED;
          break;
        }
      } else {
        ret = common::OB_ERR_UNEXPECTED;
      }
      return ret;
    }

    inline RulesApplyer& result_as(int64_t obj_type_or_choice) {
      auto set_result =
          [&] (ObArithRule &rule, common::ObObjType param1, common::ObObjType param2) -> int
      {
        return set_type(obj_type_or_choice, param1, param2, rule.result_type);
      };
      return for_each(set_result);
    }

    inline RulesApplyer& cast_param1_as(int64_t obj_type_or_choice) {
      auto set_param1 =
          [&] (ObArithRule &rule, common::ObObjType param1, common::ObObjType param2) -> int
      {
        return set_type(obj_type_or_choice, param1, param2, rule.param1_calc_type);
      };
      return for_each(set_param1);
    }

    inline RulesApplyer& cast_param2_as(int64_t obj_type_or_choice) {
      auto set_param2 =
          [&] (ObArithRule &rule, common::ObObjType param1, common::ObObjType param2) -> int
      {
        return set_type(obj_type_or_choice, param1, param2, rule.param2_calc_type);
      };
      return for_each(set_param2);
    }

  private:
    template <typename func>
    RulesApplyer& for_each(func &f)
    {
      int ret = ret_;
      int64_t i = 0;
      int64_t j = 0;
      ObArithFlag flags = op_flags_;
      OB_LOG(DEBUG, "foreach assign", K(type1_bitset_), K(type2_bitset_), K(flags));

      for (int op_k = flags.pop(); OB_SUCC(ret) && op_k >= 0; op_k = flags.pop()) {
        for (type1_bitset_.find_first(i); OB_SUCC(ret) && i >= 0; type1_bitset_.find_next(i, i)) {
          for (type2_bitset_.find_first(j); OB_SUCC(ret) && j >= 0; type2_bitset_.find_next(j, j)) {
            common::ObObjType type1 = static_cast<common::ObObjType>(i);
            common::ObObjType type2 = static_cast<common::ObObjType>(j);
            if (op_k >= flag2bit(MAX_OP) || type1 >= TYPE_COUNT || type2 >= TYPE_COUNT) {
              ret = common::OB_ERR_UNEXPECTED;
            } else {
              ret = f(map_.arith_rule_maps_[op_k].get_rule(type1, type2), type1, type2);
              OB_LOG(DEBUG, "assign rules", K(type1), K(type2));
            }
          }
        }
      }
      ret_ = ret;
      return *this;
    }
    ObArithResultTypeMap &map_;
    ObArithFlag op_flags_;
    //store the type1s and type2s that need to operated
    TypeBitset &type1_bitset_;
    TypeBitset &type2_bitset_;
    int ret_;
  };


  ObArithResultTypeMap()
  {}

  ~ObArithResultTypeMap()
  {}

  int init()
  {
    int ret = common::OB_SUCCESS;
    for (int64_t tc_idx = 0; OB_SUCC(ret) && tc_idx < TC_COUNT; ++tc_idx) {
      types_in_tc_set_[tc_idx].reset();
      for (int64_t type_idx = 0; OB_SUCC(ret) && type_idx < TYPE_COUNT; ++type_idx) {
        if (tc_idx == ob_obj_type_class(static_cast<common::ObObjType>(type_idx))) {
          ret = types_in_tc_set_[tc_idx].add_member(type_idx);
        }
      }
      OB_LOG(DEBUG, "tc bitset", K(ret), K(tc_idx), K(types_in_tc_set_[tc_idx]));
    }
    for (int64_t type_idx = 0; OB_SUCC(ret) && type_idx < TYPE_COUNT; ++type_idx) {
      types_set_[type_idx].reset();
      ret = types_set_[type_idx].add_member(type_idx);
      OB_LOG(DEBUG, "type bitset", K(ret), K(type_idx), K(types_set_[type_idx]));
    }
    if (OB_SUCC(ret)) {
      ret = define_rules();
    }
    return ret;
  }

  virtual int define_rules();

  inline int init_type_set_by_func(TypeBitset &type_set, is_type_func func2)
  {
    int ret = common::OB_SUCCESS;
    type_set.reset();
    for (int64_t type_idx = 0; OB_SUCC(ret) && type_idx < TYPE_COUNT; ++type_idx) {
      if (func2(static_cast<common::ObObjType>(type_idx))) {
        ret = type_set.add_member(type_idx);
      }
    }
    return ret;
  }

  TypeBitset &get_type_set(common::ObObjType type, int param_idx, int &ret)
  {
    UNUSED(param_idx);
    UNUSED(ret);
    return types_set_[type];
  }
  TypeBitset &get_type_set(common::ObObjTypeClass type_class, int param_idx, int &ret)
  {
    UNUSED(param_idx);
    UNUSED(ret);
    return types_in_tc_set_[type_class];
  }
  TypeBitset &get_type_set(is_type_func func, int param_idx, int &ret)
  {
    TypeBitset &res_set = (param_idx == 1 ? func1_set_ : func2_set_);
    ret = init_type_set_by_func(res_set, func);
    return res_set;
  }

  template<typename T1, typename T2>
  inline RulesApplyer new_rules(T1 type_desc1, T2 type_desc2, uint64_t flags)
  {
    int ret1 = common::OB_SUCCESS;
    int ret2 = common::OB_SUCCESS;
    TypeBitset &set1 = get_type_set(type_desc1, 1, ret1);
    TypeBitset &set2 = get_type_set(type_desc2, 2, ret2);
    RulesApplyer applyer = RulesApplyer(*this, ObArithFlag(flags), set1, set2);
    applyer.update_ret(ret1);
    applyer.update_ret(ret2);
    OB_LOG(DEBUG, "new rules", K(set1), K(set2), K(flags));
    return applyer;
  }

  //getter

  inline const ObArithRule &get_rule(common::ObObjType type1,
                                     common::ObObjType type2,
                                     uint64_t flag) const
  {
    return arith_rule_maps_[flag2bit(flag)].get_rule(type1, type2);
  }
private:

  ObArithRuleMap<TYPE_COUNT, TYPE_COUNT> arith_rule_maps_[OP_CNT];
  TypeBitset types_in_tc_set_[TC_COUNT]; //把typeclass包含的objtype转换成bitset表示
  TypeBitset types_set_[TYPE_COUNT]; //把objtype转换成bitset表示
  TypeBitset func1_set_;
  TypeBitset func2_set_;
};

extern ObArithResultTypeMap ARITH_RESULT_TYPE_ORACLE;

}
}



#endif // OB_EXPR_RES_TYPE_MAP_
