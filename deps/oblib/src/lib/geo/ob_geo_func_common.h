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
 * This file contains implementation for ob_geo_func_common
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_COMMON_H_
#define OCEANBASE_LIB_OB_GEO_FUNC_COMMON_H_

#define BOOST_GEOMETRY_DISABLE_DEPRECATED_03_WARNING 1
#define BOOST_ALLOW_DEPRECATED_HEADERS 1

#include <exception>
#pragma push_macro("E")
#undef E
#include <boost/geometry.hpp>
#include <boost/geometry/core/exception.hpp>
#pragma pop_macro("E")
#include "lib/ob_errno.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_bin_traits.h"
#include "lib/geo/ob_geo_tree_traits.h"
#include "lib/string/ob_string.h"
//#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{
// boost::geometry strategies
typedef boost::geometry::strategy::within::geographic_winding<common::ObWkbGeogPoint> ObPlPaStrategy;
typedef boost::geometry::strategy::intersection::geographic_segments<> ObLlLaAaStrategy;

// non-geo arguments, e.g. distance_sphere
union ObGeoNormalVal {
  int64_t int64_;
  double double_;
  const common::ObString *string_;
  common::ObGeoBufferStrategy *strategy_; // todo@dazhi
};

// geo eval ctx to call geo functions
class ObGeoEvalCtx
{
public:
  ObGeoEvalCtx() : allocator_(NULL), srs_(NULL), g_arg_c_(0), v_arg_c_(0), is_called_in_pg_expr_(false){};
  ObGeoEvalCtx(common::ObIAllocator * allocator) :
    allocator_(allocator), srs_(NULL), g_arg_c_(0), v_arg_c_(0), is_called_in_pg_expr_(false) {};
  ObGeoEvalCtx(common::ObIAllocator * allocator, const common::ObSrsItem *srs_item) :
    allocator_(allocator), srs_(srs_item), g_arg_c_(0), v_arg_c_(0), is_called_in_pg_expr_(false) {};
  ~ObGeoEvalCtx() = default;

  inline int append_geo_arg(const common::ObGeometry *g)
  {
    INIT_SUCC(ret);
    if (g_arg_c_ < MAX_ARG_COUNT) {
      gis_args_[g_arg_c_++] = g;
    } else {
      ret = common::OB_ERR_ARGUMENT_OUT_OF_RANGE;
    }
    return ret;
  }

  inline int append_val_arg(ObGeoNormalVal &value)
  {
    INIT_SUCC(ret);
    if (v_arg_c_ < MAX_ARG_COUNT) {
      val_args_[v_arg_c_++] = value;
    } else {
      ret = common::OB_ERR_ARGUMENT_OUT_OF_RANGE;
    }
    return ret;
  }

  inline int append_val_arg(common::ObGeoBufferStrategy *value)
  {
    INIT_SUCC(ret);
    if (v_arg_c_ < MAX_ARG_COUNT) {
      ObGeoNormalVal n_val; // todo@dazhi: remove stack variable
      n_val.strategy_ = value;
      val_args_[v_arg_c_++] = n_val;
    } else {
      ret = common::OB_ERR_ARGUMENT_OUT_OF_RANGE;
    }
    return ret;
  }

  inline int append_val_arg(int64_t value)
  {
    INIT_SUCC(ret);
    if (v_arg_c_ < MAX_ARG_COUNT) {
      ObGeoNormalVal n_val; // todo@dazhi: remove stack variable
      n_val.int64_ = value;
      val_args_[v_arg_c_++] = n_val;
    } else {
      ret = common::OB_ERR_ARGUMENT_OUT_OF_RANGE;
    }
    return ret;
  }

  inline int append_val_arg(double value)
  {
    INIT_SUCC(ret);
    if (v_arg_c_ < MAX_ARG_COUNT) {
      ObGeoNormalVal n_val;
      n_val.double_ = value;
      val_args_[v_arg_c_++] = n_val;
    } else {
      ret = common::OB_ERR_ARGUMENT_OUT_OF_RANGE;
    }
    return ret;
  }

  inline int append_val_arg(const common::ObString *value)
  {
    INIT_SUCC(ret);
    if (v_arg_c_ < MAX_ARG_COUNT) {
      ObGeoNormalVal n_val;
      n_val.string_ = value;
      val_args_[v_arg_c_++] = n_val;
    } else {
      ret = common::OB_ERR_ARGUMENT_OUT_OF_RANGE;
    }
    return ret;
  }

  inline int get_geo_count() const { return g_arg_c_; }
  inline int get_val_count() const { return v_arg_c_; }
  inline const common::ObGeometry *get_geo_arg(int idx) const
  {
    const common::ObGeometry *geo_ret = NULL;
    if (idx >= 0 && idx < g_arg_c_) {
      geo_ret = gis_args_[idx];
    }
    return geo_ret;
  }

  inline const ObGeoNormalVal *get_val_arg(int idx) const
  {
    const ObGeoNormalVal *val_ret = NULL;
    if (idx >= 0 && idx < v_arg_c_) {
        val_ret = &val_args_[idx];
    }
    return val_ret;
  }

  inline common::ObIAllocator * get_allocator() const { return allocator_; }
  inline const common::ObSrsItem *get_srs() const { return srs_; }

  inline void set_is_called_in_pg_expr(bool in) { is_called_in_pg_expr_ = in; }
  inline bool get_is_called_in_pg_expr() const { return is_called_in_pg_expr_; }

  // interfaces for unittest only
  inline void ut_set_geo_count(int count)
  {
    g_arg_c_ = (count >= MAX_ARG_COUNT ? MAX_ARG_COUNT - 1 : count);
  }

  inline void ut_set_geo_arg(int index, common::ObGeometry *g)
  {
    index = (index >= MAX_ARG_COUNT ? MAX_ARG_COUNT - 1 : index);
    gis_args_[index] = g;
  }
  // end interfaces for unittest

private:
  static const int MAX_ARG_COUNT = 3;

  common::ObIAllocator *allocator_; // reserved for allocator
  const common::ObSrsItem *srs_; // get parsed srs or boost context
  int g_arg_c_; // num of geo arguments
  int v_arg_c_; // num of other arguments, e.g. distance_sphere
  const common::ObGeometry *gis_args_[MAX_ARG_COUNT]; // geo arguments
  ObGeoNormalVal val_args_[MAX_ARG_COUNT]; // other arguments
  bool is_called_in_pg_expr_; // distinguish pg/mysql expr call

private:
  DISALLOW_COPY_AND_ASSIGN(ObGeoEvalCtx);
};

struct ObGeoFuncResWithNull {
  bool bret = false;
  bool is_null = false;
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_COMMON_H_