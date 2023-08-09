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
 * This file contains implementation support for the SRS wkt parser abstraction.
 */


#define USING_LOG_PREFIX LIB
#include "boost/fusion/include/adapt_struct.hpp"
#include "boost/spirit/include/qi.hpp"
#include "boost/spirit/include/phoenix.hpp"
#include "boost/bind/bind.hpp"
#include "lib/geo/ob_srs_wkt_parser.h"
#include "lib/string/ob_string_buffer.h"
#include "boost/lambda/lambda.hpp"

using namespace boost::spirit;
using namespace oceanbase::common;

BOOST_FUSION_ADAPT_STRUCT(ObGeographicRs,
                          (ObString, rs_name)
                          (ObRsDatum, datum_info)
                          (ObPrimem, primem)
                          (ObRsUnit, unit)
                          (ObRsAxisPair, axis)
                          (ObRsAuthority, authority))

BOOST_FUSION_ADAPT_STRUCT(ObRsDatum,
                          (ObString, name)
                          (ObSpheroid, spheroid)
                          (ObTowgs84, towgs84)
                          (ObRsAuthority, authority))

BOOST_FUSION_ADAPT_STRUCT(ObSpheroid,
                          (ObString, name)
                          (double, semi_major_axis)
                          (double, inverse_flattening)
                          (ObRsAuthority, authority))

BOOST_FUSION_ADAPT_STRUCT(ObRsAuthority,
                          (bool, is_valid)
                          (ObString, org_name)
                          (ObString, org_code))

BOOST_FUSION_ADAPT_STRUCT(ObTowgs84,
                          (bool, is_valid)
                          (double, value[0])
                          (double, value[1])
                          (double, value[2])
                          (double, value[3])
                          (double, value[4])
                          (double, value[5])
                          (double, value[6]))

BOOST_FUSION_ADAPT_STRUCT(ObRsAxisPair,
                          (ObRsAxis, x)
                          (ObRsAxis, y))

BOOST_FUSION_ADAPT_STRUCT(ObRsAxis,
                          (ObString, name)
                          (ObAxisDirection, direction))

BOOST_FUSION_ADAPT_STRUCT(ObPrimem,
                          (ObString, name)
                          (double, longtitude)
                          (ObRsAuthority, authority))

BOOST_FUSION_ADAPT_STRUCT(ObRsUnit,
                          (ObString, type)
                          (double, conversion_factor)
                          (ObRsAuthority, authority))

BOOST_FUSION_ADAPT_STRUCT(ObProjection,
                          (ObString, name)
                          (ObRsAuthority, authority))

BOOST_FUSION_ADAPT_STRUCT(ObProjectionPram,
                          (ObString, name)
                          (double, value)
                          (ObRsAuthority, authority))

BOOST_FUSION_ADAPT_STRUCT(ObProjectionRs,
                          (ObString, rs_name)
                          (ObGeographicRs, projected_rs)
                          (ObProjection, projection)
                          (ObProjectionPrams, proj_params)
                          (ObRsUnit, unit)
                          (ObRsAxisPair, axis)
                          (ObRsAuthority, authority))


namespace boost
{
namespace spirit
{
namespace traits
{

template<>
struct is_container<ObProjectionPrams> : boost::mpl::true_{};

template<>
struct container_value<ObProjectionPrams>
{
    typedef ObProjectionPram type;
};

template<>
struct push_back_container<ObProjectionPrams, ObProjectionPram>
{
    static bool call(ObProjectionPrams& c, ObProjectionPram val)
    {
        c.vals.push_back(val);
        return true;
    }
};

template<>
struct is_container<ObQiString> : boost::mpl::true_
{};

template<>
struct container_value<ObQiString>
{
    typedef char type;
};

template<>
struct push_back_container<ObQiString, char>
{
    static bool call(ObQiString& c, char val)
    {
        c.val_.append(&val, sizeof(char));
        return true;
    }
};

}
}
}

namespace oceanbase
{
namespace common
{

typedef boost::variant<ObGeographicRs, ObProjectionRs> ObGeoRs;

// for more details about coordinate system definition, refer to
// https://dev.mysql.com/blog-archive/geographic-spatial-reference-systems-in-mysql-8-0
// https://dev.mysql.com/blog-archive/projected-spatial-reference-systems-in-mysql-8-0
template <typename Iterator, typename Skipper>
struct SrsWktGrammar : qi::grammar<Iterator, ObGeoRs(), Skipper>
{
  SrsWktGrammar(char l_brac, char r_brac, common::ObIAllocator &allocator) : SrsWktGrammar::base_type(start_)
  {
    // rules definition
    l_brac_ = qi::lit(l_brac);
    r_brac_ = qi::lit(r_brac);
    comma_ = qi::lit(',');
    geog_lit_ = qi::no_case[qi::lit("GEOGCS")];
    datum_lit_ = qi::no_case[qi::lit("DATUM")];
    spher_lit_ = qi::no_case[qi::lit("SPHEROID")];
    towgs_lit_ = qi::no_case[qi::lit("TOWGS84")];
    auth_lit_= qi::no_case[qi::lit("AUTHORITY")];
    prim_lit_ = qi::no_case[qi::lit("PRIMEM")];
    unit_lit_= qi::no_case[qi::lit("UNIT")];
    axis_lit_ = qi::no_case[qi::lit("AXIS")];
    proj_lit_ = qi::no_case[qi::lit("PROJECTION")];
    proj_pram_lit_ = qi::no_case[qi::lit("PARAMETER")];
    proj_rs_lit_ = qi::no_case[qi::lit("PROJCS")];

    q_str_ = '"' >> *~qi::char_('"') >> '"';
    q_obstr_ = q_str_[
      // semantic action used to convert std::string to obstring
      boost::phoenix::bind([&](ObString &ob_str, ObQiString &str) {
        int ret = OB_SUCCESS;
        uint64_t str_len = str.val_.length();
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator.alloc(str_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          _pass = false; // force parser failure
          LOG_WARN("failed to allocate memory during parsing srs definition", K(ret));
        } else {
          char *ptr = static_cast<char *>(buf);
          MEMCPY(ptr, str.val_.ptr(), str_len);
          ob_str = ObString(str_len, ptr);
        }
      }, qi::_val, qi::_1)
    ];

    // must be lower case to be insensitive.
    direct_symbol_.add("east", ObAxisDirection::EAST)("south", ObAxisDirection::SOUTH)(
                       "west", ObAxisDirection::WEST)("north", ObAxisDirection::NORTH)(
                       "other", ObAxisDirection::OTHER);
    direct_ = qi::no_case[direct_symbol_];

    auth_ = auth_lit_ >> qi::attr(true) >> l_brac_ >>
            q_obstr_ >> comma_ >>
            q_obstr_ >> r_brac_;

    towgs_ = towgs_lit_ >> qi::attr(true) >> l_brac_ >>
             double_ >> comma_ >>
             double_ >> comma_ >>
             double_ >> comma_ >>
             double_ >> comma_ >>
             double_ >> comma_ >>
             double_ >> comma_ >>
             double_ >> r_brac_;

    spher_ = spher_lit_ >> l_brac_ >> q_obstr_ >>
             comma_ >> double_ >>
             comma_ >> double_ >>
             -(comma_ >> auth_) >> r_brac;

    prim_ = prim_lit_ >> l_brac_ >>
            q_obstr_ >> comma_ >>
            double_ >> -(comma_ >> auth_) >> r_brac_;

    datum_ = datum_lit_ >> l_brac_ >>
             q_obstr_ >> comma_ >>
             spher_ >> -(comma_ >> towgs_) >>
             -(comma_ >> auth_) >> r_brac;

    unit_ = unit_lit_ >> l_brac_ >>
            q_obstr_ >> comma_ >>
            double_ >> -(comma_ >> auth_) >> r_brac;

    axis_ = axis_lit_ >> l_brac_ >>
            q_obstr_ >> comma_ >>
            direct_ >> r_brac_;

    axis_pair_ = axis_ >> comma_ >> axis_;

    geog_rs_ = geog_lit_ >> l_brac_ >>
               q_obstr_ >> comma_ >>
               datum_ >> comma_ >>
               prim_ >> comma_ >>
               unit_ >> comma_ >>
               axis_pair_ >> -(comma_ >> auth_) >> r_brac_;

    proj_ = proj_lit_ >> l_brac_ >>
            q_obstr_ >> -(comma_ >> auth_) >> r_brac_;

    proj_param_ = proj_pram_lit_ >> l_brac_ >>
                  q_obstr_ >> comma_ >>
                  double_ >> -(comma_ >> auth_) >> r_brac_;

    proj_params_ = proj_param_ % comma_;

    proj_rs_ = proj_rs_lit_ >> l_brac >>
               q_obstr_ >> comma_ >>
               geog_rs_ >> comma_ >>
               proj_ >> -(comma_ >> proj_params_) >> comma_ >>
               unit_ >> -(comma_ >> axis_pair_) >>
               -(comma_ >> auth_) >> r_brac_;

    start_ = proj_rs_ | geog_rs_;
  }

  // rules declaration
  qi::rule<Iterator> l_brac_;
  qi::rule<Iterator> r_brac_;
  qi::rule<Iterator> comma_;
  qi::rule<Iterator> geog_lit_;
  qi::rule<Iterator> datum_lit_;
  qi::rule<Iterator> spher_lit_;
  qi::rule<Iterator> auth_lit_;
  qi::rule<Iterator> prim_lit_;
  qi::rule<Iterator> unit_lit_;
  qi::rule<Iterator> axis_lit_;
  qi::rule<Iterator> towgs_lit_;
  qi::rule<Iterator> proj_lit_;
  qi::rule<Iterator> proj_pram_lit_;
  qi::rule<Iterator> proj_rs_lit_;
  qi::rule<Iterator, ObQiString()> q_str_;
  qi::rule<Iterator, ObString()> q_obstr_;
  qi::symbols<char,  ObAxisDirection> direct_symbol_;
  qi::rule<Iterator, ObAxisDirection> direct_;

  qi::rule<Iterator, ObRsAuthority(), Skipper> auth_;
  qi::rule<Iterator, ObTowgs84(), Skipper> towgs_;
  qi::rule<Iterator, ObRsDatum(), Skipper> datum_;
  qi::rule<Iterator, ObPrimem(), Skipper> prim_;
  qi::rule<Iterator, ObRsUnit(), Skipper> unit_;
  qi::rule<Iterator, ObRsAxis(), Skipper> axis_;
  qi::rule<Iterator, ObRsAxisPair(), Skipper> axis_pair_;
  qi::rule<Iterator, ObSpheroid(), Skipper> spher_;
  qi::rule<Iterator, ObProjection(), Skipper> proj_;
  qi::rule<Iterator, ObProjectionPram(), Skipper> proj_param_;
  qi::rule<Iterator, ObProjectionPrams(), Skipper> proj_params_;

  qi::rule<Iterator, ObProjectionRs(), Skipper> proj_rs_;
  qi::rule<Iterator, ObGeographicRs(), Skipper> geog_rs_;
  qi::rule<Iterator, ObGeoRs(), Skipper> start_;
};

// cannnot make this function a member of ObSrsWktParser cause "boost/variant/variant.hpp" conflict with ob macro
static int parse_coordinate_system(common::ObIAllocator &allocator, const common::ObString &srs_str, ObGeoRs &rs)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_alloc;
  int last = srs_str.length() - 1;
  while (isspace(srs_str[last])) {
    last--;
  }
  char r_brac = srs_str[last];
  if (r_brac != ')' && r_brac != ']') {
    ret = OB_ERR_PARSER_SYNTAX;
  } else {
    char l_brac = r_brac == ')' ? '(' : '[';
    const char *begin = srs_str.ptr();
    const char *end = begin + last + 1; // past the last
    void *buf = tmp_alloc.alloc(sizeof(SrsWktGrammar<decltype(begin), boost::spirit::ascii::space_type>));
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc SrsWktGrammar failed", K(ret));
    } else {
      SrsWktGrammar<decltype(begin), boost::spirit::ascii::space_type> *parser =
        new (buf) SrsWktGrammar<decltype(begin), boost::spirit::ascii::space_type>(l_brac, r_brac, allocator);
      bool bret = qi::phrase_parse(begin, end, *parser, boost::spirit::ascii::space, rs);
      if (!bret) {
        ret = OB_ERR_PARSER_SYNTAX; // todo@dazhi: ER_SRS_PARSE_ERROR
        LOG_WARN("failed to parse coodinate system, the srs definition maybe wrong", K(ret), K(bret));
      } else if (begin != end) {
        ret = OB_ERR_PARSER_SYNTAX; // todo@dazhi: ER_SRS_PARSE_ERROR
        ObString trailing_str(end - begin, begin);
        LOG_WARN("failed to parse coodinate sytem, has extra trailing characters", K(trailing_str));
      }
      if (OB_NOT_NULL(parser)) {
        parser->~SrsWktGrammar<decltype(begin), boost::spirit::ascii::space_type>();
      }
    }
  }
  return ret;
}

int ObSrsWktParser::parse_srs_wkt(common::ObIAllocator &allocator, uint64_t srid,
                                  const common::ObString &srs_str,
                                  ObSpatialReferenceSystemBase *&srs) {
  int ret = OB_SUCCESS;
  ObGeoRs geo_rs;
  ObSpatialReferenceSystemBase *tmp_result;
  ObGeographicRs *geog_rs = NULL;
  ObProjectionRs *proj_rs = NULL;

  if (srs_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srs string is empty", K(ret), K(srid));
  } else if (OB_FAIL(parse_coordinate_system(allocator, srs_str, geo_rs))) {
    LOG_WARN("failed to parse srs wkt", K(ret));
  } else if (OB_NOT_NULL(geog_rs = boost::get<ObGeographicRs>(&geo_rs))) {
    if (OB_FAIL(ObSpatialReferenceSystemBase::create_geographic_srs(&allocator, srid, geog_rs, tmp_result))) {
      LOG_WARN("failed to create geographic srs from parsed coordinate system", K(ret));
    } else {
      srs = tmp_result;
    }
  } else if (OB_NOT_NULL(proj_rs = boost::get<ObProjectionRs>(&geo_rs))) {
    if (OB_FAIL(ObSpatialReferenceSystemBase::create_project_srs(&allocator, srid, proj_rs, tmp_result))) {
      LOG_WARN("failed to create geographic srs from parsed coordinate system", K(ret));
    } else {
      srs = tmp_result;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error when parse srs wkt", K(ret));
  }

  return ret;
}

int ObSrsWktParser::parse_geog_srs_wkt(common::ObIAllocator& allocator, const common::ObString &srs_str, ObGeographicRs &result) {
  int ret = OB_SUCCESS;
  ObGeoRs geo_rs;
  ObGeographicRs *geog_rs;
  if (OB_FAIL(parse_coordinate_system(allocator, srs_str, geo_rs))) {
    LOG_WARN("failed to parse srs wkt", K(ret));
  } else if (OB_ISNULL(geog_rs = boost::get<ObGeographicRs>(&geo_rs))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error when parse srs wkt", K(ret));
  } else {
    result = *geog_rs;
  }
  return ret;
}

int ObSrsWktParser::parse_proj_srs_wkt(common::ObIAllocator& allocator, const common::ObString &srs_str, ObProjectionRs &result) {
  int ret = OB_SUCCESS;
  ObGeoRs geo_rs;
  ObProjectionRs *proj_rs;
  if (OB_FAIL(parse_coordinate_system(allocator, srs_str, geo_rs))) {
    LOG_WARN("failed to parse srs wkt", K(ret));
  } else if (OB_ISNULL(proj_rs = boost::get<ObProjectionRs>(&geo_rs))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error when parse srs wkt", K(ret));
  } else {
    result = *proj_rs;
  }
  return ret;
}

} // common
} // oceanbase