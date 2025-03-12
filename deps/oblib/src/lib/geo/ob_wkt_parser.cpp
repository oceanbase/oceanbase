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
 * This file contains implementation support for the WKT Parser abstraction.
 */

#define USING_LOG_PREFIX LIB
#include "ob_wkt_parser.h"
#include "lib/function/ob_function.h"
#include "common/ob_smart_call.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{

int ObWktParser::check_next_token(ObWktTokenType tkn_type)
{
  int ret = OB_SUCCESS;
  ObWktTokenVal tkn_val;
  if (OB_FAIL(check_next_token_with_val(tkn_type, tkn_val))) {
    LOG_WARN("fail to check next token", K(ret), K(tkn_type));
  }
  return ret;
}

int ObWktParser::check_next_token_with_val(ObWktTokenType tkn_type, ObWktTokenVal &tkn_val)
{
  int ret = OB_SUCCESS;
  skip_left_space();
  if (is_wkt_end()) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("fail to get next token, have reached the end of wkt", K(ret));
  } else {
    char ch = wkt_[cur_pos_];
    switch(tkn_type) {
      case ObWktTokenType::W_LEFT_B: {
        if ('(' == ch) {
          cur_pos_++;
        } else {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to check next token W_LEFT_B", K(ret), K(ch), K(cur_pos_));
        }
        break;
      }
      case ObWktTokenType::W_RIGHT_B: {
        if (')' == ch) {
          cur_pos_++;
        } else {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to check next token W_RIGHT_B", K(ret), K(ch), K(cur_pos_));
        }
        break;
      }
      case ObWktTokenType::W_COMMA: {
        if (',' == ch) {
          cur_pos_++;
        } else {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to check next token W_COMMA", K(ret), K(ch), K(cur_pos_));
        }
        break;
      }
      case ObWktTokenType::W_NUMBER: {
        if (!is_number_beginning(ch)) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to check next W_NUMBER, wkt should begin with number", K(ret), K(cur_pos_));
        } else if (OB_FAIL(process_number(tkn_val))) {
          LOG_WARN("fail to process number", K(ret));
        }
        break;
      }
      case ObWktTokenType::W_EMPTY:
      case ObWktTokenType::W_WORD: {
        if (!is_word_beginning(ch)) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to check next W_WORD, wkt should begin with word", K(ret), K(cur_pos_));
        } else if (OB_FAIL(process_word(tkn_val))) {
          LOG_WARN("fail to process word", K(ret));
        } else if (ObWktTokenType::W_EMPTY == tkn_type && tkn_val.string_val_.case_compare("empty")){
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to check next empty token", K(ret), K(tkn_val.string_val_));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check next invalid ObWktTokenType", K(ret), K(tkn_type));
        break;
      }
    }
  }
  return ret;
}

int ObWktParser::check_next_token_with_val_keep_pos(ObWktTokenType tkn_type, ObWktTokenVal &tkn_val)
{
  int ret = OB_SUCCESS;
  uint64_t pos = cur_pos_;
  if (OB_FAIL(check_next_token_with_val(tkn_type, tkn_val))) {
    LOG_WARN("fail to check next token", K(ret), K(tkn_type));
  } else {
    cur_pos_ = pos;
  }
  return ret;
}

int ObWktParser::get_next_token(ObWktTokenType &tkn_type, ObWktTokenVal &tkn_val)
{
  int ret = OB_SUCCESS;
  skip_left_space();
  if (is_wkt_end()) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("fail to get next token, have reached the end of wkt", K(ret));
  } else {
    char ch = wkt_[cur_pos_];
    if ('(' == ch) {
      tkn_type = ObWktTokenType::W_LEFT_B;
      tkn_val.string_val_.assign_ptr(&wkt_[cur_pos_++], 1);
    } else if (')' == ch) {
      tkn_type = ObWktTokenType::W_RIGHT_B;
      tkn_val.string_val_.assign_ptr(&wkt_[cur_pos_++], 1);
    } else if (',' == ch) {
      tkn_type = ObWktTokenType::W_COMMA;
      tkn_val.string_val_.assign_ptr(&wkt_[cur_pos_++], 1);
    } else if (is_number_beginning(ch)) {
      if (OB_FAIL(process_number(tkn_val))) {
        LOG_WARN("fail to process NUMBER token", K(ret));
      } else {
        tkn_type = ObWktTokenType::W_NUMBER;
      }
    } else if (is_word_beginning(ch)) {
      if (OB_FAIL(process_word(tkn_val))) {
        LOG_WARN("fail to process WORD token", K(ret));
      } else {
        if (tkn_val.string_val_.case_compare("empty")) {
          tkn_type = ObWktTokenType::W_WORD;
        } else {
          tkn_type = ObWktTokenType::W_EMPTY;
        }
      }
    } else {
      tkn_type = ObWktTokenType::W_INVALID;
      tkn_val.string_val_.assign_ptr(&wkt_[cur_pos_++], 1);
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("fail to get next token from wkt", K(ret), K(tkn_val.string_val_));
    }
  }
  return ret;
}

int ObWktParser::get_next_token_keep_pos(ObWktTokenType &tkn_type, ObWktTokenVal &tkn_val)
{
  int ret = OB_SUCCESS;
  uint64_t pos = cur_pos_;
  if (OB_FAIL(get_next_token(tkn_type, tkn_val))) {
    LOG_WARN("fail to get next token", K(ret));
  }
  cur_pos_ = pos;
  return ret;
}

void ObWktParser::skip_left_space()
{
  while(cur_pos_ < wkt_len_ && isspace(wkt_[cur_pos_])) {
    cur_pos_++;
  }
}

bool ObWktParser::is_wkt_end()
{
  skip_left_space();
  int bret = false;
  if (cur_pos_ >= wkt_len_) {
    bret = true;
  }
  return bret;
}

bool ObWktParser::is_number_beginning(char ch)
{
  int bret = false;
  if ('-' == ch || '.' == ch || isdigit(ch)) {
    bret = true;
  } else if (!is_oracle_mode_ && '+' == ch ) {
    bret = true;
  }
  return bret;
}

bool ObWktParser::is_word_beginning(char ch)
{
  return isalpha(ch) != 0;
}

bool ObWktParser::is_two_brac_beginning()
{
  int bret = false;
  uint64_t tmp_pos = cur_pos_;
  skip_left_space();
  if (cur_pos_ < wkt_len_ && '(' == wkt_[cur_pos_]) {
    cur_pos_++;
    skip_left_space();
    if (cur_pos_ < wkt_len_ && '(' == wkt_[cur_pos_]) {
      bret = true;
    }
  }
  cur_pos_ = tmp_pos;
  return bret;
}

int ObWktParser::process_number(ObWktTokenVal &tkn_val)
{
  int ret = OB_SUCCESS;
  char *endptr = NULL;
  int err = 0;
  double val = 0;
  val = ObCharset::strntod(wkt_+cur_pos_, wkt_len_-cur_pos_, &endptr, &err);
  if (err) {
    if (EOVERFLOW == err && (-DBL_MAX == val || DBL_MAX == val)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("fail to cast string to double, cause data is out of range", K(ret), K(cur_pos_), K(err));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error when cast string to double", K(ret), K(cur_pos_), K(err));
    }
  } else if (OB_NOT_NULL(endptr)) {
    tkn_val.number_val_ = val;
    cur_pos_ = endptr - wkt_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error when cast string to double", K(ret), K(cur_pos_), K(val));
  }

  return ret;
}

int ObWktParser::process_word(ObWktTokenVal &tkn_val)
{
  int ret = OB_SUCCESS;
  uint64_t tmp_pos = cur_pos_;
  while(tmp_pos < wkt_len_ && isalpha(wkt_[tmp_pos])) {
    tmp_pos++;
  }
  tkn_val.string_val_.assign_ptr(wkt_+cur_pos_, tmp_pos-cur_pos_);
  cur_pos_ = tmp_pos;
  return ret;
}

// with obj created
int ObWktParser::parse(ObGeometry *&geo, bool is_geographical)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_parse())){
      LOG_WARN("fail to do inner parse for wkt", K(ret));
  } else if (!is_wkt_end()) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("wkt has extra character after parse", K(ret), K(cur_pos_));
  }

  if (OB_SUCC(ret)) {
    // new geo and attach wkb buffer
    uint32_t geo_type = 0;
    if (OB_FAIL(wkb_buf_.read(sizeof(uint8_t), geo_type))) {
      LOG_WARN("fail to get geo type from wkb buff", K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator_, static_cast<ObGeoType>(geo_type), is_geographical, true, geo))) {
      LOG_WARN("fail to create geometry given type", K(ret), K(geo_type));
    } else {
      geo->set_data(wkb_buf_.string());
    }
  }

  return ret;
}

int ObWktParser::parse_geo_type(ObGeoType &geo_type)
{
  int ret = OB_SUCCESS;
  skip_left_space();
  ObWktTokenVal tkn_val_1;

  if (OB_FAIL(check_next_token_with_val(ObWktTokenType::W_WORD, tkn_val_1))) {
    LOG_WARN("fail to parse geometry type from wkt", K(ret));
  } else if (0 == tkn_val_1.string_val_.case_compare("geomcollection")) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("wkt has extra character after parse", K(ret), K(cur_pos_));
  } else {
    geo_type = ObGeoTypeUtil::get_geo_type_by_name(tkn_val_1.string_val_);
  }

  if (OB_FAIL(ret)) {
  } else if (ObGeoTypeUtil::is_3d_geo_type(geo_type)) {
    // 3d type
    if (OB_FAIL(set_dimension(ObGeoDimType::IS_3D))) {
      LOG_WARN("fail to set dimension type", K(ret));
    }
  } else {
    ObWktTokenType tkn_type_2;
    ObWktTokenVal tkn_val_2;
    if (OB_FAIL(get_next_token_keep_pos(tkn_type_2, tkn_val_2))) {
      LOG_WARN("fail to parse next token", K(ret));
    } else if (tkn_type_2 == ObWktTokenType::W_WORD && 0 == tkn_val_2.string_val_.case_compare("z")) {
      // case like 'point z(1 1 1)'
      if (OB_FAIL(set_dimension(ObGeoDimType::IS_3D))) {
        LOG_WARN("fail to set dimension type", K(ret));
      } else {
        geo_type = static_cast<ObGeoType>(static_cast<uint32_t>(geo_type) + 1000);
        if (OB_FAIL(check_next_token(ObWktTokenType::W_WORD))) {  // move the cur_pos_ to next token
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected token type", K(ret));
        }
      }
    }
  }

  if (is_oracle_mode_ && ObGeoTypeUtil::is_3d_geo_type(geo_type) && OB_SUCC(ret)) {
    // wkt + z is not supported in oracle mode
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected token type", K(ret));
  }

  return ret;
}

// [bo][geo_type][binary]
int ObWktParser::inner_parse()
{
  int ret = OB_SUCCESS;
  skip_left_space();
  ObWktTokenVal tkn_val;
  ObGeoType geo_type = ObGeoType::GEOTYPEMAX;
  if (OB_FAIL(parse_geo_type(geo_type))) {
    LOG_WARN("fail to parse geo type", K(ret));
  } else if (OB_FAIL(wkb_buf_.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
    LOG_WARN("fail to append byteorder to wkb buffer", K(ret));
  } else if (OB_FAIL(wkb_buf_.append(static_cast<uint32_t>(geo_type)))) {
    LOG_WARN("fail to append geo_type to wkb buffer", K(ret));
  } else {
    uint64_t pos = wkb_buf_.length() - sizeof(uint32_t);
    switch(geo_type) {
      case ObGeoType::POINT:
      case ObGeoType::POINTZ: {
        if (OB_FAIL(parse_point())) {
          LOG_WARN("fail to parse point wkt", K(ret));
        }
        break;
      }
      case ObGeoType::LINESTRING:
      case ObGeoType::LINESTRINGZ: {
        if (OB_FAIL(parse_linestring())) {
          LOG_WARN("fail to parse linestring wkt", K(ret));
        }
        break;
      }
      case ObGeoType::POLYGON:
      case ObGeoType::POLYGONZ: {
        if (OB_FAIL(parse_polygon())) {
          LOG_WARN("fail to parse polygon wkt", K(ret));
        }
        break;
      }
      case ObGeoType::MULTIPOINT:
      case ObGeoType::MULTIPOINTZ: {
        if (OB_FAIL(parse_multipoint())) {
          LOG_WARN("fail to parse multipoint wkt", K(ret));
        }
        break;
      }
      case ObGeoType::MULTILINESTRING:
      case ObGeoType::MULTILINESTRINGZ: {
        if (OB_FAIL(parse_mutilinestring())) {
          LOG_WARN("fail to parse multilinestring wkt", K(ret));
        }
        break;
      }
      case ObGeoType::MULTIPOLYGON:
      case ObGeoType::MULTIPOLYGONZ: {
        if (OB_FAIL(parse_multipolygen())) {
          LOG_WARN("fail to parse multipolygen wkt", K(ret));
        }
        break;
      }
      case ObGeoType::GEOMETRYCOLLECTION:
      case ObGeoType::GEOMETRYCOLLECTIONZ: {
        if (OB_FAIL(parse_geometrycollectioin())) {
          LOG_WARN("fail to parse geometrycollection wkt", K(ret));
        }
        break;
      }
      default: {
        // not reach here
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid wkt geo type", K(ret), K(geo_type), K(tkn_val.string_val_));
      break;
      }
    } // end switch
    if (OB_SUCC(ret) && OB_FAIL(refresh_type(pos))) {
      LOG_WARN("fail to refresh type", K(ret));
    }
  }

  return ret;
}

int ObWktParser::refresh_type(uint64_t pos)
{
  int ret = OB_SUCCESS;
  const char *ptr = wkb_buf_.ptr();
  uint32_t type = static_cast<uint32_t>(ObGeoType::GEO3DTYPEMAX);
  if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to refresh type",K(ret));
  } else if (OB_FAIL(wkb_buf_.read(pos, type))) {
    LOG_WARN("fail to read type", K(ret));
  } else if (dim_type_ == ObGeoDimType::IS_3D &&
            type <= 7 &&
            OB_FAIL(wkb_buf_.write(pos, type + 1000))) {
    LOG_WARN("fail to refresh type", K(ret), K(type));
  }
  return ret;
}

// encode wkt without bo and geo_type
int ObWktParser::parse_point(bool with_brackets)
{
  int ret = OB_SUCCESS;
  ObWktTokenVal x_val;
  ObWktTokenVal y_val;
  ObWktTokenVal z_val;
  if (with_brackets && OB_FAIL(check_next_token(ObWktTokenType::W_LEFT_B))) {
    LOG_WARN("fail to parse point, check next LEFT_B", K(ret));
  } else if (OB_FAIL(check_next_token_with_val(ObWktTokenType::W_NUMBER, x_val))) {
    LOG_WARN("fail to parse point, check next NUMBER", K(ret));
  } else if (OB_FAIL(check_next_token_with_val(ObWktTokenType::W_NUMBER, y_val))) {
    LOG_WARN("fail to parse point, check next NUMBER", K(ret));
  } else if (OB_FAIL(try_parse_zdim_token(z_val))) {
    LOG_WARN("fail to try parse z dim value", K(ret));
  } else if (with_brackets && OB_FAIL(check_next_token(ObWktTokenType::W_RIGHT_B))) {
    LOG_WARN("fail to parse point, check next RIGHT_B", K(ret));
  } else if (OB_FAIL(wkb_buf_.append(x_val.number_val_))) {
    LOG_WARN("fail to append x_val to point", K(ret));
  } else if (OB_FAIL(wkb_buf_.append(y_val.number_val_))) {
    LOG_WARN("fail to append y_val to point", K(ret));
  } else if (dim_type_ == ObGeoDimType::IS_3D && OB_FAIL(wkb_buf_.append(z_val.number_val_))) {
    LOG_WARN("fail to append z_val to point", K(dim_type_), K(ret));
  }
  return ret;
}

int ObWktParser::try_parse_zdim_token(ObWktTokenVal &z_val)
{
  int ret = OB_SUCCESS;
  ObGeoDimType tmp_dim_type = ObGeoDimType::NOT_INIT;
  ObWktTokenType tk_type;
  if (OB_FAIL(get_next_token_keep_pos(tk_type, z_val))) {
    LOG_WARN("fail to get next token", K(ret));
  } else if (tk_type == ObWktTokenType::W_NUMBER) {
    tmp_dim_type = ObGeoDimType::IS_3D;
  } else if (tk_type == ObWktTokenType::W_RIGHT_B || tk_type == ObWktTokenType::W_COMMA) {
    tmp_dim_type = ObGeoDimType::IS_2D;
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("fail to parse point type", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_dimension(tmp_dim_type))) {
      LOG_WARN("fail to set dimension type", K(ret));
    }

    if (OB_SUCC(ret)) { // if dim is 3，move the ptr to next token
      if (dim_type_ == ObGeoDimType::IS_3D && OB_FAIL(check_next_token(ObWktTokenType::W_NUMBER))) {
        LOG_WARN("fail to move to the next token", K(ret));
      }
    }
  }
  return ret;
}

// encode wkt without bo and geo_type
int ObWktParser::parse_linestring(bool is_ring)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_next_token(ObWktTokenType::W_LEFT_B))) {
    LOG_WARN("fail to check next token LEFT_B", K(ret));
  } else {
    ObWktTokenType tkn_type;
    ObWktTokenVal tkn_val;
    bool has_more_geo = false;
    uint32_t num_points = 0;
    int pos = wkb_buf_.length(); // backfill num_points
    if (OB_FAIL(wkb_buf_.append_zero(sizeof(uint32_t)))) {
      LOG_WARN("fail to move back wkb buffer", K(ret));
    } else {
      do {
        if (OB_FAIL(parse_point(false))) {
          LOG_WARN("fail to parse point in linestring", K(ret));
        } else if (OB_FAIL(get_next_token(tkn_type, tkn_val))) {
          LOG_WARN("fail to parse point", K(ret));
        } else {
          num_points++;
          if (ObWktTokenType::W_COMMA == tkn_type) {
            has_more_geo = true;
          } else if (ObWktTokenType::W_RIGHT_B == tkn_type){
            has_more_geo = false;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
          }
        }
      } while(has_more_geo && OB_SUCC(ret));

      if (OB_SUCC(ret)) {
        if (is_ring) {
          if (num_points < 4) {
            ret = OB_ERR_PARSER_SYNTAX;
          } else {
            uint8_t dim = dim_type_ == ObGeoDimType::IS_3D ? 3 : 2;
            // 3D ring is legal as long as the X/Y axes are equal
            bool not_same_point = MEMCMP(wkb_buf_.ptr() + pos + sizeof(uint32_t),
              wkb_buf_.ptr() + wkb_buf_.length() - dim * sizeof(double), 2 * sizeof(double));

            if (not_same_point && !lib::is_oracle_mode()) {
              ret = OB_ERR_PARSER_SYNTAX;
              LOG_WARN("first point and last point have to be the same in a ring", K(ret));
            }
          }
        } else if (num_points < 2) {
          ret = OB_ERR_PARSER_SYNTAX;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(wkb_buf_.write(pos, num_points))) {
        LOG_WARN("fail to backfill num points for linestring", K(ret));
      }
    }
  }

  return ret;
}

// encode wkt without bo and geo_type
int ObWktParser::parse_polygon()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_next_token(ObWktTokenType::W_LEFT_B))) {
    LOG_WARN("fail to check next token LEFT_B", K(ret));
  } else {
    ObWktTokenType tkn_type;
    ObWktTokenVal tkn_val;
    bool has_more_geo = false;
    uint32_t num_lines = 0;
    int pos = wkb_buf_.length(); // backfill num_lines
    if (OB_FAIL(wkb_buf_.append_zero(sizeof(uint32_t)))) {
      LOG_WARN("fail to move back wkb buffer", K(ret));
    } else {
      do {
        if (OB_FAIL(parse_linestring(true))) {
          LOG_WARN("fail to parse linestring in polygon", K(ret));
        } else if (OB_FAIL(get_next_token(tkn_type, tkn_val))) {
          LOG_WARN("fail to parse point", K(ret));
        } else {
          num_lines++;
          if (ObWktTokenType::W_COMMA == tkn_type) {
            has_more_geo = true;
          } else if (ObWktTokenType::W_RIGHT_B == tkn_type){
            has_more_geo = false;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
          }
        }
      } while(has_more_geo && OB_SUCC(ret));

      if (OB_SUCC(ret) && OB_FAIL(wkb_buf_.write(pos, num_lines))) {
        LOG_WARN("fail to backfill num lines for polygon", K(ret));
      }
    }
  }

  return ret;
}

// common routine for parse_multixxx
int ObWktParser::parse_multi_geom(ObGeoType geo_type, bool brackets)
{
  int ret = OB_SUCCESS;
  ObFunction<int(void)> parse_func;
  if (ObGeoType::POINT == geo_type) {
    parse_func = std::bind(&ObWktParser::parse_point, this, brackets);
  } else if (ObGeoType::LINESTRING == geo_type) {
    parse_func = std::bind(&ObWktParser::parse_linestring, this, false);
  } else if (ObGeoType::POLYGON == geo_type) {
    parse_func = std::bind(&ObWktParser::parse_polygon, this);
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_next_token(ObWktTokenType::W_LEFT_B))) {
      LOG_WARN("fail to check next token LEFT_B", K(ret));
    } else {
      ObWktTokenType tkn_type;
      ObWktTokenVal tkn_val;
      bool has_more_geo = false;
      uint32_t num_geo = 0;
      int pos = wkb_buf_.length(); // backfill num_lines
      if (OB_FAIL(wkb_buf_.append_zero(sizeof(uint32_t)))) {
        LOG_WARN("fail to move back wkb buffer", K(ret));
      } else {
        do {
          uint64_t type_pos = wkb_buf_.length() + sizeof(char);
          // TODO: point without brackets
          if (OB_FAIL(wkb_buf_.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
            LOG_WARN("fail to add bo to xxx inside multixxx", K(ret), K(geo_type));
          } else if (OB_FAIL(wkb_buf_.append(static_cast<uint32_t>(geo_type)))) {
            LOG_WARN("fail to add type to xxx inside multixxx", K(ret), K(geo_type));
          } else if (OB_FAIL(parse_func())) {
            LOG_WARN("fail to parse xxx in multixxx", K(ret), K(geo_type));
          } else if (OB_FAIL(get_next_token(tkn_type, tkn_val))) {
            LOG_WARN("fail to parse xxx", K(ret), K(geo_type));
          } else {
            num_geo++;
            if (ObWktTokenType::W_COMMA == tkn_type) {
              has_more_geo = true;
            } else if (ObWktTokenType::W_RIGHT_B == tkn_type){
              has_more_geo = false;
            } else {
              ret = OB_ERR_PARSER_SYNTAX;
            }
            if (OB_SUCC(ret) && OB_FAIL(refresh_type(type_pos))) { // refresh type
              LOG_WARN("fail to refresh geo type", K(ret));
            }
          }
        } while(has_more_geo && OB_SUCC(ret));

        if (OB_SUCC(ret) && OB_FAIL(wkb_buf_.write(pos, num_geo))) {
          LOG_WARN("fail to backfill num lines for polygon", K(ret));
        }
      }
    }
  }

  return ret;

}
int ObWktParser::parse_multipoint()
{
  int ret = OB_SUCCESS;
  bool two_brac_beg = is_two_brac_beginning();
  if (is_oracle_mode_) {
    if (!two_brac_beg) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("Oracle's Multipoint objects require brac around each point", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = parse_multi_geom(ObGeoType::POINT, two_brac_beg);
  }
  return ret;
}

int ObWktParser::parse_mutilinestring()
{
  return parse_multi_geom(ObGeoType::LINESTRING);
}

int ObWktParser::parse_multipolygen()
{
  return parse_multi_geom(ObGeoType::POLYGON);
}

int ObWktParser::parse_geometrycollectioin()
{
  int ret = OB_SUCCESS;
  ObWktTokenType tkn_type;
  ObWktTokenVal tkn_val;
  uint32_t num_geos = 0;
  int pos = wkb_buf_.length(); // backfill num_geos
  // in case of empty, we need move it first
  if (OB_FAIL(wkb_buf_.append_zero(sizeof(uint32_t)))) {
    LOG_WARN("fail to move back wkb buffer", K(ret));
  } else {
    if (OB_FAIL(get_next_token(tkn_type, tkn_val))) {
      LOG_WARN("fail to get next token", K(ret));
    } else if (ObWktTokenType::W_EMPTY == tkn_type) {
    } else if (ObWktTokenType::W_LEFT_B == tkn_type) {
      if (OB_FAIL(get_next_token_keep_pos(tkn_type, tkn_val))) {
        LOG_WARN("fail to get next without skip", K(ret));
      } else if (ObWktTokenType::W_RIGHT_B == tkn_type) {
        if (OB_FAIL(check_next_token(ObWktTokenType::W_RIGHT_B))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected token type", K(ret));
        }
      } else {
        bool has_more_geo = false;
          do {
            if (OB_FAIL(SMART_CALL(inner_parse()))) {
              LOG_WARN("fail to parse geom object inside geomtrycollection", K(ret));
            } else if (OB_FAIL(get_next_token(tkn_type, tkn_val))) {
              LOG_WARN("fail to get next token", K(ret));
            } else {
              num_geos++;
              if (ObWktTokenType::W_COMMA == tkn_type) {
                has_more_geo = true;
              } else if (ObWktTokenType::W_RIGHT_B == tkn_type){
                has_more_geo = false;
              } else {
                ret = OB_ERR_PARSER_SYNTAX;
              }
            }
          } while(has_more_geo && OB_SUCC(ret));
        }
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("the first token of geometrycolltion must be EMPTY or LEFT_B", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(wkb_buf_.write(pos, num_geos))) {
    LOG_WARN("fail to backfill num geos for geometrycolltion", K(ret));
  }
  return ret;
}

int ObWktParser::parse_wkt(ObIAllocator &allocator, const ObString &wkt, ObGeometry *&geo, bool to_wkb, bool is_geographical)
{
  int ret = OB_SUCCESS;
  if (!to_wkb) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("fail to parse wkt, only to_wkb is supported", K(ret), K(to_wkb));
  } else {
    ObWktParser parser(allocator, wkt);
    if (OB_FAIL(parser.parse(geo, is_geographical))) {
      LOG_WARN("fail to parse wkt", K(ret), K(wkt));
    }
  }
  return ret;
}

int ObWktParser::set_dimension(ObGeoDimType dim)
{
  int ret = OB_SUCCESS;
  if (dim_type_ == ObGeoDimType::NOT_INIT) {
    dim_type_ = dim;
  } else if (dim_type_ != dim) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("dimensions mismatch in geometry", K(ret), K(dim_type_), K(dim));
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase