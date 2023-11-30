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

#ifndef OCEANBASE_LIB_GEO_OB_WKT_PARSER_
#define OCEANBASE_LIB_GEO_OB_WKT_PARSER_

#include "lib/utility/utility.h"
#include "lib/geo/ob_geo.h"

namespace oceanbase
{
namespace common
{

class ObWktParser
{
public:
  // The only function you should use for parsing wkt
  static int parse_wkt(ObIAllocator &allocator, const ObString &wkt, ObGeometry *&geo, bool to_wkb, bool is_geographical);

private:
  enum class ObWktTokenType
  {
    W_LEFT_B, // 0
    W_RIGHT_B,
    W_WORD,
    W_NUMBER,
    W_COMMA,
    W_INVALID, // 5
    W_EMPTY
  };

  enum ObGeoDimType: uint8_t
  {
    NOT_INIT,
    IS_2D,
    IS_3D
  };

  typedef union ObWktTokenVal {
    ObString string_val_;
    double number_val_;
    ObWktTokenVal(): string_val_() {}
  } ObWktTokenVal;

  explicit ObWktParser (ObIAllocator &allocator, const ObString &wkt) :
    allocator_(allocator), wkt_(wkt.ptr()), wkb_buf_(allocator), cur_pos_(0), wkt_len_(wkt.length()),
    dim_type_(ObGeoDimType::NOT_INIT), is_oracle_mode_(lib::is_oracle_mode()) {}
  ~ObWktParser(){};

  int parse(ObGeometry *&geo, bool is_geographical);
  int inner_parse();
  int parse_point(bool with_brackets = true);
  int parse_linestring(bool is_ring = false);
  int parse_polygon();
  int parse_multipoint();
  int parse_mutilinestring();
  int parse_multipolygen();
  int parse_geometrycollectioin();
  int parse_multi_geom(ObGeoType geo_type, bool brackets = false);

  int check_next_token(ObWktTokenType tkn_type);
  int check_next_token_keep_pos(ObWktTokenType tkn_type);
  int check_next_token_with_val(ObWktTokenType tkn, ObWktTokenVal &tkn_val);
  int check_next_token_with_val_keep_pos(ObWktTokenType tkn_type, ObWktTokenVal &tkn_val);
  int get_next_token(ObWktTokenType &tkn_type, ObWktTokenVal &tkn_val);
  int get_next_token_keep_pos(ObWktTokenType &tkn_type, ObWktTokenVal &tkn_val);
  void skip_left_space();
  bool is_wkt_end();
  bool is_number_beginning(char ch);
  bool is_word_beginning(char ch);
  // for multipoint
  bool is_two_brac_beginning();
  int process_number(ObWktTokenVal &tkn_val);
  int process_word(ObWktTokenVal &tkn_val);

  int get_next_token(ObWktTokenType &tkn_type, ObString &tkn_string);
  // for 3D object
  int try_parse_zdim_token(ObWktTokenVal &z_val);
  int parse_geo_type(ObGeoType &geo_type);
  int refresh_type(uint64_t pos);
  int set_dimension(ObGeoDimType dim);

  common::ObIAllocator &allocator_;
  const char *wkt_;
  ObWkbBuffer wkb_buf_;
  int64_t cur_pos_;
  int64_t wkt_len_;
  ObGeoDimType dim_type_;
  bool is_oracle_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObWktParser);
};

} // end namespace common
} // end namespace oceanbase

#endif