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

#ifndef _OB_PROTO_TRANS_UTILL_H_
#define _OB_PROTO_TRANS_UTILL_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
  class ObProtoTransUtil
  {
    public:
      static const int64_t TYPE_LEN = 2;
      static const int64_t VAL_LENGTH_LEN = 4;
      // for encode
      static int store_type_and_len(char *buf, int64_t len, int64_t &pos, int16_t type, int32_t v_len);
      static int store_str(char *buf, int64_t len, int64_t &pos, const char *str, const uint64_t str_len, int16_t type);

      //@{Serialize integer data, save the data in v to the position of buf+pos, and update pos
      static int store_int1(char *buf, int64_t len, int64_t &pos, int8_t v, int16_t type);
      static int store_int2(char *buf, int64_t len, int64_t &pos, int16_t v, int16_t type);
      static int store_int3(char *buf, int64_t len, int64_t &pos, int32_t v, int16_t type);
      static int store_int4(char *buf, int64_t len, int64_t &pos, int32_t v, int16_t type);
      static int store_int5(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type);
      static int store_int6(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type);
      static int store_int8(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type);

      static int store_double(char *buf, const int64_t len, int64_t &pos, double val, int16_t type);
      static int store_float(char *buf, const int64_t len, int64_t &pos, float val, int16_t type);

      // for decode
      static int resolve_type(const char *buf, int64_t len, int64_t &pos, int16_t &type);
      static int resolve_type_and_len(const char *buf, int64_t len, int64_t &pos, int16_t &type, int32_t &v_len);
      static int get_str(const char *buf, int64_t len, int64_t &pos, int64_t str_len, char *&str);
      //@{ Signed integer in reverse sequence, write the result(the position of buf+pos) to v, and update pos
      static int get_int1(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int8_t &val);
      static int get_int2(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int16_t &val);
      static int get_int3(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int32_t &val);
      static int get_int4(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int32_t &val);
      static int get_int8(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int64_t &val);
      static int get_double(const char *buf, int64_t len, int64_t &pos, int64_t v_len, double &val);
      static int get_float(const char *buf, int64_t len, int64_t &pos, int64_t v_len, float &v);

      // get serialize size 
      static int get_serialize_size(int64_t seri_sz);
    private:
      DISALLOW_COPY_AND_ASSIGN(ObProtoTransUtil);
  };

}
}

#endif /* _OB_PROTO_TRANS_UTILL_H_ */
