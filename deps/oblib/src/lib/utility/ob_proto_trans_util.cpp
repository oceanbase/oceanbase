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

#define USING_LOG_PREFIX LIB_UTIL

#include "lib/utility/ob_proto_trans_util.h"
#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
namespace common
{
  int ObProtoTransUtil::store_type_and_len(char *buf, int64_t len, int64_t &pos,
                                            int16_t type, int32_t v_len)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", KP(buf), K(ret));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))){
      OB_LOG(WARN, "failed to store type", K(type), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      // do nothing
    }

    return ret;
  }

  int ObProtoTransUtil::store_float(char *buf, const int64_t len, int64_t &pos, float val, int16_t type)
  {
    int ret = OB_SUCCESS;
    static const int FLT_SIZE = sizeof (float);
    int v_len = sizeof(float);
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", KP(buf), K(ret));
    } else if (len < pos + FLT_SIZE + 6) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))) {
      OB_LOG(WARN, "failed to store type", K(type), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      MEMCPY(buf + pos, &val, FLT_SIZE);
      pos += FLT_SIZE;
    }
    return ret;
  }

  int ObProtoTransUtil::store_double(char *buf, const int64_t len, int64_t &pos, double val, int16_t type)
  {
    int ret = OB_SUCCESS;
    static const int DBL_SIZE = sizeof (double);
    int v_len = sizeof(double);
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", KP(buf), K(ret));
    } else if (len < pos + DBL_SIZE + 6) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))){
      OB_LOG(WARN, "failed to store type", K(type), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      MEMCPY(buf + pos, &val, DBL_SIZE);
      pos += DBL_SIZE;
    }
    return ret;
  }

  int ObProtoTransUtil::store_str(char *buf, int64_t len, int64_t &pos, const char *str, const uint64_t str_len, int16_t type)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", KP(buf), K(ret));
    } else if (len < pos + str_len + 6) {
        ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))) {
      OB_LOG(WARN, "failed to store type", K(type), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, str_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      MEMCPY(buf+pos, str, str_len);
      pos += str_len;
    }
    return ret;
  }

  int ObProtoTransUtil::store_int1(char *buf, int64_t len, int64_t &pos, int8_t v, int16_t type)
  {
    int ret = OB_SUCCESS;
    int32_t v_len = 1;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))){
      OB_LOG(WARN, "failed to store type", K(v), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int1(buf, len, v, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      // do nothing
    }
    return ret;
  }

  int ObProtoTransUtil::store_int2(char *buf, int64_t len, int64_t &pos, int16_t v, int16_t type)
  {
    int ret = OB_SUCCESS;
    int32_t v_len = 2;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))){
      OB_LOG(WARN, "failed to store type", K(v), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, v, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      // do nothing
    }
    return ret;
  }

  int ObProtoTransUtil::store_int3(char *buf, int64_t len, int64_t &pos, int32_t v, int16_t type)
  {
    int ret = OB_SUCCESS;
    int32_t v_len = 3;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))) {
      OB_LOG(WARN, "failed to store type", K(v), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int3(buf, len, v, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      // do nothing
    }
    return ret;
  }

  int ObProtoTransUtil::store_int4(char *buf, int64_t len, int64_t &pos, int32_t v, int16_t type)
  {
    int ret = OB_SUCCESS;
    int32_t v_len = 4;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))) {
      OB_LOG(WARN, "failed to store type", K(v), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      // do nothing
    }
    return ret;
  }

  int ObProtoTransUtil::store_int5(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type)
  {
    int ret = OB_SUCCESS;
    int32_t v_len = 5;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))) {
      OB_LOG(WARN, "failed to store type", K(v), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int5(buf, len, v, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      // do nothing
    }
    return ret;
  }

  int ObProtoTransUtil::store_int6(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type)
  {
    int ret = OB_SUCCESS;
    int32_t v_len = 6;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))) {
      OB_LOG(WARN, "failed to store type", K(v), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int6(buf, len, v, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      // do nothing
    }
    return ret;
  }

  int ObProtoTransUtil::store_int8(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type)
  {
    int ret = OB_SUCCESS;
    int32_t v_len = 8;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))) {
      OB_LOG(WARN, "failed to store type", K(v), KP(buf), K(len));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int8(buf, len, v, pos))) {
      OB_LOG(WARN, "failed to store val", K(ret), KP(buf));
    } else {
      // do nothing
    }
    return ret;
  }

  int ObProtoTransUtil::resolve_type_and_len(const char *buf, int64_t len, int64_t &pos, int16_t &type, int32_t &v_len)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + 6 > len) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(len));
    } else {
      const char * data_type = buf + pos;
      obmysql::ObMySQLUtil::get_int2(data_type, type);
      pos += 2;
      int32_t val_len = 0;
      const char * data_val = buf + pos;
      obmysql::ObMySQLUtil::get_int4(data_val, val_len);
      pos += 4;
      v_len = (int64_t)val_len;
    }

    return ret;
  }

  int ObProtoTransUtil::get_int1(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int8_t &val) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + v_len > len || v_len != sizeof(int8_t)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(v_len), K(len));
    } else {
      const char * data = buf + pos;
      obmysql::ObMySQLUtil::get_int1(data, val);
      pos++;
    }
    return ret;
  }

  int ObProtoTransUtil::get_int2(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int16_t &val) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + v_len > len || v_len != sizeof(int16_t)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(v_len), K(len));
    } else {
      const char * data = buf + pos;
      obmysql::ObMySQLUtil::get_int2(data, val);
      pos+=2;
    }
    return ret;
  }
  int ObProtoTransUtil::get_int3(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int32_t &val) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + v_len > len || v_len != 3) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(v_len), K(len));
    } else {
      const char * data = buf + pos;
      obmysql::ObMySQLUtil::get_int3(data, val);
      pos+=3;
    }
    return ret;
  }

  int ObProtoTransUtil::get_int4(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int32_t &val) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + v_len > len || v_len != 4) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(v_len), K(len));
    } else {
      const char * data = buf + pos;
      obmysql::ObMySQLUtil::get_int4(data, val);
      pos+=4;
    }
    return ret;
  }

  int ObProtoTransUtil::get_int8(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int64_t &val) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + v_len > len || v_len != 8) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(v_len), K(len));
    } else {
      const char * data = buf + pos;
      obmysql::ObMySQLUtil::get_int8(data, val);
      pos+=8;
    }
    return ret;
  }

  int ObProtoTransUtil::get_double(const char *buf, int64_t len, int64_t &pos, int64_t v_len, double &val) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + v_len > len || v_len != sizeof(double)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(v_len), K(len));
    } else {
      val = (*((double *)(buf + pos)));
      buf+=8;
      pos+=8;
    }
    return ret;
  }

  int ObProtoTransUtil::get_float(const char *buf, int64_t len, int64_t &pos, int64_t v_len, float &val) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + v_len > len || v_len != sizeof(float)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(v_len), K(len));
    } else {
      val = (*((float *)(buf + pos)));
      buf+=4;
      pos+=4;
    }
    return ret;
  }

  int ObProtoTransUtil::get_str(const char *buf, int64_t len, int64_t &pos, int64_t str_len, char *&str) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + str_len > len) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(str_len), K(len));
    } else {
      str = (char *)(buf + pos);
      buf += str_len;
      pos += str_len;
    }

    return ret;
  }

  int ObProtoTransUtil::get_serialize_size(int64_t seri_sz) {
    return TYPE_LEN + VAL_LENGTH_LEN + seri_sz;
  }

  int ObProtoTransUtil::resolve_type(const char *buf, int64_t len, int64_t &pos, int16_t &type)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)){
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KP(buf));
    } else if (pos + 2 > len) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid input", K(pos), K(len));
    } else {
      const char * data_type = buf + pos;
      obmysql::ObMySQLUtil::get_int2(data_type, type);
      pos += 2;
    }

    return ret;
  }
  } // namespace common
} // namespace oceanbase
