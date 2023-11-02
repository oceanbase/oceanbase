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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_autoinc_nextval.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace sql;
using namespace share;
using namespace blocksstable;

int ObTableLoadAutoincNextval::get_uint_value(ObStorageDatum &datum,
                                              bool &is_zero,
                                              uint64_t &casted_value,
                                              const ObObjTypeClass &tc)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    is_zero = true;
    casted_value = 0;
  } else {
    switch (tc) {
      case ObIntTC: {
        is_zero = 0 == datum.get_int();
        casted_value = datum.get_int() < 0 ? 0 : datum.get_int();
        break;
      }
      case ObUIntTC: {
        is_zero = 0 == datum.get_uint64();
        casted_value = datum.get_uint64();
        break;
      }
      case ObFloatTC: {
        is_zero = 0 == datum.get_float();
        if (datum.get_float() > 0) {
          casted_value = static_cast<uint64_t>(datum.get_float() + 0.5);
        } else {
          casted_value = 0;
        }
        break;
      }
      case ObDoubleTC: {
        is_zero = 0 == datum.get_double();
        if (datum.get_double() > 0) {
          casted_value = static_cast<uint64_t>(datum.get_double() + 0.5);
        } else {
          casted_value = 0;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only int/float/double types support auto increment", KR(ret), K(datum));
    }
  }
  return ret;
}

int ObTableLoadAutoincNextval::get_input_value(ObStorageDatum &datum,
                                               AutoincParam &autoinc_param,
                                               bool &is_to_generate,
                                               uint64_t &casted_value,
                                               const ObObjTypeClass &tc,
                                               const uint64_t &sql_mode)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    is_to_generate = true;
  } else {
    bool is_zero = false;
    if (OB_FAIL(get_uint_value(datum, is_zero, casted_value, tc))) {
      LOG_WARN("get casted unsigned int value failed", KR(ret));
    }
    if (OB_SUCC(ret)) {
      if (!(SMO_NO_AUTO_VALUE_ON_ZERO & sql_mode)) {
        if (is_zero) {
          is_to_generate = true;
        }
      }
    }
  }
  // do not generate; sync value specified by user
  if (OB_SUCC(ret)) {
    if (!is_to_generate) {
      if (casted_value > autoinc_param.value_to_sync_) {
        autoinc_param.value_to_sync_ = casted_value;
        autoinc_param.sync_flag_ = true;
      }
    }
  }
  return ret;
}

int ObTableLoadAutoincNextval::generate_autoinc_value(ObAutoincrementService &auto_service,
                                                      AutoincParam *autoinc_param,
                                                      uint64_t &new_val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(autoinc_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", KR(ret), K(autoinc_param));
  } else {
    // sync insert value globally before sync value globally
    if (OB_FAIL(auto_service.sync_insert_value_global(*autoinc_param))) {
      LOG_WARN("failed to sync insert value globally", KR(ret));
    }
    if (OB_SUCC(ret)) {
      uint64_t value = 0;
      CacheHandle *&cache_handle = autoinc_param->cache_handle_;
      // get cache handle when allocate first auto-increment value
      if (OB_ISNULL(cache_handle)) {
        if (OB_FAIL(auto_service.get_handle(*autoinc_param, cache_handle))) {
          LOG_WARN("failed to get auto_increment handle", KR(ret));
        } else if (OB_ISNULL(cache_handle)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Error unexpceted", KR(ret), K(cache_handle));
        }
      }

      if (OB_SUCC(ret)) {
        // get auto-increment value
        if (OB_FAIL(cache_handle->next_value(value))) {
          LOG_DEBUG("failed to get auto_increment value", KR(ret), K(value));
          // release handle No.1
          auto_service.release_handle(cache_handle);
          // invalid cache handle; record count
          ++autoinc_param->autoinc_intervals_count_;
          if (OB_FAIL(auto_service.get_handle(*autoinc_param, cache_handle))) {
            LOG_WARN("failed to get auto_increment handle", KR(ret));
          } else if (OB_FAIL(cache_handle->next_value(value))) {
            LOG_WARN("failed to get auto_increment value", KR(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        new_val = value;
      }
    }
  }
  return ret;
}

int ObTableLoadAutoincNextval::eval_nextval(AutoincParam *autoinc_param,
                                            ObStorageDatum &datum,
                                            const ObObjTypeClass &tc,
                                            const uint64_t &sql_mode)
{
  int ret = OB_SUCCESS;
  bool is_to_generate = false;
  ObAutoincrementService &auto_service = ObAutoincrementService::get_instance();
  if (OB_ISNULL(autoinc_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", KR(ret));
  } else {
    // this column with column_index is auto-increment column
    if (OB_ISNULL(autoinc_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should find auto-increment param", KR(ret));
    }
    // sync last user specified value first(compatible with MySQL)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(auto_service.sync_insert_value_local(*autoinc_param))) {
        LOG_WARN("failed to sync last insert value", KR(ret));
      }
    }
    uint64_t new_val = 0;
    if (OB_SUCC(ret)) {
      // check : to generate auto-increment value or not
      if (OB_FAIL(get_input_value(datum, *autoinc_param, is_to_generate, new_val, tc, sql_mode))) {
        LOG_WARN("check generation failed", KR(ret));
      } else if (is_to_generate &&
                 OB_FAIL(generate_autoinc_value(auto_service, autoinc_param, new_val))) {
        LOG_WARN("generate autoinc value failed", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (!is_to_generate) {
        // do noting, keep the input datum
      } else {
        switch (tc) {
          case ObIntTC:
          case ObUIntTC: {
            datum.set_uint(new_val);
            break;
          }
          case ObFloatTC: {
            datum.set_float(static_cast<float>(new_val));
            break;
          }
          case ObDoubleTC: {
            datum.set_double(static_cast<double>(new_val));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("only int/float/double types support auto increment", KR(ret), K(datum));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (autoinc_param->autoinc_desired_count_ > 0) {
        --autoinc_param->autoinc_desired_count_;
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase