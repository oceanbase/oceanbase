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

#ifndef SRC_PL_WARNING_H
#define SRC_PL_WARNING_H

#include "lib/container/ob_se_array.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace common
{
class ObString;
}

namespace pl
{
class PlCompilerWarningCategory final
{
public:
  enum w_status {
    WARN_UNKNOWN = -1,
    WARN_ENABLE = 0,
    WARN_DISABLE = 1,
    WARN_ERROR = 2,
  };
  enum w_conf {
    CONF_UNKNOWN = -1,
    CONF_ALL = 0,
    CONF_SEVERE = 1,
    CONF_PERF = 2,
    CONF_INFO = 3,
  };
  class WarningParser final
  {
  public:
    explicit WarningParser(ObString &str) : str_(str) {}
    virtual ~WarningParser() {}
    bool is_category() {
      return 0 == str_.case_compare("enable") ||
              0 == str_.case_compare("disable") ||
              0 == str_.case_compare("error");
    }

    bool is_config() {
      return 0 == str_.case_compare("all") ||
              0 == str_.case_compare("severe") ||
              0 == str_.case_compare("performance") ||
              0 == str_.case_compare("informational");
    }

    bool is_err_code();

    w_status get_category() {
      w_status ws = PlCompilerWarningCategory::WARN_UNKNOWN;
      if (0 == str_.case_compare("enable")) {
        ws = PlCompilerWarningCategory::WARN_ENABLE;
      } else if (0 == str_.case_compare("disable")) {
        ws = PlCompilerWarningCategory::WARN_DISABLE;
      } else if (0 == str_.case_compare("error")) {
        ws = PlCompilerWarningCategory::WARN_ERROR;
      } else {
        // do nothig
      }
      return ws;
    }

    w_conf get_conf() {
      w_conf wc = PlCompilerWarningCategory::CONF_UNKNOWN;
      if (0 == str_.case_compare("all")) {
        wc = PlCompilerWarningCategory::CONF_ALL;
      } else if (0 == str_.case_compare("severe")) {
        wc = PlCompilerWarningCategory::CONF_SEVERE;
      } else if (0 == str_.case_compare("performance")) {
        wc = PlCompilerWarningCategory::CONF_PERF;
      } else if (0 == str_.case_compare("informational")) {
        wc = PlCompilerWarningCategory::CONF_INFO;
      }
      return wc;
    }

    int get_err_code(int64_t &err_code);

  private:
    ObString str_;
    DISABLE_COPY_ASSIGN(WarningParser);
  };

  class DoParseWarning final
  {
  public:
    explicit DoParseWarning(const ObString &warn_str) : warning_str_(warn_str) {}
    ~DoParseWarning() {}

    int operator()(PlCompilerWarningCategory *category);

  private :
    ObString warning_str_;
    DISABLE_COPY_ASSIGN(DoParseWarning);
  };

  explicit PlCompilerWarningCategory() : severe_(static_cast<uint64_t>(WARN_ENABLE)),
                                         performance_(static_cast<uint64_t>(WARN_ENABLE)),
                                         informational_(static_cast<uint64_t>(WARN_ENABLE)) {}
  ~PlCompilerWarningCategory() {}

  struct err_code_wrap
  {
    err_code_wrap(w_status ws, int64_t ec) : status(ws), err_code(ec) {}
    err_code_wrap() : status(WARN_UNKNOWN), err_code(0) {}

    w_status status; // indicate this error code is disable, enable, or treat as error;
    int64_t err_code;

    inline bool is_disable() {
      return WARN_DISABLE == status;
    }
    inline bool is_treat_as_error() {
      return WARN_ERROR == status;
    }
    inline bool is_enable() {
      return WARN_ENABLE == status;
    }
    TO_STRING_KV(K(status), K(err_code));
  };
  static bool is_valid_err_code(const int64_t err_code) {
   /*
    1.       The severe code is in the range of 05000 to 05999.
    2.       The informational code is in the range of 06000 to 06999.
    3.       The performance code is in the range of 07000 to 07249.
    */
    return (5000 <= err_code && 7206 >= err_code);
  }

  int update_status(w_status &ws, w_conf &wc, const int64_t err_code);
  inline bool is_enable_all() const {
    return WARN_ENABLE == static_cast<w_status>(severe_)
          && WARN_ENABLE == static_cast<w_status>(performance_)
          && WARN_ENABLE == static_cast<w_status>(informational_);
  }
  inline bool is_disable_all() const {
    return WARN_DISABLE == static_cast<w_status>(severe_)
          && WARN_DISABLE == static_cast<w_status>(performance_)
          && WARN_DISABLE == static_cast<w_status>(informational_);
  }
  inline bool is_error_all() const {
    return WARN_ERROR == static_cast<w_status>(severe_)
          && WARN_ERROR == static_cast<w_status>(performance_)
          && WARN_ERROR == static_cast<w_status>(informational_);
  }

#define DEF_STATUS_FUNC(catg) \
  inline bool is_enable_##catg() const { \
    return WARN_ENABLE == static_cast<w_status>(catg##_); \
  } \
  inline bool is_disable_##catg() const { \
    return WARN_DISABLE == static_cast<w_status>(catg##_); \
  } \
  inline bool is_error_##catg() const { \
    return WARN_ERROR == static_cast<w_status>(catg##_); \
  }

  DEF_STATUS_FUNC(severe);
  DEF_STATUS_FUNC(performance);
  DEF_STATUS_FUNC(informational);

  #undef DEF_STATUS_FUNC

  w_status get_err_code_cat(int64_t err_code);
  int init();
  int reset();

  static int string_to_err_code(const common::ObString &err_str, int64_t &err_code);
  static int verify_warning_settings(const common::ObString &confs_str,
                                     PlCompilerWarningCategory *category);

  static int set_pl_warning_setting(sql::ObExecContext &ctx,
                                    sql::ParamStore &params,
                                    common::ObObj &result);
  static int add_pl_warning_setting_cat(sql::ObExecContext &ctx,
                                        sql::ParamStore &params,
                                        common::ObObj &result);
  static int add_pl_warning_setting_num(sql::ObExecContext &ctx,
                                        sql::ParamStore &params,
                                        common::ObObj &result);
  static int add_pl_warning_impl(sql::ObExecContext &ctx, const common::ObString &conf);

  static int get_category(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_warning_setting_cat(sql::ObExecContext &ctx,
                                     sql::ParamStore &params,
                                     common::ObObj &result);
  static int get_warning_setting_num(sql::ObExecContext &ctx,
                                     sql::ParamStore &params,
                                     common::ObObj &result);
  static int get_warning_setting_string(sql::ObExecContext &ctx,
                                        sql::ParamStore &params,
                                        common::ObObj &result);

  struct {
    uint64_t severe_ : 4; // w_status
    uint64_t performance_ : 4;
    uint64_t informational_ : 4;
  };
  uint64_t hash_val_; // plsql_warning string hash value, strip space
  common::hash::ObHashMap<int64_t, err_code_wrap, common::hash::NoPthreadDefendMode> err_code_map_;
  TO_STRING_KV(K_(hash_val), K_(severe), K_(performance), K_(informational));
};

} // namespace pl
} // namespace oceanbase

#endif
