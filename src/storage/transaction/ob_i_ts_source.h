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

#ifndef OCEANBASE_TRANSACTION_OB_I_TS_SOURCE_
#define OCEANBASE_TRANSACTION_OB_I_TS_SOURCE_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "ob_trans_define.h"

namespace oceanbase {
namespace transaction {
class ObTsCbTask;

enum {
  TS_SOURCE_UNKNOWN = -1,
  TS_SOURCE_LTS = 0,
  TS_SOURCE_GTS = 1,
  TS_SOURCE_HA_GTS = 2,
  MAX_TS_SOURCE,
};

OB_INLINE bool is_valid_ts_source(const int ts_type)
{
  return TS_SOURCE_UNKNOWN < ts_type && ts_type < MAX_TS_SOURCE;
}

inline bool is_ts_type_external_consistent(const int64_t ts_type)
{
  return ts_type == TS_SOURCE_GTS || ts_type == TS_SOURCE_HA_GTS;
}

class ObTsParam {
public:
  ObTsParam()
  {
    reset();
  }
  ~ObTsParam()
  {
    destroy();
  }
  void reset()
  {
    need_inc_ = true;
  }
  void destroy()
  {
    reset();
  }
  void set_need_inc(const bool need_inc)
  {
    need_inc_ = need_inc;
  }
  bool need_inc() const
  {
    return need_inc_;
  }

private:
  // Whether the gts value needs to be +1,
  // it is increased by one by default in the gts cache management
  bool need_inc_;
};

class ObITsSource {
public:
  virtual int update_gts(const int64_t gts, bool& update) = 0;
  virtual int update_local_trans_version(const int64_t version, bool& update) = 0;
  virtual int get_gts(const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts) = 0;
  virtual int get_gts(ObTsCbTask* task, int64_t& gts) = 0;
  // The following two interfaces are expected to be used only by the transaction layer,
  // and there is no demand for other modules
  virtual int get_local_trans_version(
      const MonotonicTs stc, ObTsCbTask* task, int64_t& version, MonotonicTs& receive_gts_ts) = 0;
  virtual int get_local_trans_version(ObTsCbTask* task, int64_t& version) = 0;
  virtual int wait_gts_elapse(const int64_t gts, ObTsCbTask* task, bool& need_wait) = 0;
  virtual int wait_gts_elapse(const int64_t gts) = 0;
  virtual int refresh_gts(const bool need_refresh) = 0;
  virtual int update_base_ts(const int64_t base_ts, const int64_t publish_version) = 0;
  virtual int get_base_ts(int64_t& base_ts, int64_t& publish_version) = 0;
  virtual bool is_external_consistent() = 0;
  virtual int update_publish_version(const int64_t publish_version) = 0;
  virtual int get_publish_version(int64_t& publish_version) = 0;

public:
  VIRTUAL_TO_STRING_KV("", "");
};

}  // namespace transaction
}  // end of namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_I_TS_SOURCE_
