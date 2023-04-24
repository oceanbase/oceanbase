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

#ifndef _OCEANBASE_WORKLOAD_OPTIONS_H_
#define _OCEANBASE_WORKLOAD_OPTIONS_H_

#include "ob_testbench_utils.h"
#include "ob_testbench_macros.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
  namespace testbench
  {

    class ObIWorkloadOptions
    {
    public:
      ObIWorkloadOptions(const char *src_opt_str)
          : src_opt_str_(src_opt_str),
            start_time_(0),
            duration_(5) {}
      virtual ~ObIWorkloadOptions(){};
      virtual int parse_options() = 0;
      virtual int64_t to_string(char *buffer, const int64_t size) const = 0;
      const char *src_opt_str_;
      int start_time_;
      int duration_;
    };

    template <enum WorkloadType>
    class ObWorkloadOptions;

    template <>
    class ObWorkloadOptions<WorkloadType::DISTRIBUTED_TRANSACTION> : public ObIWorkloadOptions
    {
    public:
      ObWorkloadOptions(const char *opt_str)
          : ObIWorkloadOptions(opt_str),
            participants_(1),
            operations_(1),
            affect_rows_(10)
      {
      }
      ~ObWorkloadOptions() {}

      int fill_options(const char *key, const char *value)
      {
        int ret = OB_SUCCESS;
        DTxnOption match = DTxnOption::END;
        for (int i = 0; i < sizeof(dtxn_opts) / sizeof(dtxn_opts[0]); ++i)
        {
          if (0 == strcmp(dtxn_opts[i], key))
          {
            match = static_cast<DTxnOption>(i);
            break;
          }
        }
        switch (match)
        {
        case STARTTIME:
          start_time_ = atoi(value);
          break;
        case DURATION:
          duration_ = atoi(value);
          break;
        case PARTICIPANTS:
          participants_ = atoi(value);
          break;
        case OPERATIONS:
          operations_ = atoi(value);
          break;
        case AFFECTROWS:
          affect_rows_ = atoi(value);
          break;
        default:
          TESTBENCH_LOG(WARN, "unexpected option for distributed transaction workload", KP(key), KP(value));
          ret = OB_INVALID_ARGUMENT;
          break;
        }
        return ret;
      }

      int parse_options() override
      {
        int ret = OB_SUCCESS;
        const char **opts = NULL;
        int opts_cnt = 0;
        TESTBENCH_LOG(INFO, "parse_option", KCSTRING(src_opt_str_));
        if (OB_FAIL(split(src_opt_str_, ",", 0, opts, opts_cnt)))
        {
          TESTBENCH_LOG(WARN, "parse options for distributed transaction workload fail, use default options");
        }
        else
        {
          TESTBENCH_LOG(INFO, "parse options for distributed transaction workload", KP(opts), KP(opts_cnt));
          for (int i = 0; i < opts_cnt; ++i)
          {
            const char **kv = NULL;
            int kv_cnt = 0;
            if (OB_FAIL(split(*(opts + i), "=", 2, kv, kv_cnt)))
            {
              TESTBENCH_LOG(WARN, "parse key value options for distributed transaction workload fail", KP(*(opts + i)));
            }
            else
            {
              fill_options(*kv, *(kv + 1));
            }
          }
        }
        TESTBENCH_LOG(INFO, "parse options for distributed transaction workload",
                      KP(participants_), KP(operations_), KP(affect_rows_), KP(start_time_), KP(duration_));
        return ret;
      }

      int64_t to_string(char *buffer, const int64_t size) const override
      {
        int64_t pos = 0;
        if (nullptr != buffer && size > 0)
        {
          databuff_printf(buffer, size, pos, "start=%d duration=%d participant=%d operation=%d row=%d",
                          start_time_,
                          duration_,
                          participants_,
                          operations_,
                          affect_rows_);
        }
        return pos;
      }

    public:
      int participants_;
      int operations_;
      int affect_rows_;
    };
  };
}
#endif