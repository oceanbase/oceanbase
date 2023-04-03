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

#ifndef __OCENABASE_RS_RESOURCE_WEIGHT_PARSER_H__
#define __OCENABASE_RS_RESOURCE_WEIGHT_PARSER_H__

#include "share/ob_kv_parser.h"
#include "rootserver/ob_balance_info.h"

namespace oceanbase
{
namespace rootserver
{
class ObResourceWeightParser
{
public:
  /*
   * if weight doesn't not sum to 1, return OB_INVALID_CONFIG
   */
  static int parse(const char *str, ObResourceWeight &weight);
private:
  class MyCb : public share::ObKVMatchCb
  {
  public:
    MyCb(ObResourceWeight &weight) : weight_(weight) {};
    int match(const char *key, const char *value);
  private:
    /* functions */
    typedef void (*WeightSetter)(ObResourceWeight &weight, double);
    static void set_iops(ObResourceWeight &weight, double val) { weight.iops_weight_ = val; }
    static void set_cpu(ObResourceWeight &weight, double val) { weight.cpu_weight_ = val; }
    static void set_memory(ObResourceWeight &weight, double val) { weight.memory_weight_ = val; }
    static void set_disk(ObResourceWeight &weight, double val) { weight.disk_weight_ = val; }
    ObResourceWeight &weight_;
  };
private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObResourceWeightParser);
};
}
}
#endif /* __OCENABASE_RS_RESOURCE_WEIGHT_PARSER_H__ */
//// end of header file


