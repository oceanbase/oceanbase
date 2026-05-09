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
 *
 * Trans ID Filter: filter specified (tenant_id, trans_id) transactions in CDC
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_TRANS_ID_FILTER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_TRANS_ID_FILTER_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "storage/tx/ob_trans_id.h"

namespace oceanbase
{
namespace libobcdc
{

class IObLogTransIDFilter
{
public:
  virtual ~IObLogTransIDFilter() {}
public:
  virtual bool should_filter(uint64_t tenant_id, const transaction::ObTransID &tx_id) = 0;
};

class ObLogTransIDFilter : public IObLogTransIDFilter
{
public:
  static const int64_t DEFAULT_TENANT_MAP_BUCKET = 8;
  static const int64_t DEFAULT_TRANS_ID_SET_BUCKET = 16;

  ObLogTransIDFilter();
  virtual ~ObLogTransIDFilter();

public:
  int init(const char *filter_trans_id_list);
  void destroy();
  virtual bool should_filter(uint64_t tenant_id, const transaction::ObTransID &tx_id);

private:
  int parse_filter_list_(const char *filter_trans_id_list);

private:
  bool inited_;
  common::hash::ObHashMap<uint64_t, common::hash::ObHashSet<int64_t> *> tenant_trans_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTransIDFilter);
};

}
}
#endif
