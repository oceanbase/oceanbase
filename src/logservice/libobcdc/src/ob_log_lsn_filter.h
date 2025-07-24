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
 * Log LSN Filter
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_LSN_FILTER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_LSN_FILTER_H_

#include "lib/container/ob_se_array.h"              // ObSEArray
#include "share/ob_ls_id.h"                         // ObLSID
#include "src/logservice/palf/log_define.h"         // LSN

namespace oceanbase
{
namespace libobcdc
{

class IObLogLsnFilter
{
public:
  virtual ~IObLogLsnFilter() {}
public:
  virtual bool filter(int64_t tenant_id, int64_t ls_id, int64_t lsn) = 0;
};

class ObLogLsnFilter : public IObLogLsnFilter
{
public:
  static const int64_t DEFAULT_LOG_LSN_BLACK_LIST_SIZE = 8;
  ObLogLsnFilter();
  virtual ~ObLogLsnFilter();

public:
  struct TLSN {
    uint64_t tenant_id_;
    int64_t ls_id_;
    uint64_t lsn_;
    TLSN(const int64_t tenant_id, const int64_t ls_id, const int64_t lsn) : tenant_id_(tenant_id), ls_id_(ls_id), lsn_(lsn) {}

    TLSN(): tenant_id_(OB_INVALID_TENANT_ID), ls_id_(share::ObLSID::INVALID_LS_ID), lsn_(palf::LOG_INVALID_LSN_VAL) {}

    ~TLSN()
    {
      reset();
    }

    void reset()
    {
      tenant_id_ = OB_INVALID_TENANT_ID;
      ls_id_ = share::ObLSID::INVALID_LS_ID;
      lsn_ = palf::LOG_INVALID_LSN_VAL;
    }

    bool is_valid() const
    {
      return tenant_id_ != OB_INVALID_TENANT_ID && ls_id_ != share::ObLSID::INVALID_LS_ID && lsn_ && palf::LOG_INVALID_LSN_VAL;
    }

    int64_t get_tenant_id() const
    {
      return tenant_id_;
    }

    int64_t get_ls_id() const
    {
      return ls_id_;
    }

    int64_t get_lsn() const
    {
      return lsn_;
    }

    TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(lsn));
  };

public:
  int init(const char *lsn_black_list);
  void destroy();
  virtual bool filter(int64_t tenant_id, int64_t ls_id, int64_t lsn);

private:
  int parse_lsn_black_list_(const char *lsn_black_list);

private:
  bool inited_;
  bool empty_;
  common::ObSEArray<TLSN, DEFAULT_LOG_LSN_BLACK_LIST_SIZE> lsn_black_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogLsnFilter);
};

}
}
#endif