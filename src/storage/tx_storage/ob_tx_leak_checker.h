/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_STORAGE_OB_TX_LEAK_CHECKER_H_
#define OCEANBASE_STORAGE_OB_TX_LEAK_CHECKER_H_
#include "share/leak_checker/ob_leak_checker.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "lib/profile/ob_trace_id.h"
#include "storage/ob_common_id_utils.h"
namespace oceanbase
{
namespace storage
{

struct ObReadOnlyTxCheckerKey
{
public:
  ObReadOnlyTxCheckerKey()
    : tenant_id_(0), seq_(0)
  {}
  ~ObReadOnlyTxCheckerKey() = default;
  int hash(uint64_t &hash_value) const
  {
    hash_value = seq_;
    return OB_SUCCESS;
  }
  OB_INLINE bool is_valid() const
  {
    return (seq_ != 0);
  }
  bool operator== (const ObReadOnlyTxCheckerKey &other) const
  {
    return (tenant_id_ == other.tenant_id_
            && seq_ == other.seq_);
  }
  TO_STRING_KV(K_(tenant_id), K_(seq));
public:
  uint64_t tenant_id_;
  int64_t seq_;
};

struct ObReadExInfo
{
public:
  enum {
    INVALID_TYPE = 0,
    WITH_PLAN    = 1,
    WITH_TRACE   = 2,
    WITH_LBT     = 3
  };
  ObReadExInfo() : type_(INVALID_TYPE) {}
  ~ObReadExInfo() {}
  TO_STRING_KV(K_(type));
public:
  int64_t type_;
};

struct ObReadExInfoPlan : public ObReadExInfo
{
public:
  ObReadExInfoPlan()
    : plan_id_(0)
  { type_ = WITH_PLAN; }
  ~ObReadExInfoPlan() {}
  INHERIT_TO_STRING_KV("ObReadExInfo", ObReadExInfo, K_(plan_id));
public:
  int64_t plan_id_;
};

struct ObReadExInfoTrace : public ObReadExInfoPlan
{
public:
  ObReadExInfoTrace()
    : trace_id_()
  { type_ = WITH_TRACE; }
  ~ObReadExInfoTrace() {}
  INHERIT_TO_STRING_KV("ObReadExInfoPlan", ObReadExInfoPlan, K_(trace_id));
public:
  common::ObCurTraceId::TraceId trace_id_;
};

struct ObReadExInfoBT : public ObReadExInfoTrace
{
public:
  ObReadExInfoBT() { type_ = WITH_LBT; bt_[0] = '\0'; }
  ~ObReadExInfoBT() {}
  INHERIT_TO_STRING_KV("ObReadExInfoTrace", ObReadExInfoTrace, K_(bt));
public:
  char bt_[512];
};

struct ObReadOnlyTxCheckerValue
{
public:
  ObReadOnlyTxCheckerValue()
    : tenant_id_(0),
      timestamp_(0),
      ls_id_(),
      tablet_id_(),
      extra_(nullptr)
  {
  }
  ~ObReadOnlyTxCheckerValue() = default;
  TO_STRING_KV(K_(tenant_id), K_(timestamp), K_(ls_id),
               K_(tablet_id), KPC_(extra));
public:
  uint64_t tenant_id_;
  int64_t timestamp_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObReadExInfo *extra_;
};

struct ObReadOnlyTxPrinter
{
  bool operator()(const ObReadOnlyTxCheckerKey &k, const ObReadOnlyTxCheckerValue &v)
  {
    bool ret = true;
    if (v.tenant_id_ == tenant_id_ && v.ls_id_ == ls_id_) {
      if (OB_ISNULL(v.extra_)) {
        COMMON_LOG(INFO, "LEAK_CHECKER ",
                   "key", k,
                   "value", v);
      } else if (OB_NOT_NULL(v.extra_)) {
        if (v.extra_->type_ >= ObReadExInfo::WITH_LBT) {
          ObReadExInfoBT *extra_info = static_cast<ObReadExInfoBT *>(v.extra_);
          COMMON_LOG(INFO, "LEAK_CHECKER ",
                 "key", k,
                 "value", v,
                 KPC(extra_info));
        } else if (v.extra_->type_ >= ObReadExInfo::WITH_TRACE) {
          ObReadExInfoTrace *extra_info = static_cast<ObReadExInfoTrace *>(v.extra_);
          COMMON_LOG(INFO, "LEAK_CHECKER ",
                 "key", k,
                 "value", v,
                 KPC(extra_info));
        } else if (v.extra_->type_ >= ObReadExInfo::WITH_PLAN) {
          ObReadExInfoPlan *extra_info = static_cast<ObReadExInfoPlan *>(v.extra_);
          COMMON_LOG(INFO, "LEAK_CHECKER ",
                 "key", k,
                 "value", v,
                 KPC(extra_info));
        }
      }
    }
    return ret;
  }
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

struct DiagnoseFunctor
{
  DiagnoseFunctor(const uint64_t tenant_id,
                  const share::ObLSID ls_id,
                  char *buf,
                  const int64_t pos,
                  const int64_t len)
    : tenant_id_(tenant_id), ls_id_(ls_id),
      buf_(buf), pos_(pos), len_(len)
  {}
  ~DiagnoseFunctor() {}
  bool operator()(const ObReadOnlyTxCheckerKey &k, const ObReadOnlyTxCheckerValue &v)
  {
    bool bool_ret = true;
    int ret = OB_SUCCESS;
    if (v.tenant_id_ == tenant_id_ && v.ls_id_ == ls_id_) {
      if (OB_FAIL(databuff_printf(buf_, len_, pos_, "{tablet_id:%ld",
                                  v.tablet_id_.id()))){
        // break the print
        bool_ret = false;
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(v.extra_)) {
        if (OB_SUCC(ret) && v.extra_->type_ >= ObReadExInfo::WITH_PLAN) {
          ObReadExInfoPlan *info = static_cast<ObReadExInfoPlan *>(v.extra_);
          if (OB_FAIL(databuff_printf(buf_, len_, pos_, ", plan_id:%ld",
                                      info->plan_id_))){
            // break the print
            bool_ret = false;
          }
        }
        if (OB_SUCC(ret) && v.extra_->type_ >= ObReadExInfo::WITH_TRACE) {
          ObReadExInfoTrace *info = static_cast<ObReadExInfoTrace *>(v.extra_);
          if (OB_FAIL(databuff_print_multi_objs(buf_, len_, pos_, ", trace:",
                                      info->trace_id_))){
            // break the print
            bool_ret = false;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(databuff_printf(buf_, len_, pos_, "}, "))){
          // break the print
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  char *buf_;
  int64_t pos_;
  int64_t len_;
};

const static int64_t TX_DEBUG_LEVEL_CACHE_REFRESH_INTERVAL = 1_s;
static int64_t get_tx_debug_level()
{
  RLOCAL_INIT(int64_t, last_check_timestamp, 0);
  RLOCAL_INIT(int64_t, last_result, 0);
  int64_t current_time = ObClockGenerator::getClock();
  if (current_time - last_check_timestamp < TX_DEBUG_LEVEL_CACHE_REFRESH_INTERVAL) {
    // Check once when the last memory burst or tenant_id does not match or the interval reaches the threshold
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (OB_LIKELY(tenant_config.is_valid())) {
      last_result = tenant_config->_tx_debug_level;
    }
    last_check_timestamp = current_time;
  }

  return last_result;
}

typedef share::ObBaseLeakChecker<ObReadOnlyTxCheckerKey, ObReadOnlyTxCheckerValue> ObReadOnlyTxChecker;
#define READ_CHECKER_RECORD(ctx)                                                         \
  do {                                                                                   \
    const static int64_t MAX_RECORD_CNT = 100000;    /* 10W * 0.5K = 50MB */             \
    int64_t tx_debug_level = get_tx_debug_level();                                       \
    if (OB_LIKELY(tx_debug_level <= 0)) {                                                \
    } else {                                                                             \
      ObReadOnlyTxCheckerKey key;                                                        \
      ObReadOnlyTxCheckerValue value;                                                    \
      key.seq_ = MTL(ObTransService *)->get_unique_seq();                                \
      key.tenant_id_ = MTL_ID();                                                         \
      value.tenant_id_ = MTL_ID();                                                       \
      value.timestamp_ = ObClockGenerator::getClock();                                   \
      value.ls_id_ = ctx.ls_id_;                                                         \
      value.tablet_id_ = ctx.tablet_id_;                                                 \
      ctx.check_seq_ = key.seq_;                                                         \
      ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();                               \
      if (OB_UNLIKELY(tx_debug_level >= 4)) {                                            \
        void* buf = ob_malloc(sizeof(ObReadExInfoBT), ObMemAttr(MTL_ID(), "readleakchecker")); \
        if (OB_NOT_NULL(buf)) {                                                                \
          ObReadExInfoBT *extra_info = new(buf) ObReadExInfoBT();                              \
          extra_info->trace_id_ = *(ObCurTraceId::get_trace_id());                             \
          if (OB_NOT_NULL(di)) {                                                               \
            extra_info->plan_id_ = di->get_ash_stat().plan_id_;                                \
          }                                                                                    \
          lbt(extra_info->bt_, sizeof(extra_info->bt_));                                       \
          value.extra_ = extra_info;                                                           \
        }                                                                                      \
      } else if (OB_UNLIKELY(tx_debug_level >= 3)) {                                              \
        void* buf = ob_malloc(sizeof(ObReadExInfoTrace), ObMemAttr(MTL_ID(), "readleakchecker")); \
        if (OB_NOT_NULL(buf)) {                                                                   \
          ObReadExInfoTrace *extra_info = new(buf) ObReadExInfoTrace();                           \
          extra_info->trace_id_ = *(ObCurTraceId::get_trace_id());                                \
          if (OB_NOT_NULL(di)) {                                                                  \
            extra_info->plan_id_ = di->get_ash_stat().plan_id_;                                   \
          }                                                                                       \
          value.extra_ = extra_info;                                                              \
        }                                                                                         \
      } else if (OB_UNLIKELY(tx_debug_level >= 2)) {                                              \
        void* buf = ob_malloc(sizeof(ObReadExInfoPlan), ObMemAttr(MTL_ID(), "readleakchecker"));  \
        if (OB_NOT_NULL(buf)) {                                                                   \
          ObReadExInfoPlan *extra_info = new(buf) ObReadExInfoPlan();                             \
          if (OB_NOT_NULL(di)) {                                                                  \
            extra_info->plan_id_ = di->get_ash_stat().plan_id_;                                   \
          }                                                                                       \
          value.extra_ = extra_info;                                                              \
        }                                                                                         \
      } else if (OB_LIKELY(tx_debug_level >= 1)) {                                                \
      }                                                                                           \
      MTL(ObTransService *)->get_read_tx_checker().record(key, value, MAX_RECORD_CNT);            \
    }                                                                                             \
  } while(0)

#define READ_CHECKER_RELEASE(ctx)                                                                \
  do {                                                                                           \
    if (OB_UNLIKELY(!MTL(ObTransService *)->get_read_tx_checker().is_empty())) {                 \
      ObReadOnlyTxCheckerKey key;                                                                \
      ObReadOnlyTxCheckerValue value;                                                            \
      key.seq_ = ctx.check_seq_;                                                                 \
      key.tenant_id_ = MTL_ID();                                                                 \
      MTL(ObTransService *)->get_read_tx_checker().release(key, value);                          \
      if (OB_NOT_NULL(value.extra_)) {                                                           \
        ob_free(value.extra_);                                                                   \
      }                                                                                          \
    }                                                                                            \
  } while(0)

#define READ_CHECKER_PRINT(ls_id) \
  do {                            \
    ObReadOnlyTxPrinter fn;       \
    fn.tenant_id_ = MTL_ID();     \
    fn.ls_id_ = ls_id;            \
    MTL(ObTransService *)->get_read_tx_checker().for_each(fn); \
  } while(0)

#define READ_CHECKER_FOR_EACH(fn)                              \
  do {                                                         \
    MTL(ObTransService *)->get_read_tx_checker().for_each(fn); \
  } while(0)

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_TX_LEAK_CHECKER_H_
