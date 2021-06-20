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

#ifndef OCEANBASE_SHARE_PARAMETER_OB_PARAMETER_MACRO_H_
#define OCEANBASE_SHARE_PARAMETER_OB_PARAMETER_MACRO_H_

////////////////////////////////////////////////////////////////////////////////
// SCOPE macro to support cluster or tenant parameter
////////////////////////////////////////////////////////////////////////////////
#define _OB_CLUSTER_PARAMETER common::Scope::CLUSTER
#define _OB_TENANT_PARAMETER common::Scope::TENANT

#define _DEF_PARAMETER_SCOPE_EASY(param, name, SCOPE, args...) SCOPE(_DEF_PARAMETER_EASY(param, _##SCOPE, name, args))
#define _DEF_PARAMETER_SCOPE_RANGE_EASY(param, name, SCOPE, args...) \
  SCOPE(_DEF_PARAMETER_RANGE_EASY(param, _##SCOPE, name, args))
#define _DEF_PARAMETER_SCOPE_CHECKER_EASY(param, name, SCOPE, args...) \
  SCOPE(_DEF_PARAMETER_CHECKER_EASY(param, _##SCOPE, name, args))

#define _DEF_PARAMETER_SCOPE_IP_EASY(param, name, SCOPE, def, args...) \
  SCOPE(_DEF_PARAMETER_CHECKER_EASY(param, _##SCOPE, name, def, common::ObConfigIpChecker, args))
#define _DEF_PARAMETER_SCOPE_LOG_LEVEL_EASY(param, name, SCOPE, def, args...) \
  SCOPE(_DEF_PARAMETER_CHECKER_EASY(param, _##SCOPE, name, def, common::ObConfigLogLevelChecker, args))

#define _DEF_PARAMETER_SCOPE_WORK_AREA_POLICY_EASY(param, name, SCOPE, def, args...) \
  SCOPE(_DEF_PARAMETER_CHECKER_EASY(param, _##SCOPE, name, def, common::ObConfigWorkAreaPolicyChecker, args))

// TODO: use parameter instead of config
#define _DEF_PARAMETER_EASY(param, scope, name, args...)                                                    \
  class ObConfig##param##Item##_##name : public common::ObConfig##param##Item {                             \
  public:                                                                                                   \
    ObConfig##param##Item##_##name() : common::ObConfig##param##Item(local_container(), scope, #name, args) \
    {}                                                                                                      \
    template <class T>                                                                                      \
    ObConfig##param##Item##_##name& operator=(T value)                                                      \
    {                                                                                                       \
      common::ObConfig##param##Item::operator=(value);                                                      \
      return *this;                                                                                         \
    }                                                                                                       \
  } name;

#define _DEF_PARAMETER_RANGE_EASY(param, scope, name, args...)                                              \
  class ObConfig##param##Item##_##name : public common::ObConfig##param##Item {                             \
  public:                                                                                                   \
    ObConfig##param##Item##_##name() : common::ObConfig##param##Item(local_container(), scope, #name, args) \
    {}                                                                                                      \
    template <class T>                                                                                      \
    ObConfig##param##Item##_##name& operator=(T value)                                                      \
    {                                                                                                       \
      common::ObConfig##param##Item::operator=(value);                                                      \
      return *this;                                                                                         \
    }                                                                                                       \
  } name;

#define _DEF_PARAMETER_CHECKER_EASY(param, scope, name, def, checker, args...)                                   \
  class ObConfig##param##Item##_##name : public common::ObConfig##param##Item {                                  \
  public:                                                                                                        \
    ObConfig##param##Item##_##name() : common::ObConfig##param##Item(local_container(), scope, #name, def, args) \
    {                                                                                                            \
      add_checker(new (std::nothrow) checker());                                                                 \
    }                                                                                                            \
    template <class T>                                                                                           \
    ObConfig##param##Item##_##name& operator=(T value)                                                           \
    {                                                                                                            \
      common::ObConfig##param##Item::operator=(value);                                                           \
      return *this;                                                                                              \
    }                                                                                                            \
  } name;

////////////////////////////////////////////////////////////////////////////////
#define DEF_INT(args...) _DEF_PARAMETER_SCOPE_RANGE_EASY(Int, args)

#define DEF_INT_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(Int, args)

#define DEF_DBL(args...) _DEF_PARAMETER_SCOPE_RANGE_EASY(Double, args)

#define DEF_CAP(args...) _DEF_PARAMETER_SCOPE_RANGE_EASY(Capacity, args)

#define DEF_CAP_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(Capacity, args)

#define DEF_TIME(args...) _DEF_PARAMETER_SCOPE_RANGE_EASY(Time, args)

#define DEF_TIME_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(Time, args)

#define DEF_BOOL(args...) _DEF_PARAMETER_SCOPE_EASY(Bool, args)

#define DEF_STR(args...) _DEF_PARAMETER_SCOPE_EASY(String, args)

#define DEF_STR_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(String, args)

#define DEF_IP(args...) _DEF_PARAMETER_SCOPE_IP_EASY(String, args)

#define DEF_MOMENT(args...) _DEF_PARAMETER_SCOPE_EASY(Moment, args)

#define DEF_INT_LIST(args...) _DEF_PARAMETER_SCOPE_EASY(IntList, args)

#define DEF_STR_LIST(args...) _DEF_PARAMETER_SCOPE_EASY(StrList, args)

#define DEF_LOG_ARCHIVE_OPTIONS_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(LogArchiveOptions, args)
#define DEF_LOG_LEVEL(args...) _DEF_PARAMETER_SCOPE_LOG_LEVEL_EASY(String, args)

#define DEF_WORK_AREA_POLICY(args...) _DEF_PARAMETER_SCOPE_WORK_AREA_POLICY_EASY(String, args)

// for ERRSIM

#ifdef ERRSIM

#define ERRSIM_DEF_INT(args...) _DEF_PARAMETER_SCOPE_RANGE_EASY(Int, args)

#define ERRSIM_DEF_INT_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(Int, args)

#define ERRSIM_DEF_DBL(args...) _DEF_PARAMETER_SCOPE_RANGE_EASY(Double, args)

#define ERRSIM_DEF_CAP(args...) _DEF_PARAMETER_SCOPE_RANGE_EASY(Capacity, args)

#define ERRSIM_DEF_CAP_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(Capacity, args)

#define ERRSIM_DEF_TIME(args...) _DEF_PARAMETER_SCOPE_RANGE_EASY(Time, args)

#define ERRSIM_DEF_TIME_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(Time, args)

#define ERRSIM_DEF_BOOL(args...) _DEF_PARAMETER_SCOPE_EASY(Bool, args)

#define ERRSIM_DEF_STR(args...) _DEF_PARAMETER_SCOPE_EASY(String, args)

#define ERRSIM_DEF_STR_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(String, args)

#define ERRSIM_DEF_IP(args...) _DEF_PARAMETER_SCOPE_IP_EASY(String, args)

#define ERRSIM_DEF_MOMENT(args...) _DEF_PARAMETER_SCOPE_EASY(Moment, args)

#define ERRSIM_DEF_INT_LIST(args...) _DEF_PARAMETER_SCOPE_EASY(IntList, args)

#define ERRSIM_DEF_STR_LIST(args...) _DEF_PARAMETER_SCOPE_EASY(StrList, args)

#define ERRSIM_DEF_LOG_ARCHIVE_OPTIONS_WITH_CHECKER(args...) _DEF_PARAMETER_SCOPE_CHECKER_EASY(LogArchiveOptions, args)

#define ERRSIM_DEF_LOG_LEVEL(args...) _DEF_PARAMETER_SCOPE_LOG_LEVEL_EASY(String, args)

#define ERRSIM_DEF_WORK_AREA_POLICY(args...) _DEF_PARAMETER_SCOPE_WORK_AREA_POLICY_EASY(String, args)

#else
#define ERRSIM_DEF_INT(args...)
#define ERRSIM_DEF_INT_WITH_CHECKER(args...)
#define ERRSIM_DEF_DBL(args...)
#define ERRSIM_DEF_CAP(args...)
#define ERRSIM_DEF_CAP_WITH_CHECKER(args...)
#define ERRSIM_DEF_TIME(args...)
#define ERRSIM_DEF_TIME_WITH_CHECKER(args...)
#define ERRSIM_DEF_BOOL(args...)
#define ERRSIM_DEF_STR(args...)
#define ERRSIM_DEF_STR_WITH_CHECKER(args...)
#define ERRSIM_DEF_IP(args...)
#define ERRSIM_DEF_MOMENT(args...)
#define ERRSIM_DEF_INT_LIST(args...)
#define ERRSIM_DEF_STR_LIST(args...)
#define ERRSIM_DEF_LOG_ARCHIVE_OPTIONS_WITH_CHECKER(args...)
#define ERRSIM_DEF_LOG_LEVEL(args...)
#define ERRSIM_DEF_WORK_AREA_POLICY(args...)
#endif

#endif
