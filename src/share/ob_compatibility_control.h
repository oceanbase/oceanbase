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

#ifndef OCEABASE_SHARE_OB_COMPATIBILITY_CONTROL_H_
#define OCEABASE_SHARE_OB_COMPATIBILITY_CONTROL_H_

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace share
{

enum ObCompatType
{
  COMPAT_MYSQL57,
  COMPAT_MYSQL8,
};

enum ObCompatFeatureType
{
#define DEF_COMPAT_CONTROL_FEATURE(type, args...) type,
#include "share/ob_compatibility_control_feature_def.h"
  COMPAT_FEATURE_END,
#include "share/ob_compatibility_security_feature_def.h"
#undef DEF_COMPAT_CONTROL_FEATURE
  MAX_TYPE
};

class ObICompatInfo
{
public:
  ObICompatInfo(ObCompatFeatureType type, const char *name, const char *description)
  : type_(type), name_(name), description_(description)
  {}
  virtual bool is_valid_version(uint64_t version) const;
  virtual int print_version_range(common::ObString &range_str, ObIAllocator &allocator) const;
protected:
  virtual void get_versions(const uint64_t *&versions, int64_t &version_num) const = 0;
public:
  ObCompatFeatureType type_;
  const char *name_;
  const char *description_;
};

template <int64_t N>
class ObCompatInfo : public ObICompatInfo
{
public:
  template <typename... Args>
  ObCompatInfo(ObCompatFeatureType type, const char *name, const char *description, Args... args)
  : ObICompatInfo(type, name, description),
    versions_{ args... }
  {
  }
protected:
  virtual void get_versions(const uint64_t *&versions, int64_t &version_num) const override
  {
    versions = versions_;
    version_num = ARRAYSIZEOF(versions_);
  }
private:
  uint64_t versions_[N];
};

class ObCompatControl
{
public:
  static int get_compat_version(const common::ObString &str, uint64_t &version);
  static int check_compat_version(const uint64_t tenant_id, const uint64_t compat_version);
  static int get_version_str(uint64_t version, common::ObString &str, ObIAllocator &allocator);
  static void get_compat_feature_infos(const ObICompatInfo **&infos, int64_t &len)
  { infos = infos_; len = ObCompatFeatureType::MAX_TYPE; }
  static int check_feature_enable(const uint64_t compat_version,
                                  const ObCompatFeatureType feature_type,
                                  bool &is_enable);
private:
#define DEF_COMPAT_CONTROL_FEATURE(type, description, args...)      \
  static const ObCompatInfo<ARGS_NUM(args)> COMPAT_##type;
#include "share/ob_compatibility_control_feature_def.h"
#include "share/ob_compatibility_security_feature_def.h"
#undef DEF_COMPAT_CONTROL_FEATURE
  static const ObICompatInfo* infos_[];
};

} // end of namespace share
} // end of namespace oceanbase

#endif /* OCEABASE_SHARE_OB_COMPATIBILITY_CONTROL_H_*/
