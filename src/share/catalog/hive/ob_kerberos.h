/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef _SHARE_CATALOG_HIVE_OB_KERBEROS_H
#define _SHARE_CATALOG_HIVE_OB_KERBEROS_H

#include <krb5/krb5/krb5.h>

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{

class ObKerberos
{
public:
  ObKerberos()
      : default_cache_(nullptr), options_(nullptr), keytab_(nullptr),
        default_princ_(nullptr), creds_(), k5_data_() {}

  virtual ~ObKerberos();

public:
  int init(common::ObString &keytab, common::ObString &principal,
           common::ObString &conf_file, common::ObString &cache_name);

private:
  void reset_();

  static bool check_file_exists(const char *path);

private:
  struct Krb5Data {
    krb5_context ctx = nullptr;
    krb5_ccache out_cc = nullptr;
    krb5_principal princ = nullptr;
    char *name = nullptr;
    krb5_boolean switch_to_cache = FALSE;

    Krb5Data() { reset(); }

    void cleanup()
    {
      if (OB_NOT_NULL(ctx)) {
        if (OB_NOT_NULL(out_cc)) {
          krb5_cc_close(ctx, out_cc);
        }
        if (OB_NOT_NULL(princ)) {
          krb5_free_principal(ctx, princ);
        }
        if (OB_NOT_NULL(name)) {
          krb5_free_unparsed_name(ctx, name);
        }
        krb5_free_context(ctx);
      }
      reset();
    }

    void reset()
    {
      ctx = nullptr;
      princ = nullptr;
      out_cc = nullptr;
      name = nullptr;
      switch_to_cache = FALSE;
    }
  };

private:
  krb5_ccache default_cache_;
  krb5_get_init_creds_opt *options_;
  krb5_keytab keytab_;
  krb5_principal default_princ_;
  // Credentials structure including ticket, session key, and lifetime info.
  krb5_creds creds_;
  Krb5Data k5_data_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKerberos);
};

} // namespace share
} // namespace oceanbase

#endif /* _SHARE_CATALOG_HIVE_OB_KERBEROS_H */