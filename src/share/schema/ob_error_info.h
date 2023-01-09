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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_ERROR_INFO_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_ERROR_INFO_H_
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{

namespace common
{
class ObISQLClient;
class ObMySQLTransaction;
class ObWarningBuffer;
class ObIAllocator;
}

namespace share
{
class ObDMLSqlSplicer;
}

namespace share
{
namespace schema
{
enum ObErrorInfoType 
{
  ERROR_INFO_ERROR,
  ERROR_INFO_WARNING,
  ERROR_INFO_UNDEFINED,
};

enum ObErrorInfoStatus
{
  ERROR_STATUS_UNDEFINED,
  ERROR_STATUS_NO_ERROR,  // no error, clean the error entry if __all_error has
  ERROR_STATUS_HAS_ERROR, // insert or replace in the __all_error
};

class ObSchemaService;

class ObErrorInfo : public ObSchema 
{
  OB_UNIS_VERSION(1);
public:
  ObErrorInfo();
  ObErrorInfo(const ObErrorInfo &src_schema);
  explicit ObErrorInfo(common::ObIAllocator *allocator);
  virtual ~ObErrorInfo();

  ObErrorInfo &operator=(const ObErrorInfo &src_schema);
  int assign(const ObErrorInfo &other);

#define DEFINE_GETTER(ret_type, name) \
  OB_INLINE ret_type get_##name() const { return name##_; }

  DEFINE_GETTER(uint64_t, tenant_id)
  DEFINE_GETTER(uint64_t, obj_id)
  DEFINE_GETTER(uint64_t, obj_type)
  DEFINE_GETTER(uint64_t, obj_seq)
  DEFINE_GETTER(uint64_t, line)
  DEFINE_GETTER(uint64_t, position)
  DEFINE_GETTER(uint64_t, text_length)
  DEFINE_GETTER(uint64_t, property)
  DEFINE_GETTER(uint64_t, error_number)
  DEFINE_GETTER(uint64_t, database_id)
  DEFINE_GETTER(int64_t, schema_version)
  DEFINE_GETTER(ObErrorInfoStatus, error_status)
  DEFINE_GETTER(const common::ObString&, text)

#undef DEFINE_GETTER

#define DEFINE_SETTER(name, type) \
  OB_INLINE void set_##name(type name) { name##_ = name; }

  DEFINE_SETTER(tenant_id, uint64_t)
  DEFINE_SETTER(obj_id, uint64_t)
  DEFINE_SETTER(obj_type, uint64_t)
  DEFINE_SETTER(obj_seq, uint64_t)
  DEFINE_SETTER(line, uint64_t)
  DEFINE_SETTER(position, uint64_t)
  DEFINE_SETTER(text_length, uint64_t)
  DEFINE_SETTER(property, uint64_t)
  DEFINE_SETTER(error_number, uint64_t)
  DEFINE_SETTER(database_id, uint64_t)
  DEFINE_SETTER(schema_version, int64_t)
  DEFINE_SETTER(error_status, ObErrorInfoStatus)
  OB_INLINE int set_text(common::ObString &text) { return deep_copy_str(text, text_); }

#undef DEFINE_SETTER

  bool is_valid() const;
  bool is_user_field_valid() const;
  void reset();
  int64_t get_convert_size() const;
  int collect_error_info(const IObErrorInfo *info);
  int collect_error_info(const IObErrorInfo *info, const common::ObWarningBuffer *buf, bool fill_info);
  int update_error_info(const IObErrorInfo *info);
  int get_error_info_from_table(common::ObISQLClient &sql_client, ObErrorInfo *old_err_info);
  int handle_error_info(common::ObMySQLTransaction &trans, const IObErrorInfo *info);
  int handle_error_info(const IObErrorInfo *info);
  int del_error(common::ObISQLClient &sql_client);
  int del_error(common::ObMySQLProxy *sql_proxy);
  int add_error(common::ObISQLClient & sql_client, bool is_replace, bool only_history);
  int get_error_obj_seq(common::ObISQLClient &sql_client, bool &exist);
  int delete_error(const IObErrorInfo *info);

  TO_STRING_KV(K_(tenant_id),
               K_(obj_id),
               K_(obj_type),
               K_(obj_seq),
               K_(line),
               K_(position),
               K_(text_length),
               K_(text),
               K_(property),
               K_(error_number),
               K_(database_id),
               K_(schema_version),
               K_(error_status))
private:
  int gen_error_dml(const uint64_t exec_tenant_id, oceanbase::share::ObDMLSqlSplicer &dml);
  uint64_t extract_tenant_id() const;
  uint64_t extract_obj_id() const;

private:
  uint64_t tenant_id_;
  uint64_t obj_id_;                 // errorinfo from which object. such as package etc.
  uint64_t obj_type_;
  uint64_t obj_seq_;
  uint64_t line_;                   // row number
  uint64_t position_;               // column number
  uint64_t text_length_;
  common::ObString text_;           // error text
  uint64_t property_;               // 0:error, 1:warning, other: undefined
  uint64_t error_number_;           // pls error number, others is 0;
  uint64_t database_id_;
  int64_t schema_version_;
  ObErrorInfoStatus error_status_; 
};

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_ERROR_INFO_H_ */
