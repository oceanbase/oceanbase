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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_TRIGGER_SQL_SERVICE_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_TRIGGER_SQL_SERVICE_H_
#include "share/schema/ob_ddl_sql_service.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObISQLClient;
}

namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
class ObTriggerInfo;
class ObTriggerSqlService : public ObDDLSqlService
{
protected:
  class ObTriggerValues
  {
  public:
    ObTriggerValues()
      : bit_value_(0),
        database_id_(OB_INVALID_ID),
        base_object_id_(OB_INVALID_ID),
        trigger_name_(),
        spec_source_(),
        body_source_()
    {}
    OB_INLINE void set_database_id(uint64_t database_id)
    { database_id_ = database_id; new_database_id_ = 1; }
    OB_INLINE void set_base_object_id(uint64_t base_object_id)
    { base_object_id_ = base_object_id; new_base_object_id_ = 1; }
    OB_INLINE void set_trigger_name(const common::ObString &trigger_name)
    { trigger_name_ = trigger_name; new_trigger_name_ = 1; }
    OB_INLINE void set_spec_source(const common::ObString &spec_source)
    { spec_source_ = spec_source; new_spec_source_ = 1; }
    OB_INLINE void set_body_source(const common::ObString &body_source)
    { body_source_ = body_source; new_body_source_ = 1; }
    OB_INLINE uint64_t get_database_id() const { return database_id_; }
    OB_INLINE uint64_t get_base_object_id() const { return base_object_id_; }
    OB_INLINE const common::ObString &get_trigger_name() const { return trigger_name_; }
    OB_INLINE const common::ObString &get_spec_source() const { return spec_source_; }
    OB_INLINE const common::ObString &get_body_source() const { return body_source_; }
    OB_INLINE bool new_database_id() const { return 1 == new_database_id_; }
    OB_INLINE bool new_base_object_id() const { return 1 == new_base_object_id_; }
    OB_INLINE bool new_trigger_name() const { return 1 == new_trigger_name_; }
    OB_INLINE bool new_spec_source() const { return 1 == new_spec_source_; }
    OB_INLINE bool new_body_source() const { return 1 == new_body_source_; }
  private:
    union
    {
      uint64_t bit_value_;
      struct {
        uint64_t new_database_id_:1;
        uint64_t new_base_object_id_:1;
        uint64_t new_trigger_name_:1;
        uint64_t new_spec_source_:1;
        uint64_t new_body_source_:1;
      };
    };
    uint64_t database_id_;
    uint64_t base_object_id_;
    common::ObString trigger_name_;
    common::ObString spec_source_;
    common::ObString body_source_;
  };
public:
  explicit ObTriggerSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObTriggerSqlService() {}

  int create_trigger(const ObTriggerInfo &trigger_info,
                     bool is_replace,
                     common::ObISQLClient &sql_client,
                     const common::ObString *ddl_stmt_str = NULL);
  int drop_trigger(const ObTriggerInfo &trigger_info,
                   bool drop_to_recyclebin,
                   int64_t new_schema_version,
                   common::ObISQLClient &sql_client,
                   const common::ObString *ddl_stmt_str = NULL);
  int alter_trigger(const ObTriggerInfo &trigger_info,
                    int64_t new_schema_version,
                    ObISQLClient &sql_client,
                    const ObString *ddl_stmt_str);
  int flashback_trigger(const ObTriggerInfo &trigger_info,
                        int64_t new_schema_version,
                        common::ObISQLClient &sql_client);
  int rebuild_trigger_package(const ObTriggerInfo &trigger_info,
                              const common::ObString &base_object_database,
                              const common::ObString &base_object_name,
                              int64_t new_schema_version,
                              common::ObISQLClient &sql_client,
                              ObSchemaOperationType op_type = OB_DDL_ALTER_TRIGGER);
  int update_base_object_id(const ObTriggerInfo &trigger_info,
                            uint64_t base_object_id,
                            int64_t new_schema_version,
                            common::ObISQLClient &sql_client);
private:
  int fill_dml_sql(const ObTriggerInfo &trigger_info,
                   int64_t new_schema_version,
                   ObDMLSqlSplicer &dml);
  int fill_dml_sql(const ObTriggerInfo &trigger_info,
                   const ObTriggerValues &new_values,
                   int64_t new_schema_version,
                   ObDMLSqlSplicer &dml);
  int log_trigger_operation(const ObTriggerInfo &trigger_info,
                            int64_t new_schema_version,
                            ObSchemaOperationType op_type,
                            const common::ObString *ddl_stmt_str,
                            common::ObISQLClient &sql_client);
  DISALLOW_COPY_AND_ASSIGN(ObTriggerSqlService);
};
} //end of schema
} //end of share
} //end of oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_TRIGGER_SQL_SERVICE_H_ */
