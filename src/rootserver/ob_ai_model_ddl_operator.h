/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_ROOTSERVER_OB_AI_MODEL_DDL_OPERATOR_H_
 #define OCEANBASE_ROOTSERVER_OB_AI_MODEL_DDL_OPERATOR_H_

 #include "share/ob_rpc_struct.h"
 #include "share/schema/ob_schema_service.h"

namespace oceanbase
{

namespace rootserver
{
class ObAiModelDDLOperator
{
public:
  ObAiModelDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service)
      : schema_service_(schema_service) {}
  virtual ~ObAiModelDDLOperator() {}

  int create_ai_model(ObAiModelSchema &ai_model_schema,
                      const ObString &ddl_stmt,
                      common::ObMySQLTransaction &trans);
  int drop_ai_model(const ObAiModelSchema &ai_model_schema,
                    const ObString &ddl_stmt,
                    common::ObMySQLTransaction &trans);

private:
  share::schema::ObMultiVersionSchemaService &schema_service_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_AI_MODEL_DDL_OPERATOR_H_