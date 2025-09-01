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

#ifndef OB_AI_FUNC_H_
#define OB_AI_FUNC_H_

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_json_utils.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_udf/ob_expr_udf_utils.h"
#include "share/ai_service/ob_ai_service_struct.h"

namespace oceanbase
{
namespace common
{

struct ObAIFuncExprInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObAIFuncExprInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
      name_(), type_(), url_(), api_key_(), model_(), provider_(),
      parameters_(), request_transform_fn_(),
      response_transform_fn_()
  {
  }
  virtual ~ObAIFuncExprInfo() {}
  void reset()
  {
    name_.reset();
    type_ = share::EndpointType::MAX_TYPE;
    url_.reset();
    api_key_.reset();
    model_.reset();
    provider_.reset();
    parameters_.reset();
    request_transform_fn_.reset();
    response_transform_fn_.reset();
  }
  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;
  int init(ObExecContext &exec_ctx, ObString &model_id);
  int init(ObIAllocator &allocator, ObString &model_id);
  int get_model_info(ObIAllocator &allocator, ObString &model_id);
  int init_by_endpoint_info(common::ObIAllocator &allocator,
                            share::ObAiModelEndpointInfo *endpoint_info);
  common::ObString name_;
  share::EndpointType::TYPE type_;
  common::ObString url_;
  common::ObString api_key_;
  common::ObString model_;
  common::ObString provider_;
  common::ObString parameters_;
  common::ObString request_transform_fn_;
  common::ObString response_transform_fn_;
};

class ObAIFuncBase
{
public:
  ObAIFuncBase() {}
  virtual ~ObAIFuncBase() {}
  virtual int get_header(common::ObIAllocator &allocator,
                         common::ObString &api_key,
                         ObArray<ObString> &headers) = 0;
  virtual int parse_output(common::ObIAllocator &allocator,
                           common::ObJsonObject *http_response,
                           common::ObIJsonBase *&result) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncBase);
};

class ObAIFuncIComplete : public ObAIFuncBase
{
public:
  ObAIFuncIComplete() {}
  virtual ~ObAIFuncIComplete() {}
  virtual int get_body(common::ObIAllocator &allocator,
                       common::ObString &model,
                       common::ObString &prompt,
                       common::ObString &content,
                       common::ObJsonObject *config,
                       common::ObJsonObject *&body) = 0;
  virtual int set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncIComplete);
};

class ObAIFuncIEmbed : public ObAIFuncBase
{
public:
  ObAIFuncIEmbed() {}
  virtual ~ObAIFuncIEmbed() {}
  virtual int get_body(common::ObIAllocator &allocator,
                       common::ObString &model,
                       common::ObArray<ObString> &contents,
                       common::ObJsonObject *config,
                       common::ObJsonObject *&body) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncIEmbed);
};

class ObAIFuncIRerank : public ObAIFuncBase
{
public:
  ObAIFuncIRerank() {}
  virtual ~ObAIFuncIRerank() {}
  virtual int get_body(common::ObIAllocator &allocator,
                       common::ObString &model,
                       common::ObString &query,
                       common::ObJsonArray *document_array,
                       common::ObJsonObject *config,
                       common::ObJsonObject *&body) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncIRerank);
};

class ObAIFuncHandle
{
public:
  ObAIFuncHandle() {}
  virtual ~ObAIFuncHandle() {}
  virtual int send_post(common::ObIAllocator &allocator,
                        ObString &url,
                        ObArray<ObString> &headers,
                        ObJsonObject *data,
                        ObJsonObject *&response) = 0;
  virtual int send_post_batch(common::ObIAllocator &allocator,
                              ObString &url,
                              ObArray<ObString> &headers,
                              ObArray<ObJsonObject *> &data_array,
                              ObArray<ObJsonObject *> &responses) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncHandle);
};

} // namespace common
} // namespace oceanbase

#endif /* OB_AI_FUNC_H_ */