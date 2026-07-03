/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_BUILTIN_PROVIDER_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_BUILTIN_PROVIDER_H_

namespace oceanbase
{
namespace share
{

struct ObAIBuiltinProvider
{
  // Data source for built-in provider defaults.
  // default_base_url_ is the reference URL; at runtime if __all_ai_model_provider.base_url
  // is empty, the system falls back to this default via code-side lookup.
  const char *name_;
  const char *protocol_;
  const char *default_base_url_;
};

static const ObAIBuiltinProvider BUILTIN_PROVIDERS[] = {
  {"aliyun",           "openai",    "https://dashscope.aliyuncs.com/compatible-mode/v1"},
  {"aliyun-dashscope", "dashscope", "https://dashscope.aliyuncs.com/api/v1"},
  {"deepseek",         "openai",    "https://api.deepseek.com"},
  {"siliconflow",      "openai",    "https://api.siliconflow.cn/v1"},
  {"openai",           "openai",    "https://api.openai.com/v1"},
  {"cohere",           "cohere",    "https://api.cohere.com/v2"},
  {"tencent",          "openai",    "https://api.hunyuan.cloud.tencent.com/v1"},
};

static const int64_t BUILTIN_PROVIDER_COUNT = sizeof(BUILTIN_PROVIDERS) / sizeof(BUILTIN_PROVIDERS[0]);

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_BUILTIN_PROVIDER_H_
