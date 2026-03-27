/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_ROUTINE_LOAD_RESOLVER_H
#define OCEANBASE_SQL_RESOLVER_CMD_OB_ROUTINE_LOAD_RESOLVER_H
#include "lib/ob_define.h"
#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/cmd/ob_load_data_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObCreateRoutineLoadStmt;
class ObPauseRoutineLoadStmt;
class ObResumeRoutineLoadStmt;
class ObStopRoutineLoadStmt;

class ObCreateRoutineLoadResolver : public ObLoadBaseResolver
{
public:
  explicit ObCreateRoutineLoadResolver(ObResolverParams &params):
    ObLoadBaseResolver(params)
  {
  }
  virtual ~ObCreateRoutineLoadResolver()
  {
  }
  virtual int resolve(const ParseNode &parse_tree);
  int resolve_load_properties(const ParseNode &node,
                              ObCreateRoutineLoadStmt &stmt);
  int resolve_job_properties(const ParseNode &node,
                             ObCreateRoutineLoadStmt &stmt);
  int file_format_to_str(const ObExternalFileFormat &format,
                         char *buf,
                         const int64_t buf_len,
                         int64_t &pos);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateRoutineLoadResolver);
};

class ObPauseRoutineLoadResolver : public ObLoadBaseResolver
{
public:
  explicit ObPauseRoutineLoadResolver(ObResolverParams &params):
    ObLoadBaseResolver(params)
  {
  }
  virtual ~ObPauseRoutineLoadResolver()
  {
  }
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPauseRoutineLoadResolver);
};

class ObResumeRoutineLoadResolver : public ObLoadBaseResolver
{
public:
  explicit ObResumeRoutineLoadResolver(ObResolverParams &params):
    ObLoadBaseResolver(params)
  {
  }
  virtual ~ObResumeRoutineLoadResolver()
  {
  }
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObResumeRoutineLoadResolver);
};

class ObStopRoutineLoadResolver : public ObLoadBaseResolver
{
public:
  explicit ObStopRoutineLoadResolver(ObResolverParams &params):
    ObLoadBaseResolver(params)
  {
  }
  virtual ~ObStopRoutineLoadResolver()
  {
  }
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObStopRoutineLoadResolver);
};

}//sql
}//oceanbase
#endif
