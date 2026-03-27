/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef JIT_CONTEXT_DI_H_
#define JIT_CONTEXT_DI_H_

#include "expr/ob_llvm_type.h"

namespace oceanbase
{
namespace jit
{
namespace core
{
struct JitDIContext
{
public:
  JitDIContext(ObIRModule &module)
    : dbuilder_(module),
      cu_(NULL),
      file_(NULL),
      sp_(NULL)
  {}

  ObDIBuilder dbuilder_;
  ObDICompileUnit *cu_;
  ObDIFile *file_;
  ObDISubprogram *sp_;
};

}  // core
}  // jit
}  // oceanbase

#endif /* JIT_DI_CONTEXT_H_ */
