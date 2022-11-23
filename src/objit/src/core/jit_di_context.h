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
