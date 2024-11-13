// Copyright 2024-present the vsag project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>

namespace vsag {

/**
  * Get the version based on git revision
  * 
  * @return the version text
  */
extern std::string
version();

/**
  * Init the vsag library
  * 
  * @return true always
  */
extern bool
init();

}  // namespace vsag

#include "allocator.h"
#include "binaryset.h"
#include "bitset.h"
#include "constants.h"
#include "dataset.h"
#include "errors.h"
#include "expected.hpp"
#include "factory.h"
#include "index.h"
#include "logger.h"
#include "options.h"
#include "readerset.h"
#include "utils.h"
