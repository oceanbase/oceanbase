
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

#include <spdlog/spdlog.h>

#include "vsag/logger.h"
#include "vsag/options.h"

namespace vsag {

class DefaultLogger : public Logger {
public:
    void
    SetLevel(Logger::Level log_level) override;

    void
    Trace(const std::string& msg) override;

    void
    Debug(const std::string& msg) override;

    void
    Info(const std::string& msg) override;

    void
    Warn(const std::string& msg) override;

    void
    Error(const std::string& msg) override;

    void
    Critical(const std::string& msg) override;
};

}  // namespace vsag
