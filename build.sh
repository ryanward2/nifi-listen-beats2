#!/usr/bin/env bash
# Copyright 2026 DDS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if ! command -v java >/dev/null 2>&1; then
  echo "Java 21 is required" >&2
  exit 1
fi
if ! java -version 2>&1 | head -1 | grep -Eq '"21([.]|\")'; then
  echo "Java 21 is required; found: $(java -version 2>&1 | head -1)" >&2
  exit 1
fi
if ! command -v mvn >/dev/null 2>&1; then
  echo "Maven 3.9.16 or newer is required" >&2
  exit 1
fi

maven_version="$(mvn -v | sed -n '1s/^Apache Maven \([^ ]*\).*/\1/p')"
if [[ -z "$maven_version" ]] || [[ "$(printf '%s\n%s\n' "3.9.16" "$maven_version" | sort -V | head -1)" != "3.9.16" ]]; then
  echo "Maven 3.9.16 or newer is required; found: ${maven_version:-unknown}" >&2
  exit 1
fi

mvn -B -T1C clean verify "$@"

nar="nifi-listen-beats2-nar/target/nifi-listen-beats2-nar-1.0.0.nar"
if [[ -f "$nar" ]]; then
  echo "Built: $nar"
fi
