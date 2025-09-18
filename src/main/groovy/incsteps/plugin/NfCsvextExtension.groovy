/*
 * Copyright 2025, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package incsteps.plugin

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.PluginExtensionPoint

import java.nio.file.Path

/**
 * Implements a custom function which can be imported by
 * Nextflow scripts.
 */
@CompileStatic
class NfCsvextExtension extends PluginExtensionPoint {

    @Override
    protected void init(Session session) {
    }

    /**
     * Concat 2 files
     *
     * @param target
     */
    @Function
    Path csv_concat(Map params=[:], Path source, Path append) {
        csv_concat(params, source, [append])
    }

    /**
     * Concat n files
     *
     * @param target
     */
    @Function
    Path csv_concat(Map params=[:], Path source, List<Path> appends) {
        CsvConcat.csv_concat(params, source, appends)
    }

    /**
     * Sort by column
     *
     * @param target
     */
    @Function
    Path csv_sort(Map params=[:], Path source) {
        CsvSort.sortCSVByColumn(source,
                params.containsKey("column") ? params.column.toString().trim() : "0",
                params.containsKey("sep") ? params.sep.toString() : ",")
    }
}
