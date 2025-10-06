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
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Session
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint

import java.nio.file.Path

/**
 * Implements a custom function which can be imported by
 * Nextflow scripts.
 */
@CompileStatic
class CSVExtension extends PluginExtensionPoint {

    private Session session

    @Override
    protected void init(Session session) {
        this.session = session
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
        validateConcatArgs(params)
        CsvConcat.csv_concat(source, appends, (params?.header ?: "false") as boolean, (params?.sep ?: ",").toString())
    }

    protected void validateConcatArgs(Map params){
        //nothing by the moment
    }

    /**
     * Sort by column
     *
     * @param target
     */
    @Function
    Path csv_sort(Map params=[:], Path source) {
        CsvSort.csv_sort(source,
                params.containsKey("column") ? params.column.toString().trim() : "0",
                params.containsKey("sep") ? params.sep.toString() : ",")
    }

    /**
     * Trim column(s)
     *
     * @param target
     */
    @Function
    Path csv_trim(Map params=[:], Path source) {
        validateTrimArgs(params)

        String sep = (params.sep ?: ",").toString()
        List<String> columns = params.columns as List<String>
        CsvTrim.csv_trim(source, columns, sep)
    }

    protected static void validateTrimArgs(Map params){
        if( !params.containsKey("column") && !params.containsKey("columns") ) {
            throw new IllegalArgumentException("Column(s) to trim are required")
        }
        if( params.containsKey("column") && params.containsKey("columns") ){
            throw new IllegalArgumentException("Only column or columns must to be specified, not both")
        }
        if( params.containsKey("column") && params.column.toString().indexOf(",") != -1){
            throw new IllegalArgumentException("Column argument can't be a list. Use instead `columns`")
        }
        if( params.containsKey("column") ){
            params.columns = [params.column]
        }
        if( params.containsKey("columns") && !params.columns instanceof List){
            params.columns = params.columns.toString().split(",")
        }
    }

    /**
     * Create a CSV and consume the channel until stop
     */
    @Operator
    DataflowWriteChannel csv_create( final DataflowReadChannel source, final Map map=[:], final Closure closure =null ){
        List<String> headers = map.headers as List<String> ?: []
        String sep = map.sep ?: ","
        new CsvCreateOp(source, headers, sep, closure).apply()
    }

    /**
     * Generate a new CSV resizing columns
     */
    @Function
    Path csv_prettyprint(Map params=[:], Path source){
        String sep = params.sep ?: ","
        String newSep = params.containsKey("newSep") ? params.newSep : sep
        CsvPretty.pretty_csv(source, sep, newSep)
    }

}
