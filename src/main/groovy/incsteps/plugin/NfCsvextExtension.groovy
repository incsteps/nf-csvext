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

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

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
     * Say hello to the given target.
     *
     * @param target
     */
    @Function
    Path csv_concat(Map params=[:], Path source, Path append) {

        validateInputs(source, append, params)

        Path result =
                Files.createTempFile( Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")

        Files.copy(source, result, StandardCopyOption.REPLACE_EXISTING)
        if( !params.containsKey('header') || !params.header as boolean) {
            Files.write(result, append.toFile().bytes, StandardOpenOption.APPEND)
        }else{
            def brAppend = new BufferedReader(new FileReader(append.toFile()))
            brAppend.readLine() //skip header
            brAppend.lines().forEach { line->
                Files.write(result, line.bytes, StandardOpenOption.APPEND)
                Files.write(result, "\n".bytes, StandardOpenOption.APPEND)
            }
        }

        result
    }

    static void validateInputs(Path source, Path append, Map params=[:]){
        if(params.containsKey('header') && params.header as boolean){
            def brSource = new BufferedReader(new FileReader(source.toFile()))
            def headerSource = brSource.readLine()

            def brAppend = new BufferedReader(new FileReader(append.toFile()))
            def headerAppend = brAppend.readLine()

            String sep = params.containsKey("sep") ? params.sep : ","
            if( headerSource.split(sep).size() != headerAppend.split(sep).size() ){
                throw new IllegalArgumentException("source and append must to be same size")
            }
        }
    }
}
