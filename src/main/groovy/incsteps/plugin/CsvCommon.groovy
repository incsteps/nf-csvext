package incsteps.plugin

import groovy.transform.CompileStatic

import java.nio.file.Files
import java.nio.file.Path

@CompileStatic
class CsvCommon {

    protected static Path createOutputPath() {
        Path result =
                Files.createTempFile( Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")
        return result
    }


    protected static List<String> parseCsvLine(String line, String sep) {
        def fields = [] as List<String>
        def inQuotes = false
        def currentField = new StringBuilder()

        line.chars.each { char c ->
            if (c == '"') {
                inQuotes = !inQuotes
            } else if (c == sep && !inQuotes) {
                fields << currentField.toString()
                currentField = new StringBuilder()
            } else {
                currentField.append(c)
            }
        }
        fields << currentField.toString()

        return fields
    }


}
