package incsteps.plugin

import groovy.transform.CompileStatic

import java.nio.file.Files
import java.nio.file.Path

@CompileStatic
class CsvPretty {

    static Path pretty_csv(Path source, String sep, String newSep) {
        Path inputPath = Files.createTempFile(Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")
        inputPath.bytes = source.bytes
        inputPath.deleteOnExit()

        def definition = parseFile(inputPath, sep)
        return rewrite(inputPath, sep, newSep, definition)
    }

    static List<Integer>parseFile(Path source, String sep){
        boolean headers = false
        def brInitial = new BufferedReader(new FileReader(source.toFile()))
        def ret = [] as List<Integer>
        brInitial.lines().forEach { line->
            def fields = CsvCommon.parseCsvLine(line, sep)
            if( !headers ){
                headers=!headers
                ret = (0..fields.size()).collect{ 0 }
            }
            for(var i=0; i<fields.size(); i++){
                ret[i] = Math.max(ret[i], fields[i].length())
            }
        }
        ret
    }


    static Path rewrite(Path source, String sep, String newSep, List<Integer>definition){
        def destination = CsvCommon.createOutputPath()
        def brInitial = new BufferedReader(new FileReader(source.toFile()))

        destination.withWriter { writer ->
            StringBuilder newLine = new StringBuilder()
            brInitial.lines().forEach { line ->
                newLine.length = 0
                def fields = CsvCommon.parseCsvLine(line, sep)
                fields.eachWithIndex { String f, int i ->
                    newLine.append(f).append(' '*( definition[i]- f.length()))
                    if( i < fields.size()-1){
                        newLine.append(newSep)
                    }
                }
                writer.println(newLine)
            }
        }
        return destination
    }
}
