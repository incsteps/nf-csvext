package incsteps.plugin

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

class CsvConcat {

    static Path csv_concat(Map params=[:], Path source, List<Path> appends) {
        appends.each {append->
            validateInputs(source, append, params)
        }
        Path first = appends.removeFirst()
        Path ret = csv_concat(params, source, first)
        appends.each{ append->
            ret = csv_concat(params, ret, append)
        }
        ret
    }

    static Path csv_concat(Map params=[:], Path source, Path append) {

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
