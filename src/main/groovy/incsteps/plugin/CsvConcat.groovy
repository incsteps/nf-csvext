package incsteps.plugin

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class CsvConcat {

    static Path csv_concat(Map params=[:], Path source, List<Path> appends) {

        Path newSource = Files.createTempFile( Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")
        newSource.bytes = source.bytes
        newSource.deleteOnExit()

        List<Path>newAppends =  appends.collect{path->
            Path newAppend = Files.createTempFile( Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")
            newAppend.deleteOnExit()
            newAppend.bytes = path.bytes
            newAppend
        }

        newAppends.each {append->
            validateInputs(newSource, append, params)
        }
        Path first = newAppends.removeFirst()
        Path ret = concat(params, newSource, first)
        newAppends.each{ append->
            ret = concat(params, ret, append)
        }
        ret
    }

    static private Path concat(Map params=[:], Path source, Path append) {

        validateInputs(source, append, params)

        Path result =
                Files.createTempFile( Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")

        def brInitial = new BufferedReader(new FileReader(source.toFile()))
        brInitial.lines().forEach { line->
            Files.write(result, line.bytes, StandardOpenOption.APPEND)
            if( !line.endsWith("\n"))
                Files.write(result, "\n".bytes, StandardOpenOption.APPEND)
        }
        if( !params.containsKey('header') || !params.header as boolean) {
            Files.write(result, append.toFile().bytes, StandardOpenOption.APPEND)
        }else{
            def brAppend = new BufferedReader(new FileReader(append.toFile()))
            brAppend.readLine() //skip header
            brAppend.lines().forEach { line->
                Files.write(result, line.bytes, StandardOpenOption.APPEND)
                if( !line.endsWith("\n"))
                    Files.write(result, "\n".bytes, StandardOpenOption.APPEND)
            }
        }

        result
    }

    static void validateInputs(Path source, Path append, Map params=[:]){
        if(params.containsKey('header') && params.header as boolean){
            def brSource = new BufferedReader(source.newInputStream().newReader())
            def headerSource = brSource.readLine()

            def brAppend = new BufferedReader(append.newInputStream().newReader())
            def headerAppend = brAppend.readLine()

            String sep = params.containsKey("sep") ? params.sep : ","
            if( headerSource.split(sep).size() != headerAppend.split(sep).size() ){
                throw new IllegalArgumentException("source and append must to be same size")
            }
        }
    }
}
