package incsteps.plugin

import groovy.transform.CompileStatic

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

@CompileStatic
class CsvConcat {

    static Path csv_concat(Path source, List<Path> appends, boolean header, String sep) {

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
            validateInputs(newSource, append, header, sep)
        }
        Path first = newAppends.removeFirst()
        Path ret = concat(newSource, first, header, sep)
        newAppends.each{ append->
            ret = concat(ret, append, header, sep)
        }
        ret
    }

    static private Path concat(Path source, Path append, boolean header, String sep) {

        validateInputs(source, append, header, sep)

        Path result =
                Files.createTempFile( Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")

        def brInitial = new BufferedReader(new FileReader(source.toFile()))
        brInitial.lines().forEach { line->
            Files.write(result, line.bytes, StandardOpenOption.APPEND)
            if( !line.endsWith("\n"))
                Files.write(result, "\n".bytes, StandardOpenOption.APPEND)
        }
        if( !header ) {
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

    static void validateInputs(Path source, Path append, boolean header, String sep){
        if(header){
            def brSource = new BufferedReader(source.newInputStream().newReader())
            def headerSource = brSource.readLine()

            def brAppend = new BufferedReader(append.newInputStream().newReader())
            def headerAppend = brAppend.readLine()

            if( headerSource.split(sep).size() != headerAppend.split(sep).size() ){
                throw new IllegalArgumentException("source and append must to be same size")
            }
        }
    }
}
