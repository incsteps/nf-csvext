package incsteps.plugin

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class CsvTrim {

    static Path csv_trim(Path source, List<String>columns, String sep) {
        Path inputPath = Files.createTempFile(Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")
        inputPath.bytes = source.bytes
        inputPath.deleteOnExit()

        csvTrim(source, columns, sep)
    }

    static private Path csvTrim(Path source, List<String>columns, String sep) {

        def positions = columnsToPositions(source, columns, sep)

        Path result =
                Files.createTempFile( Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")

        def brInitial = new BufferedReader(new FileReader(source.toFile()))

        brInitial.lines().forEach { line->
            def fields = line.split(sep)
            def dump = []
            for(int i=0; i<fields.length; i++){
                if( !positions.contains(i) ){
                    dump << fields[i]
                }
            }
            line = dump.join(sep)

            Files.write(result, line.bytes, StandardOpenOption.APPEND)
            if( !line.endsWith("\n"))
                Files.write(result, "\n".bytes, StandardOpenOption.APPEND)
        }

        result
    }

    static List<Integer> columnsToPositions(Path source, List<String>trim, String sep){
        def brSource = new BufferedReader(new FileReader(source.toFile()))
        def headerSource = brSource.readLine()
        def columns = headerSource.split(sep) as List<String>
        trim.collect{ column->
            columnToPosition(column, columns)
        }
    }

    static Integer columnToPosition(String column, List<String> headers){
        if( column.isNumber() ){
            return column as int
        }
        headers.findIndexOf { it == column }
    }

}
