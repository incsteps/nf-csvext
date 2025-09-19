package incsteps.plugin

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class CsvTrim {

    static Path csv_trim(Map params=[:], Path source) {
        Path inputPath = Files.createTempFile(Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")
        inputPath.bytes = source.bytes
        inputPath.deleteOnExit()

        csvTrim(params, inputPath)
    }

    static private Path csvTrim(Map params=[:], Path source) {
        validateInputs(source, params)

        List<String> columns = params.containsKey("column") ?
                [params.column] as List<String>
                :
                params.columns.toString().split(",") as List<String>

        String sep = (params.sep ?: ",").toString()

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

    static void validateInputs(Path source, Map params=[:]) {
        if (!params.containsKey("column") && !params.containsKey("columns")) {
            throw new IllegalArgumentException("column(s) to trim are required")
        }
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
