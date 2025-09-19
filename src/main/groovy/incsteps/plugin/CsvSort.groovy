package incsteps.plugin

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.file.Files
import java.nio.file.Path

@Slf4j
@CompileStatic
class CsvSort {

    static final int DEFAULT_CHUNK_SIZE = 1000
    static final String TEMP_PREFIX = "csv_sort_temp_"

    static class FileLineEntry {
        String line
        int fileIndex

        FileLineEntry(String line, int fileIndex) {
            this.line = line
            this.fileIndex = fileIndex
        }
    }


    static Path csv_sort(Path source, String sortBy, String sep=",", int chunkSize = DEFAULT_CHUNK_SIZE) {

        Path inputPath = Files.createTempFile(Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")
        inputPath.bytes = source.bytes
        inputPath.deleteOnExit()

        sortCSVByColumn(inputPath, sortBy, sep, chunkSize)
    }

    static Path sortCSVByColumn(Path inputPath, String sortBy, String sep=",", int chunkSize = DEFAULT_CHUNK_SIZE) {

        def outputPath = createOutputPath()
        def tempFiles = [] as List<File>

        try {
            String header = null
            def lineCount = 0
            def chunk = [] as List<String>

            int columnIndex = -1

            inputPath.newInputStream().withReader { reader ->
                header = reader.readLine()
                if (!header) {
                    throw new IllegalArgumentException("El archivo CSV está vacío")
                }

                columnIndex = validateColumnIndex(header, sortBy, sep)

                def line
                while ((line = reader.readLine()) != null) {
                    chunk << line
                    lineCount++

                    if (lineCount >= chunkSize) {
                        def tempFile = createSortedTempFile(chunk, columnIndex, sep)
                        tempFiles << tempFile

                        chunk.clear()
                        lineCount = 0
                    }
                }

                if (chunk) {
                    def tempFile = createSortedTempFile(chunk, columnIndex, sep)
                    tempFiles << tempFile
                }
            }

            mergeFiles(tempFiles, outputPath, header, columnIndex, sep)

            return outputPath

        }catch( Exception e){
            e.printStackTrace()
        } finally {
            cleanupTempFiles(tempFiles)
        }
    }

    private static Path createOutputPath() {
        Path result =
                Files.createTempFile( Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), "tmp", "csv")
        return result
    }

    private static int validateColumnIndex(String header, String columnIndex, String sep) {
        def headerColumns = parseCsvLine(header, sep)*.trim()
        if( columnIndex.isInteger() ) {
            int idx = columnIndex as int
            if (idx < 0 || idx >= headerColumns.size()) {
                throw new IllegalArgumentException("Invalid column index ${columnIndex}")
            }
            return idx
        }else{
            for(int idx=0; idx < headerColumns.size(); idx++){
                if( headerColumns[idx].equalsIgnoreCase(columnIndex) ){
                    return idx
                }
            }
            throw new IllegalArgumentException("Invalid column index ${columnIndex}")
        }
    }

    private static File createSortedTempFile(List<String> lines, int columnIndex, String sep) {

        lines.sort { line1, line2 -> compareLines(line1, line2, columnIndex, sep) }

        def tempFile = Files.createTempFile(Path.of(System.getenv('NXF_TEMP') ?: "/tmp"), TEMP_PREFIX, ".csv")
        tempFile.withWriter { writer ->
            lines.each { writer.println(it) }
        }

        return tempFile.toFile()
    }


    private static void mergeFiles(List<File> tempFiles, Path outputPath, String header, int columnIndex, String sep) {
        if (!tempFiles) {
            throw new IllegalArgumentException("No hay archivos temporales para fusionar")
        }

        if (tempFiles.size() == 1) {
            // Solo un archivo, copiarlo directamente
            outputPath.toFile().withWriter { writer ->
                writer.println(header)
                tempFiles[0].eachLine { writer.println(it) }
            }
            return
        }

        // Merge con priority queue
        def readers = [] as List<BufferedReader>
        def pq = new PriorityQueue<FileLineEntry>({FileLineEntry a, FileLineEntry b ->
            compareLines(a.line, b.line, columnIndex, sep)
        } as Comparator<FileLineEntry>)

        try {
            // Inicializar readers
            tempFiles.eachWithIndex { file, index ->
                def reader = file.newReader()
                readers << reader

                def line = reader.readLine()
                if (line) {
                    pq.offer(new FileLineEntry(line, index))
                }
            }

            // Escribir archivo fusionado
            outputPath.withWriter { writer ->
                writer.println(header)

                while (!pq.isEmpty()) {
                    def entry = pq.poll()
                    writer.println(entry.line)

                    def nextLine = readers[entry.fileIndex].readLine()
                    if (nextLine) {
                        pq.offer(new FileLineEntry(nextLine, entry.fileIndex))
                    }
                }
            }

        } finally {
            readers.each { reader ->
                try {
                    reader.close()
                } catch (Exception e) {
                    println "Exception ${e.message}"
                }
            }
        }
    }

    private static int compareLines(String line1, String line2, int columnIndex, String sep) {
        def cols1 = parseCsvLine(line1, sep)
        def cols2 = parseCsvLine(line2, sep)

        def val1 = cols1[columnIndex]?.toString()?.trim() ?: ""
        def val2 = cols2[columnIndex]?.toString()?.trim() ?: ""

        if( val1.isNumber() && val2.isNumber() ){
            return val1.toFloat() <=> val2.toFloat()
        }

        return val1 <=> val2
    }

    private static List<String> parseCsvLine(String line, String sep) {
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

    private static void cleanupTempFiles(List<File> tempFiles) {
        tempFiles.each { file ->
            try {
                if (file.exists() && !file.delete()) {
                    log.info "Error deleting : ${file.path}"
                }
            } catch (Exception e) {
                log.info "Error removing file: ${e.message}"
            }
        }
    }
}
