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


    static Path sortCSVByColumn(Path inputPath, int columnIndex, int chunkSize = DEFAULT_CHUNK_SIZE) {

        def outputPath = createOutputPath()
        def tempFiles = [] as List<File>

        try {
            String header = null
            def lineCount = 0
            def chunk = [] as List<String>

            // Fase 1: Dividir en chunks y ordenar cada uno
            inputPath.toFile().withReader { reader ->
                header = reader.readLine()
                if (!header) {
                    throw new IllegalArgumentException("El archivo CSV está vacío")
                }

                validateColumnIndex(header, columnIndex)

                def line
                while ((line = reader.readLine()) != null) {
                    chunk << line
                    lineCount++

                    if (lineCount >= chunkSize) {
                        def tempFile = createSortedTempFile(chunk, columnIndex)
                        tempFiles << tempFile

                        chunk.clear()
                        lineCount = 0
                    }
                }

                // Procesar último chunk
                if (chunk) {
                    def tempFile = createSortedTempFile(chunk, columnIndex)
                    tempFiles << tempFile
                }
            }

            // Fase 2: Merge de archivos temporales
            mergeFiles(tempFiles, outputPath, header, columnIndex)

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

    private static void validateColumnIndex(String header, int columnIndex) {
        def headerColumns = parseCsvLine(header)
        if (columnIndex < 0 || columnIndex >= headerColumns.size()) {
            throw new IllegalArgumentException("Índice de columna inválido: ${columnIndex}. El archivo tiene ${headerColumns.size()} columnas.")
        }
    }

    private static File createSortedTempFile(List<String> lines, int columnIndex) {
        // Ordenar chunk en memoria
        lines.sort { line1, line2 -> compareLines(line1, line2, columnIndex) }

        // Crear archivo temporal
        def tempFile = File.createTempFile(TEMP_PREFIX, ".csv")
        tempFile.withWriter { writer ->
            lines.each { writer.println(it) }
        }

        return tempFile
    }


    private static void mergeFiles(List<File> tempFiles, Path outputPath, String header, int columnIndex) {
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
            compareLines(a.line, b.line, columnIndex)
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

    private static int compareLines(String line1, String line2, int columnIndex) {
        def cols1 = parseCsvLine(line1)
        def cols2 = parseCsvLine(line2)

        if (cols1.size() <= columnIndex || cols2.size() <= columnIndex) {
            return 0
        }

        def val1 = cols1[columnIndex]?.toString()?.trim() ?: ""
        def val2 = cols2[columnIndex]?.toString()?.trim() ?: ""

        // Comparación numérica si es posible
        try {
            return val1.toDouble() <=> val2.toDouble()
        } catch (NumberFormatException e) {
            return val1.compareToIgnoreCase(val2)
        }
    }

    private static List<String> parseCsvLine(String line) {
        def fields = [] as List<String>
        def inQuotes = false
        def currentField = new StringBuilder()

        line.chars.each { char c ->
            if (c == '"') {
                inQuotes = !inQuotes
            } else if (c == ',' && !inQuotes) {
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
