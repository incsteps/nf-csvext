include { csv_sort } from 'plugin/nf-csvext'
import java.nio.file.Files

params.rows = 1_000
params.sort = 2

String generator(int n){
  String alphabet = (('A'..'Z')+('0'..'9')+('a'..'z')).join()
  new Random().with {
    (1..n).collect { alphabet[ nextInt( alphabet.length() ) ] }.join()
  }
}

Path generateRandom(){
    def f = Files.createTempFile("", "csv")
    f.deleteOnExit()
    f << "name, surname, quote\n"
    (0..(params.rows as int)).each{ idx->
        new Random().with {
            def n = generator( nextInt(4) )
            def m = generator( nextInt(5) )
            f << "$idx,$n,$m\n"
        }
    }
    f
}

def start = 0
def end = 0
workflow {
    def input = generateRandom()

    Channel.fromPath( input )
        | map{ source ->
            start = System.currentTimeMillis()
            def out = csv_sort( source, column:params.sort)
            end = System.currentTimeMillis()
            out
        }
        | splitCsv(header:true)
        | view
}

workflow.onComplete{
    println String.format("tooks %04.02f seconds to sort %d rows", (end-start)/1000 as float, params.rows)
}