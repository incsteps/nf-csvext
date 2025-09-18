include { csv_sort } from 'plugin/nf-csvext'

params.sort = 'Cabin'

workflow {
    Channel.fromPath( 'data/titanic.tsv' )
        | map{ source ->
            def out = csv_sort( source, column:params.sort, sep:'\t')
            out
        }
        | splitCsv(header:true, sep:'\t')
        | view
}
