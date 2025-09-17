include { csv_concat } from 'plugin/nf-csvext'

params.input = 'data/names.csv'
params.concat = 'data/concat_names.csv'
csv_file = file(params.input)

workflow {

    Channel.fromPath( params.input)
        | map{ source ->
            csv_concat( source, file(params.concat), header:true )
        }
        | splitCsv(header:true)
        | view
}