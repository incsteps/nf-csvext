include { csv_concat } from 'plugin/nf-csvext'

params.input = 'data/names.csv'
params.concat = 'data/concat_names.csv'
csv_file = file(params.input)

workflow {

    def out = csv_concat(csv_file, file(params.concat))

    Channel.fromPath(out) | splitCsv(header:true)| view
}