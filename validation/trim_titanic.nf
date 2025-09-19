include { csv_trim } from 'plugin/nf-csvext'

params.trim = 'Cabin,Pclass'

workflow {
    Channel.fromPath( 'https://raw.githubusercontent.com/incsteps/nf-csvext/refs/heads/main/validation/data/titanic.tsv' )
        | map{ source ->
            csv_trim( source, column:params.trim, sep:'\t')
        }
        | splitCsv(header:true, sep:'\t')
        | view
}
