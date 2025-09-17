include { csv_concat } from 'plugin/nf-csvext'

params.rows = 10
params.concat = 'data/concat_names.csv'

process generate_csv{
    input:
    val rows

    output:
    file "random.csv"

    script:
    header = "id, name"
    cmd = (0..rows).collect{ "$it, name ${it}"}.join('\n')

    "cat <<EOF > random.csv\n$header\n$cmd\nEOF"
}


workflow {

    generate_csv(params.rows as int)
        | map{ source->
            csv_concat(source, file(params.concat), header:true)
        }
        | splitCsv
        | view

}