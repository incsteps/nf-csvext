include { csv_prettyprint } from 'plugin/nf-csvext'


Channel.fromPath( 'https://raw.githubusercontent.com/incsteps/nf-csvext/refs/heads/main/validation/data/titanic.tsv' )
    | map{ source ->
        csv_prettyprint( source, sep:'\t', newSep:';')
    }
    | map{ source ->
        file(source).text
    }
    | view
