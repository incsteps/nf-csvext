include { csv_create } from 'plugin/nf-csvext'

process foo {
  input:
  path x
  output:
  stdout
  script:
  """
  echo foo recibe $x
  """
}

workflow {
  def ch = Channel.fromPath("$baseDir/data/*")
  foo(ch)
    .csv_create([a:'b'], { line->
        line.toUpperCase()
    })
    | view
}
