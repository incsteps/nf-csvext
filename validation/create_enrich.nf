include { csv_create } from 'plugin/nf-csvext'

channel.fromList([
    [id:1, name:'a name'],
    [id:2, name:'b name'],
    [id:3, name:'c name'],
])
    .csv_create( headers:['name','id','date'], sep:";"){ sequence->
        sequence['date'] = new Date().toString()
        sequence
    }
    .view()