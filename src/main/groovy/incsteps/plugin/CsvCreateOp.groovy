package incsteps.plugin

import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.extension.CH
import nextflow.extension.DataflowHelper

class CsvCreateOp {

    private final Closure closure

    private final Map params

    private DataflowWriteChannel result

    private DataflowReadChannel channel

    CsvCreateOp(final DataflowReadChannel channel, Map params, final Closure closure = null ) {
        this.channel = channel
        this.result = CH.create()
        this.params = params
        this.closure = closure
    }

    List<String> data = []

    DataflowWriteChannel apply() {
        DataflowHelper.subscribeImpl( channel, [onNext: this.&processItem, onComplete: this.&emitItems] )
        return result
    }

    protected processItem( item ) {
        println "add to csv $item"
        data << item
    }

    protected emitItems( obj ) {
        println "escupe"
        data.each{d->
            if( closure ){
                d = closure.call(d)
            }
            result.bind(d)
        }
        result.bind(Channel.STOP)
    }
}
