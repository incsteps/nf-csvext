package incsteps.plugin

import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.extension.CH
import nextflow.extension.DataflowHelper

class CsvCreateOp {

    private final Closure closure

    private DataflowWriteChannel result

    private DataflowReadChannel channel

    final String sep
    final List<String>headerFields
    boolean headersEmitted = false

    CsvCreateOp(final DataflowReadChannel channel, List<String>headerFields, String sep, final Closure closure = null ) {
        this.channel = channel
        this.result = CH.create()
        this.closure = closure
        this.sep = sep
        this.headerFields = headerFields
    }

    DataflowWriteChannel apply() {
        DataflowHelper.subscribeImpl( channel, [onNext: this.&processItem, onComplete: this.&onComplete] )
        return result
    }


    protected processItem( item ) {
        if(!headersEmitted) {
            if (headerFields.size()) {
                String headerLine = headerFields.join(sep)
                result.bind(headerLine)
            }
            headersEmitted=true
        }
        if(closure){
            item = closure.call(item)
        }
        String line = "$item"
        if( item instanceof List){
            List list = (List)item
            line = list.join(sep)
        }else
            if( item instanceof Map){
                Map map = (Map)item
                line = headerFields.collect{
                    map[it] ?: ""
                }.join(sep)
            }
        result.bind(line)
    }

    protected onComplete( obj ) {
        result.bind(Channel.STOP)
    }
}
