package incsteps.plugin

import nextflow.Channel
import nextflow.exception.AbortRunException
import nextflow.plugin.Plugins
import nextflow.plugin.TestPluginDescriptorFinder
import nextflow.plugin.TestPluginManager
import nextflow.plugin.extension.PluginExtensionProvider
import org.pf4j.PluginDescriptorFinder
import spock.lang.Shared
import test.Dsl2Spec
import test.MockScriptRunner

import java.nio.file.Files
import java.nio.file.Path
import java.util.jar.Manifest


class CsvExtensionSpec extends Dsl2Spec{

    @Shared String pluginsMode

    def setup() {
// reset previous instances
        PluginExtensionProvider.reset()
        // this need to be set *before* the plugin manager class is created
        pluginsMode = System.getProperty('pf4j.mode')
        System.setProperty('pf4j.mode', 'dev')
        // the plugin root should
        def root = Path.of('.').toAbsolutePath().normalize()
        def manager = new TestPluginManager(root){
            @Override
            protected PluginDescriptorFinder createPluginDescriptorFinder() {
                return new TestPluginDescriptorFinder(){
                    @Override
                    protected Manifest readManifestFromDirectory(Path pluginPath) {
                        def manifestPath= getManifestPath(pluginPath)
                        final input = Files.newInputStream(manifestPath)
                        return new Manifest(input)
                    }
                    protected Path getManifestPath(Path pluginPath) {
                        return pluginPath.resolve('build/tmp/jar/MANIFEST.MF')
                    }
                }
            }
        }
        Plugins.init(root, 'dev', manager)
    }

    def cleanup() {
        Plugins.stop()
        PluginExtensionProvider.reset()
        pluginsMode ? System.setProperty('pf4j.mode',pluginsMode) : System.clearProperty('pf4j.mode')
    }

    def 'should concat two csv' () {
        given:
        def f1 = Files.createTempFile("",".csv")
        f1 << "name,surname\n"
        f1 << "a,b"

        def f2 = Files.createTempFile("",".csv")
        f2 << "name,surname\n"
        f2 << "c,d"

        when:
        def SCRIPT = """
            include { csv_concat } from 'plugin/nf-csvext\'            
            def f1 = '${f1.toAbsolutePath()}'
            def f2 = '${f2.toAbsolutePath()}'
                       
            Channel.fromPath( f1 )
                .map{ source ->
                    csv_concat( source, file(f2), header:true )
                }
                .splitCsv()
                .view()                                        
            """
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == ['name', 'surname']
        result.val == ['a', 'b']
        result.val == ['c', 'd']
        result.val == Channel.STOP
    }


    def 'should trim a column by position in a csv' () {
        given:
        def f1 = Files.createTempFile("",".csv")
        f1 << "name,surname\n"
        f1 << "a,b\n"
        f1 << "c,d\n"

        when:
        def SCRIPT = """
            include { csv_trim } from 'plugin/nf-csvext\'            
            def f1 = '${f1.toAbsolutePath()}'
                       
            Channel.fromPath( f1 )
                .map{ source ->
                    csv_trim( source, column:'0' )
                }
                .splitCsv()
                .view()                                        
            """
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == ['surname']
        result.val == ['b']
        result.val == ['d']
        result.val == Channel.STOP
    }

    def 'should trim a column by name in a csv' () {
        given:
        def f1 = Files.createTempFile("",".csv")
        f1 << "name,surname\n"
        f1 << "a,b\n"
        f1 << "c,d\n"

        when:
        def SCRIPT = """
            include { csv_trim } from 'plugin/nf-csvext\'            
            def f1 = '${f1.toAbsolutePath()}'
                       
            Channel.fromPath( f1 )
                .map{ source ->
                    csv_trim( source, column:'surname' )
                }
                .splitCsv()
                .view()                                        
            """
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == ['name']
        result.val == ['a']
        result.val == ['c']
        result.val == Channel.STOP
    }

    def 'should trim many columns in a csv' () {
        given:
        def f1 = Files.createTempFile("",".csv")
        f1 << "name,surname,date\n"
        f1 << "a,b,1992\n"
        f1 << "c,d,1991\n"

        when:
        def SCRIPT = """
            include { csv_trim } from 'plugin/nf-csvext\'            
            def f1 = '${f1.toAbsolutePath()}'
                       
            Channel.fromPath( f1 )
                .map{ source ->
                    csv_trim( source, columns:['name','surname'] )
                }
                .splitCsv()
                .view()                                        
            """
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == ['date']
        result.val == ['1992']
        result.val == ['1991']
        result.val == Channel.STOP
    }

    def 'should validate column and columns are not valid in trim' () {
        given:
        def f1 = Files.createTempFile("",".csv")
        f1 << "name,surname,date\n"
        f1 << "a,b,1992\n"
        f1 << "c,d,1991\n"

        when:
        def SCRIPT = """
            include { csv_trim } from 'plugin/nf-csvext\'            
            def f1 = '${f1.toAbsolutePath()}'
                       
            Channel.fromPath( f1 )
                .map{ source ->
                    csv_trim( source, column:0, columns:['name','surname'] )
                }
                .splitCsv()
                .view()                                        
            """
        and:
        new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        thrown(AbortRunException)
    }

}
