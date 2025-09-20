package incsteps.plugin

import nextflow.Channel
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


class CsvCreateSpec extends Dsl2Spec{

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

    def 'should emit a simple csv' () {
        when:
        def SCRIPT = '''
            include { csv_create } from 'plugin/nf-csvext'
            channel.fromList([
                [id:1, name:'a name'],
                [id:2, name:'b name'],
                [id:3, name:'c name'],
            ])
                .csv_create(headers:['id','name'])
                .view()
            '''
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == 'id,name'
        result.val == '1,a name'
        result.val == '2,b name'
        result.val == '3,c name'
        result.val == Channel.STOP
    }

    def 'should emit a transformed csv' () {
        when:
        def SCRIPT = '''
            include { csv_create } from 'plugin/nf-csvext'
            channel.fromList([
                [id:1, name:'a name'],
                [id:2, name:'b name'],
                [id:3, name:'c name'],
            ])
                .csv_create(headers:['id','name','demo']){ it['demo'] = it.id*2; it}
                .view()
            '''
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == 'id,name,demo'
        result.val == '1,a name,2'
        result.val == '2,b name,4'
        result.val == '3,c name,6'
        result.val == Channel.STOP
    }

}
