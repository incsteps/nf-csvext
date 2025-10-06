package incsteps.plugin

import spock.lang.Specification

import java.nio.file.Files

class CsvPrettySpec extends Specification{

    void "given a csv it adjust columns"(){
        given:
        def f1 = Files.createTempFile("one",".csv")
        Files.writeString(f1,"""\
            name,surname,age
            a,b,12
            c,d,1
            e,f,8
            """.stripIndent())

        when:
        def f2 = CsvPretty.pretty_csv(f1,',',',')

        then:
        Files.readString(f2).stripIndent() == """\
            name,surname,age
            a   ,b      ,12 
            c   ,d      ,1  
            e   ,f      ,8  
            """.stripIndent()

        cleanup:
        f1?.delete()
        f2?.delete()
    }

}
