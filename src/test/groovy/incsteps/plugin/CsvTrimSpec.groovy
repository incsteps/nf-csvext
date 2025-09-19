package incsteps.plugin

import spock.lang.Specification

import java.nio.file.Files

class CsvTrimSpec extends Specification{

    void "given a csv it trim a column by numeric"(){
        given:
        def f1 = Files.createTempFile("one",".csv")
        Files.writeString(f1,"""\
            name,surname,age
            a,b,12
            c,d,1
            e,f,8
            """.stripIndent())

        when:
        def f2 = CsvTrim.csv_trim([column:'0'], f1)

        then:
        Files.readString(f2) == """\
            surname,age
            b,12
            d,1
            f,8
            """.stripIndent()

        when:
        def f3 = CsvTrim.csv_trim([column:'surname'], f1)

        then:
        Files.readString(f3) == """\
            name,age
            a,12
            c,1
            e,8
            """.stripIndent()

        when:
        def f4 = CsvTrim.csv_trim([columns:'0,surname'], f1)

        then:
        Files.readString(f4) == """\
            age
            12
            1
            8
            """.stripIndent()

        cleanup:
        f1?.delete()
        f2?.delete()
        f3?.delete()
    }

}
