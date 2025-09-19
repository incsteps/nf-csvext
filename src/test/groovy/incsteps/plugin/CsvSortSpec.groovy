package incsteps.plugin

import spock.lang.Specification

import java.nio.file.Files

class CsvSortSpec extends Specification{

    void "given a csv it order by numeric column"(){
        given:
        def f1 = Files.createTempFile("one",".csv")
        Files.writeString(f1,"""\
            name,surname,age
            a,b,12
            c,d,1
            e,f,8
            """.stripIndent())

        when:
        def f2 = CsvSort.csv_sort(f1, "2")

        then:
        Files.readString(f2) == """\
            name,surname,age
            c,d,1
            e,f,8
            a,b,12
            """.stripIndent()

        when:
        def f3 = CsvSort.csv_sort(f1, "age")

        then:
        Files.readString(f3) == """\
            name,surname,age
            c,d,1
            e,f,8
            a,b,12
            """.stripIndent()

        cleanup:
        f1?.delete()
        f2?.delete()
        f3?.delete()
    }

    void "given a csv it order by string column"(){
        given:
        def f1 = Files.createTempFile("one",".csv")
        Files.writeString(f1,"""\
            name,surname,age
            a,b,12
            c,d,1
            e,f,8
            """.stripIndent())

        when:
        def f2 = CsvSort.csv_sort(f1, "name")

        then:
        Files.readString(f2) == """\
            name,surname,age
            a,b,12
            c,d,1
            e,f,8
            """.stripIndent()

        cleanup:
        f1?.delete()
        f2?.delete()
    }

}
