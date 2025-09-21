package incsteps.plugin

import spock.lang.Specification

import java.nio.file.Files

class CsvConcatSpec extends Specification{

    void "given 2 csv with 2 lines it concatenates them"(){
        given:
        def f1 = Files.createTempFile("one",".csv")
        def f2 = Files.createTempFile("two",".csv")
        Files.writeString(f1,"name,surname\na,b")
        Files.writeString(f2,"name,surname\nc,d")

        when:
        def f3 = CsvConcat.csv_concat(f1, [f2], false, ",")
        then:
        Files.readAllLines(f3) == ["name,surname","a,b", "name,surname","c,d"]

        when:
        def f4 = CsvConcat.csv_concat(f1, [f2], true, ",")
        then:
        Files.readAllLines(f4) == ["name,surname","a,b","c,d"]

        cleanup:
        f1?.delete()
        f2?.delete()
        f3?.delete()
        f4?.delete()
    }

    void "given 3 csv with 2 lines it concatenates them"(){
        given:
        def f1 = Files.createTempFile("one",".csv")
        def f2 = Files.createTempFile("two",".csv")
        def f3 = Files.createTempFile("three",".csv")
        Files.writeString(f1,"name,surname\na,b")
        Files.writeString(f2,"name,surname\nc,d")
        Files.writeString(f3,"name,surname\ne,f")

        when:
        def f4 = CsvConcat.csv_concat(f1, [f2,f3], true, ",")
        then:
        Files.readAllLines(f4) == ["name,surname","a,b","c,d","e,f"]

        cleanup:
        f1?.delete()
        f2?.delete()
        f3?.delete()
        f4?.delete()
    }

    void "given 2 tsv with 2 lines it concatenates them"(){
        given:
        def f1 = Files.createTempFile("one",".csv")
        def f2 = Files.createTempFile("two",".csv")
        Files.writeString(f1,"name\tsurname\na\tb")
        Files.writeString(f2,"name\tsurname\nc\td")

        when:
        def f3 = CsvConcat.csv_concat(f1, [f2], false, ",")
        then:
        Files.readAllLines(f3) == ["name\tsurname","a\tb", "name\tsurname","c\td"]

        when:
        def f4 = CsvConcat.csv_concat(f1, [f2], true, ",")
        then:
        Files.readAllLines(f4) == ["name\tsurname","a\tb","c\td"]

        cleanup:
        f1?.delete()
        f2?.delete()
        f3?.delete()
        f4?.delete()
    }

}
