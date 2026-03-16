package incsteps.plugin.oci.nio

import nextflow.Global
import nextflow.Session
import nextflow.SysEnv
import nextflow.file.FileHelper
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path

class FileHelperTest extends Specification{

    def setupSpec() {
        def CONFIG = [oci: [
                region: 'test'
        ]]
        Global.session = Mock(Session) { getConfig() >> CONFIG }
    }

    def cleanupSpec() {
        Global.session = null
    }

    @Unroll
    def 'should convert to canonical path with base' () {
        given:
        SysEnv.push(NXF_FILE_ROOT: 'oci://host.com/work')

        expect:
        FileHelper.toCanonicalPath(VALUE) == EXPECTED

        cleanup:
        SysEnv.pop()

        where:
        VALUE                       | EXPECTED
        null                        | null
        'file.txt'                  | FileHelper.asPath('oci://host.com/work/file.txt')
        Path.of('file.txt')         | FileHelper.asPath('oci://host.com/work/file.txt')

        and:
        './file.txt'                | FileHelper.asPath('oci://host.com/work/file.txt')
        '.'                         | FileHelper.asPath('oci://host.com/work')
        './'                        | FileHelper.asPath('oci://host.com/work')

        and:
        '/file.txt'                 | Path.of('/file.txt')
        Path.of('/file.txt')        | Path.of('/file.txt')
        '../file.txt'               | FileHelper.asPath('oci://host.com/file.txt')

    }

    def 'should convert to a canonical path' () {
        given:
        Global.session = Mock(Session) { getConfig() >> [:] }

        expect:
        FileHelper.toCanonicalPath(VALUE).toUri() == EXPECTED

        where:
        VALUE                       | EXPECTED
        'oci://foo/some/file.txt'    | new URI('oci:///foo/some/file.txt')
        'oci://foo/some///file.txt'  | new URI('oci:///foo/some/file.txt')
    }

    @Unroll
    def 'should remove consecutive slashes in the path' () {
        given:
        Global.session = Mock(Session) { getConfig() >> [:] }

        expect:
        FileHelper.asPath(STR).toUri() == EXPECTED
        where:
        STR                         | EXPECTED
        'oci://foo//this/that'       | new URI('oci:///foo/this/that')
        'oci://foo//this///that'     | new URI('oci:///foo/this/that')
    }
}
