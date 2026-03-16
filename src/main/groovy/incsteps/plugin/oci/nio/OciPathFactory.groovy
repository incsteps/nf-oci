package incsteps.plugin.oci.nio

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.file.FileHelper
import nextflow.file.FileSystemPathFactory

import java.nio.file.Path

@Slf4j
@CompileStatic
class OciPathFactory extends FileSystemPathFactory {

    @Override
    protected Path parseUri(String str) {
        if( str.startsWith('oci://') && str[6]!='/' ) {
            final path = "oci:///${str.substring(6)}"
            return create(path)
        }
        return null
    }

    @Override
    protected String toUriString(Path path) {
        return path instanceof OciPath ? "oci:/$path".toString() : null
    }

    @Override
    protected String getBashLib(Path path) {
        ""
    }

    @Override
    protected String getUploadCmd(String s, Path path) {
        println("getUploadCmd not implemented (yet)")
        null
    }

    static private Map config() {
        final result = Global.config?.get('oci') as Map
        return result != null ? result : Collections.emptyMap()
    }

    static OciPath create(String path) {
        if( !path ) throw new IllegalArgumentException("Missing Oci path argument")
        if( !path.startsWith('oci:///') ) throw new IllegalArgumentException("Oci path must start with oci:/// prefix -- offending value '$path'")

        final uri = new URI(null,null, path,null,null)
        return (OciPath) FileHelper.getOrCreateFileSystemFor(uri,config()).provider().getPath(uri)
    }
}
