package incsteps.plugin.oci.config

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.config.schema.ConfigScope

@Slf4j
@CompileStatic
class OciObjectStorageConfig implements ConfigScope{

    OciObjectStorageConfig(Map opts) {
    }

}
