package incsteps.plugin.oci.client


import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import incsteps.plugin.oci.config.OciConfig

@CompileStatic
@Slf4j
class OciClientFactory {

    private OciConfig config

    OciClientFactory(OciConfig config) {
        this.config = config
    }

    OciClient createOciClient() {
        new OciClient(config)
    }


}
