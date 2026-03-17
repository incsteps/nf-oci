package incsteps.plugin.oci.config

import spock.lang.Specification

class OciConfigTest extends Specification{

    void "given an empty config return a default region"(){
        given:
        def config = new OciConfig([:])
        when:
        def region = config.defaultRegion
        then:
        region
    }

}
