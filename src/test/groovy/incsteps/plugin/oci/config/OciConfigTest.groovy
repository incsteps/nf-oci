package incsteps.plugin.oci.config

import com.oracle.bmc.Region
import spock.lang.Specification

class OciConfigTest extends Specification{

    void "given an empty config return a default region"(){
        given:
        def config = new OciConfig([:])
        when:
        def region = config.defaultRegion
        then:
        region == Region.US_PHOENIX_1.regionCode
    }

}
