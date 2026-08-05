package incsteps.plugin.oci.client

import incsteps.plugin.oci.config.OciConfig
import incsteps.plugin.oci.nio.PrivKeyUtil
import spock.lang.Specification
import spock.lang.Unroll

class OciClientTest extends Specification {

    private Map credentials(String region) {
        return [
                region     : region,
                tenantId   : 'test',
                userId     : 'test',
                fingerprint: 'test',
                privateKey : PrivKeyUtil.generatePrivateKeyPem()
        ]
    }

    @Unroll
    void "builds a client for region '#region'"() {
        when:
        new OciClient(new OciConfig(credentials(region)))

        then:
        noExceptionThrown()

        where:
        // a region id is what the OCI console, the CLI and ~/.oci/config all report,
        // so it has to work as well as the short code
        region << ['lhr', 'uk-london-1', 'eu-madrid-1', 'us-phoenix-1']
    }

    void "reports an unknown region clearly"() {
        when:
        new OciClient(new OciConfig(credentials('atlantis-1')))

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('atlantis-1')
    }
}
