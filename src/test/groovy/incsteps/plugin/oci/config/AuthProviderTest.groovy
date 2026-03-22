package incsteps.plugin.oci.config

import com.oracle.bmc.Region
import incsteps.plugin.oci.nio.PrivKeyUtil
import spock.lang.IgnoreIf
import spock.lang.Specification

class AuthProviderTest extends Specification{

    void "create simple from env"(){
        given:
        def config = [
                region: Region.US_PHOENIX_1,
                tenantId:'test',
                userId:'test',
                fingerprint:'test',
                privateKey: PrivKeyUtil.generatePrivateKeyPem()
        ]
        def detailProvider = new AuthentificationDetailProvider(config,Region.US_PHOENIX_1.regionCode)

        when:
        def provider = detailProvider.provider

        then:
        provider
    }

    @IgnoreIf({ !new File(System.getProperty("user.home")+"/.oci/config").exists() })
    void "create simple from file"(){
        given:
        def detailProvider = new AuthentificationDetailProvider([:],Region.US_PHOENIX_1.regionCode)

        when:
        def provider = detailProvider.provider

        then:
        provider
    }

}
