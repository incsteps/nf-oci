package incsteps.plugin.oci.config

import com.oracle.bmc.Region
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider
import incsteps.plugin.oci.nio.PrivKeyUtil
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.Unroll

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
        provider instanceof SimpleAuthenticationDetailsProvider
    }

    void "explicit simple authType builds a simple provider"(){
        given:
        def config = [
                authType: 'simple',
                tenantId:'test',
                userId:'test',
                fingerprint:'test',
                privateKey: PrivKeyUtil.generatePrivateKeyPem()
        ]

        when:
        def detailProvider = new AuthentificationDetailProvider(config, Region.US_PHOENIX_1.regionCode)

        then:
        detailProvider.provider instanceof SimpleAuthenticationDetailsProvider
    }

    void "explicit simple authType fails when credentials are missing"(){
        when:
        new AuthentificationDetailProvider([authType:'simple'], Region.US_PHOENIX_1.regionCode)

        then:
        thrown(IllegalStateException)
    }

    void "unknown authType is rejected"(){
        when:
        new AuthentificationDetailProvider([authType:'nonsense'], Region.US_PHOENIX_1.regionCode)

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    void "normalizes authType alias '#alias' to '#expected'"(){
        expect:
        AuthentificationDetailProvider.normalizeAuthType(alias) == expected

        where:
        alias                    | expected
        null                     | AuthentificationDetailProvider.AUTH_AUTO
        ''                       | AuthentificationDetailProvider.AUTH_AUTO
        'auto'                   | AuthentificationDetailProvider.AUTH_AUTO
        'simple'                 | AuthentificationDetailProvider.AUTH_SIMPLE
        'api_key'                | AuthentificationDetailProvider.AUTH_SIMPLE
        'config_file'            | AuthentificationDetailProvider.AUTH_CONFIG_FILE
        'config'                 | AuthentificationDetailProvider.AUTH_CONFIG_FILE
        'instance_principal'     | AuthentificationDetailProvider.AUTH_INSTANCE_PRINCIPAL
        'instance-principal'     | AuthentificationDetailProvider.AUTH_INSTANCE_PRINCIPAL
        'resource_principal'     | AuthentificationDetailProvider.AUTH_RESOURCE_PRINCIPAL
        'workload_identity'      | AuthentificationDetailProvider.AUTH_WORKLOAD_IDENTITY
        'OKE'                    | AuthentificationDetailProvider.AUTH_WORKLOAD_IDENTITY
        'Workload-Identity'      | AuthentificationDetailProvider.AUTH_WORKLOAD_IDENTITY
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
