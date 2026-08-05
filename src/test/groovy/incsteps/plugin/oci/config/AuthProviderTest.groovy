package incsteps.plugin.oci.config

import com.oracle.bmc.Region
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider
import incsteps.plugin.oci.nio.PrivKeyUtil
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.Unroll

class AuthProviderTest extends Specification{

    /**
     * Stands in for the two candidates that would otherwise need a real config file or a
     * reachable instance metadata service, so the auto-detection order can be asserted
     * without touching either. The overrides are stateless because they run from the
     * superclass constructor, before subclass fields are initialised.
     */
    static class StubbedProvider extends AuthentificationDetailProvider {

        static AbstractAuthenticationDetailsProvider instancePrincipal
        static Boolean failFastUsed
        static boolean configFileAvailable

        static void reset() {
            instancePrincipal = null
            failFastUsed = null
            configFileAvailable = false
        }

        StubbedProvider(Map opts, String region) {
            super(opts, region)
        }

        @Override
        protected AbstractAuthenticationDetailsProvider tryConfigFileProvider(Map opts) {
            return configFileAvailable ? super.tryConfigFileProvider(opts) : null
        }

        @Override
        protected AbstractAuthenticationDetailsProvider buildInstancePrincipalProvider(boolean failFast) {
            failFastUsed = failFast
            if (instancePrincipal == null)
                throw new RuntimeException("instance metadata service unreachable")
            return instancePrincipal
        }
    }

    def setup() {
        StubbedProvider.reset()
    }

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

    void "auto prefers the config file over the instance principal"(){
        given:
        StubbedProvider.configFileAvailable = true

        when:
        def detailProvider = new StubbedProvider([:], null)

        then:
        detailProvider.provider instanceof ConfigFileAuthenticationDetailsProvider
        and: 'the instance metadata service was never probed'
        StubbedProvider.failFastUsed == null
    }

    void "auto falls back to the instance principal when there is no config file"(){
        given:
        StubbedProvider.configFileAvailable = false
        StubbedProvider.instancePrincipal = Mock(AbstractAuthenticationDetailsProvider)

        when:
        def detailProvider = new StubbedProvider([:], null)

        then:
        detailProvider.provider === StubbedProvider.instancePrincipal
        and: 'auto-detection keeps the metadata probe short'
        StubbedProvider.failFastUsed
    }

    void "auto explains itself when no credentials can be found at all"(){
        given:
        StubbedProvider.configFileAvailable = false
        StubbedProvider.instancePrincipal = null

        when:
        new StubbedProvider([:], null)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("oci.authType")
    }

    void "explicit instance_principal does not shorten the metadata probe"(){
        given:
        StubbedProvider.instancePrincipal = Mock(AbstractAuthenticationDetailsProvider)

        when:
        def detailProvider = new StubbedProvider([authType:'instance_principal'], null)

        then:
        detailProvider.provider === StubbedProvider.instancePrincipal
        and: 'the user asked for it, so give the SDK its full retry budget'
        !StubbedProvider.failFastUsed
    }

    void "unknown authType is rejected"(){
        when:
        new AuthentificationDetailProvider([authType:'nonsense'], Region.US_PHOENIX_1.regionCode)

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    void "normalizes authType '#raw' to '#expected'"(){
        expect:
        AuthentificationDetailProvider.normalizeAuthType(raw) == expected

        where:
        raw                  | expected
        null                 | AuthentificationDetailProvider.AUTH_AUTO
        ''                   | AuthentificationDetailProvider.AUTH_AUTO
        'auto'               | AuthentificationDetailProvider.AUTH_AUTO
        'simple'             | AuthentificationDetailProvider.AUTH_SIMPLE
        'config_file'        | AuthentificationDetailProvider.AUTH_CONFIG_FILE
        'workload_identity'  | AuthentificationDetailProvider.AUTH_WORKLOAD_IDENTITY
        'WORKLOAD_IDENTITY'  | AuthentificationDetailProvider.AUTH_WORKLOAD_IDENTITY
        'instance_principal' | AuthentificationDetailProvider.AUTH_INSTANCE_PRINCIPAL
        ' Instance_Principal ' | AuthentificationDetailProvider.AUTH_INSTANCE_PRINCIPAL
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
