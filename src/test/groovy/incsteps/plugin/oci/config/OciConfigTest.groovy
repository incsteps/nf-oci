package incsteps.plugin.oci.config

import spock.lang.Specification

class OciConfigTest extends Specification{

    void "given an empty config return a default region"(){
        given:
        def config = new OciConfig([:])
        when:
        def region = config.region
        then:
        region
    }

    void "authType is resolved from config"(){
        expect:
        OciConfig.getOciAuthType([:], [authType:'workload_identity']) == 'workload_identity'
    }

    void "authType falls back to OCI_AUTH_TYPE env"(){
        expect:
        OciConfig.getOciAuthType([OCI_AUTH_TYPE:'instance_principal'], [:]) == 'instance_principal'
    }

    void "config authType takes precedence over env"(){
        expect:
        OciConfig.getOciAuthType([OCI_AUTH_TYPE:'instance_principal'], [authType:'workload_identity']) == 'workload_identity'
    }

    void "authType defaults to null when unset"(){
        expect:
        OciConfig.getOciAuthType([:], [:]) == null
    }

}
