package incsteps.plugin.oci.config

import com.oracle.bmc.ConfigFileReader
import com.oracle.bmc.Region
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider
import groovy.transform.CompileStatic

import java.nio.charset.StandardCharsets

@CompileStatic
class AuthentificationDetailProvider {

    final AbstractAuthenticationDetailsProvider provider

    AuthentificationDetailProvider(Map otps, String region){
        provider = build(otps, region)
    }

    AbstractAuthenticationDetailsProvider getProvider() {
        this.provider
    }

    private AbstractAuthenticationDetailsProvider buildEnvProvider(Map otps, String region){
        if( !otps.containsKey("tenantId") )
            return null
        if( !otps.containsKey("userId") )
            return null
        if( !otps.containsKey("fingerprint") )
            return null
        if( !otps.containsKey("privateKey") )
            return null
        final String privKey = otps.get("privateKey").toString()
        return SimpleAuthenticationDetailsProvider.builder()
                .tenantId(otps.get("tenantId").toString())
                .userId(otps.get("userId").toString())
                .fingerprint(otps.get("fingerprint").toString())
                .region(Region.fromRegionCode(region))
                .privateKeySupplier(() -> {
                    return new ByteArrayInputStream(privKey.getBytes(StandardCharsets.UTF_8))
                })
                .build();
    }

    private AbstractAuthenticationDetailsProvider buildDefaultProvider(Map opts){
        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault(opts.get("profile")?.toString())
        final ConfigFileAuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configFile)
        return provider
    }

    private AbstractAuthenticationDetailsProvider build(Map opts, String region){
        def ret = buildEnvProvider(opts, region)
        if( !ret ){
            ret = buildDefaultProvider(opts)
        }
        return ret
    }

}
