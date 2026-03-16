package incsteps.plugin.oci.client

import com.oracle.bmc.ConfigFileReader
import com.oracle.bmc.Region
import com.oracle.bmc.auth.AuthenticationDetailsProvider
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.http.client.jersey3.Jersey3HttpProvider
import com.oracle.bmc.objectstorage.ObjectStorageClient
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import incsteps.plugin.oci.config.OciConfig
import nextflow.SysEnv
import nextflow.exception.AbortOperationException

@CompileStatic
@Slf4j
class OciClientFactory {

    private OciConfig config
    private String region
    private String profile


    OciClientFactory(OciConfig config, String region=null) {
        this.config = config
        this.profile
                = config.profile
                ?: SysEnv.get('OCI_PROFILE')
                ?: SysEnv.get('OCI_DEFAULT_PROFILE')

        this.region
                = region
                ?: config.region
                ?: SysEnv.get('OCI_REGION')
                ?: SysEnv.get('OCI_DEFAULT_REGION')

        try {
            final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault()
            final ConfigFileAuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configFile)
            this.region = provider.region.regionId
        }catch(IOException ioe){}

        if( !this.region )
            throw new AbortOperationException('Missing OCI region -- Make sure to define in your system environment the variable `OCI_DEFAULT_REGION`')
    }

    private Region getRegionObj(String region) {
        final result = Region.fromRegionCode(region)
        if( !result )
            throw new IllegalArgumentException("Not a valid OCI region name: $region");
        return result
    }

    ObjectStorageClient getOciClient(){
        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault()
        final AuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configFile)
        final useRegion = region ?: configFile.get("region")
        return ObjectStorageClient.builder()
                .httpProvider(Jersey3HttpProvider.instance)
                .region(getRegionObj(useRegion))
                .build(provider)
    }

}
