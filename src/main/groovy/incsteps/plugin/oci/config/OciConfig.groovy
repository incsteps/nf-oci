package incsteps.plugin.oci.config

import com.oracle.bmc.ConfigFileReader
import com.oracle.bmc.Region
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.SysEnv
import nextflow.config.schema.ConfigOption
import nextflow.config.schema.ConfigScope
import nextflow.script.dsl.Description


import java.nio.file.Path
import java.nio.file.Paths

@Slf4j
@CompileStatic
class OciConfig implements ConfigScope{

    final OciObjectStorageConfig objectStorageConfig

    @ConfigOption
    @Description("""
        Oci region (e.g. `us-east-1`).
    """)
    final String region

    @ConfigOption
    @Description("""
        Oci profile from `~/.oci/config`.
    """)
    final String profile

    OciConfig(){
        this([:])
    }

    OciConfig(Map opts){
        this.profile = getOciProfile0(SysEnv.get(), opts)
        this.region = getOciRegion(SysEnv.get(), opts)
        this.objectStorageConfig = new OciObjectStorageConfig( (Map)opts.storage ?: Collections.emptyMap())
    }

    String getDefaultRegion(){
        return region ?: Region.US_PHOENIX_1.regionCode
    }

    static protected String getOciProfile0(Map env, Map<String,Object> config) {

        final profile = config?.profile as String
        if( profile )
            return profile

        if( env?.containsKey('OCI_PROFILE'))
            return env.get('OCI_PROFILE')

        if( env?.containsKey('OCI_DEFAULT_PROFILE'))
            return env.get('OCI_DEFAULT_PROFILE')

        return null
    }


    static protected String getOciRegion(Map env, Map config) {

        def home = Paths.get(System.properties.get('user.home') as String)
        def file = home.resolve('.oci/config')

        return getOciRegion0(env, config, file)
    }

    static protected String getOciRegion0(Map env, Map config, Path file) {
        // check nxf config file
        if( config instanceof Map ) {
            def region = config.region
            if( region )
                return region.toString()
        }

        if( env && env.OCI_DEFAULT_REGION )  {
            return env.OCI_DEFAULT_REGION.toString()
        }

        if( !file.exists() ) {
            return null
        }

        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault()
        final ConfigFileAuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configFile)
        provider.region
    }

    static private OciConfig getConfig0(Map config) {
        if( config==null ) {
            log.warn("Missing nextflow session config object")
            return new OciConfig(Collections.emptyMap())
        }
        new OciConfig( (Map)config.aws ?: Collections.emptyMap()  )
    }

    static OciConfig config() {
        getConfig0(Global.config)
    }
}
