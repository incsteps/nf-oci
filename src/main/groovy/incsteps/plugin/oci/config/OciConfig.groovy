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

    final AuthentificationDetailProvider authentificationProvider
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

    @ConfigOption
    @Description("""
        Authentication method to use. One of `auto` (default, uses inline API key
        credentials when supplied otherwise `~/.oci/config`), `workload_identity`
        (OKE Workload Identity, recommended for Kubernetes pods), `simple`
        (inline API key) or `config_file` (`~/.oci/config`).
    """)
    final String authType

    @ConfigOption
    @Description("""
        Path to the Kubernetes service account token used by `workload_identity`
        authentication. Defaults to the standard OKE pod mount path.
    """)
    final String tokenPath

    OciConfig(){
        this([:])
    }

    OciConfig(Map opts){
        this.profile = getOciProfile0(SysEnv.get(), opts)
        this.region = getOciRegion(SysEnv.get(), opts)
        this.authType = getOciAuthType(SysEnv.get(), opts)
        this.tokenPath = opts.tokenPath as String
        this.objectStorageConfig = new OciObjectStorageConfig( (Map)opts.storage ?: Collections.emptyMap())
        // make the resolved auth settings visible to the provider regardless of their source
        final Map authOpts = new LinkedHashMap(opts)
        authOpts.authType = authType
        this.authentificationProvider = new AuthentificationDetailProvider(authOpts, region)
    }

    AuthentificationDetailProvider getAuthentificationProvider(){
        this.authentificationProvider
    }

    String getRegion(){
        return region ?: Region.US_PHOENIX_1.regionCode
    }

    /** The region explicitly set via config/env, or {@code null} if unset. */
    String getConfiguredRegion(){
        return region
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


    static protected String getOciAuthType(Map env, Map<String,Object> config) {

        final authType = config?.authType as String
        if( authType )
            return authType

        if( env?.containsKey('OCI_AUTH_TYPE'))
            return env.get('OCI_AUTH_TYPE')

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
        new OciConfig( (Map)config.oci ?: Collections.emptyMap()  )
    }

    static OciConfig config() {
        getConfig0(Global.config)
    }
}
