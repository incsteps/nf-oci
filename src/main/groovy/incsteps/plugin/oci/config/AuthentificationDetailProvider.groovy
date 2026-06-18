package incsteps.plugin.oci.config

import com.oracle.bmc.ConfigFileReader
import com.oracle.bmc.Region
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider
import com.oracle.bmc.auth.okeworkloadidentity.OkeWorkloadIdentityAuthenticationDetailsProvider
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Resolves the OCI {@link AbstractAuthenticationDetailsProvider} used by the plugin.
 *
 * Supported authentication types (via the {@code oci.authType} config option or the
 * {@code OCI_AUTH_TYPE} environment variable):
 *
 * <ul>
 *   <li>{@code workload_identity} - OKE Workload Identity, for pods running in an
 *       Oracle Kubernetes Engine (enhanced) cluster. The recommended, credential-less
 *       method for Kubernetes workloads.</li>
 *   <li>{@code instance_principal} - Instance Principals, for processes running on any
 *       OCI compute instance, including OKE worker nodes.</li>
 *   <li>{@code resource_principal} - Resource Principals, for OCI Functions and similar
 *       resources.</li>
 *   <li>{@code simple} - explicit API key credentials supplied inline.</li>
 *   <li>{@code config_file} - the standard {@code ~/.oci/config} file.</li>
 *   <li>{@code auto} (default) - detect the most appropriate method for the current
 *       environment, preferring credential-less Kubernetes-friendly methods.</li>
 * </ul>
 */
@Slf4j
@CompileStatic
class AuthentificationDetailProvider {

    static final String AUTH_AUTO = 'auto'
    static final String AUTH_SIMPLE = 'simple'
    static final String AUTH_CONFIG_FILE = 'config_file'
    static final String AUTH_INSTANCE_PRINCIPAL = 'instance_principal'
    static final String AUTH_RESOURCE_PRINCIPAL = 'resource_principal'
    static final String AUTH_WORKLOAD_IDENTITY = 'workload_identity'

    /** Default Kubernetes service account token path mounted into every pod. */
    static final String DEFAULT_SA_TOKEN_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/token'

    final AbstractAuthenticationDetailsProvider provider

    AuthentificationDetailProvider(Map opts, String region) {
        provider = build(opts, region)
    }

    AbstractAuthenticationDetailsProvider getProvider() {
        this.provider
    }

    private AbstractAuthenticationDetailsProvider build(Map opts, String region) {
        final authType = normalizeAuthType(opts.get('authType'))
        switch (authType) {
            case AUTH_SIMPLE:
                return requireProvider(buildSimpleProvider(opts, region), AUTH_SIMPLE)
            case AUTH_CONFIG_FILE:
                return buildConfigFileProvider(opts)
            case AUTH_INSTANCE_PRINCIPAL:
                return buildInstancePrincipalProvider(region)
            case AUTH_RESOURCE_PRINCIPAL:
                return buildResourcePrincipalProvider()
            case AUTH_WORKLOAD_IDENTITY:
                return buildWorkloadIdentityProvider(opts, region)
            default:
                return autoDetect(opts, region)
        }
    }

    /**
     * Auto-detection order, biased towards credential-less methods so the plugin works
     * out of the box inside Kubernetes/OKE without shipping API keys into the container:
     * explicit inline credentials, then OKE Workload Identity, then Resource Principals,
     * then the local config file.
     */
    private AbstractAuthenticationDetailsProvider autoDetect(Map opts, String region) {
        if (hasSimpleCredentials(opts)) {
            log.debug("OCI auth: using inline API key credentials")
            return buildSimpleProvider(opts, region)
        }
        if (isWorkloadIdentityEnvironment(opts)) {
            log.debug("OCI auth: detected OKE Workload Identity environment")
            return buildWorkloadIdentityProvider(opts, region)
        }
        if (isResourcePrincipalEnvironment()) {
            log.debug("OCI auth: detected Resource Principal environment")
            return buildResourcePrincipalProvider()
        }
        log.debug("OCI auth: falling back to local config file")
        return buildConfigFileProvider(opts)
    }

    protected boolean hasSimpleCredentials(Map opts) {
        opts.get('tenantId') && opts.get('userId') && opts.get('fingerprint') && opts.get('privateKey')
    }

    /** True when running inside a Kubernetes pod with a mounted service account token. */
    protected boolean isWorkloadIdentityEnvironment(Map opts) {
        if (!System.getenv('KUBERNETES_SERVICE_HOST'))
            return false
        return Files.exists(Paths.get(serviceAccountTokenPath(opts)))
    }

    protected boolean isResourcePrincipalEnvironment() {
        System.getenv('OCI_RESOURCE_PRINCIPAL_VERSION') as boolean
    }

    private String serviceAccountTokenPath(Map opts) {
        opts.get('tokenPath')?.toString() ?:
                System.getenv('KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH') ?:
                        DEFAULT_SA_TOKEN_PATH
    }

    private AbstractAuthenticationDetailsProvider buildSimpleProvider(Map opts, String region) {
        if (!hasSimpleCredentials(opts))
            return null
        final String privKey = opts.get('privateKey').toString()
        final builder = SimpleAuthenticationDetailsProvider.builder()
                .tenantId(opts.get('tenantId').toString())
                .userId(opts.get('userId').toString())
                .fingerprint(opts.get('fingerprint').toString())
                .privateKeySupplier(() -> new ByteArrayInputStream(privKey.getBytes(StandardCharsets.UTF_8)))
        if (region)
            builder.region(Region.fromRegionCode(region))
        return builder.build()
    }

    private AbstractAuthenticationDetailsProvider buildConfigFileProvider(Map opts) {
        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault(opts.get('profile')?.toString())
        return new ConfigFileAuthenticationDetailsProvider(configFile)
    }

    private AbstractAuthenticationDetailsProvider buildInstancePrincipalProvider(String region) {
        // Instance Principals derive their region from the instance metadata service.
        return InstancePrincipalsAuthenticationDetailsProvider.builder().build()
    }

    private AbstractAuthenticationDetailsProvider buildResourcePrincipalProvider() {
        return ResourcePrincipalAuthenticationDetailsProvider.builder().build()
    }

    private AbstractAuthenticationDetailsProvider buildWorkloadIdentityProvider(Map opts, String region) {
        final builder = OkeWorkloadIdentityAuthenticationDetailsProvider.builder()
        final tokenPath = opts.get('tokenPath')?.toString()
        if (tokenPath)
            builder.tokenPath(tokenPath)
        if (region)
            builder.region(Region.fromRegionCode(region))
        return builder.build()
    }

    private static AbstractAuthenticationDetailsProvider requireProvider(AbstractAuthenticationDetailsProvider provider, String authType) {
        if (!provider)
            throw new IllegalStateException("OCI authType '${authType}' was requested but required credentials are missing")
        return provider
    }

    /** Normalizes user supplied auth type aliases to the canonical constants. */
    protected static String normalizeAuthType(Object raw) {
        if (!raw)
            return AUTH_AUTO
        final value = raw.toString().trim().toLowerCase().replace('-', '_')
        switch (value) {
            case ['', 'auto']:
                return AUTH_AUTO
            case ['simple', 'api_key']:
                return AUTH_SIMPLE
            case ['config_file', 'config', 'file']:
                return AUTH_CONFIG_FILE
            case ['instance_principal', 'instance_principals', 'instance']:
                return AUTH_INSTANCE_PRINCIPAL
            case ['resource_principal', 'resource_principals', 'resource']:
                return AUTH_RESOURCE_PRINCIPAL
            case ['workload_identity', 'oke_workload_identity', 'oke', 'workload']:
                return AUTH_WORKLOAD_IDENTITY
            default:
                throw new IllegalArgumentException("Unknown OCI authType '${raw}'. Valid values: " +
                        "auto, simple, config_file, instance_principal, resource_principal, workload_identity")
        }
    }
}
