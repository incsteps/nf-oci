package incsteps.plugin.oci.config

import com.oracle.bmc.ConfigFileReader
import com.oracle.bmc.Region
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider
import com.oracle.bmc.auth.okeworkloadidentity.OkeWorkloadIdentityAuthenticationDetailsProvider
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.charset.StandardCharsets

/**
 * Resolves the OCI {@link AbstractAuthenticationDetailsProvider} used by the plugin.
 *
 * The method is selected via the {@code oci.authType} config option (or the
 * {@code OCI_AUTH_TYPE} environment variable):
 *
 * <ul>
 *   <li>{@code auto} (default) - inline API key credentials when supplied, otherwise
 *       the local {@code ~/.oci/config} file.</li>
 *   <li>{@code workload_identity} - OKE Workload Identity, for pods running in an
 *       Oracle Kubernetes Engine (enhanced) cluster. The credential-less method for
 *       Kubernetes workloads.</li>
 *   <li>{@code simple} - explicit API key credentials supplied inline.</li>
 *   <li>{@code config_file} - the standard {@code ~/.oci/config} file.</li>
 * </ul>
 */
@Slf4j
@CompileStatic
class AuthentificationDetailProvider {

    static final String AUTH_AUTO = 'auto'
    static final String AUTH_SIMPLE = 'simple'
    static final String AUTH_CONFIG_FILE = 'config_file'
    static final String AUTH_WORKLOAD_IDENTITY = 'workload_identity'

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
            case AUTH_WORKLOAD_IDENTITY:
                return buildWorkloadIdentityProvider(opts, region)
            default:
                return autoDetect(opts, region)
        }
    }

    /** Inline API key credentials when supplied, otherwise the local config file. */
    private AbstractAuthenticationDetailsProvider autoDetect(Map opts, String region) {
        return buildSimpleProvider(opts, region) ?: buildConfigFileProvider(opts)
    }

    protected boolean hasSimpleCredentials(Map opts) {
        opts.get('tenantId') && opts.get('userId') && opts.get('fingerprint') && opts.get('privateKey')
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

    private AbstractAuthenticationDetailsProvider buildWorkloadIdentityProvider(Map opts, String region) {
        final builder = OkeWorkloadIdentityAuthenticationDetailsProvider.builder()
        // The OKE SDK reads the service account token from the standard pod mount path
        // by default; only override it when the user points us elsewhere.
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

    /** Resolves the configured auth type to one of the supported canonical values. */
    protected static String normalizeAuthType(Object raw) {
        if (!raw)
            return AUTH_AUTO
        final value = raw.toString().trim().toLowerCase()
        switch (value) {
            case AUTH_AUTO:
                return AUTH_AUTO
            case AUTH_SIMPLE:
                return AUTH_SIMPLE
            case AUTH_CONFIG_FILE:
                return AUTH_CONFIG_FILE
            case AUTH_WORKLOAD_IDENTITY:
                return AUTH_WORKLOAD_IDENTITY
            default:
                throw new IllegalArgumentException("Unknown OCI authType '${raw}'. Valid values: " +
                        "auto, simple, config_file, workload_identity")
        }
    }
}
