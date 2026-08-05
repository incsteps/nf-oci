package incsteps.plugin.oci.config

import com.oracle.bmc.ConfigFileReader
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider
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
 *   <li>{@code instance_principal} - the identity of the OCI compute instance the
 *       plugin runs on. The credential-less method outside Kubernetes.</li>
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
    static final String AUTH_INSTANCE_PRINCIPAL = 'instance_principal'

    /**
     * Auto-detection may run on a machine that is not an OCI instance at all, where the
     * instance metadata service is simply unroutable. Probe it briefly so that case fails
     * fast with a legible error instead of retrying for minutes.
     */
    private static final int AUTO_DETECT_RETRIES = 1
    private static final int AUTO_DETECT_TIMEOUT_MILLIS = 2_000

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
            case AUTH_INSTANCE_PRINCIPAL:
                return buildInstancePrincipalProvider(false)
            default:
                return autoDetect(opts, region)
        }
    }

    /**
     * Inline API key credentials when supplied, otherwise the local config file, otherwise
     * the instance principal. The instance principal comes last because it is the only
     * candidate that has to reach the network to be ruled out, and because a machine with
     * a config file has told us which identity to prefer.
     */
    private AbstractAuthenticationDetailsProvider autoDetect(Map opts, String region) {
        final simple = buildSimpleProvider(opts, region)
        if (simple)
            return simple

        final configFile = tryConfigFileProvider(opts)
        if (configFile)
            return configFile

        log.debug("No inline OCI credentials and no usable config file -- trying instance principal")
        try {
            return buildInstancePrincipalProvider(true)
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to determine OCI credentials: no inline API key " +
                    "was configured, no usable OCI config file was found, and this host is not an OCI " +
                    "instance. Set 'oci.authType' to select an authentication method explicitly.", e)
        }
    }

    /**
     * The config file provider, or {@code null} when there is no config file to read. A file
     * that exists but cannot be used is a misconfiguration, so those errors propagate.
     */
    protected AbstractAuthenticationDetailsProvider tryConfigFileProvider(Map opts) {
        try {
            return buildConfigFileProvider(opts)
        }
        catch (IOException e) {
            log.debug("No OCI config file available -- ${e.message}")
            return null
        }
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
            builder.region(OciRegions.of(region))
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
            builder.region(OciRegions.of(region))
        return builder.build()
    }

    /**
     * The instance principal derives both its credentials and its region from the instance
     * metadata service, so there is nothing to configure and no region to pass.
     *
     * @param failFast shorten the metadata probe, for use during auto-detection where this
     *      host may not be an OCI instance at all.
     */
    protected AbstractAuthenticationDetailsProvider buildInstancePrincipalProvider(boolean failFast) {
        final builder = InstancePrincipalsAuthenticationDetailsProvider.builder()
        if (failFast) {
            builder.detectEndpointRetries(AUTO_DETECT_RETRIES)
            builder.timeoutForEachRetry(AUTO_DETECT_TIMEOUT_MILLIS)
        }
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
            case AUTH_INSTANCE_PRINCIPAL:
                return AUTH_INSTANCE_PRINCIPAL
            default:
                throw new IllegalArgumentException("Unknown OCI authType '${raw}'. Valid values: " +
                        "auto, simple, config_file, workload_identity, instance_principal")
        }
    }
}
