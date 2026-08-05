package incsteps.plugin.oci.config

import com.oracle.bmc.Region
import groovy.transform.CompileStatic

/**
 * Resolves an OCI region from a user supplied value.
 *
 * OCI names a region either by its short code (e.g. {@code lhr}) or by its full
 * region id (e.g. {@code uk-london-1}). The console, the CLI and {@code ~/.oci/config}
 * all use the id, so both spellings have to be accepted wherever a region is read
 * from configuration.
 */
@CompileStatic
class OciRegions {

    static Region of(String value) {
        if( !value )
            throw new IllegalArgumentException("Missing OCI region")
        try {
            return Region.fromRegionCodeOrId(value)
        }
        catch( IllegalArgumentException e ) {
            throw new IllegalArgumentException("Unknown OCI region '${value}' -- " +
                    "specify a region code (e.g. 'lhr') or a region id (e.g. 'uk-london-1')", e)
        }
    }
}
