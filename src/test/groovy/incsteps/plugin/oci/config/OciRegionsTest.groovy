package incsteps.plugin.oci.config

import com.oracle.bmc.Region
import spock.lang.Specification
import spock.lang.Unroll

class OciRegionsTest extends Specification {

    @Unroll
    void "resolves '#value' to #expected"() {
        expect:
        OciRegions.of(value) == expected

        where:
        value          | expected
        'lhr'          | Region.UK_LONDON_1
        'uk-london-1'  | Region.UK_LONDON_1
        'iad'          | Region.US_ASHBURN_1
        'us-ashburn-1' | Region.US_ASHBURN_1
        'phx'          | Region.US_PHOENIX_1
        'us-phoenix-1' | Region.US_PHOENIX_1
    }

    void "rejects a missing region"() {
        when:
        OciRegions.of(null)

        then:
        thrown(IllegalArgumentException)
    }

    void "rejects an unknown region with a message naming both spellings"() {
        when:
        OciRegions.of('not-a-region')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("not-a-region")
        e.message.contains("region id")
    }
}
