package incsteps.plugin.oci.nio

import com.google.common.base.Preconditions
import com.oracle.bmc.objectstorage.responses.ListObjectsResponse
import groovy.transform.CompileStatic
import incsteps.plugin.oci.client.OciClient

import java.nio.file.Path

@CompileStatic
class OciIterator implements Iterator<Path> {

    private OciFileSystem fileSystem
    private String bucket
    private String key

    private Iterator<OciPath> iterator

    OciIterator(OciFileSystem fileSystem, String bucket, String key) {
        Preconditions.checkArgument(key != null && key.endsWith("/"), "key %s should be ended with slash '/'", key);

        this.bucket = bucket
        this.key = !key || key.length() == 1 ? "" : key
        this.fileSystem = fileSystem
    }

    @Override
    void remove() {
        throw new UnsupportedOperationException()
    }

    @Override
    OciPath next() {
        return getIterator().next()
    }

    @Override
    boolean hasNext() {
        return getIterator().hasNext()
    }

    private Iterator<OciPath> getIterator() {
        if (iterator == null) {
            OciClient ociClient = fileSystem.getClient();

            iterator = ociClient.listObjects(bucket, key)
                    .collect((ListObjectsResponse r) -> parseObjectListing(r))
                    .collectMany([],{it})
                    .iterator()
        }
        iterator
    }

    private List<OciPath> parseObjectListing(ListObjectsResponse current) {
        List<OciPath> listPath = new ArrayList<>()
        for (final def objectSummary : current.listObjects.objects) {
            final String key = objectSummary.name
            if (this.key.equals(key)) continue
            final OciPath path = new OciPath(fileSystem, "/" + bucket, key.split("/"));
            path.setObjectSummary(new OciPath.ObjectSummary(
                    name: objectSummary.name,
                    size: objectSummary.size,
                    lastModified: objectSummary.timeModified,
            ));
            listPath.add(path);
        }
        return listPath;
    }
}
