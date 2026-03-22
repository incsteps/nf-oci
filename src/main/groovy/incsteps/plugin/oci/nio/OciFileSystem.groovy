package incsteps.plugin.oci.nio

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import com.oracle.bmc.objectstorage.model.BucketSummary
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import incsteps.plugin.oci.client.OciClient

import java.nio.file.*
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

@CompileStatic
@Slf4j
class OciFileSystem extends FileSystem {

    private final OciFileSystemProvider provider;
    private final OciClient client;
    private final String endpoint;
    private final String bucketName;

    OciFileSystem(OciFileSystemProvider provider, OciClient client, URI uri) {
        this.provider = provider;
        this.client = client;
        this.endpoint = uri.getHost();
        this.bucketName = OciPath.bucketName(uri)
    }

    @Override
    FileSystemProvider provider() {
        return provider;
    }

    @Override
    void close() {
        this.provider.fileSystems.remove(bucketName);
    }

    @Override
    boolean isOpen() {
        return this.provider.fileSystems.containsKey(bucketName);
    }

    @Override
    boolean isReadOnly() {
        return false;
    }

    @Override
    String getSeparator() {
        return OciPath.PATH_SEPARATOR;
    }

    @Override
    Iterable<Path> getRootDirectories() {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();

        for (BucketSummary bucket : client.listBuckets()) {
            builder.add(new OciPath(this, bucket.name));
        }

        return builder.build();
    }

    @Override
    Iterable<FileStore> getFileStores() {
        return ImmutableList.of();
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        return ImmutableSet.of("basic");
    }

    @Override
    Path getPath(String first, String... more) {
        if (more.length == 0) {
            return new OciPath(this, first);
        }

        return new OciPath(this, first, more);
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException();
    }

    OciClient getClient() {
        return client;
    }

    String getEndpoint() {
        return endpoint;
    }

    String getBucketName() {
        return bucketName;
    }

}
