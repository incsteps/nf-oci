package incsteps.plugin.oci.nio

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Sets
import com.oracle.bmc.model.BmcException
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import incsteps.plugin.oci.client.OciClient
import incsteps.plugin.oci.client.OciClientFactory
import incsteps.plugin.oci.config.OciConfig
import nextflow.extension.FilesEx
import org.apache.commons.io.IOUtils

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.FileSystemNotFoundException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileTime
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.TimeUnit

import static com.google.common.collect.Sets.difference
import static java.lang.String.format

@CompileStatic
@Slf4j
class OciFileSystemProvider extends FileSystemProvider{

    OciObjectSummaryLookup objectSummaryLookup = new OciObjectSummaryLookup()
    final Map<String, OciFileSystem> fileSystems = new HashMap<>()

    static private URI key(String s, String a) {
        new URI("$s://$a")
    }

    static private URI key(URI uri) {
        final base = uri.authority
        return key(uri.scheme.toLowerCase(), base.toLowerCase())
    }

    @Override
    String getScheme() {
        "oci"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkArgument(uri.getScheme().equals("oci"), "uri scheme must be 'oci': '%s'", uri);

        final String bucketName = OciPath.bucketName(uri);
        synchronized (fileSystems) {
            if( fileSystems.containsKey(bucketName))
                throw new FileSystemAlreadyExistsException("Oci filesystem already exists. Use getFileSystem() instead");

            final OciConfig config = new OciConfig(env);

            final OciFileSystem result = createFileSystem(uri, config);
            fileSystems.put(bucketName, result);
            return result;
        }
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        final String bucketName = OciPath.bucketName(uri);
        final FileSystem fileSystem = this.fileSystems.get(bucketName);

        if (fileSystem == null) {
            throw new FileSystemNotFoundException("oci filesystem not yet created. Use newFileSystem() instead");
        }

        return fileSystem;
    }

    @Override
    Path getPath(URI uri) {
        Preconditions.checkArgument(uri.getScheme().equals(getScheme()),"URI scheme must be %s", getScheme());
        def path = uri.path
        return getFileSystem(uri).getPath(path)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        Preconditions.checkArgument(path instanceof OciPath,
                "path must be an instance of %s", OciPath.class.getName());
        final OciPath ociPath = (OciPath) path;
        // we resolve to a file inside the temp folder with the ociPath name
        final Path tempFile = createTempDir().resolve(path.getFileName().toString());

        try {
            InputStream is = ociPath.getFileSystem().getClient()
                    .getInputStream(ociPath.getBucket(), ociPath.getKey());

            if (is == null)
                throw new IOException(String.format("The specified path is a directory: %s", path));

            Files.write(tempFile, IOUtils.toByteArray(is));
        }
        catch (BmcException e) {
            if (e.statusCode != 404)
                throw new IOException(String.format("Cannot access file: %s", path),e);
        }

        final SeekableByteChannel seekable = Files.newByteChannel(tempFile, options);
        final String contentType = ((OciPath) path).contentType

        return new SeekableByteChannel() {
            @Override
            boolean isOpen() {
                return seekable.isOpen();
            }

            @Override
            void close() throws IOException {

                if (!seekable.isOpen()) {
                    return;
                }
                seekable.close();
                // upload the content where the seekable ends (close)
                if (Files.exists(tempFile)) {
                    try (InputStream stream = Files.newInputStream(tempFile)) {
                        ociPath.fileSystem
                                .client
                                .putObject(ociPath.bucket, ociPath.key, stream, [], contentType, Files.size(tempFile));
                    }
                }
                else {
                    ociPath.fileSystem
                            .client.deleteObject(ociPath.bucket, ociPath.key);
                }
                // and delete the temp dir
                Files.deleteIfExists(tempFile);
                Files.deleteIfExists(tempFile.getParent());
            }

            @Override
            int write(ByteBuffer src) throws IOException {
                return seekable.write(src);
            }

            @Override
            SeekableByteChannel truncate(long size) throws IOException {
                return seekable.truncate(size);
            }

            @Override
            long size() throws IOException {
                return seekable.size();
            }

            @Override
            int read(ByteBuffer dst) throws IOException {
                return seekable.read(dst);
            }

            @Override
            SeekableByteChannel position(long newPosition)
                    throws IOException {
                return seekable.position(newPosition);
            }

            @Override
            long position() throws IOException {
                return seekable.position();
            }
        }
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        Preconditions.checkArgument(dir instanceof OciPath,"path must be an instance of %s", OciPath.class.getName());
        final OciPath ociPath = (OciPath) dir;

        return new DirectoryStream<Path>() {
            @Override
            void close() throws IOException {
                // nothing to do here
            }

            @Override
            Iterator<Path> iterator() {
                return new OciIterator(ociPath.getFileSystem(), ociPath.getBucket(), ociPath.getKey() + "/");
            }
        };
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        Preconditions.checkArgument(dir instanceof OciPath,"path must be an instance of %s", OciPath.class.getName());
        OciPath ociPath = (OciPath)dir
        Preconditions.checkArgument(attrs.length == 0,
                "attrs not yet supported: %s", ImmutableList.copyOf(attrs))
        String keyName = ociPath.getKey() + (ociPath.getKey().endsWith("/") ? "" : "/");

        ociPath.fileSystem.client
                .putObject(ociPath.getBucket(), keyName, new ByteArrayInputStream(new byte[0]), [], null, 0);
    }

    @Override
    void delete(Path path) throws IOException {
        Preconditions.checkArgument(path instanceof OciPath,"path must be an instance of %s", OciPath.class.getName());
        OciPath ociPath = (OciPath)path
        ociPath.fileSystem.client.deleteObject(ociPath.bucket, ociPath.key)
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        Preconditions.checkArgument(source instanceof OciPath,
                "source must be an instance of %s", OciPath.class.getName());
        Preconditions.checkArgument(target instanceof OciPath,
                "target must be an instance of %s", OciPath.class.getName());

        if (isSameFile(source, target)) {
            return;
        }

        OciPath ociSource = (OciPath) source;
        OciPath ociTarget = (OciPath) target;
        ImmutableSet<CopyOption> actualOptions = ImmutableSet.copyOf(options);
        verifySupportedOptions(EnumSet.of(StandardCopyOption.REPLACE_EXISTING),
                actualOptions);

        if (!actualOptions.contains(StandardCopyOption.REPLACE_EXISTING)) {
            if (exists(ociTarget)) {
                throw new FileAlreadyExistsException(format(
                        "target already exists: %s", FilesEx.toUriString(ociTarget)));
            }
        }
        OciClient client = ociSource.fileSystem.client
        client.copyFile(ociSource.bucket, ociSource.key, ociTarget.bucket, ociTarget.key)
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        for( CopyOption it : options ) {
            if( it==StandardCopyOption.ATOMIC_MOVE )
                throw new IllegalArgumentException("Atomic move not supported by oci file system provider");
        }
        copy(source,target,options);
        delete(source);
    }

    @Override
    boolean isSameFile(Path path1, Path path2) throws IOException {
        return path1.isAbsolute() && path2.isAbsolute() && path1.equals(path2);
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        return false
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        Preconditions.checkArgument(path instanceof OciPath,
                "path must be an instance of %s", OciPath.class.getName());
        OciPath ociPath = (OciPath) path;
        Preconditions.checkArgument(ociPath.isAbsolute(), "path must be absolute: %s", ociPath);

        OciClient client = ociPath.fileSystem.client
        if( modes==null || modes.length==0 ) {
            // when no modes are given, the method is invoked
            // by `Files.exists` method, therefore just use summary lookup
            objectSummaryLookup.lookup(ociPath);
            return
        }
        //TODO find how to check ACL
    }

    @Override
    <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        Preconditions.checkArgument(path instanceof OciPath,
                "path must be an instance of %s", OciPath.class.getName());
        OciPath ociPath = (OciPath) path;
        if (type.isAssignableFrom(BasicFileAttributeView.class)) {
            try {
                return (V) new OciFileAttributesView(readAttr0(ociPath));
            }
            catch (IOException e) {
                throw new RuntimeException("Unable read attributes for file: " + FilesEx.toUriString(ociPath), e);
            }
        }
        log.trace("Unsupported oci file system provider file attribute view: " + type.getName());
        return null;
    }

    @Override
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        Preconditions.checkArgument(path instanceof OciPath,
                "path must be an instance of %s", OciPath.class.getName());
        OciPath ociPath = (OciPath) path;
        if (type.isAssignableFrom(BasicFileAttributes.class)) {
            return (A) ("".equals(ociPath.getKey())
                    // the root bucket is implicitly a directory
                    ? new OciFileAttributes("/", null, 0, true, false)
                    // read the target path attributes
                    : readAttr0(ociPath));
        }
        // not support attribute class
        throw new UnsupportedOperationException(format("only %s supported", BasicFileAttributes.class));
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    protected Properties loadOciProperties() {
        Properties props = new Properties();
        return props;
    }

    protected OciFileSystem createFileSystem(URI uri, OciConfig config){
        Properties props = loadOciProperties()

        final OciClientFactory clientFactory = new OciClientFactory(config, config.region)
        final OciClient client = new OciClient(clientFactory, props)
        return new OciFileSystem(this, client, uri, props);
    }

    protected Path createTempDir() throws IOException {
        return Files.createTempDirectory("temp-oci-");
    }

    private OciFileAttributes readAttr0(OciPath ociPath) throws IOException {
        OciPath.ObjectSummary objectSummary = objectSummaryLookup.lookup(ociPath)
        FileTime lastModifiedTime = null
        if( objectSummary.lastModified != null ) {
            lastModifiedTime = FileTime.from(objectSummary.lastModified.time, TimeUnit.MILLISECONDS)
        }
        long size =  objectSummary.size ?: 0
        boolean directory = false
        boolean regularFile = false
        String key = ociPath.key

        if (objectSummary.name.equals(ociPath.key + "/") && objectSummary.name.endsWith("/")) {
            directory = true;
        }
        // is a directory but does not exist
        else if ((!objectSummary.name.equals(ociPath.key) || "".equals(ociPath.key)) && objectSummary.name.startsWith(ociPath.key)){
            directory = true;
            size = 0;
            key = ociPath.key + "/";
        }
        // is a file:
        else {
            regularFile = true;
        }
        return new OciFileAttributes(key, lastModifiedTime, size, directory, regularFile)
    }

    private <T> void verifySupportedOptions(Set<? extends T> allowedOptions,
                                            Set<? extends T> actualOptions) {
        Sets.SetView<? extends T> unsupported = difference(actualOptions,
                allowedOptions);
        Preconditions.checkArgument(unsupported.isEmpty(),
                "the following options are not supported: %s", unsupported);
    }
}
