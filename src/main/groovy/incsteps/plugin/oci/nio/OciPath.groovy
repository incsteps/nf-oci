package incsteps.plugin.oci.nio

import com.google.common.base.*
import com.google.common.collect.ImmutableList
import com.google.common.collect.Lists
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.TagAwareFile

import javax.annotation.Nullable
import java.nio.file.*

import static com.google.common.collect.Iterables.*
import static java.lang.String.format

@CompileStatic
@Slf4j
class OciPath implements Path, TagAwareFile {

    static final String PATH_SEPARATOR = "/"

    private final String bucket
    private final List<String> parts
    private ObjectSummary summary
    private OciFileSystem fileSystem
    private String contentType
    private String storageClass

    OciPath(OciFileSystem fileSystem, String path) {
        this(fileSystem, path, "")
    }

    OciPath(OciFileSystem fileSystem, String first, String... more) {
        String bucket = null;
        List<String> parts = Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(first));

        if (first.endsWith(PATH_SEPARATOR)) {
            parts.remove(parts.size() - 1);
        }

        if (first.startsWith(PATH_SEPARATOR)) { // absolute path
            Preconditions.checkArgument(parts.size() >= 1,
                    "path must start with bucket name");
            Preconditions.checkArgument(!parts.get(1).isEmpty(),
                    "bucket name must be not empty");

            bucket = parts.get(1);

            if (!parts.isEmpty()) {
                parts = parts.subList(2, parts.size());
            }
        }

        if (bucket != null) {
            bucket = bucket.replace("/", "");
        }

        List<String> moreSplitted = Lists.newArrayList();

        for (String part : more) {
            moreSplitted.addAll(Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(part)));
        }

        parts.addAll(moreSplitted);


        this.bucket = bucket;
        this.parts = KeyParts.parse(parts);
        this.fileSystem = fileSystem;
    }

    private OciPath(OciFileSystem fileSystem, String bucket,
                    Iterable<String> keys) {
        this.bucket = bucket;
        this.parts = KeyParts.parse(keys);
        this.fileSystem = fileSystem;
    }


    String getBucket() {
        return bucket;
    }


    String getKey() {
        if (parts.isEmpty()) {
            return "";
        }

        ImmutableList.Builder<String> builder = ImmutableList
                .<String> builder().addAll(parts);

        return Joiner.on(PATH_SEPARATOR).join(builder.build());
    }


    @Override
    OciFileSystem getFileSystem() {
        return this.fileSystem;
    }

    @Override
    boolean isAbsolute() {
        return bucket != null;
    }

    @Override
    Path getRoot() {
        if (isAbsolute()) {
            return new OciPath(fileSystem, bucket, ImmutableList.<String> of());
        }

        return null;
    }

    @Override
    Path getFileName() {
        if (!parts.isEmpty()) {
            return new OciPath(fileSystem, null, parts.subList(parts.size() - 1,
                    parts.size()));
        } else {
            // bucket dont have fileName
            return null;
        }
    }

    @Override
    Path getParent() {
        // bucket is not present in the parts
        if (parts.isEmpty()) {
            return null;
        }

        if (parts.size() == 1 && (bucket == null || bucket.isEmpty())) {
            return null;
        }

        return new OciPath(fileSystem, bucket,
                parts.subList(0, parts.size() - 1));
    }

    @Override
    int getNameCount() {
        return parts.size();
    }

    @Override
    Path getName(int index) {
        return new OciPath(fileSystem, null, parts.subList(index, index + 1));
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        return new OciPath(fileSystem, null, parts.subList(beginIndex, endIndex));
    }

    @Override
    boolean startsWith(Path other) {

        if (other.getNameCount() > this.getNameCount()) {
            return false;
        }

        if (!(other instanceof OciPath)) {
            return false;
        }

        OciPath path = (OciPath) other;

        if (path.parts.size() == 0 && path.bucket == null &&
                (this.parts.size() != 0 || this.bucket != null)) {
            return false;
        }

        if ((path.getBucket() != null && !path.getBucket().equals(this.getBucket())) ||
                (path.getBucket() == null && this.getBucket() != null)) {
            return false;
        }

        for (int i = 0; i < path.parts.size(); i++) {
            if (!path.parts.get(i).equals(this.parts.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    boolean startsWith(String path) {
        OciPath other = new OciPath(this.fileSystem, path);
        return this.startsWith(other);
    }

    @Override
    boolean endsWith(Path other) {
        if (other.getNameCount() > this.getNameCount()) {
            return false;
        }
        // empty
        if (other.getNameCount() == 0 &&
                this.getNameCount() != 0) {
            return false;
        }

        if (!(other instanceof OciPath)) {
            return false;
        }

        OciPath path = (OciPath) other;

        if ((path.getBucket() != null && !path.getBucket().equals(this.getBucket())) ||
                (path.getBucket() != null && this.getBucket() == null)) {
            return false;
        }

        // check subkeys

        int i = path.parts.size() - 1;
        int j = this.parts.size() - 1;
        for (; i >= 0 && j >= 0;) {

            if (!path.parts.get(i).equals(this.parts.get(j))) {
                return false;
            }
            i--;
            j--;
        }
        return true;
    }

    @Override
    boolean endsWith(String other) {
        return this.endsWith(new OciPath(this.fileSystem, other));
    }

    @Override
    Path normalize() {
        if (parts == null || parts.size() == 0)
            return this;

        return new OciPath(fileSystem, bucket, normalize0(parts));
    }

    private Iterable<String> normalize0(List<String> parts) {
        final String s0 = Path.of(String.join(PATH_SEPARATOR, parts)).normalize().toString();
        return Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(s0));
    }

    @Override
    Path resolve(Path other) {
        Preconditions.checkArgument(other instanceof OciPath,
                "other must be an instance of %s", OciPath.class.getName());

        OciPath OciPath = (OciPath) other;

        if (OciPath.isAbsolute()) {
            return OciPath;
        }

        if (OciPath.parts.isEmpty()) { // other is relative and empty
            return this;
        }

        return new OciPath(fileSystem, bucket, concat(parts, OciPath.parts));
    }

    @Override
    Path resolve(String other) {
        return resolve(new OciPath(this.getFileSystem(), other));
    }

    @Override
    Path resolveSibling(Path other) {
        Preconditions.checkArgument(other instanceof OciPath,
                "other must be an instance of %s", OciPath.class.getName());

        OciPath OciPath = (OciPath) other;

        Path parent = getParent();

        if (parent == null || OciPath.isAbsolute()) {
            return OciPath;
        }

        if (OciPath.parts.isEmpty()) { // other is relative and empty
            return parent;
        }

        return new OciPath(fileSystem, bucket, concat(
                parts.subList(0, parts.size() - 1), OciPath.parts));
    }

    @Override
    Path resolveSibling(String other) {
        return resolveSibling(new OciPath(this.getFileSystem(), other));
    }

    @Override
    Path relativize(Path other) {
        Preconditions.checkArgument(other instanceof OciPath,
                "other must be an instance of %s", OciPath.class.getName());
        OciPath OciPath = (OciPath) other;

        if (this.equals(other)) {
            return new OciPath(this.getFileSystem(), "");
        }

        Preconditions.checkArgument(isAbsolute(),
                "Path is already relative: %s", this);
        Preconditions.checkArgument(OciPath.isAbsolute(),
                "Cannot relativize against a relative path: %s", OciPath);
        Preconditions.checkArgument(bucket.equals(OciPath.getBucket()),
                "Cannot relativize paths with different buckets: '%s', '%s'",
                this, other);

        Preconditions.checkArgument(parts.size() <= OciPath.parts.size(),
                "Cannot relativize against a parent path: '%s', '%s'",
                this, other);


        int startPart = 0;
        for (int i = 0; i < this.parts.size(); i++) {
            if (this.parts.get(i).equals(OciPath.parts.get(i))) {
                startPart++;
            }
        }

        List<String> resultParts = new ArrayList<>();
        for (int i = startPart; i < OciPath.parts.size(); i++) {
            resultParts.add(OciPath.parts.get(i));
        }

        return new OciPath(fileSystem, null, resultParts);
    }

    @Override
    URI toUri() {
        StringBuilder builder = new StringBuilder();
        builder.append("oci://");
        if (fileSystem.getEndpoint() != null) {
            builder.append(fileSystem.getEndpoint());
        }
        builder.append("/");
        builder.append(bucket);
        builder.append(PATH_SEPARATOR);
        builder.append(Joiner.on(PATH_SEPARATOR).join(parts));
        return URI.create(builder.toString());
    }

    @Override
    Path toAbsolutePath() {
        if (isAbsolute()) {
            return this;
        }

        throw new IllegalStateException(format(
                "Relative path cannot be made absolute: %s", this));
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    File toFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events,
                      WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    Iterator<Path> iterator() {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();

        for (Iterator<String> iterator = parts.iterator(); iterator.hasNext();) {
            String part = iterator.next();
            builder.add(new OciPath(fileSystem, null, ImmutableList.of(part)));
        }

        return builder.build().iterator();
    }

    @Override
    int compareTo(Path other) {
        return toString().compareTo(other.toString());
    }

    @Override
    String toString() {
        StringBuilder builder = new StringBuilder();

        if (isAbsolute()) {
            builder.append(PATH_SEPARATOR);
            builder.append(bucket);
            builder.append(PATH_SEPARATOR);
        }

        builder.append(Joiner.on(PATH_SEPARATOR).join(parts));

        return builder.toString();
    }

    @Override
    boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OciPath paths = (OciPath) o;

        if (bucket != null ? !bucket.equals(paths.bucket)
                : paths.bucket != null) {
            return false;
        }
        if (!parts.equals(paths.parts)) {
            return false;
        }

        return true;
    }

    @Override
    int hashCode() {
        int result = bucket != null ? bucket.hashCode() : 0;
        result = 33 * result + parts.hashCode();
        return result;
    }


    ObjectSummary fetchObject() {
        ObjectSummary result = summary;
        summary = null;
        return result;
    }

    void setObjectSummary(ObjectSummary map) {
        this.summary = map;
    }


    @Override
    void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    @Override
    void setContentType(String type) {
        this.contentType = type;
    }

    @Override
    void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    String getContentType() {
        return contentType;
    }

    String getStorageClass() {
        return storageClass;
    }

    private static Function<String, String> strip(final String... strs) {
        return new Function<String, String>() {
            String apply(String input) {
                String res = input;
                for (String str : strs) {
                    res = res.replace(str, "");
                }
                return res;
            }
        };
    }

    private static Predicate<String> notEmpty() {
        return new Predicate<String>() {
            @Override
            boolean apply(@Nullable String input) {
                return input != null && !input.isEmpty();
            }
        };
    }
    /*
     * delete redundant "/" and empty parts
     */

    private abstract static class KeyParts {

        private static ImmutableList<String> parse(List<String> parts) {
            return ImmutableList.copyOf(filter(transform(parts, strip("/")), notEmpty()));
        }

        private static ImmutableList<String> parse(Iterable<String> parts) {
            return ImmutableList.copyOf(filter(transform(parts, strip("/")), notEmpty()));
        }
    }

    static String bucketName(URI uri) {
        final String path = uri.getPath();
        if (path == null || !path.startsWith("/"))
            throw new IllegalArgumentException("Invalid Oci path: " + uri);
        final String[] parts = path.split("/");
        // note the element 0 contains the slash char
        return parts.length > 1 ? parts[1] : null;
    }

    static class ObjectSummary{
        String name
        Date lastModified
        Long size
    }
}