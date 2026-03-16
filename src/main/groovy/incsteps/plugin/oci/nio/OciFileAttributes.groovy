package incsteps.plugin.oci.nio

import groovy.transform.CompileStatic

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

import static java.lang.String.format

@CompileStatic
class OciFileAttributes implements BasicFileAttributes {
    private final FileTime lastModifiedTime;
    private final long size;
    private final boolean directory;
    private final boolean regularFile;
    private final String key;

    OciFileAttributes(String key, FileTime lastModifiedTime, long size,
                      boolean isDirectory, boolean isRegularFile) {
        this.key = key;
        this.lastModifiedTime = lastModifiedTime;
        this.size = size;
        directory = isDirectory;
        regularFile = isRegularFile;
    }

    @Override
    FileTime lastModifiedTime() {
        return lastModifiedTime;
    }

    @Override
    FileTime lastAccessTime() {
        return lastModifiedTime;
    }

    @Override
    FileTime creationTime() {
        return lastModifiedTime;
    }

    @Override
    boolean isRegularFile() {
        return regularFile;
    }

    @Override
    boolean isDirectory() {
        return directory;
    }

    @Override
    boolean isSymbolicLink() {
        return false;
    }

    @Override
    boolean isOther() {
        return false;
    }

    @Override
    long size() {
        return size;
    }

    @Override
    Object fileKey() {
        return key;
    }

    @Override
    String toString() {
        return format(
                "[%s: lastModified=%s, size=%s, isDirectory=%s, isRegularFile=%s]",
                key, lastModifiedTime, size, directory, regularFile);
    }
}
