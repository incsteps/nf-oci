package incsteps.plugin.oci.nio

import groovy.transform.CompileStatic

import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

@CompileStatic
class OciFileAttributesView implements BasicFileAttributeView {

    private OciFileAttributes target

    OciFileAttributesView(OciFileAttributes target) {
        this.target = target
    }

    @Override
    public String name() {
        return "basic";
    }

    @Override
    public BasicFileAttributes readAttributes() throws IOException {
        return target;
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        // not supported
    }
}
