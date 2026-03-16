package incsteps.plugin.oci.nio

import com.oracle.bmc.objectstorage.model.ObjectSummary
import groovy.transform.CompileStatic
import incsteps.plugin.oci.client.OciClient

import java.nio.file.NoSuchFileException

@CompileStatic
class OciObjectSummaryLookup {

    OciPath.ObjectSummary lookup(OciPath ociPath)throws NoSuchFileException{

        def summary = ociPath.fetchObject()
        if( summary != null ){
            return summary
        }

        def client = ociPath.fileSystem.client
        if( "".equals(ociPath.key)){
            def metadata = client.getBucketMetadata(ociPath.bucket)
            if( metadata == null )
                throw new NoSuchFileException("oci://" + ociPath.bucket)

            return new OciPath.ObjectSummary()
        }

        OciPath.ObjectSummary item = getObject(ociPath, client)
        if( item != null )
            return item;

        throw new NoSuchFileException("oci://" + ociPath.getBucket() + "/" + ociPath.getKey());
    }

    OciPath.ObjectSummary getObject(OciPath ociPath, OciClient client){
        def list = client.listObjects(ociPath.bucket, ociPath.key)
        for(def item : list.listObjects.objects){
            if( matchName(ociPath.key, item)) {
                return new OciPath.ObjectSummary(
                        name: item.name,
                        lastModified: item.timeModified,
                        size: item.size
                );
            }
        }
        null
    }

    private boolean matchName(String fileName, ObjectSummary summary) {
        String foundKey = summary.name;

        // they are different names return false
        if( !foundKey.startsWith(fileName) ) {
            return false;
        }

        // when they are the same length, they are identical
        if( foundKey.length() == fileName.length() )
            return true;

        return foundKey.charAt(fileName.length()) == '/';
    }
}
