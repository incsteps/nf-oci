package incsteps.plugin.oci.client

import com.oracle.bmc.objectstorage.ObjectStorage
import com.oracle.bmc.objectstorage.model.BucketSummary
import com.oracle.bmc.objectstorage.model.CopyObjectDetails
import com.oracle.bmc.objectstorage.requests.*
import com.oracle.bmc.objectstorage.responses.CopyObjectResponse
import com.oracle.bmc.objectstorage.responses.DeleteObjectResponse
import com.oracle.bmc.objectstorage.responses.GetBucketResponse
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse
import com.oracle.bmc.objectstorage.responses.ListBucketsResponse
import com.oracle.bmc.objectstorage.responses.ListObjectsResponse
import com.oracle.bmc.objectstorage.responses.PutObjectResponse
import com.oracle.bmc.requests.BmcRequest
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import incsteps.plugin.oci.OciPlugin
import nextflow.util.Threads

import java.util.concurrent.Semaphore
import java.util.function.Supplier

@CompileStatic
@Slf4j
class OciClient {

    private ObjectStorage client
    private OciClientFactory clientFactory
    private Semaphore semaphore
    private Properties props
    private String nameSpace

    OciClient(OciClientFactory clientFactory, Properties props){
        int maxConnections = 10
        this.clientFactory = clientFactory
        this.props = props
        this.client = clientFactory.ociClient
        this.semaphore = Threads.useVirtual() ? new Semaphore(maxConnections) : null;
        this.nameSpace = runWithPermit(()->safeNameSpace())
    }

    private <T> T runWithPermit(Supplier<T> action) {
        try {
            if (semaphore != null) semaphore.acquire();
            try {
                Thread.currentThread().setContextClassLoader(OciPlugin.pluginClassLoader)
                return action.get();
            } finally {
                if (semaphore != null) semaphore.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while acquiring Oci client semaphore", e);
        }
    }

    private String safeNameSpace(){
        GetNamespaceResponse namespaceResponse = client.getNamespace(GetNamespaceRequest.builder().build())
        namespaceResponse.value
    }

    List<BucketSummary> listBuckets() {
        return runWithPermit(() -> safeListBuckets())
    }

    private List<BucketSummary> safeListBuckets() {
        BmcRequest.Builder listBucketsBuilder = ListBucketsRequest.builder()

        List<BucketSummary> ret = []
        String nextToken = null
        do {
            listBucketsBuilder.page(nextToken)
            ListBucketsResponse listBucketsResponse = client.listBuckets(listBucketsBuilder.build())
            ret.addAll(listBucketsResponse.items)
            nextToken = listBucketsResponse.getOpcNextPage()
        } while (nextToken != null)

        return ret
    }

    ListObjectsResponse listObjects(String bucket, String key, String delimiter="/") {
        ListObjectsRequest request = ListObjectsRequest.builder()
                .namespaceName(nameSpace)
                .bucketName(bucket)
                .prefix(key)
                .delimiter(delimiter)
                .fields('name,timeCreated,timeModified,storageTier,size')
                .build()
        return runWithPermit(() -> client.listObjects(request));
    }

    InputStream getInputStream(String bucketName, String key) {
        GetObjectRequest.Builder reqBuilder = GetObjectRequest.builder()
                .namespaceName(nameSpace)
                .bucketName(bucketName)
                .objectName(key)
        return runWithPermit(() -> {
            def obj = client.getObject(reqBuilder.build())
            obj.inputStream
        })
    }

    GetBucketResponse getBucketMetadata(String bucketName){
        GetBucketRequest request = GetBucketRequest.builder()
                .namespaceName(nameSpace)
                .bucketName(bucketName)
                .build()
        runWithPermit(()->client.getBucket(request))
    }

    DeleteObjectResponse deleteObject(String bucket, String keyName){
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .namespaceName(nameSpace)
                .bucketName(bucket)
                .objectName(keyName)
                .build()
        runWithPermit(()->client.deleteObject(request))
    }

    PutObjectResponse putObject(String bucket, String keyName, InputStream inputStream, List tags, String contentType, long contentLength) {
        PutObjectRequest.Builder reqBuilder = PutObjectRequest.builder()
                .namespaceName(nameSpace)
                .bucketName(bucket)
                .objectName(keyName)
                .contentType(contentType)
                .contentLength(contentLength)
                .putObjectBody(inputStream)
        PutObjectRequest request = reqBuilder.build()
        runWithPermit(()->{
            client.putObject(request)
        })
    }

    CopyObjectResponse copyFile(String sourceBucket, String sourceObjectName, String destinationBucket, String destinationObjetName){
        CopyObjectDetails details = CopyObjectDetails.builder()
                .sourceObjectName(sourceObjectName)
                .destinationBucket(destinationBucket)
                .destinationNamespace(nameSpace)
                .destinationObjectName(destinationObjetName)
                .build()
        CopyObjectRequest request = CopyObjectRequest.builder()
                .namespaceName(nameSpace)
                .bucketName(sourceBucket)
                .copyObjectDetails(details)
                .build()
        runWithPermit(()->client.copyObject(request))
    }


}
