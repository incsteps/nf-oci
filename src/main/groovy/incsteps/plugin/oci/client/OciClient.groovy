package incsteps.plugin.oci.client


import com.oracle.bmc.Region
import com.oracle.bmc.http.client.jersey3.Jersey3HttpProvider
import com.oracle.bmc.objectstorage.ObjectStorage
import com.oracle.bmc.objectstorage.ObjectStorageClient
import com.oracle.bmc.objectstorage.model.BucketSummary
import com.oracle.bmc.objectstorage.model.CopyObjectDetails
import com.oracle.bmc.objectstorage.requests.*
import com.oracle.bmc.objectstorage.responses.*
import com.oracle.bmc.requests.BmcRequest
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import incsteps.plugin.oci.OciPlugin
import incsteps.plugin.oci.config.OciConfig
import nextflow.util.Threads

import java.util.concurrent.Semaphore
import java.util.function.Supplier

@CompileStatic
@Slf4j
class OciClient {

    private ObjectStorage clientObjectStorage
    private OciConfig ociConfig
    private Semaphore semaphore
    private Properties props
    private String nameSpace

    OciClient(OciConfig ociConfig){
        int maxConnections = 10
        this.ociConfig = ociConfig
        this.props = props
        this.clientObjectStorage = getObjectStorageClient()
        this.semaphore = Threads.useVirtual() ? new Semaphore(maxConnections) : null
    }

    private ObjectStorageClient getObjectStorageClient(){
        final useRegion = ociConfig.region
        final provider = ociConfig.authentificationProvider?.provider
        return ObjectStorageClient.builder()
                .httpProvider(Jersey3HttpProvider.instance)
                .region(Region.fromRegionCode(useRegion))
                .build(provider)
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

    String getNameSpace(){
        if(!nameSpace)
            nameSpace = runWithPermit(()->safeNameSpace())
        nameSpace
    }

    private String safeNameSpace(){
        GetNamespaceResponse namespaceResponse = clientObjectStorage.getNamespace(GetNamespaceRequest.builder().build())
        namespaceResponse.value
    }

    List<BucketSummary> listBuckets() {
        return runWithPermit(() -> safeListBuckets())
    }

    private List<BucketSummary> safeListBuckets() {
        BmcRequest.Builder listBucketsBuilder = ListBucketsRequest
                .builder()
                .namespaceName(getNameSpace())

        List<BucketSummary> ret = []
        String nextToken = null
        do {
            listBucketsBuilder.page(nextToken)
            ListBucketsResponse listBucketsResponse = clientObjectStorage.listBuckets(listBucketsBuilder.build())
            ret.addAll(listBucketsResponse.items)
            nextToken = listBucketsResponse.getOpcNextPage()
        } while (nextToken != null)

        return ret
    }

    ListObjectsResponse listObjects(String bucket, String key, String delimiter="/") {
        ListObjectsRequest request = ListObjectsRequest.builder()
                .namespaceName(getNameSpace())
                .bucketName(bucket)
                .prefix(key)
                .delimiter(delimiter)
                .fields('name,timeCreated,timeModified,storageTier,size')
                .build()
        return runWithPermit(() -> clientObjectStorage.listObjects(request));
    }

    InputStream getInputStream(String bucketName, String key) {
        GetObjectRequest.Builder reqBuilder = GetObjectRequest.builder()
                .namespaceName(getNameSpace())
                .bucketName(bucketName)
                .objectName(key)
        return runWithPermit(() -> {
            def obj = clientObjectStorage.getObject(reqBuilder.build())
            obj.inputStream
        })
    }

    GetBucketResponse getBucketMetadata(String bucketName){
        GetBucketRequest request = GetBucketRequest.builder()
                .namespaceName(getNameSpace())
                .bucketName(bucketName)
                .build()
        runWithPermit(()->clientObjectStorage.getBucket(request))
    }

    DeleteObjectResponse deleteObject(String bucket, String keyName){
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .namespaceName(getNameSpace())
                .bucketName(bucket)
                .objectName(keyName)
                .build()
        runWithPermit(()->clientObjectStorage.deleteObject(request))
    }

    PutObjectResponse putObject(String bucket, String keyName, InputStream inputStream, List tags, String contentType, long contentLength) {
        PutObjectRequest.Builder reqBuilder = PutObjectRequest.builder()
                .namespaceName(getNameSpace())
                .bucketName(bucket)
                .objectName(keyName)
                .contentType(contentType)
                .contentLength(contentLength)
                .putObjectBody(inputStream)
        PutObjectRequest request = reqBuilder.build()
        runWithPermit(()->{
            clientObjectStorage.putObject(request)
        })
    }

    CopyObjectResponse copyFile(String sourceBucket, String sourceObjectName, String destinationBucket, String destinationObjetName){
        CopyObjectDetails details = CopyObjectDetails.builder()
                .sourceObjectName(sourceObjectName)
                .destinationBucket(destinationBucket)
                .destinationNamespace(getNameSpace())
                .destinationObjectName(destinationObjetName)
                .build()
        CopyObjectRequest request = CopyObjectRequest.builder()
                .namespaceName(getNameSpace())
                .bucketName(sourceBucket)
                .copyObjectDetails(details)
                .build()
        runWithPermit(()->clientObjectStorage.copyObject(request))
    }


}
