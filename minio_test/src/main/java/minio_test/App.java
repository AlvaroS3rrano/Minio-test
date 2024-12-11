package minio_test;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.MinioException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Bucket;

public class App {

	public static void main(String[] args) throws Exception {
		MinioClient minioClient = demo(); // Conectamos con minio 
		try {
			List<Bucket> bList = minioClient.listBuckets();
			System.out.println("Connection success, total buckets: "+bList.size());
		} catch (MinioException e) {
			System.out.println("Connection failed: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			createBucket(minioClient);
		} catch (MinioException e) {
			System.out.println("The bucket already exists");
		}
		upload(minioClient);

	}
	
	// to connect with minio
	private static MinioClient demo() {
		MinioClient minioClient = MinioClient.builder()
				.endpoint("http://127.0.0.1:9000")
				.credentials("minioadmin", "minioadmin")
				.build();
		return minioClient;
	}
	
	// to upload files
	private static void upload(MinioClient minioClient) throws Exception {
		String bucketName = "hola";
		String objectName = "trial.txt";
		String filename = "/home/alvaro/eclipse-workspace/Minio-test/minio_test/src/main/resources/trial.txt";
		UploadObjectArgs uArgs = UploadObjectArgs.builder()
				.bucket(bucketName)
				.object(objectName)
				.filename(filename)
				.build();
		ObjectWriteResponse resp = minioClient.uploadObject(uArgs);
		
		System.out.println(resp.object() + ": "+ resp.etag()+": "+resp.versionId());
	}
	
	// to create a bucket
	private static void createBucket(MinioClient minioClient) throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IOException {
		String bucketName = "hola";
		MakeBucketArgs mbArgs = MakeBucketArgs.builder() // otras opciones podrian ser region 
				.bucket(bucketName)						 // objectLock ...
				.build();
		minioClient.makeBucket(mbArgs); // no devuelve nada
		
		BucketExistsArgs beArgs = BucketExistsArgs.builder()
				.bucket(bucketName)
				.build();
		
		if (minioClient.bucketExists(beArgs)) {
			System.out.println("Bucket " + bucketName + " exists");
		} else {
			System.out.println("Bucket " + bucketName + " does not exists");
		}
	}

}
