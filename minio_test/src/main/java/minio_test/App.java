package minio_test;

import java.util.List;

import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import io.minio.messages.Bucket;

public class App {

	public static void main(String[] args) throws Exception {
		MinioClient minioClient = demo();
		try {
			List<Bucket> bList = minioClient.listBuckets();
			System.out.println("Connection success, total buckets: "+bList.size());
		} catch (MinioException e) {
			System.out.println("Connection failed: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		upload(minioClient);

	}
	
	private static MinioClient demo() {
		MinioClient minioClient = MinioClient.builder()
				.endpoint("http://127.0.0.1:9000")
				.credentials("minioadmin", "minioadmin")
				.build();
		return minioClient;
	}
	
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

}
