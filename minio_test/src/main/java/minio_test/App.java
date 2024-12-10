package minio_test;

import java.util.List;

import io.minio.MinioClient;
import io.minio.errors.MinioException;
import io.minio.messages.Bucket;

public class App {

	public static void main(String[] args) {
		MinioClient minioClient = demo();
		try {
			List<Bucket> bList = minioClient.listBuckets();
			System.out.println("Connection success, total buckets: "+bList.size());
		} catch (MinioException e) {
			System.out.println("Connection failed: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private static MinioClient demo() {
		MinioClient minioClient = MinioClient.builder()
				.endpoint("https://play.min.io")
				.credentials("sdf", "sdf")
				.build();
		return minioClient;
	}

}
