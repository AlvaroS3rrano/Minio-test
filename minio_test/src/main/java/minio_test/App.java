package minio_test;

import java.io.FileInputStream;
import java.util.Iterator;
import java.util.List;

import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.GetBucketPolicyArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.SetBucketPolicyArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import io.minio.messages.Bucket;
import io.minio.messages.Item;

public class App {
	
	private static String LOCATION = "/home/alvaro/eclipse-workspace/Minio-test/minio_test/src/main/resources/";
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
			createBucket(minioClient, "hola");
		} catch (MinioException e) {
			System.out.println("The bucket already exists");
		}
		upload(minioClient);
		/*
		try {
			downloadFile(minioClient);
		} catch (MinioException e) {
			System.out.println("Error downloading file");
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		updateStream(minioClient);
		
		listBuckets(minioClient);
		
		System.out.println("Object list before delete");
		listObjects(minioClient);
		
		//deleteObject(minioClient);
		
		System.out.println("Object list after delete");
		listObjects(minioClient);
		
		System.out.println("" );
		System.out.println("get Object");
		System.out.println("");
		
		getObject(minioClient);
		
		System.out.println("" );
		System.out.println("stat Object");
		System.out.println("");
		
		statObject(minioClient);
		
		try {
			createBucket(minioClient, "test");
			setBucketPolicy(minioClient);
			showPolicy(minioClient, "test");
		} catch (MinioException e) {
			System.out.println("The bucket already exists");
		} 
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
		String contentType = "text/plain";
		String filename = LOCATION + "trial.txt";
		UploadObjectArgs uArgs = UploadObjectArgs.builder()
				.bucket(bucketName)
				.object(objectName)
				.contentType(contentType)
				.filename(filename)
				.build();
		ObjectWriteResponse resp = minioClient.uploadObject(uArgs);
		
		System.out.println(resp.object() + ": "+ resp.etag()+": "+resp.versionId());
	}
	
	// to create a bucket
	private static void createBucket(MinioClient minioClient, String bucketName) 
			throws Exception {
		MakeBucketArgs mbArgs = MakeBucketArgs.builder() // otras opciones podrian ser region 
				.bucket(bucketName)						 // objectLock ...
				.build();
		minioClient.makeBucket(mbArgs); // no devuelve nada
		
		BucketExistsArgs beArgs = BucketExistsArgs.builder() // para comprobar si el bucket existe
				.bucket(bucketName)
				.build();
		
		if (minioClient.bucketExists(beArgs)) {
			System.out.println("Bucket " + bucketName + " exists");
		} else {
			System.out.println("Bucket " + bucketName + " does not exists");
		}
	}
	
	// to download files
	private static void downloadFile(MinioClient minioClient) 
			throws Exception {
		String bucketName = "hola";
		String objectName = "trial.txt";
		String fileName = "/home/alvaro/Desktop/poddera/new.txt"; // donde te descarga el archivo y con que nombre
		
		DownloadObjectArgs dArgs = DownloadObjectArgs.builder()
				.bucket(bucketName)
				.object(objectName)
				.filename(fileName)
				.build();
		minioClient.downloadObject(dArgs);
	}
	
	// to upload a stream 
	private static void updateStream(MinioClient minioClient) throws Exception{
		String bucketName = "hola";
		String objectName = "hello-world.txt";
		String contentType = "text/plain";
		String fileName = LOCATION + "hello-world.txt";
		
		FileInputStream stream = new FileInputStream(fileName);
		
		PutObjectArgs uArgs = PutObjectArgs.builder()
				.bucket(bucketName)
				.object(objectName)
				.stream(stream,11, -1) // hay que saber cuantos bits tine el archivo o sino ponerlo a -1 y poner part size
				.contentType(contentType)
				.build();
		ObjectWriteResponse resp = minioClient.putObject(uArgs);
		
		System.out.println(resp.object() + ": "+ resp.etag()+": "+resp.versionId());
	}
	
	// to list objects
	private static void listObjects(MinioClient minioClient) throws Exception {
		String bucketName = "hola";
		ListObjectsArgs lArgs = ListObjectsArgs.builder() // otras opciones serian para mostrar todas la versiones 
				.bucket(bucketName)						  // que sea recursivo, la version de la api de amazon que queremos usar 
				.build();								  // y muchas más
		Iterable<Result<Item>> resp = minioClient.listObjects(lArgs);
		
		Iterator<Result<Item>> it = resp.iterator();
		
		while (it.hasNext()) {
			Item i = it.next().get();
			System.out.println("Object: "+i.objectName()+ " with size: "+ i.size()+ " last modified: "+i.lastModified());
		}
	}
	
	// to list bucktes 
	private static void listBuckets(MinioClient minioClient) throws Exception{
		List<Bucket> bucketList = minioClient.listBuckets();
		for (Bucket bucket: bucketList) {
			System.out.println(bucket.name()+ ", "+ bucket.creationDate());
		}
	}
	
	// to delete an object
	private static void deleteObject(MinioClient minioClient) throws Exception{ // si intentas borrar un objeto que no existe no dice nada
		String bucketName = "hola";
		String objectName = "hello-world.txt";
		
		RemoveObjectArgs rArgs = RemoveObjectArgs.builder()
				.bucket(bucketName)
				.object(objectName)
				.build();
		
		minioClient.removeObject(rArgs);
	}
	
	// to get the object content
	private static void getObject(MinioClient minioClient) throws Exception{ // permite leer el contenido del objeto
		String bucketName ="hola";
		String objectName = "trial.txt";
		
		GetObjectArgs gArgs = GetObjectArgs.builder()
				.bucket(bucketName)
				.object(objectName)
				.build();
		GetObjectResponse resp = minioClient.getObject(gArgs);
		
		System.out.println("Object: "+resp.object());
		
	}
	
	// to get the object metadata
		private static void statObject(MinioClient minioClient) throws Exception{ // permite leer los metadatos 
			String bucketName ="hola";											  // sin descargar el contenido
			String objectName = "trial.txt";
			
			StatObjectArgs gArgs = StatObjectArgs.builder()
					.bucket(bucketName)
					.object(objectName)
					.build();
			StatObjectResponse resp = minioClient.statObject(gArgs);
			
			System.out.println("Object: "+resp.object() + " last modified: " + resp.lastModified());
			
		}
	
	private static void setBucketPolicy(MinioClient minioClient) throws Exception{
		String bucketName = "test";
		String policyJson = "{"
			    + "\"Version\": \"2012-10-17\","
			    + "\"Statement\": ["
			    + "  {"
			    + "    \"Effect\": \"Allow\","
			    + "    \"Principal\": {\"AWS\": \"*\"},"
			    + "    \"Action\": [\"s3:GetObject\"],"
			    + "    \"Resource\": [\"arn:aws:s3:::%s/*\"]"
			    + "  }"
			    + "]"
			    + "}";

		policyJson = String.format(policyJson, bucketName);
        // Aplicar la política al bucket
        minioClient.setBucketPolicy(
            SetBucketPolicyArgs.builder()
                .bucket(bucketName)
                .config(policyJson)
                .build()
        );
	}
	
	private static void showPolicy(MinioClient minioClient, String bucketName) throws Exception{
		String policy = minioClient.getBucketPolicy(
			    GetBucketPolicyArgs.builder()
			        .bucket(bucketName)
			        .build()
			);

			System.out.println("Política actual del bucket: " + policy);
	}
	
}
