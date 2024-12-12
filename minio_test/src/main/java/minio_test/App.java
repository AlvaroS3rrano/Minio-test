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
	private static String SERVER = "http://127.0.0.1:9000";
	private static String ACCESS_KEY = "minioadmin";
	private static String SECRET_KEY = "minioadmin";
	private static String ACCESS_KEY_2 = "newuser";
	private static String SECRET_KEY_2 = "newuserpassword";
	
	
	public static void main(String[] args) throws Exception {
		MinioClient minioClient = connectToMinio(ACCESS_KEY, SECRET_KEY); // Conectamos con minio
		try {
			List<Bucket> bList = minioClient.listBuckets();
			System.out.println("Connection success");
		} catch (MinioException e) {
			System.out.println("Connection failed: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// crear el bucket hola
		try {
			System.out.println("Creating bucket hola");
			createBucket(minioClient, "hola");
			System.out.println("Bucket created successfully");
			showPolicy(minioClient, "hola");
		} catch (MinioException e) {
			System.out.println("The bucket already exists: " +e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// subir el archivo trial.txt
		try {
			System.out.println("Uploading file trial.txt to bucket hola");
			upload(minioClient, "hola", "trial.txt");
			System.out.println("File uploaded successfully");
		} catch (MinioException e) {
			System.out.println("Error uploading de file: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*
		// descargar el archivo trial.txt
		try {
			downloadFile(minioClient);
		} catch (MinioException e) {
			System.out.println("Error downloading file: "+ e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		// subir archivo hello-world.txt como un stream a hola
		try {
			System.out.println("Uploading file hello-world.txt as a stream to bucket hola");
			updateStream(minioClient);
			System.out.println("File uploaded successfully");
		} catch (MinioException e) {
			System.out.println("Error uploading de file: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Mostrar lista de buckets
		try {
			System.out.println("Buckets list: ");
			listBuckets(minioClient);
		} catch (MinioException e) {
			System.out.println("Error showing buckets: " +e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// mostrar objetos en bucket hola
		try {
			System.out.println("Objects in hola list: ");
			listObjects(minioClient);
		} catch (MinioException e) {
			System.out.println("Error showing objects: " +e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// mostrar objetos en bucket hola despues de eliminar hello-world.txt
		try {
			System.out.println("Objects in hola list after deleting hello-world.txt: ");
			deleteObject(minioClient);
			listObjects(minioClient);
		} catch (MinioException e) {
			System.out.println("Error showing objects: " +e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// usar getObject
		try {
			System.out.println("Example of using getObject");
			getObject(minioClient);
		} catch (MinioException e) {
			System.out.println("Error using getObject: " +e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// usar statObject
		try {
			System.out.println("Example of using statObject");
			statObject(minioClient);
		} catch (MinioException e) {
			System.out.println("Error using statObject: " +e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// probando las politicas
		try {
			System.out.println("Creating a new bucket (test) with a only read policy ");
			createBucket(minioClient, "test");
			setOnlyReadBucketPolicy(minioClient);
			showPolicy(minioClient, "test");
		} catch (MinioException e) {
			System.out.println("Error: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// intentar subir un archivo en test con solo lectura
		try {
			System.out.println("Trying to upload a file in bucket test");
			upload(minioClient, "test", "trial.txt");
			System.out.println("The file can be uploaded with the admin account");
		} catch (MinioException e) {
			System.out.println("Error uploading the file: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// intentar subir un archivo en test con una cuenta no admin
		try {
			System.out.println("Login as non admin user");
			MinioClient minioClient2 = connectToMinio(ACCESS_KEY_2, SECRET_KEY_2);
			System.out.println("Trying to upload a file in bucket test");
			upload(minioClient2, "test", "hello-world.txt");
			System.out.println("The file can be uploaded with the admin account");
		} catch (MinioException e) {
			System.out.println("Error uploading the file: "+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// to connect with minio
	private static MinioClient connectToMinio(String accesKey, String secretKey) {
		MinioClient minioClient = MinioClient.builder()
				.endpoint(SERVER)
				.credentials(accesKey, secretKey)
				.build();
		return minioClient;
	}
	
	// to upload files
	private static void upload(MinioClient minioClient, String bucketName, String objectName) throws Exception {
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
		setBucketPolicies(minioClient, bucketName);
		if (minioClient.bucketExists(beArgs)) {
			System.out.println("Created bucket " + bucketName);
		} else {
			System.out.println("Could not create bucket " + bucketName);
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
	
	private static void setOnlyReadBucketPolicy(MinioClient minioClient) throws Exception{
		String bucketName = "test";
		String policyJson = "{"
			    + "\"Version\": \"2012-10-17\","
			    + "\"Statement\": ["
			    + "  {"
			    + "    \"Effect\": \"Allow\","
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
	
	private static void setBucketPolicies(MinioClient minioClient, String bucketName) throws Exception{
		
        String policyJson = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": ["
                + "  {"
                + "    \"Effect\": \"Allow\","
                + "    \"Principal\": {\"AWS\": \"*\"},"
                + "    \"Action\": \"s3:*\"," // Permite todas las acciones
                + "    \"Resource\": ["
                + "      \"arn:aws:s3:::" + bucketName + "\","
                + "      \"arn:aws:s3:::" + bucketName + "/*\""
                + "    ]"
                + "  }"
                + "]"
                + "}";
        
       
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
