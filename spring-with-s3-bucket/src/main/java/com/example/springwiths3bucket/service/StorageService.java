package com.example.springwiths3bucket.service;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

@Service
public class StorageService {

    private static final int VALIDITY_IN_MILLISECOND = 500 * 1000;

    @Value("${application.bucket.name}")
    private String bucketName;

    @Value("${cloud.aws.credentials.access-key}")
    private String accessKey;

    @Value("${cloud.aws.credentials.secret-key}")
    private String secretKey;

    @Value("${cloud.aws.region.static}")
    private String region;

    @Autowired
    private AmazonS3 s3Cient;

    public String uploadFileName(MultipartFile file) {
        File fileObject = convertMultipartFileToFile(file);
        String fileName = getFileName(file);
        s3Cient.putObject(new PutObjectRequest(bucketName, fileName, fileObject));
        fileObject.delete();
        return "File Uploaded: " + fileName;
    }

    public Optional<byte[]> downloadFile(String fileName) {
        S3Object object = s3Cient.getObject(bucketName, fileName);
        if (Objects.isNull(object)) {
            throw new IllegalArgumentException("S3Object is null. MethodName: downloadFile");
        }
        S3ObjectInputStream inputStream = object.getObjectContent();
        if (Objects.isNull(inputStream)) {
            throw new IllegalArgumentException("S3ObjectInputStream is null. MethodName: downloadFile");
        }
        try {
            return Optional.of(IOUtils.toByteArray(inputStream));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return Optional.empty();
    }

    public String deleteFile(String fileName) {
        s3Cient.deleteObject(bucketName, fileName);
        return "File deleted successfully.";
    }

    //Generate PreSignedUrl
    public String getPreSignedUrl(String fileName) {
        return getPreSignedUrlForUpload(fileName, bucketName, accessKey, secretKey);
    }

    private String getPreSignedUrlForUpload(String fileName, String bucketName, String accessKey, String secretKey) {
        if (StringUtils.isNullOrEmpty(fileName)) {
            throw new IllegalArgumentException("File name can not be null!");
        }
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(accessKey, secretKey)))
                .withRegion(Regions.fromName(region))
                .build()
                .generatePresignedUrl(new GeneratePresignedUrlRequest(bucketName, fileName)
                        .withMethod(HttpMethod.PUT)
                        .withExpiration(new Date(new Date().getTime() + VALIDITY_IN_MILLISECOND)))
                .toString();
    }

    public String getFileName(MultipartFile file) {
        return System.currentTimeMillis() + "-" + file.getOriginalFilename();
    }

    public String getPreSignedUrl(MultipartFile file) {
        return getPreSignedUrlForUpload(getFileName(file), bucketName, accessKey, secretKey);
    }

    private File convertMultipartFileToFile(MultipartFile file) {
        File convertedFile = new File(file.getOriginalFilename());
        try (FileOutputStream outputStream = new FileOutputStream(convertedFile)) {
            outputStream.write(file.getBytes());
        } catch (Exception e) {
            System.out.println("Error Occurred During File Upload!!!"+e);
        }
        return convertedFile;
    }

}
