package com.example.springwiths3bucket.controller;
/*
    View all files : aws s3 ls s3://audio-book-test-bucket
 */
import com.example.springwiths3bucket.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/file")
public class StorageController {

    private final StorageService storageService;

    @Autowired
    public StorageController(StorageService storageService) {
        this.storageService = storageService;
    }

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestParam(value = "file") MultipartFile file) {
        return new ResponseEntity<>(storageService.uploadFileName(file), HttpStatus.OK);
    }

    @GetMapping("/download/{fileName}")
    public ResponseEntity<ByteArrayResource> downloadFile(@PathVariable String fileName) {
        if (storageService.downloadFile(fileName).isEmpty()) {
            return ResponseEntity.badRequest().body(null);
        }
        byte[] data = storageService.downloadFile(fileName).get();
        ByteArrayResource resource = new ByteArrayResource(data);
        return ResponseEntity
                .ok()
                .contentLength(data.length)
                .header("Content-type", "application/octet-stream")
                .header("Content-disposition", "attachment; filename=\"" + fileName + "\"")
                .body(resource);
    }

    @DeleteMapping("/delete/{fileName}")
    public ResponseEntity<String> deleteFile(@PathVariable String fileName) {
        return new ResponseEntity<>(storageService.deleteFile(fileName), HttpStatus.OK);
    }

    @GetMapping("/get-pre-signed-url")
    public ResponseEntity<String> getPreSignedUrl(@RequestParam(value = "fileName") String fileName) {
        return ResponseEntity.ok(storageService.getPreSignedUrl(fileName));
    }
}
