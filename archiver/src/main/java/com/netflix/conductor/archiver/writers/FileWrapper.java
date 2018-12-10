package com.netflix.conductor.archiver.writers;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileWrapper {
    private boolean manifest;
    private BufferedWriter bw;
    private File file;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    FileWrapper(String name, boolean manifest, boolean append) throws IOException {
        this.file = new File(name);
        this.manifest = manifest;
        if (file.exists()) {
            file.createNewFile();
        }

        bw = new BufferedWriter(new FileWriter(file, append));
    }

    synchronized void write(String line) throws IOException {
        bw.write(line);
        bw.write("\n");
    }

    void close() {
        try {
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getZipFileName() {
        return FilenameUtils.removeExtension(file.getAbsolutePath()) + ".zip";
    }

    public boolean isManifest() {
        return manifest;
    }

    public void zip() throws IOException {
        byte[] buffer = new byte[1024 * 1024];
        try (FileOutputStream fos = new FileOutputStream(getZipFileName())) {
            try (ZipOutputStream zos = new ZipOutputStream(fos)) {
                zos.putNextEntry(new ZipEntry(file.getName()));
                try (FileInputStream in = new FileInputStream(file)) {
                    int len;
                    while ((len = in.read(buffer)) > 0)
                    {
                        zos.write(buffer, 0, len);
                    }
                }
            }
        }
    }

    public void upload(AmazonS3 s3, String bucketName, String folderName) {
        if (manifest) {
            s3.putObject(bucketName, folderName + "/" + file.getName(), file);
        } else {
            File zipFile = new File( getZipFileName());
            s3.putObject(bucketName, folderName + "/" + zipFile.getName(), zipFile);
        }
    }

    public Stream<String> stream() throws IOException {
        return Files.lines(file.toPath());
    }

    @Override
    public String toString() {
        return file.getName();
    }
}