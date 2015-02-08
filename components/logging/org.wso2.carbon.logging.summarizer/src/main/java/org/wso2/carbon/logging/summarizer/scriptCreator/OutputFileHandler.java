package org.wso2.carbon.logging.summarizer.scriptCreator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.wso2.carbon.logging.summarizer.utils.LoggingConfig;
import org.wso2.carbon.logging.summarizer.utils.LoggingConfigManager;

import java.io.*;
import java.util.zip.GZIPOutputStream;

/*
 * Copyright 2005,2006 WSO2, Inc. http://www.wso2.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class OutputFileHandler {

    private static final Log log = LogFactory.getLog(OutputFileHandler.class);
    LoggingConfig config = LoggingConfigManager.loadLoggingConfiguration();
    String archivedLogLocation = config.getArchivedLogLocation();
    String hdfsConfig = config.getHdfsConfig();

    public void fileReStructure(String colFamilyName) throws IOException {
        log.info("CF "+colFamilyName);


        Configuration conf = new Configuration(false);
        /**
         * Create HDFS Client configuration to use name node hosted on host master and port 9000.
         * Client configured to connect to a remote distributed file system.
         */
        conf.set("fs.default.name", hdfsConfig);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        /**
         * Get connection to remote file sytem
         */
        FileSystem fs = FileSystem.get(conf);

        /**
         * Crate file sourcePath object
         */
        Path filePath = new Path(archivedLogLocation);

        String tmpStrArr[] = colFamilyName.split("_");
        String tenantId = tmpStrArr[1];
        String serverName = "";

//        The server name can have names such as DEV_AS, TEST_AS
        String createdDate = tmpStrArr[tmpStrArr.length - 3] + "_" + tmpStrArr[tmpStrArr.length - 2] + "_" + tmpStrArr[tmpStrArr.length - 1];
        for (int i = 2; i < tmpStrArr.length - 3; i++) {
            if (serverName.equals("")) {
                serverName = tmpStrArr[i];
            } else {
                serverName = serverName + "_" + tmpStrArr[i];
            }
        }

//        stratos/archievedLogs0/AS/
        String directoryPathName = archivedLogLocation + tenantId + "/" + serverName;
//        stratos/archievedLogs0/AS/2013_09_30/
        String filePathName = directoryPathName + File.separator + createdDate;

        log.info("filePathName " + filePathName);
        log.info("createdDate " + createdDate);
        //Rename the 000000_0 file as a .tmp file
        Path sourceFileName = new Path(filePathName + "/000000_0");
        Path destnFileName = new Path(filePathName + "/" + createdDate + ".tmp");


        Path rootFilePath = new Path(filePathName);
        FileStatus fileStatustatus = fs.getFileStatus(rootFilePath);
        if (!fileStatustatus.isDir()) {
            throw new IOException("File : " + filePathName + " is not a directory");
        }

        FileStatus[] childStatus = fs.listStatus(rootFilePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith("payload_appname=");
            }
        });

        for (FileStatus child : childStatus) {
            String childName = child.getPath().getName();
//            These should be the directories with the name payload_appname=*
//            If the appName is empty, the the folder name is like payload_appname=__HIVE_DEFAULT_PARTITION__
//            We copy them to this level

            if (!childName.equals("payload_appname=__HIVE_DEFAULT_PARTITION__")) {
//                Get the app name
                String appName = childName.replace("payload_appname=", "");

//                Path in HDFS = stratos/archievedLogs0/AS/2013_09_30/payload_appname=HelloService
//                we change this to stratos/archievedLogs0/AS/HelloService/2013_09_30
//                This is done to make the download more efficient.
                String currentPath = filePathName + File.separator + childName;
                String appDirectoryName = directoryPathName + File.separator + appName + File.separator + createdDate;

                Path appDirectoryPath = new Path(appDirectoryName);

//                If the directory is not there, then we create it
                FileStatus[] directory = fs.listStatus(appDirectoryPath.getParent());
                if (directory == null) {
//                Create directories
                    fs.mkdirs(appDirectoryPath.getParent());
                    fs.mkdirs(appDirectoryPath);
                } else {
                    FileStatus[] dataDirectory = fs.listStatus(appDirectoryPath);
                    if (dataDirectory == null) {
                        fs.mkdirs(appDirectoryPath);
                    }
                }

//                We are expecting only one child resource within this directory.
//                Rename the 000000_0 file as a .tmp file
                String sourcePath = currentPath + File.separator + "000000_0";
                String targetPath = appDirectoryName + File.separator + createdDate + ".tmp";
                restructureLogFile(fs, tenantId, serverName, createdDate, sourcePath, targetPath, appDirectoryName);


            } else {
//                We are expecting only one child resource within this directory.
//                Rename the 000000_0 file as a .tmp file
                String sourcePath = filePathName + File.separator + childName + File.separator + "000000_0";
                String targetPath = filePathName + File.separator + createdDate + ".tmp";

                restructureLogFile(fs, tenantId, serverName, createdDate, sourcePath, targetPath, filePathName);
            }
        }
    }

    private void restructureLogFile(FileSystem fs, String tenantId, String serverName, String createdDate,
                                    String sourcePath, String targetPath, String appDirectoryPath) throws IOException {

        boolean isRenamed = renameFile(fs, sourcePath, targetPath);
        //To remove the unicode character in the created .tmp file
        if (isRenamed) {
//        We delete the parent directories
            fs.delete(new Path(sourcePath).getParent(), true);
            Path sanitizedFileName = new Path(appDirectoryPath + File.separator + createdDate + ".log");

//            Delete the log file if it exists
            fs.delete(sanitizedFileName, false);
            replaceChar(new Path(targetPath), sanitizedFileName, fs);

            log.info("Logs of Tenant " + tenantId + " of " + serverName + " on " + createdDate + " are successfully archived");
        } else {
            log.info("Logs of Tenant " + tenantId + " of " + serverName + " on " + createdDate + " are not ******* successfully archived");

        }
    }

    private boolean renameFile(FileSystem fs, String sourceFileName, String destinationFileName) throws IOException {
        Path sourceFilePath = new Path(sourceFileName);
        Path destinationPath = new Path(destinationFileName);

        return fs.rename(sourceFilePath, destinationPath);
    }

    public static void replaceChar(Path oldPath, Path newPath, FileSystem fs)
            throws IOException {
        FSDataInputStream in = fs.open(oldPath);
        FSDataOutputStream out = fs.create(newPath);
        BufferedReader dataInput = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = dataInput.readLine()) != null) {
            out.write(line.replace("\001", "").getBytes());
            out.write("\n".getBytes());
        }

        out.close();
        dataInput.close();
        if (fs.exists(oldPath)) {
            fs.delete(oldPath, true);
        }
        in.close();
    }

    public void compressLogFile(String temp) throws IOException {
        File file = new File(temp);
        FileOutputStream outputStream = new FileOutputStream(file + ".gz");
        GZIPOutputStream gzos = new GZIPOutputStream(outputStream);
        FileInputStream inputStream = new FileInputStream(temp);
        BufferedInputStream in = new BufferedInputStream(inputStream);
        byte[] buffer = new byte[1024];
        int i;
        while ((i = in.read(buffer)) >= 0) {
            gzos.write(buffer, 0, i);
        }
        in.close();
        gzos.close();
    }


    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }

}
