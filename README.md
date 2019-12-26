hdfs-over-ftp
=============
FTP server which works on a top of HDFS
Source code is provided under MIT License

FTP server is configurable by hdfs-over-ftp.properties and users.properties. It allows to use secure connection over SSL and supports all HDFS permissions.

Installation and running
1. Download and install java, maven
2. Set users in src/main/resources/users.properties. All passwords are md5 encrypted.
3. Set connection port, data-ports and hdfs-uri in src/main/resources/hdfs-over-ftp.properties.
4. Start server using hdfs-over-ftp.sh

Under linux you can mount ftp using curlftpfs:
sudo curlftpfs  -o allow_other ftp://user:pass@localhost:2222 ftpfs


## Build

```bash
 mvn clean package -DskipTests
```

## Release

* GIO-1.0
  * hadoop-2.7.7, ftp-1.1.1
  * Build tar.gz
  * Use `FileSystem` to support hcfs
* GIO-1.1
  * Add `permission` disable
  * Add `supergroup` property
  * Support command: `SHA1`, `SHA256`, `SHA512` 
  