package org.apache.hadoop.contrib.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/**
 * Class to store DFS connection
 */
public class HdfsOverFtpSystem {

	private static FileSystem dfs = null;
	private static String superuser = "error";
	private static String supergroup = "supergroup";

	private static HdfsOverFtpConf ftpConf = null;
	private static Path dataDir = null;

	private static void hdfsInit() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", superuser + "," + supergroup);
		dfs = dataDir.getFileSystem(conf);
        dfs.setWriteChecksum(false);
        dfs.setVerifyChecksum(false);
        dfs.setWorkingDirectory(dataDir);
        if (!ftpConf.isPermission()) {
            dfs.setPermission(dataDir, FsPermission.getDefault());
            dfs.setOwner(dataDir, superuser, supergroup);
        }
        // 初始化目录
        if (!dfs.exists(dataDir)) {
            dfs.mkdirs(dataDir);
        }
	}

	public static void setConf(HdfsOverFtpConf conf) {
		HdfsOverFtpSystem.ftpConf = conf;
		superuser = conf.getSuperuser();
		supergroup = conf.getSupergroup();
		Path p = new Path(conf.getHdfsPath());
		// 硬编码
        if (p.toUri().getPath().equals("/")) {
            throw new IllegalArgumentException("文件路径不能使用根路径: /");
        }
        dataDir = p;
	}

    public static HdfsOverFtpConf getFtpConf() {
        return ftpConf;
    }

    /**
	 * Get dfs
	 *
	 * @return dfs
	 * @throws IOException
	 */
	public static FileSystem getDfs() throws IOException {
		if (dfs == null) {
			hdfsInit();
		}
		return dfs;
	}

    /**
     * 获取数据根目录
     */
    public static Path getDataDir() {
        return dataDir;
    }

    public static String getSuperuser() {
        return superuser;
    }

    public static String getSupergroup() {
        return supergroup;
    }

    //  public static String dirList(String path) throws IOException {
//    String res = "";
//
//        getDfs();
//
//        Path file = new Path(path);
//        FileStatus fileStats[] = dfs.listStatus(file);
//
//        for (FileStatus fs : fileStats) {
//            if (fs.isDir()) {
//                res += "d";
//            } else {
//                res += "-";
//            }
//
//            res += fs.getPermission();
//            res += " 1";
//            res += " " + fs.getOwner();
//            res += " " + fs.getGroup();
//            res += " " + fs.getLen();
//            res += " " + new Date(fs.getModificationTime()).toString().substring(4, 16);
//            res += " " + fs.getPath().getName();
//            res += "\n";
//        }
//    return res;
//  }
}
