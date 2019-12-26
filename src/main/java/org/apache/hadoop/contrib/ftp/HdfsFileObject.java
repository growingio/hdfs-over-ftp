package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class implements all actions to HDFS
 */
public class HdfsFileObject implements FtpFile {

	private final Logger log = LoggerFactory.getLogger(HdfsFileObject.class);

	private Path path;
	private Path hdfsPath;
	private HdfsUser user;

	/**
	 * Constructs HdfsFileObject from path
	 *
	 * @param path path to represent object
	 * @param user accessor of the object
	 */
	public HdfsFileObject(String path, User user) {
	    this(path, user, false);
	}

    public HdfsFileObject(String path, User user, Boolean create) {
        // 加入工作目录前缀
        this.user = (HdfsUser) user;
        this.path = this.removeHdfsDataDir(new Path(path));
        this.hdfsPath = this.concatPath(HdfsOverFtpSystem.getDataDir(), new Path(user.getName()), this.path);

        try {
            FileSystem fs = HdfsOverFtpSystem.getDfs();
            if (create && !fs.exists(this.hdfsPath)) {
                fs.mkdirs(this.hdfsPath);
                fs.setOwner(this.hdfsPath, this.user.getName(), this.user.getMainGroup());
            }
        } catch (Exception e) {
            // ignore
        }
    }


	@Nullable
	private Path concatPath(Path... ps) {
        return Arrays.stream(ps).reduce((p1, p2) -> {
            String path1 = p1.toUri().getPath();
            String path2 = p2.toUri().getPath();
            if (path1.endsWith("/") || path2.startsWith("/")) {
                return new Path(path1 + path2);
            }
            else if (path1.endsWith("/") && path2.startsWith("/")) {
                if (path1.length() == 1) {
                    return new Path(path2);
                } else {
                    return new Path(path1.substring(0, path1.length() - 1) + path2);
                }
            }
            else {
                return new Path(path1 + "/" + path2);
            }
        }).orElse(null);
    }

    // remove hdfs working dir
    private Path removeHdfsDataDir(Path p) {
        String pathInHdfs = p.toUri().getPath();
        Path hdfsWorkDir = this.concatPath(HdfsOverFtpSystem.getDataDir(), new Path(user.getName()));
        String actualPath = pathInHdfs.replaceAll(hdfsWorkDir.toUri().getPath(), "");
        if (!actualPath.startsWith("/")) {
            actualPath = "/" + actualPath;
        }
        return new Path(actualPath);
    }

	/**
	 * Get full name of the object
	 *
	 * @return full name of the object
	 */
	@Override
    public String getAbsolutePath() {
		return path.toString();
	}

	/**
	 * Get short name of the object
	 *
	 * @return short name of the object
	 */
	@Override
    public String getName() {
	    return path.getName();
	}

	/**
	 * HDFS has no hidden objects
	 *
	 * @return always false
	 */
	@Override
    public boolean isHidden() {
		return false;
	}

	/**
	 * Checks if the object is a directory
	 *
	 * @return true if the object is a directory
	 */
	@Override
    public boolean isDirectory() {
		try {
			log.debug("is directory? : " + path);
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(hdfsPath);
			return fs.isDirectory();
		} catch (IOException e) {
			log.debug(path + " is not dir", e);
			return false;
		}
	}

	/**
	 * Get HDFS permissions
	 *
	 * @return HDFS permissions as a FsPermission instance
	 * @throws IOException if path doesn't exist so we get permissions of parent object in that case
	 */
	private FsPermission getPermissions() throws IOException {
//        try {
        FileSystem dfs = HdfsOverFtpSystem.getDfs();
		return dfs.getFileStatus(hdfsPath).getPermission();
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }
	}

	/**
	 * Checks if the object is a file
	 *
	 * @return true if the object is a file
	 */
	@Override
    public boolean isFile() {
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			return dfs.isFile(hdfsPath);
		} catch (IOException e) {
			log.debug(path + " is not file", e);
			return false;
		}
	}

	/**
	 * Checks if the object does exist
	 *
	 * @return true if the object does exist
	 */
	@Override
    public boolean doesExist() {
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
            // dfs.getFileStatus(path);
            // return true;
            return dfs.exists(hdfsPath);
		} catch (IOException e) {
			//   log.debug(path + " does not exist", e);
			return false;
		}
	}

	/**
	 * Checks if the user has a read permission on the object
	 *
	 * @return true if the user can read the object
	 */
	@Override
    public boolean isReadable() {
		try {
			FsPermission permissions = getPermissions();
			if (user.getName().equals(getOwnerName())) {
				if (permissions.toString().substring(0, 1).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for user");
					return true;
				}
			} else if (user.isGroupMember(getGroupName())) {
				if (permissions.toString().substring(3, 4).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for group");
					return true;
				}
			} else {
				if (permissions.toString().substring(6, 7).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for others");
					return true;
				}
			}
			log.debug("PERMISSIONS: " + path + " - " + " read denied");
			return false;
		} catch (IOException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			return false;
		}
	}

	private HdfsFileObject getParent() {
		String pathS = path.toString();
		String parentS = "/";
		int pos = pathS.lastIndexOf("/");
		if (pos > 0) {
			parentS = pathS.substring(0, pos);
		}
		return new HdfsFileObject(parentS, user);
	}

	/**
	 * Checks if the user has a write permission on the object
	 *
	 * @return true if the user has write permission on the object
	 */
	@Override
    public boolean isWritable() {
		try {
			FsPermission permissions = getPermissions();
			if (user.getName().equals(getOwnerName())) {
				if (permissions.toString().substring(1, 2).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for user");
					return true;
				}
			} else if (user.isGroupMember(getGroupName())) {
				if (permissions.toString().substring(4, 5).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for group");
					return true;
				}
			} else {
				if (permissions.toString().substring(7, 8).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for others");
					return true;
				}
			}
			log.debug("PERMISSIONS: " + path + " - " + " write denied");
			return false;
		} catch (IOException e) {
            HdfsFileObject parent = getParent();
            if (parent.path.toUri().getPath().equals("/")) {
                return true;
            } else {
                return parent.isWritable();
            }

		}
	}

	/**
	 * Checks if the user has a delete permission on the object
	 *
	 * @return true if the user has delete permission on the object
	 */
	@Override
    public boolean isRemovable() {
		return isWritable();
	}

	/**
	 * Get owner of the object
	 *
	 * @return owner of the object
	 */
	@Override
    public String getOwnerName() {
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(hdfsPath);
			return fs.getOwner();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get group of the object
	 *
	 * @return group of the object
	 */
	@Override
    public String getGroupName() {
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(hdfsPath);
			return fs.getGroup();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get link count
	 *
	 * @return 3 is for a directory and 1 is for a file
	 */
	@Override
    public int getLinkCount() {
		return isDirectory() ? 3 : 1;
	}

	/**
	 * Get last modification date
	 *
	 * @return last modification date as a long
	 */
	@Override
    public long getLastModified() {
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(hdfsPath);
			return fs.getModificationTime();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

    @Override
    public boolean setLastModified(long time) {
        return false;
    }

	/**
	 * Get a size of the object
	 *
	 * @return size of the object in bytes
	 */
	@Override
    public long getSize() {
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(hdfsPath);
			log.info("getSize(): " + path + " : " + fs.getLen());
			return fs.getLen();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

    @Override
    public Object getPhysicalFile() {
        return null;
    }

	/**
	 * Create a new dir from the object
	 *
	 * @return true if dir is created
	 */
	@Override
    public boolean mkdir() {

		if (!isWritable()) {
			log.debug("No write permission : " + path);
			return false;
		}

		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.mkdirs(hdfsPath);
			dfs.setOwner(hdfsPath, user.getName(), user.getMainGroup());
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Delete object from the HDFS filesystem
	 *
	 * @return true if the object is deleted
	 */
	@Override
    public boolean delete() {
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.delete(hdfsPath, true);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Move the object to another location
	 *
	 * @param fileObject location to move the object
	 * @return true if the object is moved successfully
	 */
	@Override
    public boolean move(FtpFile fileObject) {
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
            HdfsFileObject hdfsFileObject = new HdfsFileObject(fileObject.getAbsolutePath(), user);
            dfs.rename(hdfsPath, hdfsFileObject.hdfsPath);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * List files of the directory
	 *
	 * @return List of files in the directory
	 */
	@Override
    public List<FtpFile> listFiles() {

		if (!isReadable()) {
			log.debug("No read permission : " + path);
			return null;
		}

		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fileStats[] = dfs.listStatus(hdfsPath);

            ArrayList<FtpFile> fileObjects = new ArrayList<FtpFile>();
			for (int i = 0; i < fileStats.length; i++) {
			    // 移除hdfs的工作目录
                fileObjects.add(new HdfsFileObject(fileStats[i].getPath().toUri().getPath(), user));
			}
			return fileObjects;
		} catch (IOException e) {
			log.debug("", e);
			return null;
		}
	}

	/**
	 * Creates output stream to write to the object
	 *
	 * @param l is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	@Override
    public OutputStream createOutputStream(long l) throws IOException {

		// permission check
		if (!isWritable()) {
			throw new IOException("No write permission : " + path);
		}

		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FSDataOutputStream out = dfs.create(hdfsPath);
			dfs.setOwner(hdfsPath, user.getName(), user.getMainGroup());
			return out;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates input stream to read from the object
	 *
	 * @param l is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	@Override
    public InputStream createInputStream(long l) throws IOException {
		// permission check
		if (!isReadable()) {
			throw new IOException("No read permission : " + path);
		}
		try {
            FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FSDataInputStream in = dfs.open(hdfsPath);
			return in;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
