package org.apache.hadoop.contrib.ftp;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Desc: 配置参数
 * <p>
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-12-25
 */
public class HdfsOverFtpConf implements Serializable {
    private static Logger log = Logger.getLogger(HdfsOverFtpConf.class);

    private String hdfsPath;
    private boolean permission;
    private String superuser;
    private String supergroup;
    private String host;
    private Integer port;
    private Integer sslPort;
    private String passivePorts;
    private String sslPassivePorts;

    public HdfsOverFtpConf(String hdfsPath, boolean permission, String superuser, String supergroup, String host, Integer port, Integer sslPort, String passivePorts, String sslPassivePorts) {
        this.hdfsPath = hdfsPath;
        this.permission = permission;
        this.superuser = superuser;
        this.supergroup = supergroup;
        this.host = host;
        this.port = port;
        this.sslPort = sslPort;
        this.passivePorts = passivePorts;
        this.sslPassivePorts = sslPassivePorts;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public String getSuperuser() {
        return superuser;
    }

    public String getSupergroup() {
        return supergroup;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public Integer getSslPort() {
        return sslPort;
    }

    public String getPassivePorts() {
        return passivePorts;
    }

    public String getSslPassivePorts() {
        return sslPassivePorts;
    }

    public boolean isPermission() {
        return permission;
    }

    /**
     * 加载配置
     */
    public static HdfsOverFtpConf load(InputStream inputStream) throws IOException {
        Properties props = new Properties();
        props.load(inputStream);

        String host = null;
        int port = 0;
        int sslPort = 0;
        boolean permission;
        String passivePorts = null;
        String sslPassivePorts = null;
        String hdfsUri = null;
        try {
            host = props.getProperty("host", null);
            if(host != null) {
                log.info("host is set. ftp server will listen on " + host);
            }
        } catch (Exception e) {
            log.info("unexpected error", e);
        }

        try {
            port = Integer.parseInt(props.getProperty("port"));
            log.info("port is set. ftp server will be started");
        } catch (Exception e) {
            log.info("port is not set. so ftp server will not be started");
        }

        try {
            sslPort = Integer.parseInt(props.getProperty("ssl-port"));
            log.info("ssl-port is set. ssl server will be started");
        } catch (Exception e) {
            log.info("ssl-port is not set. so ssl server will not be started");
        }

        if (port != 0) {
            passivePorts = props.getProperty("data-ports");
            if (passivePorts == null) {
                log.fatal("data-ports is not set");
                System.exit(1);
            }
        }

        if (sslPort != 0) {
            sslPassivePorts = props.getProperty("ssl-data-ports");
            if (sslPassivePorts == null) {
                log.fatal("ssl-data-ports is not set");
                System.exit(1);
            }
        }

        hdfsUri = props.getProperty("hdfs-uri");
        if (hdfsUri == null) {
            log.fatal("hdfs-uri is not set");
            System.exit(1);
        }

        permission = Boolean.valueOf(props.getProperty("permission", "false"));

        String superuser = props.getProperty("superuser");
        if (superuser == null) {
            log.fatal("superuser is not set");
            System.exit(1);
        }

        String supergroup = props.getProperty("supergroup");
        if (supergroup == null) {
            log.fatal("supergroup is not set");
            System.exit(1);
        }

        return new HdfsOverFtpConf(hdfsUri, permission, superuser, supergroup, host, port, sslPort, passivePorts, sslPassivePorts);
    }

    @Override
    public String toString() {
        return "HdfsOverFtpConf{" +
                "hdfsPath='" + hdfsPath + '\'' +
                ", permission=" + permission +
                ", superuser='" + superuser + '\'' +
                ", supergroup='" + supergroup + '\'' +
                ", host=" + host +
                ", port=" + port +
                ", sslPort=" + sslPort +
                ", passivePorts='" + passivePorts + '\'' +
                ", sslPassivePorts='" + sslPassivePorts + '\'' +
                '}';
    }
}
