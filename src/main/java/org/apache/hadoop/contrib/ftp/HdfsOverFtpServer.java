package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.CommandFactory;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.hadoop.contrib.ftp.command.SHA1;
import org.apache.hadoop.contrib.ftp.command.SHA256;
import org.apache.hadoop.contrib.ftp.command.SHA512;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

/**
 * Start-up class of FTP server
 */
public class HdfsOverFtpServer {

	private static Logger log = Logger.getLogger(HdfsOverFtpServer.class);

	private static HdfsOverFtpConf conf = null;

	public static void main(String[] args) throws Exception {
		loadConfig();

		if (conf.getPort() != 0) {
			startServer();
		}

		if (conf.getSslPort() != 0) {
			startSSLServer();
		}
	}

	/**
	 * Load configuration
	 */
	private static void loadConfig() throws IOException {
        conf = HdfsOverFtpConf.load(new FileInputStream(loadResource("/conf/hdfs-over-ftp.properties")));
	}

	/**
	 * Starts FTP server
	 */
	public static void startServer() throws Exception {

		log.info(
				"Starting Hdfs-Over-Ftp server. port: " + conf.getPort() + " data-ports: " + conf.getPassivePorts() + " hdfs-uri: " + conf.getHdfsPath());

		HdfsOverFtpSystem.setConf(conf);

        FtpServerFactory serverFactory = new FtpServerFactory();

        DataConnectionConfigurationFactory dataConFactory = new DataConnectionConfigurationFactory();
		if(conf.getHost() != null) {
			dataConFactory.setPassiveAddress(conf.getHost());
		}
        dataConFactory.setPassivePorts(conf.getPassivePorts());
		if(conf.getExtAddr() != null) {
			dataConFactory.setPassiveExternalAddress(conf.getExtAddr());
		}
        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setDataConnectionConfiguration(dataConFactory.createDataConnectionConfiguration());
        listenerFactory.setPort(conf.getPort());
        if(conf.getHost() != null) {
			listenerFactory.setServerAddress(conf.getHost());
		}
        serverFactory.addListener("default", listenerFactory.createListener());
		HdfsUserManager userManager = new HdfsUserManager();
		final File file = loadResource("/conf/users.properties");

		userManager.setFile(file);
        serverFactory.setUserManager(userManager);
        serverFactory.setFileSystem(new HdfsFileSystemManager());
        serverFactory.setCommandFactory(createCommandFactory());

        // start
        FtpServer server = serverFactory.createServer();
		server.start();
	}

	private static File loadResource(String resourceName) {
		final URL resource = HdfsOverFtpServer.class.getResource(resourceName);
		if (resource == null) {
			throw new RuntimeException("Resource not found: " + resourceName);
		}
		return new File(resource.getFile());
	}

	/**
	 * Starts SSL FTP server
	 *
	 * @throws Exception
	 */
	public static void startSSLServer() throws Exception {

		log.info(
				"Starting Hdfs-Over-Ftp SSL server. ssl-port: " + conf.getSslPort() + " ssl-data-ports: " + conf.getSslPassivePorts() + " hdfs-uri: " + conf.getHdfsPath());


		HdfsOverFtpSystem.setConf(conf);

        FtpServerFactory serverFactory = new FtpServerFactory();

		MySslConfiguration ssl = new MySslConfiguration();
		ssl.setKeystoreFile(new File("ftp.jks"));
		ssl.setKeystoreType("JKS");
		ssl.setKeyPassword("333333");

        DataConnectionConfigurationFactory dataConFactory = new DataConnectionConfigurationFactory();
        dataConFactory.setPassivePorts(conf.getSslPassivePorts());

        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setDataConnectionConfiguration(dataConFactory.createDataConnectionConfiguration());
        listenerFactory.setPort(conf.getSslPort());
        listenerFactory.setSslConfiguration(ssl);
        listenerFactory.setImplicitSsl(true);

        serverFactory.addListener("default", listenerFactory.createListener());


		HdfsUserManager userManager = new HdfsUserManager();
		userManager.setFile(new File("users.conf"));
        serverFactory.setUserManager(userManager);
        serverFactory.setFileSystem(new HdfsFileSystemManager());
        serverFactory.setCommandFactory(createCommandFactory());

        // start
        FtpServer server = serverFactory.createServer();
		server.start();
	}

	// add site command
	private static CommandFactory createCommandFactory() {
        CommandFactoryFactory cFac = new CommandFactoryFactory();
        cFac.addCommand("SHA1", new SHA1());
        cFac.addCommand("SHA256", new SHA256());
        cFac.addCommand("SHA512", new SHA512());
        return cFac.createCommandFactory();
    }
}
