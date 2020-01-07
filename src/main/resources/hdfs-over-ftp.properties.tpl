#uncomment this to run ftp server
port = {{ custom_configs.hdfs_over_ftp.port }}
data-ports = {{ custom_configs.hdfs_over_ftp.data_port }}

#uncomment this to run ssl ftp server
#ssl-port = 2226
#ssl-data-ports = 2227-2229

# hdfs uri
hdfs-uri = {{ custom_configs.hdfs_over_ftp.data_dir }}
permission = false

# have to be a user which runs HDFS
# this allows you to start ftp server as a root to use 21 port
# and use hdfs as a superuser
superuser = {{ custom_configs.hdfs_over_ftp.superuser }}
supergroup = {{ custom_configs.hdfs_over_ftp.supergroup }}
