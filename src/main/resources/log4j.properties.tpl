log4j.rootLogger=INFO, RFA

log4j.appender.RFA=org.apache.log4j.RollingFileAppender
log4j.appender.RFA.File={{ app_log_dir }}/server.log
log4j.appender.RFA.MaxFileSize=100MB
log4j.appender.RFA.MaxBackupIndex=4
log4j.appender.RFA.layout=org.apache.log4j.PatternLayout
log4j.appender.RFA.layout.ConversionPattern=[%d] %p [%t] %l %m%n
log4j.appender.RFA.Threshold = INFO

log4j.appender.F=org.apache.log4j.FileAppender
log4j.appender.F.File={{ app_log_dir }}/error.log
log4j.appender.F.MaxFileSize=100MB
log4j.appender.F.MaxBackupIndex=4
log4j.appender.F.layout=org.apache.log4j.PatternLayout
log4j.appender.F.layout.ConversionPattern=[%d] %p [%t] %l %m%n
log4j.appender.F.threshold=ERROR


log4j.logger.org.apache.hadoop.contrib.ftp=INFO,F
log4j.logger.org=WARN
log4j.logger.com=WARN
