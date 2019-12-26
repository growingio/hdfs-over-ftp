package org.apache.hadoop.contrib.ftp.command;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.ftpserver.util.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

/**
 * Desc: Digestsite command
 * <p>
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-12-26
 */
public abstract class DigestCommand extends AbstractCommand {

    private final Logger LOG = LoggerFactory.getLogger(DigestCommand.class);

    /**
     * Execute command.
     */
    @Override
    public void execute(final FtpIoSession session, final FtpServerContext context,
                        final FtpRequest request) throws IOException {
        // reset state variables
        session.resetState();

        // print file information
        String argument = request.getArgument();

        if (argument == null || argument.trim().length() == 0) {
            session
                    .write(LocalizedFtpReply
                            .translate(
                                    session,
                                    request,
                                    context,
                                    FtpReply.REPLY_504_COMMAND_NOT_IMPLEMENTED_FOR_THAT_PARAMETER,
                                    "Digest.invalid", null));
            return;
        }

        String[] fileNames = argument.split(",");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fileNames.length; i++) {
            String fileName = fileNames[i].trim();

            // get file object
            FtpFile file = null;

            try {
                file = session.getFileSystemView().getFile(fileName);
            } catch (Exception ex) {
                LOG.error("Exception getting the file object: " + fileName, ex);
            }

            if (file == null) {
                session
                        .write(LocalizedFtpReply
                                .translate(
                                        session,
                                        request,
                                        context,
                                        FtpReply.REPLY_504_COMMAND_NOT_IMPLEMENTED_FOR_THAT_PARAMETER,
                                        "Digest.invalid", fileName));
                return;
            }

            // check file
            if (!file.isFile()) {
                session
                        .write(LocalizedFtpReply
                                .translate(
                                        session,
                                        request,
                                        context,
                                        FtpReply.REPLY_504_COMMAND_NOT_IMPLEMENTED_FOR_THAT_PARAMETER,
                                        "Digest.invalid", fileName));
                return;
            }

            InputStream is = null;
            try {
                is = file.createInputStream(0);
                String digestString = digest(is);

                if (i > 0) {
                    sb.append(", ");
                }
                boolean nameHasSpaces = fileName.indexOf(' ') >= 0;
                if (nameHasSpaces) {
                    sb.append('"');
                }
                sb.append(fileName);
                if (nameHasSpaces) {
                    sb.append('"');
                }

                sb.append(' ');
                sb.append(digestString);

            } catch (NoSuchAlgorithmException e) {
                LOG.error("Digest algorithm not available", e);
                session.write(LocalizedFtpReply.translate(session, request, context,
                        FtpReply.REPLY_502_COMMAND_NOT_IMPLEMENTED,
                        "Digest.notimplemened", null));
            } finally {
                IoUtils.close(is);
            }
        }

        // 必须是 251.MD5 模板, see DefaultMessageResource#getMessage
        // 或者自定义模板
        session.write(LocalizedFtpReply.translate(session, request, context,
                251, "MD5", sb.toString()));
    }

    /**
     * @param is InputStream for which the hash is calculated
     *
     * @return The hash of the content in the input stream
     *
     * @throws IOException
     */
    protected abstract String digest(InputStream is) throws IOException, NoSuchAlgorithmException;

}
