package org.apache.hadoop.contrib.ftp.command;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

/**
 * Desc: SHA512 site command
 * <p>
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-12-26
 */
public class SHA512 extends DigestCommand {

    /**
     * @param is InputStream for which the hash is calculated
     * @return The hash of the content in the input stream
     * @throws IOException
     */
    @Override
    protected String digest(InputStream is) throws IOException, NoSuchAlgorithmException {
        return DigestUtils.sha512Hex(is);
    }
}
