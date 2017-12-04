/* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package io.datonis.sdk.compress.lzf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFException;

/**
 * Simple command-line utility that can be used for testing LZF compression, or
 * as rudimentary command-line tool. Arguments are the same as used by the
 * "standard" lzf command line tool
 * 
 * @author Tatu Saloranta (tatu@ning.com)
 */
public class LZF {
	private static final Logger logger = LoggerFactory.getLogger(LZF.class);
    public static void main(String[] args) throws IOException {
        
        String str = "Hello World";
        testLZF(str);
    }

    public static byte[] compress(String input) {    	
    	return LZFEncoder.encode(input.getBytes(StandardCharsets.UTF_8));
    }

	private static void testLZF(String input) throws LZFException{
		logger.info("String length is"+ input.length());
    	byte[] op = LZF.compress(input);
    	logger.info("Compressed Length " + op.length);

        byte[] uncompressed = LZF.decompress(op);
        String unc = null;
        if (uncompressed != null) {
            unc = new String(uncompressed);
        	logger.info("\n" + unc);
        	logger.info(unc.length() + " " + input.length());
        }

    	logger.info("Comparison result- "+ unc.equals(input));
	}

    public static byte[] decompress(byte[] input) throws LZFException{
    	logger.debug("Compressed block Length is " + input.length);
    	return LZFDecoder.decode(input);
    }
}
