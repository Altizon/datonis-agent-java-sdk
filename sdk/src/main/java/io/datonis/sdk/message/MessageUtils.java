package io.datonis.sdk.message;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.json.simple.JSONObject;



public class MessageUtils
{

	final protected static char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	private static String bytesToHex(byte[] bytes)
	{

		char[] hexChars = new char[bytes.length * 2];
		int v;

		for (int j = 0; j < bytes.length; j++)
		{
			v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}

		return new String(hexChars);
	}

	public static String encode(String key, String value)
	{
		if(key == null || value == null)
			return null;
		
		String encodedString = null;
		try
		{
	
			Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
			SecretKeySpec secret_key = new SecretKeySpec(key.getBytes(), "HmacSHA256");
			sha256_HMAC.init(secret_key);

			byte[] checksum = sha256_HMAC.doFinal(value.getBytes());
			encodedString = MessageUtils.bytesToHex(checksum);
		}
		catch (Exception e)
		{
		}
		return encodedString;

	}
	
	public static JSONObject encodeObject(JSONObject data, String accessKey, String secretKey)
	{
		// Encode the JSON data string.
		data.put(MessageConstants.HASH, encode(secretKey, data.toJSONString()));
		data.put(MessageConstants.ACCESS_KEY, accessKey);
		return data;
	}
	
	public static void main(String[] args) {
        System.out.println(encode("ab59t2d22c2363daf51863tcd568834c2ccc3c99", "{\"alert\":{\"thing_key\":\"3fceb99e5ed9b34b9ba5993b9e61t6f7ce741t5e\",\"timestamp\":\"1420540311\",\"status\":\"0\",\"message\":\"Happy holi\",\"data\":{\"action\":\"kill\"}}}"));
    }
}
