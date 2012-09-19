import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import com.flashmedia.dbase.DAOException;
import com.flashmedia.dbase.DBUser;
import com.flashmedia.dbase.UserDAO;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;



public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Load properties file. Name=" + args[0]);
		if (args.length > 0) {
			FileInputStream sr = null;
			Properties p = new Properties();
			try {
				sr = new FileInputStream(args[0]);
				p.load(sr);
				sr.close();
			} catch (IOException e) {
				System.out.println("Could not load properties!");
				e.printStackTrace();
				return;
			}
			System.out.println("message=" + p.getProperty("message"));
			System.out.println("api_url=" + p.getProperty("api_url"));
			System.out.println("appId=" + p.getProperty("appId"));
			System.out.println("secure_api_key=" + p.getProperty("secure_api_key"));
			if (p.containsKey("message") && p.containsKey("api_url") && p.containsKey("appId") && p.containsKey("secure_api_key")) {
				Sender s = new Sender(
						p.getProperty("secure_api_key"),
						p.getProperty("api_url"),
						p.getProperty("appId"),
						p.getProperty("message"));
				Thread t = new Thread(s);
				t.start();
			}
			else {
				System.out.println("Could not get all parameters.");
			}
		}
		else {
			System.out.println("File name can be passed");
		}
	}

}

class Sender implements Runnable {

	private final int ERROR_ACCESS_DENIED = 15;
	private final int MAX_UIDS_PER_OPERATION = 100;
	
	private String message;
	private String appId;
	private String api_url;
	private String secure_api_key;
	
	private int successNotifications;
	private int failedNotifications;
	private int errorsAccessDenied;
	private int errorsUnknown;
	
	public Sender(String $secure_api_key, String $api_url, String $appId, String $msg) {
		secure_api_key = $secure_api_key;
		api_url = $api_url;
		appId = $appId;
		message = $msg;
	}
	
	@Override
	public void run() {
		System.out.println("Sending started...");
		int startId = 0;
		Vector<DBUser> users = null;
		String uidsStr = null;
//		send("9028622");
//		send("118759665");
		while (true) {
			uidsStr = "";
			try {
				users = UserDAO.getInstance().getSomeUsers(startId, MAX_UIDS_PER_OPERATION);
			}
			catch (DAOException e) {
				e.printStackTrace();
				break;
			}
			if (users.size() == 0) {
				break;
			}
			for (int i = 0; i < users.size(); i++) {
				uidsStr += ((i != 0) ? ",": "");
				uidsStr += new Integer(users.get(i).id_vk).toString();
			}
			System.out.println(uidsStr);
			send(uidsStr);
			startId = users.lastElement().id;
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Sending completed.");
		System.out.println("Success notifications: " + successNotifications);
		System.out.println("Failed notifications: " + failedNotifications);
		System.out.println("	Errors access denied: " + errorsAccessDenied);
		System.out.println("	Errors unknown: " + errorsUnknown);
	}
	
	private void send(String uids) {
		int uidsCount = uids.split(",").length;
		Random r = new Random();
		r.nextInt(1000);
		String timeStamp = new Long(System.currentTimeMillis()).toString();
		String randStr = Integer.toString(r.nextInt(1000));
		String params = "api_id=" + appId +
				"format=json" +
				"message=" + message + /* Исходное сообщение: Message From Bar*/
				"method=secure.sendNotification" +
				"random=" + randStr +
				"timestamp=" + timeStamp +
				"uids=" + uids +
				"v=2.0";
		String sig = md5(params + secure_api_key);
		String urlEncodedMessage = null;
		try {
			urlEncodedMessage = URLEncoder.encode(message, "UTF-8");
		}
		catch (UnsupportedEncodingException e) {
		}
		String requestStr = null;
		if (urlEncodedMessage != null) {
			requestStr = api_url + "?" +
					"api_id=" + appId +
					"&format=json" +
					"&message=" + urlEncodedMessage +	/* Уже коилрованное сообщение urlencoded: Message%20From%20Bar*/
					"&method=secure.sendNotification" +
					"&random=" + randStr +
					"&timestamp=" + timeStamp +
					"&uids=" + uids +
					"&v=2.0" +
					"&sig=" + sig;
		}
		try {
			URL vkUrl = new URL(requestStr);
			URLConnection uc = vkUrl.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(uc.getInputStream()));
			String responseStr = "";
			String inputLine = "";
			while ((inputLine = in.readLine()) != null) {
				responseStr += inputLine;
			}
			in.close();
			try {
				//при успешной отправке в ответ должен прийти ид пользователя
				JSONObject resp = (JSONObject)JSONSerializer.toJSON(responseStr);
				String response = null;
				try {
					response = resp.getString("response");
				}
				catch (JSONException e) {
				}
				if (response != null) {
					JSONObject error = null;
					try {
						error = resp.getJSONObject("error");
					}
					catch (JSONException e) {
					}
					if (error != null) {
						int errorCode = error.getInt("error_code");
						switch (errorCode) {
							case ERROR_ACCESS_DENIED: {
								errorsAccessDenied++;
								break;
							}
							default: {
								errorsUnknown++;
								System.out.println(responseStr);
							}
						}
						failedNotifications += uidsCount;
					}
					else {
						//надо посчитать по количеству возвращенных uids - сколько дейсвительно было отправлено нотификаций
						if (response.length() == 0) {
							failedNotifications += uidsCount;
						}
						else {
							String[] responseUids = response.split(",");
							successNotifications += responseUids.length;
							failedNotifications += (uidsCount - responseUids.length);
						}
					}
				}
				else {
					failedNotifications += uidsCount;
					errorsUnknown++;
					System.out.println(responseStr);
				}
			}
			catch (Exception e) {
				failedNotifications++;
				errorsUnknown++;
				System.out.println(responseStr);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String md5(String s) {
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			byte[] bytes = s.getBytes("UTF-8");
			m.update(bytes, 0, bytes.length); //ISO-8859-1, Cp1251, US-ASCII
			BigInteger i = new BigInteger(1,m.digest());
			return String.format("%1$032x", i);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
//	public static String md5(String text) {
//		try {
//	        MessageDigest md;
//	        md = MessageDigest.getInstance("MD5");
//	        byte[] md5hash = new byte[32];
//	        md.update(text.getBytes("UTF-8"), 0, text.length());
//	        md5hash = md.digest();
//	        return convertToHex(md5hash);
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//		}
//		return "";
//	}
//
//	private static String convertToHex(byte[] data) {
//	    StringBuffer buf = new StringBuffer();
//	    for (int i = 0; i < data.length; i++) {
//	        int halfbyte = (data[i] >>> 4) & 0x0F;
//	        int two_halfs = 0;
//	        do {
//	                if ((0 <= halfbyte) && (halfbyte <= 9))
//	                buf.append((char) ('0' + halfbyte));
//	            else
//	                buf.append((char) ('a' + (halfbyte - 10)));
//	                halfbyte = data[i] & 0x0F;
//	        } while(two_halfs++ < 1);
//	    }
//	    return buf.toString();
//	}
}
