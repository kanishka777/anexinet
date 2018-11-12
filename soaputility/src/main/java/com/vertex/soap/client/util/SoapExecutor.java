package com.vertex.soap.client.util;

 
import java.nio.charset.StandardCharsets;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;
 
public class SoapExecutor {

	private static final String HEADER = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">\r\n"
			+ "	<s:Body xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">  ";

	private static final String FOOTER = "	</s:Body>\r\n" + "</s:Envelope>";

	public static void execute(String input, StringBuilder builder, String fileName) throws Exception {
		String xml = HEADER + input + FOOTER;
		HttpClient httpClient = new HttpClient();
		PostMethod methodPost = new PostMethod("");
		RequestEntity requestEntity = new StringRequestEntity(xml, "text/xml", "UTF-8");
		methodPost.setRequestEntity(requestEntity);
		try {
			int returnCode = httpClient.executeMethod(methodPost);
			if (returnCode == HttpStatus.SC_OK) {
				String readLine = IOUtils.toString(methodPost.getResponseBodyAsStream(), StandardCharsets.UTF_8.name());
				builder.append("<tr><td>").append(fileName).append("</td><td>").append("Executed Successfully")
						.append("</td><td>").append(readLine).append("</td></tr>");
			} else if (returnCode == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
				String readLine = IOUtils.toString(methodPost.getResponseBodyAsStream(), StandardCharsets.UTF_8.name());
				builder.append("<tr><td>").append(fileName).append("</td><td>").append("Execution Failed")
						.append("</td><td>").append(readLine).append("</td></tr>");
			}
		} catch (Exception e) {
			builder.append("<tr><td>").append(fileName).append("</td><td>").append("Execution Failed")
					.append("</td><td>").append(e.getMessage()).append("</td></tr>");
		} finally {
			methodPost.releaseConnection();
		}
	}

 
}
