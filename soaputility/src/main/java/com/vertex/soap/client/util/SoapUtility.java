package com.vertex.soap.client.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;

public class SoapUtility {
	
	static StringBuilder buf = new StringBuilder();

	public static void main(String[] args) throws Exception {
		
		if (args.length == 0) {
			System.err.println("Provide the base location for the xml files");
			System.exit(0);
		}
		buf.append("<html>" + "<body>" + "<table border=\"1\">" + "<tr>" + "<th>File-Name</th>" + "<th>Status</th>"
				+ "<th>Response</th>" + "</tr>");
		String location = args[0];
		try (Stream<Path> paths = Files.walk(Paths.get(location))) {
			paths.forEach(filePath -> {
				if (Files.isRegularFile(filePath) && filePath.toString().endsWith(".xml")) {
					try {
						executeSoapCall(filePath);
					} catch (Exception e) {
						buf.append("<tr><td>").append(filePath.toFile().toString()).append("</td><td>")
								.append("Execution Failed").append("</td><td>").append(e.getMessage())
								.append("</td></tr>");
					}
				}
			});
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
		buf.append("</table>" + "</body>" + "</html>");
		String html = buf.toString();
		Files.write(Paths.get(location + "\\output.html"), html.getBytes());
	}

	public static void executeSoapCall(Path filePath) throws Exception {
		try {
			String xml = FileUtils.readFileToString(filePath.toFile(), "UTF-8");
			SoapExecutor.execute(xml, buf, filePath.toFile().toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
